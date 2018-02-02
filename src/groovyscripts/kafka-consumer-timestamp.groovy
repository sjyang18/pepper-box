/**
 * A Kafka consumer which adds a timestamp to each received-message.
 *
 * The script calculates the delay between when the message was generated and
 * when it was received.
 *
 * The processing uses a micro-batch polling system so the timestamp
 * includes a subset of the polling interval as well as the main processing/
 * propagation time intrinsic to the Kafka subsystem.
 *
 * Basic support for SASL_SSL is included as that's essential for some of the
 * testing I need to do.
 *
 * Known limitations:
 * The script relies on pre-defined variables, which I define in the Test Plan
 * as User Defined Variables. It doesn't really check their validity (yet) so
 * please consider this a proof-of-concept that's prone to problems if it's
 * not provided with valid settings.
 *
 * The support for SASL_SSL is based on the immediate needs of the project I'm
 * working on. YMMV. Improvements are welcome.
 */

import groovy.json.JsonSlurper
import org.apache.jmeter.samplers.SampleResult
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.protocol.SecurityProtocol

import java.nio.charset.StandardCharsets

String bootstrap_servers = getParam("bootstrap.servers", true)
String topic_prefix = getParam("topic.prefix", false, "")
String topic = topic_prefix + getParam("topic", true)
String generate_per_thread_topics = getParam("generate.per-thread.topics", false, "YES")
String threadz = getParam("threadz", true, 5, 'integer')
Integer counter = Integer.valueOf(args[0]) % Integer.valueOf(threadz)

String sasl_jaas_username = getParam("sasl.jaas.username")
String sasl_jaas_password = getParam("sasl.jaas.password")
String security_protocol = getParam("security.protocol")
log.info("using security.protocol:" + security_protocol)

String ssl_truststore_location = getParam("ssl.truststore.location")
String ssl_truststore_password = getParam("ssl.truststore.password")

Long WAITING_PERIOD = 30000  // 30 seconds to wait for additional messages.

// Add validation of the input parameters around here. These are only examples
// See https://github.com/commercetest/pepper-box/issues/8 for context

if (bootstrap_servers.length() < 8) {
    log.error("bootstrap.servers too short to be trusted: " + bootstrap_servers)
    ctx.getEngine().stopTest()
} else {
    log.info("bootstrap.servers:" + bootstrap_servers)
}

if (sasl_jaas_username.length() < 3) {
   log.error("sasl.jaas.username too short, aborting test")
   ctx.getEngine().stopTest()
}

String sasl_jaas_config = "org.apache.kafka.common.security.plain.PlainLoginModule required" +
        " username=\"${sasl_jaas_username}\"" +
        " password=\"${sasl_jaas_password}\";"
log.info("sasl_jaas_config: " + sasl_jaas_config)

String group = "jmeter-consumer"

// TODO find a way to obtain the hostname,
// log.info("host:" + vars.get("HOST")) isn't correct.
// See https://github.com/commercetest/pepper-box/issues/7 for the context

Properties props = new Properties()
props.put("bootstrap.servers", bootstrap_servers)
props.put("group.id", group)
props.put("enable.auto.commit", "false")
props.put("auto.offset.reset", "earliest")  // start at the beginning of the topic (since it's created for us)
props.put("session.timeout.ms", "10000")
props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")

/*
 * The following is essentially a replica of what I've coded in the Java code
 * PepperBoxKafkaSampler.java
 * I'd prefer to share the configuration settings and will aim to do so if and
 * when I find a practical way to so do.
 */
if (security_protocol == SecurityProtocol.SASL_SSL.name) {
    props.put("security.protocol", security_protocol)
    props.put("sasl.jaas.config", sasl_jaas_config)
    props.put("sasl.mechanism", "PLAIN") // hard-coded for now
    props.put("ssl.enabled.protocols", "TLSv1.2")
    props.put("ssl.truststore.location", ssl_truststore_location)
    props.put("ssl.truststore.password", ssl_truststore_password)
    props.put("ssl.truststore.type", "JKS")
}

// Here's where the Kafka Consumer is created, using the properties we've set earlier.
KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)

// Here we decide whether to listen to distinct topics or a shared topic.
if (generate_per_thread_topics?.trim() && generate_per_thread_topics.equalsIgnoreCase("yes")) {
    thread_topic_name = topic + "." + counter
    consumer.subscribe(Arrays.asList(thread_topic_name))
    log.info("Subscribed to per thread topic:" + thread_topic_name)
} else {
    consumer.subscribe(Arrays.asList(topic))
    log.info("Subscribed to common topic:" + topic)
}

// This is the way to 'tell' jmeter whether the result of this script is OK.
// However, the script processes potentially many messages but can only provide
// one 'result'. Therefore we use sub-results too, sadly these aren't reported
// on by jmeter.
SampleResult globalResult = new SampleResult()
globalResult.sampleStart()

def jsonSlurper = new JsonSlurper()
long end = System.currentTimeMillis() + WAITING_PERIOD

String results_filename = "results-" + topic + "-" + counter + ".csv"
log.info("Creating file [" + results_filename + "]")

f = new FileOutputStream(results_filename, true)
p = new PrintStream(f)
p.println("batchReceived,messageGenerated,consumerLag,messageId,recordOffset")

int messagesProcessed = 0
long prevMessageId
int previousCount = -1  // Initialise to a value that shouldn't be returned so the first results are always shown.
while (System.currentTimeMillis()<end)
{
    long batchReceived = System.currentTimeMillis()
    ConsumerRecords<String, String> records = consumer.poll(100)
    // First record what we received this time, if it's changed from before to keep the logs compact.
    if (records.count() != previousCount) {
        log.info("" + records.count() + " messages received this time.")
        previousCount = records.count()
    }

    if (records.count() == 0) {
        continue  // skip the rest of this loop and poll again instead.
    }
    for (ConsumerRecord<String, String> record : records)
    {
       SampleResult sampleResult = new SampleResult()
       sampleResult.sampleStart()

       def result = jsonSlurper.parseText(record.value())
       long messageId = Long.valueOf(result.messageId)

       if (prevMessageId || messageId == prevMessageId + 1) {
           sampleResult.setResponseData(record.value(), StandardCharsets.UTF_8.name())
           sampleResult.setSuccessful(true)
       } else {
           log.warn("Messages were not contiguous. [prevMessageId="+prevMessageId+"] [thisMessageId="+messageId+"]")
           OUT.println("WARN - Messages were not contiguous. [prevMessageId="+prevMessageId+"] [thisMessageId="+messageId+"]")
           sampleResult.setResponseData(record.value(), StandardCharsets.UTF_8.name())
           sampleResult.setSuccessful(false)
       }

       sampleResult.sampleEnd()
       globalResult.addSubResult(sampleResult)

        Long consumerLag = batchReceived - result.messageTime

        prevMessageId = messageId
        messagesProcessed++

       p.println("" + batchReceived.toString() + "," + result.messageTime.toString() + "," + consumerLag.toString() + "," + result.messageId.toString() + "," + record.offset().toString());
       end = System.currentTimeMillis() + WAITING_PERIOD  // increment the how long to wait for more data time
   }
   consumer.commitSync()  // Now we've processed the records, let Kafka know.
}
globalResult.setResponseData("" + messagesProcessed + " messages processed.", StandardCharsets.UTF_8.name())
globalResult.setSuccessful(true)
globalResult.sampleEnd()

consumer.close()
p.close()
f.close()


def getParam(String paramName, boolean required = false, fallbackValue = null, castType = 'string'){
    String val = vars.get(paramName);
    if(val == null) {
        if(required) {
            log.error("InvalidArgument - Parameter [" + paramName + "] is required");
            ctx.getEngine().stopTestImmediately();
        }
        log.info("CONFIG ["+paramName+"="+fallbackValue+"]");
        return fallbackValue;
    } else {
        try {
            log.info("CONFIG ["+paramName+"="+val+"]");
            if (castType == 'string') {
                return val;
            } else if (castType == 'integer') {
                return Integer.valueOf(val);
            } else  if(castType == 'boolean') {
                return Boolean.valueOf(val);
            } else {
                log.warn("InvalidArgumentType - Unexpected type ["+castType+"] for parameter ["+paramName+"] - Use one of [string,integer,boolean]");
                return val;
            }
        } catch(e) {
            log.error("InvalidArgument - Unable to cast ["+paramName+"] (["+val+"]) to a ["+castType+"]");
            ctx.getEngine().stopTestImmediately();
        }
    }
}

return globalResult