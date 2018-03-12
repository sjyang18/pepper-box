package com.gslab.pepper;

import com.google.common.util.concurrent.RateLimiter;
import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.util.ConsumerKeys;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.utils.CommandLineUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * PepperBoxLoadConsumer: the partner of PepperBoxLoadGenerator.
 * Calculates key metrics: consumerLag, percentiles, and standard deviation
 * using a combination of data provided by the Generator in each message and
 * local info. The time-clocks are assumed to be similar on both the Generator
 * and where this program runs. The may be run on the same machine or separately.
 *
 * Optionally this program can reduce the rate of consumption to simulate a
 * slower consumer.
 */
public class PepperBoxLoadConsumer extends Thread {
    private static int totalThreads;
    private static int offset;
    private long durationInMillis;
    private RateLimiter limiter;
    KafkaConsumer<String, String> kafkaConsumer;
    String perThreadTopic;

    private static String PEPPERBOX_GROUP_NAME = "pepperbox_loadgenerator";
    private static Long POLLING_INTERVAL = 100L;
    private static Logger LOGGER = Logger.getLogger(PepperBoxLoadConsumer.class.getName());

    PepperBoxLoadConsumer(Integer thread, String consumerConfig, Integer throughput, Integer duration) throws PepperBoxException {
        Integer topicId = thread + offset;
        Thread t = currentThread();
        t.setName(topicId.toString());

        Properties kafkaProperties = populateConsumerProperties(consumerConfig);

        perThreadTopic = kafkaProperties.getProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG) + "." + topicId.toString();
        kafkaProperties.setProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG, perThreadTopic);
        LOGGER.log(Level.INFO, "Thread [" + topicId.toString() + "] using topic [" +
                kafkaProperties.getProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG) + "]");

        createConsumer(kafkaProperties, throughput, duration);
    }

    PepperBoxLoadConsumer(String consumerConfig, Integer throughput, Integer duration) throws PepperBoxException {
        Properties kafkaProperties = populateConsumerProperties(consumerConfig);
        perThreadTopic = kafkaProperties.getProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG);
        createConsumer(kafkaProperties, throughput, duration);
    }

    Properties populateConsumerProperties(String consumerProps) throws PepperBoxException {
        Properties kafkaProperties = new Properties();
        try {
            kafkaProperties.load(new FileInputStream(consumerProps));
        } catch (IOException e) {
            throw new PepperBoxException(e);
        }
        return kafkaProperties;
    }

    private void createConsumer(Properties kafkaProperties, Integer throughput, Integer duration) throws PepperBoxException {

        limiter = RateLimiter.create(throughput);
        durationInMillis = TimeUnit.SECONDS.toMillis(duration);
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put("group.id", PEPPERBOX_GROUP_NAME);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest") ; // start at the beginning of the topic (since it's created for us)
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        final String security_protocol = kafkaProperties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        System.out.println("security_protocol set to[" + security_protocol + "], comparing to [" + SecurityProtocol.SASL_SSL.name +"].");
        if (security_protocol.equals(SecurityProtocol.SASL_SSL.name)) {
            LOGGER.info("Adding SASL_SSL parameters for Kafka to use.");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, security_protocol);
            String sasl_jaas_config = "org.apache.kafka.common.security.plain.PlainLoginModule required" +
                    " username=\"" + kafkaProperties.getProperty("sasl.jaas.username") +
                    "\" password=\"" + kafkaProperties.getProperty("sasl.jaas.password") + "\";";
            System.out.println("sasl.jaas.config: " + sasl_jaas_config);
            props.put("sasl.jaas.config", sasl_jaas_config);
            props.put("sasl.mechanism", kafkaProperties.getProperty("sasl.mechanism"));

            props.put("ssl.enabled.protocols", kafkaProperties.getProperty("ssl.enabled.protocols"));
            props.put("ssl.truststore.location", kafkaProperties.getProperty("ssl.truststore.location"));
            props.put("ssl.truststore.password", kafkaProperties.getProperty("ssl.truststore.password"));
            props.put("ssl.truststore.type", kafkaProperties.getProperty("ssl.truststore.type"));
        }


        kafkaConsumer = new KafkaConsumer<>(props);
        LOGGER.info("Created Kafka Consumer");
    }

    @Override
    public void run() {
        int messagesProcessed = 0;
        try {
            kafkaConsumer.subscribe(Arrays.asList(perThreadTopic));
            LOGGER.info("Duration: " + durationInMillis + " Subscribed to: " + perThreadTopic);
            long endTime = durationInMillis + System.currentTimeMillis();
            int previousCount = -1;

            String resultsFilename = "results-" + perThreadTopic + "-of-" + (offset + totalThreads) + ".csv";
            // Create/open the results file and write the header row.
            LOGGER.info("Creating File:" + resultsFilename);
            FileOutputStream f = new FileOutputStream(resultsFilename, true);
            PrintStream p = new PrintStream(f);
            p.println("batchReceived,messageGenerated,consumerLag,messageId,recordOffset,messageSize,recordTimestamp");

            while (endTime > System.currentTimeMillis()) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(POLLING_INTERVAL);
                int currentCount = consumerRecords.count();
                if (currentCount != previousCount) {
                    System.out.println("Received [" + currentCount + "] records in " + POLLING_INTERVAL);
                    previousCount = currentCount;
                }

                if (currentCount == 0) {
                    // No more processing needed for this batch
                    continue;
                }
                long batchReceived = System.currentTimeMillis();

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    JSONParser jsonParser = new JSONParser();
                    try {
                        Object jsonObj = jsonParser.parse(record.value());
                        int i = record.value().length();  // Does this vary from the toString.length()?
                        JSONObject jsonObject = (JSONObject) jsonObj;
                        Long messageId = (Long) jsonObject.get("messageId");
                        Long createTimestamp = (Long) jsonObject.get("messageTime");
                        int size = record.toString().length();

                        Long consumerLag = batchReceived - createTimestamp;
                        p.println(String.format("%d,%d,%d,%d,%d,%d,%d",
                                batchReceived, createTimestamp, consumerLag, messageId, record.offset(), size, record.timestamp()));
                        messagesProcessed++;
                    } catch (ParseException pe) {
                        LOGGER.warning("Unable to parse record: " + record.toString());
                    }
                }
                kafkaConsumer.commitSync();
            }
            kafkaConsumer.unsubscribe();
            kafkaConsumer.close();
            p.close();
            f.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Finished topic: " + perThreadTopic + ", processed: " + messagesProcessed + " messages.");
        }
    }

    public static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec... required) {
        for (OptionSpec optionSpec : required) {
            if (!options.has(optionSpec)) {
                CommandLineUtils.printUsageAndDie(parser, "Missing required argument \"" + optionSpec + "\"");
            }
        }
    }

    public static void main(String args[]) {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> consumerConfig = parser.accepts("consumer-config-file", "REQUIRED: Kafka consumer properties filename.")
                .withRequiredArg()
                .describedAs("consumer properties")
                .ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> throughput = parser.accepts("throughput-per-consumer", "REQUIRED: Throttle rate per thread.")
                .withRequiredArg()
                .describedAs("throughput")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<Integer> duration = parser.accepts("test-duration", "REQUIRED: Test duration in seconds.")
                .withRequiredArg()
                .describedAs("test duration")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<Integer> threadCount = parser.accepts("num-consumers", "REQUIRED: Number of consumer threads.")
                .withRequiredArg()
                .describedAs("consumers")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<String> aTopicPerThread = parser.accepts("per-thread-topics", "OPTIONAL: Create a separate topic per producer")
                .withOptionalArg()
                .describedAs("create a topic per thread")
                .defaultsTo("NO")
                .ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> startingOffset = parser.accepts("starting-offset", "OPTIONAL: Starting count for separate topics, default 0")
                .withOptionalArg().ofType(Integer.class).defaultsTo(Integer.valueOf(0), new Integer[0])
                .describedAs("starting offset for the topic per thread")
                ;

        if (args.length == 0) {
            CommandLineUtils.printUsageAndDie(parser, "Kafka console load consumer.");
            System.exit(2);
        }
        OptionSet options = parser.parse(args);
        checkRequiredArgs(parser, options, consumerConfig, throughput, duration, threadCount);
        offset = options.valueOf(startingOffset);
        LOGGER.info("starting-offset: " + offset);
        try {
            totalThreads = options.valueOf(threadCount);
            for (int i = 0; i < totalThreads; i++) {
                PepperBoxLoadConsumer jsonConsumer;
                if (options.valueOf(aTopicPerThread).equalsIgnoreCase("YES")) {

                    jsonConsumer = new PepperBoxLoadConsumer(i,options.valueOf(consumerConfig), options.valueOf(throughput), options.valueOf(duration));
                } else {
                    jsonConsumer = new PepperBoxLoadConsumer(options.valueOf(consumerConfig), options.valueOf(throughput), options.valueOf(duration));
                }
                LOGGER.info("Starting Thread: " + i);
                jsonConsumer.start();
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to generate load", e);
            System.exit(1);
        }
        // System.exit(0);
    }

}
