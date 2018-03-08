package com.gslab.pepper;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.google.common.util.concurrent.RateLimiter;
import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.input.SchemaProcessor;
import com.gslab.pepper.util.ProducerKeys;
import com.gslab.pepper.util.PropsKeys;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.server.KafkaConfig;
import kafka.utils.CommandLineUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The com.gslab.pepper.PepperBoxLoadGenerator standalone load generator.
 * This class takes arguments including throttle per thread, test duration, no of thread and schema file and kafka
 * producer properties and generates load at throttled rate.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 28/02/2017
 */
public class PepperBoxLoadGenerator extends Thread {

    private static Logger LOGGER = Logger.getLogger(PepperBoxLoadGenerator.class.getName());
    private RateLimiter limiter;
    private Iterator iterator = null;
    private KafkaProducer<String, String> producer;
    private String topic;
    private long durationInMillis;
    private long messageCount = 0;

    /**
     * Start kafka load generator from input properties and schema
     *
     * @param threadIn the logical thread ID, starting with 0
     * @param schemaFile
     * @param producerProps
     * @param throughput
     * @param duration
     * @throws PepperBoxException
     */
    PepperBoxLoadGenerator(Integer threadIn, String schemaFile, String producerProps, Integer throughput, Integer duration) throws PepperBoxException {
        Thread t = currentThread();
        t.setName(threadIn.toString());

        Properties inputProps = populateProducerProps(producerProps);

        String perThreadTopic = inputProps.getProperty(ProducerKeys.KAFKA_TOPIC_CONFIG) + "." + threadIn.toString();
        inputProps.setProperty(ProducerKeys.KAFKA_TOPIC_CONFIG, perThreadTopic);
        LOGGER.log(Level.INFO, "Thread [" + threadIn.toString() + "] using topic [" +
                inputProps.getProperty(ProducerKeys.KAFKA_TOPIC_CONFIG) + "]");

        createProducer(schemaFile, throughput, duration, inputProps);
    }

    /**
     * Start kafka load generator from input properties and schema
     *
     * @param schemaFile
     * @param producerProps
     * @param throughput
     * @param duration
     * @throws PepperBoxException
     */
    PepperBoxLoadGenerator(String schemaFile, String producerProps, Integer throughput, Integer duration) throws PepperBoxException {

        Properties inputProps = populateProducerProps(producerProps);
        createProducer(schemaFile, throughput, duration, inputProps);
    }

    private Properties populateProducerProps(String producerProps) throws PepperBoxException {
        Properties inputProps = new Properties();
        try {
            inputProps.load(new FileInputStream(producerProps));
        } catch (IOException e) {
            throw new PepperBoxException(e);
        }
        return inputProps;
    }

    private void createProducer(String schemaFile, Integer throughput, Integer duration, Properties inputProps) throws PepperBoxException {
        Path path = Paths.get(schemaFile);
        try {
            String inputSchema = new String(Files.readAllBytes(path));
            SchemaProcessor schemaProcessor = new SchemaProcessor();
            iterator = schemaProcessor.getPlainTextMessageIterator(inputSchema);
        } catch (IOException e) {
            throw new PepperBoxException(e);
        }

        limiter = RateLimiter.create(throughput);
        durationInMillis = TimeUnit.SECONDS.toMillis(duration);
        Properties brokerProps = new Properties();
        brokerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, inputProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        brokerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, inputProps.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        brokerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, inputProps.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        brokerProps.put(ProducerConfig.ACKS_CONFIG, inputProps.getProperty(ProducerConfig.ACKS_CONFIG));
        brokerProps.put(ProducerConfig.SEND_BUFFER_CONFIG, inputProps.getProperty(ProducerConfig.SEND_BUFFER_CONFIG));
        brokerProps.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, inputProps.getProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG));
        brokerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, inputProps.getProperty(ProducerConfig.BATCH_SIZE_CONFIG));
        brokerProps.put(ProducerConfig.LINGER_MS_CONFIG, inputProps.getProperty(ProducerConfig.LINGER_MS_CONFIG));
        brokerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, inputProps.getProperty(ProducerConfig.BUFFER_MEMORY_CONFIG));
        brokerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, inputProps.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        brokerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        LOGGER.info(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " set to: " + 1);
        brokerProps.put(ProducerConfig.RETRIES_CONFIG, 5);

        final String security_protocol = inputProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        System.out.println("security_protocol set to[" + security_protocol + "], comparing to [" + SecurityProtocol.SASL_SSL.name +"].");
        if (security_protocol.equals(SecurityProtocol.SASL_SSL.name)) {
            LOGGER.info("Adding SASL_SSL parameters for Kafka to use.");
            brokerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, security_protocol);
            String sasl_jaas_config = "org.apache.kafka.common.security.plain.PlainLoginModule required" +
                    " username=\"" + inputProps.getProperty("sasl.jaas.username") +
                    "\" password=\"" + inputProps.getProperty("sasl.jaas.password") + "\";";
            System.out.println("sasl.jaas.config: " + sasl_jaas_config);
            brokerProps.put(ProducerKeys.SASL_JAAS_CONFIG, sasl_jaas_config);
            brokerProps.put(ProducerKeys.SASL_MECHANISM, inputProps.getProperty(ProducerKeys.SASL_MECHANISM));

            brokerProps.put(ProducerKeys.SSL_ENABLED_PROTOCOLS, inputProps.getProperty(ProducerKeys.SSL_ENABLED_PROTOCOLS));
            brokerProps.put(ProducerKeys.SSL_TRUSTSTORE_LOCATION, inputProps.getProperty(ProducerKeys.SSL_TRUSTSTORE_LOCATION));
            brokerProps.put(ProducerKeys.SSL_TRUSTSTORE_PASSWORD, inputProps.getProperty(ProducerKeys.SSL_TRUSTSTORE_PASSWORD));
            brokerProps.put(ProducerKeys.SSL_TRUSTSTORE_TYPE, inputProps.getProperty(ProducerKeys.SSL_TRUSTSTORE_TYPE));
        }


        String kerbsEnabled = inputProps.getProperty(ProducerKeys.KERBEROS_ENABLED);

        if (kerbsEnabled != null && kerbsEnabled.equals(ProducerKeys.FLAG_YES)) {

            System.setProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, inputProps.getProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG));
            System.setProperty(ProducerKeys.JAVA_SEC_KRB5_CONFIG, inputProps.getProperty(ProducerKeys.JAVA_SEC_KRB5_CONFIG));
            brokerProps.put(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, inputProps.getProperty(ProducerKeys.SASL_KERBEROS_SERVICE_NAME));
            brokerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, inputProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        }

        Set<String> parameters = inputProps.stringPropertyNames();
        parameters.forEach(parameter -> {
            if (parameter.startsWith("_")) {
                brokerProps.put(parameter.substring(1), inputProps.getProperty(parameter));
            }
        });

        topic = inputProps.getProperty(ProducerKeys.KAFKA_TOPIC_CONFIG);
        producer = new KafkaProducer<>(brokerProps);
    }

    /**
     * Retrieve brokers from zookeeper servers
     *
     * @param properties
     * @return
     */
    private String getBrokerServers(Properties properties) {

        StringBuilder kafkaBrokers = new StringBuilder();

        String zookeeperServers = properties.getProperty(ProducerKeys.ZOOKEEPER_SERVERS);

        if (zookeeperServers != null && !zookeeperServers.equalsIgnoreCase(ProducerKeys.ZOOKEEPER_SERVERS_DEFAULT)) {

            try {

                ZooKeeper zk = new ZooKeeper(zookeeperServers, 10000, null);
                List<String> ids = zk.getChildren(PropsKeys.BROKER_IDS_ZK_PATH, false);

                for (String id : ids) {

                    String brokerInfo = new String(zk.getData(PropsKeys.BROKER_IDS_ZK_PATH + "/" + id, false, null));
                    JsonObject jsonObject = Json.parse(brokerInfo).asObject();

                    String brokerHost = jsonObject.getString(PropsKeys.HOST, "");
                    int brokerPort = jsonObject.getInt(PropsKeys.PORT, -1);

                    if (!brokerHost.isEmpty() && brokerPort != -1) {

                        kafkaBrokers.append(brokerHost);
                        kafkaBrokers.append(":");
                        kafkaBrokers.append(brokerPort);
                        kafkaBrokers.append(",");

                    }

                }
            } catch (IOException | KeeperException | InterruptedException e) {

                LOGGER.log(Level.SEVERE, "Failed to get broker information", e);

            }

        }

        if (kafkaBrokers.length() > 0) {

            kafkaBrokers.setLength(kafkaBrokers.length() - 1);

            return kafkaBrokers.toString();

        } else {

            return properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        }
    }

    @Override
    public void run() {
    
        long endTime = durationInMillis + System.currentTimeMillis();
        while (endTime > System.currentTimeMillis()) {
            sendMessage();
        }
        System.out.println("{status:testCompleted}, {thread:" + Thread.currentThread().getId() + "}, {topic:" + topic + "}, {messages:" + messageCount + "}");
        producer.close();
    }

    public void sendMessage() {
        limiter.acquire();
        ProducerRecord<String, String> keyedMsg = new ProducerRecord<>(topic, iterator.next().toString());
        if (++messageCount % 100 == 1) {
            LOGGER.info("{status:interim}, {topic:" +topic + "}, {messages:" + messageCount + "}");
        }
        producer.send(keyedMsg);
    }

    public static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec... required) {
        for (OptionSpec optionSpec : required) {
            if (!options.has(optionSpec)) {
                CommandLineUtils.printUsageAndDie(parser, "Missing required argument \"" + optionSpec + "\"");
            }
        }
    }

    public static void main(String[] args) {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> schemaFile = parser.accepts("schema-file", "REQUIRED: Input schema file absolute path.")
                .withRequiredArg()
                .describedAs("schema file")
                .ofType(String.class);
        ArgumentAcceptingOptionSpec<String> producerConfig = parser.accepts("producer-config-file", "REQUIRED: Kafka producer properties file absolute path.")
                .withRequiredArg()
                .describedAs("producer properties")
                .ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> throughput = parser.accepts("throughput-per-producer", "REQUIRED: Throttle rate per thread.")
                .withRequiredArg()
                .describedAs("throughput")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<Integer> duration = parser.accepts("test-duration", "REQUIRED: Test duration in seconds.")
                .withRequiredArg()
                .describedAs("test duration")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<Integer> threadCount = parser.accepts("num-producers", "REQUIRED: Number of producer threads.")
                .withRequiredArg()
                .describedAs("producers")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<String> aTopicPerThread = parser.accepts("per-thread-topics", "OPTIONAL: Create a separate topic per producer")
                .withRequiredArg()
                .describedAs("create a topic per thread")
                .ofType(String.class)
                .defaultsTo("NO");
        ArgumentAcceptingOptionSpec<Integer> startingOffset = parser.accepts("starting-offset", "OPTIONAL: Starting count for separate topics, default 0")
                .withOptionalArg().ofType(Integer.class).defaultsTo(Integer.valueOf(0), new Integer[0])
                .describedAs("starting offset for the topic per thread")
                ;

        if (args.length == 0) {
            CommandLineUtils.printUsageAndDie(parser, "Kafka console load generator.");
        }
        OptionSet options = parser.parse(args);
        checkRequiredArgs(parser, options, schemaFile, producerConfig, throughput, duration, threadCount);
        LOGGER.info("starting-offset: " + options.valueOf(startingOffset));
        try {
            int totalThreads = options.valueOf(threadCount);
            for (int i = 0; i < totalThreads; i++) {
                PepperBoxLoadGenerator jsonProducer;
                if (options.valueOf(aTopicPerThread).equalsIgnoreCase("YES")) {
                    int topicId = i + options.valueOf(startingOffset);
                    jsonProducer = new PepperBoxLoadGenerator(topicId, options.valueOf(schemaFile), options.valueOf(producerConfig), options.valueOf(throughput), options.valueOf(duration));
                } else {
                    jsonProducer = new PepperBoxLoadGenerator(options.valueOf(schemaFile), options.valueOf(producerConfig), options.valueOf(throughput), options.valueOf(duration));
                }
                jsonProducer.start();
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to generate load", e);
        }
    }

}
