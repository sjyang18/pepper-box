package com.gslab.pepper;

import com.google.common.util.concurrent.RateLimiter;
import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.util.ConsumerKeys;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.utils.CommandLineUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import com.gslab.pepper.exception.PepperBoxException;
import org.xbill.DNS.DNSSEC;

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
    private long durationInMillis;
    private RateLimiter limiter;
    private static String PEPPERBOX_GROUP_NAME = "pepperbox_loadgenerator";

    private static Logger LOGGER = Logger.getLogger(PepperBoxLoadConsumer.class.getName());

    PepperBoxLoadConsumer(Integer threadId, String consumerConfig, Integer throughput, Integer duration) throws PepperBoxException {
        Thread t = currentThread();
        t.setName(threadId.toString());

        Properties inputProps = populateConsumerProperties(consumerConfig);

        String perThreadTopic = inputProps.getProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG) + "." + threadId.toString();
        inputProps.setProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG, perThreadTopic);
        LOGGER.log(Level.INFO, "Thread [" + threadId.toString() + "] using topic [" +
                inputProps.getProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG) + "]");

        createConsumer(inputProps, throughput, duration);
    }

    PepperBoxLoadConsumer(String consumerConfig, Integer throughput, Integer duration) throws PepperBoxException {
        Properties kafkaProperties = populateConsumerProperties(consumerConfig);
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

        // Add code to create the Kafka Consumer

        throw new PepperBoxException("Unfinished");
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
                .withRequiredArg()
                .describedAs("create a topic per thread")
                .defaultsTo("NO")
                .ofType(String.class);

        if (args.length == 0) {
            CommandLineUtils.printUsageAndDie(parser, "Kafka console load consumer.");
            System.exit(2);
        }
        OptionSet options = parser.parse(args);
        checkRequiredArgs(parser, options, consumerConfig, throughput, duration, threadCount);
        try {
            int totalThreads = options.valueOf(threadCount);
            for (int i = 0; i < totalThreads; i++) {
                PepperBoxLoadConsumer jsonConsumer;
                if (options.valueOf(aTopicPerThread).equalsIgnoreCase("YES")) {
                    jsonConsumer = new PepperBoxLoadConsumer(i, options.valueOf(consumerConfig), options.valueOf(throughput), options.valueOf(duration));
                } else {
                    jsonConsumer = new PepperBoxLoadConsumer(options.valueOf(consumerConfig), options.valueOf(throughput), options.valueOf(duration));
                }
                jsonConsumer.start();
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to generate load", e);
            System.exit(1);
        }
        System.exit(0);
    }

}
