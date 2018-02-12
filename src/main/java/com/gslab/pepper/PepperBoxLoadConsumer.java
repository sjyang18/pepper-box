package com.gslab.pepper;

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
    private static Logger LOGGER = Logger.getLogger(PepperBoxLoadConsumer.class.getName());

    PepperBoxLoadConsumer(Integer threadId, String consumerConfig, Integer throughput, Integer duration) throws PepperBoxException {
        Thread t = currentThread();
        t.setName(threadId.toString());

        Properties inputProps = populateConsumerProperties(consumerConfig);

        String perThreadTopic = inputProps.getProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG) + "." + threadId.toString();
        inputProps.setProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG, perThreadTopic);
        LOGGER.log(Level.INFO, "Thread [" + threadId.toString() + "] using topic [" +
                inputProps.getProperty(ConsumerKeys.KAFKA_TOPIC_CONFIG) + "]");

        createConsumer(consumerConfig, throughput, duration);
    }

    PepperBoxLoadConsumer(String consumerConfig, Integer throughput, Integer duration) throws PepperBoxException {
        createConsumer(consumerConfig, throughput, duration);
    }

    Properties populateConsumerProperties(String consumerProps) throws PepperBoxException {
        Properties inputProps = new Properties();
        try {
            inputProps.load(new FileInputStream(consumerProps));
        } catch (IOException e) {
            throw new PepperBoxException(e);
        }
        return inputProps;
    }

    private void createConsumer(String consumerConfig, Integer throughput, Integer duration) throws PepperBoxException {

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
        ArgumentAcceptingOptionSpec<String> consumerConfig = parser.accepts("consumer-config-file", "REQUIRED: Kafka producer properties file absolute path.")
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
        ArgumentAcceptingOptionSpec<Integer> threadCount = parser.accepts("num-consumers", "REQUIRED: Number of producer threads.")
                .withRequiredArg()
                .describedAs("consumers")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<String> aTopicPerThread = parser.accepts("per-thread-topics", "REQUIRED: Create a separate topic per producer")
                .withRequiredArg()
                .describedAs("create a topic per thread")
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
