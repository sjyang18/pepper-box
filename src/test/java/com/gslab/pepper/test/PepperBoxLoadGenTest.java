package com.gslab.pepper.test;

import com.gslab.pepper.PepperBoxLoadGenerator;
import com.gslab.pepper.util.PropsKeys;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by satish on 5/3/17.
 */
public class PepperBoxLoadGenTest {
    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC = "test";
    private static final int NUM_PRODUCERS = 3;
    public static final int POLL_DURATION = 30000;

    private static Logger LOGGER = Logger.getLogger(PepperBoxLoadGenTest.class.getName());

    private EmbeddedZookeeper zkServer = null;

    private KafkaServer kafkaServer = null;

    private ZkClient zkClient = null;

    @Before
    public void setup() throws IOException {

        zkServer = new EmbeddedZookeeper();

        String zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, POLL_DURATION, POLL_DURATION, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        //AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        JMeterContext jmcx = JMeterContextService.getContext();
        jmcx.setVariables(new JMeterVariables());

    }
    @Test
    public void consoleLoadGenTest() throws IOException {
        File schemaFile = createTempSchemaFile();
        File producerFile = createTempProducerPropertiesConfigFile();

        String vargs []  = new String[]{"--schema-file", schemaFile.getAbsolutePath(),
                "--producer-config-file", producerFile.getAbsolutePath(),
                "--throughput-per-producer", "1000",
                "--test-duration", "3",
                "--num-producers", "2",
                "--per-thread-topics", "NO"
        };
        PepperBoxLoadGenerator.main(vargs); // This starts the Generator in another thread.
        // The following code runs in parallel and needs to wait for long enough for the
        // generator to start and actually produce some messages. So, we need to wait for
        // 10's of seconds (30 seems adequate, at the time of writing).

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group");
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));
        ConsumerRecords<String, String> records = consumer.poll(POLL_DURATION);
        Assert.assertTrue("PepperBoxLoadGenerator validation failed", records.count() > 0);
        LOGGER.info("Retrieved [" + records.count() + "] records in [" + POLL_DURATION + "] mS.");
    }
    @Test
    public void consoleLoadGenMultiTopicTest() throws IOException {
        File schemaFile = createTempSchemaFile();
        File producerFile = createTempProducerPropertiesConfigFile();

        String vargs []  = new String[]{"--schema-file", schemaFile.getAbsolutePath(),
                "--producer-config-file", producerFile.getAbsolutePath(),
                "--throughput-per-producer", "100",
                "--test-duration", "8",
                "--num-producers", Integer.toString(NUM_PRODUCERS),
                "--per-thread-topics", "YES"
        };
        PepperBoxLoadGenerator.main(vargs); // This starts the Generator in another thread.
        // The following code runs in parallel and needs to wait for long enough for the
        // generator to start and actually produce some messages. So, we need to wait for
        // 10's of seconds (30 seems adequate, at the time of writing).

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group");
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        for (int i = 0; i < NUM_PRODUCERS; i++) {
            String topic = TOPIC + "." + Integer.toString(i);
            consumer.subscribe(Arrays.asList(topic));
            ConsumerRecords<String, String> records = consumer.poll(POLL_DURATION);
            Assert.assertTrue("PepperBoxLoadGenerator validation failed", records.count() > 0);
            LOGGER.info("Retrieved [" + records.count() + "] records in [" + POLL_DURATION + "] mS from topic:" + topic);
            consumer.unsubscribe();
        }
    }
    private File createTempProducerPropertiesConfigFile() throws IOException {
        File producerFile = File.createTempFile("producer", ".properties");
        producerFile.deleteOnExit();
        FileWriter producerPropsWriter = new FileWriter(producerFile);
        producerPropsWriter.write(String.format(TestInputUtils.producerProps, BROKERHOST, BROKERPORT, ZKHOST, zkServer.port()));
        producerPropsWriter.close();
        return producerFile;
    }

    private File createTempSchemaFile() throws IOException {
        File schemaFile = File.createTempFile("json", ".schema");
        schemaFile.deleteOnExit();
        FileWriter schemaWriter = new FileWriter(schemaFile);
        schemaWriter.write(TestInputUtils.testSchema);
        schemaWriter.close();
        return schemaFile;
    }

    @After
    public void teardown(){
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();

    }

}
