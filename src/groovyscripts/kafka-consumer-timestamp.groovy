import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord
      
String topic = "jmeter-test"
String group = "jmeter-consumer"

Properties props = new Properties()
props.put("bootstrap.servers", "localhost:9092")
props.put("group.id", group)
props.put("enable.auto.commit", "true")
props.put("auto.commit.interval.ms", "1000")
props.put("session.timeout.ms", "10000")
props.put("key.deserializer",          
    "org.apache.kafka.common.serialization.StringDeserializer")
props.put("value.deserializer", 
    "org.apache.kafka.common.serialization.StringDeserializer")
KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)
      
consumer.subscribe(Arrays.asList(topic))
log.info("Subscribed to topic " + topic)

long t = System.currentTimeMillis()
long end = t + 5000
f = new FileOutputStream("results.json", true)
p = new PrintStream(f)
while (System.currentTimeMillis()<end) 
{
   ConsumerRecords<String, String> records = consumer.poll(100)
   for (ConsumerRecord<String, String> record : records)
   {
      p.println( "{\n\"received\":{\n\t\"batchReceivedAt\":" + System.currentTimeMillis() + ",\n\t\"offset\":" + record.offset() +"\n} \n\"generated\":" + record.value() + "\n}")
   }
   consumer.commitSync() 	 
}
consumer.close()
p.close()
f.close()
