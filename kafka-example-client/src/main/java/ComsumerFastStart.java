import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 *
 * @author sun. on 1/1/2020.
 */
public class ComsumerFastStart {
	private static final String brokerList = "10.0.0.224:9092";
	private static final String topic = "topic-demo";
	private static final String groupId = "group.demo";

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("bootstrap.servers", brokerList);
		properties.put("group.id", groupId);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Collections.singletonList(topic));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.value());
			}
		}
	}
}
