import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 *
 * @author sun. on 1/1/2020.
 */
public class ProducerFastStart {

	private static final String brokerList = "10.0.0.224:9092";
	private static final String topic = "topic-demo";

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("bootstrap.servers", brokerList);

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "hello,kafka!");
		try {
			producer.send(producerRecord);
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.close();
	}
}
