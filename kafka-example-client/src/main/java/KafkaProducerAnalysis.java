import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author sun. on 1/2/2020.
 */
public class KafkaProducerAnalysis {

	private static final String brokerList = "10.0.0.224:9092";
	private static final String topic = "topic-demo";

	private static Properties initConfig() {
		Properties properties = new Properties();
		// 必填 Kafka集群地址，不需要要有的broker地址，生产者会从给定的broker查找其他的broker的信息
		// 建议最少两个，当一个宕机时，生产者仍然可以连接到Kafka
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		// broker 接收的消息必须以字节数组存在，序列化器
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// KafkaProducer 对应的客户端id，默认为 "" ，如果不配置会自动生成一个非空字符串，producer-与数字的拼接
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
		// KafkaProducer中一般会发生两种异常：可重试的异常和不可重试的异常。
		// 可重试异常可通过重试解决，retries 重试次数
		properties.put(ProducerConfig.RETRIES_CONFIG, 10);

		return properties;
	}

	public static void main(String[] args) {
		Properties properties = initConfig();

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "hello,kafka!");

		// Kafka有三种发布模式， fire-and-forget, sync, async

		// fire-and-forget 发后即忘：只管发送消息，不关心消息是否正确到达。性能最高。
		try {
			producer.send(producerRecord);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// sync 同步，get()方法来阻塞等待Kafka的响应
		try {
			// RecortMetadata 包含了消息的一个元数据信息，比如主题、分区号、分区中的偏移量、时间戳等
			// 如果不需要，可以直接使用 producer.send(record).get();
			RecordMetadata recordMetadata = producer.send(producerRecord).get(2000, TimeUnit.SECONDS);
			System.out
					.println(recordMetadata.topic() + "-" + recordMetadata.partition() + ":" + recordMetadata.offset());
		} catch (ExecutionException | InterruptedException | TimeoutException e) {
			e.printStackTrace();
		}
		// async Future对象容易引起代码混乱，使用Callback的方式非常简洁明了
		// Kafka有响应时就会有回调，要么发送成功，要么抛出异常。
		producer.send(producerRecord, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// metadata 和 exception 互斥，发送成功时，exception为空，不成功metadata为空。
				if (exception != null) {
					exception.printStackTrace();
				} else {
					System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
				}
			}
		});

		// close()方法会阻塞等待之前所有的发送请求完成后再关闭KafkaProducer
		producer.close();
	}
}
