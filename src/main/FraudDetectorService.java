

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FraudDetectorService {

	public static void main(String args[]) {

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties());

		consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			System.out.println("FraudDetectorService.class.getSimpleName():" + FraudDetectorService.class.getSimpleName());
			
			if (!records.isEmpty()) {

				System.out.println("Encontrei " + records.count() + "registros");

				for (ConsumerRecord record : records) {

					System.out.print("========================================================================");
					System.out.print("Processing nwew order, checking for fraude ");
					System.out.println("key" + record.key());
					System.out.println("Value: " + record.value());
					System.out.println("Partition" + record.partition());
					System.out.println("Record" + record.offset());

					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch blockvsco
						e.printStackTrace();
					}

				}
			}
		}

	}

	public static Properties properties() {

		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.15.191:9092");
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		properties.put(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());

		return properties;
	}

}