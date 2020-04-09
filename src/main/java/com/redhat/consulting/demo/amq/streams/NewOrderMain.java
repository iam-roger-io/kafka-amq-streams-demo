package com.redhat.consulting.demo.amq.streams;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 
 * @author Rogerio L Santos
 *
 */
public class NewOrderMain {

	public static void main(String args[]) throws InterruptedException, ExecutionException {

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties());

		for (int i = 0; i < 1; i++) {

			// ORder
			String key = UUID.randomUUID().toString();
			String value = "key + 123,45 ";
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", key, value);

			// Send EMAIL
			String email = "Thank you for your order! We are processing your order!";
			ProducerRecord<String, String> emailRecord = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL", key,
					email);

			// Send to Kafka
			producer.send(record, callback()).get();
			producer.send(emailRecord, callback()).get();			
			
		}
		

	}

	private static Callback callback() {
		return (data, ex) -> {
			;

			if (ex != null) {

				ex.printStackTrace();
				return;

			}
			System.out.println(
					"sucesso " + data.topic() + " ### partition " + data.partition() + " ### OFFSET " + data.offset());

		};
	}

	public static Properties properties() {

		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.15.191:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return properties;
	}

}
