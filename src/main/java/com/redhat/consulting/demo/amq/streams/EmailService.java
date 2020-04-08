package com.redhat.consulting.demo.amq.streams;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 
 * @author Rogerio L Santos
 *
 */
public class EmailService {

	public static void main(String args[]) {

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties());

		consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			
			if (!records.isEmpty()) {

				System.out.println("Encontrei " + records.count() + " registros");

				for (ConsumerRecord record : records) {

					System.out.println("========================================================================");
					System.out.println("Sending email, checking for fraude ");
					System.out.println("key" + record.key());
					System.out.println("Value: " + record.value());
					System.out.println("Partition: " + record.partition());
					System.out.println("Record: " + record.offset());

					try {
						Thread.sleep(1000);
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

		properties.put(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());

		return properties;
	}

}
