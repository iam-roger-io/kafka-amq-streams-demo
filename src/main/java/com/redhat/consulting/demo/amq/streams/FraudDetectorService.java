package com.redhat.consulting.demo.amq.streams;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

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
public class FraudDetectorService {

	public static void main(String args[]) {

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties());

		consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

		while (true) {
			
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			if (!records.isEmpty()) {

				System.out.println("Encontrei " + records.count() + " registros");

				for (ConsumerRecord record : records) {

					System.out.println("========================================================================");
					System.out.println("Processing nwew order, checking for fraude ");
					System.out.println("key" + record.key());
					System.out.println("Value: " + record.value());
					System.out.println("Partition: " + record.partition());
					System.out.println("Record: " + record.offset());

					try {
						Thread.sleep(1);
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
		
		//Give a friendly name to client
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService.class.getSimpleName() + "-" + UUID.randomUUID().toString());
		
		/*
		 * Quantidade maxima de recors recebidos simultaneamente. 
		 * Neste exemplo foi diminuido para garantir o commit da mensagem recebida mais rapido.
		 */
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        
		return properties;
	}

}
