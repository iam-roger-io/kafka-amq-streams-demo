package com.redhat.consulting.demo.amq.streams;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER",  "Olá mundo");
			
			//Assincrono
			//producer.send(record);
									
			//Espere
			producer.send(record, (data, ex) -> {;
			
				if (ex != null) {
					
					ex.printStackTrace();
					return;
					
				}				
				System.out.println("sucesso " + data.topic() + " ### partition " + data.partition() + " ### OFFSET " + data.offset());
			
			
			}).get();
			
		}

	
	public static Properties properties() {		
		
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.15.191:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		return properties;
	}

}
