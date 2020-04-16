package com.redhat.consulting.demo.amq.streams;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


public class NewOrderMain {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
				
		KafkaDispatcher<String> emailrDispatcher = new KafkaDispatcher<String>();
		KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();
		String email = Math.random() + "@redhat.com"; 
		for (int i = 0; i < 10; i++) {

			String userId = UUID.randomUUID().toString();
			String orderId = UUID.randomUUID().toString();
			BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
			
			Order order = new Order(orderId, amount, email);
						
			orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

			String emailCode = "Thank you for your order! We are processing your order!";						
			emailrDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
		
		}
				
	}

}
