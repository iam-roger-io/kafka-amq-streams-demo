package com.redhat.consulting.demo.amq.streams;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.redhat.consulting.demo.amq.streams.model.Order;

public class NewOrderMain {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
				
		KafkaDispatcher<String> emailrDispatcher = new KafkaDispatcher<String>();
		KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();
			
		for (int i = 0; i < 10; i++) {

			String userId = UUID.randomUUID().toString();
			String orderId = UUID.randomUUID().toString();
			BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
			Order order = new Order(userId, orderId, amount);
						
			orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

			String email = "Thank you for your order! We are processing your order!";						
			emailrDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
		
		}
				
	}

}
