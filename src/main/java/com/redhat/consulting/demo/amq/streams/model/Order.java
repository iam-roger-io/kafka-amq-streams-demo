package com.redhat.consulting.demo.amq.streams.model;

import java.math.BigDecimal;

public class Order {

	private String userId;
	private String orderId;
	private BigDecimal amount;
	
	public Order(String userId, String orderId, BigDecimal amount) {
		super();
		this.userId = userId;
		this.orderId = orderId;
		this.amount = amount;
	}
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	public BigDecimal getAmount() {
		return amount;
	}
	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}
	
	

}
