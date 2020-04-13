package com.redhat.consulting.demo.amq.streams;


import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * NOTE: We will to use the kafka serializer and not java.io.
 */
public class GsonSerializer<T> implements Serializer<T> {


	private final Gson gson = new GsonBuilder().create();

	@Override
	public byte[] serialize(String str, T object) {
		// TODO Auto-generated method stub
		return gson.toJson(object).getBytes();
	}
	
	
	

}
