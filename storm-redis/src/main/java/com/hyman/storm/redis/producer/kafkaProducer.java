package com.hyman.storm.redis.producer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.hyman.storm.redis.util.ApplicationContext;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class kafkaProducer extends Thread{

	private String topic;
	public kafkaProducer(String topic){
		super();
		this.topic = topic;
	}
	
	@Override
	public void run() {
		Producer<String, String> producer = createProducer();
		int i=0;
		while(true){
			producer.send(new KeyedMessage<String, String>(topic, "message: " + i));
			System.out.println("hello: " + i);
			try {
				TimeUnit.SECONDS.sleep(1);
				i++;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Producer<String, String> createProducer() {
		
		Properties props = new Properties();
		props.put("zookeeper.connect", ApplicationContext.getConfig("kafka.zookeeper.connect"));
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("producer.type", "async");
		props.put("compression.codec", "1");
		props.put("metadata.broker.list", ApplicationContext.getConfig("kafka.metadata.broker.list"));
		
		return new Producer<String, String>(new ProducerConfig(props));
	 }
	
	public static void main(String[] args) {
		new kafkaProducer(ApplicationContext.getConfig("kafka.topic")).start();
		
	}
	 
}
