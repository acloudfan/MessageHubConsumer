package com.acloudfan.mhub;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Main class for the Message Hub Consumer
 */

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
	static private KafkaConsumer<byte[], byte[]> kafkaConsumer;
	static private String topic="";
	static private String groupId="";
	
	// Main method
	public static void main(String[] args)
	throws Exception {
		// Suppress warnings - ignore
		Util.suppressSLF4JWarnings();
		// This sets the jaas.conf as a system property
				Util.setJaasProperty();
		
		// Check if the topic is provided
		if(args.length < 1){
			System.out.println("Usage:  > java -jar consumer.jar  topic [groupid]");
			System.exit(1);
		}
		
		// Set the topic and group Id
		topic = args[0];
		if(args.length > 1) groupId=args[1];
		
		// Initialize the consumer
		init();
		
		// Subscribe to the topic
		subscribe();
		
		// Poll the topic for messages
		poll();
		
		// unsubscribe and close consumer
		shutdown();
	}
	
	/**
	 * 1. Initialize the KafkaConsumer
	 */
	private static void init(){
		// If you want to change the consumer props change it in ConsumerConfig Class
		Properties props = ConsumerConfig.getProperties();
		// You can override any property here
		props.setProperty("group.id", groupId);
		
		// Create an instance of the consumer
		kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props);
	}
	
	/**
	 * 2. Subscribe to the topic
	 */
	private static void subscribe(){
		// Consumer can listen to a list of topics
		ArrayList<String> topics = new ArrayList<String>();
		topics.add(topic);
		kafkaConsumer.subscribe(topics);
		System.out.println("Consumer Subscribed: Group="+groupId+" Topic="+topic);
	}
	

	
	/**
	 * 3. Poll the topic for messages
	 * 
	 * Polls till a message="exit" is received
	 */
	private static void poll(){
		
		boolean exitReceieved=false;
		
		// Infinite loop 
		while(!exitReceieved){
			// Wait and batch all messages for 1 second = 1000 ms
			ConsumerRecords< byte[], byte[]> recs = kafkaConsumer.poll(1000);
			
			// Messages returned in a batch (not a single message like traditional MQ)
			Iterator<ConsumerRecord<byte[],byte[]>> it = recs.iterator();
			
			// Loop and print the message value
			while(it.hasNext()){
				ConsumerRecord<byte[],byte[]> cr = it.next();
				String key = "";
				if(cr.key() != null) key = new String(cr.key());
				String value = new String(cr.value());
				long offset = cr.offset();
				String topic_on_msg = cr.topic();
								
				String output = "{key="+key+", offset="+offset+", value="+value+", topic="+topic_on_msg+" ,partition="+cr.partition()+"}";
				System.out.println(output);
				
				if(value.equals("exit")) exitReceieved=true;
			}
			
			// Synchronously commit the last offset so that we do not read the meesage again
			kafkaConsumer.commitSync();
		}
	}
	
	/**
	 * 4. Unsubscribe & close
	 */
	private static void shutdown(){
		kafkaConsumer.unsubscribe();
		kafkaConsumer.close();
		System.out.println("Consumer Unsubscribed & Closed");
	}
	

}
