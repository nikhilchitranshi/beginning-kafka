package com.learning.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {
	
	public static void main(String[] args) {
		
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//Create record to send
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("my_topic", "This is so cool");
		
		producer.send(record);
		
		producer.flush();
		producer.close();
		
		
	}

}
