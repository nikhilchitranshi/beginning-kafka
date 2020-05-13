package com.learning.kafka;



import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


public class TwitterProducer {
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
	private final String consumerKey = "OnykKr6W9gmtCB9l9HYHXbrJJ"; 
	private final String consumerSecret = "RAFTyzuuURc48wSkMV2DGO4suopsrgSiftUp4KD4bZlWcb62dG";
	private final String token = "1254465844395995137-cxtXHJENLGMnliUusm0d6PgBobOoIv";
	private final String secret = "J2PwmMliKBWmuGuTHN3WWBBRiVXkmC29gmyQOcEyltLtq";
	
	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run() {
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		Client client = fetchTwitterClient(msgQueue);
		
		client.connect();
		
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		Runtime.getRuntime().addShutdownHook(new Thread(	() -> {
				System.out.println("Stopping Application...");
				System.out.println("Stopping client....");
				client.stop();
				System.out.println("Closing Producer...");
				producer.close();
				System.out.println("Done......");
		}));
		
		while(!client.isDone()) {
			String message = null;
			try {
				message = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			
			if(null != message) {
				System.out.println(message);
				producer.send(new ProducerRecord<String, String>("twitter_feed", null, message), new Callback() {
					
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(null != exception) {
							System.out.println("Exception while sending data "+exception);
						}						
					}
				});
			}
		}		
		logger.info("End Of Application");
	}
	
	public Client fetchTwitterClient(BlockingQueue<String> msgQueue) {
		
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		hosebirdEndpoint.trackTerms(Lists.newArrayList("corona"));
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
							  .hosts(Constants.STREAM_HOST)
							  .authentication(hosebirdAuth)
							  .endpoint(hosebirdEndpoint)
							  .processor(new StringDelimitedProcessor(msgQueue));
		
		return builder.build();		
	}
	
	
	private KafkaProducer<String, String> createKafkaProducer() {
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());		
		
		return new KafkaProducer<String, String>(props);
	}
}
