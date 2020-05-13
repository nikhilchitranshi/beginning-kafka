package com.learning.kafka.elasticsearch;

import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchConsumer {
	
	public static RestHighLevelClient fetchElasticSearchClient() {
		
		String hostname = "kafka-course-poc-2216051411.ap-southeast-2.bonsaisearch.net";
		String username = "20dmjazmhw";
		String password = "2ody1pzhzn";
		
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY	, new UsernamePasswordCredentials(username,password));
		RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(hostname, 443))
											.setHttpClientConfigCallback(new HttpClientConfigCallback() {
												
												@Override
												public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBilder) {
													// TODO Auto-generated method stub
													return httpClientBilder.setDefaultCredentialsProvider(credentialsProvider);
												}
											});
		RestHighLevelClient client = new RestHighLevelClient(clientBuilder);
		return client;
	}
	
	public static Consumer<String, String> fetchKafkaConsumer(){
		
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "elastic_search_group");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");		
		
		KafkaConsumer<String , String> consumer = new KafkaConsumer<String, String>(properties);
		
		consumer.subscribe(Collections.singleton("twitter_feed"));
		return consumer;
	}

}
