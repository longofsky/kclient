package com.adachina.mqKafka.core;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

import com.adachina.mqKafka.callback.ProducerCallBack;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * @Description
 * @Author litianlong
 * @Version 1.0
 * @Param
 * @Return
 * @Exception
 * @Date 2019-03-18 18:35
 */
public class AdaKafkaProducer {
	protected static Logger log = LoggerFactory.getLogger(AdaKafkaProducer.class);


	private KafkaProducer<String, String> producer;

	private String defaultTopic;

	private String propertiesFile;
	private Properties properties;

	public AdaKafkaProducer() {
		// For Spring context
	}

	public AdaKafkaProducer(String propertiesFile, String defaultTopic) {
		this.propertiesFile = propertiesFile;
		this.defaultTopic = defaultTopic;

		init();
	}

	public AdaKafkaProducer(Properties properties, String defaultTopic) {
		this.properties = properties;
		this.defaultTopic = defaultTopic;

		init();
	}

	protected void init() {
		if (properties == null) {
			properties = new Properties();
			try {
				properties.load(Thread.currentThread().getContextClassLoader()
						.getResourceAsStream(propertiesFile));
			} catch (IOException e) {
				log.error("The properties file is not loaded.", e);
				throw new IllegalArgumentException(
						"The properties file is not loaded.", e);
			}
		}
		log.info("Producer properties:" + properties);

		producer = new KafkaProducer(properties);
	}

	/*
	 发送string message
	 */

	public Future<RecordMetadata> send2Topic(String topic, Integer partition, Long timestamp, String key, String value, Iterable<Header> headers) {

		/**
		 * todo
		 * 内存记录发送消息
		 */

		if (value == null) {
			return null;
		}

		if (topic == null) {
			topic = defaultTopic;
		}

		ProducerRecord record = new ProducerRecord<String,String>(topic,partition,timestamp, key, value,headers);

		return producer.send(record,new ProducerCallBack());

	}

	public Future<RecordMetadata> send(String message) {

		return send(null,null,null, message);
	}
	public Future<RecordMetadata> send(String topic,String message) {

		return send(topic,null,null, message);
	}
	public Future<RecordMetadata> send(Integer partition,String message) {

		return send(null,partition,null, message);
	}

	public Future<RecordMetadata> send(String topic, Integer partition,String message) {

		return send(topic,partition,null, message);
	}
	public Future<RecordMetadata> send(String topic, String key,String message) {

		return send(topic,null,key, message);
	}
	public Future<RecordMetadata> send(String topic, Integer partition, String key,String message) {

		return send(topic,partition,null,key, message);
	}
	public Future<RecordMetadata> send(String topic, Integer partition, Long timestamp, String key,String message) {

		return send2Topic(topic,partition,timestamp,key, message,null);
	}

	/*
	  发送bean message
	 */

	public <T> Future<RecordMetadata> sendBean(T bean) {
		return sendBean2Topic(null, bean);
	}

	public <T> Future<RecordMetadata> sendBean2Topic(String topicName, T bean) {
		return sendBean2Topic(topicName,null,null, bean);
	}

	public <T> Future<RecordMetadata> sendBean(String key, T bean) {
		return sendBean2Topic(null,null, key, bean);
	}
	public <T> Future<RecordMetadata> sendBean(Integer partition, T bean) {
		return sendBean2Topic(null,partition, null, bean);
	}

	public <T> Future<RecordMetadata> sendBean2Topic(String topicName, Integer partition, T bean) {
		return sendBean2Topic(topicName,partition, null,bean);
	}

	public <T> Future<RecordMetadata> sendBean2Topic(String topicName, Integer partition, String key, T bean) {
		return send(topicName,partition, key, JSON.toJSONString(bean));
	}


	/*
	  发送JSON Object message
	 */

	public Future<RecordMetadata> sendObject(JSONObject jsonObject) {
		return sendObject2Topic(null, jsonObject);
	}

	public Future<RecordMetadata> sendObject2Topic(String topicName, JSONObject jsonObject) {
		return sendObject2Topic(topicName,null,null, jsonObject);
	}

	public Future<RecordMetadata> sendObject(String key, JSONObject jsonObject) {
		return sendObject2Topic(null,null, key, jsonObject);
	}

	public Future<RecordMetadata> sendObject(Integer partition, JSONObject jsonObject) {
		return sendObject2Topic(null,partition, null, jsonObject);
	}

	public Future<RecordMetadata> sendObject2Topic(String topicName, Integer partition, String key, JSONObject jsonObject) {
		return send(topicName,partition, key,jsonObject.toJSONString());
	}

	public Future<RecordMetadata> sendObjects(JSONArray jsonArray) {
		return sendObjects2Topic(null, jsonArray);
	}

	public Future<RecordMetadata> sendObjects2Topic(String topicName, JSONArray jsonArray) {
		return send(topicName, jsonArray.toJSONString());
	}

	public void close() {
		producer.close();
	}

	public String getDefaultTopic() {
		return defaultTopic;
	}

	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}

	public String getPropertiesFile() {
		return propertiesFile;
	}

	public void setPropertiesFile(String propertiesFile) {
		this.propertiesFile = propertiesFile;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}
