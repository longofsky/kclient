package com.adachina.mqKafka.core;

import com.adachina.mqKafka.callback.ProducerCallBack;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @Description AdaKafkaProducer集合 允许单个应用内有多个AdaKafkaProducer对象，采用spring方式加载，确保单例
 * @Author litianlong
 * @Version 1.0
 * @Param
 * @Return
 * @Exception
 * @Date 2019-03-18 18:35
 */
public class AdaKafkaProducerMap {
	protected static Logger log = LoggerFactory.getLogger(AdaKafkaProducerMap.class);

	private Map<String,AdaKafkaProducer> adaKafkaProducerMap;



	public AdaKafkaProducerMap(Map<String, AdaKafkaProducer> adaKafkaProducerMap) {
		this.adaKafkaProducerMap = adaKafkaProducerMap;
	}


	public Map<String, AdaKafkaProducer> getKafkaProducermap() {
		return adaKafkaProducerMap;
	}

	public void setKafkaProducermap(Map<String, AdaKafkaProducer> adaKafkaProducerMap) {
		this.adaKafkaProducerMap = adaKafkaProducerMap;
	}
}
