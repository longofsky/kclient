package com.adachina.mqkafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.adachina.mqKafka.core.AdaKafkaProducer;
import com.adachina.mqKafka.core.AdaKafkaProducerMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * @Description
 * @Author litianlong
 * @Version 1.0
 * @Param
 * @Return
 * @Exception
 * @Date 2019-03-19 13:45
 */
@SpringBootApplication
public class ProducerApplication {
	protected static Logger log = LoggerFactory.getLogger(ProducerApplication.class);

//	@Bean
//	public AdaKafkaProducer getAdaKafkaProducer() {
//
//		return new AdaKafkaProducer("kafka-producer.properties", "test");
//	}

	@Bean
	public AdaKafkaProducerMap getAdaKafkaProducerMap() {

		Map<String, AdaKafkaProducer> adaKafkaProducerMap = new HashMap<>(16);

		adaKafkaProducerMap.put("adaKafkaProducer_clust1",new AdaKafkaProducer("kafka-producer.properties", "test")) ;

		return new AdaKafkaProducerMap(adaKafkaProducerMap);
	}

	public static void main(String[] args) {
		ApplicationContext ctxBackend = SpringApplication.run(ProducerApplication.class, args);

		String startupTime = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z").format(new Date(ctxBackend.getStartupDate()));
		log.info("KClient application starts at: " + startupTime);

	}
}
