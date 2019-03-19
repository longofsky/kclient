package com.adachina.mqkafka;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.adachina.mqKafka.core.AdaKafkaConsumer;
import com.adachina.mqKafka.messageExecuteHandle.AbstractMessageExecuteHandle;
import com.adachina.mqKafka.messageExecuteHandle.AsyncMessageExecuteHandle;
import com.adachina.mqKafka.messageExecuteHandle.SyncMessageExecuteHandle;
import com.adachina.mqkafka.handler.DogHandler;
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
 * @Date 2019-03-19 13:30
 */
@SpringBootApplication
public class ConsumerApplication {
	protected static Logger log = LoggerFactory.getLogger(ConsumerApplication.class);


	@Bean
	public DogHandler getDogHandler() {
		return new DogHandler();
	}

	@Bean
	public AbstractMessageExecuteHandle getAbstractMessageExecuteHandle(DogHandler bean) {

		/**
		 *同步线性消费
		 */
		return new SyncMessageExecuteHandle("test",bean);
		/**
		 *异步多线程消费
		 */
//		return new AsyncMessageExecuteHandle("test",bean,5);
	}

	@Bean
	public AdaKafkaConsumer getAdaKafkaConsumer(AbstractMessageExecuteHandle bean) {

		return new AdaKafkaConsumer("kafka-consumer.properties",bean);
	}

	public static void main(String[] args) {
		ApplicationContext ctxBackend = SpringApplication.run(ConsumerApplication.class, args);

		String startupTime = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z").format(new Date(ctxBackend.getStartupDate()));
		log.info("KClient application starts at: " + startupTime);

		try {
			ctxBackend.getBean(AdaKafkaConsumer.class).startup();

		} finally {
			System.out.println("Start to exit...");
			ctxBackend.getBean(AdaKafkaConsumer.class).shutdownGracefully();
		}
	}
}
