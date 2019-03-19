package com.adachina.mqKafka.sample.spring;

import java.io.IOException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.adachina.mqKafka.core.AdaKafkaConsumer;

/**
 * Sample for use {@link AdaKafkaConsumer} with Spring context.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaConsumerSpringSample {

	public static void main(String[] args) {
		ApplicationContext ac = new ClassPathXmlApplicationContext(
				"kafka-consumer.xml");

		AdaKafkaConsumer adaKafkaConsumer = (AdaKafkaConsumer) ac.getBean("consumer");
		try {
			adaKafkaConsumer.startup();

			try {
				System.in.read();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			adaKafkaConsumer.shutdownGracefully();
		}
	}
}
