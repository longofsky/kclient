package com.adachina.mqKafka.sample.spring;

import com.adachina.mqKafka.core.AdaKafkaProducer;
import com.adachina.mqKafka.sample.domain.Dog;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.fastjson.JSON;

/**
 * Sample for use {@link AdaKafkaProducer} with Spring context.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaProducerSpringSample {
	public static void main(String[] args) throws InterruptedException {
		ApplicationContext ac = new ClassPathXmlApplicationContext(
				"kafka-producer.xml");

		AdaKafkaProducer adaKafkaProducer = (AdaKafkaProducer) ac.getBean("producer");

		for (int i = 0; i < 10; i++) {
			Dog dog = new Dog();
			dog.setName("Yours " + i);
			dog.setId(i);
			adaKafkaProducer.send("test", JSON.toJSONString(dog));

			System.out.format("Sending dog: %d \n", i + 1);

			Thread.sleep(100);
		}
	}
}
