package com.adachina.mqKafka.sample.api;

import com.adachina.mqKafka.core.AdaKafkaProducer;
import com.adachina.mqKafka.sample.domain.Dog;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * Sample for use {@link AdaKafkaProducer} with Java API.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaProducerSample {
	public static void main(String[] args) throws InterruptedException {
		AdaKafkaProducer adaKafkaProducer = new AdaKafkaProducer("kafka-producer.properties", "test");

		for (int i = 0; i < 10; i++) {
			Dog dog = new Dog();
			dog.setName("Yours " + i);
			dog.setId(i);
			Future<RecordMetadata> future =  adaKafkaProducer.sendBean2Topic("test", dog);

			System.out.format(future.toString()+"++++++++++Sending dog: %d \n", i + 1);

			Thread.sleep(100);
		}
	}
}
