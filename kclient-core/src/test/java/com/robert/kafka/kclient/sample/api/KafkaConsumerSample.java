package com.robert.kafka.kclient.sample.api;

import java.io.IOException;

import com.robert.kafka.kclient.core.AdaKafkaConsumer;

/**
 * Sample for use {@link AdaKafkaConsumer} with Java API.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class KafkaConsumerSample {

	public static void main(String[] args) {
		DogHandler mbe = new DogHandler();

		AdaKafkaConsumer adaKafkaConsumer = new AdaKafkaConsumer("kafka-consumer.properties", "test", 5, mbe);
		try {
			adaKafkaConsumer.startup();

			try {
				System.in.read();
				System.out.println("Read the exit command.");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			System.out.println("Start to exit...");
			adaKafkaConsumer.shutdownGracefully();
		}
	}
}
