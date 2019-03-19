package com.adachina.mqKafka.sample.api;

import java.io.IOException;

import com.adachina.mqKafka.messageExecuteHandle.AbstractMessageExecuteHandle;
import com.adachina.mqKafka.messageExecuteHandle.AsyncMessageExecuteHandle;
import com.adachina.mqKafka.core.AdaKafkaConsumer;
import com.adachina.mqKafka.messageExecuteHandle.SyncMessageExecuteHandle;

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

		//同步消费 Kafka消息
		AbstractMessageExecuteHandle syncMessageExecuteHandle = new SyncMessageExecuteHandle("test",mbe);

		//异步消费 Kafka消息
		AbstractMessageExecuteHandle asyncMessageExecuteHandle = new AsyncMessageExecuteHandle("test",mbe,5);

		AdaKafkaConsumer adaKafkaConsumer = new AdaKafkaConsumer("kafka-consumer.properties",asyncMessageExecuteHandle);


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
