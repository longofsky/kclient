package com.robert.kafka.kclient.sample.api;

import java.io.IOException;

import com.robert.kafka.kclient.core.AdaKafkaConsumer;
import com.robert.kafka.kclient.messageExecuteHandle.AbstractMessageExecuteHandle;
import com.robert.kafka.kclient.messageExecuteHandle.AsyncMessageExecuteHandle;
import com.robert.kafka.kclient.messageExecuteHandle.SyncMessageExecuteHandle;

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
