package com.adachina.mqKafka.boot;

import java.util.List;

import com.adachina.mqKafka.excephandler.ExceptionHandler;
import com.adachina.mqKafka.core.AdaKafkaConsumer;
import com.adachina.mqKafka.core.AdaKafkaProducer;

/**
 * The context class which stores the runtime Kafka processor reference.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
public class KafkaHandler {
	private AdaKafkaConsumer adaKafkaConsumer;

	private AdaKafkaProducer adaKafkaProducer;

	private List<ExceptionHandler> excepHandlers;

	private KafkaHandlerMeta kafkaHandlerMeta;

	public KafkaHandler(AdaKafkaConsumer adaKafkaConsumer, AdaKafkaProducer adaKafkaProducer, List<ExceptionHandler> excepHandlers, KafkaHandlerMeta kafkaHandlerMeta) {
		super();
		this.adaKafkaConsumer = adaKafkaConsumer;
		this.adaKafkaProducer = adaKafkaProducer;
		this.excepHandlers = excepHandlers;
		this.kafkaHandlerMeta = kafkaHandlerMeta;
	}

	public AdaKafkaConsumer getAdaKafkaConsumer() {
		return adaKafkaConsumer;
	}

	public void setAdaKafkaConsumer(AdaKafkaConsumer adaKafkaConsumer) {
		this.adaKafkaConsumer = adaKafkaConsumer;
	}

	public AdaKafkaProducer getAdaKafkaProducer() {
		return adaKafkaProducer;
	}

	public void setAdaKafkaProducer(AdaKafkaProducer adaKafkaProducer) {
		this.adaKafkaProducer = adaKafkaProducer;
	}

	public List<ExceptionHandler> getExcepHandlers() {
		return excepHandlers;
	}

	public void setExcepHandlers(List<ExceptionHandler> excepHandlers) {
		this.excepHandlers = excepHandlers;
	}

	public KafkaHandlerMeta getKafkaHandlerMeta() {
		return kafkaHandlerMeta;
	}

	public void setKafkaHandlerMeta(KafkaHandlerMeta kafkaHandlerMeta) {
		this.kafkaHandlerMeta = kafkaHandlerMeta;
	}

}
