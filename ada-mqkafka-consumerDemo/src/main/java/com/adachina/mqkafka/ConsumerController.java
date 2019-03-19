package com.adachina.mqkafka;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.adachina.mqKafka.core.AdaKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Description
 * @Author litianlong
 * @Version 1.0
 * @Param
 * @Return
 * @Exception
 * @Date 2019-03-19 13:22
 */

@RestController
public class ConsumerController {
	protected static Logger log = LoggerFactory.getLogger(ConsumerApplication.class);

	@Autowired
	private ApplicationContext context;

	@Autowired
	private AdaKafkaConsumer consumer;

	@RequestMapping("/")
	public String hello() {
		return "Greetings from AdaKafkaConsumer processor!";
	}

	@RequestMapping("/status")
	public String status() {
		return "Handler Number: [" +consumer.getAbstractMessageExecuteHandle().status
				+ "]";
	}

	@RequestMapping("/stop")
	public String stop() {
		log.info("Shutdowning AdaKafkaConsumer now...");
		consumer.shutdownGracefully();

		String startupTime = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z")
				.format(new Date(context.getStartupDate()));
		log.info("AdaKafkaConsumer application stops at: " + startupTime);

		return "AdaKafkaConsumer application stops at: " + startupTime;
	}

	@RequestMapping("/restart")
	public String restart() {
		log.info("Shutdowning AdaKafkaConsumer now...");
		consumer.shutdownGracefully();

		log.info("Restarting AdaKafkaConsumer now...");
		consumer.startup();

		String startupTime = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z")
				.format(new Date(context.getStartupDate()));
		log.info("AdaKafkaConsumer application restarts at: " + startupTime);

		return "AdaKafkaConsumer application restarts at: " + startupTime;
	}

}