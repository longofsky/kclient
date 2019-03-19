package com.adachina.mqkafka;

import java.util.concurrent.Future;

import com.adachina.mqKafka.core.AdaKafkaProducer;
import com.adachina.mqKafka.core.AdaKafkaProducerMap;
import com.adachina.mqkafka.domain.Dog;
import org.apache.kafka.clients.producer.RecordMetadata;
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
public class ProducerController {
	protected static Logger log = LoggerFactory.getLogger(ProducerApplication.class);

	@Autowired
	private ApplicationContext context;

	@Autowired
	private AdaKafkaProducerMap producerMap;

	@RequestMapping("/")
	public String hello() {
		return "Greetings from AdaKafkaProducer processor!";
	}


	@RequestMapping("/send")
	public String send() {
		for (int i = 0; i < 10; i++) {
			Dog dog = new Dog();
			dog.setName("Yours " + i);
			dog.setId(i);
			Future<RecordMetadata> future =  producerMap.getKafkaProducermap().get("adaKafkaProducer_clust1").sendBean2Topic("test", dog);

			System.out.format(future.toString()+"++++++++++Sending dog: %d \n", i + 1);

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return "Sending dog:10";
	}

}