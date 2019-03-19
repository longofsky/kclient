package com.adachina.mqKafka.sample.api;

import com.adachina.mqKafka.sample.domain.Dog;
import com.adachina.mqKafka.handlers.BeanMessageHandler;

/**
 * The message handler sample to handle the actual beans.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */

public class DogHandler extends BeanMessageHandler<Dog> {
	public DogHandler() {
		super(Dog.class);
	}

	protected void doExecuteBean(Dog dog) {
		System.out.format(System.currentTimeMillis()+":++++++++++++++++++++++++Receiving dog++++++++++++++++++++++: %s\n", dog);
	}
}
