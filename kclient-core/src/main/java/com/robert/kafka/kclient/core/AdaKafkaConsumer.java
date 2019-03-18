package com.robert.kafka.kclient.core;

import java.io.IOException;
import java.util.*;

import com.robert.kafka.kclient.mainThreadEnum.MainThreadStatusEnum;
import com.robert.kafka.kclient.messageExecuteHandle.AbstractMessageExecuteHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.robert.kafka.kclient.handlers.MessageHandler;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @Description
 * @Author litianlong
 * @Version 1.0
 * @Param
 * @Return
 * @Exception
 * @Date 2019-03-18 18:49
 */
public class AdaKafkaConsumer {
	protected static Logger log = LoggerFactory.getLogger(AdaKafkaConsumer.class);


	@Autowired
	private AbstractMessageExecuteHandle abstractMessageExecuteHandle;

	private String propertiesFile;
	private Properties properties;

	public AdaKafkaConsumer() {
		// For Spring context
	}

	public AdaKafkaConsumer(String propertiesFile,AbstractMessageExecuteHandle abstractMessageExecuteHandle) {
		this.propertiesFile = propertiesFile;
		this.abstractMessageExecuteHandle = abstractMessageExecuteHandle;
		init();
	}



	public AdaKafkaConsumer(Properties properties,AbstractMessageExecuteHandle abstractMessageExecuteHandle) {
		this.properties = properties;
		this.abstractMessageExecuteHandle = abstractMessageExecuteHandle;
		init();
	}

	public void init() {
		if (properties == null && propertiesFile == null) {
			log.error("The properties object or file can't be null.");
			throw new IllegalArgumentException(
					"The properties object or file can't be null.");
		}

		if (properties == null) {
			properties = loadPropertiesfile();
		}

		initGracefullyShutdown();
		abstractMessageExecuteHandle.init();
		abstractMessageExecuteHandle.initKafka(properties);
	}

	protected Properties loadPropertiesfile() {
		Properties properties = new Properties();
		try {
			properties.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream(propertiesFile));
		} catch (IOException e) {
			log.error("The consumer properties file is not loaded.", e);
			throw new IllegalArgumentException(
					"The consumer properties file is not loaded.", e);
		}

		return properties;
	}

	protected void initGracefullyShutdown() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdownGracefully();
			}
		});
	}

	public void startup() {
		if (abstractMessageExecuteHandle.status != MainThreadStatusEnum.INIT) {
			log.error("The client has been started.");
			throw new IllegalStateException("The client has been started.");
		}

		abstractMessageExecuteHandle.status = MainThreadStatusEnum.RUNNING;

		abstractMessageExecuteHandle.execute();

	}

	public void shutdownGracefully() {

//		System.out.println("shutdownGracefully");

		abstractMessageExecuteHandle.shutdownGracefully();
	}


	public String getPropertiesFile() {
		return propertiesFile;
	}

	public void setPropertiesFile(String propertiesFile) {
		this.propertiesFile = propertiesFile;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public AbstractMessageExecuteHandle getAbstractMessageExecuteHandle() {
		return abstractMessageExecuteHandle;
	}

	public void setAbstractMessageExecuteHandle(AbstractMessageExecuteHandle abstractMessageExecuteHandle) {
		this.abstractMessageExecuteHandle = abstractMessageExecuteHandle;
	}
}
