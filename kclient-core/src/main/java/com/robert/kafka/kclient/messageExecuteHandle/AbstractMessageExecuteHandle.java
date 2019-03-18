package com.robert.kafka.kclient.messageExecuteHandle;

import com.robert.kafka.kclient.handlers.MessageHandler;
import com.robert.kafka.kclient.mainThreadEnum.MainThreadStatusEnum;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * @ProjectName: kclient
 * @Package: com.robert.kafka.kclient.messageExecuteHandle
 * @ClassName: AbstractMessageExecuteHandle
 * @Author: litianlong
 * @Description: ${description}
 * @Date: 2019-03-18 11:16
 * @Version: 1.0
 */
@Component
public abstract class AbstractMessageExecuteHandle implements MessageExecuteHandle {

    protected static Logger log = LoggerFactory.getLogger(AbstractMessageExecuteHandle.class);

    public volatile MainThreadStatusEnum status = MainThreadStatusEnum.INIT;

    public long defTimeOutLong = 60;


    private String topic;

    public KafkaConsumer<String, String> consumer;

    public MessageHandler handler;

    public Properties properties;

    public AbstractMessageExecuteHandle(String topic, MessageHandler handler) {
        this.handler = handler;
        this.topic = topic;
    }


    public void  initKafka(Properties properties) {

        this.properties = properties;

        // 定义consumer
        consumer = new KafkaConsumer(properties);

        if (consumer == null) {
            log.error("consumer is null.");
            throw new IllegalArgumentException("consumer is null.");
        }
    };

    public void shutdownGracefully() {

        System.out.println("AbstractMessageExecuteHandle+shutdownGracefully");

    };

    @Override
    public void execute(){

    }

    public void init() {

        if (StringUtils.isEmpty(topic)) {
            log.error("The topic can't be empty.");
            throw new IllegalArgumentException("The topic can't be empty.");
        }

        if (handler == null) {
            log.error("Exectuor can't be null!");
            throw new RuntimeException("Exectuor can't be null!");
        }

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public MessageHandler getHandler() {
        return handler;
    }

    public void setHandler(MessageHandler handler) {
        this.handler = handler;
    }

}
