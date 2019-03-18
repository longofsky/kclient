package com.robert.kafka.kclient.messageExecuteHandle;

import com.robert.kafka.kclient.handlers.MessageHandler;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @ProjectName: kclient
 * @Package: com.robert.kafka.kclient.messageExecuteHandle
 * @ClassName: MessageExecuteHandle
 * @Author: litianlong
 * @Description: ${description}
 * @Date: 2019-03-15 15:06
 * @Version: 1.0
 */
public interface MessageExecuteHandle {

    public void execute();
}
