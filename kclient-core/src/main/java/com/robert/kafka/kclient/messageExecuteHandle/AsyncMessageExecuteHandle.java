package com.robert.kafka.kclient.messageExecuteHandle;

import com.robert.kafka.kclient.core.AdaKafkaConsumer;
import com.robert.kafka.kclient.handlers.MessageHandler;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @ProjectName: kclient
 * @Package: com.robert.kafka.kclient.messageExecuteHandle
 * @ClassName: AsyncMessageExecuteHandle
 * @Author: litianlong
 * @Description: ${description}
 * @Date: 2019-03-15 15:39
 * @Version: 1.0
 */
public class AsyncMessageExecuteHandle  extends AdaKafkaConsumer implements MessageExecuteHandle  {


    @Override
    public void execute(KafkaConsumer<String, String> consumer, MessageHandler handler, Properties properties) {

    }
}
