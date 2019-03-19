package com.adachina.mqKafka.messageExecuteHandle;

import com.adachina.mqKafka.handlers.MessageHandler;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * @ProjectName: kclient
 * @Package: com.robert.kafka.kclient.messageExecuteHandle
 * @ClassName: SyncMessageExecuteHandle
 * @Author: litianlong
 * @Description: ${description}
 * @Date: 2019-03-15 15:10
 * @Version: 1.0
 */
public class SyncMessageExecuteHandle extends AbstractMessageExecuteHandle {

    protected static Logger log = LoggerFactory.getLogger(SyncMessageExecuteHandle.class);

    /*
        控制偏移量提交
     */
    public Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>(16);

    public SyncMessageExecuteHandle(String topic, MessageHandler handler) {
        super(topic,handler);
    }


    @Override
    public void execute() {

        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Arrays.asList(topic),new HandleRebalance());

        String timeOut = properties.getProperty("adachina.timeOut");

        Long timeOutLong = StringUtils.isEmpty(timeOut) ? defTimeOutLong : Long.parseLong(timeOut);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(timeOutLong);

                for (ConsumerRecord<String, String> record : records) {

                    /**
                     *  对遍历到的消息 进行记录并添加 待处理标记，目的：确保丢失的消息有记录 缺点：降低处理速度
                     */

                    syncHandleMessage(record, handler);

                    /**
                     *  对当前消息 擦除待处理标记
                     */

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }

                consumer.commitAsync(currentOffsets, null);

            }
        } catch (Exception e) {
            log.error("",e);
        } finally {

            try {
                consumer.commitSync(currentOffsets);

            } finally {
                consumer.close();
            }
        }

    }

    /**
     * @Description
     * @Author litianlong
     * @Version 1.0
     * @Param
     * @Return
     * @Exception
     * @Date 2019-03-15 14:21
     */
    private void syncHandleMessage (ConsumerRecord<String, String> record,MessageHandler handler) {

        handler.execute(record.value());

    }

    @Override
    public void initKafka(Properties properties) {
        super.initKafka(properties);

    }

    private class HandleRebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            consumer.commitSync(currentOffsets);
        }
    }
}
