package com.adachina.mqKafka.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ProjectName: kclient
 * @Package: com.robert.kafka.kclient.callback
 * @ClassName: ProducerCallBack
 * @Author: litianlong
 * @Description: ${description}
 * @Date: 2019-03-18 18:27
 * @Version: 1.0
 */
public class ProducerCallBack implements Callback {

    protected static Logger log = LoggerFactory.getLogger(ProducerCallBack.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

        if (e != null) {
           /*
            * todo
            * 封装发送失败消息实体：消息实体记录包括：源消息内容，Kafka信息，错误信息
            * 思路1. 将所有的发送失败的消息，发送到特定的 topic->异步线程持久化->业务逻辑处理
            * 思路2. 将所有的发送失败的消息放在内存集合中->另起异步线程不断的重试
            * 思路3. 将所有的发送失败的消息持久化到数据库
            */
            log.error(recordMetadata.toString()+"+++++++++"+e.toString());
        } else {
            /**
             * 消息发送成功，清楚内存中暂存的消息记录
             */
        }

    }
}
