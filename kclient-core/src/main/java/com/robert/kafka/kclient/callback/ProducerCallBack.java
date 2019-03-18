package com.robert.kafka.kclient.callback;

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
            //todo
            log.error(recordMetadata.toString()+"+++++++++"+e.toString());
        }

    }
}
