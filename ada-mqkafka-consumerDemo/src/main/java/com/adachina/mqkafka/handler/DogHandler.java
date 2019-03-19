package com.adachina.mqkafka.handler;

import com.adachina.mqKafka.handlers.BeanMessageHandler;
import com.adachina.mqkafka.domain.Dog;

/**
 * @ProjectName: kclient
 * @Package: com.adachina.mqkafka.handler
 * @ClassName: DogHandler
 * @Author: litianlong
 * @Description: ${description}
 * @Date: 2019-03-19 13:02
 * @Version: 1.0
 */
public class DogHandler extends BeanMessageHandler<Dog> {

    public DogHandler() {
        super(Dog.class);
    }

    @Override
    protected void doExecuteBean(Dog bean) {

        System.out.format(System.currentTimeMillis()+":++++++++++++++++++++++++Receiving dog++++++++++++++++++++++: %s\n", bean);

    }
}
