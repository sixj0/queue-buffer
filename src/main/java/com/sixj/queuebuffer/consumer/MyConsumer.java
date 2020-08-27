package com.sixj.queuebuffer.consumer;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author sixiaojie
 * @date 2020-08-27-13:37
 */
@Component
public class MyConsumer implements QueueTaskConsumer {

    @Override
    public void consume(List<String> dataList){
        System.out.println(Thread.currentThread().getName()+"批量消费："+dataList.size());
    }
}
