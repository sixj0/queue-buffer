package com.sixj.queuebuffer.consumer;

import java.util.List;

/**
 * @author sixiaojie
 * @date 2020-08-27-11:46
 */
public interface QueueTaskConsumer {


    /**
     * 消费多条任务
     * @param dataList
     * @throws Exception
     */
    void consume(List<String> dataList) throws Exception;
}
