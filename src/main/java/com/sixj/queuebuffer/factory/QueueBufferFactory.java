package com.sixj.queuebuffer.factory;

import com.sixj.queuebuffer.consumer.QueueTaskConsumer;
import com.sixj.queuebuffer.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;

/**
 * @author sixiaojie
 * @date 2020-08-27-10:48
 */
@Component
public class QueueBufferFactory implements ApplicationContextAware, DisposableBean {
    private final static Logger logger = LoggerFactory.getLogger(QueueBufferFactory.class);

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    /** 任务缓冲队列 */
    private LinkedBlockingQueue<String> taskQueue = new LinkedBlockingQueue<>();
    /** 消费者列表 */
    private List<QueueTaskConsumer> consumerList = new ArrayList<>();
    /** 待消费的任务列表*/
    private List<String> consumeDataList = new ArrayList<>();

    /**
     * 新增任务
     * @param queueTask
     */
    public void produce(String queueTask){
        taskQueue.add(queueTask);
        logger.info("新增任务：{}",queueTask);
    }


    /**
     * 监听任务队列
     */
    private void queueListen(){
        while (true) {
            try {
                consumeDataList = new ArrayList<>();
                int drain = drain(taskQueue, consumeDataList, 1000, 10, TimeUnit.SECONDS);
                logger.info("消费数量：{}", drain);
                if (!CollectionUtils.isEmpty(consumeDataList)) {
                    // 回调消费方法
                    callBackConsume(consumeDataList);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static <E> int drain( BlockingQueue<E> q, Collection<? super E> buffer, int numElements, long timeout, TimeUnit unit) throws InterruptedException {
        if ( buffer == null) {
            throw new NullPointerException();
        }

        long deadline = System.nanoTime() + unit.toNanos(timeout);
        int added = 0;
        while (added < numElements) {
            added += q.drainTo(buffer, numElements - added);
            if (added < numElements) {
                E e = q.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                if (e == null) {
                    break;
                }
                buffer.add(e);
                added++;
            }
        }
        return added;
    }

    @PostConstruct
    public void startListen(){
        threadPoolExecutor.submit(this::queueListen);
    }


    /**
     * 回调消费方法
     * @param dataList
     */
    public void callBackConsume(List<String> dataList){
        for (QueueTaskConsumer queueTaskConsumer : consumerList) {
            try {
                queueTaskConsumer.consume(dataList);
            } catch (Exception e) {
                logger.error("回调消费方法异常:{}",e.getMessage());
            }
        }
    }



    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, QueueTaskConsumer> consumerMap = applicationContext.getBeansOfType(QueueTaskConsumer.class);
        logger.info("扫描消费者列表：{}",consumerMap);
        if (consumerMap.size() > 0) {
            consumerList.addAll(consumerMap.values());
        }
        // 扫描持久化文件到任务队列
        boolean fileExist = FileUtils.isFileExist(FileUtils.getQueueBufferFilePath());
        if(fileExist){
            List<String> dataList = FileUtils.readFileContent(FileUtils.getQueueBufferFilePath());
            taskQueue.addAll(dataList);
            // 清空持久化的数据
            FileUtils.fileLinesWrite(FileUtils.getQueueBufferFilePath(),"",false);
        }

    }

    @Override
    public void destroy() throws Exception {
        logger.info("项目关闭");

        logger.info("队列积压数据："+taskQueue.size());
        logger.info("待持久化数据："+consumeDataList.size());

        // 将任务队列中积压的任务持久化到本地文件
        for (String data : taskQueue) {
            FileUtils.fileLinesWrite(FileUtils.getQueueBufferFilePath(),data,true);
        }
        for (String data : consumeDataList) {
            FileUtils.fileLinesWrite(FileUtils.getQueueBufferFilePath(),data,true);
        }
    }
}
