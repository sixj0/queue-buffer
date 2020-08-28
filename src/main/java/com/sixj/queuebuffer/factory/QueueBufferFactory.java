package com.sixj.queuebuffer.factory;

import com.sixj.queuebuffer.consumer.QueueTaskConsumer;
import com.sixj.queuebuffer.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

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
    @Autowired
    private Environment environment;

    /** 当队列长度达到这个值时会触发消费，默认1000 */
    @Value("${queue.buffer.numElements:1000}")
    private Integer numElements;

    /** 间隔这个时间会触发消费，单位为秒，默认10秒 */
    @Value("${queue.buffer.timeout:10}")
    private Integer timeout;

    /**
     * 数据持久化的文件地址，默认项目根目录
     */
    private String filepath = "";


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
                int drain = drain(taskQueue, consumeDataList, numElements, timeout, TimeUnit.SECONDS);
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
        // 初始化任务缓冲队列
        String capacity = environment.getProperty("queue.buffer.capacity");
        logger.info("任务缓冲队列长度配置：{}",capacity);
        if(StringUtils.isEmpty(capacity)){
            //默认Integer.MAX_VALUE 2147483647
            taskQueue = new LinkedBlockingQueue<>();
        }else{
            taskQueue = new LinkedBlockingQueue<>(Integer.valueOf(capacity.trim()));
        }

        // 读取持久化文件地址
        String filepath = environment.getProperty("queue.buffer.persistence.filepath");
        logger.info("持久化文件地址配置：{}",filepath);
        if(!StringUtils.isEmpty(filepath)){
            this.filepath = filepath.trim();
        }

        // 扫描消费者列表
        Map<String, QueueTaskConsumer> consumerMap = applicationContext.getBeansOfType(QueueTaskConsumer.class);
        logger.info("扫描消费者列表：{}",consumerMap);
        if (consumerMap.size() > 0) {
            consumerList.addAll(consumerMap.values());
        }

        // 扫描持久化文件到任务队列
        boolean fileExist = FileUtils.isFileExist(FileUtils.getQueueBufferFilePath(filepath));
        if(fileExist){
            List<String> dataList = FileUtils.readFileContent(FileUtils.getQueueBufferFilePath(filepath));
            // 过滤空字符串
            dataList = dataList.stream().filter(data->!StringUtils.isEmpty(data)).collect(Collectors.toList());
            taskQueue.addAll(dataList);
            // 清空持久化的数据
            FileUtils.deleteFile(FileUtils.getQueueBufferFilePath(filepath));
        }

    }

    @Override
    public void destroy() throws Exception {
        logger.info("项目关闭");

        logger.info("队列积压数据："+taskQueue.size());
        logger.info("待持久化数据："+consumeDataList.size());

        // 将任务队列中积压的任务持久化到本地文件
        for (String data : taskQueue) {
            FileUtils.fileLinesWrite(FileUtils.getQueueBufferFilePath(filepath),data,true);
        }
        for (String data : consumeDataList) {
            FileUtils.fileLinesWrite(FileUtils.getQueueBufferFilePath(filepath),data,true);
        }
    }
}
