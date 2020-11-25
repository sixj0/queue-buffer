### 《任务缓冲队列模型》

### 场景：

在统一日志收集插件中，会记录每个接口的请求日志，每产生一条记录就会往数据库中添加一条记录，每天产生几百万条数据，随着应用该插件的服务越来越多，这个数据量还会越来越多，为了减轻对数据库的压力，减少写数据的频率。计划改为批量写数据库，比如每产生500条日志，向数据库批量插入一次，或者每间隔多长时间批量插入一次。所以就实现了一个队列缓冲的功能。

### 主要组件：

```java
/** 任务缓冲队列 */
private LinkedBlockingQueue<String> taskQueue = new LinkedBlockingQueue<>();
/** 待消费的任务列表*/
private List<String> consumeDataList = new ArrayList<>();
/** 消费者列表 */
private List<QueueTaskConsumer> consumerList = new ArrayList<>();
```

- 任务缓冲队列： 所有生产者生成的的任务会先进入到这个任务缓冲队列中，就好比是一个仓库，生产者将任务放到仓库中。
- 待消费的任务列表： 好比是一辆运输车，主要负责将仓库中的数据运输给消费者，让消费者来处理这些数据。
- 消费者列表：包含多个消费者，主要负责消费处理数据。所有消费者需要实现`QueueTaskConsumer`接口，然后重写`consume(List<String> dataList)`方法，自定义消费逻辑。

### 配置项：

```properties
### 任务缓冲队列长度 默认Integer.MAX_VALUE 2147483647
queue.buffer.capacity = 2147483647
### 当队列长度达到这个值时会触发消费，默认1000
queue.buffer.numElements = 500
### 间隔这个时间会触发消费，单位为秒，默认10秒
queue.buffer.timeout = 60
### 持久化文件地址,默认为项目根目录
queue.buffer.persistence.filepath = /Users/sixj/Desktop
```

- queue.buffer.capacity：缓冲队列的大小，也就是仓库的容量
- queue.buffer.numElements：当运输车上的数据量达到500个的时候，就会给消费者运输一次
- queue.buffer.timeout：当运输车等待时间超过60秒，不管有没有数据量有没有达到500个，都会进行一次运输
- queue.buffer.persistence.filepath：任务持久化的地址，如果项目挂掉了，但是仓库中还有数据没有被消费，或者运输车中的数据还没有被消费，会将这些数据持久化到文件中。下次项目启动的时候，再将数据从文件中读写到缓冲队列中。

### 执行流程：

1. 项目启动时，读取配置，初始化任务缓冲队列；扫描都有哪些消费者；将持久化文件中的数据到任务队列，并开启对任务队列的监听
2. 生产者向任务队列中添加数据
3. 将任务缓冲队列队列中的数据转移到待消费任务列表，就是将仓库中的数据装到运输车中
4. 当运输车的数据装到一定数量，或者运输车等待够一定时长，将数据运输给消费者
5. 消费者进行消费
6. 循环执行3、4、5步骤
7. 项目关闭时，将仓库和运输车中的数据持久化到文件中

### 核心代码：

```java
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

        // 开启任务队列监听
        threadPoolExecutor.submit(this::queueListen);

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

```



