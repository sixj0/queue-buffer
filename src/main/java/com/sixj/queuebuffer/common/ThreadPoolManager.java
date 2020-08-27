package com.sixj.queuebuffer.common;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 使用说明
 * 1、注入ThreadPoolExecutor 使用默认的线程池
 * 2、注入ThreadPoolManager 调用getThreadPoolExecutor方法获取自定义的线程池
 *
 * @program: FSP
 * @description: 线程池管理器
 * @author: yufangze
 * @create: 2019-08-29 14:54
 */
@Component
public class ThreadPoolManager {

    /**cpu个数*/
    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();
    /** 线程池运行的核心线程数*/
    private static final int CORE_POOL_SIZE = Math.max(2, Math.min(CPU_COUNT - 1, 4));
    /** 线程池最大线程数*/
    private static final int MAXIMUM_POOL_SIZE = CPU_COUNT * 2 + 1;
    /** 线程空闲后的存活时长*/
    private static final long KEEP_ALIVE_TIME = 10L;
    /** 线程池名称*/
    private static final String THREAD_POOL_NAME = "fsp";
    /** 提交到线程池的Runnable队列*/
    private static final BlockingQueue<Runnable> POOL_WORK_QUEUE = new LinkedBlockingQueue(2000);

    @Bean(destroyMethod = "shutdown")
    public ThreadPoolExecutor threadPoolExecutor() {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAXIMUM_POOL_SIZE,
                KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                POOL_WORK_QUEUE,
                new NamedThreadFactory(THREAD_POOL_NAME),
                new CallerRunsPolicy());
        return poolExecutor;
    }

    /**
     * 可根据实际业务场景，创建不同的线程池
     *
     * @param threadPoolName  线程池名称
     * @param corePoolSize    核心线程数量
     * @param maximumPoolSize 最大线程数量
     * @param keepAliveTime   当线程空闲时，保持活跃的时间
     * @return
     */
    public ThreadPoolExecutor getThreadPoolExecutor(String threadPoolName, int corePoolSize,
                                                    int maximumPoolSize, long keepAliveTime) {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                POOL_WORK_QUEUE,
                new NamedThreadFactory(threadPoolName),
                new CallerRunsPolicy());
        return poolExecutor;
    }

    /**
     * 命名线程工厂
     */
    static class NamedThreadFactory implements ThreadFactory {

        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NamedThreadFactory(String name) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            if (null == name || name.isEmpty()) {
                name = "pool";
            }
            namePrefix = "【" + name + "】-线程池编号：" + poolNumber.getAndIncrement() + "-线程编号：";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
