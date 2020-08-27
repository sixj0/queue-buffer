package com.sixj.queuebuffer;

import com.sixj.queuebuffer.factory.QueueBufferFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class QueueBufferApplicationTests {

    @Autowired
    private QueueBufferFactory queueBufferFactory;

    @Test
    public void contextLoads() {
    }

    @Test
    public void test1() throws InterruptedException {
        List<Thread> threadGroup = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    String data = Thread.currentThread().getName() + "数据" + j;
                    if(j%23==0){
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    queueBufferFactory.produce(data);
                }
            });
            threadGroup.add(thread);
        }
        for (Thread thread : threadGroup) {
            thread.start();
        }
        Thread.sleep(1000000);
    }
}
