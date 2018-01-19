package com.wzy.redis.jedis;

import com.google.common.base.Stopwatch;
import com.wzy.redis.BaseTest;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class JedisTest extends BaseTest {

    JedisPool jedisPool;

    @Before
    public void setUp() throws Exception {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMinIdle(10000);
        poolConfig.setMaxIdle(100000);
        poolConfig.setBlockWhenExhausted(false);
        poolConfig.setJmxEnabled(false);
        poolConfig.setMaxWaitMillis(10000);
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestWhileIdle(false);
        poolConfig.setTestOnReturn(false);
        jedisPool = new JedisPool(poolConfig, "100.80.129.31", 6379, 500, null);
        preheat();
        System.out.println("预热完毕");
    }

    protected void preheat() {
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 10; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        try (Jedis jedis = jedisPool.getResource()) {
                            jedis.set("test" + i, "test" + i);
                        }
                    }
                }
            });
        }

        ThreadPoolExecutor t = (ThreadPoolExecutor) executorService;

        try {
            while (t.getActiveCount() > 0) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void run() {

    }

    @Test
    public void t1() throws InterruptedException, IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 3; i++) {
            executorService.submit(() -> {
                System.out.println(Thread.currentThread() + " start");
                Stopwatch stopwatch = Stopwatch.createStarted();
                for (int i1 = 100000; i1 < 1100000; i1++) {
                    try (Jedis jedis = jedisPool.getResource()) {
                        jedis.set("test" + i1, "test" + i1);
                    }

                    if (i1 % 100000 == 0) {
                        System.out.println(Thread.currentThread() + "   " + i1);
                    }
                }

                stopwatch.stop();

                System.out.println(Thread.currentThread() + ":" + stopwatch);
            });
        }

        System.in.read();
    }
}
