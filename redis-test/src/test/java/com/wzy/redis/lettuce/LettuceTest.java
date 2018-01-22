package com.wzy.redis.lettuce;

import com.google.common.base.Stopwatch;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.support.ConnectionPoolSupport;
import com.wzy.redis.BaseTest;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class LettuceTest extends BaseTest {

    GenericObjectPool<StatefulRedisConnection<String, String>> pool;

    @Before
    public void setUp() throws Exception {
        RedisClient redisClient = RedisClient.create("redis://100.80.129.31:6379/0");
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(50);
        poolConfig.setMinIdle(10000);
        poolConfig.setMaxIdle(50000);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setMaxWaitMillis(1000);
        poolConfig.setBlockWhenExhausted(false);

        pool
                = ConnectionPoolSupport.createGenericObjectPool(redisClient::connect, poolConfig);
        preheat();
    }

    @Test
    public void t1() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        for (int i = 0; i < 6; i++) {
            executorService.submit(() -> {
                AtomicInteger atomicInteger = new AtomicInteger(0);
                System.out.println(Thread.currentThread() + " start");
                Stopwatch stopwatch = Stopwatch.createStarted();
                for (int i1 = 100000; i1 < 1100000; i1++) {
                    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
                        RedisFuture<String> set = connection.async().set("test" + i1, "test" + i1);
                        set.thenAccept((s) -> {
                            if (s != null) {
                                int i2 = atomicInteger.incrementAndGet();
                                if (i2 == 1000000) {
                                    stopwatch.stop();
                                    System.out.println(stopwatch);
                                }
                            }
                        });

                        if (i1 % 100000 == 0) {
                            System.out.println(Thread.currentThread() + "   " + i1);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

//                stopwatch.stop();

                System.out.println(atomicInteger.get());

                System.out.println(Thread.currentThread() + ":" + stopwatch);
            });
        }

        System.in.read();
    }

    @Override
    protected void preheat() throws Exception {
        for (int i = 0; i < 10000; i++) {
            try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
                connection.async().set("test" + i, "test" + i);
            }
        }
    }

    @Override
    protected void run() {

    }

    @After
    public void tearDown() throws Exception {
        pool.close();
    }
}
