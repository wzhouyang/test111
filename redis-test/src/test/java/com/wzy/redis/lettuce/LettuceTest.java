package com.wzy.redis.lettuce;

import com.google.common.base.Stopwatch;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.wzy.redis.BaseTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LettuceTest extends BaseTest {
    RedisCommands<String, String> syncCommands;
    RedisAsyncCommands<String, String> async;

    @Before
    public void setUp() throws Exception {
        RedisClient redisClient = RedisClient.create("redis://100.80.129.31:6379/0");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        async = connection.async();
        syncCommands = connection.sync();
        preheat();
    }

    @Test
    public void t1() throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 3; i++) {
            executorService.submit(() -> {
                System.out.println(Thread.currentThread() + " start");
                Stopwatch stopwatch = Stopwatch.createStarted();
                for (int i1 = 100000; i1 < 1100000; i1++) {
                    try {
                        async.set("test" + i1, "test" + i1).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
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

    @Override
    protected void preheat() {
        for (int i = 0; i < 10000; i++) {
            syncCommands.set("test" + i, "test" + i);
        }

        for (int i = 0; i < 10000; i++) {
            RedisFuture<String> set = async.set("test" + i, "test" + i);
        }
    }

    @Override
    protected void run() {

    }
}
