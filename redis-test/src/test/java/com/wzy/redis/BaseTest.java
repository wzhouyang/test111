package com.wzy.redis;

public abstract class BaseTest {

    protected abstract void preheat() throws Exception;

    protected abstract void run();
}
