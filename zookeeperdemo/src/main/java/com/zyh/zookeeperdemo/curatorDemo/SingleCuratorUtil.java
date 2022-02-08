package com.zyh.zookeeperdemo.curatorDemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;

/**
 * <p>Curator连接类</p>
 *
 * @author : zyh
 **/
@Slf4j
public abstract class SingleCuratorUtil {


    private String CONNECT_STR = "192.168.6.128:2181";
    private int SESSIONTIMEOUT = 60 * 1000;
    private int CONNECTTIMEOUT = 60 * 1000;
    private CuratorFramework curatorFramework;

    @Before
    public void init() {
        //超时设置 5秒 3次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(5000, 3);
        //
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_STR)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(SESSIONTIMEOUT)
                .connectionTimeoutMs(CONNECTTIMEOUT)
                .canBeReadOnly(true).build();
        //
        curatorFramework.getConnectionStateListenable().addListener((client, newStat) -> {
            if (newStat == ConnectionState.CONNECTED) {
                log.info("连接成功！");
            }
        });
        log.info("连接中。。。。");
        curatorFramework.start();
    }


    @After
    public void keyAliveTest() throws InterruptedException {
        Thread.sleep(Integer.MAX_VALUE);
    }

    public CuratorFramework getCuratorFramework() {
        return curatorFramework;
    }

    public void createIfNotExists(String path) throws Exception {
        Stat stat = curatorFramework.checkExists().forPath(path);
        if (stat == null) {
            curatorFramework.create().forPath(path);
            log.info("path 创建了");
        }
    }
}
