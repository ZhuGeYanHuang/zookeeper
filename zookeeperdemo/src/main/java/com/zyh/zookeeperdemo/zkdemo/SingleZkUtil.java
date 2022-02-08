package com.zyh.zookeeperdemo.zkdemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * <p>单机zk工具类</p>
 *
 * @author : zyh
 **/
@Slf4j
public abstract class SingleZkUtil {

    private static final String CONNECTSTR = "192.168.6.128:2181";

    private static final Integer TIME_OUT = 30 * 1000;


    private CountDownLatch countDownLatch = new CountDownLatch(1);


    private static ZooKeeper zooKeeper = null;


    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
                try {
                    countDownLatch.countDown();
                    log.info("----连接成功---");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    };


    @Before
    public void init() {
        try {
            log.info("--正在连接。。。{}",getCONNECTSTR());
            zooKeeper = new ZooKeeper(getCONNECTSTR(), getTimeOut(), watcher);
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @After
    public void keepAliveTest(){
        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    protected  String getCONNECTSTR() {
        return CONNECTSTR;
    }

    protected  Integer getTimeOut() {
        return TIME_OUT;
    }
}
