package com.zyh.zookeeperdemo.zkdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * <p>zk client  demo</p>
 *
 * @author : zyh
 **/
@Slf4j
public class ZookeeperClientDemo {

    private static String conect_str = "192.168.6.128:2181";

    private static Integer SESSION_TIME_OUT = 30 * 1000;

    private static ZooKeeper zooKeeper = null;

    private static CountDownLatch countDownLatch = new CountDownLatch(1);


    public static void main(String[] args) throws Exception {
        zooKeeper = new ZooKeeper(conect_str, SESSION_TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
                    log.info("连接成功！");
                    //连接成功去除线程栅栏
                    countDownLatch.countDown();
                }
            }
        });
        countDownLatch.await();
        ObjectMapper objectMapper = new ObjectMapper();
        //MyData myData = new MyData();
        //myData.setUsername("username");
        //myData.setPassword("password");

        //byte [] data=objectMapper.writeValueAsBytes(myData);
        //String reuslt=zooKeeper.create("/myData",data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);//创建持久节点  权限公开
        //System.out.println(reuslt);

        //监听事件
        Watcher watcher = new Watcher() {
            @SneakyThrows
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDataChanged && event.getPath() != null) {
                    log.info("{}--发生改变", event.getPath());
                    //持续监听改变事件
                    byte[] dataByte = zooKeeper.getData(event.getPath(), this, null);
                    MyData myData = objectMapper.readValue(dataByte, MyData.class);
                    log.info("数据变成了---》{}", myData);
                }
            }
        };
        byte[] dataByte = zooKeeper.getData("/myData", watcher, null);
        MyData oldData = objectMapper.readValue(dataByte, MyData.class);
        log.info("oldData--->{}", oldData);
        Thread.sleep(Integer.MAX_VALUE);
    }

}
