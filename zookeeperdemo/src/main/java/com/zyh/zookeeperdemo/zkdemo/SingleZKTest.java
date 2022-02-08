package com.zyh.zookeeperdemo.zkdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;

/**
 * <p>zk调用基本使用</p>
 *
 * @author : zyh
 **/
@Slf4j
public class SingleZKTest extends SingleZkUtil {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void createTest() throws JsonProcessingException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = getZooKeeper();
        byte[] data = objectMapper.writeValueAsBytes("{param:100}");
        log.info("创建目录");
        String reuslt=zooKeeper.create("/createTest", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("----reuslt--{}",reuslt);
        log.info("创建成功");
    }


    @Test
    public void getTest() throws KeeperException, InterruptedException, IOException {
        ZooKeeper zooKeeper = getZooKeeper();
        Watcher watcher = new Watcher() {
            @SneakyThrows
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDataChanged && "/createTest".equals(event.getPath())) {
                    byte[] result = zooKeeper.getData("/createTest", this, null);
                    log.info("数据发生改变--newValue-{}", new String(result));
                }
            }
        };
        byte [] result = zooKeeper.getData("/createTest", watcher, null);
        log.info("--oldValue--{}", new String(result));
    }

    @Test
    public void setTest() {
        ZooKeeper zooKeeper = getZooKeeper();
        try {
            byte[] data = objectMapper.writeValueAsBytes("www");
            zooKeeper.setData("/createTest", data, 8);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void delete() {
        ZooKeeper zooKeeper = getZooKeeper();
        try {
            // 9 Dataversion 版本号
            zooKeeper.delete("/createTest", 9);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void asyGetDataTest() {
        ZooKeeper zooKeeper = getZooKeeper();
        zooKeeper.getData("/createTest", false, (rc, path, ctx, data, stat) -> {
            Thread thread = Thread.currentThread();
            //这里的data 是ascii码
            log.info("thread ->{}---rc--{}--path--{}--ctx---{}--data--{}--stat--{}", thread.getName(), rc, path, ctx, data, stat);
        }, "creataTest");
        log.info("---end---");
    }
}
