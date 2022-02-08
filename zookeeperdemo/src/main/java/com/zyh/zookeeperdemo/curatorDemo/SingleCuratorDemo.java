package com.zyh.zookeeperdemo.curatorDemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>CuratorTest</p>
 *
 * @author : zyh
 **/
@Slf4j
public class SingleCuratorDemo extends SingleCuratorUtil {


    /**
     * 递归创建 子节点
     */
    @Test
    public void createWithParentTest() throws Exception {
        CuratorFramework curatorFramework = getCuratorFramework();
        byte[] data = new String("test---->").getBytes();
        curatorFramework.create().creatingParentsIfNeeded().forPath("/curatorCreateTestParent/children", data);
        log.info("创建成功");
    }


    /**
     * 保护模式创建节点 防止出现僵尸节点  会有uuid前缀
     *
     * @throws Exception
     */
    @Test
    public void createTest() throws Exception {
        CuratorFramework curatorFramework = getCuratorFramework();
        byte[] data = "testCreate".getBytes();
        curatorFramework.create().withProtection().withMode(CreateMode.PERSISTENT_SEQUENTIAL).
                forPath("/CuratorTest", data);
        log.info("创建成功  --- CuratorTest");
    }

    @Test
    public void getTest() throws Exception {
        CuratorFramework curatorFramework = getCuratorFramework();
        byte[] data = curatorFramework.getData().forPath("/test");
        log.info("data--->{}", new String(data));
    }


    @Test
    public void setTest() throws Exception {
        CuratorFramework curatorFramework = getCuratorFramework();
        byte[] data = "newValue".getBytes();
        curatorFramework.setData().forPath("/createTest", data);
        byte[] result = curatorFramework.getData().forPath("/createTest");
        log.info("result--->{}", new String(result));
    }


    @Test
    public void delete() throws Exception {
        CuratorFramework curatorFramework = getCuratorFramework();
        curatorFramework.delete().forPath("/createTest");
        log.info("----删除成功--");
    }

    /**
     * 获取节数据列表
     * @throws Exception
     */
    @Test
    public void getListChildren() throws Exception {
        CuratorFramework curatorFramework = getCuratorFramework();
        List<String> list = curatorFramework.getChildren().forPath("/test");
        list.forEach((e) -> log.info(e));
    }


    /**
     * 异步查询  返回 ASCII
     * @throws Exception
     */
    @Test
    public void threadGetTest() throws Exception {
        CuratorFramework curatorFramework = getCuratorFramework();
        ExecutorService  executorService = Executors.newSingleThreadExecutor();
        curatorFramework.getData().inBackground((client,event)->{
            log.info("后台查询-->{}",event.getData());
        },executorService).forPath("/test");
    }


}
