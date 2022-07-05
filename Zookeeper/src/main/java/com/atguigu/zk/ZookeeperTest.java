package com.atguigu.zk;


import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 1.获取zk客户端连接对象
 * 2.调用API
 * 3.关闭连接
 */
public class ZookeeperTest {
    private ZooKeeper zk ;

    /**
     * path 制定创建节点路径
     * data  指定创建节点下的内容
     * acl 对操作用户的权限控制
     * CreateMode 制定当前节点类型：持久化，临时
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        zk.create("/sanguo","guanyu".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
    }



    @Before
    public void init() throws IOException {
        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 10000;
        zk = new ZooKeeper(connectString,sessionTimeout,new Watcher(){

            public void process(WatchedEvent watchedEvent) {
                System.out.println("根据具体业务进行下一步操作。。。");
            }
        });
        System.out.println("zk : "+zk);
    }

    @After
    public void close() throws InterruptedException {
        zk.close();
    }




    /**
     * connectString 连接地址
     * sessionTimeout 超时时长
     * Watcher
     */
//    @Test
//    public void testCreateZKClient() throws IOException, InterruptedException {
//        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
//        int sessionTimeout = 10000;
//        ZooKeeper zk = new ZooKeeper(connectString,sessionTimeout,new Watcher(){
//
//            public void process(WatchedEvent watchedEvent) {
//                System.out.println("根据具体业务进行下一步操作。。。");
//            }
//        });
//        System.out.println("zk : "+zk);
//        zk.close();
//    }
}
