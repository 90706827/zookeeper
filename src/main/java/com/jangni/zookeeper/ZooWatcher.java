package com.jangni.zookeeper;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * @ClassName ZooWatcher
 * @Description 接收会话事件的对象，用来监控客户端与服务端会话健康状况
 * @Author Mr.Jangni
 * @Date 2019/5/11 16:26
 * @Version 1.0
 **/
public class ZooWatcher  implements Watcher {
    private final Logger logger = LoggerFactory.getLogger(ZooWatcher.class);
    ZooKeeper zk;

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info(watchedEvent.toString());
    }

    void startZk(String host) throws IOException {
        zk = new ZooKeeper(host,15000,this);
    }

    void createMaster(){

        String serverId = Integer.toHexString(new Random().nextInt());
        try {
            zk.create("/master",serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.ConnectionLossException e) {
            logger.error("客户端与服务端失去连接时抛出");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    void closeZk() throws InterruptedException {
        if(zk!=null){
            zk.close();
        }
    }
    public static void main() throws IOException, InterruptedException {
        ZooWatcher zw = new ZooWatcher();
        zw.startZk("172.0.0.1:2181");


        zw.closeZk();
    }
}
