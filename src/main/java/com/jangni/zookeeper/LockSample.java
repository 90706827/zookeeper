package com.jangni.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @ClassName LockSample
 * @Description 分布式锁实现
 * @Author Mr.Jangni
 * @Date 2019/4/29 17:14
 * @Version 1.0
 **/
public class LockSample {
    private ZooKeeper zkClient;
    //锁的根路径
    private static final String LOCK_ROOT_PATH = "/Locks";
    //自增节点的前缀
    private static final String LOCK_NODE_NAME = "Lock_";
    private String lockPath;
    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            System.out.println(event.getPath() + " 前锁释放");
            synchronized (this) {
                // 让主线程继续执行，以便再次调用attemptLock()，去尝试获取lock。如果没有异常情况的话，此时当前客户端应该能够成功获取锁。
                notifyAll();
            }
        }
    };

    public LockSample() throws IOException {
        zkClient = new ZooKeeper("localhost:2181", 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.Disconnected) {
                    System.out.println("失去连接");

                }
            }
        });
    }

    //获取锁的实现
    public void acquireLock() throws InterruptedException, KeeperException {
        //创建锁节点
        createLock();
        //尝试获取锁
        attemptLock();
    }

    /**
     * 先判断锁的根节点/Locks是否存在，不存在的话创建。
     * 然后在/Locks下创建有序临时节点，并设置当前的锁路径变量lockPath。
     * CreateMode.PERSISTENT(0, false, false): 持久化目录节点，存储的数据不会丢失。
     * CreateMode.PERSISTENT_SEQUENTIAL(2, false, true):顺序自动编号的持久化目录节点，存储的数据不会丢失，并且根据当前已近存在的节点数自动加 1，
     * 然后返回给客户端已经成功创建的目录节点名。
     * CreateMode.EPHEMERAL(1, true, false):临时目录节点，一旦创建这个节点的客户端与服务器端口也就是session 超时，这种节点会被自动删除。
     * CreateMode.EPHEMERAL_SEQUENTIAL(3, true, true):临时自动编号节点，一旦创建这个节点的客户端与服务器端口也就是session 超时，这种节点会被自动删除，
     * 并且根据当前已近存在的节点数自动加 1，然后返回给客户端已经成功创建的目录节点名
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void createLock() throws KeeperException, InterruptedException {
        //如果根节点不存在，则创建根节点
        Stat stat = zkClient.exists(LOCK_ROOT_PATH, false);
        if (stat == null) {
            zkClient.create(LOCK_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 创建EPHEMERAL_SEQUENTIAL类型节点
        String lockPath = zkClient.create(LOCK_ROOT_PATH + "/" + LOCK_NODE_NAME,
                Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(Thread.currentThread().getName() + " 锁创建: " + lockPath);
        this.lockPath = lockPath;
    }

    /**
     * 客户端尝试去获取锁
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void attemptLock() throws KeeperException, InterruptedException {
        // 获取Lock所有子节点，按照节点序号排序
        List<String> lockPaths = null;

        lockPaths = zkClient.getChildren(LOCK_ROOT_PATH, false);

        Collections.sort(lockPaths);

        int index = lockPaths.indexOf(lockPath.substring(LOCK_ROOT_PATH.length() + 1));

        // 如果lockPath是序号最小的节点，则获取锁
        if (index == 0) {
            System.out.println(Thread.currentThread().getName() + " 锁获得, lockPath: " + lockPath);
            return;
        } else {
            // lockPath不是序号最小的节点，监听前一个节点
            String preLockPath = lockPaths.get(index - 1);
            //我们在获取前一个节点的时候，同时设置了监听watcher。如果前锁存在，则阻塞主线程。
            Stat stat = zkClient.exists(LOCK_ROOT_PATH + "/" + preLockPath, watcher);

            // 假如前一个节点不存在了，比如说执行完毕，或者执行节点掉线，重新获取锁
            if (stat == null) {
                attemptLock();
            } else { // 阻塞当前进程，直到preLockPath释放锁，被watcher观察到，notifyAll后，重新acquireLock
                System.out.println(" 等待前锁释放，prelocakPath：" + preLockPath);
                synchronized (watcher) {
                    watcher.wait();
                }
                attemptLock();
            }
        }
    }

    /**
     * 释放锁实现
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void releaseLock() throws KeeperException, InterruptedException {
        zkClient.delete(lockPath, -1);
        zkClient.close();
        System.out.println(" 锁释放：" + lockPath);
    }
}
