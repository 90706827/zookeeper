package com.jangni.zookeeper;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @ClassName TicketSeller
 * @Description 客户端使用分布式锁
 * @Author Mr.Jangni
 * @Date 2019/4/29 17:27
 * @Version 1.0
 **/
public class TicketSeller {
    /**
     * 不带锁的业务逻辑方法
     */
    private void sell(){
        System.out.println("售票开始");
        // 线程随机休眠数毫秒，模拟现实中的费时操作
        int sleepMillis = (int) (Math.random() * 2000);
        try {
            //为了演示，sleep了一段时间
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("售票结束");
    }

    /**
     * 加锁后执行业务逻辑
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     */
    public void sellTicketWithLock() throws KeeperException, InterruptedException, IOException {
        LockSample lock = new LockSample();
        lock.acquireLock();
        sell();
        lock.releaseLock();
    }


    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        TicketSeller ticketSeller = new TicketSeller();
        for(int i=0;i<1000;i++){
            ticketSeller.sellTicketWithLock();
        }
    }
}
