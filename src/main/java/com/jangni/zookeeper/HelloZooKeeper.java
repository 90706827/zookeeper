package com.jangni.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.AbstractRefreshableApplicationContext;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName HelloZooKeeper
 * @Description
 * @Author Mr.Jangni
 * @Date 2019/4/26 21:31
 * @Version 1.0
 **/

public class HelloZooKeeper {

    static Logger logger = LoggerFactory.getLogger(HelloZooKeeper.class);
    public static void main(String[] args) throws IOException {

        Properties config = new Properties();
        config.load(ClassLoader.getSystemResourceAsStream("zk-client.properties"));
        //以逗号分隔的主机:端口号列表
        String connectString = config.getProperty("zk.connectString");
        if (connectString == null) {
            throw new NullPointerException("'zk.connectString' cant be empty.. ");
        }
        //以毫秒为单位的会话超时时间
        int sessionTimeout = Integer.parseInt(config.getProperty("zk.sessionTimeout", "-1"));


        //允许创建的客户端在网络分区的情况下进入只读模式
        boolean canBeReadOnly = Boolean.getBoolean(config.getProperty("zk.canBeReadOnly", "true"));
        //在客户端重新连接到ZooKeeper服务器的情况下，可以使用特定的会话ID来引用先前连接的会话
        long sessionId = 100;
        //如果指定的会话需要密码，可以在这里指定
        byte[] sessionPasswd = new byte[]{};
        String zpath = "/";
        List<String> zooChildren = new ArrayList<String>();
        System.setProperty("zookeeper.sasl.client", config.getProperty("zk.sasl"));
        ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, null);

        zk.getSaslClient();


        try {
            String auth = "admin:admin";
            //anywhere,but before znode operation
            //can addauth more than once
            zk.addAuthInfo("digest", auth.getBytes("UTF-8"));
            Id id = new Id("digest", DigestAuthenticationProvider.generateDigest(auth));
            ACL acl = new ACL(ZooDefs.Perms.ALL, id);
            List<ACL> acls = Collections.singletonList(acl);
            //如果不需要访问控制,可以使用acls = ZooDefs.Ids.OPEN_ACL_UNSAFE
            zk.create("/singleWorker", null, acls, CreateMode.PERSISTENT);

            if (zk.exists("/test", false) == null) {
                zk.create("/test", "znode1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            logger.info("=============查看节点是否安装成功===============");
            logger.info(new String(zk.getData("/test", false, null)));

            logger.info("=========修改节点的数据==========");
            zk.setData("/test", "zNode2".getBytes(), -1);
            logger.info("========查看修改的节点是否成功=========");
            logger.info(new String(zk.getData("/test", false, null)));

            logger.info("=======删除节点==========");
            zk.delete("/test", -1);
            logger.info("==========查看节点是否被删除============");
            logger.info("节点状态：" + zk.exists("/test", false));
            zk.close();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

    }
}