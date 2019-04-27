package com.jangni.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName SessionWatcher
 * @Description
 * @Author Mr.Jangni
 * @Date 2019/4/27 14:20
 * @Version 1.0
 **/
public class ZkClient {
    static Logger logger = LoggerFactory.getLogger(ZkClient.class);
    private String connectString;
    private Integer sessionTimeout;
    private Object waiter = new Object();//simple object-lock
    private ZooKeeper zooKeeper;

    //"zk-client.properties";//classpath下
    public ZkClient(String configLocation) throws Exception {
        Properties config = new Properties();
        config.load(ClassLoader.getSystemResourceAsStream(configLocation));
        connectString = config.getProperty("zk.connectString");
        if (connectString == null) {
            throw new NullPointerException("'zk.connectString' cant be empty.. ");
        }
        sessionTimeout = Integer.parseInt(config.getProperty("zk.sessionTimeout", "-1"));
        connectZK();
    }

    /**
     * core method,启动zk服务 本实例基于自动重连策略,如果zk连接没有建立成功或者在运行时断开,将会自动重连.
     */
    private void connectZK() {
        synchronized (waiter) {
            try {
                SessionWatcher watcher = new SessionWatcher();
                // session的构建是异步的
                this.zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher, false);


            } catch (Exception e) {
                e.printStackTrace();
            }


            try { //授权与验证
                String auth = "admin:admin";
                //anywhere,but before znode operation
                //can addauth more than once
                zooKeeper.addAuthInfo("digest", auth.getBytes("UTF-8"));
                Id id = new Id("digest", DigestAuthenticationProvider.generateDigest(auth));
                ACL acl = new ACL(ZooDefs.Perms.ALL, id);
                List<ACL> acls = Collections.singletonList(acl);
                //如果不需要访问控制,可以使用acls = ZooDefs.Ids.OPEN_ACL_UNSAFE
                zooKeeper.create("/singleWorker", null, acls, CreateMode.PERSISTENT);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (KeeperException.NodeExistsException e) {
                e.printStackTrace();
            }catch (KeeperException e){
                e.printStackTrace();
            }

            waiter.notifyAll();
        }
    }

    public static void main(String[] args) {
        try {
            ZkClient zkClient = new ZkClient("zk-client.properties");
            zkClient.connectZK();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        try {
            synchronized (waiter) {
                if (this.zooKeeper != null) {
                    zooKeeper.close();
                }
                waiter.notifyAll();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class SessionWatcher implements Watcher {

        public void process(WatchedEvent event) {
            // 如果是“数据变更”事件
            if (event.getType() != Event.EventType.None) {
                return;
            }

            // 如果是链接状态迁移
            // 参见keeperState
            synchronized (waiter) {
                switch (event.getState()) {
                    // zk连接建立成功,或者重连成功
                    case SyncConnected:
                        logger.info("Connected...");
                        waiter.notifyAll();
                        break;
                    // session过期,这是个非常严重的问题,有可能client端出现了问题,也有可能zk环境故障
                    // 此处仅仅是重新实例化zk client
                    case Expired:
                        logger.info("Expired...");
                        // 重连
                        connectZK();
                        break;
                    // session过期
                    case Disconnected:
                        // 链接断开，或session迁移
                        logger.info("Connecting....");
                        break;
                    case AuthFailed:
                        close();
                        throw new RuntimeException("ZK Connection auth failed...");
                    default:
                        break;
                }
            }
        }
    }

}