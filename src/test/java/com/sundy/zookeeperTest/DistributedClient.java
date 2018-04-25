package com.sundy.zookeeperTest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DistributedClient {

	private static final int SESSION_TIMEOUT = 5000;
	private static final String hostPort = "192.168.152.128:2181";
	private static final String groupNode = "locks";
	private static final String subNode = "sub";
	
	private ZooKeeper zooKeeper;
	private String thisPath;
	private String waitPath;
	
	private CountDownLatch latch = new CountDownLatch(1);
	
	public void connectZookeeper() throws IOException, InterruptedException, KeeperException{
		this.zooKeeper = new ZooKeeper(hostPort, SESSION_TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				if(event.getState()==KeeperState.SyncConnected){
					latch.countDown();
				}
				//判断是否有节点删除，并且该节点是其等待的节点
				// 发生了waitPath的删除事件  
				if (event.getType() == EventType.NodeDeleted && event.getPath().equals(waitPath)) {  
				    // 确认thisPath是否真的是列表中的最小节点  
				    List<String> childrenNodes = null;
					try {
						childrenNodes = zooKeeper.getChildren("/" + groupNode, false);
					} catch (KeeperException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}  
				    String thisNode = thisPath.substring(("/" + groupNode + "/").length());  
				    // 排序  
				    Collections.sort(childrenNodes);  
				    int index = childrenNodes.indexOf(thisNode);  
				    if (index == 0) {  
				        // 确实是最小节点  
				        try {
							doSomething();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (KeeperException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}  
				    } else {  
				        // 说明waitPath是由于出现异常而挂掉的  
				        // 更新waitPath  
				        waitPath = "/" + groupNode + "/" + childrenNodes.get(index - 1);  
				        // 重新注册监听, 并判断此时waitPath是否已删除  
				        try {
							if (zooKeeper.exists(waitPath, true) == null) {  
							    try {
									doSomething();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (KeeperException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}  
							}
						} catch (KeeperException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}  
				    }  
				}  
			}
		});
		latch.await();
		this.thisPath = zooKeeper.create("/"+groupNode+"/"+subNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		Thread.sleep(1000);
		List<String> childrenNodes = zooKeeper.getChildren("/"+groupNode, false);
		if(childrenNodes.size()==1){
			doSomething();
		}else{
			 String thisNode = thisPath.substring(("/" + groupNode + "/").length()); 
			 Collections.sort(childrenNodes);  
	            int index = childrenNodes.indexOf(thisNode);  
	            if (index == -1) {  
	                // never happened  
	            } else if (index == 0) {  
	                // inddx == 0, 说明thisNode在列表中最小, 当前client获得锁  
	                doSomething();  
	            } else {  
	                // 获得排名比thisPath前1位的节点  
	                this.waitPath = "/" + groupNode + "/" + childrenNodes.get(index - 1);  
	                // 在waitPath上注册监听器, 当waitPath被删除时, zookeeper会回调监听器的process方法  
	                zooKeeper.getData(waitPath, true, new Stat());  
	            } 
		}
		
	}
	
	private void doSomething() throws InterruptedException, KeeperException {
		try {  
            System.out.println("gain lock: " + thisPath);  
            Thread.sleep(2000);  
            // do something  
        } finally {  
            System.out.println("finished: " + thisPath);  
            // 将thisPath删除, 监听thisPath的client将获得通知  
            // 相当于释放锁  
            zooKeeper.delete(this.thisPath, -1);  
        }
	}
	
	
}
