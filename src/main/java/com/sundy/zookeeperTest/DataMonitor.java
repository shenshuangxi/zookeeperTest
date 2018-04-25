package com.sundy.zookeeperTest;

import java.util.Arrays;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DataMonitor implements StatCallback {

	private ZooKeeper zooKeeper;
	private String znode;
	private Watcher watcher;
	private DataMonitorListener dataMonitorListener;
	public boolean dead;
	private byte[] prevData;
	
	
	
	public DataMonitor(ZooKeeper zooKeeper, String znode, Watcher watcher,DataMonitorListener dataMonitorListener) {
		this.znode = znode;
		this.zooKeeper = zooKeeper;
		this.watcher = watcher;
		this.dataMonitorListener = dataMonitorListener;
		zooKeeper.exists(znode, true, this, null);
	}

	public interface DataMonitorListener {

		void exists(byte[] data);
		
		void closing(int rc);
		
	}

	@SuppressWarnings("deprecation")
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		boolean exists;
		switch (rc) {
		case Code.Ok:
			exists = true;
			break;
		case Code.NoNode:
			exists = false;
			break;
		case Code.SessionExpired:
		case Code.NoAuth:
			dead = true;
			dataMonitorListener.closing(rc);
			return;
		default:
			zooKeeper.exists(path, true, this, null);
			return;
		}
		
		byte b[] = null;
		if(exists){
			try {
				b = zooKeeper.getData(path, false, null);
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
		}
		if((b==null&&b!=prevData)||(b!=null&&!Arrays.equals(prevData, b))){
			dataMonitorListener.exists(b);
			prevData = b;
		}
		
	}

	@SuppressWarnings({ "incomplete-switch", "deprecation" })
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if(event.getType()==EventType.None){
			switch (event.getState()) {
			case SyncConnected:
				break;
			case Expired:
				dead = true;
				dataMonitorListener. closing(KeeperException.Code.SessionExpired);
				break;
			}
		}else{
			if(path!=null&&path.equals(znode)){
				zooKeeper.exists(path, true, this, null);
			}
		}
		if(watcher!=null){
			watcher.process(event);
		}
		
	}
}
