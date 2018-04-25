package com.sundy.zookeeperTest;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Executor implements Watcher, Runnable, DataMonitor.DataMonitorListener {

	private String filename;
	private String[] exec;
	private ZooKeeper zooKeeper;
	private DataMonitor dataMonitor;
	private Process child;
	
	public Executor(String hostPort, String znode, String filename,String[] exec) throws IOException {
		this.filename = filename;
		this.exec = exec;
		this.zooKeeper = new ZooKeeper(hostPort, 3000, this);
		this.dataMonitor = new DataMonitor(zooKeeper,znode,null,this);
	}

	public static void main(String[] args) {
		
		String hostPort = "192.168.152.128:2181";
		String znode = "/zk";
		String filename = "zookeeper";
		try {
			new Executor(hostPort, znode,filename,new String[]{"sh.bat"}).run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run() {
		try {
			synchronized (this) {
				while(!dataMonitor.dead){
					wait();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void process(WatchedEvent event) {
		this.dataMonitor.process(event);
	}

	public void exists(byte[] data) {
		if (data == null) {
            if (child != null) {
                System.out.println("Killing process");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                }
            }
            child = null;
        } else {
            if (child != null) {
                System.out.println("Stopping child");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                FileOutputStream fos = new FileOutputStream(filename);
                fos.write(data);
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                System.out.println("Starting child");
                child = Runtime.getRuntime().exec(exec);
                new StreamWriter(child.getInputStream(), System.out);
                new StreamWriter(child.getErrorStream(), System.err);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
	}

	public void closing(int rc) {
		synchronized (this) {
			notifyAll();
		}
	}
	
	static class StreamWriter extends Thread{
		OutputStream os;
		InputStream is;
		
		public StreamWriter(InputStream is, OutputStream os) {
			this.is = is;
			this.os = os;
			start();
		}
		
		public void run(){
			byte[] b = new byte[80];
			int rc;
			try {
				while ((rc=is.read(b))>0) {
					os.write(b, 0, rc);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

}
