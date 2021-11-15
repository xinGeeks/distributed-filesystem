package com.zhss.dfs.datanode.server;

/**
 * 负责同步editslog的进程
 * @author zhonghuashishan
 *
 */
public class BackupNode {

	private volatile Boolean isRunning = true;
	private FSNamesystem namesystem;
	
	public static void main(String[] args) throws Exception {
		BackupNode backupNode = new BackupNode();
		backupNode.init();  
		backupNode.start();
	}
	
	public void init() {
		this.namesystem = new FSNamesystem();
	}
	
	public void start() throws Exception {
		EditsLogFetcher editsLogFetcher = new EditsLogFetcher(this, namesystem);   
		editsLogFetcher.start();
	}
	
	public void run() throws Exception {
		while(isRunning) {
			Thread.sleep(1000);  
		}  
	}
	
	public Boolean isRunning() {
		return isRunning;
	}
	
}
