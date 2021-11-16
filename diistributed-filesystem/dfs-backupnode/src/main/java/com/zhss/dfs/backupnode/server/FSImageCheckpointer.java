package com.zhss.dfs.backupnode.server;

/**
 * fsimage文件的checkpoint组件
 * @author zhonghuashishan
 *
 */
public class FSImageCheckpointer extends Thread {
	
	/**
	 * checkpoint操作的时间间隔
	 */
	public static final Integer CHECKPOINT_INTERVAL = 1 * 60 * 60 * 1000;

	private BackupNode backupNode;
	private FSNamesystem namesystem;
	
	public FSImageCheckpointer(BackupNode backupNode, FSNamesystem namesystem) {
		this.backupNode = backupNode;
		this.namesystem = namesystem;
	}
	
	@Override
	public void run() {
		System.out.println("fsimage checkpoint定时调度线程启动......");  
		
		while(backupNode.isRunning()) {
			try {
				Thread.sleep(CHECKPOINT_INTERVAL);
				
				// 就可以触发这个checkpoint操作，去把内存里的数据写入磁盘就可以了
			} catch (Exception e) {
				e.printStackTrace(); 
			}
		}
	}
	
}
