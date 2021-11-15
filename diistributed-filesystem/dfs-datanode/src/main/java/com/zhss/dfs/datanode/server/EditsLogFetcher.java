package com.zhss.dfs.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 从namenode同步editslog的组件
 * @author zhonghuashishan
 *
 */
public class EditsLogFetcher extends Thread {

	private BackupNode backupNode;
	private NameNodeRpcClient namenode;
	private FSNamesystem namesystem;
	
	public EditsLogFetcher(BackupNode backupNode, FSNamesystem namesystem) {
		this.backupNode = backupNode;
		this.namenode = new NameNodeRpcClient();
		this.namesystem = namesystem;
	}
	
	@Override
	public void run() {
		while(backupNode.isRunning()) {  
			JSONArray editsLogs = namenode.fetchEditsLog();
			for(int i = 0; i < editsLogs.size(); i++) {
				JSONObject editsLog = editsLogs.getJSONObject(i);
				String op = editsLog.getString("OP");
				
				if(op.equals("MKDIR")) {
					String path = editsLog.getString("PATH"); 
					try {
						namesystem.mkdir(path);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
}
