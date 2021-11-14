package com.zhss.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;

/**
 * @author SemperFi
 * @Title: null.java
 * @Package diistributed-filesystem
 * @Description: 从namenode同步edits log
 * @date 2021-11-14 17:31
 */
public class EditsLogFetcher extends Thread {

    private BackupNode backupNode;

    private NameNodeRpcClient nameNodeRpcClient;

    public EditsLogFetcher(BackupNode backupNode) {
        this.backupNode = backupNode;
        this.nameNodeRpcClient = new NameNodeRpcClient();
    }

    @Override
    public void run() {
        while (backupNode.isRunning()) {
            JSONArray editsLog = nameNodeRpcClient.fetchEditsLog();
        }
    }


}
