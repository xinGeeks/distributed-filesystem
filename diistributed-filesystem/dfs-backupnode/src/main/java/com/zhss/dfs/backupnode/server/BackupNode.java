package com.zhss.dfs.backupnode.server;

/**
 * @author SemperFi
 * @Title: null.java
 * @Package diistributed-filesystem
 * @Description:
 * @date 2021-11-14 17:32
 */
public class BackupNode {

    private volatile Boolean isRunning = true;

    public static void main(String[] args) throws InterruptedException {
        BackupNode backupNode = new BackupNode();
        backupNode.start();
    }

    public void start() throws InterruptedException {
        EditsLogFetcher editsLogFetcher = new EditsLogFetcher(this);
        editsLogFetcher.start();
    }

    public void run() throws InterruptedException {
        while (isRunning) {
            Thread.sleep(1000);
        }
    }

    public Boolean isRunning(){
        return isRunning;
    }

}
