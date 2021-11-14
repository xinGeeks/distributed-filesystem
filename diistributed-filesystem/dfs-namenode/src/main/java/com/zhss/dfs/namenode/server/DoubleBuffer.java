package com.zhss.dfs.namenode.server;

/**
 * @author SemperFi
 * @Title: null.java
 * @Package diistributed-filesystem
 * @Description:
 * @date 2021-11-08 22:24
 */

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

/**
 * 内存双缓冲
 * @author zhonghuashishan
 *
 */
class DoubleBuffer {

    /**
     * 单块editlog缓冲区的最大值 默认512字节
     */
    public static final Integer EDIT_LOG_BUFFER_LIMIT = 512 * 1024;


    /**
     * 是专门用来承载线程写入edits log
     */
    private EditLogBuffer currentBuffer = new EditLogBuffer();
    /**
     * 专门用来将数据同步到磁盘中去的一块缓冲
     */
    private EditLogBuffer syncBuffer = new EditLogBuffer();


    /**
     * 将edits log写到内存缓冲里去
     * @param log
     */
    public void write(EditLog log) throws IOException {
        currentBuffer.write(log);
    }

    /**
     * 交换两块缓冲区，为了同步内存数据到磁盘做准备
     */
    public void setReadyToSync() {
        EditLogBuffer tmp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = tmp;
    }

    /**
     * 将syncBuffer缓冲区中的数据刷入磁盘中
     */
    public void flush() throws IOException {
        syncBuffer.flush();
        syncBuffer.clear();
    }

    /**
     * 是否将缓冲区中的内容刷入磁盘
     * @return
     */
    public boolean shouldSyncToDisk() {
         return currentBuffer.size() >= EDIT_LOG_BUFFER_LIMIT;
    }

    class EditLogBuffer {

        /**
         * 内存缓冲区字节数组IO流
         */
        ByteArrayOutputStream buffer;

        /**
         * 磁盘上的editslog日志文件的channel
         */
        FileChannel editsLogFileChannel;

        /**
         * 当前缓冲区写入的最大的txid
         */
        long maxTxid = 0L;

        long lastMaxTxid = 0L;

        public EditLogBuffer() {
            this.buffer = new ByteArrayOutputStream(EDIT_LOG_BUFFER_LIMIT);
        }

        public void write(EditLog log) throws IOException {
            this.maxTxid = log.getTxid();
            buffer.write(log.getContent().getBytes());
            buffer.write("\n".getBytes());
            System.out.println("在 currentBuffer 中写入一条数据： " + log.getContent());
        }

        // 获取当前缓冲区大小
        public Integer size() {
            return buffer.size();
        }

        /**
         * 将 sync buffer中的数据刷入磁盘中
         */
        public void flush() throws IOException {
            byte[] data = buffer.toByteArray();
            ByteBuffer wrap = ByteBuffer.wrap(data);
            String editLogPath = "G:" + File.separator + "temp" + File.separator + "editslog" + File.separator + "edits-" + (++lastMaxTxid) + "-"  + maxTxid + ".log";

            try(RandomAccessFile randomAccessFile = new RandomAccessFile(editLogPath, "rw");
                FileOutputStream fileOutputStream = new FileOutputStream(randomAccessFile.getFD())) {
                this.editsLogFileChannel = fileOutputStream.getChannel();
                editsLogFileChannel.write(wrap);
                // 强制把数据刷入磁盘上
                editsLogFileChannel.force(false);
            }

            this.lastMaxTxid = maxTxid;
        }

        /**
         * 清空掉内存缓冲中的数据
         */
        public void clear() {
            buffer.reset();
        }
    }

}
