package com.zhss.dfs.namenode.server;

/**
 * @author SemperFi
 * @Title: null.java
 * @Package diistributed-filesystem
 * @Description:
 * @date 2021-11-08 22:24
 */

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
    public static final Long EDIT_LOG_BUFFER_LIMIT = 512 * 1024L;


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
    public void write(EditLog log) {
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
    public void flush() {
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

        public void write(EditLog log) {
            System.out.println("在 currentBuffer 中写入一条数据： " + log.getContent());
        }

        // 获取当前缓冲区大小
        public Long size() {
            return 0L;
        }

        public void flush() {

        }

        public void clear() {

        }
    }

}
