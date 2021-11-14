package com.zhss.dfs.namenode.server;

/**
 * @author SemperFi
 * @Title: null.java
 * @Package diistributed-filesystem
 * @Description:
 * @date 2021-11-08 22:25
 */

import com.alibaba.fastjson.JSONObject;

/**
 * 代表了一条edits log
 * @author zhonghuashishan
 *
 */
class EditLog {

    private long txid;
    private String content;

    public EditLog(long txid, String content) {
        this.txid = txid;
        this.content = content;
    }

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txid) {
        this.txid = txid;
        JSONObject jsonObject = JSONObject.parseObject(content);
        jsonObject.put("txid", txid);
        this.content = jsonObject.toJSONString();
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}