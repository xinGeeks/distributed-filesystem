package com.zhss.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.zhss.dfs.namenode.rpc.model.FetchEditsLogRequest;
import com.zhss.dfs.namenode.rpc.model.FetchEditsLogResponse;
import com.zhss.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * @author SemperFi
 * @Title: null.java
 * @Package diistributed-filesystem
 * @Description:
 * @date 2021-11-14 17:27
 */
public class NameNodeRpcClient {

    private static final String NAMENODE_HOSTNAME = "localhost";

    private static final Integer NAMENODE_PORT = 50070;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public NameNodeRpcClient() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    public JSONArray fetchEditsLog() {
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setCode(1)
                .build();
        FetchEditsLogResponse fetchEditsLogResponse = namenode.fetchEditsLog(request);
        String editsLog = fetchEditsLogResponse.getEditsLog();

        return JSONArray.parseArray(editsLog);
    }
}
