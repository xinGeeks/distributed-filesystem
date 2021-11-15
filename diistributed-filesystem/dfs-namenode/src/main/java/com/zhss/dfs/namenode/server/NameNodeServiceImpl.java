package com.zhss.dfs.namenode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhss.dfs.namenode.rpc.model.*;
import com.zhss.dfs.namenode.rpc.service.*;

import io.grpc.stub.StreamObserver;

import java.util.List;

/**
 * NameNode的rpc服务的接口
 * @author zhonghuashishan
 *
 */
public class NameNodeServiceImpl implements NameNodeServiceGrpc.NameNodeService {

	public static final Integer STATUS_SUCCESS = 1;
	public static final Integer STATUS_FAILURE = 2;
	public static final Integer STATUS_SHUTDOWN = 3;
	public static final Integer BACKUP_NODE_FETCH_SIZE = 10;
	
	/**
	 * 负责管理元数据的核心组件
	 */
	// 他是一个逻辑上的组件，主要是负责管理元数据的更新
	// 比如说你要更新内存里的文件目录树的话，就可以去找他，他更新的就是元数据
	private FSNamesystem namesystem; 
	/**
	 * 负责管理集群中所有的datanode的组件
	 */
	private DataNodeManager datanodeManager;
	/**
	 * 是否还在运行
	 */
	private volatile Boolean isRunning = true;

	/**
	 * 当前backupNode节点同步到了哪一条
	 */
	private long backupSyncTxid = 0L;

	/**
	 * 当前缓冲的editslog
	 */
	private JSONArray currentBufferedEditsLog = new JSONArray();
	
	public NameNodeServiceImpl(
			FSNamesystem namesystem, 
			DataNodeManager datanodeManager) {
		this.namesystem = namesystem;
		this.datanodeManager = datanodeManager;
	}

	/**
	 * datanode进行注册
	 * @param request
	 * @param responseObserver
	 * @return
	 * @throws Exception
	 */
	@Override
	public void register(RegisterRequest request, 
			StreamObserver<RegisterResponse> responseObserver) {
		datanodeManager.register(request.getIp(), request.getHostname());
		
		RegisterResponse response = RegisterResponse.newBuilder()
				.setStatus(STATUS_SUCCESS)
				.build();
	
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * datanode进行心跳
	 * @param request
	 * @param responseObserver
	 * @return
	 * @throws Exception
	 */
	@Override
	public void heartbeat(HeartbeatRequest request, 
			StreamObserver<HeartbeatResponse> responseObserver) {
		datanodeManager.heartbeat(request.getIp(), request.getHostname());
		
		HeartbeatResponse response = HeartbeatResponse.newBuilder()
				.setStatus(STATUS_SUCCESS)
				.build();
	
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * 创建目录
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void mkdir(MkdirRequest request, StreamObserver<MkdirResponse> responseObserver) {
		try {
			MkdirResponse response = null;
			
			if(!isRunning) {
				response = MkdirResponse.newBuilder()
						.setStatus(STATUS_SHUTDOWN)
						.build();
			} else {
				this.namesystem.mkdir(request.getPath());
				
				response = MkdirResponse.newBuilder()
						.setStatus(STATUS_SUCCESS)
						.build();
			}
			
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace(); 
		}
	}

	/**
	 * 优雅关闭
	 */
	public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
		this.isRunning = false;
		this.namesystem.flush();  
	}

	/**
	 * 拉取edits log
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void fetchEditsLog(FetchEditsLogRequest request, StreamObserver<FetchEditsLogResponse> responseObserver) {
		FetchEditsLogResponse response = null;
		JSONArray fecthedEditLog = new JSONArray();

		FSEditlog fsEditlog = namesystem.getEditlog();
		List<String> editlog = fsEditlog.getFlushedTxid();

		if (editlog.isEmpty()) {
			if (backupSyncTxid != 0) {
				// 清理掉上次的缓存
				currentBufferedEditsLog.clear();

				// 拉取缓存中的数据
				String[] bufferedEditsLog = fsEditlog.getBufferedEditsLog();
				for (String log : bufferedEditsLog) {
					currentBufferedEditsLog.add(JSONObject.parseObject(log));
				}

				int fetchCount = 0;
				for (int i = 0; i < currentBufferedEditsLog.size(); i++) {
					if (currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
						fecthedEditLog.add(currentBufferedEditsLog.getJSONObject(i));
						backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
						fetchCount++;
					}
					if (fetchCount == backupSyncTxid) {
						break;
					}
				}
			} else {
				// 此时数据全在内存缓冲中
				String[] bufferedEditsLog = fsEditlog.getBufferedEditsLog();
				for (String log : bufferedEditsLog) {
					currentBufferedEditsLog.add(JSONObject.parseObject(log));
				}

				// 从内存缓冲中的数据获取
				int fetchSize = Math.min(BACKUP_NODE_FETCH_SIZE, currentBufferedEditsLog.size());
				for (int i = 0; i < fetchSize; i++) {
					fecthedEditLog.add(currentBufferedEditsLog.getJSONObject(i));
					backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
				}
			}
		}
		response = FetchEditsLogResponse.newBuilder().setEditsLog(fecthedEditLog.toJSONString()).build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();

	}

}
