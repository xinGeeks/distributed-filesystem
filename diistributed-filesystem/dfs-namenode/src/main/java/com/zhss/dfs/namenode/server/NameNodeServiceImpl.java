package com.zhss.dfs.namenode.server;

import com.zhss.dfs.namenode.rpc.model.HeartbeatRequest;
import com.zhss.dfs.namenode.rpc.model.HeartbeatResponse;
import com.zhss.dfs.namenode.rpc.model.RegisterRequest;
import com.zhss.dfs.namenode.rpc.model.RegisterResponse;
import com.zhss.dfs.namenode.rpc.service.*;

import io.grpc.stub.StreamObserver;

/**
 * NameNode的rpc服务的接口
 * @author zhonghuashishan
 *
 */
public class NameNodeServiceImpl implements NameNodeServiceGrpc.NameNodeService {

	public static final Integer STATUS_SUCCESS = 1;
	public static final Integer STATUS_FAILURE = 2;
	
	/**
	 * 负责管理元数据的核心组件
	 */
	private FSNamesystem namesystem;
	/**
	 * 负责管理集群中所有的datanode的组件
	 */
	private DataNodeManager datanodeManager;
	
	public NameNodeServiceImpl(
			FSNamesystem namesystem, 
			DataNodeManager datanodeManager) {
		this.namesystem = namesystem;
		this.datanodeManager = datanodeManager;
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否创建成功
	 * @throws Exception
	 */
	public Boolean mkdir(String path) throws Exception {
		return this.namesystem.mkdir(path);
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
	
}
