package com.zhss.dfs.client;

import com.zhss.dfs.namenode.rpc.model.MkdirRequest;
import com.zhss.dfs.namenode.rpc.model.MkdirResponse;
import com.zhss.dfs.namenode.rpc.model.ShutdownRequest;

import com.zhss.dfs.namenode.rpc.service.NameNodeServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * 文件系统客户端的实现类
 * @author zhonghuashishan
 *
 */
public class FileSystemImpl implements FileSystem {

	private static final String NAMENODE_HOSTNAME = "localhost";
	private static final Integer NAMENODE_PORT = 50070;
	
	private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;
	
	public FileSystemImpl() {
		ManagedChannel channel = NettyChannelBuilder
				.forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
				.negotiationType(NegotiationType.PLAINTEXT)
				.build();
		this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
	}
	
	/**
	 * 创建目录
	 */
	@Override
	public void mkdir(String path) throws Exception {
		MkdirRequest request = MkdirRequest.newBuilder()
				.setPath(path)
				.build();
		
		MkdirResponse response = namenode.mkdir(request);
		// 网络那块知识大家应该都懂了
		// NIO和网络编程底层的一些知识大家懂了
		// 用屁股想想都知道他底层原理
		// 一定是跟NameNode指定的端口通过Socket的方式建立TCP连接
		// 然后呢按照protobuf的协议，把数据按照一定的格式进行封装，TCP包
		// 就会通过底层的以太网，传输出去到NameNode那儿去
		// 人家就会从TCP包里获取到数据，基于protobuf协议的标准提取数据，交给你的代码来处理
		
		System.out.println("创建目录的响应：" + response.getStatus());  
	}

	/**
	 * 优雅关闭
	 */
	@Override
	public void shutdown() throws Exception {
		ShutdownRequest request = ShutdownRequest.newBuilder()
				.setCode(1)
				.build();
		namenode.shutdown(request);
	}

}
