package com.zhss.dfs.namenode.server;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhss.dfs.namenode.rpc.model.FetchEditsLogRequest;
import com.zhss.dfs.namenode.rpc.model.FetchEditsLogResponse;
import com.zhss.dfs.namenode.rpc.model.HeartbeatRequest;
import com.zhss.dfs.namenode.rpc.model.HeartbeatResponse;
import com.zhss.dfs.namenode.rpc.model.MkdirRequest;
import com.zhss.dfs.namenode.rpc.model.MkdirResponse;
import com.zhss.dfs.namenode.rpc.model.RegisterRequest;
import com.zhss.dfs.namenode.rpc.model.RegisterResponse;
import com.zhss.dfs.namenode.rpc.model.ShutdownRequest;
import com.zhss.dfs.namenode.rpc.model.ShutdownResponse;
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
	 * 当前backupNode节点同步到了哪一条txid了
	 */
	private long backupSyncTxid = 0L;
	/**
	 * 当前缓冲的一小部分editslog
	 */
	private JSONArray currentBufferedEditsLog = new JSONArray();
	/**
	 * 当前内存里缓冲了哪个磁盘文件的数据
	 */
	private String bufferedFlushedTxid;
	
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
	@Override
	public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
		this.isRunning = false;
		this.namesystem.flush();  
	}

	/**
	 * 拉取editslog
	 */
	@Override
	public void fetchEditsLog(FetchEditsLogRequest request, StreamObserver<FetchEditsLogResponse> responseObserver) {
		FetchEditsLogResponse response = null;
		JSONArray fetchedEditsLog = new JSONArray();
		
		List<String> flushedTxids = namesystem.getEditsLog().getFlushedTxids();
		
		if(flushedTxids.size() == 0) {
			if(backupSyncTxid != 0) {
				currentBufferedEditsLog.clear();
				
				String[] bufferedEditsLog = namesystem.getEditsLog().getBufferedEditsLog();
				for(String editsLog : bufferedEditsLog) {
					currentBufferedEditsLog.add(JSONObject.parseObject(editsLog));
				}
				
				int fetchCount = 0;
				
				for(int i = 0; i < currentBufferedEditsLog.size(); i++) {
					if(currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
						fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
						backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid"); 
						fetchCount++;
					}
					if(fetchCount == BACKUP_NODE_FETCH_SIZE) {
						break;
					}
				}
			} else {
				// 此时数据全部都存在于内存缓冲里
				String[] bufferedEditsLog = namesystem.getEditsLog().getBufferedEditsLog();
				for(String editsLog : bufferedEditsLog) {
					currentBufferedEditsLog.add(JSONObject.parseObject(editsLog));
				}
				
				// 此时就可以从刚刚内存缓冲里的数据开始取数据了
				int fetchSize = Math.min(BACKUP_NODE_FETCH_SIZE, currentBufferedEditsLog.size());
				
				for(int i = 0; i < fetchSize; i++) {
					fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));    
					backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid"); 
				}
				
			}
		} else {
			// 第一种情况，你要拉取的txid是在某个磁盘文件里的
			if(bufferedFlushedTxid != null) {
				String[] flushedTxidSplited = bufferedFlushedTxid.split("_");    
				
				long startTxid = Long.valueOf(flushedTxidSplited[0]);
				long endTxid = Long.valueOf(flushedTxidSplited[1]);  
				long fetchBeginTxid = backupSyncTxid + 1;
				
				if(fetchBeginTxid >= startTxid && fetchBeginTxid <= endTxid) {
					int fetchCount = 0;
					
					for(int i = 0; i < currentBufferedEditsLog.size(); i++) {
						if(currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
							fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
							backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid"); 
							fetchCount++;
						}  
						if(fetchCount == BACKUP_NODE_FETCH_SIZE) {
							break;
						}
					}
				} else {
					String nextFlushedTxid = null;
					
					for(int i = 0; i < flushedTxids.size(); i++) {  
						if(flushedTxids.get(i).equals(bufferedFlushedTxid)) {
							if(i + 1 < flushedTxids.size()) {
								nextFlushedTxid = flushedTxids.get(i + 1);  
							}
						}
					}
					
					if(nextFlushedTxid != null) {
						bufferedFlushedTxid = nextFlushedTxid;
						
						flushedTxidSplited = nextFlushedTxid.split("_");    
						startTxid = Long.valueOf(flushedTxidSplited[0]);
						endTxid = Long.valueOf(flushedTxidSplited[1]);  
						
						String currentEditsLogFile = "F:\\development\\editslog\\edits-" 
								+ startTxid + "-" + endTxid + ".log";
						
						try {
							currentBufferedEditsLog.clear();
							List<String> editsLogs = Files.readAllLines(Paths.get(currentEditsLogFile));
							for(String editsLog : editsLogs) {
								currentBufferedEditsLog.add(JSONObject.parseObject(editsLog)); 
							}
							
							int fetchCount = 0;
							
							for(int i = 0; i < currentBufferedEditsLog.size(); i++) {
								if(currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
									fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
									backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid"); 
									fetchCount++;
								}
								if(fetchCount == BACKUP_NODE_FETCH_SIZE) {
									break;
								}
							}
						} catch (Exception e) {
							e.printStackTrace();  
						}
					} else {
						// 如果没有找到下一个文件，此时就需要从内存里去继续读取
						bufferedFlushedTxid = null;
						currentBufferedEditsLog.clear();
						
						String[] bufferedEditsLog = namesystem.getEditsLog().getBufferedEditsLog();
						for(String editsLog : bufferedEditsLog) {
							currentBufferedEditsLog.add(JSONObject.parseObject(editsLog));
						}
						
						int fetchCount = 0;
						
						for(int i = 0; i < currentBufferedEditsLog.size(); i++) {
							if(currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
								fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
								backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid"); 
								fetchCount++;
							}
							if(fetchCount == BACKUP_NODE_FETCH_SIZE) {
								break;
							}
						}
					}
				}
			}
			
			for(String flushedTxid : flushedTxids) {
				String[] flushedTxidSplited = flushedTxid.split("_");    
				
				long startTxid = Long.valueOf(flushedTxidSplited[0]);
				long endTxid = Long.valueOf(flushedTxidSplited[1]);  
				long fetchBeginTxid = backupSyncTxid + 1;
				
				if(fetchBeginTxid >= startTxid && fetchBeginTxid <= endTxid) {
					// 此时可以把这个磁盘文件里以及下一个磁盘文件的的数据都读取出来，放到内存里来缓存
					// 就怕一个磁盘文件的数据不足够10条
					bufferedFlushedTxid = flushedTxid;
					
					currentBufferedEditsLog.clear();
					
					try {
						String currentEditsLogFile = "F:\\development\\editslog\\edits-" 
								+ startTxid + "-" + endTxid + ".log";
						
						List<String> editsLogs = Files.readAllLines(Paths.get(currentEditsLogFile));
						for(String editsLog : editsLogs) {
							currentBufferedEditsLog.add(JSONObject.parseObject(editsLog)); 
						}
						
						int fetchCount = 0;
						
						for(int i = 0; i < currentBufferedEditsLog.size(); i++) {
							if(currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
								fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
								backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid"); 
								fetchCount++;
							}
							if(fetchCount == BACKUP_NODE_FETCH_SIZE) {
								break;
							}
						}
					} catch (Exception e) {
						e.printStackTrace();  
					}
					
					break;
				}
			}
			
			// 第二种情况，你要拉取的txid已经比磁盘文件里的全部都新了，还在内存缓冲里
			// 如果没有找到下一个文件，此时就需要从内存里去继续读取
			currentBufferedEditsLog.clear();
			
			String[] bufferedEditsLog = namesystem.getEditsLog().getBufferedEditsLog();
			for(String editsLog : bufferedEditsLog) {
				currentBufferedEditsLog.add(JSONObject.parseObject(editsLog));
			}
			
			int fetchCount = 0;
			
			for(int i = 0; i < currentBufferedEditsLog.size(); i++) {
				if(currentBufferedEditsLog.getJSONObject(i).getLong("txid") > backupSyncTxid) {
					fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
					backupSyncTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid"); 
					fetchCount++;
				}
				if(fetchCount == BACKUP_NODE_FETCH_SIZE) {
					break;
				}
			}
		}
		
		response = FetchEditsLogResponse.newBuilder()
				.setEditsLog(fetchedEditsLog.toJSONString())
				.build();
		
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}
	
}
