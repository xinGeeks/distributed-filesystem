package com.zhss.dfs.client;

/**
 * 作为文件系统的接口
 * @author zhonghuashishan
 *
 */
public interface FileSystem {

	/**
	 * 创建目录
	 * @param path 目录对应的路径
	 * @throws Exception
	 */
	void mkdir(String path) throws Exception;
	
	/**
	 * 优雅关闭
	 * @throws Exception
	 */
	void shutdown() throws Exception;
	
}
