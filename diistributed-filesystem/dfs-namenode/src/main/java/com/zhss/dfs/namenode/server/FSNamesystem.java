package com.zhss.dfs.namenode.server;

/**
 * 负责管理元数据的核心组件
 * @author zhonghuashishan
 *
 */
public class FSNamesystem {

	/**
	 * 负责管理内存文件目录树的组件
	 */
	// 这个组件，就是专门负责维护内存中的文件目录树的
	private FSDirectory directory;
	/**
	 * 负责管理edits log写入磁盘的组件
	 */
	private FSEditlog editlog;
	
	public FSNamesystem() {
		this.directory = new FSDirectory();
		this.editlog = new FSEditlog();
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否成功
	 */
	public Boolean mkdir(String path) throws Exception {
		this.directory.mkdir(path); // 第一步就是基于FSDirectory这个组件来真正去管理文件目录树
		this.editlog.logEdit("{'OP':'MKDIR','PATH':'" + path + "'}");     
		return true;
	}
	
	/**
	 * 强制把内存里的edits log刷入磁盘中
	 */
	public void flush() {
		this.editlog.flush();
	}

	public FSEditlog getEditlog() {
		return editlog;
	}

}
