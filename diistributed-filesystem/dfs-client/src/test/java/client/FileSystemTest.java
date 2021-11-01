package client;

import com.zhss.dfs.client.FileSystem;
import com.zhss.dfs.client.FileSystemImpl;

public class FileSystemTest {
	
	public static void main(String[] args) throws Exception {
		FileSystem filesystem = new FileSystemImpl();
		filesystem.mkdir("/usr/local/kafka/data"); 
	}
	
}
