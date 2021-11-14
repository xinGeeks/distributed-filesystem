package client;

import com.zhss.dfs.client.FileSystem;
import com.zhss.dfs.client.FileSystemImpl;

public class FileSystemTest {
	
	public static void main(String[] args) throws Exception {
		FileSystem filesystem = new FileSystemImpl();
		for (int i = 0; i < 10; i++) {
			new Thread(() -> {
				for (int j = 0; j < 100; j++) {
					try {
						filesystem.mkdir("/usr/local/kafka/data_" + j);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();
		}
	}
	
}
