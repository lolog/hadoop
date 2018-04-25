package adj.felix.hadoop.hdfs.delete;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import adj.felix.hadoop.config.HadoopConfig;

public class DelFileOrDirectory {
	private static String URLS = HadoopConfig.HDFS_URL;
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(URLS), conf);
		
		// 当path为目录时,recursive=true,不会抛异常
		boolean flag = fs.delete(new Path("/tmp/dir"), true);
		System.out.println("delete flag = " + (flag ? "successful" : "failed"));
		
		fs.close();
	}
}
