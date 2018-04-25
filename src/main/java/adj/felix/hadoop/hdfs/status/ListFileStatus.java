package adj.felix.hadoop.hdfs.status;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import adj.felix.hadoop.config.HadoopConfig;

/**
 * listStatus列出目录中的内容
 * @author adolf felix
 */
public class ListFileStatus {
	private static String URLS = HadoopConfig.HDFS_URL;
	public static void main(String[] args) throws IOException {
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(URLS), config);
		
		Path[] paths = new Path[]{new Path(URLS + "/tmp"), new Path(URLS + "/usr")};
		
		FileStatus[] status = fs.listStatus(paths);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		
		for (Path path: listedPaths) {
			System.out.println(path);
		}
	}
}
