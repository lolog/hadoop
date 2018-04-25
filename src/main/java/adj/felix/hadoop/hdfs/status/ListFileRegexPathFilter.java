package adj.felix.hadoop.hdfs.status;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import adj.felix.hadoop.config.HadoopConfig;
import adj.felix.hadoop.hdfs.filter.RegexExcludePathFilter;

public class ListFileRegexPathFilter {
	private static String URLS = HadoopConfig.HDFS_URL;
	public static void main(String[] args) throws IOException {
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(URLS), config);
		
		FileStatus[] status = fs.globStatus(new Path("/tmp/tmp/*"), new RegexExcludePathFilter(".*.0.txt"));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		
		for (Path path: listedPaths) {
			System.out.println(path);
		}
	}
}
