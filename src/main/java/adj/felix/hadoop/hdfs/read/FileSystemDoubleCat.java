package adj.felix.hadoop.hdfs.read;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import adj.felix.hadoop.config.HadoopConfig;

public class FileSystemDoubleCat {
	private static String URLS = HadoopConfig.HDFS_URL;
	
	public static void main(String[] args) throws IOException {
		String path = URLS + "/tmp/tmp/temp.txt";
		
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), configuration);
		
		FSDataInputStream in = fs.open(new Path(path));
		
		IOUtils.copyBytes(in, System.out, 4069, false);
		in.seek(0);
		IOUtils.copyBytes(in, System.out, 4069, false);
		
		IOUtils.closeStream(in);
	}
}
