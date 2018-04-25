package adj.felix.hadoop.hdfs.write;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import adj.felix.hadoop.config.HadoopConfig;

public class FileCopyWithProgress {
	private static String URLS = HadoopConfig.HDFS_URL;
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		String localSrc = "/home/iot/Templates/temp.0.txt";
		String dst = URLS + "/tmp/tmp/tmp.0.txt";
		
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			@Override
			public void progress() {
				System.out.println("progress");
			}
		});
		
		IOUtils.copyBytes(in, out, 4096, true);
	}
}
