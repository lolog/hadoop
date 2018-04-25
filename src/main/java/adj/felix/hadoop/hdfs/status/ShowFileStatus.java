package adj.felix.hadoop.hdfs.status;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import adj.felix.hadoop.config.HadoopConfig;

public class ShowFileStatus {
	private static String URLS = HadoopConfig.HDFS_URL;
	private FileSystem fs;
	
	@Before
	public void before() throws IOException {
		Configuration conf = new Configuration();
		fs = FileSystem.get(URI.create(URLS + "/tmp/file"), conf);
	}
	
	@After
	public void after() throws IOException {
		if(fs != null) {
			fs.close();
		}
	}
	
	@Test(expected = FileNotFoundException.class)
	public void fileNotFoundForNonExistentFile () throws IllegalArgumentException, IOException {
		OutputStream out = fs.create(new Path(URLS + "/tmp/file"));
		out.write("content \r\n".getBytes());
		out.flush();
		out.close();
		fs.getFileStatus(new Path(URLS + "/tmp/no-such-file"));
	}
	
	@Test
	public void fileStatusForFile() throws IOException {
		Path path = new Path(URLS  + "/tmp/file");
		FileStatus status = fs.getFileStatus(path);
		System.out.println("path = "+ status.getPath().toUri().getPath());
		System.out.println("isDIr = "+ status.isDirectory());
		System.out.println("len = "+ status.getLen());
		System.out.println("modified time = "+ status.getModificationTime());
		System.out.println("replication = "+ status.getReplication());
		System.out.println("block size = "+ status.getBlockSize());
		System.out.println("owner = "+ status.getOwner());
		System.out.println("group = "+ status.getGroup());
		System.out.println("permission = "+ status.getPermission());
	}
	
	@Test
	public void fileStatusForDirectory () throws IOException {
		Path path = new Path(URLS  + "/tmp");
		FileStatus status = fs.getFileStatus(path);
		System.out.println("path = "+ status.getPath().toUri().getPath());
		System.out.println("isDIr = "+ status.isDirectory());
		System.out.println("len = "+ status.getLen());
		System.out.println("modified time = "+ status.getModificationTime());
		System.out.println("replication = "+ status.getReplication());
		System.out.println("block size = "+ status.getBlockSize());
		System.out.println("owner = "+ status.getOwner());
		System.out.println("group = "+ status.getGroup());
		System.out.println("permission = "+ status.getPermission());
	}
}
