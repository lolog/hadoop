package adj.felix.hadoop.hdfs.read;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import adj.felix.hadoop.config.HadoopConfig;

/**
 * 从hadoop url读取数据
 * @author adolf felix
 */
public class UrlCat {
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	private static String URLS = HadoopConfig.HDFS_URL;
	// 注意：文件必须在hdfs中存在
	private static String FILES = "/tmp/tmp/temp.txt";

	public static void main(String[] args) {
		InputStream in = null;
		try {
			in = new URL(URLS + FILES).openStream();
			// 读取数据到System.out输出流,并且输出到控制台
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
