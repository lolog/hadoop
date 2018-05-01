package adj.felix.hadoop.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import adj.felix.hadoop.config.HadoopConfig;

public class FileDecompressor {
	private static String URLS = HadoopConfig.HDFS_URL;
	public static void main(String[] args) throws IOException {
		String uri = URLS + "/tmp/temp.tar.gz";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found");
			System.exit(1);
		}
		
		uri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		
		InputStream in = null;
		OutputStream out = null;
		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(uri));
			IOUtils.copyBytes(in, out, conf);
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
}
