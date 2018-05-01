package adj.felix.hadoop.codec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

public class PooledStreamCompressor {
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String clodecClassName = args[0];
		Class<?> codecClass = Class.forName(clodecClassName);
		
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		
		Compressor compressor = null;
		try {
			compressor = CodecPool.getCompressor(codec);
			CompressionOutputStream out = codec.createOutputStream(System.out, compressor);
			IOUtils.copyBytes(System.in, out, conf);
			out.finish();
		} finally {
			CodecPool.returnCompressor(compressor);
		}
	}
}
