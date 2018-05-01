package adj.felix.hadoop.codec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class StreamCompressor {
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		String codecClassName = args[0];
		Class<?> codecClass = Class.forName(codecClassName);
		Configuration config = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, config);
		
		CompressionOutputStream out = codec.createOutputStream(System.out);
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.flush();
	}
}
