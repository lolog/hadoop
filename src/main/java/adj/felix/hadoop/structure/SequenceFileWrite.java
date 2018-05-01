package adj.felix.hadoop.structure;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileWrite {
	private static final String[] DATA = new String[] {
		"One, tow, buckle my shoe",
		"Three, four, shut the door",
		"Five, siz. pick up sticks",
		"Seven, eight, lay them straight",
		"Nine, ten, a big fat hen"
	};
	
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
		
		Path path = new Path(uri);
		
		IntWritable key = new IntWritable();
		Text data = new Text();
		
		@SuppressWarnings("deprecation")
		SequenceFile.Writer writer = SequenceFile.createWriter(hdfs, conf, path, key.getClass(), data.getClass());
		for (int i=0; i<100; i++) {
			key.set(100 - i);
			data.set(DATA[i % DATA.length]);
			writer.append(key, data);
		}
		org.apache.hadoop.io.IOUtils.closeStream(writer);
	}
}
