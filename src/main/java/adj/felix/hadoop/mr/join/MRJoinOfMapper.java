package adj.felix.hadoop.mr.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import adj.felix.hadoop.pojo.TextPair;

public class MRJoinOfMapper {
	/* userId,userName,userAge */
	static class JoinUserMapper extends Mapper<LongWritable, Text, TextPair, Text> {
		@Override
		protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
			String[] words = line.toString().split(",");
			context.write(new TextPair(words[0], "0"), new Text(words[1] + "," + words[2]));
		}
	}
	
	/* userId,userInfo */
	static class JoinInfoMapper extends Mapper<LongWritable, Text, TextPair, Text> {
		@Override
		protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
			String[] words = line.toString().split(",");
			context.write(new TextPair(words[0], "1"), new Text(words[1]));
		}
	}
	
	static class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
		@Override
		protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			
			Text user = iter.next();
			while (iter.hasNext()) {
				Text value = new Text(user + "\t" + iter.next().toString());
				context.write(key.getFirst(), value);
			}
		}
	}
	
	static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	
	@SuppressWarnings("rawtypes")
	static class KeyComparator extends WritableComparator {
		public KeyComparator() {
			super(TextPair.class, true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TextPair ip1 = (TextPair) a;
			TextPair ip2 = (TextPair) b;
			int cmp = ip1.getFirst().compareTo(ip2.getFirst());
			
			return cmp;
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Join");
		
		Path userPath = new Path(args[0]);
		Path infoPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		MultipleInputs.addInputPath(job, userPath, TextInputFormat.class, JoinUserMapper.class);
		MultipleInputs.addInputPath(job, infoPath, TextInputFormat.class, JoinInfoMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(KeyComparator.class);
		
		job.setMapOutputKeyClass(TextPair.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		
		job.waitForCompletion(true);
	}
}
