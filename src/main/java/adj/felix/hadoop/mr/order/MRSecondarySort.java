package adj.felix.hadoop.mr.order;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import adj.felix.hadoop.pojo.IntPair;

public class MRSecondarySort {
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd"); 
	
	static class SortMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
		@Override
		protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
			String[] words = line.toString().split(",");
			
			try {
				Calendar calendar=Calendar.getInstance();
				calendar.setTime(format.parse(words[0]));
				
				int temperature = Integer.parseInt(words[1]);
				
				context.write(new IntPair(calendar.get(Calendar.MONTH), temperature), NullWritable.get());
			} catch (ParseException e) {
				System.out.println("Error = " + e.getMessage());
			}
		}
	}
	
	static class SortReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {
		@Override
		protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			// 此处不允许循环values,因为reduce的输入key值是相同的
			context.write(key, NullWritable.get());
		}
	}
	
	static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
		@Override
		public int getPartition(IntPair key, NullWritable value, int numPartitions) {
			return Math.abs(key.getFirst().get() * 127) % numPartitions;
		}
	}
	
	@SuppressWarnings("rawtypes")
	static class KeyComparator extends WritableComparator {
		public KeyComparator() {
			super(IntPair.class, true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPair ip1 = (IntPair) a;
			IntPair ip2 = (IntPair) b;
			int cmp = ip1.getFirst().compareTo(ip2.getFirst());
			if(cmp != 0) {
				return cmp;
			}
			return -ip1.getSecond().compareTo(ip2.getSecond());
		}
	}
	
	@SuppressWarnings("rawtypes")
	static class GroupComparator extends WritableComparator {
		public GroupComparator() {
			super(IntPair.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPair ip1 = (IntPair) a;
			IntPair ip2 = (IntPair) b;
			return ip1.getFirst().compareTo(ip2.getFirst());
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// hadoop命令运行的Jar包
		// conf.set("mapreduce.job.jar", "hadoop-1.0.0.jar");
		
		Job job = Job.getInstance(conf, "SortMr");
		
		job.setMapperClass(SortMapper.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setNumReduceTasks(2);
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
