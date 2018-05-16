package adj.felix.hadoop.mr.counter;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MRCounter {
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd"); 
	
	public static enum MAX {
		ERROR,
	}
	
	public static class CounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
			String[] words = line.toString().split(",");
			
			String dynamic = "";
			String year = "";
			
			try {
				IntWritable counter = new IntWritable();
				Calendar calendar=Calendar.getInstance();
				calendar.setTime(format.parse(words[0]));
				counter.set(Integer.parseInt(words[1]));
				
				dynamic += calendar.get(Calendar.DAY_OF_MONTH);
				year += calendar.get(Calendar.YEAR);
				
				context.write(new Text(year), counter);
			} catch (Exception e) {
				// 枚举类型对应的Counter
				context.getCounter(MAX.ERROR).increment(1);
			}
			
			// 动态计数器
			context.getCounter("Dynamic", dynamic).increment(1);
		}
		
	}
	
	public static class CounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int max = 0;
			for (IntWritable val : values) {
				if(max < val.get()) {
					max = val.get();
				}
			}
			context.write(key, new IntWritable(max));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// job运行的Jar包
		conf.set("mapreduce.job.jar", "hadoop-1.0.0.jar");
		
		Job job = Job.getInstance(conf, "Counter");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(CounterMapper.class);
		job.setCombinerClass(CounterReducer.class);
		job.setReducerClass(CounterReducer.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
