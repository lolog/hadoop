package adj.felix.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WholeFileMapreduce {
	private static class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			/* 禁止文件拆分 */
			return false;
		}

		@Override
		public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split,
				TaskAttemptContext context) throws IOException, InterruptedException {
			WholeFileReader reader = new WholeFileReader();
			reader.initialize(split, context);
			return reader;
		}
	}
	
	private static class SquenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		private Text fileNameKey;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			InputSplit inputSplit = context.getInputSplit();
			Path path = ((FileSplit) inputSplit).getPath();
			
			fileNameKey = new Text(path.toString());
		}
		@Override
		protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			context.write(fileNameKey, value);
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		
		// 文件的输入路径
		WholeFileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(WholeFileInputFormat.class);
		
		// reduce 任务数
		job.setNumReduceTasks(2);  
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// reduce 的输出键值
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setMapperClass(SquenceFileMapper.class);
		
		job.waitForCompletion(true);
	}
}

class WholeFileReader extends org.apache.hadoop.mapreduce.RecordReader<NullWritable, BytesWritable> {
	private FileSplit fileSplit;
	private Configuration config;
	private BytesWritable value = new BytesWritable();
	private boolean processed;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.config = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!processed) {
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path path = fileSplit.getPath();
			FileSystem hdfs = path.getFileSystem(config);
			FSDataInputStream in = null;
			try {
				in = hdfs.open(path);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);
			} 
			finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
	}
	
}
