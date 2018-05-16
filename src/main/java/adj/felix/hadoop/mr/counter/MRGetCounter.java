package adj.felix.hadoop.mr.counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

public class MRGetCounter {
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		Cluster cluster = new Cluster(conf);
		
		String jobIdStr = "job_1526383545081_0004";
		JobID jobId = JobID.forName(jobIdStr);
		Job job = cluster.getJob(jobId);
		
		Counters counter = job.getCounters();
		
		long error = counter.findCounter(MRCounter.MAX.ERROR).getValue();
		
		System.out.println("ERROR Counter = " + error);
	}
}
