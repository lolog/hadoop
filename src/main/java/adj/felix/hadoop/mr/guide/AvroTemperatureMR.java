package adj.felix.hadoop.mr.guide;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroTemperatureMR  extends Configured implements Tool {
	public static Schema schema;
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd"); 

	static {
		initial();
	}

	private static void initial() {
		try {
			schema = new Schema.Parser().parse(AvroTemperatureMR.class.getResourceAsStream("/avro/weather.avsc"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static class MaxTempetatureMapper extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
		@Override
		public void map(Utf8 datum, AvroCollector<Pair<Integer, GenericRecord>> collector, Reporter reporter)
				throws IOException {
			String line  = datum.toString();
			String[] words = line.split(",");
			
			GenericRecord record = new GenericData.Record(schema);
			Calendar calendar=Calendar.getInstance();
			try {
				calendar.setTime(format.parse(words[0]));
				record.put("year", calendar.get(Calendar.YEAR));
				record.put("month", calendar.get(Calendar.MONTH) + 1);
				record.put("day", calendar.get(Calendar.DAY_OF_MONTH));
				record.put("temperature",Integer.parseInt( words[1]));
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
			collector.collect(new Pair<Integer, GenericRecord>(calendar.get(Calendar.MONTH), record));
		}
	}
	
	public static class MaxTemperatureReducer extends AvroReducer<Integer, GenericRecord, GenericRecord> {
		@Override
		public void reduce(Integer key, Iterable<GenericRecord> values, AvroCollector<GenericRecord> collector,
				Reporter reporter) throws IOException {
			GenericRecord max = null;
			if (values.iterator().hasNext()) {
				GenericRecord record = values.iterator().next();
				max = new GenericData.Record(schema);
				max.put("year", record.get("year"));
				max.put("month", record.get("month"));
				max.put("day", record.get("day"));
				max.put("temperature", record.get("temperature"));
			}
			
			for (GenericRecord record: values) {
				if ((Integer) record.get("temperature") > (Integer) max.get("temperature") ) {
					max.put("year", record.get("year"));
					max.put("month", record.get("month"));
					max.put("day", record.get("day"));
					max.put("temperature", record.get("temperature"));
				}
			}
			collector.collect(max);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("Max Temperature");
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputSchema(conf, Pair.getPairSchema(Schema.create(Schema.Type.INT), schema));
		AvroJob.setOutputSchema(conf, schema);
		
		conf.setInputFormat(AvroUtf8InputFormat.class);
		AvroJob.setMapperClass(conf, MaxTempetatureMapper.class);
		AvroJob.setReducerClass(conf, MaxTemperatureReducer.class);
		
		JobClient.runJob(conf);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AvroTemperatureMR(), args);
		System.exit(exitCode);
	}
}
