package adj.felix.hadoop.type.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import adj.felix.hadoop.config.HadoopConfig;
import adj.felix.hadoop.pojo.Data;

public class Avro {
	Schema.Parser parser = new Schema.Parser();
	Schema schema;
	
	@Before
	public void before () throws IOException {
		schema = parser.parse(Avro.class.getResourceAsStream("/avro/data.avsc"));
	}
	@Test
	public void genericRecord() throws IOException {
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("name", new Utf8("name"));
		datum.put("age", new Integer(20));
		datum.put("info", new Utf8("info"));
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datum, encoder);
		encoder.flush();
		out.close();
		
		DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		GenericRecord result = reader.read(null, decoder);
		
		System.out.println("{name = "+ result.get("name") + ", age=" + result.get("age") + ", info=" + result.get("info") + "}");
	}
	@Test
	public void avro() throws IOException {
		Data pojo = new Data();
		pojo.setName("pojo");
		pojo.setAge(20);
		pojo.setInfo("info");
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<Data> writer = new SpecificDatumWriter<>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(pojo, encoder);
		encoder.flush();
		out.close();
		
		DatumReader<Data> reader = new SpecificDatumReader<>(Data.class);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		Data result = reader.read(null, decoder);
		
		System.out.println(result);
	}
	
	@Test
	public void avroWriteFile() throws IOException {
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("name", new Utf8("name"));
		datum.put("age", new Integer(20));
		datum.put("info", new Utf8("info"));
		
		File file = new File("out/data.avro");
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
		fileWriter.create(schema, file);
		fileWriter.append(datum);
		fileWriter.close();
	}
	
	@Test
	public void avroReaderFile() throws IOException {
		File file = new File("out/data.avro");
		DatumReader<GenericRecord> reader = new GenericDatumReader<>();
		DataFileReader<GenericRecord> fileReader = new DataFileReader<>(file, reader);
		while(fileReader.hasNext()) {
			// 增加内存的开销
			GenericRecord record = fileReader.next();
			System.out.println("{name = "+ record.get("name") + ", age=" + record.get("age") + ", info=" + record.get("info") + "}");
		}
		
		// 指针指到起始位置
		fileReader.sync(0);
		GenericRecord record = null;
		while(fileReader.hasNext()) {
			// 减少对象的分配和垃圾回收所产生的开销
			record = fileReader.next(record);
			System.out.println("{name = "+ record.get("name") + ", age=" + record.get("age") + ", info=" + record.get("info") + "}");
		}
		
		fileReader.close();
	}
	
	@Test
	public void avroReaderFileFromHdfs() throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(HadoopConfig.HDFS_URL + "/tmp/data.avro");
		SeekableInput input = new FsInput(path, conf);
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<>();
		DataFileReader<GenericRecord> fileReader = new DataFileReader<>(input, reader);
		
		GenericRecord record = null;
		while(fileReader.hasNext()) {
			// 减少对象的分配和垃圾回收所产生的开销
			record = fileReader.next(record);
			System.out.println("{name = "+ record.get("name") + ", age=" + record.get("age") + ", info=" + record.get("info") + "}");
		}
		
		fileReader.close();
	}
}
