package adj.felix.hadoop.type.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Before;
import org.junit.Test;

public class AvroWether {
	Schema.Parser parser = new Schema.Parser();
	Schema schema;
	
	@Before
	public void before () throws IOException {
		schema = parser.parse(AvroWether.class.getResourceAsStream("/avro/weather.avsc"));
	}
	@Test
	public void genericRecord() throws IOException {
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("year", new Integer(20));
		datum.put("month", new Integer(20));
		datum.put("day", new Integer(20));
		datum.put("temperature", new Integer(20));
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datum, encoder);
		encoder.flush();
		out.close();
		
		DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		GenericRecord result = reader.read(null, decoder);
		
		System.out.println("{year = "+ result.get("year") + ", month=" + result.get("month") + ", day=" + result.get("day") + ", temperature=" + result.get("temperature") + "}");
	}
}
