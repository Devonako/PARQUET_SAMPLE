import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageTypeParser;
class Test {
    public static void main(String[] args) {
    	Path file = new Path("test.parquet");
    	Path outfile = new Path("testout2.parquet");
    	Schema schem =null;
    	try {
			schem= Schema.parse(new File("schema.avrc"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	ParquetReader<GenericRecord> reader = null;
    	ParquetWriter<GenericRecord> writer = null;
		try {
			reader = AvroParquetReader.<GenericRecord>builder(file).build();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	try {
    		writer=new AvroParquetWriter<GenericRecord>(outfile, schem);
    		List<GenericRecord> recordsToWrite= new ArrayList<GenericRecord>(); 
			GenericRecord nextRecord = reader.read();
			org.apache.avro.Schema sch=nextRecord.getSchema();
			List<org.apache.avro.Schema.Field> fds=sch.getFields();
			while(nextRecord!=null) {
				int counter=0;
				for (org.apache.avro.Schema.Field fd: fds) {
					counter++;
					if (counter>2) {
					nextRecord.put(fd.name(), nextRecord.get(fd.name()).toString().replace("1", "9"));
					}
				}
				recordsToWrite.add(nextRecord);
				//writer.write(nextRecord.get(null));
				System.out.println(nextRecord.toString());
				nextRecord = reader.read();
			}
    	
			for (GenericRecord record : recordsToWrite) {
		        writer.write(record);
		    }
			writer.close();
			System.out.println("Finished");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
