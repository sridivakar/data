package inakonda.sridivakar.data.encoding.avro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AvroUtil {

	public static <T extends SpecificRecord> void storeSpecificRecords(String avroFileName, Class<T> klass,
			T... avroObjects)
			throws IOException {
		if (ArrayUtils.isEmpty(avroObjects)) {
			log.warn("Nothing to write, skipping. Filename: " + avroFileName);
			return;
		}
		try (DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(new SpecificDatumWriter<>(klass))) {
			dataFileWriter.create(avroObjects[0].getSchema(), new File(avroFileName));
			for (T avroObject : avroObjects) {
				dataFileWriter.append(avroObject);
			}
		} catch (IOException e) {
			log.error("Write failed:", e);
			throw e;
		}
	}

	public static <T extends SpecificRecord> List<T> fetchSpecificRecords(String avroFileName, Class<T> klass)
			throws IOException {
		if (StringUtils.isEmpty(avroFileName) || klass == null) {
			log.warn("Nothing to read, skipping. Filename: " + avroFileName + ", klass:" + klass);
			return Collections.emptyList();
		}
		List<T> data = new ArrayList<>();
		try (DataFileReader<T> dataFileReader = new DataFileReader<T>(new File(avroFileName),
				new SpecificDatumReader<>(klass))) {
			while (dataFileReader.hasNext()) {
				data.add(dataFileReader.next());
			}
			return data;
		} catch (IOException e) {
			log.error("Write failed:", e);
			throw e;
		}
	}

	public static <T extends SpecificRecord> Schema readSchema(String avroFileName, Class<T> klass) throws IOException {
		if (StringUtils.isEmpty(avroFileName) || klass == null) {
			log.warn("Nothing to read, skipping. Filename: " + avroFileName + ", klass:" + klass);
			return null;
		}

		try (DataFileReader<T> dataFileReader = new DataFileReader<T>(new File(avroFileName),
				new SpecificDatumReader<>(klass))) {
			return dataFileReader.getSchema();
		} catch (IOException e) {
			log.error("readSchema failed:", e);
			throw e;
		}
	}

	public static void storeGenericRecords(String avroFileName, Schema schema, GenericRecord... avroObjects)
			throws IOException {
		if (ArrayUtils.isEmpty(avroObjects)) {
			log.warn("Nothing to write, skipping. Filename: " + avroFileName);
			return;
		}
		try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				new GenericDatumWriter<GenericRecord>(schema))) {
			dataFileWriter.create(schema, new File(avroFileName));
			for (GenericRecord avroObject : avroObjects) {
				dataFileWriter.append(avroObject);
			}
		} catch (IOException e) {
			log.error("Write failed:", e);
			throw e;
		}
	}

	public static List<GenericRecord> fetchGenericRecords(String avroFileName, Schema schema) throws IOException {
		if (StringUtils.isEmpty(avroFileName) || schema == null) {
			log.warn("Nothing to read, skipping. Filename: " + avroFileName + ", schema:" + schema);
			return Collections.emptyList();
		}
		List<GenericRecord> data = new ArrayList<>();
		try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(avroFileName),
				new GenericDatumReader<>(schema))) {
			while (dataFileReader.hasNext()) {
				data.add(dataFileReader.next());
			}
			return data;
		} catch (IOException e) {
			log.error("Write failed:", e);
			throw e;
		}
	}

}
