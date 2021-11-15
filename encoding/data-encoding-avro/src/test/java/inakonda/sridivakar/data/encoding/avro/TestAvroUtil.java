package inakonda.sridivakar.data.encoding.avro;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import inakonda.sridivakar.data.encoding.avro.gen.example_1.Department;
import inakonda.sridivakar.data.encoding.avro.gen.example_1.Employee;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestAvroUtil {
	@Test
	public void testSpecificRecord() throws IOException {
		Department stonehouse = Department.newBuilder()
				.setName("Stonehouse")
				.setPhone(3456)
				.setPlace("Kakinada")
				.build();

		Employee divakar = new Employee();
		divakar.setName("Divakar");
		divakar.setPhone(12345);
		divakar.setDepartment(stonehouse);
		divakar.setPlace("Kakinada");
		log.info("divakar: {}", divakar);
		String d = new String(divakar.toByteBuffer().array(), StandardCharsets.UTF_8);
		//	log.info("divakar (byte buffer): {}", d);

		Employee seshidhar = new Employee("Seshidhar", stonehouse, 23456, "Kakinada");
		log.info("seshidhar: {}", seshidhar);
		String s = new String(seshidhar.toByteBuffer().array(), StandardCharsets.UTF_8);
		//	log.info("seshidhar (byte buffer): {}", s);

		AvroUtil.storeSpecificRecords("target/stonehouse.avro", Employee.class, divakar, seshidhar);

		List<Employee> list = AvroUtil.fetchSpecificRecords("target/stonehouse.avro", Employee.class);
		Assert.assertEquals(2, list.size());
		Assert.assertEquals(divakar, list.get(0));
		Assert.assertEquals(seshidhar, list.get(1));
	}

	@Test
	public void testGenericRecord() throws IOException {
		Schema.Parser parser = new Schema.Parser();
		Schema departmentSchema = parser.parse(new File("src/main/resources/avro/example_1/department.avsc"));

		GenericRecord stonehouse = new GenericData.Record(departmentSchema);
		stonehouse.put("name", "Stonehouse");
		stonehouse.put("phone", 3456);
		stonehouse.put("place", "Kakinada");
		log.info("stonehouse: {}", stonehouse);

		Schema employeeSchema = parser.parse(new File("src/main/resources/avro/example_1/employee.avsc"));
		GenericRecord divakar = new GenericData.Record(employeeSchema);
		divakar.put("name", "Divakar");
		divakar.put("department", stonehouse);
		divakar.put("phone", 12345);
		divakar.put("place", "Kakinada");
		log.info("divakar: {}", divakar);

		GenericRecord seshidhar = new GenericData.Record(employeeSchema);
		seshidhar.put("name", "Seshidhar");
		seshidhar.put("department", stonehouse);
		seshidhar.put("phone", 23456);
		seshidhar.put("place", "Kakinada");
		log.info("seshidhar: {}", seshidhar);

		AvroUtil.storeGenericRecords("target/stonehouse-generic.avro", employeeSchema, divakar, seshidhar);

		List<GenericRecord> genericList = AvroUtil.fetchGenericRecords("target/stonehouse-generic.avro",
				employeeSchema);
		Assert.assertEquals(2, genericList.size());
		Assert.assertEquals(divakar.toString(), genericList.get(0).toString());
		Assert.assertEquals(divakar, genericList.get(0));
		Assert.assertEquals(seshidhar.toString(), genericList.get(1).toString());
		Assert.assertEquals(seshidhar, genericList.get(1));

		List<Employee> specificList = AvroUtil.fetchSpecificRecords("target/stonehouse-generic.avro", Employee.class);
		Assert.assertEquals(2, specificList.size());
		Assert.assertEquals(divakar.toString(), specificList.get(0).toString());
		Assert.assertEquals(seshidhar.toString(), specificList.get(1).toString());
	}

}
