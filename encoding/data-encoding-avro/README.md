

#### How to view avro file contents ?
##### How to view avro file schema ?
###### Command:
```sh
data/encoding$ java -jar ~/.m2/raptor3/org/apache/avro/avro-tools/1.11.0/avro-tools-1.11.0.jar getschema data-encoding-avro/stonehouse.avro
```
###### Output:
```json 
21/11/15 17:27:40 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
{
  "type" : "record",
  "name" : "Employee",
  "namespace" : "inakonda.sridivakar.data.encoding.avro.gen.example_1",
  "fields" : [ {
    "name" : "name",
    "type" : "string"
  }, {
    "name" : "deptartment",
    "type" : [ {
      "type" : "record",
      "name" : "Department",
      "fields" : [ {
        "name" : "name",
        "type" : "string"
      }, {
        "name" : "phone",
        "type" : [ "int", "null" ]
      }, {
        "name" : "place",
        "type" : [ "string", "null" ]
      } ]
    }, "null" ]
  }, {
    "name" : "phone",
    "type" : [ "int", "null" ]
  }, {
    "name" : "place",
    "type" : [ "string", "null" ]
  } ]
}
```

##### How to view avro file data ?
###### Command
```sh
data/encoding$ java -jar ~/.m2/raptor3/org/apache/avro/avro-tools/1.11.0/avro-tools-1.11.0.jar tojson data-encoding-avro/stonehouse.avro 
```
###### Output
```json
21/11/15 17:28:24 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
{"name":"Divakar","deptartment":{"inakonda.sridivakar.data.encoding.avro.gen.example_1.Department":{"name":"Stonehouse","phone":{"int":3456},"place":{"string":"Kakinada"}}},"phone":{"int":12345},"place":{"string":"Kakinada"}}
{"name":"Seshidhar","deptartment":{"inakonda.sridivakar.data.encoding.avro.gen.example_1.Department":{"name":"Stonehouse","phone":{"int":3456},"place":{"string":"Kakinada"}}},"phone":{"int":23456},"place":{"string":"Kakinada"}}
``` 

#### References 
https://www.michael-noll.com/blog/2013/03/17/reading-and-writing-avro-files-from-the-command-line/
