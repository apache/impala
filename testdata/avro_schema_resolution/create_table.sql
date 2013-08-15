USE functional_avro_snap;

DROP TABLE IF EXISTS schema_resolution_test;

-- Specify schema in SERDEPROPERTIES instead of TBLPROPERTIES to validate IMP-538
CREATE EXTERNAL TABLE schema_resolution_test
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.literal'='{
"name": "a",
"type": "record",
"fields": [
  {"name":"boolean1", "type":"boolean", "default": true},
  {"name":"int1",     "type":"int",     "default": 1},
  {"name":"long1",    "type":"long",    "default": 1},
  {"name":"float1",   "type":"float",   "default": 1.0},
  {"name":"double1",  "type":"double",  "default": 1.0},
  {"name":"string1",  "type":"string",  "default": "default string"},
  {"name":"string2",  "type": ["string", "null"],  "default": ""},
  {"name":"string3",  "type": ["null", "string"],  "default": null}
]}')
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/avro_schema_resolution_test/';

LOAD DATA LOCAL INPATH 'records1.avro' OVERWRITE INTO TABLE schema_resolution_test;
LOAD DATA LOCAL INPATH 'records2.avro' INTO TABLE schema_resolution_test;
