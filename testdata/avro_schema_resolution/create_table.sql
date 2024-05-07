-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- NOTE: Queries in this file have to be executed against Hive and some won't work with
-- Impala due to different type compatibility rules.

USE functional_avro_snap;

DROP TABLE IF EXISTS schema_resolution_test;

-- Specify the Avro schema in SERDEPROPERTIES instead of TBLPROPERTIES to validate
-- IMPALA-538. Also, give the table a different column definition (col1, col2) than what
-- is defined in the Avro schema for testing mismatched table/deserializer schemas.
CREATE EXTERNAL TABLE schema_resolution_test (col1 string, col2 string)
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
  {"name":"date1",    "type": {"type": "int", "logicalType": "date"}, "default": 1},
  {"name":"string1",  "type":"string",  "default": "default string"},
  {"name":"string2",  "type": ["string", "null"],  "default": ""},
  {"name":"string3",  "type": ["null", "string"],  "default": null}
]}')
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/test-warehouse/avro_schema_resolution_test/';

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/avro_schema_resolution/records1.avro' OVERWRITE INTO TABLE schema_resolution_test;
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/avro_schema_resolution/records2.avro' INTO TABLE schema_resolution_test;

-- The following tables are used to test Impala's handling of HIVE-6308 which causes
-- COMPUTE STATS and Hive's ANALYZE TABLE to fail for Avro tables with mismatched
-- column definitions and Avro-schema columns. In such cases, COMPUTE STATS is expected to
-- fail in analysis and not after actually computing stats (IMPALA-867).
-- TODO: The creation of these tables should migrate into our regular data loading
-- framework. There are issues with beeline for these CREATE TABLE stmts (NPEs).

-- No explicit column definitions for an Avro table.
DROP TABLE IF EXISTS alltypes_no_coldef;
CREATE EXTERNAL TABLE IF NOT EXISTS alltypes_no_coldef
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/test-warehouse/alltypes_avro_snap'
TBLPROPERTIES ('avro.schema.url'='${env:DEFAULT_FS}/test-warehouse/avro_schemas/functional/alltypes.json');

-- Column definition list has one more column than the Avro schema.
DROP TABLE IF EXISTS alltypes_extra_coldef;
CREATE EXTERNAL TABLE IF NOT EXISTS alltypes_extra_coldef (
id int,
bool_col boolean,
tinyint_col tinyint,
smallint_col smallint,
int_col int,
bigint_col bigint,
float_col float,
double_col double,
date_string_col string,
string_col string,
timestamp_col timestamp,
extra_col string)
PARTITIONED BY (year int, month int)
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/test-warehouse/alltypes_avro_snap'
TBLPROPERTIES ('avro.schema.url'='${env:DEFAULT_FS}/test-warehouse/avro_schemas/functional/alltypes.json');

-- Column definition list is missing 'tinyint_col' and 'timestamp_col' from the Avro schema.
DROP TABLE IF EXISTS alltypes_missing_coldef;
CREATE EXTERNAL TABLE IF NOT EXISTS alltypes_missing_coldef (
id int,
bool_col boolean,
smallint_col smallint,
int_col int,
bigint_col bigint,
float_col float,
double_col double,
date_string_col string,
string_col string)
PARTITIONED BY (year int, month int)
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/test-warehouse/alltypes_avro_snap'
TBLPROPERTIES ('avro.schema.url'='${env:DEFAULT_FS}/test-warehouse/avro_schemas/functional/alltypes.json');

-- Matching number of columns and column names, but mismatched type (bigint_col is a string).
DROP TABLE IF EXISTS alltypes_type_mismatch;
CREATE EXTERNAL TABLE IF NOT EXISTS alltypes_type_mismatch (
id int,
bool_col boolean,
tinyint_col tinyint,
smallint_col smallint,
int_col int,
bigint_col string,
float_col float,
double_col double,
date_string_col string,
string_col string,
timestamp_col timestamp)
PARTITIONED BY (year int, month int)
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/test-warehouse/alltypes_avro_snap'
TBLPROPERTIES ('avro.schema.url'='${env:DEFAULT_FS}/test-warehouse/avro_schemas/functional/alltypes.json');

-- IMPALA-2798, create two avro tables with same underlying data location, one table
-- has an extra column at the end. Validate Impala doesn't use codegen decoding function
-- if table schema is not the same as file schema.
DROP TABLE IF EXISTS avro_coldef;
CREATE EXTERNAL TABLE IF NOT EXISTS avro_coldef (
bool_col boolean,
tinyint_col int,
smallint_col int,
int_col int,
bigint_col bigint,
float_col float,
double_col double,
date_string_col string,
string_col string,
timestamp_col timestamp)
PARTITIONED BY (year int, month int)
STORED AS avro
LOCATION '/test-warehouse/avro_coldef_snap'
TBLPROPERTIES ('avro.schema.literal'='{
"name": "a",
"type": "record",
"fields": [
  {"name":"bool_col", "type":"boolean"},
  {"name":"tinyint_col",     "type":"int"},
  {"name":"smallint_col",    "type":"int"},
  {"name":"int_col",   "type":"int"},
  {"name":"bigint_col",  "type":"long"},
  {"name":"float_col",  "type":"float"},
  {"name":"double_col",  "type": "double"},
  {"name":"date_string_col",  "type": "string"},
  {"name":"string_col",  "type": "string"},
  {"name":"timestamp_col",  "type": "long"}
]}');

-- Reload existing partitions from HDFS. Without this, the overwrite will fail to remove
-- any preexisting data files, which in turn will fail the query.
MSCK REPAIR TABLE avro_coldef;

-- Disable the restriction of HIVE-24157, otherwise casting TIMESTAMP to BIGINT is
-- prohibited. Note that type of timestamp_col is long in 'avro.schema.literal'.
SET hive.strict.timestamp.conversion=false;

INSERT OVERWRITE TABLE avro_coldef PARTITION(year=2014, month=1)
SELECT bool_col, tinyint_col, smallint_col, int_col, bigint_col,
float_col, double_col, date_string_col, string_col, timestamp_col
FROM (select * from functional.alltypes order by id limit 5) a;

DROP TABLE IF EXISTS avro_extra_coldef;
CREATE EXTERNAL TABLE IF NOT EXISTS avro_extra_coldef (
bool_col boolean,
tinyint_col int,
smallint_col int,
int_col int,
bigint_col bigint,
float_col float,
double_col double,
date_string_col string,
string_col string,
timestamp_col timestamp,
extra_col string)
PARTITIONED BY (year int, month int)
STORED AS avro
LOCATION '/test-warehouse/avro_coldef_snap'
TBLPROPERTIES ('avro.schema.literal'='{
"name": "a",
"type": "record",
"fields": [
  {"name":"bool_col", "type":"boolean"},
  {"name":"tinyint_col",     "type":"int"},
  {"name":"smallint_col",    "type":"int"},
  {"name":"int_col",   "type":"int"},
  {"name":"bigint_col",  "type":"long"},
  {"name":"float_col",  "type":"float"},
  {"name":"double_col",  "type": "double"},
  {"name":"date_string_col",  "type": "string"},
  {"name":"string_col",  "type": "string"},
  {"name":"timestamp_col",  "type": "long"},
  {"name":"extra_col",  "type": "string", "default": "null"}
]}');

-- Reload existing partitions from HDFS. Without this, the overwrite will fail to remove
-- any preexisting data files, which in turn will fail the query.
MSCK REPAIR TABLE avro_extra_coldef;

INSERT OVERWRITE TABLE avro_extra_coldef PARTITION(year=2014, month=2)
SELECT bool_col, tinyint_col, smallint_col, int_col, bigint_col,
float_col, double_col, date_string_col, string_col,
timestamp_col, "avro" AS extra_col FROM
(select * from functional.alltypes order by id limit 5) a;

-- Reload the partitions for the first table once again. This will make sure that the new
-- partition from the second insert shows up in the first table, too.
MSCK REPAIR TABLE avro_coldef;
