====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypes
---- PARTITION_COLUMNS
year int
month int
---- COLUMNS
id int COMMENT 'Add a comment'
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=1);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=2);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=3);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=4);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=5);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=6);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=7);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=8);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=9);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=10);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=11);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=12);

ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=2);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=3);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=4);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=5);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=6);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=7);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=8);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=9);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=10);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=11);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=12);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/090101.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/090201.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/090301.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/090401.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=4);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/090501.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=5);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/090601.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=6);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/090701.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=7);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/090801.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=8);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/090901.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=9);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/091001.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=10);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/091101.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=11);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/091201.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=12);

LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/100101.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/100201.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/100301.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=3);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/100401.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=4);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/100501.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=5);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/100601.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=6);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/100701.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=7);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/100801.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=8);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/100901.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=9);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/101001.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=10);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/101101.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=11);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypes/101201.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=12);
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  id INT PRIMARY KEY,
  bool_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  date_string_col STRING,
  string_col STRING,
  timestamp_col TIMESTAMP,
  year INT,
  month INT
)
PARTITION BY HASH (id) PARTITIONS 3 STORED AS KUDU;
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col,
       timestamp_col, year, month
FROM {db_name}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypesnopart
---- COLUMNS
id int
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypessmall
---- PARTITION_COLUMNS
year int
month int
---- COLUMNS
id int
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- HBASE_REGION_SPLITS
'1','3','5','7','9'
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=1);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=2);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=3);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=4);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesSmall/090101.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesSmall/090201.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesSmall/090301.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesSmall/090401.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=4);
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  id INT PRIMARY KEY,
  bool_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  date_string_col STRING,
  string_col STRING,
  timestamp_col TIMESTAMP,
  year INT,
  month INT
)
PARTITION BY HASH (id) PARTITIONS 3 STORED AS KUDU;
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col,
       timestamp_col, year, month
FROM {db_name}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypestiny
---- PARTITION_COLUMNS
year int
month int
---- COLUMNS
id int
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=1);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=2);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=3);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2009, month=4);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesTiny/090101.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesTiny/090201.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesTiny/090301.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesTiny/090401.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=4);
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  id INT PRIMARY KEY,
  bool_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  date_string_col STRING,
  string_col STRING,
  timestamp_col TIMESTAMP,
  year INT,
  month INT
)
PARTITION BY HASH (id) PARTITIONS 3 STORED AS KUDU;
---- DEPENDENT_LOAD_KUDU
INSERT INTO TABLE {db_name}{db_suffix}.{table_name}
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col,
       timestamp_col, year, month
FROM {db_name}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypesinsert
---- CREATE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name}
LIKE {db_name}{db_suffix}.alltypes STORED AS {file_format};
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypesnopart_insert
---- CREATE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name}
LIKE {db_name}{db_suffix}.alltypesnopart STORED AS {file_format};
====
---- DATASET
functional
---- BASE_TABLE_NAME
insert_overwrite_nopart
---- CREATE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (col1 int)
STORED AS {file_format};
====
---- DATASET
functional
---- BASE_TABLE_NAME
insert_overwrite_partitioned
---- CREATE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (col1 int)
PARTITIONED BY (col2 int)
STORED AS {file_format};
====
---- DATASET
functional
---- BASE_TABLE_NAME
insert_string_partitioned
---- CREATE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (s1 string)
PARTITIONED BY (s2 string)
STORED AS {file_format};
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypeserror
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
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
  timestamp_col timestamp)
partitioned by (year int, month int)
row format delimited fields terminated by ','  escaped by '\\'
stored as {file_format}
LOCATION '{hdfs_location}';
USE {db_name}{db_suffix};
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (year=2009, month=1);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (year=2009, month=2);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (year=2009, month=3);

-- Create external temp table with desired file format with same data file location
-- Tmp tables must not specify an escape character we don't want any
-- data transformation to happen when inserting it into tmp tables.
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name}_tmp (
  id STRING,
  bool_col STRING,
  tinyint_col STRING,
  smallint_col STRING,
  int_col STRING,
  bigint_col STRING,
  float_col STRING,
  double_col STRING,
  date_string_col STRING,
  string_col STRING,
  timestamp_col STRING)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS {file_format}
LOCATION '{hdfs_location}';

-- Make metastore aware of the partition directories for the temp table
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION (year=2009, month=1);
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION (year=2009, month=2);
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION (year=2009, month=3);
---- TABLE_PROPERTIES
transactional=false
---- DEPENDENT_LOAD
USE {db_name}{db_suffix};
-- Step 4: Stream the data from tmp text table to desired format tmp table
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}_tmp PARTITION (year, month)
SELECT * FROM {db_name}.{table_name}_tmp;

-- Cleanup the temp table
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name}_tmp;
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/AllTypesError/0901.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/AllTypesError/0902.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/AllTypesError/0903.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=3);
====
---- DATASET
functional
---- BASE_TABLE_NAME
hbasealltypeserror
---- CREATE_HIVE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
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
  timestamp_col timestamp)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,d:bool_col,d:tinyint_col,d:smallint_col,d:int_col,d:bigint_col,d:float_col,d:double_col,d:date_string_col,d:string_col,d:timestamp_col"
)
TBLPROPERTIES("hbase.table.name" = "functional_hbase.hbasealltypeserror");
====
---- DATASET
functional
---- BASE_TABLE_NAME
hbasecolumnfamilies
---- HBASE_COLUMN_FAMILIES
0
1
2
3
d
---- CREATE_HIVE
-- Create an HBase table with multiple column families
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
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
  timestamp_col timestamp)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,0:bool_col,1:tinyint_col,2:smallint_col,3:int_col,d:bigint_col,d:float_col,d:double_col,d:date_string_col,d:string_col,d:timestamp_col"
)
TBLPROPERTIES("hbase.table.name" = "functional_hbase.hbasecolumnfamilies");
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col,
date_string_col, string_col, timestamp_col FROM functional.alltypestiny;
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypeserrornonulls
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS  {db_name}{db_suffix}.{table_name} (
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
  timestamp_col timestamp)
partitioned by (year int, month int)
row format delimited fields terminated by ','  escaped by '\\'
stored as {file_format}
LOCATION '{hdfs_location}';
-- Make metastore aware of the new partitions directories
-- ALTER does not take a fully qualified name.
USE {db_name}{db_suffix};

ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (year=2009, month=1);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (year=2009, month=2);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (year=2009, month=3);

-- Create external temp table with desired file format with same data file location
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name}_tmp (
  id STRING,
  bool_col STRING,
  tinyint_col STRING,
  smallint_col STRING,
  int_col STRING,
  bigint_col STRING,
  float_col STRING,
  double_col STRING,
  date_string_col STRING,
  string_col STRING,
  timestamp_col STRING)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
STORED AS {file_format}
LOCATION '{hdfs_location}';

-- Make metastore aware of the partition directories
USE {db_name}{db_suffix};
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION (year=2009, month=1);
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION (year=2009, month=2);
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION (year=2009, month=3);
---- TABLE_PROPERTIES
transactional=false
---- DEPENDENT_LOAD
USE {db_name}{db_suffix};
-- Step 4: Stream the data from tmp text table to desired format tmp table
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}_tmp PARTITION (year, month)
SELECT * FROM {db_name}.{table_name}_tmp;

-- Cleanup the temp table
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name}_tmp;
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/AllTypesErrorNoNulls/0901.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/AllTypesErrorNoNulls/0902.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/AllTypesErrorNoNulls/0903.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2009, month=3);
====
---- DATASET
functional
---- BASE_TABLE_NAME
hbasealltypeserrornonulls
---- CREATE_HIVE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
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
  timestamp_col timestamp)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key,d:bool_col,d:tinyint_col,d:smallint_col,d:int_col,d:bigint_col,d:float_col,d:double_col,d:date_string_col,d:string_col,d:timestamp_col"
)
TBLPROPERTIES("hbase.table.name" = "functional_hbase.hbasealltypeserrornonulls");
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypesagg
---- PARTITION_COLUMNS
year int
month int
day int
---- COLUMNS
id int
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- HBASE_REGION_SPLITS
'1','3','5','7','9'
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=1);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=2);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=3);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=4);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=5);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=6);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=7);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=8);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=9);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=10);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=NULL);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM {db_name}.{table_name};
---- LOAD
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100101.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100102.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100103.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100104.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100105.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100106.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100107.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100108.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100109.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAgg/100110.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=10);
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, tinyint_col as day FROM {db_name}.{table_name} WHERE year=2010 and month=1 and day IS NOT NULL and tinyint_col IS NULL order by id;
---- CREATE_KUDU
DROP VIEW IF EXISTS {db_name}{db_suffix}.{table_name};
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name}_idx;

CREATE TABLE {db_name}{db_suffix}.{table_name}_idx (
  kudu_idx BIGINT PRIMARY KEY,
  id INT NULL,
  bool_col BOOLEAN NULL,
  tinyint_col TINYINT NULL,
  smallint_col SMALLINT NULL,
  int_col INT NULL,
  bigint_col BIGINT NULL,
  float_col FLOAT NULL,
  double_col DOUBLE NULL,
  date_string_col STRING NULL,
  string_col STRING NULL,
  timestamp_col TIMESTAMP NULL,
  year INT NULL,
  month INT NULL,
  day INT NULL
)
PARTITION BY HASH (kudu_idx) PARTITIONS 3 STORED AS KUDU;
CREATE VIEW {db_name}{db_suffix}.{table_name} AS
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col,
       double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM {db_name}{db_suffix}.{table_name}_idx;
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}_idx
SELECT row_number() over (order by year, month, id, day),
       id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col,
       double_col, date_string_col, string_col,
       timestamp_col, year, month, day
FROM {db_name}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypesaggnonulls
---- PARTITION_COLUMNS
year int
month int
day int
---- COLUMNS
id int
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=1);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=2);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=3);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=4);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=5);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=6);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=7);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=8);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=9);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=10);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100101.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100102.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100103.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100104.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100105.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100106.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100107.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100108.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100109.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/target/AllTypesAggNoNulls/100110.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=1, day=10);
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  id INT PRIMARY KEY,
  bool_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  date_string_col STRING,
  string_col STRING,
  timestamp_col TIMESTAMP,
  year INT,
  month INT,
  day INT
)
PARTITION BY HASH (id) PARTITIONS 3 STORED AS KUDU;
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col,
       double_col, date_string_col, string_col,
       timestamp_col, year, month, day
FROM {db_name}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
allcomplextypes
---- PARTITION_COLUMNS
year int
month int
---- COLUMNS
id int
int_array_col array<int>
array_array_col array<array<int>>
map_array_col array<map<string,int>>
struct_array_col array<struct<f1: bigint, f2: string>>
int_map_col map<string, int>
array_map_col map<string, array<int>>
map_map_col map<string, map<string, int>>
struct_map_col map<string, struct<f1: bigint, f2: string>>
int_struct_col struct<f1: int, f2: int>
complex_struct_col struct<f1: int, f2: array<int>, f3: map<string, int>>
nested_struct_col struct<f1: int, f2: struct<f11: bigint, f12: struct<f21: bigint>>>
complex_nested_struct_col struct<f1: int, f2: array<struct<f11: bigint, f12: map<string, struct<f21: bigint>>>>>
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- DEPENDENT_LOAD
---- LOAD
====
---- DATASET
functional
---- BASE_TABLE_NAME
complextypestbl
---- COLUMNS
id bigint
int_array array<int>
int_array_array array<array<int>>
int_map map<string, int>
int_map_array array<map<string, int>>
nested_struct struct<a: int, b: array<int>, c: struct<d: array<array<struct<e: int, f: string>>>>, g: map<string, struct<h: struct<i: array<double>>>>>
---- DEPENDENT_LOAD
`hadoop fs -mkdir -p /test-warehouse/complextypestbl_parquet && \
hadoop fs -put -f ${IMPALA_HOME}/testdata/ComplexTypesTbl/nullable.parq \
/test-warehouse/complextypestbl_parquet/ && \
hadoop fs -put -f ${IMPALA_HOME}/testdata/ComplexTypesTbl/nonnullable.parq \
/test-warehouse/complextypestbl_parquet/
---- DEPENDENT_LOAD_ACID
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM functional_parquet.complextypestbl;
---- LOAD
====
---- DATASET
functional
---- BASE_TABLE_NAME
complextypestbl_minor_compacted
---- COLUMNS
id bigint
int_array array<int>
int_array_array array<array<int>>
int_map map<string, int>
int_map_array array<map<string, int>>
nested_struct struct<a: int, b: array<int>, c: struct<d: array<array<struct<e: int, f: string>>>>, g: map<string, struct<h: struct<i: array<double>>>>>
---- DEPENDENT_LOAD_ACID
INSERT INTO TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}{db_suffix}.complextypestbl where id = 1;
INSERT INTO TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}{db_suffix}.complextypestbl where id = 2;
INSERT INTO TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}{db_suffix}.complextypestbl where id = 3;
INSERT INTO TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}{db_suffix}.complextypestbl where id = 4;
INSERT INTO TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}{db_suffix}.complextypestbl where id = 5;
INSERT INTO TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}{db_suffix}.complextypestbl where id = 6;
INSERT INTO TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}{db_suffix}.complextypestbl where id = 7;
INSERT INTO TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}{db_suffix}.complextypestbl where id = 8;
ALTER TABLE {db_name}{db_suffix}.{table_name} compact 'minor';
---- TABLE_PROPERTIES
transactional=true
====
---- DATASET
functional
---- BASE_TABLE_NAME
complextypestbl_non_transactional
---- COLUMNS
id bigint
int_array array<int>
int_array_array array<array<int>>
int_map map<string, int>
int_map_array array<map<string, int>>
nested_struct struct<a: int, b: array<int>, c: struct<d: array<array<struct<e: int, f: string>>>>, g: map<string, struct<h: struct<i: array<double>>>>>
---- TABLE_PROPERTIES
transactional=false
---- DEPENDENT_LOAD
`hadoop fs -mkdir -p /test-warehouse/complextypestbl_non_transactional_orc_def && \
hadoop fs -put -f ${IMPALA_HOME}/testdata/ComplexTypesTbl/nullable.orc \
/test-warehouse/complextypestbl_non_transactional_orc_def/ && \
hadoop fs -put -f ${IMPALA_HOME}/testdata/ComplexTypesTbl/nonnullable.orc \
/test-warehouse/complextypestbl_non_transactional_orc_def/
---- LOAD
====
---- DATASET
functional
---- BASE_TABLE_NAME
complextypestbl_medium
---- COLUMNS
id bigint
int_array array<int>
int_array_array array<array<int>>
int_map map<string, int>
int_map_array array<map<string, int>>
nested_struct struct<a: int, b: array<int>, c: struct<d: array<array<struct<e: int, f: string>>>>, g: map<string, struct<h: struct<i: array<double>>>>>
---- DEPENDENT_LOAD_HIVE
-- This INSERT must run in Hive, because Impala doesn't support inserting into tables
-- with complex types.
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT c.* FROM functional_parquet.complextypestbl c join functional.alltypes sort by id;
====
---- DATASET
functional
---- BASE_TABLE_NAME
complextypes_fileformat
---- CREATE_HIVE
-- Used for positive/negative testing of complex types on various file formats.
-- In particular, queries on file formats for which we do not support complex types
-- should fail gracefully.
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  id int,
  s struct<f1:string,f2:int>,
  a array<int>,
  m map<string,bigint>)
STORED AS {file_format};
---- LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT id, named_struct("f1",string_col,"f2",int_col), array(1, 2, 3), map("k", cast(0 as bigint)) FROM functional.alltypestiny;
---- DEPENDENT_LOAD_HIVE
-- This INSERT must run in Hive, because Impala doesn't support inserting into tables
-- with complex types.
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM functional.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
complextypes_multifileformat
---- CREATE_HIVE
-- Used for positive/negative testing of complex types on various file formats.
-- In particular, queries on file formats for which we do not support complex types
-- should fail gracefully. This table allows testing at a partition granularity.
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  id int,
  s struct<f1:string,f2:int>,
  a array<int>,
  m map<string,bigint>)
PARTITIONED BY (p int)
STORED AS {file_format};
---- LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION(p=1) SELECT id, named_struct("f1",string_col,"f2",int_col), array(1, 2, 3), map("k", cast(0 as bigint)) FROM functional.alltypestiny;
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION(p=2) SELECT id, named_struct("f1",string_col,"f2",int_col), array(1, 2, 3), map("k", cast(0 as bigint)) FROM functional.alltypestiny;
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION(p=3) SELECT id, named_struct("f1",string_col,"f2",int_col), array(1, 2, 3), map("k", cast(0 as bigint)) FROM functional.alltypestiny;
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION(p=4) SELECT id, named_struct("f1",string_col,"f2",int_col), array(1, 2, 3), map("k", cast(0 as bigint)) FROM functional.alltypestiny;
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION(p=5) SELECT id, named_struct("f1",string_col,"f2",int_col), array(1, 2, 3), map("k", cast(0 as bigint)) FROM functional.alltypestiny;
-- The order of insertions and alterations is deliberately chose to work around a Hive
-- bug where the format of an altered partition is reverted back to the original format after
-- an insert. So we first do the insert, and then alter the format.
USE {db_name}{db_suffix};
ALTER TABLE {table_name} PARTITION (p=2) SET FILEFORMAT PARQUET;
ALTER TABLE {table_name} PARTITION (p=3) SET FILEFORMAT AVRO;
ALTER TABLE {table_name} PARTITION (p=4) SET FILEFORMAT RCFILE;
ALTER TABLE {table_name} PARTITION (p=5) SET FILEFORMAT ORC;
USE default;
====
---- DATASET
functional
---- BASE_TABLE_NAME
testtbl
---- COLUMNS
id bigint
name string
zip int
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
create table {db_name}{db_suffix}.{table_name} (
  id bigint primary key,
  name string null,
  zip int null
)
partition by range(id) (partition values <= 1003, partition 1003 < values <= 1007,
partition 1007 < values) stored as kudu;
====
---- DATASET
functional
---- BASE_TABLE_NAME
dimtbl
---- COLUMNS
id bigint
name string
zip int
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/DimTbl/data.csv' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
create table {db_name}{db_suffix}.{table_name} (
  id bigint primary key,
  name string,
  zip int
)
partition by range(id) (partition values <= 1003, partition 1003 < values <= 1007,
partition 1007 < values) stored as kudu;
====
---- DATASET
functional
---- BASE_TABLE_NAME
jointbl
---- COLUMNS
test_id bigint
test_name string
test_zip int
alltypes_id int
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/JoinTbl/data.csv' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
create table {db_name}{db_suffix}.{table_name} (
  test_id bigint,
  test_name string,
  test_zip int,
  alltypes_id int,
  primary key (test_id, test_name, test_zip, alltypes_id)
)
partition by range(test_id, test_name)
  (partition values <= (1003, 'Name3'),
   partition (1003, 'Name3') < values <= (1007, 'Name7'),
   partition (1007, 'Name7') < values)
stored as kudu;
====
---- DATASET
functional
---- BASE_TABLE_NAME
liketbl
---- COLUMNS
str_col string
match_like_col string
no_match_like_col string
match_regex_col string
no_match_regex_col string
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/LikeTbl/data.csv' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypessmallbinary
---- CREATE_HIVE
-- This table does not define a ':key' column spec. If one is not specified, the
-- first column is implied.
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
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
  year int,
  month int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  "d:bool_col#-,
   d:tinyint_col#-,
   d:smallint_col#-,
   d:int_col#-,
   d:bigint_col#-,
   d:float_col#-,
   d:double_col#-,
   d:date_string_col#-,
   d:string_col#-,
   d:timestamp_col#s,
   d:year#-,
   d:month#-"
)
TBLPROPERTIES ("hbase.table.name" = "functional_hbase.alltypessmallbinary",
               "hbase.table.default.storage.type" = "binary");
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * FROM functional.alltypessmall;
====
---- DATASET
functional
---- BASE_TABLE_NAME
insertalltypesaggbinary
---- CREATE_HIVE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
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
  year int,
  month int,
  day int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  ":key#b,d:bool_col#b,d:tinyint_col#b,d:smallint_col#b,d:int_col#b,d:bigint_col#b,d:float_col#b,d:double_col#b,d:date_string_col,d:string_col,d:timestamp_col,d:year#b,d:month#b,d:day#b"
)
TBLPROPERTIES("hbase.table.name" = "functional_hbase.insertalltypesaggbinary");
====
---- DATASET
functional
---- BASE_TABLE_NAME
insertalltypesagg
---- PARTITION_COLUMNS
year int
month int
day int
---- COLUMNS
id int
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
====
---- DATASET
functional
---- BASE_TABLE_NAME
stringids
---- PARTITION_COLUMNS
year int
month int
day int
---- COLUMNS
id string
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypes_view
---- CREATE
CREATE VIEW IF NOT EXISTS {db_name}{db_suffix}.{table_name}
AS SELECT * FROM {db_name}{db_suffix}.alltypes;
---- LOAD
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypes_hive_view
---- CREATE_HIVE
-- Test that Impala can handle incorrect column metadata created by Hive (IMPALA-994).
DROP VIEW IF EXISTS {db_name}{db_suffix}.{table_name};
-- Beeline cannot handle the stmt below when broken up into multiple lines.
CREATE VIEW {db_name}{db_suffix}.{table_name} AS SELECT * FROM {db_name}{db_suffix}.alltypes;
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypes_view_sub
---- CREATE
DROP VIEW IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE VIEW {db_name}{db_suffix}.{table_name} (x, y, z)
AS SELECT int_col, string_col, timestamp_col FROM {db_name}{db_suffix}.alltypes;
---- LOAD
====
---- DATASET
functional
---- BASE_TABLE_NAME
complex_view
---- CREATE
CREATE VIEW IF NOT EXISTS {db_name}{db_suffix}.complex_view
(abc COMMENT 'agg', xyz COMMENT 'gby') AS
SELECT COUNT(a.bigint_col), b.string_col FROM
{db_name}{db_suffix}.alltypesagg a INNER JOIN {db_name}{db_suffix}.alltypestiny b
ON a.id = b.id WHERE a.bigint_col < 50
GROUP BY b.string_col HAVING COUNT(a.bigint_col) > 1
ORDER BY b.string_col LIMIT 100;
---- LOAD
====
---- DATASET
functional
---- BASE_TABLE_NAME
view_view
---- CREATE
CREATE VIEW IF NOT EXISTS {db_name}{db_suffix}.{table_name}
AS SELECT * FROM {db_name}{db_suffix}.alltypes_view;
---- LOAD
====
---- DATASET
functional
---- BASE_TABLE_NAME
subquery_view
---- CREATE
CREATE VIEW IF NOT EXISTS {db_name}{db_suffix}.{table_name}
AS SELECT COUNT(*) FROM {db_name}{db_suffix}.alltypes
WHERE id IN (SELECT id FROM {db_name}{db_suffix}.alltypessmall where int_col < 5);
---- LOAD
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypes_parens
---- CREATE
CREATE VIEW IF NOT EXISTS {db_name}{db_suffix}.{table_name}
AS SELECT * FROM {db_name}{db_suffix}.alltypes
WHERE year = 2009 and (int_col < 100 OR bool_col = false) and month = 1;
---- LOAD
====
---- DATASET
functional
---- BASE_TABLE_NAME
text_comma_backslash_newline
---- COLUMNS
col1 string
col2 string
col3 int
col4 int
---- ROW_FORMAT
delimited fields terminated by ',' escaped by '\\' lines terminated by '\n'
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/text-comma-backslash-newline.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
text_dollar_hash_pipe
---- COLUMNS
col1 string
col2 string
col3 int
col4 int
---- ROW_FORMAT
delimited fields terminated by '$' escaped by '#' lines terminated by '|'
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/text-dollar-hash-pipe.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
text_thorn_ecirc_newline
---- COLUMNS
col1 string
col2 string
col3 int
col4 int
---- ROW_FORMAT
-- -2 => ASCII 254 (thorn character) and -22 is a lowercase e with a circumflex
delimited fields terminated by '-2' escaped by '-22' lines terminated by '\n'
---- LOAD
-- Hive has a bug where it will not load a table's table metadata if ESCAPED BY and
-- TERMINATED BY are specified at the same time and set to extended ASCII characters.
-- To work around this, the data file is loaded into a temp table with the same location.
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name}_tmp(i int) LOCATION '/test-warehouse/{table_name}';
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/text-thorn-ecirc-newline.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name}_tmp;
DROP TABLE {db_name}{db_suffix}.{table_name}_tmp;
====
---- DATASET
functional
---- BASE_TABLE_NAME
overflow
---- COLUMNS
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
decimal0_col DECIMAL(13,4)
decimal1_col DECIMAL(38,0)
decimal2_col DECIMAL(38,38)
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/overflow.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
widerow
---- COLUMNS
string_col string
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/widerow.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
greptiny
---- COLUMNS
field string
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/hive_benchmark/grepTiny/part-00000' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
rankingssmall
---- COLUMNS
pageRank int
pageURL string
avgDuration int
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/hive_benchmark/htmlTiny/Rankings.dat' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
uservisitssmall
---- COLUMNS
sourceIP string
destURL string
visitDate string
adRevenue float
userAgent string
cCode string
lCode string
sKeyword string
avgTimeOnSite int
---- ROW_FORMAT
delimited fields terminated by '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/hive_benchmark/htmlTiny/UserVisits.dat' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
emptytable
---- PARTITION_COLUMNS
f2 int
---- COLUMNS
field string
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  field STRING PRIMARY KEY,
  f2 INT
)
PARTITION BY HASH (field) PARTITIONS 3 STORED AS KUDU;
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypesaggmultifiles
---- PARTITION_COLUMNS
year int
month int
day int
---- COLUMNS
id int
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=1);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=2);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=3);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=4);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=5);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=6);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=7);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=8);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=9);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=10);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(year=2010, month=1, day=NULL);
---- DEPENDENT_LOAD
insert overwrite table {db_name}{db_suffix}.{table_name} partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM {db_name}.{table_name} where id % 4 = 0;
insert into table {db_name}{db_suffix}.{table_name} partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM {db_name}.{table_name} where id % 4 = 1;
insert into table {db_name}{db_suffix}.{table_name} partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM {db_name}.{table_name} where id % 4 = 2;
insert into table {db_name}{db_suffix}.{table_name} partition (year, month, day) SELECT  id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM {db_name}.{table_name} where id % 4 = 3;
---- LOAD
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
insert overwrite table {db_name}{db_suffix}.{table_name} partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM functional.alltypesagg where id % 4 = 0;
insert into table {db_name}{db_suffix}.{table_name} partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM functional.alltypesagg where id % 4 = 1;
insert into table {db_name}{db_suffix}.{table_name} partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM functional.alltypesagg where id % 4 = 2;
insert into table {db_name}{db_suffix}.{table_name} partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM functional.alltypesagg where id % 4 = 3;
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltypesaggmultifilesnopart
---- COLUMNS
id int
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
---- DEPENDENT_LOAD
insert into table {db_name}{db_suffix}.{table_name} SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM {db_name}.{table_name} where id % 4 = 0;
insert into table {db_name}{db_suffix}.{table_name} SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM {db_name}.{table_name} where id % 4 = 1;
insert into table {db_name}{db_suffix}.{table_name} SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM {db_name}.{table_name} where id % 4 = 2;
insert into table {db_name}{db_suffix}.{table_name} SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM {db_name}.{table_name} where id % 4 = 3;
---- LOAD
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table {db_name}{db_suffix}.{table_name} SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM functional.alltypesagg where id % 4 = 0;
insert into table {db_name}{db_suffix}.{table_name} SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM functional.alltypesagg where id % 4 = 1;
insert into table {db_name}{db_suffix}.{table_name} SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM functional.alltypesagg where id % 4 = 2;
insert into table {db_name}{db_suffix}.{table_name} SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM functional.alltypesagg where id % 4 = 3;
====
---- DATASET
functional
---- BASE_TABLE_NAME
stringpartitionkey
---- PARTITION_COLUMNS
string_col string
---- COLUMNS
id int
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (string_col = "partition1");
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (string_col = "2009-01-01 00:00:00");
---- LOAD
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION(string_col)
SELECT id, timestamp_col as string_col from functional.alltypestiny
WHERE timestamp_col = "2009-01-01 00:00:00";
====
---- DATASET
functional
---- BASE_TABLE_NAME
tinytable
---- COLUMNS
a string
b string
---- ROW_FORMAT
delimited fields terminated by ','
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/TinyTable/data.csv' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
create table {db_name}{db_suffix}.{table_name} (
  a string primary key,
  b string
)
partition by range(a) (partition values <= 'b', partition 'b' < values <= 'd',
partition 'd' < values) stored as kudu;
====
---- DATASET
functional
---- BASE_TABLE_NAME
tinyinttable
---- COLUMNS
int_col int
---- ROW_FORMAT
delimited fields terminated by ','
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/TinyIntTable/data.csv' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
create table {db_name}{db_suffix}.{table_name} (
  int_col int primary key
)
partition by range(int_col) (partition values <= 2, partition 2 < values <= 4,
partition 4 < values <= 6, partition 6 < values <= 8, partition 8 < values)
stored as kudu;
====
---- DATASET
functional
---- BASE_TABLE_NAME
nulltable
---- COLUMNS
a string
b string
c string
d int
e double
f string
g string
---- ROW_FORMAT
delimited fields terminated by ','
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} select * from functional.nulltable;
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/NullTable/data.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
create table {db_name}{db_suffix}.{table_name} (
  a string primary key, b string null, c string null, d int null, e double null,
  f string null, g string null
)
partition by hash(a) partitions 3 stored as kudu;
====
---- DATASET
-- Table with varying ratios of nulls. Used to test NDV with nulls
-- Also useful to test null counts as the count varies from 0 to
-- some to all rows.
functional
---- BASE_TABLE_NAME
nullrows
---- COLUMNS
id string
blank string
null_str string
null_int int
null_double double
group_str string
some_nulls string
bool_nulls boolean
---- ROW_FORMAT
delimited fields terminated by ','
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} select * from functional.nullrows;
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/NullRows/data.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
nullescapedtable
---- COLUMNS
a string
b string
c string
d int
e double
f string
g string
---- ROW_FORMAT
delimited fields terminated by ',' escaped by '\\'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} select * from functional.nulltable;
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/NullTable/data.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
create table {db_name}{db_suffix}.{table_name} (
  a string primary key, b string null, c string null, d int null, e double null,
  f string null, g string null
)
partition by hash(a) partitions 3 stored as kudu;
====
---- DATASET
functional
---- BASE_TABLE_NAME
nullformat_custom
---- CREATE_HIVE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  id int,
  a boolean,
  b string,
  c int,
  d double)
STORED AS {file_format}
TBLPROPERTIES("serialization.null.format" = "xyz");
====
---- DATASET
functional
---- BASE_TABLE_NAME
TblWithRaggedColumns
---- COLUMNS
str_col string
int_col int
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/TblWithRaggedColumns/data.csv' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
zipcode_incomes
---- COLUMNS
id STRING
zip STRING
description1 STRING
description2 STRING
income int
---- ROW_FORMAT
DELIMITED FIELDS TERMINATED BY ','
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/ImpalaDemoDataset/DEC_00_SF3_P077_with_ann_noheader.csv' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
create table {db_name}{db_suffix}.{table_name} (
  id string primary key,
  zip string null,
  description1 string null,
  description2 string null,
  income int null)
partition by range(id)
(partition values <= '8600000US01475',
 partition '8600000US01475' < values <= '8600000US63121',
 partition '8600000US63121' < values <= '8600000US84712',
 partition '8600000US84712' < values
) stored as kudu;
====
---- DATASET
functional
---- BASE_TABLE_NAME
unsupported_types
---- CREATE_HIVE
-- Create a table that mixes supported and unsupported scalar types.
-- We should be able to read the column values of supported types and
-- fail queries that reference  columns of unsupported types.
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  int_col INT,
  dec_col DECIMAL,
  date_col DATE,
  str_col STRING,
  bin_col BINARY,
  bigint_col BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS {file_format}
LOCATION '{hdfs_location}';
---- TABLE_PROPERTIES
transactional=false
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/UnsupportedTypes/data.csv' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
unsupported_partition_types
---- CREATE_HIVE
-- Create a table that is partitioned on an unsupported partition-column type
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  int_col INT)
PARTITIONED BY (t TIMESTAMP);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
old_rcfile_table
---- COLUMNS
key INT
value STRING
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/oldrcfile.rc'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_text_lzo
---- COLUMNS
field STRING
---- DEPENDENT_LOAD_HIVE
-- Error recovery test data for LZO compression.
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_text_lzo/bad_text.lzo'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_text_gzip
---- COLUMNS
s STRING
i INT
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_text_gzip/file_not_finished.gz'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_seq_snap
---- COLUMNS
field STRING
---- DEPENDENT_LOAD_HIVE
-- This data file contains format errors and is accessed by the unit test: sequence-file-recover-test.
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_seq_snap/bad_file'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_avro_snap_strings
---- COLUMNS
s STRING
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_avro_snap/negative_string_len.avro'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_avro_snap/invalid_union.avro'
INTO TABLE {db_name}{db_suffix}.{table_name};
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_avro_snap/truncated_string.avro'
INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_avro_snap_floats
---- COLUMNS
c1 FLOAT
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_avro_snap/truncated_float.avro'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_avro_decimal_schema
---- COLUMNS
name STRING
value DECIMAL(5,2)
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_avro_snap/invalid_decimal_schema.avro'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_avro_date_out_of_range
---- COLUMNS
d DATE
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_avro_snap/out_of_range_date.avro'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
hive2_bad_avro_date_pre_gregorian
---- COLUMNS
d DATE
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_avro_snap/hive2_pre_gregorian_date.avro'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
hive3_avro_date_pre_gregorian
---- COLUMNS
d DATE
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/bad_avro_snap/hive3_pre_gregorian_date.avro'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
-- IMPALA-694: uses data file produced by parquet-mr version 1.2.5-cdh4.5.0
functional
---- BASE_TABLE_NAME
bad_parquet
---- COLUMNS
field STRING
---- DEPENDENT_LOAD_HIVE
-- IMPALA-694: data file produced by parquet-mr version 1.2.5-cdh4.5.0
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/bad_parquet_data.parquet'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
-- Data file produced by parquet-mr with repeated values (produces 0 bit width dictionary)
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/repeated_values.parquet'
INTO TABLE {db_name}{db_suffix}.{table_name};
-- IMPALA-720: data file produced by parquet-mr with multiple row groups
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/multiple_rowgroups.parquet'
INTO TABLE {db_name}{db_suffix}.{table_name};
-- IMPALA-1401: data file produced by Hive 13 containing page statistics with long min/max
-- string values
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/long_page_header.parquet'
INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_parquet_strings_negative_len
---- COLUMNS
s STRING
---- DEPENDENT_LOAD_HIVE
-- IMPALA-3732: parquet files with corrupt strings
LOAD DATA LOCAL INPATH
'{impala_home}/testdata/bad_parquet_data/dict-encoded-negative-len.parq'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
LOAD DATA LOCAL INPATH
'{impala_home}/testdata/bad_parquet_data/plain-encoded-negative-len.parq'
INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_parquet_strings_out_of_bounds
---- COLUMNS
s STRING
---- DEPENDENT_LOAD_HIVE
-- IMPALA-3732: parquet files with corrupt strings
LOAD DATA LOCAL INPATH
'{impala_home}/testdata/bad_parquet_data/dict-encoded-out-of-bounds.parq'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
LOAD DATA LOCAL INPATH
'{impala_home}/testdata/bad_parquet_data/plain-encoded-out-of-bounds.parq'
INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
-- IMPALA-2130: Wrong verification of parquet file version
functional
---- BASE_TABLE_NAME
bad_magic_number
---- COLUMNS
field STRING
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/bad_magic_number.parquet'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
-- IMPALA-1658: Timestamps written by Hive are local-to-UTC adjusted.
functional
---- BASE_TABLE_NAME
alltypesagg_hive_13_1
---- COLUMNS
id int
bool_col boolean
tinyint_col tinyint
smallint_col smallint
int_col int
bigint_col bigint
float_col float
double_col double
date_string_col string
string_col string
timestamp_col timestamp
year int
month int
day int
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/alltypesagg_hive_13_1.parquet'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
-- Parquet file with invalid metadata size in the file footer.
functional
---- BASE_TABLE_NAME
bad_metadata_len
---- COLUMNS
field TINYINT
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/bad_metadata_len.parquet'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
-- Parquet file with invalid column dict_page_offset.
functional
---- BASE_TABLE_NAME
bad_dict_page_offset
---- COLUMNS
field TINYINT
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/bad_dict_page_offset.parquet'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
-- Parquet file with invalid column total_compressed_size.
functional
---- BASE_TABLE_NAME
bad_compressed_size
---- COLUMNS
field TINYINT
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/bad_compressed_size.parquet'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
-- Parquet file with required columns written by Kite. Hive and Impala always write files
-- with fields as optional.
functional
---- BASE_TABLE_NAME
kite_required_fields
---- COLUMNS
req_int bigint
opt_int bigint
req_string string
opt_string string
req_bool boolean
opt_bool boolean
opt_int_2 bigint
opt_int_3 bigint
req_int_2 bigint
req_int_3 bigint
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/kite_required_fields.parquet'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
-- Parquet file with incorrect column metadata in multiple row groups
functional
---- BASE_TABLE_NAME
bad_column_metadata
---- COLUMNS
id bigint
int_array array<int>
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/bad_column_metadata.parquet'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bad_serde
---- CREATE_HIVE
-- For incompatible SerDe testing
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (col int)
ROW FORMAT serde "org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe";
====
---- DATASET
functional
---- BASE_TABLE_NAME
rcfile_lazy_binary_serde
---- CREATE_HIVE
-- For incompatible SerDe testing
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (int_col int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat';
====
---- DATASET
functional
---- BASE_TABLE_NAME
decimal_tbl
---- COLUMNS
d1 DECIMAL
d2 DECIMAL(10, 0)
d3 DECIMAL(20, 10)
d4 DECIMAL(38, 38)
d5 DECIMAL(10, 5)
---- PARTITION_COLUMNS
d6 DECIMAL(9, 0)
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(d6=1);
---- ROW_FORMAT
delimited fields terminated by ','
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/decimal_tbl.txt'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(d6=1);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition(d6)
select * from functional.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  d1 DECIMAL,
  d2 DECIMAL(10, 0),
  d3 DECIMAL(20, 10),
  d4 DECIMAL(38, 38),
  d5 DECIMAL(10, 5),
  d6 DECIMAL(9, 0),
  PRIMARY KEY (d1, d2, d3, d4, d5, d6)
)
PARTITION BY HASH PARTITIONS 3
STORED AS KUDU;
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}
SELECT d1, d2, d3, d4, d5, d6
FROM {db_name}.{table_name};
====
---- DATASET
-- Reasonably large table with decimal values.  This is used for
-- testing min-max filters with decimal types on kudu tables
functional
---- BASE_TABLE_NAME
decimal_rtf_tbl
---- COLUMNS
d5_0   DECIMAL(5, 0)
d5_1   DECIMAL(5, 1)
d5_3   DECIMAL(5, 3)
d5_5   DECIMAL(5, 5)
d9_0   DECIMAL(9, 0)
d9_1   DECIMAL(9, 1)
d9_5   DECIMAL(9, 5)
d9_9   DECIMAL(9, 9)
d14_0  DECIMAL(14, 0)
d14_1  DECIMAL(14, 1)
d14_7  DECIMAL(14, 7)
d14_14 DECIMAL(14, 14)
d18_0  DECIMAL(18, 0)
d18_1  DECIMAL(18, 1)
d18_9  DECIMAL(18, 9)
d18_18 DECIMAL(18, 18)
d28_0  DECIMAL(28, 0)
d28_1  DECIMAL(28, 1)
d28_14 DECIMAL(28, 14)
d28_28 DECIMAL(28, 28)
d38_0  DECIMAL(38, 0)
d38_1  DECIMAL(38, 1)
d38_19 DECIMAL(38, 19)
d38_38 DECIMAL(38, 38)
---- PARTITION_COLUMNS
dpc    DECIMAL(9, 0)
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(dpc=1);
---- ROW_FORMAT
delimited fields terminated by ','
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/decimal_rtf_tbl.txt'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(dpc=1);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition(dpc)
select * from functional.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  d5_0   DECIMAL(5, 0),
  d5_1   DECIMAL(5, 1),
  d5_3   DECIMAL(5, 3),
  d5_5   DECIMAL(5, 5),
  d9_0   DECIMAL(9, 0),
  d9_1   DECIMAL(9, 1),
  d9_5   DECIMAL(9, 5),
  d9_9   DECIMAL(9, 9),
  d14_0  DECIMAL(14, 0),
  d14_1  DECIMAL(14, 1),
  d14_7  DECIMAL(14, 7),
  d14_14 DECIMAL(14, 14),
  d18_0  DECIMAL(18, 0),
  d18_1  DECIMAL(18, 1),
  d18_9  DECIMAL(18, 9),
  d18_18 DECIMAL(18, 18),
  d28_0  DECIMAL(28, 0),
  d28_1  DECIMAL(28, 1),
  d28_14 DECIMAL(28, 14),
  d28_28 DECIMAL(28, 28),
  d38_0  DECIMAL(38, 0),
  d38_1  DECIMAL(38, 1),
  d38_19 DECIMAL(38, 19),
  d38_38 DECIMAL(38, 38),
  PRIMARY KEY (d5_0, d5_1, d5_3, d5_5, d9_0, d9_1, d9_5, d9_9, d14_0, d14_1, d14_7, d14_14, d18_0, d18_1, d18_9, d18_18, d28_0, d28_1, d28_14, d28_28, d38_0, d38_1, d38_19, d38_38)
)
PARTITION BY HASH PARTITIONS 10
STORED AS KUDU;
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}
SELECT d5_0, d5_1, d5_3, d5_5, d9_0, d9_1, d9_5, d9_9, d14_0, d14_1, d14_7, d14_14, d18_0, d18_1, d18_9, d18_18, d28_0, d28_1, d28_14, d28_28, d38_0, d38_1, d38_19, d38_38
FROM {db_name}.{table_name};
====
---- DATASET
-- Small table with decimal values.  This is used for
-- testing min-max filters with decimal types on kudu tables
functional
---- BASE_TABLE_NAME
decimal_rtf_tiny_tbl
---- COLUMNS
d5_0   DECIMAL(5, 0)
d5_1   DECIMAL(5, 1)
d5_3   DECIMAL(5, 3)
d5_5   DECIMAL(5, 5)
d9_0   DECIMAL(9, 0)
d9_1   DECIMAL(9, 1)
d9_5   DECIMAL(9, 5)
d9_9   DECIMAL(9, 9)
d14_0  DECIMAL(14, 0)
d14_1  DECIMAL(14, 1)
d14_7  DECIMAL(14, 7)
d14_14 DECIMAL(14, 14)
d18_0  DECIMAL(18, 0)
d18_1  DECIMAL(18, 1)
d18_9  DECIMAL(18, 9)
d18_18 DECIMAL(18, 18)
d28_0  DECIMAL(28, 0)
d28_1  DECIMAL(28, 1)
d28_14 DECIMAL(28, 14)
d28_28 DECIMAL(28, 28)
d38_0  DECIMAL(38, 0)
d38_1  DECIMAL(38, 1)
d38_19 DECIMAL(38, 19)
d38_38 DECIMAL(38, 38)
---- PARTITION_COLUMNS
dpc    DECIMAL(9, 0)
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(dpc=1);
---- ROW_FORMAT
delimited fields terminated by ','
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/decimal_rtf_tiny_tbl.txt'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(dpc=1);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition(dpc)
select * from functional.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  d5_0   DECIMAL(5, 0),
  d5_1   DECIMAL(5, 1),
  d5_3   DECIMAL(5, 3),
  d5_5   DECIMAL(5, 5),
  d9_0   DECIMAL(9, 0),
  d9_1   DECIMAL(9, 1),
  d9_5   DECIMAL(9, 5),
  d9_9   DECIMAL(9, 9),
  d14_0  DECIMAL(14, 0),
  d14_1  DECIMAL(14, 1),
  d14_7  DECIMAL(14, 7),
  d14_14 DECIMAL(14, 14),
  d18_0  DECIMAL(18, 0),
  d18_1  DECIMAL(18, 1),
  d18_9  DECIMAL(18, 9),
  d18_18 DECIMAL(18, 18),
  d28_0  DECIMAL(28, 0),
  d28_1  DECIMAL(28, 1),
  d28_14 DECIMAL(28, 14),
  d28_28 DECIMAL(28, 28),
  d38_0  DECIMAL(38, 0),
  d38_1  DECIMAL(38, 1),
  d38_19 DECIMAL(38, 19),
  d38_38 DECIMAL(38, 38),
  PRIMARY KEY (d5_0, d5_1, d5_3, d5_5, d9_0, d9_1, d9_5, d9_9, d14_0, d14_1, d14_7, d14_14, d18_0, d18_1, d18_9, d18_18, d28_0, d28_1, d28_14, d28_28, d38_0, d38_1, d38_19, d38_38)
)
PARTITION BY HASH PARTITIONS 10
STORED AS KUDU;
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}
SELECT d5_0, d5_1, d5_3, d5_5, d9_0, d9_1, d9_5, d9_9, d14_0, d14_1, d14_7, d14_14, d18_0, d18_1, d18_9, d18_18, d28_0, d28_1, d28_14, d28_28, d38_0, d38_1, d38_19, d38_38
FROM {db_name}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
decimal_tiny
---- COLUMNS
c1 DECIMAL(10, 4)
c2 DECIMAL(15, 5)
c3 DECIMAL(1,1)
---- ROW_FORMAT
delimited fields terminated by ','
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/decimal-tiny.txt'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
select * from functional.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  c1 DECIMAL(10, 4),
  c2 DECIMAL(15, 5),
  c3 DECIMAL(1, 1),
  PRIMARY KEY (c1, c2, c3)
)
PARTITION BY HASH PARTITIONS 3
STORED AS KUDU;
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}
SELECT c1, c2, c3
FROM {db_name}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
parent_table
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
id INT, year string, primary key(id, year) DISABLE NOVALIDATE RELY)
row format delimited fields terminated by ','
LOCATION '/test-warehouse/{table_name}';
---- ROW_FORMAT
delimited fields terminated by '',''
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/parent_table.txt'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
parent_table_2
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
a INT, primary key(a) DISABLE NOVALIDATE RELY)
row format delimited fields terminated by ','
LOCATION '/test-warehouse/{table_name}';
---- ROW_FORMAT
delimited fields terminated by ','
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/parent_table_2.txt'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
child_table
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
seq int, id int, year string, a int, primary key(seq) DISABLE NOVALIDATE RELY, foreign key
(id, year) references {db_name}{db_suffix}.parent_table(id, year) DISABLE NOVALIDATE
RELY, foreign key(a) references {db_name}{db_suffix}.parent_table_2(a) DISABLE
NOVALIDATE RELY)
row format delimited fields terminated by ','
LOCATION '/test-warehouse/{table_name}';
---- ROW_FORMAT
delimited fields terminated by ','
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/child_table.txt'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
chars_tiny
---- COLUMNS
cs CHAR(5)
cl CHAR(140)
vc VARCHAR(32)
---- ROW_FORMAT
delimited fields terminated by ','
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/chars-tiny.txt'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
select * from functional.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
widetable_250_cols
---- COLUMNS
`${IMPALA_HOME}/testdata/common/widetable.py --get_columns -n 250
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
select * from functional.{table_name};
---- LOAD
`${IMPALA_HOME}/testdata/common/widetable.py --create_data -n 250 -o /tmp/widetable_data.csv
====
---- DATASET
functional
---- BASE_TABLE_NAME
widetable_500_cols
---- COLUMNS
`${IMPALA_HOME}/testdata/common/widetable.py --get_columns -n 500
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
select * from functional.{table_name};
---- LOAD
`${IMPALA_HOME}/testdata/common/widetable.py --create_data -n 500 -o /tmp/widetable_data.csv
====
---- DATASET
functional
---- BASE_TABLE_NAME
widetable_1000_cols
---- COLUMNS
`${IMPALA_HOME}/testdata/common/widetable.py --get_columns -n 1000
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
select * from functional.{table_name};
---- LOAD
`${IMPALA_HOME}/testdata/common/widetable.py --create_data -n 1000 -o /tmp/widetable_data.csv
====
---- DATASET
functional
---- BASE_TABLE_NAME
avro_decimal_tbl
---- COLUMNS
name STRING
value DECIMAL(5,2)
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/avro_decimal_tbl.avro'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
no_avro_schema
---- CREATE_HIVE
-- Avro schema is inferred from the column definitions (IMPALA-1136)
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
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
timestamp_col string)
PARTITIONED BY (year int, month int)
STORED AS AVRO
LOCATION '/test-warehouse/alltypes_avro_snap';
---- ALTER
-- The second partition is added twice because there seems to be a Hive/beeline
-- bug where the last alter is not executed properly.
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (year=2009,month=9);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (year=2010,month=10);
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (year=2010,month=10);
====
---- DATASET
functional
---- BASE_TABLE_NAME
table_no_newline
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
id INT, col_1 BOOLEAN, col_2 DOUBLE, col_3 TIMESTAMP)
row format delimited fields terminated by ','
LOCATION '/test-warehouse/{table_name}';
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/table_no_newline.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
table_no_newline_part
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
id INT, col_1 BOOLEAN, col_2 DOUBLE, col_3 TIMESTAMP)
partitioned by (year INT, month INT)
row format delimited fields terminated by ','
LOCATION '/test-warehouse/{table_name}';
ALTER TABLE {db_name}{db_suffix}.{table_name} ADD IF NOT EXISTS PARTITION (year=2015, month=3);
ALTER TABLE {db_name}{db_suffix}.{table_name} ADD IF NOT EXISTS PARTITION (year=2010, month=3);
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/table_no_newline.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2010, month=3);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/table_missing_columns.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(year=2015, month=3);
====
---- DATASET
functional
---- BASE_TABLE_NAME
insert_only_transactional_table
---- HIVE_MAJOR_VERSION
3
---- CREATE_HIVE
---- COLUMNS
col1 int
---- TABLE_PROPERTIES
transactional=true
transactional_properties=insert_only
---- LOAD
-- TODO(todd) we need an empty load section with a comment in it here.
-- This works around some "logic" in generate-schema-statements.py that
-- says that, if a table has no LOAD section, it shouldn't be in non-text
-- formats.
====
---- DATASET
functional
---- BASE_TABLE_NAME
full_transactional_table
---- HIVE_MAJOR_VERSION
3
---- CREATE_HIVE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col1 int)
STORED AS ORC
TBLPROPERTIES('transactional'='true');
====
---- DATASET
functional
---- BASE_TABLE_NAME
materialized_view
---- HIVE_MAJOR_VERSION
3
---- CREATE_HIVE
CREATE MATERIALIZED VIEW IF NOT EXISTS {db_name}{db_suffix}.{table_name}
  AS SELECT * FROM {db_name}{db_suffix}.insert_only_transactional_table;
====
---- DATASET
functional
---- BASE_TABLE_NAME
insert_only_transactional_bucketed_table
---- HIVE_MAJOR_VERSION
3
---- CREATE_HIVE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col1 int, col2 int)
CLUSTERED BY (col1) INTO 5 BUCKETS
STORED AS ORC
TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only');
====
---- DATASET
functional
---- BASE_TABLE_NAME
bucketed_ext_table
---- CREATE_HIVE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col1 int, col2 int)
CLUSTERED BY (col1) INTO 5 BUCKETS
STORED AS {file_format}
LOCATION '/test-warehouse/{db_name}{db_suffix}{table_name}';
====
---- DATASET
functional
---- BASE_TABLE_NAME
bucketed_table
---- CREATE_HIVE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col1 int, col2 int)
CLUSTERED BY (col1) INTO 5 BUCKETS
STORED AS {file_format};
---- LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT id, int_col from functional.alltypes;
---- DEPENDENT_LOAD_HIVE
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT * from functional.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
uncomp_src_alltypes
---- CREATE_HIVE
CREATE TABLE {db_name}{db_suffix}.{table_name} LIKE functional.alltypes STORED AS ORC;
---- DEPENDENT_LOAD_HIVE
SET orc.compress=NONE;
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM functional.alltypes;
====
---- DATASET
functional
---- BASE_TABLE_NAME
uncomp_src_decimal_tbl
---- CREATE_HIVE
CREATE TABLE {db_name}{db_suffix}.{table_name} LIKE functional.decimal_tbl STORED AS ORC;
---- DEPENDENT_LOAD_HIVE
SET orc.compress=NONE;
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (d6)
SELECT d1, d2, d3, d4, d5, d6 FROM functional.decimal_tbl;
====
---- DATASET
functional
---- BASE_TABLE_NAME
testescape_16_lf
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col string)
row format delimited fields terminated by ','  escaped by '\\'
LOCATION '/test-warehouse/{table_name}';
---- LOAD
`${IMPALA_HOME}/testdata/common/text_delims_table.py --table_dir '/tmp/testescape_16_lf' --file_len 16 --only_newline
====
---- DATASET
functional
---- BASE_TABLE_NAME
testescape_16_crlf
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col string)
row format delimited fields terminated by ','  escaped by '\\'
LOCATION '/test-warehouse/{table_name}';
---- LOAD
`${IMPALA_HOME}/testdata/common/text_delims_table.py --table_dir '/tmp/testescape_16_crlf' --file_len 16
====
---- DATASET
functional
---- BASE_TABLE_NAME
testescape_17_lf
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col string)
row format delimited fields terminated by ','  escaped by '\\'
LOCATION '/test-warehouse/{table_name}';
---- LOAD
`${IMPALA_HOME}/testdata/common/text_delims_table.py --table_dir '/tmp/testescape_17_lf' --file_len 17 --only_newline
====
---- DATASET
functional
---- BASE_TABLE_NAME
testescape_17_crlf
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col string)
row format delimited fields terminated by ','  escaped by '\\'
LOCATION '/test-warehouse/{table_name}';
---- LOAD
`${IMPALA_HOME}/testdata/common/text_delims_table.py --table_dir '/tmp/testescape_17_crlf' --file_len 17
====
---- DATASET
functional
---- BASE_TABLE_NAME
testescape_32_lf
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col string)
row format delimited fields terminated by ','  escaped by '\\'
LOCATION '/test-warehouse/{table_name}';
---- LOAD
`${IMPALA_HOME}/testdata/common/text_delims_table.py --table_dir '/tmp/testescape_32_lf' --file_len 32 --only_newline
====
---- DATASET
functional
---- BASE_TABLE_NAME
testescape_32_crlf
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  col string)
row format delimited fields terminated by ','  escaped by '\\'
LOCATION '/test-warehouse/{table_name}';
---- LOAD
`${IMPALA_HOME}/testdata/common/text_delims_table.py --table_dir '/tmp/testescape_32_crlf' --file_len 32
====
---- DATASET
functional
---- BASE_TABLE_NAME
alltimezones
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
timezone STRING, utctime TIMESTAMP, localtime TIMESTAMP)
row format delimited fields terminated by ','
LOCATION '/test-warehouse/{table_name}';
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/timezoneverification.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
avro_unicode_nulls
---- CREATE_HIVE
create external table if not exists {db_name}{db_suffix}.{table_name} like {db_name}{db_suffix}.liketbl stored as avro LOCATION '{hdfs_location}';
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/avro_null_char/000000_0'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
-- IMPALA-1881: Maximize data locality when scanning Parquet files with multiple row groups.
functional
---- BASE_TABLE_NAME
lineitem_multiblock
---- COLUMNS
L_ORDERKEY BIGINT
L_PARTKEY BIGINT
L_SUPPKEY BIGINT
L_LINENUMBER INT
L_QUANTITY DECIMAL(12,2)
L_EXTENDEDPRICE DECIMAL(12,2)
L_DISCOUNT DECIMAL(12,2)
L_TAX DECIMAL(12,2)
L_RETURNFLAG STRING
L_LINESTATUS STRING
L_SHIPDATE STRING
L_COMMITDATE STRING
L_RECEIPTDATE STRING
L_SHIPINSTRUCT STRING
L_SHIPMODE STRING
L_COMMENT STRING
====
---- DATASET
-- IMPALA-2466: Add more tests to the HDFS Parquet scanner
functional
---- BASE_TABLE_NAME
lineitem_sixblocks
---- COLUMNS
L_ORDERKEY BIGINT
L_PARTKEY BIGINT
L_SUPPKEY BIGINT
L_LINENUMBER INT
L_QUANTITY DECIMAL(12,2)
L_EXTENDEDPRICE DECIMAL(12,2)
L_DISCOUNT DECIMAL(12,2)
L_TAX DECIMAL(12,2)
L_RETURNFLAG STRING
L_LINESTATUS STRING
L_SHIPDATE STRING
L_COMMITDATE STRING
L_RECEIPTDATE STRING
L_SHIPINSTRUCT STRING
L_SHIPMODE STRING
L_COMMENT STRING
====
---- DATASET
-- IMPALA-2466: Add more tests to the HDFS Parquet scanner (this has only one row group)
functional
---- BASE_TABLE_NAME
lineitem_multiblock_one_row_group
---- COLUMNS
L_ORDERKEY BIGINT
L_PARTKEY BIGINT
L_SUPPKEY BIGINT
L_LINENUMBER INT
L_QUANTITY DECIMAL(12,2)
L_EXTENDEDPRICE DECIMAL(12,2)
L_DISCOUNT DECIMAL(12,2)
L_TAX DECIMAL(12,2)
L_RETURNFLAG STRING
L_LINESTATUS STRING
L_SHIPDATE STRING
L_COMMITDATE STRING
L_RECEIPTDATE STRING
L_SHIPINSTRUCT STRING
L_SHIPMODE STRING
L_COMMENT STRING
====
---- DATASET
-- IMPALA-4933: tests nested collections stored in multiple row-groups.
---- BASE_TABLE_NAME
customer_multiblock
---- COLUMNS
C_CUSTKEY BIGINT
C_NAME STRING
C_ADDRESS STRING
C_NATIONKEY SMALLINT
C_PHONE STRING
C_ACCTBAL DECIMAL(12, 2)
C_MKTSEGMENT STRING
C_COMMENT STRING
C_ORDERS ARRAY<STRUCT<O_ORDERKEY: BIGINT, O_ORDERSTATUS: STRING, O_TOTALPRICE: DECIMAL(12, 2), O_ORDERDATE: STRING, O_ORDERPRIORITY: STRING, O_CLERK: STRING, O_SHIPPRIORITY: INT, O_COMMENT: STRING, O_LINEITEMS: ARRAY<STRUCT<L_PARTKEY: BIGINT, L_SUPPKEY: BIGINT, L_LINENUMBER: INT, L_QUANTITY: DECIMAL(12, 2), L_EXTENDEDPRICE: DECIMAL(12, 2), L_DISCOUNT: DECIMAL(12, 2), L_TAX: DECIMAL(12, 2), L_RETURNFLAG: STRING, L_LINESTATUS: STRING, L_SHIPDATE: STRING, L_COMMITDATE: STRING, L_RECEIPTDATE: STRING, L_SHIPINSTRUCT: STRING, L_SHIPMODE: STRING, L_COMMENT: STRING>>>>
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/CustomerMultiBlock/customer_multiblock.parquet'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
bzip2_tbl
---- COLUMNS
col string
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/data-bzip2.bz2'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
large_bzip2_tbl
---- COLUMNS
col string
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/large_bzip2.bz2'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
multistream_bzip2_tbl
---- COLUMNS
col string
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/data-pbzip2.bz2'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
large_multistream_bzip2_tbl
---- COLUMNS
col string
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/large_pbzip2.bz2'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
table_with_header
---- COLUMNS
c1 int
c2 double
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- ALTER
ALTER TABLE {table_name} SET TBLPROPERTIES('skip.header.line.count'='1');
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/table_with_header.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/table_with_header.gz'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
table_with_header_2
---- COLUMNS
c1 int
c2 double
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- ALTER
ALTER TABLE {table_name} SET TBLPROPERTIES('skip.header.line.count'='2');
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/table_with_header_2.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- DEPENDENT_LOAD_HIVE
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/table_with_header_2.gz'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
table_with_header_insert
---- CREATE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (i1 integer)
STORED AS {file_format}
TBLPROPERTIES('skip.header.line.count'='2');
====
---- DATASET
functional
---- BASE_TABLE_NAME
strings_with_quotes
---- COLUMNS
s string
i int
---- ROW_FORMAT
delimited fields terminated by ','  escaped by '\\'
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/strings_with_quotes.csv'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT s, i
FROM {db_name}.{table_name};
---- CREATE_KUDU
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name};
CREATE TABLE {db_name}{db_suffix}.{table_name} (
  s string PRIMARY KEY,
  i int
)
PARTITION BY HASH (s) PARTITIONS 3 STORED AS KUDU;
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}
SELECT s, i
FROM {db_name}.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
part_strings_with_quotes
---- COLUMNS
i int
---- PARTITION_COLUMNS
p string
---- LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (p="\"") VALUES (1);
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (p='\'') VALUES (2);
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (p="\\\"") VALUES (3);
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION (p='\\\'') VALUES (4);
====
---- DATASET
functional
---- BASE_TABLE_NAME
manynulls
---- COLUMNS
id int
nullcol int
---- ALTER
-- Ensure the nulls are clustered together.
ALTER TABLE {table_name} SORT BY (id);
---- CREATE_KUDU
DROP VIEW IF EXISTS {db_name}{db_suffix}.{table_name};
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name}_idx;

CREATE TABLE {db_name}{db_suffix}.{table_name}_idx (
  kudu_idx BIGINT PRIMARY KEY,
  id INT,
  nullcol INT NULL
)
PARTITION BY HASH (kudu_idx) PARTITIONS 3 STORED AS KUDU;
CREATE VIEW {db_name}{db_suffix}.{table_name} AS
SELECT id, nullcol
FROM {db_name}{db_suffix}.{table_name}_idx;
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT id, nullcol
FROM {db_name}.{table_name};
---- DEPENDENT_LOAD_KUDU
INSERT into TABLE {db_name}{db_suffix}.{table_name}_idx
SELECT row_number() over (order by id),
       id, nullcol
FROM {db_name}.{table_name};
---- LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
SELECT id, if((id div 500) % 2 = 0, NULL, id) as nullcol
FROM functional.alltypesagg;
====
---- DATASET
functional
---- BASE_TABLE_NAME
chars_medium
---- COLUMNS
id int
date_char_col char(8)
char_col char(3)
date_varchar_col varchar(8)
varchar_col varchar(3)
---- DEPENDENT_LOAD
insert overwrite table {db_name}{db_suffix}.{table_name}
select id, date_char_col, char_col, date_varchar_col, varchar_col
from {db_name}.{table_name};
---- LOAD
insert overwrite table {db_name}{db_suffix}.{table_name}
select id, date_string_col, case when id % 3 in (0, 1) then string_col end, date_string_col, case when id % 3 = 0 then string_col end
from functional.alltypesagg;
====
---- DATASET
functional
---- BASE_TABLE_NAME
date_tbl
---- PARTITION_COLUMNS
date_part DATE
---- COLUMNS
id_col INT
date_col DATE
---- ALTER
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(date_part='0001-01-01');
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(date_part='1399-06-27');
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(date_part='2017-11-27');
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(date_part='9999-12-31');
---- ROW_FORMAT
delimited fields terminated by ','
---- HBASE_REGION_SPLITS
'1','3','5','7','9'
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/date_tbl/0000.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(date_part='0001-01-01');
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/date_tbl/0001.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(date_part='1399-06-27');
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/date_tbl/0002.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(date_part='2017-11-27');
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/date_tbl/0003.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(date_part='9999-12-31');
---- DEPENDENT_LOAD
insert overwrite table {db_name}{db_suffix}.{table_name} partition(date_part)
select id_col, date_col, date_part from functional.{table_name};
====
---- DATASET
functional
---- BASE_TABLE_NAME
date_tbl_error
---- CREATE
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name} (
  id_col int,
  date_col date)
partitioned by (date_part date)
row format delimited fields terminated by ','  escaped by '\\'
stored as {file_format}
LOCATION '{hdfs_location}';
USE {db_name}{db_suffix};
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(date_part='0001-01-01');
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(date_part='1399-06-27');
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(date_part='2017-11-27');
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(date_part='9999-12-31');

-- Create external temp table with desired file format with same data file location
-- Tmp tables must not specify an escape character we don't want any
-- data transformation to happen when inserting it into tmp tables.
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name}_tmp (
  id_col STRING,
  date_col STRING)
PARTITIONED BY (date_part DATE)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS {file_format}
LOCATION '{hdfs_location}';

-- Make metastore aware of the partition directories for the temp table
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION(date_part='0001-01-01');
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION(date_part='1399-06-27');
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION(date_part='2017-11-27');
ALTER TABLE {table_name}_tmp ADD IF NOT EXISTS PARTITION(date_part='9999-12-31');
---- DEPENDENT_LOAD
USE {db_name}{db_suffix};
-- Step 4: Stream the data from tmp text table to desired format tmp table
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}_tmp PARTITION (date_part)
SELECT * FROM {db_name}.{table_name}_tmp;

-- Cleanup the temp table
DROP TABLE IF EXISTS {db_name}{db_suffix}.{table_name}_tmp;
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/date_tbl_error/0000.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(date_part='0001-01-01');
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/date_tbl_error/0001.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(date_part='1399-06-27');
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/date_tbl_error/0002.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(date_part='2017-11-27');
LOAD DATA LOCAL INPATH '{impala_home}/testdata/data/date_tbl_error/0003.txt' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(date_part='9999-12-31');
====
---- DATASET
functional
---- BASE_TABLE_NAME
insert_date_tbl
---- PARTITION_COLUMNS
date_part DATE
---- COLUMNS
id_col INT
date_col DATE
====
---- DATASET
functional
---- BASE_TABLE_NAME
hudi_partitioned
---- CREATE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name}
LIKE PARQUET '/test-warehouse/hudi_parquet/year=2015/month=03/day=16/5f541af5-ca07-4329-ad8c-40fa9b353f35-0_2-103-391_20200210090618.parquet'
PARTITIONED BY (year int, month int, day int)
STORED AS HUDIPARQUET
LOCATION '/test-warehouse/hudi_parquet';
ALTER TABLE {db_name}{db_suffix}.{table_name} RECOVER PARTITIONS;
---- DEPENDENT_LOAD
`hadoop fs -mkdir -p /test-warehouse/hudi_parquet && \
hadoop fs -put -f ${IMPALA_HOME}/testdata/data/hudi_parquet /test-warehouse/
====
---- DATASET
functional
---- BASE_TABLE_NAME
hudi_non_partitioned
---- CREATE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name}
LIKE PARQUET '/test-warehouse/hudi_parquet/year=2015/month=03/day=16/5f541af5-ca07-4329-ad8c-40fa9b353f35-0_2-103-391_20200210090618.parquet'
STORED AS HUDIPARQUET
LOCATION '/test-warehouse/hudi_parquet';
====
---- DATASET
functional
---- BASE_TABLE_NAME
hudi_as_parquet
---- CREATE
CREATE TABLE IF NOT EXISTS {db_name}{db_suffix}.{table_name}
LIKE PARQUET '/test-warehouse/hudi_parquet/year=2015/month=03/day=16/5f541af5-ca07-4329-ad8c-40fa9b353f35-0_2-103-391_20200210090618.parquet'
STORED AS PARQUET
LOCATION '/test-warehouse/hudi_parquet';
====
