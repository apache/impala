functional
----
alltypes
----
CREATE EXTERNAL TABLE %(table_name)s (
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
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';

ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=1);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=2);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=3);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=4);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=5);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=6);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=7);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=8);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=9);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=10);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=11);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=12);

ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=2);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=3);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=4);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=5);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=6);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=7);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=8);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=9);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=10);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=11);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=12);
----
INSERT OVERWRITE TABLE %(table_name)s partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM %(base_table_name)s;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  partition (year, month) \
  select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month \
  FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090101.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090201.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090301.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090401.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090501.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090601.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090701.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090801.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090901.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/091001.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=10);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/091101.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=11);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/091201.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=12);

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100101.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100201.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100301.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100401.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100501.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100601.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100701.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100801.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100901.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/101001.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=10);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/101101.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=11);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/101201.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=12);
----
ANALYZE TABLE %(table_name)s PARTITION(year, month) COMPUTE STATISTICS;
====
functional
----
alltypesnopart
----
CREATE EXTERNAL TABLE %(table_name)s (
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
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
----
----
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
alltypessmall
----
CREATE EXTERNAL TABLE %(table_name)s (
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
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';

ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=1);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=2);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=3);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=4);
----
INSERT OVERWRITE TABLE %(table_name)s partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM %(base_table_name)s;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  partition (year, month) \
  select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month \
  FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090101.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090201.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090301.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090401.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=4);
----
ANALYZE TABLE %(table_name)s PARTITION(year, month) COMPUTE STATISTICS;
====
functional
----
alltypestiny
----
CREATE EXTERNAL TABLE %(table_name)s (
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
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';

ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=1);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=2);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=3);
ALTER TABLE %(table_name)s ADD PARTITION(year=2009, month=4);
----
INSERT OVERWRITE TABLE %(table_name)s partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM %(base_table_name)s;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  partition (year, month) \
  select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month \
  FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesTiny/090101.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesTiny/090201.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesTiny/090301.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesTiny/090401.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=4);
----
ANALYZE TABLE %(table_name)s PARTITION(year, month) COMPUTE STATISTICS;
====
functional
----
alltypesinsert
----
CREATE TABLE %(table_name)s LIKE AllTypes;
----
----
----
----
ANALYZE TABLE %(table_name)s PARTITION(year, month) COMPUTE STATISTICS;
====
functional
----
alltypesnopart_insert
----
CREATE TABLE %(table_name)s like AllTypesNoPart;
----
----
----
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
insert_overwrite_nopart
----
CREATE TABLE %(table_name)s (col1 int);
----
----
----
----
====
functional
----
insert_overwrite_partitioned
----
CREATE TABLE %(table_name)s (col1 int) PARTITIONED BY (col2 int);
----
----
----
----
====
functional
----
alltypeserror
----
CREATE EXTERNAL TABLE %(table_name)s (
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
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';

ALTER TABLE %(table_name)s ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

-- Create external temp table with desired file format with same data file location
DROP TABLE IF EXISTS %(table_name)s_tmp;
CREATE EXTERNAL TABLE %(table_name)s_tmp (
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
STORED AS %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
-- Create a temp table in text format that interprets the data as strings
DROP TABLE IF EXISTS %(base_table_name)s_tmp;
CREATE EXTERNAL TABLE %(base_table_name)s_tmp (
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
STORED AS TEXTFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(base_table_name)s';

-- Make metastore aware of the partition directories
ALTER TABLE %(base_table_name)s_tmp ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

-- Step 4: Stream the data from tmp text table to desired format tmp table
INSERT OVERWRITE TABLE %(table_name)s_tmp PARTITION (year, month)
SELECT * FROM %(base_table_name)s_tmp;

-- Cleanup the temp tables
DROP TABLE %(base_table_name)s_tmp;
DROP TABLE %(table_name)s_tmp;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  partition (year, month) \
  select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month \
  FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesError/0901.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesError/0902.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesError/0903.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=3);
----
====
functional
----
alltypeserrornonulls
----
CREATE EXTERNAL TABLE %(table_name)s (
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
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';

-- Make metastore aware of the new partitions directories
ALTER TABLE %(table_name)s ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

-- Create external temp table with desired file format with same data file location
DROP TABLE IF EXISTS %(table_name)s_tmp;
CREATE EXTERNAL TABLE %(table_name)s_tmp (
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
STORED AS %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
-- Create a temp table in text format that interprets the data as strings
DROP TABLE IF EXISTS %(base_table_name)s_tmp;
CREATE EXTERNAL TABLE %(base_table_name)s_tmp (
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
STORED AS TEXTFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(base_table_name)s';

-- Make metastore aware of the partition directories
ALTER TABLE %(base_table_name)s_tmp ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

-- Step 4: Stream the data from tmp text table to desired format tmp table
INSERT OVERWRITE TABLE %(table_name)s_tmp PARTITION (year, month)
SELECT * FROM %(base_table_name)s_tmp;

-- Cleanup the temp tables
DROP TABLE %(base_table_name)s_tmp;
DROP TABLE %(table_name)s_tmp;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  partition (year, month) \
  select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month \
  FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesErrorNoNulls/0901.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesErrorNoNulls/0902.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesErrorNoNulls/0903.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2009, month=3);
----
====
functional
----
alltypesagg
----
CREATE EXTERNAL TABLE %(table_name)s (
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
partitioned by (year int, month int, day int)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=1);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=2);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=3);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=4);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=5);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=6);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=7);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=8);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=9);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=10);
----
INSERT OVERWRITE TABLE %(table_name)s partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM %(base_table_name)s;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  partition (year, month, day) \
  select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day \
  FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100101.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100102.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100103.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100104.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100105.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100106.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100107.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100108.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100109.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100110.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=10);
----
ANALYZE TABLE %(table_name)s PARTITION(year, month, day) COMPUTE STATISTICS;
====
functional
----
alltypesaggnonulls
----
CREATE EXTERNAL TABLE %(table_name)s (
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
partitioned by (year int, month int, day int)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=1);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=2);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=3);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=4);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=5);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=6);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=7);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=8);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=9);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=10);
----
INSERT OVERWRITE TABLE %(table_name)s partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM %(base_table_name)s;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  partition (year, month, day) \
  select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day \
  FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100101.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100102.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100103.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100104.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100105.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100106.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100107.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100108.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100109.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100110.txt' OVERWRITE INTO TABLE %(table_name)s PARTITION(year=2010, month=1, day=10);
----
ANALYZE TABLE %(table_name)s PARTITION(year, month, day) COMPUTE STATISTICS;
====
functional
----
testtbl
----
-- testtbl is empty
CREATE EXTERNAL TABLE %(table_name)s (
  id bigint,
  name string,
  zip int)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
----
----
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
dimtbl
----
CREATE EXTERNAL TABLE %(table_name)s (
  id bigint,
  name string,
  zip int)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/DimTbl/data.csv' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
jointbl
----
CREATE EXTERNAL TABLE %(table_name)s (
  test_id bigint,
  test_name string,
  test_zip int,
  alltypes_id int)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/JoinTbl/data.csv' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
testdb1.alltypes
----
CREATE DATABASE IF NOT EXISTS testdb1;
CREATE EXTERNAL TABLE %(table_name)s (
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
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
----
----
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
testdb1.testtbl
----
CREATE DATABASE IF NOT EXISTS testdb1;
CREATE EXTERNAL TABLE %(table_name)s (
  id bigint,
  name string,
  birthday string)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
----
----
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
liketbl
----
CREATE EXTERNAL TABLE %(table_name)s (
  str_col string,
  match_like_col string,
  no_match_like_col string,
  match_regex_col string,
  no_match_regex_col string)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/LikeTbl/data.csv' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
hbasealltypessmall
----
CREATE EXTERNAL TABLE %(table_name)s (
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
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypessmall");
----
----
----
INSERT OVERWRITE TABLE %(table_name)s
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col
FROM alltypessmall;
----
====
functional
----
hbasealltypeserror
----
CREATE EXTERNAL TABLE %(table_name)s (
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
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypeserror");
----
----
----
----
====
functional
----
hbasealltypeserrornonulls
----
CREATE EXTERNAL TABLE %(table_name)s (
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
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypeserrornonulls");
----
----
----
----
====
functional
----
hbasealltypesagg
----
CREATE EXTERNAL TABLE %(table_name)s (
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
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypesagg");
----
----
----
INSERT OVERWRITE TABLE %(table_name)s
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col
FROM alltypesagg;
----
====
functional
----
hbasestringids
----
CREATE EXTERNAL TABLE %(table_name)s (
  id string,
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
  ":key,bools:bool_col,ints:tinyint_col,ints:smallint_col,ints:int_col,ints:bigint_col,floats:float_col,floats:double_col,strings:date_string_col,strings:string_col,strings:timestamp_col"
)
TBLPROPERTIES("hbase.table.name" = "hbasealltypesagg");
----
----
----
----
====
functional
----
escapenoquotes
----
CREATE EXTERNAL TABLE %(table_name)s (
  col1 string,
  col2 string,
  col3 int,
  col4 int)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/data/escape-no-quotes.txt' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
overflow
----
CREATE EXTERNAL TABLE %(table_name)s (
  tinyint_col tinyint,
  smallint_col smallint,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/data/overflow.txt' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
greptiny
----
CREATE EXTERNAL TABLE %(table_name)s (
  field string);
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grepTiny/part-00000' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
rankingssmall
----
CREATE EXTERNAL TABLE %(table_name)s (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/htmlTiny/Rankings.dat' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
uservisitssmall
----
CREATE EXTERNAL TABLE %(table_name)s (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
row format delimited fields terminated by '|'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/htmlTiny/UserVisits.dat' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
emptytable
----
CREATE EXTERNAL TABLE %(table_name)s (
  field string)
partitioned by (f2 int);
----
----
----
----
ANALYZE TABLE %(table_name)s PARTITION(f2) COMPUTE STATISTICS;
====
functional
----
alltypesaggmultifiles
----
CREATE EXTERNAL TABLE %(table_name)s (
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
partitioned by (year int, month int, day int)
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';

ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=1);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=2);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=3);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=4);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=5);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=6);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=7);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=8);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=9);
ALTER TABLE %(table_name)s ADD PARTITION(year=2010, month=1, day=10);
----
insert into table %(table_name)s partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM %(base_table_name)s where id % 4 = 0;
insert into table %(table_name)s partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM %(base_table_name)s where id % 4 = 1;
insert into table %(table_name)s partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM %(base_table_name)s where id % 4 = 2;
insert into table %(table_name)s partition (year, month, day) SELECT  id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM %(base_table_name)s where id % 4 = 3;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  partition (year, month, day) \
  select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day \
  FROM %(base_table_name)s"
----
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
insert into table %(table_name)s partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
insert into table %(table_name)s partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
insert into table %(table_name)s partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
insert into table %(table_name)s partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;
----
ANALYZE TABLE %(table_name)s PARTITION(year, month, day) COMPUTE STATISTICS;
====
functional
----
alltypesaggmultifilesnopart
----
CREATE EXTERNAL TABLE %(table_name)s (
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
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
insert into table %(table_name)s SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM %(base_table_name)s where id % 4 = 0;
insert into table %(table_name)s SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM %(base_table_name)s where id % 4 = 1;
insert into table %(table_name)s SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM %(base_table_name)s where id % 4 = 2;
insert into table %(table_name)s SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM %(base_table_name)s where id % 4 = 3;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert into table %(table_name)s SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table %(table_name)s SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table %(table_name)s SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table %(table_name)s SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
stringpartitionkey
----
-- Regression for IMP-163, failure to load tables partitioned by string column
CREATE EXTERNAL TABLE %(table_name)s (
  id int) 
PARTITIONED BY (string_col string)
STORED AS %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';

ALTER TABLE %(table_name)s ADD PARTITION (string_col = "partition1");
----
----
----
----
ANALYZE TABLE %(table_name)s PARTITION(string_col) COMPUTE STATISTICS;
====
functional
----
tinytable
----
CREATE EXTERNAL TABLE %(table_name)s (
  a string,
  b string)
row format delimited fields terminated by ','
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/TinyTable/data.csv' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
tinyinttable
----
CREATE EXTERNAL TABLE %(table_name)s (
  int_col int)
row format delimited fields terminated by ','
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/TinyIntTable/data.csv' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
nulltable
----
CREATE TABLE %(table_name)s (
  a string,
  b string,
  c string,
  d int,
  e double)
row format delimited fields terminated by ','
stored as %(file_format)s;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE TABLE nulltable select 'a', '', NULL, NULL, NULL from alltypes limit 1;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
----
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE TABLE %(table_name)s select 'a', '', NULL, NULL, NULL from alltypes limit 1;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
nullescapedtable
----
CREATE TABLE %(table_name)s (
  a string,
  b string,
  c string,
  d int,
  e double)
row format delimited fields terminated by ',' escaped by '\\'
stored as %(file_format)s;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE TABLE nullescapedtable select 'a', '', NULL, NULL, NULL from alltypes limit 1;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
----
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE TABLE %(table_name)s select 'a', '', NULL, NULL, NULL from alltypes limit 1;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
functional
----
TblWithColsPastEnd
----
CREATE EXTERNAL TABLE %(table_name)s (
  str_col string,
  int_col int)
row format delimited fields terminated by ','  escaped by '\\'
stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
${IMPALA_HOME}/bin/run-query.sh --query=" \
  INSERT OVERWRITE TABLE %(table_name)s \
  select * FROM %(base_table_name)s"
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/TblWithColsPastEnd/data.csv' OVERWRITE INTO TABLE %(table_name)s;
----
ANALYZE TABLE %(table_name)s COMPUTE STATISTICS;
====
