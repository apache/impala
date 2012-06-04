-- This file is rerun if the data changes.  It is used to create the hdfs files which
-- are then copied out to testdata/target/test-warehouse
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090101.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090201.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090301.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090401.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090501.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090601.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090701.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090801.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090901.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/091001.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=10);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/091101.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=11);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/091201.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2009, month=12);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100101.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100201.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100301.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100401.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100501.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100601.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100701.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100801.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100901.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/101001.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=10);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/101101.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=11);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/101201.txt' OVERWRITE INTO TABLE AllTypes PARTITION(year=2010, month=12);

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE alltypes_rc partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypes_seq partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090101.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090201.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090301.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090401.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=4);

INSERT OVERWRITE TABLE alltypessmall_rc partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypessmall_seq partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesError/0901.txt' OVERWRITE INTO TABLE AllTypesError PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesError/0902.txt' OVERWRITE INTO TABLE AllTypesError PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesError/0903.txt' OVERWRITE INTO TABLE AllTypesError PARTITION(year=2009, month=3);

-- Load broken data into the AllTypesError_rc table.
-- This is a multistep process since we have to prevent
-- Hive from interpreting the data while streaming it
-- from the AllTypesError table into the AllTypesError_rc table.

-- Step 1: Create a temporary EXTERNAL table on top of the AllTypesError
-- table that interprets all fields as STRINGs:
DROP TABLE IF EXISTS alltypeserror_tmp;
CREATE EXTERNAL TABLE alltypeserror_tmp (
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
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror';

-- Step 2: Make the Metastore aware of the partition directories:
ALTER TABLE alltypeserror_tmp ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

-- Step 3: Create a temporary EXTERNAL table on top of the AllTypesError_rc
-- table that interprets all fields as STRINGs:
DROP TABLE IF EXISTS alltypeserror_rc_tmp;
CREATE EXTERNAL TABLE alltypeserror_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_rc';

-- also for SeqFile
DROP TABLE IF EXISTS alltypeserror_seq_tmp;
CREATE EXTERNAL TABLE alltypeserror_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_seq';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Step 4: Stream the data from AllTypesError_tmp into AllTypesError_rc_tmp:
INSERT OVERWRITE TABLE alltypeserror_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

-- And into AllTypesError_seq_tmp
INSERT OVERWRITE TABLE alltypeserror_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

SET hive.exec.compress.output=true; 
set mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;

DROP TABLE IF EXISTS alltypeserror_rc_tmp;
CREATE EXTERNAL TABLE alltypeserror_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_rc_def';
INSERT OVERWRITE TABLE alltypeserror_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

DROP TABLE IF EXISTS alltypeserror_seq_tmp;
CREATE EXTERNAL TABLE alltypeserror_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_seq_def';

INSERT OVERWRITE TABLE alltypeserror_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

set mapred.output.compression.type=RECORD;

DROP TABLE IF EXISTS alltypeserror_seq_tmp;
CREATE EXTERNAL TABLE alltypeserror_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_seq_record_def';

INSERT OVERWRITE TABLE alltypeserror_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
DROP TABLE IF EXISTS alltypeserror_rc_tmp;
CREATE EXTERNAL TABLE alltypeserror_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_rc_gzip';
INSERT OVERWRITE TABLE alltypeserror_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

DROP TABLE IF EXISTS alltypeserror_seq_tmp;
CREATE EXTERNAL TABLE alltypeserror_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_seq_gzip';

INSERT OVERWRITE TABLE alltypeserror_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

set mapred.output.compression.type=RECORD;

DROP TABLE IF EXISTS alltypeserror_seq_tmp;
CREATE EXTERNAL TABLE alltypeserror_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_seq_record_gzip';

INSERT OVERWRITE TABLE alltypeserror_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
DROP TABLE IF EXISTS alltypeserror_rc_tmp;
CREATE EXTERNAL TABLE alltypeserror_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_rc_bzip';
INSERT OVERWRITE TABLE alltypeserror_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

DROP TABLE IF EXISTS alltypeserror_seq_tmp;
CREATE EXTERNAL TABLE alltypeserror_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_seq_bzip';

set mapred.output.compression.type=RECORD;

INSERT OVERWRITE TABLE alltypeserror_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

DROP TABLE IF EXISTS alltypeserror_seq_tmp;
CREATE EXTERNAL TABLE alltypeserror_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_seq_record_bzip';

INSERT OVERWRITE TABLE alltypeserror_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
DROP TABLE IF EXISTS alltypeserror_rc_tmp;
CREATE EXTERNAL TABLE alltypeserror_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_rc_snap';
INSERT OVERWRITE TABLE alltypeserror_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

DROP TABLE IF EXISTS alltypeserror_seq_tmp;
CREATE EXTERNAL TABLE alltypeserror_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_seq_snap';

INSERT OVERWRITE TABLE alltypeserror_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

set mapred.output.compression.type=RECORD;

DROP TABLE IF EXISTS alltypeserror_seq_tmp;
CREATE EXTERNAL TABLE alltypeserror_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserror_seq_record_snap';

INSERT OVERWRITE TABLE alltypeserror_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

SET hive.exec.compress.output=false; 
set mapred.output.compression.type=NONE;
SET mapred.output.compression.codec=NONE;

-- Step 5: Make the Metastore aware of the new partition directories under the
-- AllTypesError_rc table:
ALTER TABLE AllTypesError_rc ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_rc_def ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_rc_gzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_rc_bzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_rc_snap ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

-- and for AllTypesError_Seq
ALTER TABLE AllTypesError_seq ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_seq_def ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_seq_gzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_seq_bzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_seq_snap ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_seq_record_def ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_seq_record_gzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_seq_record_bzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

ALTER TABLE AllTypesError_seq_record_snap ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

-- Step 6: Cleanup by dropping the the temporary tables:
DROP TABLE AllTypesError_tmp;
DROP TABLE AllTypesError_rc_tmp;
DROP TABLE AllTypesError_seq_tmp;

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesErrorNoNulls/0901.txt' OVERWRITE INTO TABLE AllTypesErrorNoNulls PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesErrorNoNulls/0902.txt' OVERWRITE INTO TABLE AllTypesErrorNoNulls PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/AllTypesErrorNoNulls/0903.txt' OVERWRITE INTO TABLE AllTypesErrorNoNulls PARTITION(year=2009, month=3);


-- Load data into AllTypesErrorNoNulls_rc following the same steps as above:
DROP TABLE IF EXISTS alltypeserrornonulls_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_tmp (
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
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls';

ALTER TABLE alltypeserrornonulls_tmp ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

DROP TABLE IF EXISTS alltypeserrornonulls_rc_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_rc';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_rc ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

DROP TABLE AllTypesErrorNoNulls_tmp;
DROP TABLE AllTypesErrorNoNulls_rc_tmp;

-- Load data into AllTypesErrorNoNulls_seq* following the same steps as above:
DROP TABLE IF EXISTS alltypeserrornonulls_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_tmp (
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
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls';

ALTER TABLE alltypeserrornonulls_tmp ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

DROP TABLE AllTypesErrorNoNulls_seq_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_seq';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_seq ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

SET hive.exec.compress.output=true; 
set mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;

DROP TABLE IF EXISTS alltypeserrornonulls_rc_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_rc_def';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_rc_def ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

DROP TABLE AllTypesErrorNoNulls_seq_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_seq_def';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_seq_def ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
DROP TABLE IF EXISTS alltypeserrornonulls_rc_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_rc_gzip';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_rc_gzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

DROP TABLE AllTypesErrorNoNulls_seq_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_seq_gzip';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_seq_gzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
DROP TABLE IF EXISTS alltypeserrornonulls_rc_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_rc_bzip';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_rc_bzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

DROP TABLE AllTypesErrorNoNulls_seq_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_seq_bzip';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_seq_bzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
DROP TABLE IF EXISTS alltypeserrornonulls_rc_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_rc_tmp (
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
STORED AS RCFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_rc_snap';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_rc_snap ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

DROP TABLE AllTypesErrorNoNulls_seq_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_seq_snap';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_seq_snap ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

set mapred.output.compression.type=RECORD;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;
DROP TABLE AllTypesErrorNoNulls_seq_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_seq_record_def';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_seq_record_def ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
DROP TABLE AllTypesErrorNoNulls_seq_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_seq_record_gzip';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_seq_record_gzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
DROP TABLE AllTypesErrorNoNulls_seq_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_seq_record_bzip';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_seq_record_bzip ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
DROP TABLE AllTypesErrorNoNulls_seq_tmp;
CREATE EXTERNAL TABLE alltypeserrornonulls_seq_tmp (
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
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypeserrornonulls_seq_record_snap';

SET hive.exec.compress.output=false; 
set mapred.output.compression.type=NONE;
SET mapred.output.compression.codec=NONE;


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE alltypeserrornonulls_seq_tmp PARTITION (year, month)
SELECT * FROM alltypeserrornonulls_tmp;

ALTER TABLE AllTypesErrorNoNulls_seq_record_snap ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

DROP TABLE AllTypesErrorNoNulls_tmp;
DROP TABLE AllTypesErrorNoNulls_seq_tmp;

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100101.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100102.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100103.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100104.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100105.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100106.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100107.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100108.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100109.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAgg/100110.txt' OVERWRITE INTO TABLE AllTypesAgg PARTITION(year=2010, month=1, day=10);

INSERT OVERWRITE TABLE alltypesagg_rc partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesagg_seq partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100101.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100102.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100103.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100104.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100105.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100106.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100107.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100108.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100109.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesAggNoNulls/100110.txt' OVERWRITE INTO TABLE AllTypesAggNoNulls PARTITION(year=2010, month=1, day=10);

INSERT OVERWRITE TABLE alltypesaggnonulls_rc partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

INSERT OVERWRITE TABLE alltypesaggnonulls_seq partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

SET hive.exec.compress.output=true; 
set mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;

INSERT OVERWRITE TABLE alltypes_rc_def partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_rc_def partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_rc_def partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_rc_def partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

INSERT OVERWRITE TABLE alltypes_seq_def partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_seq_def partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_seq_def partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_seq_def partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
INSERT OVERWRITE TABLE alltypes_rc_gzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_rc_gzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_rc_gzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_rc_gzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

INSERT OVERWRITE TABLE alltypes_seq_gzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_seq_gzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_seq_gzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_seq_gzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
INSERT OVERWRITE TABLE alltypes_rc_bzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_rc_bzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_rc_bzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_rc_bzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

INSERT OVERWRITE TABLE alltypes_seq_bzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_seq_bzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_seq_bzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_seq_bzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
INSERT OVERWRITE TABLE alltypes_rc_snap partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_rc_snap partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_rc_snap partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_rc_snap partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

INSERT OVERWRITE TABLE alltypes_seq_snap partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_seq_snap partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_seq_snap partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_seq_snap partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

set mapred.output.compression.type=RECORD;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;

INSERT OVERWRITE TABLE alltypes_seq_record_def partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_seq_record_def partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_seq_record_def partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_seq_record_def partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
INSERT OVERWRITE TABLE alltypes_seq_record_gzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_seq_record_gzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_seq_record_gzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_seq_record_gzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
INSERT OVERWRITE TABLE alltypes_seq_record_bzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_seq_record_bzip partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_seq_record_bzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_seq_record_bzip partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
INSERT OVERWRITE TABLE alltypes_seq_record_snap partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypes;

INSERT OVERWRITE TABLE alltypessmall_seq_record_snap partition (year, month)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month
FROM alltypessmall;

INSERT OVERWRITE TABLE alltypesagg_seq_record_snap partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesagg;

INSERT OVERWRITE TABLE alltypesaggnonulls_seq_record_snap partition (year, month, day)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day
FROM alltypesaggnonulls;

SET hive.exec.compress.output=false; 
set mapred.output.compression.type=NONE;
SET mapred.output.compression.codec=NONE;


LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/LikeTbl/data.csv' OVERWRITE INTO TABLE LikeTbl;
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/DimTbl/data.csv' OVERWRITE INTO TABLE DimTbl;
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/JoinTbl/data.csv' OVERWRITE INTO TABLE JoinTbl;

INSERT OVERWRITE TABLE hbasealltypessmall
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col
FROM alltypessmall;

INSERT OVERWRITE TABLE hbasealltypesagg
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col
FROM alltypesagg;

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/data/escape-no-quotes.txt' OVERWRITE INTO TABLE EscapeNoQuotes;

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/data/overflow.txt' OVERWRITE INTO TABLE Overflow;

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/grepTiny/part-00000' OVERWRITE INTO TABLE GrepTiny;

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/htmlTiny/Rankings.dat' OVERWRITE INTO TABLE RankingsSmall;
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/hive_benchmark/htmlTiny/UserVisits.dat' OVERWRITE INTO TABLE UserVisitsSmall;

-- Create multiple (now, 4) files for AllTypesAggMultiFilesNoPart (hdfs/rc/text)
insert into table alltypesaggmultifilesnopart      SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart      SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart      SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart      SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;
insert into table alltypesaggmultifilesnopart_rc   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_rc   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_rc   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_rc   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;
insert into table alltypesaggmultifilesnopart_text SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_text SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_text SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_text SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

insert into table alltypesaggmultifilesnopart_seq   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_seq   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_seq   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_seq   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

SET hive.exec.compress.output=true; 
set mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;

insert into table alltypesaggmultifilesnopart_rc_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_rc_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_rc_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_rc_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

insert into table alltypesaggmultifilesnopart_seq_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_seq_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_seq_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_seq_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
insert into table alltypesaggmultifilesnopart_rc_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_rc_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_rc_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_rc_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

insert into table alltypesaggmultifilesnopart_seq_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_seq_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_seq_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_seq_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
insert into table alltypesaggmultifilesnopart_rc_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_rc_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_rc_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_rc_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

insert into table alltypesaggmultifilesnopart_seq_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_seq_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_seq_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_seq_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
insert into table alltypesaggmultifilesnopart_rc_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_rc_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_rc_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_rc_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

insert into table alltypesaggmultifilesnopart_seq_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_seq_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_seq_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_seq_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

set mapred.output.compression.type=RECORD;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;
insert into table alltypesaggmultifilesnopart_seq_record_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_seq_record_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_seq_record_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_seq_record_def   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
insert into table alltypesaggmultifilesnopart_seq_record_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_seq_record_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_seq_record_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_seq_record_gzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
insert into table alltypesaggmultifilesnopart_seq_record_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_seq_record_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_seq_record_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_seq_record_bzip   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
insert into table alltypesaggmultifilesnopart_seq_record_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 0;
insert into table alltypesaggmultifilesnopart_seq_record_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 1;
insert into table alltypesaggmultifilesnopart_seq_record_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 2;
insert into table alltypesaggmultifilesnopart_seq_record_snap   SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col FROM alltypesagg where id % 4 = 3;

SET hive.exec.compress.output=false; 
set mapred.output.compression.type=NONE;
SET mapred.output.compression.codec=NONE;
-- Create multiple files for alltypesaggmultifiles (hdfs/rc/text)
INSERT INTO TABLE alltypesaggmultifiles partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

INSERT INTO TABLE alltypesaggmultifiles_rc partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_rc partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_rc partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_rc partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

INSERT INTO TABLE alltypesaggmultifiles_text partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_text partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_text partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_text partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

INSERT INTO TABLE alltypesaggmultifiles_seq partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_seq partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_seq partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_seq partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

SET hive.exec.compress.output=true; 
set mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;
INSERT INTO TABLE alltypesaggmultifiles_rc_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_rc_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_rc_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_rc_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

INSERT INTO TABLE alltypesaggmultifiles_seq_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_seq_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_seq_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_seq_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
INSERT INTO TABLE alltypesaggmultifiles_rc_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_rc_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_rc_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_rc_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

INSERT INTO TABLE alltypesaggmultifiles_seq_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_seq_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_seq_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_seq_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
INSERT INTO TABLE alltypesaggmultifiles_rc_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_rc_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_rc_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_rc_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

INSERT INTO TABLE alltypesaggmultifiles_seq_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_seq_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_seq_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_seq_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
INSERT INTO TABLE alltypesaggmultifiles_rc_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_rc_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_rc_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_rc_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

INSERT INTO TABLE alltypesaggmultifiles_seq_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_seq_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_seq_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_seq_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

set mapred.output.compression.type=RECORD;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_def partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_gzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_bzip partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 0;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 1;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 2;
INSERT INTO TABLE alltypesaggmultifiles_seq_record_snap partition (year, month, day) SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col, year, month, day FROM alltypesagg where id % 4 = 3;
