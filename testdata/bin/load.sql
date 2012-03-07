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

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090101.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090201.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090301.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypesSmall/090401.txt' OVERWRITE INTO TABLE AllTypesSmall PARTITION(year=2009, month=4);

INSERT OVERWRITE TABLE alltypessmall_rc partition (year, month)
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

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Step 4: Stream the data from AllTypesError_tmp into AllTypesError_rc_tmp:
INSERT OVERWRITE TABLE alltypeserror_rc_tmp PARTITION (year, month)
SELECT * FROM alltypeserror_tmp;

-- Step 5: Make the Metastore aware of the new partition directories under the
-- AllTypesError_rc table:
ALTER TABLE AllTypesError_rc ADD
PARTITION (year=2009, month=1)
PARTITION (year=2009, month=2)
PARTITION (year=2009, month=3);

-- Step 6: Cleanup by dropping the two temporary tables:
DROP TABLE AllTypesError_tmp;
DROP TABLE AllTypesError_rc_tmp;


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
