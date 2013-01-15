-- Create and load tables that depend upon data in the hive test-warehouse already existing

-- Load a mixed-format table. Hive behaves oddly when mixing formats,
-- but the following incantation ensures that the result is a
-- three-partition table. First is text format, second is sequence
-- file, third is RC file.  Must be called after test-warehouse is
-- successfully populated
USE functional;
DROP TABLE IF EXISTS alltypesmixedformat;
CREATE EXTERNAL TABLE alltypesmixedformat (
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
stored as TEXTFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypesmixedformat';

INSERT OVERWRITE TABLE alltypesmixedformat PARTITION (year=2009, month=1)
SELECT id, bool_col, tinyint_col, smallint_col, int_col, bigint_col,
float_col, double_col, date_string_col, string_col, timestamp_col
FROM alltypes
WHERE year=2009 and month=1;

ALTER TABLE alltypesmixedformat SET FILEFORMAT SEQUENCEFILE;
LOAD DATA INPATH '/tmp/alltypes_seq/year=2009/month=2/'
OVERWRITE INTO TABLE alltypesmixedformat PARTITION (year=2009, month=2);

ALTER TABLE alltypesmixedformat SET FILEFORMAT RCFILE;
LOAD DATA INPATH '/tmp/alltypes_rc/year=2009/month=3/'
OVERWRITE INTO TABLE alltypesmixedformat PARTITION (year=2009, month=3);


ALTER TABLE alltypesmixedformat PARTITION (year=2009, month=1)
  SET SERDEPROPERTIES('field.delim'=',', 'escape.delim'='\\');
ALTER TABLE alltypesmixedformat PARTITION (year=2009, month=1)
  SET FILEFORMAT TEXTFILE;
ALTER TABLE alltypesmixedformat PARTITION (year=2009, month=2)
  SET SERDEPROPERTIES('field.delim'=',', 'escape.delim'='\\');
ALTER TABLE alltypesmixedformat PARTITION (year=2009, month=2)
  SET FILEFORMAT SEQUENCEFILE;
ALTER TABLE alltypesmixedformat PARTITION (year=2009, month=3)
  SET FILEFORMAT RCFILE;

-- Not really dependent: this table contains format errors and
-- is accessed by the unit test: sequence-file-recover-test.
CREATE DATABASE IF NOT EXISTS functional_seq_snap;
USE functional_seq_snap;
DROP TABLE IF EXISTS bad_seq_snap;
CREATE EXTERNAL TABLE bad_seq_snap (field string) stored as SEQUENCEFILE
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/bad_seq_snap';

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/bad_seq_snap/bad_file' OVERWRITE INTO TABLE bad_seq_snap;

-- Error recovery test data for LZO compression.
CREATE DATABASE IF NOT EXISTS functional_lzo;
USE functional_lzo;
DROP TABLE IF EXISTS bad_text;
CREATE EXTERNAL TABLE bad_text (field string) stored as 
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/bad_text_lzo';

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/bad_text_lzo/bad_text.lzo' OVERWRITE INTO TABLE bad_text;

DROP TABLE IF EXISTS alltypes;
CREATE EXTERNAL TABLE alltypes (
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
stored as
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypes_lzo';

ALTER TABLE alltypes ADD PARTITION(year=2009, month=1);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=2);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=3);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=4);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=5);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=6);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=7);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=8);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=9);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=10);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=11);
ALTER TABLE alltypes ADD PARTITION(year=2009, month=12);

ALTER TABLE alltypes ADD PARTITION(year=2010, month=1);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=2);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=3);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=4);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=5);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=6);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=7);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=8);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=9);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=10);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=11);
ALTER TABLE alltypes ADD PARTITION(year=2010, month=12);

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090101.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090201.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090301.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090401.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090501.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090601.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090701.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090801.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/090901.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/091001.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=10);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/091101.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=11);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/091201.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2009, month=12);

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100101.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100201.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100301.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100401.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100501.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=5);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100601.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=6);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100701.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=7);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100801.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=8);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/100901.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=9);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/101001.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=10);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/101101.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=11);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/target/AllTypes/101201.txt.lzo' OVERWRITE INTO TABLE alltypes PARTITION(year=2010, month=12);

DROP TABLE IF EXISTS alltypesaggmultifiles;
CREATE EXTERNAL TABLE alltypesaggmultifiles (
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
stored as 
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/alltypesaggmultifiles_lzo';

ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=1);
ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=2);
ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=3);
ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=4);
ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=5);
ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=6);
ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=7);
ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=8);
ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=9);
ALTER TABLE alltypesaggmultifiles ADD PARTITION(year=2010, month=1, day=10);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=1/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=1/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=1/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=1);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=1/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=1);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=2/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=2/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=2/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=2);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=2/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=2);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=3/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=3/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=3/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=3);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=3/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=3);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=4/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=4/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=4/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=4);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=4/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=4);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=5/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=5/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=5/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=5);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=5/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=5);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=6/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=6/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=6/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=6);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=6/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=6);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=7/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=7/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=7/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=7);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=7/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=7);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=8/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=8/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=8/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=8);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=8/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=8);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=9/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=9/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=9/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=9);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=9/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=9);

LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=10/000000_0.lzo' OVERWRITE INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=10);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=10/000000_0_copy_1.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=10);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=10/000000_0_copy_2.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=10);
LOAD DATA LOCAL INPATH '/tmp/alltypesaggmultifiles/year=2010/month=1/day=10/000000_0_copy_3.lzo' INTO TABLE alltypesaggmultifiles PARTITION(year=2010, month=1, day=10);

----
-- Used by CatalogTest to confirm that non-external HBase tables are identified
-- correctly (IMP-581) 
-- Note that the usual 'hbase.table.name' property is not specified to avoid
-- creating tables in HBase as a side-effect.
USE functional;
CREATE TABLE internal_hbase_table(key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val");
----
-- For structured-type testing
DROP TABLE IF EXISTS map_table;
CREATE TABLE map_table(map_col map<int, string>);
DROP TABLE IF EXISTS array_table;
CREATE TABLE array_table(array_col array<int>);
