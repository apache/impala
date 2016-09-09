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

---- Unsupported Impala table types
USE functional;
CREATE VIEW IF NOT EXISTS hive_view AS SELECT 1 AS int_col FROM alltypes limit 1;

USE functional;
DROP INDEX IF EXISTS hive_index ON alltypes;
CREATE INDEX hive_index ON TABLE alltypes (int_col)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD IN TABLE hive_index_tbl
