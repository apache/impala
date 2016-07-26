# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# This file is used to define schema templates for generating and loading data for
# Impala tests. The goal is to provide a single place to define a table + data files
# and have the schema and data load statements generated for each combination of file
# format, compression, etc. The way this works is by specifying how to create a
# 'base table'. The base table can be used to generate tables in other file formats
# by performing the defined INSERT / SELECT INTO statement. Each new table using the
# file format/compression combination needs to have a unique name, so all the
# statements are pameterized on table name.
# This file is read in by the 'generate_schema_statements.py' script to
# to generate all the schema for the Imapla benchmark tests.
#
# Each table is defined as a new section in this file with the following format:
# ====
#---- DATASET <- Start new section
# Data set name - Used to group sets of tables together
# ---- <- End sub-section
# Base table name
# ---- <- End sub-section
# CREATE TABLE statement - Statement to drop and create a table
# ---- <- End sub-section
# INSERT/SELECT * - The INSERT/SELECT * command for loading from the base table
# ---- <- End sub-section
# Parquet loading code executed by bash.
# ---- <- End sub-section
# LOAD from LOCAL - How to load data for the the base table
#----- <- End sub-section
# ANALYZE TABLE ... COMPUTE STATISTICS - Compute statistics statement for table
====
---- DATASET
hive-benchmark
---- BASE_TABLE_NAME
grep1gb
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (field string) partitioned by (chunk int) stored as {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- ALTER
ALTER TABLE {table_name} ADD PARTITION (chunk=0);
ALTER TABLE {table_name} ADD PARTITION (chunk=1);
ALTER TABLE {table_name} ADD PARTITION (chunk=2);
ALTER TABLE {table_name} ADD PARTITION (chunk=3);
ALTER TABLE {table_name} ADD PARTITION (chunk=4);
ALTER TABLE {table_name} ADD PARTITION (chunk=5);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} partition(chunk)
SELECT field, chunk FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep1GB/part-00000' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=0);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep1GB/part-00001' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep1GB/part-00002' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep1GB/part-00003' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=3);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep1GB/part-00004' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=4);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep1GB/part-00005' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=5);
====
---- DATASET
hive-benchmark
---- BASE_TABLE_NAME
grep10gb
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (field string) partitioned by (chunk int) stored as {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- ALTER
ALTER TABLE {table_name} ADD PARTITION (chunk=0);
ALTER TABLE {table_name} ADD PARTITION (chunk=1);
ALTER TABLE {table_name} ADD PARTITION (chunk=2);
ALTER TABLE {table_name} ADD PARTITION (chunk=3);
ALTER TABLE {table_name} ADD PARTITION (chunk=4);
ALTER TABLE {table_name} ADD PARTITION (chunk=5);
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk) SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep10GB/part-00000' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=0);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep10GB/part-00001' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=1);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep10GB/part-00002' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=2);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep10GB/part-00003' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=3);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep10GB/part-00004' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=4);
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/grep10GB/part-00005' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name} PARTITION(chunk=5);
====
---- DATASET
hive-benchmark
---- BASE_TABLE_NAME
rankings
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|' stored as {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}/Rankings.dat';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/html1GB/Rankings.dat' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
hive-benchmark
---- BASE_TABLE_NAME
uservisits
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
row format delimited fields terminated by '|' stored as {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}/UserVisits.dat';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}
select sourceIP, destURL, visitDate, adRevenue, userAgent, cCode, lCode,
sKeyword, avgTimeOnSite FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/html1GB/UserVisits.dat' OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
