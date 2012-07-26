# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
# ==== <- Start new section
# Data set name - Used to group sets of tables together
# ---- <- End sub-section
# Base table name
# ---- <- End sub-section
# CREATE TABLE statement - Statement to drop and create a table
# ---- <- End sub-section
# INSERT/SELECT * - The INSERT/SELECT * command for loading from the base table
# ---- <- End sub-section
# LOAD from LOCAL - How to load data for the the base table
# ==== <- End section
====
grep1gb
----
grep1gb
----
CREATE EXTERNAL TABLE %(table_name)s (field string) partitioned by (chunk int) stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
ALTER TABLE %(table_name)s ADD PARTITION (chunk=0);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=1);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=2);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=3);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=4);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=5);
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s PARTITION(chunk) SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep1GB/part-00000' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=0);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep1GB/part-00001' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep1GB/part-00002' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep1GB/part-00003' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep1GB/part-00004' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep1GB/part-00005' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=5);
====
grep10gb
----
grep10gb
----
CREATE EXTERNAL TABLE %(table_name)s (field string) partitioned by (chunk int) stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s';
ALTER TABLE %(table_name)s ADD PARTITION (chunk=0);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=1);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=2);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=3);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=4);
ALTER TABLE %(table_name)s ADD PARTITION (chunk=5);
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s PARTITION(chunk) SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep10GB/part-00000' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=0);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep10GB/part-00001' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=1);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep10GB/part-00002' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=2);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep10GB/part-00003' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=3);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep10GB/part-00004' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=4);
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/grep10GB/part-00005' OVERWRITE INTO TABLE %(table_name)s PARTITION(chunk=5);
====
web
----
rankings
----
CREATE EXTERNAL TABLE %(table_name)s (
  pageRank int,
  pageURL string,
  avgDuration int)
row format delimited fields terminated by '|' stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s/Rankings.dat';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/html1GB/Rankings.dat' OVERWRITE INTO TABLE %(table_name)s;
====
web
----
uservisits
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
row format delimited fields terminated by '|' stored as %(file_format)s
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/%(table_name)s/UserVisits.dat';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/html1GB/UserVisits.dat' OVERWRITE INTO TABLE %(table_name)s;
====
