# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# For details on this file format please see hive-benchmark_schema_template.sql
====
---- DATASET
tpch
---- BASE_TABLE_NAME
lineitem
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
L_ORDERKEY BIGINT,
L_PARTKEY BIGINT,
L_SUPPKEY BIGINT,
L_LINENUMBER INT,
L_QUANTITY DOUBLE,
L_EXTENDEDPRICE DOUBLE,
L_DISCOUNT DOUBLE,
L_TAX DOUBLE,
L_RETURNFLAG STRING,
L_LINESTATUS STRING,
L_SHIPDATE STRING,
L_COMMITDATE STRING,
L_RECEIPTDATE STRING,
L_SHIPINSTRUCT STRING,
L_SHIPMODE STRING,
L_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpch
---- BASE_TABLE_NAME
part
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
P_PARTKEY BIGINT,
P_NAME STRING,
P_MFGR STRING,
P_BRAND STRING,
P_TYPE
STRING,
P_SIZE INT,
P_CONTAINER STRING,
P_RETAILPRICE DOUBLE,
P_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpch
---- BASE_TABLE_NAME
partsupp
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
PS_PARTKEY BIGINT,
PS_SUPPKEY BIGINT,
PS_AVAILQTY INT,
PS_SUPPLYCOST DOUBLE,
PS_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpch
---- BASE_TABLE_NAME
supplier
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
S_SUPPKEY BIGINT,
S_NAME STRING,
S_ADDRESS STRING,
S_NATIONKEY SMALLINT,
S_PHONE STRING,
S_ACCTBAL DOUBLE,
S_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpch
---- BASE_TABLE_NAME
nation
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
N_NATIONKEY SMALLINT,
N_NAME STRING,
N_REGIONKEY SMALLINT,
N_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpch
---- BASE_TABLE_NAME
region
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
R_REGIONKEY SMALLINT,
R_NAME STRING,
R_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpch
---- BASE_TABLE_NAME
orders
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
O_ORDERKEY BIGINT,
O_CUSTKEY BIGINT,
O_ORDERSTATUS STRING,
O_TOTALPRICE DOUBLE,
O_ORDERDATE STRING,
O_ORDERPRIORITY STRING,
O_CLERK STRING,
O_SHIPPRIORITY INT,
O_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpch
---- BASE_TABLE_NAME
customer
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
C_CUSTKEY BIGINT,
C_NAME STRING,
C_ADDRESS STRING,
C_NATIONKEY SMALLINT,
C_PHONE STRING,
C_ACCTBAL DOUBLE,
C_MKTSEGMENT STRING,
C_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q2_minimum_cost_supplier_tmp1
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
s_acctbal double,
s_name string,
n_name string,
p_partkey bigint,
ps_supplycost double,
p_mfgr string,
s_address string,
s_phone string,
s_comment string)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q2_minimum_cost_supplier_tmp2
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
p_partkey bigint,
ps_min_supplycost double)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q7_volume_shipping_tmp
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
supp_nation string,
cust_nation string,
s_nationkey smallint,
c_nationkey smallint)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q11_part_tmp
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
ps_partkey bigint,
part_value double)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q11_sum_tmp
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (total_value double)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
revenue
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
supplier_no bigint,
total_revenue double)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
max_revenue
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (max_revenue double)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
supplier_tmp
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (s_suppkey bigint)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q16_tmp
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
p_brand string,
p_type string,
p_size int,
ps_suppkey bigint)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
lineitem_tmp
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
t_partkey bigint,
t_avg_quantity double)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q18_tmp
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
l_orderkey bigint,
t_sum_quantity double)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q20_tmp1
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (p_partkey bigint)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q20_tmp2
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
l_partkey bigint,
l_suppkey bigint,
sum_quantity double)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q20_tmp3
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (
ps_suppkey bigint,
ps_availqty int,
sum_quantity double)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q20_tmp4
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (ps_suppkey bigint)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
---- DATASET
tpch
---- BASE_TABLE_NAME
q22_customer_tmp1
---- CREATE
CREATE EXTERNAL TABLE {db_name}{db_suffix}.{table_name} (avg_acctbal double, cust_name_char string)
STORED AS {file_format}
LOCATION '${{hiveconf:hive.metastore.warehouse.dir}}/{hdfs_location}';
====
