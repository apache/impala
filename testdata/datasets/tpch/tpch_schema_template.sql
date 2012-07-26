# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# For details on this file format please see benchmark_schema_template.sql
====
tpch
----
lineitem
----
CREATE EXTERNAL TABLE %(table_name)s (
L_ORDERKEY INT,
L_PARTKEY INT,
L_SUPPKEY INT,
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
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/tpch/lineitem.tbl'
OVERWRITE INTO TABLE %(table_name)s;
====
tpch
----
part
----
CREATE EXTERNAL TABLE %(table_name)s (
P_PARTKEY INT,
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
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/tpch/part.tbl'
OVERWRITE INTO TABLE %(table_name)s;
====
tpch
----
partsupp
----
CREATE EXTERNAL TABLE %(table_name)s (
PS_PARTKEY INT,
PS_SUPPKEY INT,
PS_AVAILQTY INT,
PS_SUPPLYCOST DOUBLE,
PS_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/tpch/partsupp.tbl'
OVERWRITE INTO TABLE %(table_name)s;
====
tpch
----
supplier
----
CREATE EXTERNAL TABLE %(table_name)s (
S_SUPPKEY INT,
S_NAME STRING,
S_ADDRESS STRING,
S_NATIONKEY INT,
S_PHONE STRING,
S_ACCTBAL DOUBLE,
S_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/tpch/supplier.tbl'
OVERWRITE INTO TABLE %(table_name)s;
====
tpch
----
nation
----
CREATE EXTERNAL TABLE %(table_name)s (
N_NATIONKEY INT,
N_NAME STRING,
N_REGIONKEY INT,
N_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/tpch/nation.tbl'
OVERWRITE INTO TABLE %(table_name)s;
====
tpch
----
region
----
CREATE EXTERNAL TABLE %(table_name)s (
R_REGIONKEY INT,
R_NAME STRING,
R_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/tpch/region.tbl'
OVERWRITE INTO TABLE %(table_name)s;
====
tpch
----
orders
----
CREATE EXTERNAL TABLE %(table_name)s (
O_ORDERKEY INT,
O_CUSTKEY INT,
O_ORDERSTATUS STRING,
O_TOTALPRICE DOUBLE,
O_ORDERDATE STRING,
O_ORDERPRIORITY STRING,
O_CLERK STRING,
O_SHIPPRIORITY INT,
O_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/tpch/orders.tbl'
OVERWRITE INTO TABLE %(table_name)s;
====
tpch
----
customer
----
CREATE EXTERNAL TABLE %(table_name)s (
C_CUSTKEY INT,
C_NAME STRING,
C_ADDRESS STRING,
C_NATIONKEY INT,
C_PHONE STRING,
C_ACCTBAL DOUBLE,
C_MKTSEGMENT STRING,
C_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
FROM %(base_table_name)s INSERT OVERWRITE TABLE %(table_name)s SELECT *;
----
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/impala-data/tpch/customer.tbl'
OVERWRITE INTO TABLE %(table_name)s;
====
tpch
----
q2_minimum_cost_supplier_tmp1
----
CREATE EXTERNAL TABLE %(table_name)s (
s_acctbal double,
s_name string,
n_name string,
p_partkey int,
ps_supplycost double,
p_mfgr string,
s_address string,
s_phone string,
s_comment string)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q2_minimum_cost_supplier_tmp2
----
CREATE EXTERNAL TABLE %(table_name)s (
p_partkey int,
ps_min_supplycost double)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
q7_volume_shipping_tmp
----
CREATE EXTERNAL TABLE %(table_name)s (
supp_nation string,
cust_nation string,
s_nationkey int,
c_nationkey int)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q11_part_tmp
----
CREATE EXTERNAL TABLE %(table_name)s (
ps_partkey int,
part_value double)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q11_sum_tmp
----
CREATE EXTERNAL TABLE %(table_name)s (total_value double)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
revenue
----
CREATE EXTERNAL TABLE %(table_name)s (
supplier_no int,
total_revenue double)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
max_revenue
----
CREATE EXTERNAL TABLE %(table_name)s (max_revenue double)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
supplier_tmp
----
CREATE EXTERNAL TABLE %(table_name)s (s_suppkey int)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q16_tmp
----
CREATE EXTERNAL TABLE %(table_name)s (
p_brand string,
p_type string,
p_size int,
ps_suppkey int)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
lineitem_tmp
----
CREATE EXTERNAL TABLE %(table_name)s (
t_partkey int,
t_avg_quantity double)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q18_tmp
----
CREATE EXTERNAL TABLE %(table_name)s (
l_orderkey int,
t_sum_quantity double)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q20_tmp1
----
CREATE EXTERNAL TABLE %(table_name)s (p_partkey int)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q20_tmp2
----
CREATE EXTERNAL TABLE %(table_name)s (
l_partkey int,
l_suppkey int,
sum_quantity double)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q20_tmp3
----
CREATE EXTERNAL TABLE %(table_name)s (
ps_suppkey int,
ps_availqty int,
sum_quantity double)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q20_tmp4
----
CREATE EXTERNAL TABLE %(table_name)s (ps_suppkey int)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
tpch
----
q22_customer_tmp1
----
CREATE EXTERNAL TABLE %(table_name)s (avg_acctbal double, cust_name_char string)
STORED AS %(file_format)s
LOCATION '/test-warehouse/%(table_name)s';
----
----
====
