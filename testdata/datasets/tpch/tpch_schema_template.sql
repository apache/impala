# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# For details on this file format please see hive-benchmark_schema_template.sql
====
---- DATASET
tpch
---- BASE_TABLE_NAME
lineitem
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
---- ROW_FORMAT
DELIMITED FIELDS TERMINATED BY '|'
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
---- COLUMNS
P_PARTKEY BIGINT
P_NAME STRING
P_MFGR STRING
P_BRAND STRING
P_TYPE STRING
P_SIZE INT
P_CONTAINER STRING
P_RETAILPRICE DECIMAL(12,2)
P_COMMENT STRING
---- ROW_FORMAT
DELIMITED FIELDS TERMINATED BY '|'
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
---- COLUMNS
PS_PARTKEY BIGINT
PS_SUPPKEY BIGINT
PS_AVAILQTY INT
PS_SUPPLYCOST DECIMAL(12,2)
PS_COMMENT STRING
---- ROW_FORMAT
DELIMITED FIELDS TERMINATED BY '|'
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
---- COLUMNS
S_SUPPKEY BIGINT
S_NAME STRING
S_ADDRESS STRING
S_NATIONKEY SMALLINT
S_PHONE STRING
S_ACCTBAL DECIMAL(12,2)
S_COMMENT STRING
---- ROW_FORMAT
DELIMITED FIELDS TERMINATED BY '|'
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
---- COLUMNS
N_NATIONKEY SMALLINT
N_NAME STRING
N_REGIONKEY SMALLINT
N_COMMENT STRING
---- ROW_FORMAT
DELIMITED FIELDS TERMINATED BY '|'
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
---- COLUMNS
R_REGIONKEY SMALLINT
R_NAME STRING
R_COMMENT STRING
---- ROW_FORMAT
DELIMITED FIELDS TERMINATED BY '|'
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
---- COLUMNS
O_ORDERKEY BIGINT
O_CUSTKEY BIGINT
O_ORDERSTATUS STRING
O_TOTALPRICE DECIMAL(12,2)
O_ORDERDATE STRING
O_ORDERPRIORITY STRING
O_CLERK STRING
O_SHIPPRIORITY INT
O_COMMENT STRING
---- ROW_FORMAT
DELIMITED FIELDS TERMINATED BY '|'
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
---- COLUMNS
C_CUSTKEY BIGINT
C_NAME STRING
C_ADDRESS STRING
C_NATIONKEY SMALLINT
C_PHONE STRING
C_ACCTBAL DECIMAL(12,2)
C_MKTSEGMENT STRING
C_COMMENT STRING
---- ROW_FORMAT
DELIMITED FIELDS TERMINATED BY '|'
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
---- DATASET
tpch
---- BASE_TABLE_NAME
revenue
---- COLUMNS
supplier_no bigint
total_revenue Decimal(38,4)
====
---- DATASET
tpch
---- BASE_TABLE_NAME
max_revenue
---- COLUMNS
max_revenue Decimal(38, 4)
====
