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
# For details on this file format please see ../README
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
---- CREATE_KUDU
create table if not exists {db_name}{db_suffix}.{table_name} (
  L_ORDERKEY BIGINT,
  L_PARTKEY BIGINT,
  L_SUPPKEY BIGINT,
  L_LINENUMBER INT,
  L_QUANTITY DECIMAL(12,2),
  L_EXTENDEDPRICE DECIMAL(12,2),
  L_DISCOUNT DECIMAL(12,2),
  L_TAX DECIMAL(12,2),
  L_RETURNFLAG STRING,
  L_LINESTATUS STRING,
  L_SHIPDATE STRING,
  L_COMMITDATE STRING,
  L_RECEIPTDATE STRING,
  L_SHIPINSTRUCT STRING,
  L_SHIPMODE STRING,
  L_COMMENT STRING,
  PRIMARY KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
)
partition by hash (l_orderkey) partitions 9 stored as kudu;
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
---- CREATE_KUDU
create table if not exists {db_name}{db_suffix}.{table_name} (
  P_PARTKEY BIGINT PRIMARY KEY,
  P_NAME STRING,
  P_MFGR STRING,
  P_BRAND STRING,
  P_TYPE STRING,
  P_SIZE INT,
  P_CONTAINER STRING,
  P_RETAILPRICE DECIMAL(12,2),
  P_COMMENT STRING
)
partition by hash (p_partkey) partitions 9 stored as kudu;
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
---- CREATE_KUDU
create table if not exists {db_name}{db_suffix}.{table_name} (
  PS_PARTKEY BIGINT,
  PS_SUPPKEY BIGINT,
  PS_AVAILQTY BIGINT,
  PS_SUPPLYCOST DECIMAL(12,2),
  PS_COMMENT STRING,
  PRIMARY KEY(PS_PARTKEY, PS_SUPPKEY)
)
partition by hash (ps_partkey, ps_suppkey) partitions 9 stored as kudu;
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
---- CREATE_KUDU
create table if not exists {db_name}{db_suffix}.{table_name} (
  S_SUPPKEY BIGINT PRIMARY KEY,
  S_NAME STRING,
  S_ADDRESS STRING,
  S_NATIONKEY SMALLINT,
  S_PHONE STRING,
  S_ACCTBAL DECIMAL(12,2),
  S_COMMENT STRING
)
partition by hash (s_suppkey) partitions 9 stored as kudu;
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
---- CREATE_KUDU
create table if not exists {db_name}{db_suffix}.{table_name} (
  N_NATIONKEY SMALLINT PRIMARY KEY,
  N_NAME STRING,
  N_REGIONKEY SMALLINT,
  N_COMMENT STRING
)
stored as kudu;
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
---- CREATE_KUDU
create table if not exists {db_name}{db_suffix}.{table_name} (
  R_REGIONKEY SMALLINT PRIMARY KEY,
  R_NAME STRING,
  R_COMMENT STRING
)
stored as kudu;
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
---- CREATE_KUDU
create table if not exists {db_name}{db_suffix}.{table_name} (
  O_ORDERKEY BIGINT PRIMARY KEY,
  O_CUSTKEY BIGINT,
  O_ORDERSTATUS STRING,
  O_TOTALPRICE DECIMAL(12,2),
  O_ORDERDATE STRING,
  O_ORDERPRIORITY STRING,
  O_CLERK STRING,
  O_SHIPPRIORITY INT,
  O_COMMENT STRING
)
partition by hash (o_orderkey) partitions 9 stored as kudu;
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
---- CREATE_KUDU
create table if not exists {db_name}{db_suffix}.{table_name} (
  C_CUSTKEY BIGINT PRIMARY KEY,
  C_NAME STRING,
  C_ADDRESS STRING,
  C_NATIONKEY SMALLINT,
  C_PHONE STRING,
  C_ACCTBAL DECIMAL(12,2),
  C_MKTSEGMENT STRING,
  C_COMMENT STRING
)
partition by hash (c_custkey) partitions 9 stored as kudu;
---- DEPENDENT_LOAD
INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name} SELECT * FROM {db_name}.{table_name};
---- LOAD
LOAD DATA LOCAL INPATH '{impala_home}/testdata/impala-data/{db_name}/{table_name}'
OVERWRITE INTO TABLE {db_name}{db_suffix}.{table_name};
====
