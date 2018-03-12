---- Licensed to the Apache Software Foundation (ASF) under one
---- or more contributor license agreements.  See the NOTICE file
---- distributed with this work for additional information
---- regarding copyright ownership.  The ASF licenses this file
---- to you under the Apache License, Version 2.0 (the
---- "License"); you may not use this file except in compliance
---- with the License.  You may obtain a copy of the License at
----
----   http://www.apache.org/licenses/LICENSE-2.0
----
---- Unless required by applicable law or agreed to in writing,
---- software distributed under the License is distributed on an
---- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
---- KIND, either express or implied.  See the License for the
---- specific language governing permissions and limitations
---- under the License.

---- Template SQL statements to create and load TPCH tables in KUDU.
---- TODO: Fix the primary key column order
---- TODO: Remove the 'kudu.master_addresses' from TBLPROPERTIES once CM properly sets
---- the 'kudu_masters' startup option in Impala.
---- TODO: Remove the CREATE_KUDU sections from tpch_schema_template.sql and use
---- this file instead for loading TPC-H data in Kudu.

--- LINEITEM
CREATE TABLE IF NOT EXISTS {target_db_name}.lineitem (
  L_ORDERKEY BIGINT,
  L_LINENUMBER INT,
  L_PARTKEY BIGINT,
  L_SUPPKEY BIGINT,
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
  PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
)
partition by hash (l_orderkey) partitions {buckets}
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.lineitem
SELECT
  L_ORDERKEY,
  L_LINENUMBER,
  L_PARTKEY,
  L_SUPPKEY,
  L_QUANTITY,
  L_EXTENDEDPRICE,
  L_DISCOUNT,
  L_TAX,
  L_RETURNFLAG,
  L_LINESTATUS,
  L_SHIPDATE,
  L_COMMITDATE,
  L_RECEIPTDATE,
  L_SHIPINSTRUCT,
  L_SHIPMODE,
  L_COMMENT
FROM {source_db_name}.lineitem;

---- PART
CREATE TABLE IF NOT EXISTS {target_db_name}.part (
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
partition by hash (p_partkey) partitions {buckets}
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.part SELECT * FROM {source_db_name}.part;

---- PARTSUPP
CREATE TABLE IF NOT EXISTS {target_db_name}.partsupp (
  PS_PARTKEY BIGINT,
  PS_SUPPKEY BIGINT,
  PS_AVAILQTY BIGINT,
  PS_SUPPLYCOST DECIMAL(12,2),
  PS_COMMENT STRING,
  PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
)
partition by hash (ps_partkey, ps_suppkey) partitions {buckets}
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.partsupp SELECT * FROM {source_db_name}.partsupp;

---- SUPPLIER
CREATE TABLE IF NOT EXISTS {target_db_name}.supplier (
  S_SUPPKEY BIGINT PRIMARY KEY,
  S_NAME STRING,
  S_ADDRESS STRING,
  S_NATIONKEY BIGINT,
  S_PHONE STRING,
  S_ACCTBAL DECIMAL(12,2),
  S_COMMENT STRING
)
partition by hash (s_suppkey) partitions {buckets}
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.supplier SELECT * FROM {source_db_name}.supplier;

---- NATION
CREATE TABLE IF NOT EXISTS {target_db_name}.nation (
  N_NATIONKEY BIGINT PRIMARY KEY,
  N_NAME STRING,
  N_REGIONKEY BIGINT,
  N_COMMENT STRING
)
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.nation SELECT * FROM {source_db_name}.nation;

---- REGION
CREATE TABLE IF NOT EXISTS {target_db_name}.region (
  R_REGIONKEY BIGINT PRIMARY KEY,
  R_NAME STRING,
  R_COMMENT STRING
)
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.region SELECT * FROM {source_db_name}.region;

---- ORDERS
CREATE TABLE IF NOT EXISTS {target_db_name}.orders (
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
partition by hash (o_orderkey) partitions {buckets}
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.orders SELECT * FROM {source_db_name}.orders;

---- CUSTOMER
CREATE TABLE IF NOT EXISTS {target_db_name}.customer (
  C_CUSTKEY BIGINT PRIMARY KEY,
  C_NAME STRING,
  C_ADDRESS STRING,
  C_NATIONKEY BIGINT,
  C_PHONE STRING,
  C_ACCTBAL DECIMAL(12,2),
  C_MKTSEGMENT STRING,
  C_COMMENT STRING
)
partition by hash (c_custkey) partitions {buckets}
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.customer SELECT * FROM {source_db_name}.customer;

---- COMPUTE STATS
compute stats {target_db_name}.customer;
compute stats {target_db_name}.lineitem;
compute stats {target_db_name}.nation;
compute stats {target_db_name}.orders;
compute stats {target_db_name}.part;
compute stats {target_db_name}.partsupp;
compute stats {target_db_name}.region;
compute stats {target_db_name}.supplier;
