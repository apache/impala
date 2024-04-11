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

---- Template SQL statements to create external JDBC tables for TPCH dataset
---- in specified 'jdbc_db_name' JDBC database.

--- lineitem
CREATE EXTERNAL TABLE IF NOT EXISTS {jdbc_db_name}.lineitem (
  l_orderkey BIGINT,
  l_partkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber INT,
  l_quantity DECIMAL(12,2),
  l_extendedprice DECIMAL(12,2),
  l_discount DECIMAL(12,2),
  l_tax DECIMAL(12,2),
  l_returnflag STRING,
  l_linestatus STRING,
  l_shipdate STRING,
  l_commitdate STRING,
  l_receiptdate STRING,
  l_shipinstruct STRING,
  l_shipmode STRING,
  l_comment STRING
)
STORED AS JDBC
TBLPROPERTIES (
  'database.type'='{database_type}',
  'jdbc.url'='{jdbc_url}',
  'jdbc.auth'='{jdbc_auth}',
  'jdbc.properties'='{jdbc_properties}',
  'jdbc.driver'='{jdbc_driver}',
  'driver.url'='{driver_url}',
  'dbcp.username'='{dbcp_username}',
  'dbcp.password'='{dbcp_password}',
  'table'='lineitem');

--- part
CREATE EXTERNAL TABLE IF NOT EXISTS {jdbc_db_name}.part (
  p_partkey BIGINT,
  p_name STRING,
  p_mfgr STRING,
  p_brand STRING,
  p_type STRING,
  p_size INT,
  p_container STRING,
  p_retailprice DECIMAL(12,2),
  p_comment STRING
)
STORED AS JDBC
TBLPROPERTIES (
  'database.type'='{database_type}',
  'jdbc.url'='{jdbc_url}',
  'jdbc.auth'='{jdbc_auth}',
  'jdbc.properties'='{jdbc_properties}',
  'jdbc.driver'='{jdbc_driver}',
  'driver.url'='{driver_url}',
  'dbcp.username'='{dbcp_username}',
  'dbcp.password'='{dbcp_password}',
  'table'='part');

--- partsupp
CREATE EXTERNAL TABLE IF NOT EXISTS {jdbc_db_name}.partsupp (
  ps_partkey BIGINT,
  ps_suppkey BIGINT,
  ps_availqty INT,
  ps_supplycost DECIMAL(12,2),
  ps_comment STRING
)
STORED AS JDBC
TBLPROPERTIES (
  'database.type'='{database_type}',
  'jdbc.url'='{jdbc_url}',
  'jdbc.auth'='{jdbc_auth}',
  'jdbc.properties'='{jdbc_properties}',
  'jdbc.driver'='{jdbc_driver}',
  'driver.url'='{driver_url}',
  'dbcp.username'='{dbcp_username}',
  'dbcp.password'='{dbcp_password}',
  'table'='partsupp');

--- supplier
CREATE EXTERNAL TABLE IF NOT EXISTS {jdbc_db_name}.supplier (
  s_suppkey BIGINT,
  s_name STRING,
  s_address STRING,
  s_nationkey SMALLINT,
  s_phone STRING,
  s_acctbal DECIMAL(12,2),
  s_comment STRING
)
STORED AS JDBC
TBLPROPERTIES (
  'database.type'='{database_type}',
  'jdbc.url'='{jdbc_url}',
  'jdbc.auth'='{jdbc_auth}',
  'jdbc.properties'='{jdbc_properties}',
  'jdbc.driver'='{jdbc_driver}',
  'driver.url'='{driver_url}',
  'dbcp.username'='{dbcp_username}',
  'dbcp.password'='{dbcp_password}',
  'table'='supplier');

--- nation
CREATE EXTERNAL TABLE IF NOT EXISTS {jdbc_db_name}.nation (
  n_nationkey SMALLINT,
  n_name STRING,
  n_regionkey SMALLINT,
  n_comment STRING
)
STORED AS JDBC
TBLPROPERTIES (
  'database.type'='{database_type}',
  'jdbc.url'='{jdbc_url}',
  'jdbc.auth'='{jdbc_auth}',
  'jdbc.properties'='{jdbc_properties}',
  'jdbc.driver'='{jdbc_driver}',
  'driver.url'='{driver_url}',
  'dbcp.username'='{dbcp_username}',
  'dbcp.password'='{dbcp_password}',
  'table'='nation');

--- region
CREATE EXTERNAL TABLE IF NOT EXISTS {jdbc_db_name}.region (
  r_regionkey SMALLINT,
  r_name STRING,
  r_comment STRING
)
STORED AS JDBC
TBLPROPERTIES (
  'database.type'='{database_type}',
  'jdbc.url'='{jdbc_url}',
  'jdbc.auth'='{jdbc_auth}',
  'jdbc.properties'='{jdbc_properties}',
  'jdbc.driver'='{jdbc_driver}',
  'driver.url'='{driver_url}',
  'dbcp.username'='{dbcp_username}',
  'dbcp.password'='{dbcp_password}',
  'table'='region');

--- orders
CREATE EXTERNAL TABLE IF NOT EXISTS {jdbc_db_name}.orders (
  o_orderkey BIGINT,
  o_custkey BIGINT,
  o_orderstatus STRING,
  o_totalprice DECIMAL(12,2),
  o_orderdate STRING,
  o_orderpriority STRING,
  o_clerk STRING,
  o_shippriority INT,
  o_comment STRING
)
STORED AS JDBC
TBLPROPERTIES (
  'database.type'='{database_type}',
  'jdbc.url'='{jdbc_url}',
  'jdbc.auth'='{jdbc_auth}',
  'jdbc.properties'='{jdbc_properties}',
  'jdbc.driver'='{jdbc_driver}',
  'driver.url'='{driver_url}',
  'dbcp.username'='{dbcp_username}',
  'dbcp.password'='{dbcp_password}',
  'table'='orders');

--- customer
CREATE EXTERNAL TABLE IF NOT EXISTS {jdbc_db_name}.customer (
  c_custkey BIGINT,
  c_name STRING,
  c_address STRING,
  c_nationkey SMALLINT,
  c_phone STRING,
  c_acctbal DECIMAL(12,2),
  c_mktsegment STRING,
  c_comment STRING
)
STORED AS JDBC
TBLPROPERTIES (
  'database.type'='{database_type}',
  'jdbc.url'='{jdbc_url}',
  'jdbc.auth'='{jdbc_auth}',
  'jdbc.properties'='{jdbc_properties}',
  'jdbc.driver'='{jdbc_driver}',
  'driver.url'='{driver_url}',
  'dbcp.username'='{dbcp_username}',
  'dbcp.password'='{dbcp_password}',
  'table'='customer');
