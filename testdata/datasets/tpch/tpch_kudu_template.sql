---- Template SQL statements to create and load TPCH tables in KUDU.
---- TODO: Fix the primary key column order
---- TODO: Remove the 'kudu.master_addresses' from TBLPROPERTIES once CM properly sets
---- the 'kudu_masters' startup option in Impala.
---- TODO: Remove the CREATE_KUDU sections from tpch_schema_template.sql and use
---- this file instead for loading TPC-H data in Kudu.

--- LINEITEM
CREATE TABLE IF NOT EXISTS {target_db_name}.lineitem (
  L_ORDERKEY BIGINT,
  L_LINENUMBER BIGINT,
  L_PARTKEY BIGINT,
  L_SUPPKEY BIGINT,
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
  L_COMMENT STRING,
  PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
)
distribute by hash (l_orderkey) into {buckets} buckets
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
  P_SIZE BIGINT,
  P_CONTAINER STRING,
  P_RETAILPRICE DOUBLE,
  P_COMMENT STRING
)
distribute by hash (p_partkey) into {buckets} buckets
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.part SELECT * FROM {source_db_name}.part;

---- PARTSUPP
CREATE TABLE IF NOT EXISTS {target_db_name}.partsupp (
  PS_PARTKEY BIGINT,
  PS_SUPPKEY BIGINT,
  PS_AVAILQTY BIGINT,
  PS_SUPPLYCOST DOUBLE,
  PS_COMMENT STRING,
  PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
)
distribute by hash (ps_partkey, ps_suppkey) into {buckets} buckets
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
  S_ACCTBAL DOUBLE,
  S_COMMENT STRING
)
distribute by hash (s_suppkey) into {buckets} buckets
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
distribute by hash (n_nationkey) into {buckets} buckets
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.nation SELECT * FROM {source_db_name}.nation;

---- REGION
CREATE TABLE IF NOT EXISTS {target_db_name}.region (
  R_REGIONKEY BIGINT PRIMARY KEY,
  R_NAME STRING,
  R_COMMENT STRING
)
distribute by hash (r_regionkey) into {buckets} buckets
STORED AS KUDU
tblproperties ('kudu.master_addresses' = '{kudu_master}:7051');

INSERT INTO TABLE {target_db_name}.region SELECT * FROM {source_db_name}.region;

---- ORDERS
CREATE TABLE IF NOT EXISTS {target_db_name}.orders (
  O_ORDERKEY BIGINT PRIMARY KEY,
  O_CUSTKEY BIGINT,
  O_ORDERSTATUS STRING,
  O_TOTALPRICE DOUBLE,
  O_ORDERDATE STRING,
  O_ORDERPRIORITY STRING,
  O_CLERK STRING,
  O_SHIPPRIORITY BIGINT,
  O_COMMENT STRING
)
distribute by hash (o_orderkey) into {buckets} buckets
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
  C_ACCTBAL DOUBLE,
  C_MKTSEGMENT STRING,
  C_COMMENT STRING
)
distribute by hash (c_custkey) into {buckets} buckets
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
