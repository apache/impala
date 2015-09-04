#!/bin/bash
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

set -e

SHELL_CMD=${IMPALA_HOME}/bin/impala-shell.sh

usage() {
  echo "Usage: $0 [-s source db] [-t target db]" 1>&2;
  echo "[-p num partitions] [-d base HDFS directory" 1>&2;
  exit 1;
}

SOURCE_DATABASE=tpch_parquet
TARGET_DATABASE=tpch_nested_parquet
BASE_DIR="/test-warehouse"

# NUM_PARTITIONS represents how many insert statements to generate when loading nested
# tables. Each query would insert 1 / NUM_PARTITIONS of the data. This prevents Impala
# from running out of memory.
NUM_PARTITIONS=1
while getopts ":s:t:p:d:" OPTION; do
  case "${OPTION}" in
    s)
      SOURCE_DATABASE=${OPTARG}
      ;;
    t)
      TARGET_DATABASE=${OPTARG}
      ;;
    p)
      NUM_PARTITIONS=${OPTARG}
      ;;
    d)
      BASE_DIR=${OPTARG}
      ;;
    *)
      usage
      ;;
  esac
done

# If the part table exists, assume everything is already loaded.
if ${SHELL_CMD} -q "desc $TARGET_DATABASE.part" &>/dev/null; then
  echo $TARGET_DATABASE already loaded
  exit
fi

# As of this writing, Impala isn't able to write nested data in parquet format.
# Instead, the data will be written in text format, then Hive will be used to
# convert from text to parquet.

${SHELL_CMD} -q "CREATE DATABASE $TARGET_DATABASE;"

# Create the nested data in text format. The \00#'s are nested field terminators,
# where the numbers correspond to the nesting level.

# Create customers, with nested orders including order line items. Creation is
# split into multiple queries so less memory is needed.

# Create the temporary orders table with nested line items.
for ((PART_NUM=0; PART_NUM < $NUM_PARTITIONS; PART_NUM++))
do
  TMP_ORDERS_SQL="
  SELECT STRAIGHT_JOIN
    o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority,
    o_clerk, o_shippriority, o_comment,
    GROUP_CONCAT(
      CONCAT(
        CAST(l_partkey AS STRING), '\005',
        CAST(l_suppkey AS STRING), '\005',
        CAST(l_linenumber AS STRING), '\005',
        CAST(l_quantity AS STRING), '\005',
        CAST(l_extendedprice AS STRING), '\005',
        CAST(l_discount AS STRING), '\005',
        CAST(l_tax AS STRING), '\005',
        CAST(l_returnflag AS STRING), '\005',
        CAST(l_linestatus AS STRING), '\005',
        CAST(l_shipdate AS STRING), '\005',
        CAST(l_commitdate AS STRING), '\005',
        CAST(l_receiptdate AS STRING), '\005',
        CAST(l_shipinstruct AS STRING), '\005',
        CAST(l_shipmode AS STRING), '\005',
        CAST(l_comment AS STRING)
      ), '\004'
    ) AS lineitems_string
  FROM $SOURCE_DATABASE.lineitem
  INNER JOIN [SHUFFLE] $SOURCE_DATABASE.orders ON o_orderkey = l_orderkey
  WHERE o_orderkey % $NUM_PARTITIONS = $PART_NUM
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9"

  if [ $PART_NUM = 0 ]; then
    ${SHELL_CMD} -q "
      USE $TARGET_DATABASE;
      CREATE TABLE tmp_orders_string AS $TMP_ORDERS_SQL;"
  else
    ${SHELL_CMD} -q "
      USE $TARGET_DATABASE;
      INSERT INTO TABLE tmp_orders_string $TMP_ORDERS_SQL;"
  fi
done

# Create the customers table
for ((PART_NUM=0; PART_NUM < $NUM_PARTITIONS; PART_NUM++))
do
  TMP_CUSTOMER_STRING_SQL="
  SELECT
    c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment,
    c_comment,
    GROUP_CONCAT(
      CONCAT(
        CAST(o_orderkey AS STRING), '\003',
        CAST(o_orderstatus AS STRING), '\003',
        CAST(o_totalprice AS STRING), '\003',
        CAST(o_orderdate AS STRING), '\003',
        CAST(o_orderpriority AS STRING), '\003',
        CAST(o_clerk AS STRING), '\003',
        CAST(o_shippriority AS STRING), '\003',
        CAST(o_comment AS STRING), '\003',
        CAST(lineitems_string AS STRING)
      ), '\002'
    ) orders_string
  FROM $SOURCE_DATABASE.customer
  LEFT JOIN tmp_orders_string ON c_custkey = o_custkey
  WHERE c_custkey % $NUM_PARTITIONS = $PART_NUM
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;"

  if [ $PART_NUM = 0 ]; then
    ${SHELL_CMD} -q "
      USE $TARGET_DATABASE;
      CREATE TABLE tmp_customer_string AS $TMP_CUSTOMER_STRING_SQL;"
  else
    ${SHELL_CMD} -q "
      USE $TARGET_DATABASE;
      INSERT INTO TABLE tmp_customer_string $TMP_CUSTOMER_STRING_SQL;"
  fi
done

# Create a table with nested schema to read the text file we generated above. Impala is
# currently unable to read from this table. We will use Hive to read from it in order to
# convert the table to parquet.
${SHELL_CMD} -q "
  USE $TARGET_DATABASE;

    CREATE EXTERNAL TABLE tmp_customer (
      c_custkey BIGINT,
      c_name STRING,
      c_address STRING,
      c_nationkey SMALLINT,
      c_phone STRING,
      c_acctbal DECIMAL(12, 2),
      c_mktsegment STRING,
      c_comment STRING,
      c_orders ARRAY<STRUCT<
        o_orderkey: BIGINT,
        o_orderstatus: STRING,
        o_totalprice: DECIMAL(12, 2),
        o_orderdate: STRING,
        o_orderpriority: STRING,
        o_clerk: STRING,
        o_shippriority: INT,
        o_comment: STRING,
        o_lineitems: ARRAY<STRUCT<
          l_partkey: BIGINT,
          l_suppkey: BIGINT,
          l_linenumber: INT,
          l_quantity: DECIMAL(12, 2),
          l_extendedprice: DECIMAL(12, 2),
          l_discount: DECIMAL(12, 2),
          l_tax: DECIMAL(12, 2),
          l_returnflag: STRING,
          l_linestatus: STRING,
          l_shipdate: STRING,
          l_commitdate: STRING,
          l_receiptdate: STRING,
          l_shipinstruct: STRING,
          l_shipmode: STRING,
          l_comment: STRING>>>>)
    STORED AS TEXTFILE
    LOCATION '$BASE_DIR/$TARGET_DATABASE.db/tmp_customer_string';"

# Create the temporary region table with nested nation. This table doesn't seem to get too
# big so we don't partition it (like we did with customer).
${SHELL_CMD} -q "
  USE $TARGET_DATABASE;

  CREATE TABLE tmp_region_string
  AS SELECT
    r_regionkey, r_name, r_comment,
    GROUP_CONCAT(
      CONCAT(
        CAST(n_nationkey AS STRING), '\003',
        CAST(n_name AS STRING), '\003',
        CAST(n_comment AS STRING)
      ), '\002'
    ) nations_string
  FROM $SOURCE_DATABASE.region
  JOIN $SOURCE_DATABASE.nation ON r_regionkey = n_regionkey
  GROUP BY 1, 2, 3;"

# Create a table that Hive will be able to convert to nested parquet.
${SHELL_CMD} -q "
  USE $TARGET_DATABASE;

  CREATE EXTERNAL TABLE tmp_region (
    r_regionkey SMALLINT,
    r_name STRING,
    r_comment STRING,
    r_nations ARRAY<STRUCT<
      n_nationkey: SMALLINT,
      n_name: STRING,
      n_comment: STRING>>)
  STORED AS TEXTFILE
  LOCATION '$BASE_DIR/$TARGET_DATABASE.db/tmp_region_string';"

# Suppliers with nested foreign keys to the parts they supply. Several suppliers
# supply the same part so the actual part data is not nested to avoid duplicated
# data.
${SHELL_CMD} -q "
  USE $TARGET_DATABASE;

  CREATE TABLE tmp_supplier_string AS
  SELECT
    s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment,
    GROUP_CONCAT(
      CONCAT(
        CAST(ps_partkey AS STRING), '\003',
        CAST(ps_availqty AS STRING), '\003',
        CAST(ps_supplycost AS STRING), '\003',
        CAST(ps_comment AS STRING)
      ), '\002'
    ) partsupps_string
  FROM $SOURCE_DATABASE.supplier
  JOIN $SOURCE_DATABASE.partsupp ON s_suppkey = ps_suppkey
  GROUP BY 1, 2, 3, 4, 5, 6, 7;"

# Create a table that Hive will be able to convert to nested parquet.
${SHELL_CMD} -q "
  USE $TARGET_DATABASE;

  CREATE EXTERNAL TABLE tmp_supplier (
    s_suppkey BIGINT,
    s_name STRING,
    s_address STRING,
    s_nationkey SMALLINT,
    s_phone STRING,
    s_acctbal DECIMAL(12,2),
    s_comment STRING,
    s_partsupps ARRAY<STRUCT<
      ps_partkey: BIGINT,
      ps_availqty: INT,
      ps_supplycost: DECIMAL(12,2),
      ps_comment: STRING>>)
  STORED AS TEXTFILE
  LOCATION '$BASE_DIR/$TARGET_DATABASE.db/tmp_supplier_string';"

# Copy the part table.
${SHELL_CMD} -q "
  USE $TARGET_DATABASE;

  CREATE EXTERNAL TABLE part
  STORED AS PARQUET
  AS SELECT * FROM $SOURCE_DATABASE.part;"

# Hive is used to convert the data into parquet and drop all the temp tables.
# The Hive SET values are necessary to prevent Impala remote reads of parquet files.
# These values are taken from http://blog.cloudera.com/blog/2014/12/the-impala-cookbook.
hive -e "

  SET mapred.min.split.size=1073741824;
  SET parquet.block.size=10737418240;
  SET dfs.block.size=1073741824;

  USE $TARGET_DATABASE;

  CREATE TABLE customer
  STORED AS PARQUET
  AS SELECT * FROM tmp_customer;

  DROP TABLE tmp_orders_string;
  DROP TABLE tmp_customer_string;
  DROP TABLE tmp_customer;

  CREATE TABLE region
  STORED AS PARQUET
  AS SELECT * FROM tmp_region;

  DROP TABLE tmp_region_string;
  DROP TABLE tmp_region;

  CREATE TABLE supplier
  STORED AS PARQUET
  AS SELECT * FROM tmp_supplier;

  DROP TABLE tmp_supplier;
  DROP TABLE tmp_supplier_string;"
