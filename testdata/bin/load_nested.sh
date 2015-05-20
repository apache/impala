#!/bin/bash
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

set -e

DATABASE=tpch_nested_parquet
SHELL_CMD=${IMPALA_HOME}/bin/impala-shell.sh

# If the part table exists, assume everything is already loaded.
if ${SHELL_CMD} -q "desc $DATABASE.part" &>/dev/null; then
  echo $DATABASE already loaded
  exit
fi

# As of this writing, Impala isn't able to write nested data in parquet format.
# Instead, the data will be written in text format, then Hive will be used to
# convert from text to parquet.

# Create the nested data in text format. The \00#'s are nested field terminators,
# where the numbers correspond to the nesting level.
${SHELL_CMD} -q "
    CREATE DATABASE IF NOT EXISTS $DATABASE;
    USE $DATABASE;

    /* Create customers, with nested orders including order line items. Creation is
       split into two queries so less memory is needed.
    */
    CREATE TABLE IF NOT EXISTS tmp_customer_orders
    AS SELECT STRAIGHT_JOIN
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
    FROM tpch_parquet.lineitem
    INNER JOIN [SHUFFLE] tpch_parquet.orders ON o_orderkey = l_orderkey
    GROUP BY 1, 2, 3 , 4, 5, 6, 7, 8, 9;

    CREATE EXTERNAL TABLE IF NOT EXISTS tmp_customer_string
    AS SELECT
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
    FROM tpch_parquet.customer
    LEFT JOIN tmp_customer_orders ON c_custkey = o_custkey
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

    CREATE EXTERNAL TABLE IF NOT EXISTS tmp_customer (
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
    LOCATION '/test-warehouse/$DATABASE.db/tmp_customer_string';

    CREATE TABLE IF NOT EXISTS tmp_region_string
    AS SELECT
      r_regionkey, r_name, r_comment,
      GROUP_CONCAT(
        CONCAT(
          CAST(n_nationkey AS STRING), '\003',
          CAST(n_name AS STRING), '\003',
          CAST(n_comment AS STRING)
        ), '\002'
      ) nations_string
    FROM tpch_parquet.region
    JOIN tpch_parquet.nation ON r_regionkey = n_regionkey
    GROUP BY 1, 2, 3;

    CREATE TABLE IF NOT EXISTS tmp_region (
      r_regionkey SMALLINT,
      r_name STRING,
      r_comment STRING,
      r_nations ARRAY<STRUCT<
        n_nationkey: SMALLINT,
        n_name: STRING,
        n_comment: STRING>>)
    STORED AS TEXTFILE
    LOCATION '/test-warehouse/$DATABASE.db/tmp_region_string';

    /* Suppliers with nested foreign keys to the parts they supply. Several suppliers
        supply the same part so the actual part data is not nested to avoid duplicated
        data.
    */
    CREATE TABLE IF NOT EXISTS tmp_supplier_string AS
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
    FROM tpch_parquet.supplier
    JOIN tpch_parquet.partsupp ON s_suppkey = ps_suppkey
    GROUP BY 1, 2, 3, 4, 5, 6, 7;

    CREATE EXTERNAL TABLE IF NOT EXISTS tmp_supplier (
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
    LOCATION '/test-warehouse/$DATABASE.db/tmp_supplier_string';

    CREATE TABLE IF NOT EXISTS $DATABASE.part
    STORED AS PARQUET
    AS SELECT * FROM tpch_parquet.part LIMIT 0;

    ALTER TABLE $DATABASE.part SET LOCATION '/test-warehouse/tpch.part_parquet';"

# Hive is used to convert the data into parquet.
hive -e "
    USE $DATABASE;

    CREATE TABLE IF NOT EXISTS customer
    STORED AS PARQUET
    AS SELECT * FROM tmp_customer;

    DROP TABLE tmp_customer;
    DROP TABLE tmp_customer_string;
    DROP TABLE tmp_customer_orders;

    CREATE TABLE IF NOT EXISTS region
    STORED AS PARQUET
    AS SELECT * FROM tmp_region;

    DROP TABLE tmp_region_string;
    DROP TABLE tmp_region;

    CREATE TABLE IF NOT EXISTS supplier
    STORED AS PARQUET
    AS SELECT * FROM tmp_supplier;

    DROP TABLE tmp_supplier;
    DROP TABLE tmp_supplier_string;"
