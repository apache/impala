#!/usr/bin/env impala-python
#
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

'''This script creates a nested version of TPC-H. Non-nested TPC-H must already be
   loaded.
'''
from __future__ import absolute_import, division, print_function
from builtins import range
import logging
import os

from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from tests.common.environ import HIVE_MAJOR_VERSION
import tests.comparison.cli_options as cli_options


LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

# We use Hive to transform nested text tables into parquet/orc tables. To control the
# compression, the property keys differ from file formats (see COMPRESSION_KEYS_MAP).
# The property values also differ from short names used in Impala (e.g. snap, def).
# So we define COMPRESSION_VALUES_MAP for the mapping.
COMPRESSION_KEYS_MAP = {
  "parquet": "parquet.compression",
  "orc": "orc.compress"
}
COMPRESSION_VALUES_MAP = {
  # Currently, only three codecs are supported in Hive for Parquet. See codes in
  # org.apache.parquet.hadoop.metadata.CompressionCodecName (parquet-hadoop-bundle)
  "parquet": {
    "none": "SNAPPY",
    "snap": "SNAPPY",
    "gzip": "GZIP"
  },
  # Currently, only three codecs are supported in Hive for ORC. See Hive codes in
  # org.apache.orc.impl.WriterImpl#createCodec (in module hive-orc)
  "orc": {
    "none": "NONE",
    "def": "ZLIB",
    "snap": "SNAPPY"
  }
}

# These vars are set after arg parsing.
cluster = None
source_db = None
target_db = None
file_format = None
compression_key = None
compression_value = None
chunks = None

def is_loaded():
  with cluster.impala.cursor() as cursor:
    try:
      # If the part table exists, assume everything is already loaded.
      cursor.execute("DESCRIBE %s.part" % target_db)
      return True
    except Exception as e:
      if "AnalysisException" not in str(e):
        raise
      return False


def load():
  # As of this writing, Impala isn't able to write nested data in parquet format.
  # Instead, the data will be written in text format, then Hive will be used to
  # convert from text to parquet.

  with cluster.impala.cursor() as impala:
    impala.ensure_empty_db(target_db)
    impala.execute("USE %s" % target_db)
    external = ""
    tblproperties = "'{0}' = '{1}'".format(compression_key, compression_value)
    # For Hive 3+, workaround for HIVE-22371 (CTAS puts files in the wrong place) by
    # explicitly creating an external table so that files are in the external warehouse
    # directory. Use external.table.purge=true so that it is equivalent to a Hive 2
    # managed table.
    # For Apache Hive, HIVE-20085 (Hive 4) Allow CTAS.
    if HIVE_MAJOR_VERSION >= 3 and os.environ["USE_APACHE_HIVE"] != "true":
      external = "EXTERNAL"
      tblproperties += ",'external.table.purge'='TRUE'"
    sql_params = {
        "source_db": source_db,
        "target_db": target_db,
        "file_format": file_format,
        "chunks": chunks,
        "warehouse_dir": cluster.hive.warehouse_dir,
        "tblproperties": tblproperties,
        "external": external}

    # Split table creation into multiple queries or "chunks" so less memory is needed.
    for chunk_idx in range(chunks):
      sql_params["chunk_idx"] = chunk_idx

      # Create the nested data in text format. The \00#'s are nested field terminators,
      # where the numbers correspond to the nesting level.
      tmp_orders_sql = r"""
          SELECT STRAIGHT_JOIN
            o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
            o_orderpriority, o_clerk, o_shippriority, o_comment,
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
          FROM {source_db}.lineitem
          INNER JOIN [SHUFFLE] {source_db}.orders ON o_orderkey = l_orderkey
          WHERE o_orderkey % {chunks} = {chunk_idx}
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9""".format(**sql_params)
      LOG.info("Creating temp orders (chunk {chunk} of {chunks})".format(
          chunk=(chunk_idx + 1), chunks=chunks))
      if chunk_idx == 0:
        impala.execute("CREATE TABLE tmp_orders_string AS " + tmp_orders_sql)
      else:
        impala.execute("INSERT INTO TABLE tmp_orders_string " + tmp_orders_sql)

    for chunk_idx in range(chunks):
      sql_params["chunk_idx"] = chunk_idx
      tmp_customer_sql = r"""
          SELECT STRAIGHT_JOIN
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
          FROM tmp_orders_string
          RIGHT JOIN [SHUFFLE] {source_db}.customer ON c_custkey = o_custkey
          WHERE c_custkey % {chunks} = {chunk_idx}
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8""".format(**sql_params)
      LOG.info("Creating temp customers (chunk {chunk} of {chunks})".format(
          chunk=(chunk_idx + 1), chunks=chunks))
      if chunk_idx == 0:
        impala.execute("CREATE TABLE tmp_customer_string AS " + tmp_customer_sql)
      else:
        impala.execute("INSERT INTO TABLE tmp_customer_string " + tmp_customer_sql)

    # Create a table with nested schema to read the text file we generated above. Impala
    # is currently unable to read from this table. We will use Hive to read from it in
    # order to convert the table to parquet.
    impala.execute("""
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
        LOCATION '{warehouse_dir}/{target_db}.db/tmp_customer_string'"""\
            .format(**sql_params))

    # Create the temporary region table with nested nation. This table doesn't seem to
    # get too big so we don't partition it (like we did with customer).
    LOG.info("Creating temp regions")
    impala.execute(r"""
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
        FROM {source_db}.region
        JOIN {source_db}.nation ON r_regionkey = n_regionkey
        GROUP BY 1, 2, 3""".format(**sql_params))
    impala.execute("""
        CREATE EXTERNAL TABLE tmp_region (
          r_regionkey SMALLINT,
          r_name STRING,
          r_comment STRING,
          r_nations ARRAY<STRUCT<
            n_nationkey: SMALLINT,
            n_name: STRING,
            n_comment: STRING>>)
        STORED AS TEXTFILE
        LOCATION '{warehouse_dir}/{target_db}.db/tmp_region_string'"""\
            .format(**sql_params))

    # Several suppliers supply the same part so the actual part data is not nested to
    # avoid duplicated data.
    LOG.info("Creating temp suppliers")
    impala.execute(r"""
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
      FROM {source_db}.supplier
      JOIN {source_db}.partsupp ON s_suppkey = ps_suppkey
      GROUP BY 1, 2, 3, 4, 5, 6, 7""".format(**sql_params))

    impala.execute("""
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
      LOCATION '{warehouse_dir}/{target_db}.db/tmp_supplier_string'"""\
          .format(**sql_params))

    # The part table doesn't have nesting. If it's a parquet table, we create it in Impala
    LOG.info("Creating parts")
    if file_format == "parquet":
      impala.execute("""
        CREATE EXTERNAL TABLE part
        STORED AS PARQUET
        AS SELECT * FROM {source_db}.part""".format(**sql_params))
  cluster.hdfs.ensure_home_dir()
  if file_format == "orc":
    # For ORC format, we create the 'part' table by Hive
    with cluster.hive.cursor(db_name=target_db) as hive:
      hive.execute("""
        CREATE {external} TABLE part
        STORED AS ORC
        TBLPROPERTIES({tblproperties})
        AS SELECT * FROM {source_db}.part""".format(**sql_params))

  # Hive is used to convert the data into parquet/orc and drop all the temp tables.
  # The Hive SET values are necessary to prevent Impala remote reads of parquet files.
  # These values are taken from http://blog.cloudera.com/blog/2014/12/the-impala-cookbook.
  create_final_tables_sql = """
      SET mapred.min.split.size=1073741824;
      SET parquet.block.size=10737418240;
      SET dfs.block.size=1073741824;

      CREATE {external} TABLE region
      STORED AS {file_format}
      TBLPROPERTIES({tblproperties})
      AS SELECT * FROM tmp_region;

      CREATE {external} TABLE supplier
      STORED AS {file_format}
      TBLPROPERTIES({tblproperties})
      AS SELECT * FROM tmp_supplier;"""

  # A simple CTAS for tpch_nested_parquet.customer would create 3 files with Hive3 vs
  # 4 files with Hive2. This difference would break several tests, and it seemed
  # easier to hack the loading of the table than to add Hive version specific behavior
  # for each affected test. A small part of the table is inserted in a separate statement
  # to generate the +1 file (needs hive.merge.tezfiles to avoid creating +3 files).
  # TODO: find a less hacky way to ensure a fix number of files
  if HIVE_MAJOR_VERSION >= 3 and file_format == "parquet":
    create_final_tables_sql += """
        CREATE {external} TABLE customer
        STORED AS {file_format}
        TBLPROPERTIES({tblproperties})
        AS SELECT * FROM tmp_customer
        WHERE c_custkey >= 10;

        INSERT INTO customer
        SELECT * FROM tmp_customer
        WHERE c_custkey < 10;"""
  else:
    create_final_tables_sql += """
        CREATE {external} TABLE customer
        STORED AS {file_format}
        TBLPROPERTIES({tblproperties})
        AS SELECT * FROM tmp_customer;"""

  with cluster.hive.cursor(db_name=target_db) as hive:
    LOG.info("Converting temp tables")
    for stmt in create_final_tables_sql.format(**sql_params).split(";"):
      if not stmt.strip():
        continue
      LOG.info("Executing: {0}".format(stmt))
      hive.execute(stmt)

  with cluster.impala.cursor(db_name=target_db) as impala:
    # Drop the temporary tables. These temporary tables were created
    # in Impala, so they exist in Impala's metadata. This drop is executed by
    # Impala so that the metadata is automatically updated.
    for stmt in """
        DROP TABLE tmp_orders_string;
        DROP TABLE tmp_customer_string;
        DROP TABLE tmp_customer;

        DROP TABLE tmp_region_string;
        DROP TABLE tmp_region;

        DROP TABLE tmp_supplier;
        DROP TABLE tmp_supplier_string;""".split(";"):
      if not stmt.strip():
        continue
      LOG.info("Executing: {0}".format(stmt))
      impala.execute(stmt)

    impala.invalidate_metadata(table_name="customer")
    impala.invalidate_metadata(table_name="part")
    impala.invalidate_metadata(table_name="region")
    impala.invalidate_metadata(table_name="supplier")
    impala.compute_stats()

  LOG.info("Done loading nested TPCH data")

if __name__ == "__main__":

  parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
  cli_options.add_logging_options(parser)
  cli_options.add_cluster_options(parser)  # --cm-host and similar args added here
  cli_options.add_kerberos_options(parser)
  cli_options.add_ssl_options(parser)

  parser.add_argument("-s", "--source-db", default="tpch_parquet")
  parser.add_argument("-t", "--target-db", default="tpch_nested_parquet")
  parser.add_argument("-f", "--table-format", default="parquet/none")  # can be "orc/def"
  parser.add_argument("-c", "-p", "--chunks", type=int, default=1)

  args = parser.parse_args()

  cli_options.configure_logging(args.log_level, debug_log_file=args.debug_log_file)

  cluster = cli_options.create_cluster(args)
  source_db = args.source_db
  target_db = args.target_db
  file_format, compression_value = args.table_format.split("/")
  # 'compression_value' is one of [none,def,gzip,bzip,snap]. We should translate it
  # into values that can be set to Hive.
  if file_format not in COMPRESSION_KEYS_MAP:
    raise Exception("Nested types in file format %s are not supported" % file_format)
  compression_key = COMPRESSION_KEYS_MAP[file_format]
  if compression_value not in COMPRESSION_VALUES_MAP[file_format]:
    raise Exception("Loading %s tables in %s compression is not supported by Hive. "
                    "Supported compressions: %s"
                    % (file_format, compression_value,
                       str(COMPRESSION_VALUES_MAP[file_format].keys())))
  compression_value = COMPRESSION_VALUES_MAP[file_format][compression_value]
  chunks = args.chunks

  if is_loaded():
    LOG.info("Data is already loaded")
  else:
    load()
