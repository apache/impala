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

from kudu.schema import (
    BOOL,
    DOUBLE,
    FLOAT,
    INT16,
    INT32,
    INT64,
    INT8,
    STRING,
    BINARY,
    UNIXTIME_MICROS)
import logging
import pytest
import textwrap

from tests.common import KUDU_MASTER_HOSTS
from tests.common.kudu_test_suite import KuduTestSuite

LOG = logging.getLogger(__name__)

class TestKuduOperations(KuduTestSuite):
  """
  This suite tests the different modification operations when using a kudu table.
  """

  def test_kudu_scan_node(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu-scan-node', vector, use_db=unique_database)

  def test_kudu_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_insert', vector, use_db=unique_database)

  def test_kudu_update(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_update', vector, use_db=unique_database)

  def test_kudu_upsert(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_upsert', vector, use_db=unique_database)

  def test_kudu_delete(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_delete', vector, use_db=unique_database)

  def test_kudu_partition_ddl(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_partition_ddl', vector, use_db=unique_database)

  @pytest.mark.execute_serially
  def test_kudu_alter_table(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_alter', vector, use_db=unique_database)

  def test_kudu_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_stats', vector, use_db=unique_database)

  def test_kudu_describe(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_describe', vector, use_db=unique_database)

  def test_kudu_column_options(self, cursor, kudu_client, unique_database):
    """Test Kudu column options"""
    encodings = ["ENCODING PLAIN_ENCODING", ""]
    compressions = ["COMPRESSION SNAPPY", ""]
    nullability = ["NOT NULL", "NULL", ""]
    defaults = ["DEFAULT 1", ""]
    blocksizes = ["BLOCK_SIZE 32768", ""]
    indx = 1
    for encoding in encodings:
      for compression in compressions:
        for default in defaults:
          for blocksize in blocksizes:
            for nullable in nullability:
              impala_tbl_name = "test_column_options_%s" % str(indx)
              cursor.execute("""CREATE TABLE %s.%s (a INT PRIMARY KEY
                  %s %s %s %s, b INT %s %s %s %s %s) PARTITION BY HASH (a)
                  PARTITIONS 3 STORED AS KUDU""" % (unique_database, impala_tbl_name,
                  encoding, compression, default, blocksize, nullable, encoding,
                  compression, default, blocksize))
              indx = indx + 1
              kudu_tbl_name = "impala::%s.%s" % (unique_database, impala_tbl_name)
              assert kudu_client.table_exists(kudu_tbl_name)

  def test_kudu_rename_table(self, cursor, kudu_client, unique_database):
    """Test Kudu table rename"""
    cursor.execute("""CREATE TABLE %s.foo (a INT PRIMARY KEY) PARTITION BY HASH(a)
        PARTITIONS 3 STORED AS KUDU""" % unique_database)
    kudu_tbl_name = "impala::%s.foo" % unique_database
    assert kudu_client.table_exists(kudu_tbl_name)
    new_kudu_tbl_name = "blah"
    cursor.execute("ALTER TABLE %s.foo SET TBLPROPERTIES('kudu.table_name'='%s')" % (
        unique_database, new_kudu_tbl_name))
    assert kudu_client.table_exists(new_kudu_tbl_name)
    assert not kudu_client.table_exists(kudu_tbl_name)

class TestCreateExternalTable(KuduTestSuite):

  def test_implicit_table_props(self, cursor, kudu_client):
    """Check that table properties added internally during table creation are as
       expected.
    """
    with self.temp_kudu_table(kudu_client, [STRING, INT8, BOOL], num_key_cols=2) \
        as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % kudu_table.name
      cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (impala_table_name,
          props))
      with self.drop_impala_table_after_context(cursor, impala_table_name):
        cursor.execute("DESCRIBE FORMATTED %s" % impala_table_name)
        table_desc = [[col.strip() if col else col for col in row] for row in cursor]
        LOG.info(table_desc)
        # Pytest shows truncated output on failure, so print the details just in case.
        assert ["", "EXTERNAL", "TRUE"] in table_desc
        assert ["", "kudu.master_addresses", KUDU_MASTER_HOSTS] in table_desc
        assert ["", "kudu.table_name", kudu_table.name] in table_desc
        assert ["", "storage_handler", "com.cloudera.kudu.hive.KuduStorageHandler"] \
            in table_desc

  def test_col_types(self, cursor, kudu_client):
    """Check that a table can be created using all available column types."""
    kudu_types = [STRING, BOOL, DOUBLE, FLOAT, INT16, INT32, INT64, INT8]
    with self.temp_kudu_table(kudu_client, kudu_types) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % kudu_table.name
      cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (impala_table_name,
          props))
      with self.drop_impala_table_after_context(cursor, impala_table_name):
        cursor.execute("DESCRIBE %s" % impala_table_name)
        kudu_schema = kudu_table.schema
        for i, (col_name, col_type, _, _, _, _, _, _, _) in enumerate(cursor):
          kudu_col = kudu_schema[i]
          assert col_name == kudu_col.name
          assert col_type.upper() == \
              self.kudu_col_type_to_impala_col_type(kudu_col.type.type)

  def test_unsupported_binary_col(self, cursor, kudu_client):
    """Check that external tables with BINARY columns fail gracefully.
    """
    with self.temp_kudu_table(kudu_client, [INT32, BINARY]) as kudu_table:
      impala_table_name = self.random_table_name()
      try:
        cursor.execute("""
            CREATE EXTERNAL TABLE %s
            STORED AS KUDU
            TBLPROPERTIES('kudu.table_name' = '%s')""" % (impala_table_name,
                kudu_table.name))
      except Exception as e:
        assert "Kudu type 'binary' is not supported in Impala" in str(e)

  def test_unsupported_unixtime_col(self, cursor, kudu_client):
    """Check that external tables with UNIXTIME_MICROS columns fail gracefully.
    """
    with self.temp_kudu_table(kudu_client, [INT32, UNIXTIME_MICROS]) as kudu_table:
      impala_table_name = self.random_table_name()
      try:
        cursor.execute("""
            CREATE EXTERNAL TABLE %s
            STORED AS KUDU
            TBLPROPERTIES('kudu.table_name' = '%s')""" % (impala_table_name,
                kudu_table.name))
      except Exception as e:
        assert "Kudu type 'unixtime_micros' is not supported in Impala" in str(e)

  def test_drop_external_table(self, cursor, kudu_client):
    """Check that dropping an external table only affects the catalog and does not delete
       the table in Kudu.
    """
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % kudu_table.name
      cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (impala_table_name,
          props))
      with self.drop_impala_table_after_context(cursor, impala_table_name):
        cursor.execute("SELECT COUNT(*) FROM %s" % impala_table_name)
        assert cursor.fetchall() == [(0, )]
      try:
        cursor.execute("SELECT COUNT(*) FROM %s" % impala_table_name)
        assert False
      except Exception as e:
        assert "Could not resolve table reference" in str(e)
      assert kudu_client.table_exists(kudu_table.name)

  def test_explicit_name(self, cursor, kudu_client):
    """Check that a Kudu table can be specified using a table property."""
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      table_name = self.random_table_name()
      cursor.execute("""
          CREATE EXTERNAL TABLE %s
          STORED AS KUDU
          TBLPROPERTIES('kudu.table_name' = '%s')""" % (table_name, kudu_table.name))
      with self.drop_impala_table_after_context(cursor, table_name):
        cursor.execute("SELECT * FROM %s" % table_name)
        assert len(cursor.fetchall()) == 0

  def test_explicit_name_preference(self, cursor, kudu_client):
    """Check that the table name from a table property is used when a table of the
       implied name also exists.
    """
    with self.temp_kudu_table(kudu_client, [INT64]) as preferred_kudu_table:
      with self.temp_kudu_table(kudu_client, [INT8]) as other_kudu_table:
        impala_table_name = self.get_kudu_table_base_name(other_kudu_table.name)
        cursor.execute("""
            CREATE EXTERNAL TABLE %s
            STORED AS KUDU
            TBLPROPERTIES('kudu.table_name' = '%s')""" % (
                impala_table_name, preferred_kudu_table.name))
        with self.drop_impala_table_after_context(cursor, impala_table_name):
          cursor.execute("DESCRIBE %s" % impala_table_name)
          assert cursor.fetchall() == \
              [("a", "bigint", "", "true", "false", "", "AUTO_ENCODING",
                "DEFAULT_COMPRESSION", "0")]

  def test_explicit_name_doesnt_exist(self, cursor, kudu_client):
    kudu_table_name = self.random_table_name()
    try:
      cursor.execute("""
          CREATE EXTERNAL TABLE %s
          STORED AS KUDU
          TBLPROPERTIES('kudu.table_name' = '%s')""" % (
              self.random_table_name(), kudu_table_name))
    except Exception as e:
      assert "Table does not exist in Kudu: '%s'" % kudu_table_name in str(e)

  def test_explicit_name_doesnt_exist_but_implicit_does(self, cursor, kudu_client):
    """Check that when an explicit table name is given but that table doesn't exist,
       there is no fall-through to an existing implicit table.
    """
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      table_name = self.random_table_name()
      try:
        cursor.execute("""
            CREATE EXTERNAL TABLE %s
            STORED AS KUDU
            TBLPROPERTIES('kudu.table_name' = '%s')""" % (
              self.get_kudu_table_base_name(kudu_table.name), table_name))
      except Exception as e:
        assert "Table does not exist in Kudu: '%s'" % table_name in str(e)


class TestShowCreateTable(KuduTestSuite):

  @classmethod
  def get_conn_timeout(cls):
    # For IMPALA-4454
    return 60 * 5 # 5 minutes

  def assert_show_create_equals(self, cursor, create_sql, show_create_sql):
    """Executes 'create_sql' to create a table, then runs "SHOW CREATE TABLE" and checks
       that the output is the same as 'show_create_sql'. 'create_sql' and
       'show_create_sql' can be templates that can be used with str.format(). format()
       will be called with 'table' and 'db' as keyword args.
    """
    format_args = {"table": self.random_table_name(), "db": cursor.conn.db_name}
    cursor.execute(create_sql.format(**format_args))
    cursor.execute("SHOW CREATE TABLE {table}".format(**format_args))
    assert cursor.fetchall()[0][0] == \
        textwrap.dedent(show_create_sql.format(**format_args)).strip()

  def test_primary_key_and_distribution(self, cursor):
    # TODO: Add test cases with column comments once KUDU-1711 is fixed.
    # TODO: Add case with BLOCK_SIZE
    self.assert_show_create_equals(cursor,
        """
        CREATE TABLE {table} (c INT PRIMARY KEY)
        PARTITION BY HASH (c) PARTITIONS 3 STORED AS KUDU""",
        """
        CREATE TABLE {db}.{{table}} (
          c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
          PRIMARY KEY (c)
        )
        PARTITION BY HASH (c) PARTITIONS 3
        STORED AS KUDU
        TBLPROPERTIES ('kudu.master_addresses'='{kudu_addr}')""".format(
            db=cursor.conn.db_name, kudu_addr=KUDU_MASTER_HOSTS))
    self.assert_show_create_equals(cursor,
        """
        CREATE TABLE {table} (c INT PRIMARY KEY, d STRING NULL)
        PARTITION BY HASH (c) PARTITIONS 3, RANGE (c)
        (PARTITION VALUES <= 1, PARTITION 1 < VALUES <= 2,
         PARTITION 2 < VALUES) STORED AS KUDU""",
        """
        CREATE TABLE {db}.{{table}} (
          c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
          d STRING NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
          PRIMARY KEY (c)
        )
        PARTITION BY HASH (c) PARTITIONS 3, RANGE (c) (...)
        STORED AS KUDU
        TBLPROPERTIES ('kudu.master_addresses'='{kudu_addr}')""".format(
            db=cursor.conn.db_name, kudu_addr=KUDU_MASTER_HOSTS))
    self.assert_show_create_equals(cursor,
        """
        CREATE TABLE {table} (c INT ENCODING PLAIN_ENCODING, PRIMARY KEY (c))
        PARTITION BY HASH (c) PARTITIONS 3 STORED AS KUDU""",
        """
        CREATE TABLE {db}.{{table}} (
          c INT NOT NULL ENCODING PLAIN_ENCODING COMPRESSION DEFAULT_COMPRESSION,
          PRIMARY KEY (c)
        )
        PARTITION BY HASH (c) PARTITIONS 3
        STORED AS KUDU
        TBLPROPERTIES ('kudu.master_addresses'='{kudu_addr}')""".format(
            db=cursor.conn.db_name, kudu_addr=KUDU_MASTER_HOSTS))
    self.assert_show_create_equals(cursor,
        """
        CREATE TABLE {table} (c INT COMPRESSION LZ4, d STRING, PRIMARY KEY(c, d))
        PARTITION BY HASH (c) PARTITIONS 3, HASH (d) PARTITIONS 3,
        RANGE (c, d) (PARTITION VALUE = (1, 'aaa'), PARTITION VALUE = (2, 'bbb'))
        STORED AS KUDU""",
        """
        CREATE TABLE {db}.{{table}} (
          c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION LZ4,
          d STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
          PRIMARY KEY (c, d)
        )
        PARTITION BY HASH (c) PARTITIONS 3, HASH (d) PARTITIONS 3, RANGE (c, d) (...)
        STORED AS KUDU
        TBLPROPERTIES ('kudu.master_addresses'='{kudu_addr}')""".format(
            db=cursor.conn.db_name, kudu_addr=KUDU_MASTER_HOSTS))
    self.assert_show_create_equals(cursor,
        """
        CREATE TABLE {table} (c INT, d STRING, e INT NULL DEFAULT 10, PRIMARY KEY(c, d))
        PARTITION BY RANGE (c) (PARTITION VALUES <= 1, PARTITION 1 < VALUES <= 2,
        PARTITION 2 < VALUES <= 3, PARTITION 3 < VALUES) STORED AS KUDU""",
        """
        CREATE TABLE {db}.{{table}} (
          c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
          d STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
          e INT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION DEFAULT 10,
          PRIMARY KEY (c, d)
        )
        PARTITION BY RANGE (c) (...)
        STORED AS KUDU
        TBLPROPERTIES ('kudu.master_addresses'='{kudu_addr}')""".format(
            db=cursor.conn.db_name, kudu_addr=KUDU_MASTER_HOSTS))

  def test_properties(self, cursor):
    # If an explicit table name is used for the Kudu table and it differs from what
    # would be the default Kudu table name, the name should be shown as a table property.
    kudu_table = self.random_table_name()
    props = "'kudu.table_name'='%s'" % kudu_table
    self.assert_show_create_equals(cursor,
        """
        CREATE TABLE {{table}} (c INT PRIMARY KEY)
        PARTITION BY HASH (c) PARTITIONS 3
        STORED AS KUDU
        TBLPROPERTIES ({props})""".format(props=props),
        """
        CREATE TABLE {db}.{{table}} (
          c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
          PRIMARY KEY (c)
        )
        PARTITION BY HASH (c) PARTITIONS 3
        STORED AS KUDU
        TBLPROPERTIES ('kudu.master_addresses'='{kudu_addr}', {props})""".format(
            db=cursor.conn.db_name, kudu_addr=KUDU_MASTER_HOSTS, props=props))

    # If the name is explicitly given (or not given at all) so that the name is the same
    # as the default name, the table name is not shown.
    props = "'kudu.table_name'='impala::{db}.{table}'"
    self.assert_show_create_equals(cursor,
        """
        CREATE TABLE {{table}} (c INT PRIMARY KEY)
        PARTITION BY HASH (c) PARTITIONS 3
        STORED AS KUDU
        TBLPROPERTIES ({props})""".format(props=props),
        """
        CREATE TABLE {db}.{{table}} (
          c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
          PRIMARY KEY (c)
        )
        PARTITION BY HASH (c) PARTITIONS 3
        STORED AS KUDU
        TBLPROPERTIES ('kudu.master_addresses'='{kudu_addr}')""".format(
            db=cursor.conn.db_name, kudu_addr=KUDU_MASTER_HOSTS))


class TestDropDb(KuduTestSuite):

  def test_drop_non_empty_db(self, unique_cursor, kudu_client):
    """Check that an attempt to drop a database will fail if Kudu tables are present
       and that the tables remain.
    """
    db_name = unique_cursor.conn.db_name
    with self.temp_kudu_table(kudu_client, [INT32], db_name=db_name) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % kudu_table.name
      unique_cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (
          impala_table_name, props))
      unique_cursor.execute("USE DEFAULT")
      try:
        unique_cursor.execute("DROP DATABASE %s" % db_name)
        assert False
      except Exception as e:
        assert "One or more tables exist" in str(e)
      unique_cursor.execute("SELECT COUNT(*) FROM %s.%s" % (db_name, impala_table_name))
      assert unique_cursor.fetchall() == [(0, )]

  def test_drop_db_cascade(self, unique_cursor, kudu_client):
    """Check that an attempt to drop a database will succeed even if Kudu tables are
       present and that the managed tables are removed.
    """
    db_name = unique_cursor.conn.db_name
    with self.temp_kudu_table(kudu_client, [INT32], db_name=db_name) as kudu_table:
      # Create an external Kudu table
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % kudu_table.name
      unique_cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (
          impala_table_name, props))

      # Create a managed Kudu table
      managed_table_name = self.random_table_name()
      unique_cursor.execute("""
          CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY HASH (a) PARTITIONS 3
          STORED AS KUDU TBLPROPERTIES ('kudu.table_name' = '%s')"""
          % (managed_table_name, managed_table_name))
      assert kudu_client.table_exists(managed_table_name)

      # Create a table in HDFS
      hdfs_table_name = self.random_table_name()
      unique_cursor.execute("""
          CREATE TABLE %s (a INT) PARTITIONED BY (x INT)""" % (hdfs_table_name))

      unique_cursor.execute("USE DEFAULT")
      unique_cursor.execute("DROP DATABASE %s CASCADE" % db_name)
      unique_cursor.execute("SHOW DATABASES")
      assert (db_name, '') not in unique_cursor.fetchall()
      assert kudu_client.table_exists(kudu_table.name)
      assert not kudu_client.table_exists(managed_table_name)

class TestImpalaKuduIntegration(KuduTestSuite):
  def test_replace_kudu_table(self, cursor, kudu_client):
    """Check that an external Kudu table is accessible if the underlying Kudu table is
        modified using the Kudu client.
    """
    # Create an external Kudu table
    col_names = ['a']
    with self.temp_kudu_table(kudu_client, [INT32], col_names=col_names) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % kudu_table.name
      cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (
          impala_table_name, props))
      cursor.execute("DESCRIBE %s" % (impala_table_name))
      assert cursor.fetchall() == \
          [("a", "int", "", "true", "false", "", "AUTO_ENCODING",
            "DEFAULT_COMPRESSION", "0")]

      # Drop the underlying Kudu table and replace it with another Kudu table that has
      # the same name but different schema
      kudu_client.delete_table(kudu_table.name)
      assert not kudu_client.table_exists(kudu_table.name)
      new_col_names = ['b', 'c']
      name_parts = kudu_table.name.split(".")
      assert len(name_parts) == 2
      with self.temp_kudu_table(kudu_client, [STRING, STRING], col_names=new_col_names,
          db_name=name_parts[0], name= name_parts[1]) as new_kudu_table:
        assert kudu_client.table_exists(new_kudu_table.name)
        # Refresh the external table and verify that the new schema is loaded from
        # Kudu.
        cursor.execute("REFRESH %s" % (impala_table_name))
        cursor.execute("DESCRIBE %s" % (impala_table_name))
        assert cursor.fetchall() == \
            [("b", "string", "", "true", "false", "", "AUTO_ENCODING",
              "DEFAULT_COMPRESSION", "0"),
             ("c", "string", "", "false", "true", "", "AUTO_ENCODING",
              "DEFAULT_COMPRESSION", "0")]

  def test_delete_external_kudu_table(self, cursor, kudu_client):
    """Check that Impala can recover from the case where the underlying Kudu table of
        an external table is dropped using the Kudu client.
    """
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      # Create an external Kudu table
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % kudu_table.name
      cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (
          impala_table_name, props))
      cursor.execute("DESCRIBE %s" % (impala_table_name))
      assert cursor.fetchall() == \
          [("a", "int", "", "true", "false", "", "AUTO_ENCODING",
            "DEFAULT_COMPRESSION", "0")]
      # Drop the underlying Kudu table
      kudu_client.delete_table(kudu_table.name)
      assert not kudu_client.table_exists(kudu_table.name)
      err_msg = 'The table does not exist: table_name: "%s"' % (kudu_table.name)
      try:
        cursor.execute("REFRESH %s" % (impala_table_name))
      except Exception as e:
        assert err_msg in str(e)
      cursor.execute("DROP TABLE %s" % (impala_table_name))
      cursor.execute("SHOW TABLES")
      assert (impala_table_name,) not in cursor.fetchall()


  def test_delete_managed_kudu_table(self, cursor, kudu_client, unique_database):
    """Check that dropping a managed Kudu table works even if the underlying Kudu table
        has been dropped externally."""
    impala_tbl_name = "foo"
    cursor.execute("""CREATE TABLE %s.%s (a INT PRIMARY KEY) PARTITION BY HASH (a)
        PARTITIONS 3 STORED AS KUDU""" % (unique_database, impala_tbl_name))
    kudu_tbl_name = "impala::%s.%s" % (unique_database, impala_tbl_name)
    assert kudu_client.table_exists(kudu_tbl_name)
    kudu_client.delete_table(kudu_tbl_name)
    assert not kudu_client.table_exists(kudu_tbl_name)
    cursor.execute("DROP TABLE %s.%s" % (unique_database, impala_tbl_name))
    cursor.execute("SHOW TABLES IN %s" % unique_database)
    assert (impala_tbl_name,) not in cursor.fetchall()

class TestKuduMemLimits(KuduTestSuite):

  QUERIES = ["select * from lineitem where l_orderkey = -1",
             "select * from lineitem where l_commitdate like '%cheese'",
             "select * from lineitem limit 90"]

  # The value indicates the minimum memory requirements for the queries above, the first
  # memory limit corresponds to the first query
  QUERY_MEM_LIMITS = [1, 1, 10]

  CREATE = """
    CREATE TABLE lineitem (
    l_orderkey BIGINT,
    l_linenumber INT,
    l_partkey BIGINT,
    l_suppkey BIGINT,
    l_quantity double,
    l_extendedprice double,
    l_discount double,
    l_tax double,
    l_returnflag STRING,
    l_linestatus STRING,
    l_shipdate STRING,
    l_commitdate STRING,
    l_receiptdate STRING,
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING,
    PRIMARY KEY (l_orderkey, l_linenumber))
  PARTITION BY HASH (l_orderkey, l_linenumber) PARTITIONS 3
  STORED AS KUDU"""

  LOAD = """
  insert into lineitem
  select l_orderkey, l_linenumber, l_partkey, l_suppkey, cast(l_quantity as double),
  cast(l_extendedprice as double), cast(l_discount as double), cast(l_tax as double),
  l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct,
  l_shipmode, l_comment from tpch_parquet.lineitem"""

  @classmethod
  def auto_create_db(cls):
    return True

  @pytest.fixture(scope='class')
  def test_data(cls, cls_cursor):
    cls_cursor.execute(cls.CREATE)
    cls_cursor.execute(cls.LOAD)

  @pytest.mark.execute_serially
  @pytest.mark.usefixtures("test_data")
  @pytest.mark.parametrize("mem_limit", [1, 10, 0])
  def test_low_mem_limit_low_selectivity_scan(self, cursor, mem_limit, vector):
    """Tests that the queries specified in this test suite run under the given
    memory limits."""
    exec_options = dict((k, str(v)) for k, v
                        in vector.get_value('exec_option').iteritems())
    exec_options['mem_limit'] = "{0}m".format(mem_limit)
    for i, q in enumerate(self.QUERIES):
      try:
        cursor.execute(q, configuration=exec_options)
        cursor.fetchall()
      except Exception as e:
        if (mem_limit > self.QUERY_MEM_LIMITS[i]):
          raise
        assert "Memory limit exceeded" in str(e)
