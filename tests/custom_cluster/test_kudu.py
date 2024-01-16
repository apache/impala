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

from __future__ import absolute_import, division, print_function
import logging
import os
import pytest
import tempfile
from kudu.schema import INT32
from time import sleep

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.kudu_test_suite import KuduTestSuite
from tests.common.skip import SkipIfKudu, SkipIfBuildType, SkipIf
from tests.common.test_dimensions import add_mandatory_exec_option
from tests.util.event_processor_utils import EventProcessorUtils

KUDU_MASTER_HOSTS = pytest.config.option.kudu_master_hosts
LOG = logging.getLogger(__name__)


class CustomKuduTest(CustomClusterTestSuite, KuduTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_custom_cluster_constraints(cls):
    # Override this method to relax the set of constraints added in
    # CustomClusterTestSuite.add_custom_cluster_constraints() so that a test vector with
    # 'file_format' and 'compression_codec' being "kudu" and "none" respectively will not
    # be skipped.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('exec_option')['batch_size'] == 0 and
        v.get_value('exec_option')['disable_codegen'] is False and
        v.get_value('exec_option')['num_nodes'] == 0)


class TestKuduOperations(CustomKuduTest):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduOperations, cls).add_test_dimensions()
    # The default read mode of READ_LATEST does not provide high enough consistency for
    # these tests.
    add_mandatory_exec_option(cls, "kudu_read_mode", "READ_AT_SNAPSHOT")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=\
      "--use_local_tz_for_unix_timestamp_conversions=true")
  @SkipIfKudu.no_hybrid_clock()
  @SkipIfKudu.hms_integration_enabled()
  def test_local_tz_conversion_ops(self, vector, unique_database):
    """IMPALA-5539: Test Kudu timestamp reads/writes are correct with the
       use_local_tz_for_unix_timestamp_conversions flag."""
    # These tests provide enough coverage of queries with timestamps.
    self.run_test_case('QueryTest/kudu-scan-node', vector, use_db=unique_database)
    self.run_test_case('QueryTest/kudu_insert', vector, use_db=unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_master_hosts=")
  @SkipIfKudu.hms_integration_enabled()
  def test_kudu_master_hosts(self, cursor, kudu_client):
    """Check behavior when -kudu_master_hosts is not provided to catalogd."""
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      table_name = self.get_kudu_table_base_name(kudu_table.name)
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % (kudu_table.name)
      try:
        cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (table_name,
            props))
        assert False
      except Exception as e:
        assert "Table property 'kudu.master_addresses' is required" in str(e)

      cursor.execute("""
          CREATE EXTERNAL TABLE %s STORED AS KUDU
          TBLPROPERTIES ('kudu.master_addresses' = '%s',
          'kudu.table_name'='%s')
          """ % (table_name, KUDU_MASTER_HOSTS, kudu_table.name))
      cursor.execute("DROP TABLE %s" % table_name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_error_buffer_size=1024")
  @SkipIfKudu.hms_integration_enabled()
  def test_error_buffer_size(self, cursor, unique_database):
    """Check that queries fail if the size of the Kudu client errors they generate is
    greater than kudu_error_buffer_size."""
    table_name = "%s.test_error_buffer_size" % unique_database
    cursor.execute("create table %s (a bigint primary key) stored as kudu" % table_name)
    # Insert a large number of a constant value into the table to generate many "Key
    # already present" errors. 50 errors should fit inside the 1024 byte limit.
    cursor.execute(
        "insert into %s select 1 from functional.alltypes limit 50" % table_name)
    try:
      # 200 errors should overflow the 1024 byte limit.
      cursor.execute(
          "insert into %s select 1 from functional.alltypes limit 200" % table_name)
      assert False, "Expected: 'Error overflow in Kudu session.'"
    except Exception as e:
      assert "Error overflow in Kudu session." in str(e)


class TestKuduClientTimeout(CustomKuduTest):
  """Kudu tests that set the Kudu client operation timeout to 1ms and expect
     specific timeout exceptions. While we expect all exercised operations to take at
     least 1ms, it is possible that some may not and thus the test could be flaky. If
     this turns out to be the case, specific tests may need to be re-considered or
     removed."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_operation_timeout_ms=1")
  @SkipIfKudu.hms_integration_enabled()
  def test_impalad_timeout(self, vector):
    """Check impalad behavior when -kudu_operation_timeout_ms is too low."""
    self.run_test_case('QueryTest/kudu-timeouts-impalad', vector)


@SkipIf.is_test_jdk
class TestKuduHMSIntegration(CustomKuduTest):
  START_END_TIME_LINEAGE_LOG_DIR = tempfile.mkdtemp(prefix="start_end_time")

  # TODO(IMPALA-8614): parameterize the common tests in query_test/test_kudu.py
  # to run with HMS integration enabled. Also avoid restarting Impala to reduce
  # tests time.
  """Tests the different DDL operations when using a kudu table with Kudu's integration
     with the Hive Metastore."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    # Restart Kudu cluster with HMS integration enabled
    KUDU_ARGS = "-hive_metastore_uris=thrift://%s" % os.environ['INTERNAL_LISTEN_HOST']
    cls._restart_kudu_service(KUDU_ARGS)
    super(TestKuduHMSIntegration, cls).setup_class()

  @classmethod
  def teardown_class(cls):
    # Restart Kudu cluster with HMS integration disabled
    cls._restart_kudu_service("-hive_metastore_uris=")
    super(TestKuduHMSIntegration, cls).teardown_class()

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_client_rpc_timeout_ms=30000")
  def test_create_managed_kudu_tables(self, vector, unique_database):
    """Tests the Create table operation when using a kudu table with Kudu's integration
       with the Hive Metastore for managed tables. Increase timeout of individual Kudu
       client rpcs to avoid requests fail due to operation delay in the Hive Metastore
       for managed tables (IMPALA-8856)."""
    vector.get_value('exec_option')['kudu_read_mode'] = "READ_AT_SNAPSHOT"
    self.run_test_case('QueryTest/kudu_create', vector, use_db=unique_database)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_client_rpc_timeout_ms=30000 "
                                    "--lineage_event_log_dir={0}"
                                    .format(START_END_TIME_LINEAGE_LOG_DIR))
  def test_create_kudu_tables_with_lineage_enabled(self, vector, unique_database):
    """Same as above test_create_managed_kudu_tables, but with lineage enabled."""
    vector.get_value('exec_option')['kudu_read_mode'] = "READ_AT_SNAPSHOT"
    self.run_test_case('QueryTest/kudu_create', vector, use_db=unique_database)

  @pytest.mark.execute_serially
  def test_implicit_external_table_props(self, cursor, kudu_client):
    """Check that table properties added internally for external table during
       table creation are as expected.
    """
    db_name = cursor.conn.db_name
    with self.temp_kudu_table(kudu_client, [INT32], db_name=db_name) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      external_table_name = "%s_external" % impala_table_name
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % kudu_table.name
      cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (
          external_table_name, props))
      with self.drop_impala_table_after_context(cursor, external_table_name):
        cursor.execute("DESCRIBE FORMATTED %s" % external_table_name)
        table_desc = [[col.strip() if col else col for col in row] for row in cursor]
        # Pytest shows truncated output on failure, so print the details just in case.
        LOG.info(table_desc)
        assert not any("kudu.table_id" in s for s in table_desc)
        assert any("Owner:" in s for s in table_desc)
        assert ["", "EXTERNAL", "TRUE"] in table_desc
        assert ["", "kudu.master_addresses", KUDU_MASTER_HOSTS] in table_desc
        assert ["", "kudu.table_name", kudu_table.name] in table_desc
        assert ["", "storage_handler", "org.apache.hadoop.hive.kudu.KuduStorageHandler"] \
            in table_desc

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_client_rpc_timeout_ms=30000")
  def test_implicit_managed_table_props(self, cursor, kudu_client, unique_database):
    """Check that table properties added internally for managed table during table
       creation are as expected. Increase timeout of individual Kudu client rpcs to
       avoid requests fail due to operation delay in the Hive Metastore for managed
       tables (IMPALA-8856).
    """
    comment = "kudu_comment"
    cursor.execute("""CREATE TABLE %s.foo (a INT PRIMARY KEY, s STRING) PARTITION BY
        HASH(a) PARTITIONS 3 COMMENT '%s' STORED AS KUDU""" % (unique_database, comment))
    assert kudu_client.table_exists(
      KuduTestSuite.to_kudu_table_name(unique_database, "foo"))
    cursor.execute("DESCRIBE FORMATTED %s.foo" % unique_database)
    table_desc = [[col.strip() if col else col for col in row] for row in cursor]
    # Pytest shows truncated output on failure, so print the details just in case.
    LOG.info(table_desc)

    # Commented out due to differences between toolchain and newer hive
    # assert any("EXTERNAL" in s for s in table_desc)
    # assert ["Table Type:", "EXTERNAL_TABLE", None] in table_desc

    assert any("Owner:" in s for s in table_desc)
    assert any("kudu.table_id" in s for s in table_desc)
    assert any("kudu.master_addresses" in s for s in table_desc)
    assert ["", "comment", "%s" % comment] in table_desc
    assert ["", "kudu.table_name", "%s.foo" % unique_database] in table_desc
    assert ["", "storage_handler", "org.apache.hadoop.hive.kudu.KuduStorageHandler"] \
        in table_desc

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_client_rpc_timeout_ms=30000")
  def test_drop_non_empty_db(self, unique_cursor, kudu_client):
    """Check that an attempt to drop a database will fail if Kudu tables are present
       and that the tables remain. Increase timeout of individual Kudu client rpcs
       to avoid requests fail due to operation delay in the Hive Metastore for managed
       tables (IMPALA-8856).
    """
    db_name = unique_cursor.conn.db_name
    with self.temp_kudu_table(kudu_client, [INT32], db_name=db_name) as kudu_table:
      assert kudu_client.table_exists(kudu_table.name)
      unique_cursor.execute("INVALIDATE METADATA")
      unique_cursor.execute("USE DEFAULT")
      try:
        unique_cursor.execute("DROP DATABASE %s" % db_name)
        assert False
      except Exception as e:
        assert "One or more tables exist" in str(e)

      # Dropping an empty database should succeed, once all tables
      # from the database have been dropped.
      assert kudu_client.table_exists(kudu_table.name)
      unique_cursor.execute("DROP Table %s" % kudu_table.name)
      unique_cursor.execute("DROP DATABASE %s" % db_name)
      assert not kudu_client.table_exists(kudu_table.name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_client_rpc_timeout_ms=30000")
  def test_drop_db_cascade(self, unique_cursor, kudu_client):
    """Check that an attempt to drop a database cascade will succeed even if Kudu
       tables are present. Make sure the corresponding managed tables are removed
       from Kudu. Increase timeout of individual Kudu client rpcs to avoid requests
       fail due to operation delay in the Hive Metastore for managed tables (IMPALA-8856).
    """
    db_name = unique_cursor.conn.db_name
    with self.temp_kudu_table(kudu_client, [INT32], db_name=db_name) as kudu_table:
      assert kudu_client.table_exists(kudu_table.name)
      unique_cursor.execute("INVALIDATE METADATA")

      # Create a table in HDFS
      hdfs_table_name = self.random_table_name()
      unique_cursor.execute("""
          CREATE TABLE %s (a INT) PARTITIONED BY (x INT)""" % (hdfs_table_name))

      unique_cursor.execute("USE DEFAULT")
      unique_cursor.execute("DROP DATABASE %s CASCADE" % db_name)
      unique_cursor.execute("SHOW DATABASES")
      assert (db_name, '') not in unique_cursor.fetchall()
      assert not kudu_client.table_exists(kudu_table.name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_client_rpc_timeout_ms=30000")
  def test_drop_managed_kudu_table(self, cursor, kudu_client, unique_database):
    """Check that dropping a managed Kudu table should fail if the underlying
       Kudu table has been dropped externally. Increase timeout of individual
       Kudu client rpcs to avoid requests fail due to operation delay in the
       Hive Metastore for managed tables (IMPALA-8856).
    """
    impala_tbl_name = "foo"
    cursor.execute("""CREATE TABLE %s.%s (a INT PRIMARY KEY) PARTITION BY HASH (a)
        PARTITIONS 3 STORED AS KUDU""" % (unique_database, impala_tbl_name))
    kudu_tbl_name = KuduTestSuite.to_kudu_table_name(unique_database, impala_tbl_name)
    assert kudu_client.table_exists(kudu_tbl_name)
    kudu_client.delete_table(kudu_tbl_name)
    assert not kudu_client.table_exists(kudu_tbl_name)

    # Wait for events to prevent race condition
    EventProcessorUtils.wait_for_event_processing(self)

    try:
      cursor.execute("DROP TABLE %s" % kudu_tbl_name)
      assert False
    except Exception as e:
      LOG.info(str(e))
      "Table does not exist: %s" % kudu_tbl_name in str(e)

  @pytest.mark.execute_serially
  def test_drop_external_kudu_table(self, cursor, kudu_client, unique_database):
    """Check that Impala can recover from the case where the underlying Kudu table of
       an external table is dropped using the Kudu client.
    """
    with self.temp_kudu_table(kudu_client, [INT32], db_name=unique_database) \
        as kudu_table:
      # Create an external Kudu table
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      external_table_name = "%s_external" % impala_table_name
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % kudu_table.name
      cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (
        external_table_name, props))
      cursor.execute("DESCRIBE %s" % (external_table_name))
      assert cursor.fetchall() == \
             [("a", "int", "", "true", "true", "false", "", "AUTO_ENCODING",
               "DEFAULT_COMPRESSION", "0")]
      # Drop the underlying Kudu table
      kudu_client.delete_table(kudu_table.name)
      assert not kudu_client.table_exists(kudu_table.name)
      err_msg = 'the table does not exist: table_name: "%s"' % (kudu_table.name)
      try:
        cursor.execute("REFRESH %s" % (external_table_name))
      except Exception as e:
        assert err_msg in str(e)
      cursor.execute("DROP TABLE %s" % (external_table_name))
      cursor.execute("SHOW TABLES")
      assert (external_table_name,) not in cursor.fetchall()

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_alter_table(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_hms_alter', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_create_kudu_table_like(self, vector, unique_database):
    self.run_test_case(
      'QueryTest/kudu_create_table_like_table',
      vector,
      use_db=unique_database)

class TestKuduTransactionBase(CustomClusterTestSuite):
  """
  This is a base class of other TestKuduTransaction classes.
  """

  # query to create Kudu table.
  _create_kudu_table_query = "create table {0} (a int primary key, b string) " \
      "partition by hash(a) partitions 8 stored as kudu"
  # query to create parquet table without key column.
  _create_parquet_table_query = "create table {0} (a int, b string) stored as parquet"
  # queries to insert rows into Kudu table.
  _insert_3_rows_query = "insert into {0} values (0, 'a'), (1, 'b'), (2, 'c')"
  _insert_select_query = "insert into {0} select id, string_col from " \
      "functional.alltypes where id > 2 limit 100"
  _insert_select_query2 = "insert into {0} select * from {1}"
  _insert_dup_key_query = "insert into {0} values (0, 'a'), (0, 'b'), (2, 'c')"
  # CTAS query
  _ctas_query = "create table {0} primary key (a) partition by hash(a) " \
      "partitions 8 stored as kudu as select a, b from {1}"
  # query to drop all rows from Kudu table.
  _delete_query = "delete from {0}"
  # query to update a row in Kudu table.
  _update_query = "update {0} set b='test' where a=1"
  # query to upsert a row in Kudu table.
  _upsert_query = "upsert into {0} values (3, 'hello')"
  # query to get number of rows.
  _row_num_query = "select count(*) from {0}"

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def _test_kudu_txn_succeed(self, cursor, unique_database):
    # Create Kudu table.
    table_name = "%s.test_kudu_txn_succeed" % unique_database
    self.execute_query(self._create_kudu_table_query.format(table_name))

    # Enable Kudu transactions and insert rows to Kudu table.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=true")
    self.execute_query(self._insert_3_rows_query.format(table_name))
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(3,)]
    self.execute_query(self._insert_select_query.format(table_name))
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(103,)]

    # Disable Kudu transactions and delete all rows from Kudu table.
    # Insert rows to the Kudu table. Should get same results as transaction enabled.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=false")
    self.execute_query(self._delete_query.format(table_name))
    self.execute_query(self._insert_3_rows_query.format(table_name))
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(3,)]
    self.execute_query(self._insert_select_query.format(table_name))
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(103,)]

  def _test_kudu_txn_not_implemented(self, cursor, unique_database):
    # Create Kudu table.
    table_name = "%s.test_kudu_txn_succeed" % unique_database
    self.execute_query(self._create_kudu_table_query.format(table_name))

    # Enable Kudu transactions and insert rows to Kudu table.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=true")
    self.execute_query(self._insert_3_rows_query.format(table_name))
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(3,)]

    # Kudu only support multi-row transaction for INSERT now. Impala return an error
    # if UPDATE/UPSERT/DELETE are performed within a transaction context.
    try:
      self.execute_query(self._update_query.format(table_name))
      assert False, "query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert "Query aborted:Kudu reported error: Not implemented" in str(e)

    try:
      self.execute_query(self._upsert_query.format(table_name))
      assert False, "query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert "Query aborted:Kudu reported error: Not implemented" in str(e)

    try:
      self.execute_query(self._delete_query.format(table_name))
      assert False, "query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert "Query aborted:Kudu reported error: Not implemented" in str(e)

    # Verify that number of rows has not been changed.
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(3,)]

  def _test_kudu_txn_abort_dup_key(self, cursor, unique_database,
      expect_fail_on_conflict, expected_error_msg):
    # Create Kudu table.
    table_name = "%s.test_kudu_txn_abort_dup_key" % unique_database
    self.execute_query(self._create_kudu_table_query.format(table_name))

    # Enable Kudu transactions and insert rows with duplicate key values.
    # Transaction should be aborted and no rows are inserted into table.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=true")
    try:
      self.execute_query(self._insert_dup_key_query.format(table_name))
      assert (not expect_fail_on_conflict), "query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert expected_error_msg in str(e)
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(0 if expect_fail_on_conflict else 2,)]

    # Disable Kudu transactions and run the same query. Part of rows are inserted into
    # Kudu table.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=false")
    self.execute_query(self._insert_dup_key_query.format(table_name))
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(2,)]

    # Delete all rows from Kudu table.
    self.execute_query(self._delete_query.format(table_name))
    # Create source Parquet table without primary key and insert duplicate rows.
    table_name2 = "%s.test_kudu_txn_abort_dup_key2" % unique_database
    self.execute_query(self._create_parquet_table_query.format(table_name2))
    self.execute_query(self._insert_dup_key_query.format(table_name2))
    cursor.execute(self._row_num_query.format(table_name2))
    assert cursor.fetchall() == [(3,)]

    # Enable Kudu transactions
    self.execute_query("set ENABLE_KUDU_TRANSACTION=true")
    # Insert rows from source parquet table with duplicate key values.
    # Transaction should be aborted and no rows are inserted into Kudu table.
    try:
      self.execute_query(self._insert_select_query2.format(table_name, table_name2))
      assert (not expect_fail_on_conflict), "query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert expected_error_msg in str(e)
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(0 if expect_fail_on_conflict else 2,)]

    # Disable Kudu transactions and run the same query. Part of rows are inserted into
    # Kudu table.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=false")
    self.execute_query(self._insert_select_query2.format(table_name, table_name2))
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(2,)]

  def _test_kudu_txn_ctas(self, cursor, unique_database, expect_fail_on_conflict,
      expected_error_msg):
    # Enable Kudu transactions
    self.execute_query("set ENABLE_KUDU_TRANSACTION=true")

    # Create source Kudu table and insert 3 rows.
    table_name1 = "%s.test_kudu_txn_ctas1" % unique_database
    self.execute_query(self._create_kudu_table_query.format(table_name1))
    self.execute_query(self._insert_3_rows_query.format(table_name1))
    cursor.execute(self._row_num_query.format(table_name1))
    assert cursor.fetchall() == [(3,)]

    # Run CTAS query without duplicate rows in source table.
    table_name2 = "%s.test_kudu_txn_ctas2" % unique_database
    self.execute_query(self._ctas_query.format(table_name2, table_name1))
    cursor.execute(self._row_num_query.format(table_name2))
    assert cursor.fetchall() == [(3,)]

    # Create source Parquet table without primary key and insert duplicate rows.
    table_name3 = "%s.test_kudu_txn_ctas3" % unique_database
    self.execute_query(self._create_parquet_table_query.format(table_name3))
    self.execute_query(self._insert_dup_key_query.format(table_name3))
    cursor.execute(self._row_num_query.format(table_name3))
    assert cursor.fetchall() == [(3,)]

    # Run CTAS query with duplicate rows in source table.
    # Transaction should be aborted and no rows are inserted into Kudu table.
    table_name4 = "%s.test_kudu_txn_ctas4" % unique_database
    try:
      self.execute_query(self._ctas_query.format(table_name4, table_name3))
      assert (not expect_fail_on_conflict), "query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert expected_error_msg in str(e)
    cursor.execute(self._row_num_query.format(table_name4))
    assert cursor.fetchall() == [(0 if expect_fail_on_conflict else 2,)]

    # Disable Kudu transactions and run the same CTAS query. Part of rows are inserted
    # into Kudu table.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=false")
    table_name5 = "%s.test_kudu_txn_ctas5" % unique_database
    self.execute_query(self._ctas_query.format(table_name5, table_name3))
    cursor.execute(self._row_num_query.format(table_name5))
    assert cursor.fetchall() == [(2,)]

  def _test_kudu_txn_abort_row_batch(self, cursor, unique_database):
    # Create Kudu table.
    table_name = "%s.test_kudu_txn_abort_row_batch" % unique_database
    self.execute_query(self._create_kudu_table_query.format(table_name))

    # Enable Kudu transactions and run "insert" query with injected failure in the end
    # of writing the row batch. Transaction should be aborted and no rows are inserted
    # into table.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=true")
    query_options = {'debug_action': 'FIS_KUDU_TABLE_SINK_WRITE_BATCH:FAIL@1.0'}
    try:
      self.execute_query(self._insert_3_rows_query.format(table_name), query_options)
      assert False, "query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert "FIS_KUDU_TABLE_SINK_WRITE_BATCH" in str(e)
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(0,)]

  def _test_kudu_txn_abort_partial_rows(self, cursor, unique_database):
    # Create Kudu table.
    table_name = "%s.test_kudu_txn_abort_partial_rows" % unique_database
    self.execute_query(self._create_kudu_table_query.format(table_name))

    # Enable Kudu transactions and run "insert" query with injected failure when writing
    # the partial rows. Transaction should be aborted and no rows are inserted into
    # table.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=true")
    query_options = {'debug_action': 'FIS_KUDU_TABLE_SINK_WRITE_PARTIAL_ROW:FAIL@1.0'}
    try:
      self.execute_query(self._insert_3_rows_query.format(table_name), query_options)
      assert False, "query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert "FIS_KUDU_TABLE_SINK_WRITE_PARTIAL_ROW" in str(e)
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(0,)]

  def _test_kudu_txn_abort_partition_lock(self, cursor, unique_database):
    # Running two separate queries that are inserting to the same Kudu partitions.
    # Verify that one of the queries should fail, given Kudu's current implementation
    # of partition locking.

    # Create Kudu table.
    table_name = "%s.test_kudu_txn_abort_partition_lock" % unique_database
    self.execute_query(self._create_kudu_table_query.format(table_name))

    # Enable Kudu transactions and run "insert" query with debug action to skip calling
    # Commit for Kudu transaction so that partition locking is not released. The Kudu
    # transaction object is held by KuduTransactionManager and the transaction is not
    # cleaned up after this test. But the Impala daemon will be restarted after this
    # class of custom cluster test so that the transaction will be cleaned up on Kudu
    # server after Impala daemon is restarted since there is no heart beat for the
    # uncommitted transaction.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=true")
    query_options = {'debug_action': 'CRS_NOT_COMMIT_KUDU_TXN:FAIL'}
    query = "insert into %s values (0, 'a')" % table_name
    self.execute_query(query, query_options)
    # Launch the same query again. The query should fail with error message "aborted
    # since it tries to acquire the partition lock that is held by another transaction".
    try:
      self.execute_query(query)
      assert False, "query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert "aborted since it tries to acquire the partition lock that is held by " \
          "another transaction" in str(e)


class TestKuduTransaction(TestKuduTransactionBase):
  """
  This suite tests the Kudu transaction when inserting rows to kudu table.
  """

  # expected error message from kudu on duplicate key.
  _duplicate_key_error = "Kudu reported write operation errors during transaction."

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_txn_succeed(self, cursor, unique_database):
    self._test_kudu_txn_succeed(cursor, unique_database)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_txn_not_implemented(self, cursor, unique_database):
    self._test_kudu_txn_not_implemented(cursor, unique_database)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_txn_abort_dup_key(self, cursor, unique_database):
    self._test_kudu_txn_abort_dup_key(cursor, unique_database, True,
        self._duplicate_key_error)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_txn_ctas(self, cursor, unique_database):
    self._test_kudu_txn_ctas(cursor, unique_database, True, self._duplicate_key_error)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @SkipIfBuildType.not_dev_build
  def test_kudu_txn_abort_row_batch(self, cursor, unique_database):
    self._test_kudu_txn_abort_row_batch(cursor, unique_database)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @SkipIfBuildType.not_dev_build
  def test_kudu_txn_abort_partial_rows(self, cursor, unique_database):
    self._test_kudu_txn_abort_partial_rows(cursor, unique_database)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @SkipIfBuildType.not_dev_build
  def test_kudu_txn_abort_partition_lock(self, cursor, unique_database):
    self._test_kudu_txn_abort_partial_rows(cursor, unique_database)


class TestKuduTransactionNoIgnore(TestKuduTransactionBase):
  """
  This suite tests the Kudu transaction when inserting rows to kudu table with
  kudu_ignore_conflicts flag set to false.
  """

  # impalad args to start the cluster.
  _impalad_args = "--kudu_ignore_conflicts=false"
  # expected error message from kudu on duplicated key.
  _duplicate_key_error = "Key already present in Kudu table"

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_kudu_txn_succeed(self, cursor, unique_database):
    self._test_kudu_txn_succeed(cursor, unique_database)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_kudu_txn_not_implemented(self, cursor, unique_database):
    self._test_kudu_txn_not_implemented(cursor, unique_database)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_kudu_txn_abort_dup_key(self, cursor, unique_database):
    self._test_kudu_txn_abort_dup_key(cursor, unique_database, True,
        self._duplicate_key_error)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_kudu_txn_ctas(self, cursor, unique_database):
    self._test_kudu_txn_ctas(cursor, unique_database, True, self._duplicate_key_error)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @SkipIfBuildType.not_dev_build
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_kudu_txn_abort_row_batch(self, cursor, unique_database):
    self._test_kudu_txn_abort_row_batch(cursor, unique_database)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @SkipIfBuildType.not_dev_build
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_kudu_txn_abort_partial_rows(self, cursor, unique_database):
    self._test_kudu_txn_abort_partial_rows(cursor, unique_database)

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @SkipIfBuildType.not_dev_build
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_kudu_txn_abort_partition_lock(self, cursor, unique_database):
    self._test_kudu_txn_abort_partial_rows(cursor, unique_database)


class TestKuduTransactionIgnoreConflict(TestKuduTransactionBase):
  """
  This suite tests the Kudu transaction when inserting rows to kudu table with
  kudu_ignore_conflicts=true and kudu_ignore_conflicts_in_transaction=true.
  """

  # impalad args to start the cluster.
  _impalad_args = "--kudu_ignore_conflicts=true " \
      "--kudu_ignore_conflicts_in_transaction=true"

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_kudu_txn_dup_key(self, cursor, unique_database):
    self._test_kudu_txn_abort_dup_key(cursor, unique_database, False, "no error")

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_kudu_txn_ctas(self, cursor, unique_database):
    self._test_kudu_txn_ctas(cursor, unique_database, False, "no error")


@SkipIf.is_test_jdk
class TestKuduTxnKeepalive(CustomKuduTest):
  """
  Tests the Kudu transaction to ensure the transaction handle kept by the front-end in
  KuduTransactionManager keeps the transaction open by heartbeating.
  """
  # query to create Kudu table.
  _create_kudu_table_query = "create table {0} (a int primary key, b string) " \
      "partition by hash(a) partitions 8 stored as kudu"
  # queries to insert rows into Kudu table.
  _insert_3_rows_query = "insert into {0} values (0, 'a'), (1, 'b'), (2, 'c')"
  # query to get number of rows.
  _row_num_query = "select count(*) from {0}"

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    # Restart Kudu cluster with txn_keepalive_interval_ms as 1000 ms.
    KUDU_ARGS = "-txn_keepalive_interval_ms=1000"
    cls._restart_kudu_service(KUDU_ARGS)
    super(TestKuduTxnKeepalive, cls).setup_class()

  @classmethod
  def teardown_class(cls):
    # Restart Kudu cluster with txn_keepalive_interval_ms as default 30000 ms.
    cls._restart_kudu_service("")
    super(TestKuduTxnKeepalive, cls).teardown_class()

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @SkipIfBuildType.not_dev_build
  def test_kudu_txn_heartbeat(self, cursor, unique_database):
    # Create Kudu table.
    table_name = "%s.test_kudu_txn_heartbeat" % unique_database
    self.execute_query(self._create_kudu_table_query.format(table_name))

    # Enable Kudu transactions and run "insert" query with injected sleeping time for
    # 10 seconds before creating session. Transaction should be kept alive and query
    # is finished successfully.
    self.execute_query("set ENABLE_KUDU_TRANSACTION=true")
    query_options = {'debug_action': 'FIS_KUDU_TABLE_SINK_CREATE_SESSION:SLEEP@10000'}
    self.execute_query(self._insert_3_rows_query.format(table_name), query_options)
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(3,)]


class TestKuduDmlConflictBase(CustomClusterTestSuite):
  """
  This is a base class of other TestKuduDml classes.
  """

  # query to create Kudu table.
  _create_kudu_table_query = ("create table {0} "
      "(a int primary key, b timestamp not null) "
      "partition by hash(a) partitions 8 stored as kudu")
  # queries to insert rows into Kudu table.
  _insert_dup_key_query = ("insert into {0} values "
      "(0, '1400-01-01'), (0, '1400-01-02'), "
      "(1, '1400-01-01'), (1, '1400-01-02'), "
      "(2, '1400-01-01'), (2, '1400-01-02')")
  # query to update rows in Kudu table with some constraint violation.
  _update_violate_constraint_query = ("update {0} set b = "
      "case when b <= '2022-01-01' then NULL else '1400-01-01' end")
  # query to update row by primary key.
  _update_by_key_query = "update {0} set b = '1400-02-02' where a = {1}"
  # query to delete row by primary key.
  _delete_by_key_query = "delete from {0} where a = {1}"
  # query to drop all rows from Kudu table.
  _delete_all_query = "delete from {0}"
  # query to get number of rows.
  _row_num_query = "select count(*) from {0}"

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def _check_errors(self, query_profile, expect_error, error_message, num_row_erros):
    """
    Check ocurrence of error_message and num_row_errors in query_profile.
    """

    error_line = "  Errors: {0}".format(error_message)
    num_row_error_line = "NumRowErrors: {0}".format(num_row_erros)
    assert expect_error == (error_line in query_profile)
    assert num_row_error_line in query_profile

  def _race_queries(self, fast_query, slow_query, expect_error_on_slow_query,
      error_message, num_row_erros):
    """
    Race two queries and check for error message in the slow query's profile.
    """

    fast_sleep = {'debug_action': 'FIS_KUDU_TABLE_SINK_WRITE_BEGIN:SLEEP@1000'}
    slow_sleep = {'debug_action': 'FIS_KUDU_TABLE_SINK_WRITE_BEGIN:SLEEP@3000'}
    timeout = 10

    fast_handle = self.execute_query_async(fast_query, fast_sleep)
    slow_handle = self.execute_query_async(slow_query, slow_sleep)
    try:
      # Wait for both queries to finish.
      self.wait_for_state(fast_handle, self.client.QUERY_STATES['FINISHED'], timeout)
      self.wait_for_state(slow_handle, self.client.QUERY_STATES['FINISHED'], timeout)
      self._check_errors(self.client.get_runtime_profile(slow_handle),
          expect_error_on_slow_query, error_message, num_row_erros)
    finally:
      self.client.close_query(fast_handle)
      self.client.close_query(slow_handle)

  def _test_insert_update_delete(self, cursor, unique_database,
      expect_log_on_conflict):
    """
    Do sequence of insert, update, and delete query with conflicting primary keys.
    """

    # Create Kudu table.
    table_name = "%s.insert_update_delete" % unique_database
    self.execute_query(self._create_kudu_table_query.format(table_name))

    # Insert rows with duplicate primary key.
    # Error message should exist in profile if kudu_ignore_conflicts=true.
    result = self.execute_query(self._insert_dup_key_query.format(table_name))
    self._check_errors(result.runtime_profile, expect_log_on_conflict,
        "Key already present in Kudu table", 3)
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(3,)]

    # Update rows with some constraint violation.
    # Error message should exist in profile regardless of kudu_ignore_conflicts value.
    result = self.execute_query(self._update_violate_constraint_query.format(table_name))
    self._check_errors(result.runtime_profile, True,
        "Row with null value violates nullability constraint on table", 3)

    # Update row with non-existent primary key by racing it against concurrent delete
    # query.
    delete_query = self._delete_by_key_query.format(table_name, 1)
    update_query = self._update_by_key_query.format(table_name, 1)
    self._race_queries(delete_query, update_query, expect_log_on_conflict,
        "Not found in Kudu table", 1)
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(2,)]

    # Delete row with non-existent primary key by racing it against another concurrent
    # delete.
    delete_query = self._delete_by_key_query.format(table_name, 2)
    self._race_queries(delete_query, delete_query, expect_log_on_conflict,
        "Not found in Kudu table", 1)
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(1,)]

    # Delete all rows. Expect no errors.
    result = self.execute_query(self._delete_all_query.format(table_name))
    self._check_errors(result.runtime_profile, True, "\n", 0)
    cursor.execute(self._row_num_query.format(table_name))
    assert cursor.fetchall() == [(0,)]


class TestKuduDmlConflictNoError(TestKuduDmlConflictBase):
  """
  Test that Kudu DML ignore conflict and does not log the conflict error message.
  """

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  def test_insert_update_delete(self, cursor, unique_database):
    self._test_insert_update_delete(cursor, unique_database, False)


class TestKuduDmlConflictLogError(TestKuduDmlConflictBase):
  """
  Test that Kudu DML not ignore conflict and log the conflict error message.
  """

  # impalad args to start the cluster.
  _impalad_args = "--kudu_ignore_conflicts=false"

  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @CustomClusterTestSuite.with_args(impalad_args=_impalad_args)
  def test_insert_update_delete(self, cursor, unique_database):
    self._test_insert_update_delete(cursor, unique_database, True)
