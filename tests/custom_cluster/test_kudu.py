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

import logging
import os
import pytest
from kudu.schema import INT32

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.kudu_test_suite import KuduTestSuite
from tests.common.skip import SkipIfKudu
from tests.common.test_dimensions import add_exec_option_dimension

KUDU_MASTER_HOSTS = pytest.config.option.kudu_master_hosts
LOG = logging.getLogger(__name__)

class TestKuduOperations(CustomClusterTestSuite, KuduTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduOperations, cls).add_test_dimensions()
    # The default read mode of READ_LATEST does not provide high enough consistency for
    # these tests.
    add_exec_option_dimension(cls, "kudu_read_mode", "READ_AT_SNAPSHOT")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=\
      "--use_local_tz_for_unix_timestamp_conversions=true")
  @SkipIfKudu.no_hybrid_clock
  @SkipIfKudu.hms_integration_enabled
  def test_local_tz_conversion_ops(self, vector, unique_database):
    """IMPALA-5539: Test Kudu timestamp reads/writes are correct with the
       use_local_tz_for_unix_timestamp_conversions flag."""
    # These tests provide enough coverage of queries with timestamps.
    self.run_test_case('QueryTest/kudu-scan-node', vector, use_db=unique_database)
    self.run_test_case('QueryTest/kudu_insert', vector, use_db=unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_master_hosts=")
  @SkipIfKudu.hms_integration_enabled
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
  @SkipIfKudu.hms_integration_enabled
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

class TestKuduClientTimeout(CustomClusterTestSuite, KuduTestSuite):
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
  @SkipIfKudu.hms_integration_enabled
  def test_impalad_timeout(self, vector):
    """Check impalad behavior when -kudu_operation_timeout_ms is too low."""
    self.run_test_case('QueryTest/kudu-timeouts-impalad', vector)


class TestKuduHMSIntegration(CustomClusterTestSuite, KuduTestSuite):
  # TODO(IMPALA-8614): parameterize the common tests in query_test/test_kudu.py
  # to run with HMS integration enabled.
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
  @SkipIfKudu.no_hybrid_clock
  def test_create_managed_kudu_tables(self, vector, unique_database):
    """Tests the Create table operation when using a kudu table with Kudu's integration
       with the Hive Metastore for managed tables."""
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
        assert ["", "storage_handler", "org.apache.kudu.hive.KuduStorageHandler"] \
            in table_desc

  @pytest.mark.execute_serially
  def test_implicit_managed_table_props(self, cursor, kudu_client, unique_database):
    """Check that table properties added internally for managed table during table
       creation are as expected.
    """
    cursor.execute("""CREATE TABLE %s.foo (a INT PRIMARY KEY, s STRING)
        PARTITION BY HASH(a) PARTITIONS 3 STORED AS KUDU""" % unique_database)
    assert kudu_client.table_exists(
      KuduTestSuite.to_kudu_table_name(unique_database, "foo"))
    cursor.execute("DESCRIBE FORMATTED %s.foo" % unique_database)
    table_desc = [[col.strip() if col else col for col in row] for row in cursor]
    # Pytest shows truncated output on failure, so print the details just in case.
    LOG.info(table_desc)
    assert not any("EXTERNAL" in s for s in table_desc)
    assert any("Owner:" in s for s in table_desc)
    assert any("kudu.table_id" in s for s in table_desc)
    assert any("kudu.master_addresses" in s for s in table_desc)
    assert ["Table Type:", "MANAGED_TABLE", None] in table_desc
    assert ["", "kudu.table_name", "%s.foo" % unique_database] in table_desc
    assert ["", "storage_handler", "org.apache.kudu.hive.KuduStorageHandler"] \
        in table_desc

  @pytest.mark.execute_serially
  def test_drop_non_empty_db(self, unique_cursor, kudu_client):
    """Check that an attempt to drop a database will fail if Kudu tables are present
       and that the tables remain.
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
  def test_drop_db_cascade(self, unique_cursor, kudu_client):
    """Check that an attempt to drop a database cascade will succeed even if Kudu
       tables are present. Make sure the corresponding managed tables are removed
       from Kudu.
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
  def test_drop_managed_kudu_table(self, cursor, kudu_client, unique_database):
    """Check that dropping a managed Kudu table should fail if the underlying
       Kudu table has been dropped externally.
    """
    impala_tbl_name = "foo"
    cursor.execute("""CREATE TABLE %s.%s (a INT PRIMARY KEY) PARTITION BY HASH (a)
        PARTITIONS 3 STORED AS KUDU""" % (unique_database, impala_tbl_name))
    kudu_tbl_name = KuduTestSuite.to_kudu_table_name(unique_database, impala_tbl_name)
    assert kudu_client.table_exists(kudu_tbl_name)
    kudu_client.delete_table(kudu_tbl_name)
    assert not kudu_client.table_exists(kudu_tbl_name)
    try:
      cursor.execute("DROP TABLE %s" % kudu_tbl_name)
      assert False
    except Exception as e:
      LOG.info(str(e))
      assert "Table %s no longer exists in the Hive MetaStore." % kudu_tbl_name in str(e)

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
             [("a", "int", "", "true", "false", "", "AUTO_ENCODING",
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
