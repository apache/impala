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

import os
import re

from subprocess import CalledProcessError
from logging import getLogger

from SystemTables.ttypes import TQueryTableColumn
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.workload_management import assert_query

LOG = getLogger(__name__)


class TestWorkloadManagementInitBase(CustomClusterTestSuite):

  """Defines common setup and methods for all workload management init tests."""

  WM_DB = "sys"
  QUERY_TBL_LOG_NAME = "impala_query_log"
  QUERY_TBL_LOG = "{0}.{1}".format(WM_DB, QUERY_TBL_LOG_NAME)
  QUERY_TBL_LIVE_NAME = "impala_query_live"
  QUERY_TBL_LIVE = "{0}.{1}".format(WM_DB, QUERY_TBL_LIVE_NAME)

  LATEST_SCHEMA = "1.2.0"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestWorkloadManagementInitBase, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('protocol') == 'beeswax')

  def setup_method(self, method):
    super(TestWorkloadManagementInitBase, self).setup_method(method)

  def restart_cluster(self, vector, schema_version="", wait_for_init_complete=True,
      cluster_size=3, additional_impalad_opts="", wait_for_backends=True,
      additional_catalogd_opts="", expect_startup_err=False, log_symlinks=False):
    """Wraps the existing custom cluster _start_impala_cluster function to restart the
       Impala cluster. Specifies coordinator/catalog startup flags to enable workload
       management and set the schema version. If wait_for_init_complete is True, this
       function blocks until the workload management init process completes. If
       additional_impalad_opts is specified, that string is appended to the impala_args
       startup flag."""
    coord_opts = "--impalad_args=--enable_workload_mgmt --logbuflevel=-1 "
    coord_opts += additional_impalad_opts

    catalog_opts = "--catalogd_args=--enable_workload_mgmt --logbuflevel=-1 "
    catalog_opts += additional_catalogd_opts

    if schema_version:
      coord_opts += " --workload_mgmt_schema_version={} ".format(schema_version)
      catalog_opts += "--workload_mgmt_schema_version={} ".format(schema_version)

    try:
      self.close_impala_clients()
      num_coords = cluster_size
      if cluster_size > 1:
        num_coords = cluster_size - 1

      self._start_impala_cluster(options=[coord_opts, catalog_opts],
          cluster_size=cluster_size, expected_num_impalads=cluster_size,
          num_coordinators=num_coords, wait_for_backends=wait_for_backends,
          log_symlinks=log_symlinks)
      self.create_impala_clients()
    except CalledProcessError as e:
      if not expect_startup_err:
        raise e

    if wait_for_init_complete:
      self.wait_for_wm_init_complete()

  def assert_table_prop(self, tbl_name, expected_key, expected_val="", should_exist=True):
    """Asserts database table properties. If expected_val is specified, asserts the table
       has a property on it with the specified key/value. If should_exist is False,
       asserts the specified table does not contain a property with the specified key."""
    assert expected_val == "" or should_exist, "Cannot specify both the expected_val " \
      "and should_exist properties."

    res = self.client.execute("show create table {}".format(tbl_name))
    assert res.success

    if should_exist:
      found = False
      for line in res.data:
        if re.search(r"TBLPROPERTIES.*?'{}'='{}'".format(expected_key, expected_val),
            line):
          found = True
          break

      assert found, "did not find expected table prop '{}' with value '{}' on table " \
          "'{}'".format(expected_key, expected_val, tbl_name)
    else:
      for line in res.data:
        if re.search(r"TBLPROPERTIES.*?'{}'".format(expected_key), line):
          assert False, "found table pop '{}' on table '{}' but this property should " \
              "not exist"

  def assert_catalogd_all_tables(self, line_regex, level="INFO"):
    """Asserts a given regex is found in the catalog log file for each workload management
      table. The regex is passed the fully qualified table name using python string
      substitution."""
    for table in (self.QUERY_TBL_LOG, self.QUERY_TBL_LIVE):
      self.assert_catalogd_log_contains("INFO", line_regex.format(table))

  def check_schema(self, schema_ver, vector, multiple_impalad=False):
    """Asserts that all workload management tables have the correct columns and are at the
       specified schema version."""
    for tbl_name in (self.QUERY_TBL_LOG_NAME, self.QUERY_TBL_LIVE_NAME):
      self.run_test_case('QueryTest/workload-mgmt-{}-v{}'.format(tbl_name, schema_ver),
          vector, self.WM_DB, multiple_impalad=multiple_impalad)


class TestWorkloadManagementInitWait(TestWorkloadManagementInitBase):

  """Tests for the workload management initialization process. The setup method of this
     class waits for the workload management init process to complete before allowing any
     tests to run. """

  def setup_method(self, method):
    super(TestWorkloadManagementInitWait, self).setup_method(method)
    self.wait_for_wm_init_complete()

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt --workload_mgmt_schema_version=1.1.0",
      disable_log_buffering=True)
  def test_no_upgrade(self, vector):
    """Tests that no upgrade happens when starting a cluster where the workload management
       tables are already at the latest version."""
    self.restart_cluster(vector, schema_version=self.LATEST_SCHEMA, log_symlinks=True)
    self.check_schema(self.LATEST_SCHEMA, vector)

    self.assert_catalogd_log_contains("INFO", r"Workload management table .*? will be "
        r"upgraded", expected_count=0)

  @CustomClusterTestSuite.with_args(cluster_size=10, disable_log_buffering=True,
      log_symlinks=True,
      impalad_args="--enable_workload_mgmt --workload_mgmt_schema_version=1.0.0",
      catalogd_args="--enable_workload_mgmt "
                    "--workload_mgmt_schema_version=1.0.0 "
                    "--workload_mgmt_drop_tables=impala_query_log,impala_query_live")
  def test_create_on_version_1_0_0(self, vector):
    """Asserts that workload management tables are properly created on version 1.0.0 using
       a 10 node cluster when no tables exist."""
    self.check_schema("1.0.0", vector, multiple_impalad=True)

  @CustomClusterTestSuite.with_args(cluster_size=10, disable_log_buffering=True,
      log_symlinks=True,
      impalad_args="--enable_workload_mgmt --workload_mgmt_schema_version=1.1.0",
      catalogd_args="--enable_workload_mgmt "
                    "--workload_mgmt_schema_version=1.1.0 "
                    "--workload_mgmt_drop_tables=impala_query_log,impala_query_live")
  def test_create_on_version_1_1_0(self, vector):
    """Asserts that workload management tables are properly created on version 1.1.0 using
       a 10 node cluster when no tables exist."""
    self.check_schema("1.1.0", vector, multiple_impalad=True)

  @CustomClusterTestSuite.with_args(cluster_size=10, disable_log_buffering=True,
      log_symlinks=True,
      impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt "
                    "--workload_mgmt_drop_tables=impala_query_log,impala_query_live")
  def test_create_on_version_1_2_0(self, vector):
    """Asserts that workload management tables are properly created on the latest version
       using a 10 node cluster when no tables exist."""
    self.check_schema("1.2.0", vector, multiple_impalad=True)

  @CustomClusterTestSuite.with_args(cluster_size=1,
      impalad_args="--enable_workload_mgmt --workload_mgmt_schema_version=1.0.0",
      catalogd_args="--enable_workload_mgmt "
                    "--workload_mgmt_schema_version=1.0.0 "
                    "--workload_mgmt_drop_tables=impala_query_log,impala_query_live",
      disable_log_buffering=True)
  def test_upgrade_1_0_0_to_1_1_0(self, vector):
    """Asserts that an upgrade from version 1.0.0 to 1.1.0 succeeds when starting with no
       existing workload management tables."""

    # Verify the initial table create on version 1.0.0 succeeded.
    self.check_schema("1.0.0", vector)
    self.assert_log_contains("catalogd", "WARNING", r"Target schema version '1.0.0' is "
        r"not the latest schema version '\d+\.\d+\.\d+'")

    self.restart_cluster(vector, schema_version="1.1.0", cluster_size=1,
        log_symlinks=True)

    # Assert the upgrade process ran.
    self.assert_catalogd_all_tables(r"Workload management table '{}' is at version "
        r"'1.0.0' and will be upgraded")

    self.check_schema("1.1.0", vector)

  @CustomClusterTestSuite.with_args(cluster_size=1,
      impalad_args="--enable_workload_mgmt --workload_mgmt_schema_version=1.1.0",
      catalogd_args="--enable_workload_mgmt "
                    "--workload_mgmt_schema_version=1.1.0 "
                    "--workload_mgmt_drop_tables=impala_query_log,impala_query_live",
      disable_log_buffering=True)
  def test_upgrade_1_1_0_to_1_2_0(self, vector):
    """Asserts that an upgrade from version 1.1.0 to 1.2.0 succeeds when starting with no
       existing workload management tables."""

    # Verify the initial table create on version 1.0.0 succeeded.
    self.check_schema("1.1.0", vector)
    self.assert_log_contains("catalogd", "WARNING", r"Target schema version '1.1.0' is "
        r"not the latest schema version '\d+\.\d+\.\d+'")

    self.restart_cluster(vector, schema_version="1.2.0", cluster_size=1,
        log_symlinks=True)

    # Assert the upgrade process ran.
    self.assert_catalogd_all_tables(r"Workload management table '{}' is at version "
        r"'1.1.0' and will be upgraded")

    self.check_schema("1.2.0", vector)

  @CustomClusterTestSuite.with_args(cluster_size=1,
      impalad_args="--enable_workload_mgmt --workload_mgmt_schema_version=1.0.0",
      catalogd_args="--enable_workload_mgmt "
                    "--workload_mgmt_schema_version=1.0.0 "
                    "--workload_mgmt_drop_tables=impala_query_log,impala_query_live",
      disable_log_buffering=True)
  def test_upgrade_1_0_0_to_1_2_0(self, vector):
    """Asserts that an upgrade from version 1.0.0 to 1.2.0 succeeds when starting with no
       existing workload management tables."""

    # Verify the initial table create on version 1.0.0 succeeded.
    self.check_schema("1.0.0", vector)
    self.assert_log_contains("catalogd", "WARNING", r"Target schema version '1.0.0' is "
        r"not the latest schema version '\d+\.\d+\.\d+'")

    self.restart_cluster(vector, schema_version="1.2.0", cluster_size=1,
        log_symlinks=True)

    # Assert the upgrade process ran.
    self.assert_catalogd_all_tables(r"Workload management table '{}' is at version "
        r"'1.0.0' and will be upgraded")

    self.check_schema("1.2.0", vector)

  @CustomClusterTestSuite.with_args(cluster_size=1, impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt", disable_log_buffering=True)
  def test_log_table_newer_schema_version(self, vector):
    """Asserts a catalog startup flag version that is older than the workload
       management table schema version will write only the fields associated with the
       startup flag version."""
    self.restart_cluster(vector, schema_version="1.0.0", cluster_size=1,
        log_symlinks=True, additional_impalad_opts="--query_log_write_interval_s=15")

    self.assert_catalogd_log_contains("WARNING", "Target schema version '1.0.0' is not "
        "the latest schema version '{}'".format(self.LATEST_SCHEMA))

    # The workload management tables will be on the latest schema version.
    self.check_schema(self.LATEST_SCHEMA, vector)

    # The workload management processing will be running on schema version 1.0.0.
    self.assert_catalogd_all_tables(r"Target schema version '1.0.0' of the '{}' table is "
        r"lower than the actual schema version")

    # Run a query and ensure it does not populate fields other than version 1.0.0 fields.
    res = self.client.execute("select * from functional.alltypes")
    assert res.success

    impalad = self.cluster.get_first_impalad()

    # Check the live queries table first.
    assert_query(self.QUERY_TBL_LIVE, self.client, impalad=impalad, query_id=res.query_id,
        expected_overrides={
          TQueryTableColumn.SELECT_COLUMNS: "",
          TQueryTableColumn.WHERE_COLUMNS: "",
          TQueryTableColumn.JOIN_COLUMNS: "",
          TQueryTableColumn.AGGREGATE_COLUMNS: "",
          TQueryTableColumn.ORDERBY_COLUMNS: "",
          TQueryTableColumn.COORDINATOR_SLOTS: "0",
          TQueryTableColumn.EXECUTOR_SLOTS: "0"})

    # Check the query log table.
    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.written", 2, 60)
    assert_query(self.QUERY_TBL_LOG, self.client, impalad=impalad, query_id=res.query_id,
        expected_overrides={
          TQueryTableColumn.SELECT_COLUMNS: "NULL",
          TQueryTableColumn.WHERE_COLUMNS: "NULL",
          TQueryTableColumn.JOIN_COLUMNS: "NULL",
          TQueryTableColumn.AGGREGATE_COLUMNS: "NULL",
          TQueryTableColumn.ORDERBY_COLUMNS: "NULL",
          TQueryTableColumn.COORDINATOR_SLOTS: "NULL",
          TQueryTableColumn.EXECUTOR_SLOTS: "NULL"})

  @CustomClusterTestSuite.with_args(cluster_size=1, disable_log_buffering=True,
      log_symlinks=True,
      impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt "
                    "--query_log_table_props=\"foo=bar,foo1=bar1\" "
                    "--workload_mgmt_drop_tables=impala_query_log,impala_query_live")
  def test_create_table_with_custom_props(self):
    """Asserts that creating workload management tables with additional properties
       specified adds those properties."""

    self.assert_table_prop(self.QUERY_TBL_LOG, "foo", "bar")
    self.assert_table_prop(self.QUERY_TBL_LIVE, "foo", "bar")

  @CustomClusterTestSuite.with_args(cluster_size=1, disable_log_buffering=True,
      log_symlinks=True,
      impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt "
                    "--workload_mgmt_drop_tables=impala_query_log,impala_query_live")
  def test_create_from_scratch(self, vector):
    """Tests the conditions that exist when workload management is first started by
       deleteing the workload management tables and the sys db and restarting."""
    assert self.client.execute("drop database {} cascade"
        .format(self.WM_DB)).success

    self.restart_cluster(vector, log_symlinks=True)
    self.check_schema(self.LATEST_SCHEMA, vector)

  def _run_invalid_table_prop_test(self, table, prop_name, vector, expect_success=False):
    """Runs a test where one of the workload management schema version table properties on
       a workload management table has been reset to an invalid value."""
    try:
      res = self.client.execute(
          "alter table {} set tblproperties('{}'='')".format(table, prop_name))
      assert res.success
      self.assert_catalogd_log_contains("INFO", "Finished execDdl request: ALTER_TABLE "
          "{}".format(table))

      tmp_dir = self.get_tmp_dir('invalid_schema')
      self.restart_cluster(vector, wait_for_init_complete=False, cluster_size=1,
          wait_for_backends=False, expect_startup_err=True, log_symlinks=True,
          additional_catalogd_opts="--minidump_path={}".format(tmp_dir),
          additional_impalad_opts="--minidump_path={}".format(tmp_dir))

      if not expect_success:
        self.wait_for_log_exists("catalogd", "FATAL", 30)
        self.assert_catalogd_log_contains("FATAL", "could not parse version string '' "
            "found on the '{}' property of table '{}'".format(prop_name, table),
            timeout_s=60)
      else:
        self.wait_for_wm_init_complete()
        assert len(os.listdir("{}/catalogd".format(tmp_dir))) == 0, \
            "Found minidumps but none should exist."
    finally:
      self.restart_cluster(vector, cluster_size=1,
          additional_catalogd_opts="--workload_mgmt_drop_tables=impala_query_log,"
          "impala_query_live")

  @CustomClusterTestSuite.with_args(cluster_size=1, log_symlinks=True,
      impalad_args="--enable_workload_mgmt --minidump_path={invalid_schema}",
      catalogd_args="--enable_workload_mgmt --minidump_path={invalid_schema}",
      tmp_dir_placeholders=['invalid_schema'],
      disable_log_buffering=True)
  def test_invalid_schema_version_log_table_prop(self, vector):
    """Tests that startup succeeds when the 'schema_version' table property on the
       sys.impala_query_log table contains an invalid value but the wm_schema_version
       table property contains a valid value."""
    self._run_invalid_table_prop_test(self.QUERY_TBL_LOG, "schema_version", vector, True)

  @CustomClusterTestSuite.with_args(cluster_size=1, log_symlinks=True,
      impalad_args="--enable_workload_mgmt --minidump_path={invalid_schema}",
      catalogd_args="--enable_workload_mgmt --minidump_path={invalid_schema}",
      tmp_dir_placeholders=['invalid_schema'],
      disable_log_buffering=True)
  def test_invalid_wm_schema_version_log_table_prop(self, vector):
    """Tests that startup fails when the 'wm_schema_version' table property on the
       sys.impala_query_log table contains an invalid value."""
    self._run_invalid_table_prop_test(self.QUERY_TBL_LOG, "wm_schema_version", vector)

  @CustomClusterTestSuite.with_args(cluster_size=1, log_symlinks=True,
      impalad_args="--enable_workload_mgmt --minidump_path={invalid_schema}",
      catalogd_args="--enable_workload_mgmt --minidump_path={invalid_schema}",
      tmp_dir_placeholders=['invalid_schema'],
      disable_log_buffering=True)
  def test_invalid_schema_version_live_table_prop(self, vector):
    """Tests that startup succeeds when the 'schema_version' table property on the
       sys.impala_query_live table contains an invalid value but the wm_schema_version
       table property contains a valid value."""
    self._run_invalid_table_prop_test(self.QUERY_TBL_LIVE, "schema_version", vector, True)

  @CustomClusterTestSuite.with_args(cluster_size=1, log_symlinks=True,
      impalad_args="--enable_workload_mgmt --minidump_path={invalid_schema}",
      catalogd_args="--enable_workload_mgmt --minidump_path={invalid_schema}",
      tmp_dir_placeholders=['invalid_schema'],
      disable_log_buffering=True)
  def test_invalid_wm_schema_version_live_table_prop(self, vector):
    """Tests that startup fails when the 'wm_schema_version' table property on the
       sys.impala_query_live table contains an invalid value."""
    self._run_invalid_table_prop_test(self.QUERY_TBL_LIVE, "wm_schema_version", vector)

  @CustomClusterTestSuite.with_args(cluster_size=1, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt")
  def test_upgrade_to_latest_from_previous_binary(self, vector):
    """Simulated an upgrade situation from workload management tables created by previous
       builds of Impala."""

    for table in (self.QUERY_TBL_LOG, self.QUERY_TBL_LIVE):
      assert self.client.execute("drop table if exists {} purge".format(table)).success

    for table in (self.QUERY_TBL_LOG_NAME, self.QUERY_TBL_LIVE_NAME):
      with open("{}/testdata/workload_mgmt/create_{}_table.sql"
          .format(os.environ["IMPALA_HOME"], table), "r") as f:
        create_sql = f.read()
        assert self.client.execute(create_sql).success

    self.restart_cluster(vector, cluster_size=1, log_symlinks=True,
        additional_impalad_opts="--query_log_write_interval_s=30")
    self.check_schema(self.LATEST_SCHEMA, vector)

    # Run a query and ensure it does not populate fields from the latest schema.
    res = self.client.execute("select * from functional.alltypes")
    assert res.success

    impalad = self.cluster.get_first_impalad()

    # Check the live queries table first.
    assert_query(self.QUERY_TBL_LIVE, self.client, impalad=impalad, query_id=res.query_id)

    # Check the query log table.
    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.written", 2, 60)
    assert_query(self.QUERY_TBL_LOG, self.client, impalad=impalad, query_id=res.query_id)

  @CustomClusterTestSuite.with_args(cluster_size=1, disable_log_buffering=True,
      impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt")
  def test_start_at_1_0_0(self, vector):
    """Tests the situation where workload management tables were created by the original
       workload management code, and the current code is started at workload management
       schema version 1.0.0 (even though that version is not the latest)."""

    for table in (self.QUERY_TBL_LOG, self.QUERY_TBL_LIVE):
      assert self.client.execute("drop table if exists {} purge".format(table)).success

    for table in (self.QUERY_TBL_LOG_NAME, self.QUERY_TBL_LIVE_NAME):
      with open("{}/testdata/workload_mgmt/create_{}_table.sql"
          .format(os.environ["IMPALA_HOME"], table), "r") as f:
        create_sql = f.read()
        assert self.client.execute(create_sql).success

    self.restart_cluster(vector, schema_version="1.0.0", log_symlinks=True,
        additional_impalad_opts="--query_log_write_interval_s=15")

    for table in (self.QUERY_TBL_LOG, self.QUERY_TBL_LIVE):
      self.assert_table_prop(table, "schema_version", "1.0.0")
      self.assert_table_prop(table, "wm_schema_version", should_exist=False)

    # Run a query and ensure it does not populate version 1.1.0 fields.
    res = self.client.execute("select * from functional.alltypes")
    assert res.success

    # Check the live queries table first.
    live_results = self.client.execute("select * from {} where query_id='{}'".format(
        self.QUERY_TBL_LIVE, res.query_id))
    assert live_results.success
    assert len(live_results.data) == 1, "did not find query in '{}' table '{}'".format(
        res.query_id, self.QUERY_TBL_LIVE)
    assert len(live_results.column_labels) == 49
    data = live_results.data[0].split("\t")
    assert len(data) == len(live_results.column_labels)

    # Check the query log table.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 2, 60)
    log_results = self.client.execute("select * from {} where query_id='{}'".format(
        self.QUERY_TBL_LOG, res.query_id))
    assert log_results.success
    assert len(log_results.data) == 1, "did not find query in '{}' table '{}'".format(
        res.query_id, self.QUERY_TBL_LOG)
    assert len(log_results.column_labels) == 49
    data = log_results.data[0].split("\t")
    assert len(data) == len(log_results.column_labels)

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt",
      statestored_args="--use_subscriber_id_as_catalogd_priority=true",
      start_args="--enable_statestored_ha",
      disable_log_buffering=True, log_symlinks=True)
  def test_statestore_ha(self):
    """Asserts workload management initialization completes successfully when statestore
       ha is enabled."""

    # Assert catalogd ran workload management initialization.
    self.assert_catalogd_log_contains("INFO",
        r"Completed workload management initialization")


class TestWorkloadManagementInitNoWait(TestWorkloadManagementInitBase):

  """Tests for the workload management initialization process. The setup method of this
     class does not wait for the workload management init process to complete. Instead, it
     returns as soon as the Impala cluster is live."""

  def setup_method(self, method):
    super(TestWorkloadManagementInitNoWait, self).setup_method(method)

  @CustomClusterTestSuite.with_args(cluster_size=1, log_symlinks=True,
      impalad_args="--enable_workload_mgmt --query_log_write_interval_s=3",
      catalogd_args="--enable_workload_mgmt "
                    "--workload_mgmt_drop_tables=impala_query_log,impala_query_live "
                    "--debug_actions=CATALOG_WORKLOADMGMT_STARTUP:SLEEP@15000",
      disable_log_buffering=True)
  def test_catalog_init_delay(self):
    # Workload management init is slightly delayed after catalogd startup, wait for the
    # debug action to begin before continuing since that log message guarantees the
    # workload management tables have been deleted.
    self.assert_catalogd_log_contains("INFO",
        "Debug Action: CATALOG_WORKLOADMGMT_STARTUP:SLEEP", timeout_s=30)
    res = self.client.execute("select * from functional.alltypes")
    assert res.success

    # Wait for three failed attempts to write the completed query to the query log table.
    impalad = self.cluster.get_first_impalad().service
    impalad.wait_for_metric_value("impala-server.completed-queries.failure", 3, 15)
    impalad.wait_for_metric_value("impala-server.completed-queries.queued", 0, 5)

    # Wait for workload management to fully initialize before trying another query.
    self.wait_for_wm_init_complete()

    # Try another query which should now successfully be written to the query log table.
    res = self.client.execute("select * from functional.alltypes")
    assert res.success
    impalad.wait_for_metric_value("impala-server.completed-queries.written", 1, 15)

  @CustomClusterTestSuite.with_args(cluster_size=1, expect_startup_fail=True,
      impalad_timeout_s=60, log_symlinks=True,
      impalad_args="--enable_workload_mgmt --workload_mgmt_schema_version=foo "
                   "--minidump_path={minidumps}",
      catalogd_args="--enable_workload_mgmt --workload_mgmt_schema_version=foo "
                    "--minidump_path={minidumps}", tmp_dir_placeholders=['minidumps'],
      disable_log_buffering=True)
  def test_start_invalid_version(self):
    """Asserts that starting a cluster with an invalid workload management version
       errors. Cluster sizes of 1 are used to speed up the initial setup."""
    self.wait_for_log_exists("impalad", "FATAL")
    self.assert_impalad_log_contains("FATAL", r"Invalid workload management schema "
        r"version 'foo'")

    self.wait_for_log_exists("catalogd", "FATAL")
    self.assert_catalogd_log_contains("FATAL", r"Invalid workload management schema "
        r"version 'foo'")

  @CustomClusterTestSuite.with_args(cluster_size=1, expect_startup_fail=True,
      impalad_timeout_s=60, log_symlinks=True,
      impalad_args="--enable_workload_mgmt --workload_mgmt_schema_version=0.0.1 "
                   "--minidump_path={minidumps}",
      catalogd_args="--enable_workload_mgmt --workload_mgmt_schema_version=0.0.1 "
                    "--minidump_path={minidumps}", tmp_dir_placeholders=['minidumps'],
      disable_log_buffering=True)
  def test_start_unknown_version(self):
    """Asserts that starting a cluster with an unknown workload management version errors.
       Cluster sizes of 1 are used to speed up the initial setup."""
    self.wait_for_log_exists("impalad", "FATAL")
    self.assert_impalad_log_contains("FATAL", r"Workload management schema version "
        r"'0.0.1' is not one of the known versions: '1.0.0', '1.1.0', '1.2.0'$")

    self.wait_for_log_exists("catalogd", "FATAL")
    self.assert_catalogd_log_contains("FATAL", r"Workload management schema version "
        r"'0.0.1' is not one of the known versions: '1.0.0', '1.1.0', '1.2.0'$")

  @CustomClusterTestSuite.with_args(start_args="--enable_catalogd_ha",
      statestored_args="--use_subscriber_id_as_catalogd_priority=true",
      disable_log_buffering=True, log_symlinks=True)
  def test_catalog_ha_no_workload_mgmt(self):
    """Asserts workload management initialization is not done on either catalogd when
       workload management is not enabled."""

    # Assert the active catalog skipped workload management initialization.
    self.assert_catalogd_log_contains("INFO", r"workload management initialization",
        expected_count=0)

    # Assert the standby catalog skipped workload management initialization.
    self.assert_catalogd_log_contains("INFO", r"workload management initialization",
        expected_count=0, node_index=1)


class TestWorkloadManagementCatalogHA(TestWorkloadManagementInitBase):

  """Tests for the workload management initialization process. The setup method of this
     class ensures only 1 catalogd ran the workload management initialization process."""

  def setup_method(self, method):
    super(TestWorkloadManagementCatalogHA, self).setup_method(method)

    # Find all catalog instances that have initialized workload management.
    init_logs = self.assert_catalogd_ha_contains("INFO",
        r"Completed workload management initialization", timeout_s=30)
    assert len(init_logs) == 2, "Expected length of catalogd matches to be '2' but " \
        "was '{}'".format(len(init_logs))

    # Assert only 1 catalog ran workload management initialization.
    assert init_logs[0] is None or init_logs[1] is None, "Both catalogds ran workload " \
        "management initialization"

    # Assert the standby catalog skipped workload management initialization.
    self.standby_catalog = 1
    self.active_catalog = 0
    if init_logs[0] is None:
      # Catalogd 1 is the active catalog
      self.standby_catalog = 0
      self.active_catalog = 1

    LOG.info("Found active catalogd is daemon '{}' and standby catalogd is daemon '{}'"
          .format(self.active_catalog, self.standby_catalog))

    self.assert_catalogd_log_contains("INFO",
        r"Skipping workload management initialization since catalogd HA is enabled and "
        r"this catalogd is not active", node_index=self.standby_catalog)

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt", start_args="--enable_catalogd_ha",
      statestored_args="--use_subscriber_id_as_catalogd_priority=true",
      disable_log_buffering=True, log_symlinks=True)
  def test_catalog_ha_failover(self):
    """Asserts workload management initialization is not run a second time when catalogd
       failover happens."""

    # Kill active catalogd
    catalogds = self.cluster.catalogds()
    catalogds[0].kill()

    # Wait for failover.
    catalogds[1].service.wait_for_metric_value("catalog-server.active-status",
        expected_value=True, timeout=30)

    # Wait for standby catalog to complete its initialization as the active catalogd.
    self.assert_catalogd_log_contains("INFO", r'catalog update with \d+ entries is '
        r'assembled', expected_count=-1, node_index=self.standby_catalog)

    # Assert workload management initialization did not run a second time.
    self.assert_catalogd_log_contains("INFO", r"Starting workload management "
        r"initialization", expected_count=0, node_index=self.standby_catalog)

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt",
      statestored_args="--use_subscriber_id_as_catalogd_priority=true",
      start_args="--enable_catalogd_ha --enable_statestored_ha",
      disable_log_buffering=True, log_symlinks=True)
  def test_catalog_statestore_ha(self):
    """Asserts workload management initialization is only done on the active catalogd
       when both catalog and statestore ha is enabled."""

    self.assert_log_contains("statestored", "INFO", r"Registering: catalog", 2, 30)
    self.assert_log_contains("statestored_node1", "INFO", r"Registering: catalog", 2, 30)
