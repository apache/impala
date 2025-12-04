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
from datetime import datetime
from random import choice
from string import ascii_lowercase
from time import sleep

from impala.error import HiveServer2Error
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import count_lines, wait_for_file_line_count
from tests.common.impala_connection import ERROR, FINISHED, PENDING, RUNNING
from tests.common.test_vector import BEESWAX, ImpalaTestDimension
from tests.util.otel_trace import assert_trace
from tests.util.query_profile_util import parse_query_id, parse_retry_status
from tests.util.retry import retry

OUT_DIR = "out_dir_traces"
TRACE_FILE = "export-trace.jsonl"
TRACE_FLAGS = "--otel_trace_enabled=true --otel_trace_exporter=file " \
              "--otel_trace_exhaustive_dchecks --otel_file_flush_interval_ms=500 " \
              "--otel_file_pattern={out_dir_traces}/" + TRACE_FILE


class TestOtelTraceBase(CustomClusterTestSuite):

  def setup_method(self, method):
    super(TestOtelTraceBase, self).setup_method(method)
    self.assert_impalad_log_contains("INFO", "join Impala Service pool")
    self.trace_file_path = "{}/{}".format(self.get_tmp_dir(OUT_DIR), TRACE_FILE)
    self.trace_file_count = count_lines(self.trace_file_path, True)

  def assert_trace(self, query_id, query_profile, cluster_id, trace_cnt=1, err_span="",
      missing_spans=[], async_close=False, exact_trace_cnt=False):
    """Helper method to assert a trace exists in the trace file with the required inputs
       for log file path, trace file path, and trace file line count (that was determined
       before the test ran)."""
    assert_trace(self.build_log_path("impalad", "INFO"), self.trace_file_path,
        self.trace_file_count, query_id, query_profile, cluster_id, trace_cnt, err_span,
        missing_spans, async_close, exact_trace_cnt)


@CustomClusterTestSuite.with_args(
    impalad_args="-v=2 --cluster_id=select_dml --otel_debug {}".format(TRACE_FLAGS),
    cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
class TestOtelTraceSelectsDMLs(TestOtelTraceBase):
  """Tests that exercise OpenTelemetry tracing behavior for select and dml queries."""

  @classmethod
  def setup_class(cls):
    super(TestOtelTraceSelectsDMLs, cls).setup_class()

    cls.test_db = "test_otel_trace_selects_dmls{}_{}".format(
      datetime.now().strftime("%Y%m%d%H%M%S"),
      "".join(choice(ascii_lowercase) for _ in range(7)))
    cls.execute_query_expect_success(cls.client, "CREATE DATABASE {}".format(cls.test_db))

  @classmethod
  def teardown_class(cls):
    cls.execute_query_expect_success(cls.client, "DROP DATABASE {} CASCADE"
        .format(cls.test_db))
    super(TestOtelTraceSelectsDMLs, cls).teardown_class()

  def setup_method(self, method):
    super(TestOtelTraceSelectsDMLs, self).setup_method(method)
    self.client.clear_configuration()

  def test_beeswax_no_trace(self):
    """Since tracing Beeswax queries is not supported, tests that no trace is created
    when executing a query using the Beeswax protocol."""
    with self.create_impala_client(protocol=BEESWAX) as client:
      self.execute_query_expect_success(client,
          "SELECT COUNT(*) FROM functional.alltypes")
      sleep(2)  # Wait for any spans to be flushed to the trace file.

      line_count = count_lines(self.trace_file_path, True)
      assert line_count - self.trace_file_count == 0

  def test_query_success(self):
    """Test that OpenTelemetry tracing is working by running a simple query and
    checking that the trace file is created and contains spans."""
    result = self.execute_query_expect_success(self.client,
        "SELECT COUNT(*) FROM functional.alltypes")

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml")

  def test_invalid_sql(self):
    """Asserts that queries with invalid SQL still generate the expected traces."""
    query = "SELECT * FROM functional.alltypes WHERE field_does_not_exist=1"
    self.execute_query_expect_failure(self.client, query)

    # Retrieve the query id and runtime profile from the UI since the query execute call
    # only returns a HiveServer2Error object and not the query id or profile.
    query_id, profile = self.query_id_from_ui(section="completed_queries",
        match_query=query)

    self.assert_trace(
        query_id=query_id,
        query_profile=profile,
        cluster_id="select_dml",
        trace_cnt=1,
        err_span="Planning",
        missing_spans=["AdmissionControl", "QueryExecution"])

  def test_cte_query_success(self):
    """Test that OpenTelemetry tracing is working by running a simple query that uses a
       common table expression and checking that the trace file is created and contains
       expected spans with the expected attributes."""
    result = self.execute_query_expect_success(self.client,
        "WITH alltypes_tiny1 AS (SELECT * FROM functional.alltypes WHERE tinyint_col=1 "
        "LIMIT 10) SELECT id FROM alltypes_tiny1")

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml")

  def test_query_exec_fail(self):
    """Test that OpenTelemetry tracing is working by running a query that fails during
       execution and checking that the trace file is created and contains expected spans
       with the expected attributes."""
    query = "SELECT 1 FROM functional.alltypessmall a JOIN functional.alltypessmall b " \
        "ON a.id != b.id"
    self.execute_query_expect_failure(self.client,
        query,
        {
          "abort_on_error": 1,
          "batch_size": 0,
          "debug_action": "3:PREPARE:FAIL",
          "disable_codegen": 0,
          "disable_codegen_rows_threshold": 0,
          "exec_single_node_rows_threshold": 0,
          "mt_dop": 4,
          "num_nodes": 0,
          "test_replan": 1
        })

    query_id, profile = self.query_id_from_ui(section="completed_queries",
        match_query=query)

    self.assert_trace(
        query_id=query_id,
        query_profile=profile,
        cluster_id="select_dml",
        err_span="QueryExecution",
        async_close=True)

  def test_select_timeout(self):
    """Asserts queries that timeout generate the expected traces."""
    query = "SELECT * FROM functional.alltypes WHERE id=sleep(5000)"
    self.execute_query_expect_failure(self.client, query, {"exec_time_limit_s": "1"})

    # Retrieve the query id and runtime profile from the UI since the query execute call
    # only returns a HiveServer2Error object and not the query id or profile.
    query_id, profile = self.query_id_from_ui(section="completed_queries",
        match_query=query)

    self.assert_trace(
        query_id=query_id,
        query_profile=profile,
        cluster_id="select_dml",
        trace_cnt=1,
        err_span="QueryExecution",
        async_close=True)

  def test_select_empty(self):
    """Asserts empty queries do not generate traces."""
    self.execute_query_expect_failure(self.client, "")

    # Run a query that will succeed to ensure all traces have been flushed.
    result = self.execute_query_expect_success(self.client, "select 1")

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=1,
        err_span="QueryExecution",
        async_close=True)

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        exact_trace_cnt=True)

  def test_select_union(self):
    """Asserts queries with a union generate the expected traces."""
    result = self.execute_query_expect_success(self.client, "SELECT * FROM "
        "functional.alltypes LIMIT 5 UNION ALL SELECT * FROM functional.alltypessmall")

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        exact_trace_cnt=True)

  def test_dml_timeout(self):
    """Asserts insert DMLs that timeout generate the expected traces."""
    query = "INSERT INTO functional.alltypes (id, string_col, year, month) VALUES " \
        "(99999, 'foo', 2025, 1)"
    self.execute_query_expect_failure(self.client, query,
        {"debug_action": "CRS_AFTER_COORD_STARTS:SLEEP@3000", "exec_time_limit_s": "1"})

    # Retrieve the query id and runtime profile from the UI since the query execute call
    # only returns a HiveServer2Error object and not the query id or profile.
    query_id, profile = self.query_id_from_ui(section="completed_queries",
        match_query=query)

    self.assert_trace(
        query_id=query_id,
        query_profile=profile,
        cluster_id="select_dml",
        trace_cnt=1,
        err_span="QueryExecution",
        async_close=True)

  def test_dml_update(self, unique_name):
    """Asserts update DMLs generate the expected traces."""
    tbl = "{}.{}".format(self.test_db, unique_name)

    self.execute_query_expect_success(self.client, "CREATE TABLE {} (id int, "
        "string_col string) STORED AS ICEBERG TBLPROPERTIES('format-version'='2')"
        .format(tbl))

    self.execute_query_expect_success(self.client, "INSERT INTO {} (id, string_col) "
        "VALUES (1, 'foo'), (2, 'bar')".format(tbl))

    result = self.execute_query_expect_success(self.client,
        "UPDATE {} SET string_col='a' WHERE id=1".format(tbl))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=3)

  def test_dml_delete(self, unique_name):
    """Asserts delete DMLs generate the expected traces."""
    tbl = "{}.{}".format(self.test_db, unique_name)

    self.execute_query_expect_success(self.client, "CREATE TABLE {} (id int, "
        "string_col string) STORED AS ICEBERG TBLPROPERTIES('format-version'='2')"
        .format(tbl))

    self.execute_query_expect_success(self.client, "INSERT INTO {} (id, string_col) "
        "VALUES (1, 'foo'), (2, 'bar')".format(tbl))

    result = self.execute_query_expect_success(self.client,
        "DELETE FROM {} WHERE id=1".format(tbl))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=3)

  def test_dml_delete_join(self, unique_name):
    """Asserts delete join DMLs generate the expected traces."""
    tbl1 = "{}.{}_1".format(self.test_db, unique_name)
    self.execute_query_expect_success(self.client, "CREATE TABLE {} STORED AS ICEBERG "
      "TBLPROPERTIES('format-version'='2') AS SELECT id, bool_col, int_col, year, month "
      "FROM functional.alltypes ORDER BY id limit 100".format(tbl1))

    tbl2 = "{}.{}_2".format(self.test_db, unique_name)
    self.execute_query_expect_success(self.client, "CREATE TABLE {} STORED AS ICEBERG "
      "TBLPROPERTIES('format-version'='2') AS SELECT id, bool_col, int_col, year, month "
      "FROM functional.alltypessmall ORDER BY id LIMIT 100".format(tbl2))

    result = self.execute_query_expect_success(self.client, "DELETE t1 FROM {} t1 JOIN "
        "{} t2 ON t1.id = t2.id WHERE t2.id < 5".format(tbl1, tbl2))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=3)

  def test_ignored_queries(self, unique_name):
    """Asserts queries that should not generate traces do not generate traces."""
    tbl = "{}.{}".format(self.test_db, unique_name)
    res_create = self.execute_query_expect_success(self.client,
        "CREATE TABLE {} (a int)".format(tbl))

    # These queries are not expected to have traces created for them.
    ignore_queries = [
      "COMMENT ON DATABASE {} IS 'test'".format(self.test_db),
      "DESCRIBE {}".format(tbl),
      "EXPLAIN SELECT * FROM {}".format(tbl),
      "REFRESH FUNCTIONS functional",
      "REFRESH functional.alltypes",
      "SET ALL",
      "SHOW TABLES IN {}".format(self.test_db),
      "SHOW DATABASES",
      "TRUNCATE TABLE {}".format(tbl),
      "USE functional",
      "VALUES (1, 2, 3)",
      "/*comment1*/SET EXPLAIN_LEVEL=0",
      "--comment1\nSET EXPLAIN_LEVEL=0"
    ]

    for query in ignore_queries:
      self.execute_query_expect_success(self.client, query)

    # REFRESH AUTHORIZATION will error if authorization is not enabled. Since the test is
    # only asserting that no trace is created, ignore any errors from the query.
    try:
      self.execute_query_expect_success(self.client, "REFRESH AUTHORIZATION")
    except HiveServer2Error:
      pass

    # Run one more query that is expected to have a trace created to ensure that all
    # traces have been flushed to the trace file.
    res_drop = self.execute_query_expect_success(self.client, "DROP TABLE {} PURGE"
        .format(tbl))

    # Ensure the expected number of traces are present in the trace file.
    # The expected line count is 2 because:
    #     1. test runs create table
    #     2. test runs drop table
    wait_for_file_line_count(file_path=self.trace_file_path,
        expected_line_count=2 + self.trace_file_count, max_attempts=10, sleep_time_s=1,
        backoff=1, exact_match=True)

    # Assert the traces for the create/drop table query to ensure both were created.
    self.assert_trace(
        query_id=res_create.query_id,
        query_profile=res_create.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=2,
        missing_spans=["AdmissionControl"])

    self.assert_trace(
        query_id=res_drop.query_id,
        query_profile=res_drop.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=2,
        missing_spans=["AdmissionControl"])

  def test_dml_insert_success(self, unique_name):
    """Asserts successful insert DMLs generate the expected traces."""
    self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} (id int, string_col string, int_col int)"
        .format(self.test_db, unique_name))

    result = self.execute_query_expect_success(self.client,
        "INSERT INTO {}.{} (id, string_col, int_col) VALUES (1, 'a', 10), (2, 'b', 20), "
        "(3, 'c', 30)".format(self.test_db, unique_name))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=2)

  def test_dml_insert_fail(self, unique_name):
    """Asserts failed insert DMLs generate the expected traces."""
    self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} (id int, string_col string, int_col int)"
        .format(self.test_db, unique_name))

    fail_query = "INSERT INTO {}.{} (id, string_col, int_col) VALUES (1, 'a', 10), " \
        "(2, 'b', 20), (3, 'c', 30)".format(self.test_db, unique_name)
    self.execute_query_expect_failure(self.client, fail_query,
        {"debug_action": "0:OPEN:FAIL"})
    query_id, profile = self.query_id_from_ui(section="completed_queries",
        match_query=fail_query)

    self.assert_trace(
        query_id=query_id,
        query_profile=profile,
        cluster_id="select_dml",
        trace_cnt=2,
        err_span="QueryExecution")

  def test_dml_insert_cte_success(self, unique_name):
    """Asserts insert DMLs that use a CTE generate the expected traces."""
    self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} (id int)".format(self.test_db, unique_name))

    result = self.execute_query_expect_success(self.client,
        "WITH a1 AS (SELECT * FROM functional.alltypes WHERE tinyint_col=1 limit 10) "
        "INSERT INTO {}.{} SELECT id FROM a1".format(self.test_db, unique_name))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=2)

  def test_dml_insert_overwrite(self, unique_name):
    """Test that OpenTelemetry tracing is working by running an insert overwrite query and
       checking that the trace file is created and contains expected spans with the
       expected attributes."""
    self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} AS SELECT * FROM functional.alltypes WHERE id < 500 ".format(
            self.test_db, unique_name))

    result = self.execute_query_expect_success(self.client,
        "INSERT OVERWRITE TABLE {}.{} SELECT * FROM functional.alltypes WHERE id > 500 "
        "AND id < 1000".format(self.test_db, unique_name))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=2)

  def test_debug_logs(self):
    """Asserts OpenTelemetry SDK debug logs are written to the Impalad INFO log when
       vlog is set to 2."""
    result = self.execute_query_expect_success(self.client,
        "SELECT COUNT(*) FROM functional.alltypes")
    assert result.success

    # Assert the trace exporter debug log lines were emitted.
    self.assert_impalad_log_contains("INFO", r"otel-log-handler.cc:\d+\] \[OTLP TRACE "
        r"FILE Exporter\] Export \d+ trace span\(s\) success file=\".*?"
        r"\/opentelemetry-cpp-\d+.\d+.\d+\/exporters\/otlp\/src\/otlp_file_exporter.cc\" "
        r"line=\"\d+\"", -1)

    self.assert_impalad_log_contains("INFO", r"otel-log-handler.cc:\d+\] \[OTLP FILE "
        r"Client\] Write body\(Json\).*? file=\".*?"
        r"\/opentelemetry-cpp-\d+.\d+.\d+\/exporters\/otlp\/src\/otlp_file_client.cc\" "
        r"line=\"\d+\"", -1)


class TestOtelTraceSelectQueued(TestOtelTraceBase):
  """Tests that require setting additional startup flags to assert admission control
     queueing behavior. The cluster must be restarted after each test to apply the
     new flags."""

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=select_queued --default_pool_max_requests=1 {}"
                   .format(TRACE_FLAGS),
      cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
  def test_select_queued(self):
    """Asserts a query that is queued in admission control then completes successfully
       generates the expected trace."""
    # Launch two queries, the second will be queued until the first completes.
    query = "SELECT * FROM functional.alltypes WHERE id = 1"
    handle1 = self.client.execute_async("{} AND int_col = SLEEP(5000)".format(query))
    self.client.wait_for_impala_state(handle1, RUNNING, 60)
    query_id_1 = self.client.handle_id(handle1)

    handle2 = self.client.execute_async(query)
    query_id_2 = self.client.handle_id(handle2)

    self.client.wait_for_impala_state(handle1, FINISHED, 60)
    self.client.fetch(None, handle1)
    query_profile_1 = self.client.get_runtime_profile(handle1)
    self.client.close_query(handle1)

    self.client.wait_for_impala_state(handle2, FINISHED, 60)
    self.client.fetch(None, handle2)
    query_profile_2 = self.client.get_runtime_profile(handle2)

    self.client.close_query(handle2)

    self.assert_trace(
        query_id=query_id_1,
        query_profile=query_profile_1,
        cluster_id="select_queued",
        trace_cnt=3)

    self.assert_trace(
        query_id=query_id_2,
        query_profile=query_profile_2,
        cluster_id="select_queued",
        trace_cnt=3)

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=cancel_adm_ctl --default_pool_max_requests=1 {}"
      .format(TRACE_FLAGS), cluster_size=1, tmp_dir_placeholders=[OUT_DIR],
      disable_log_buffering=True)
  def test_cancel_during_admission_control(self):
    """Tests that a query that is queued in admission control can be cancelled and
       the trace is created with the expected spans and events."""
    # Start a long-running query that will take up the only admission control slot.
    handle1 = self.client.execute_async("SELECT * FROM functional.alltypes WHERE id = "
        "SLEEP(5000)")
    self.client.wait_for_impala_state(handle1, RUNNING, 60)

    # Start a second query that will be queued and then cancelled.
    handle2 = self.client.execute_async("SELECT * FROM functional.alltypes")
    self.client.wait_for_impala_state(handle2, PENDING, 30)
    query_id = self.client.handle_id(handle2)

    # Cancel the second query while it is queued.
    self.execute_query_expect_success(self.client, "KILL QUERY '{}'".format(query_id))
    self.client.wait_for_impala_state(handle2, ERROR, 30)
    query_profile = self.client.get_runtime_profile(handle2)

    self.assert_trace(
        query_id=query_id,
        query_profile=query_profile,
        cluster_id="cancel_adm_ctl",
        trace_cnt=1,
        err_span="AdmissionControl",
        missing_spans=["QueryExecution"],
        async_close=True)


class TestOtelTraceSelectRetry(TestOtelTraceBase):
  """Tests the require ending an Impala daemon and thus the cluster must restart after
     each test."""

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=retry_select_success {}".format(TRACE_FLAGS),
      cluster_size=3, num_exclusive_coordinators=1, tmp_dir_placeholders=[OUT_DIR],
      disable_log_buffering=True,
      statestored_args="-statestore_heartbeat_frequency_ms=60000")
  def test_retry_select_success(self):
    """Asserts select queries that are successfully retried generate the expected
       traces."""
    self.cluster.impalads[1].kill()

    result = self.execute_query_expect_success(self.client,
        "SELECT COUNT(*) FROM tpch_parquet.lineitem WHERE l_orderkey < 50",
        {"RETRY_FAILED_QUERIES": True})
    retried_query_id = parse_query_id(result.runtime_profile)
    orig_query_profile = self.query_profile_from_ui(result.query_id)

    # Assert the trace from the original query.
    self.assert_trace(
        query_id=result.query_id,
        query_profile=orig_query_profile,
        cluster_id="retry_select_success",
        trace_cnt=2,
        err_span="QueryExecution",
        async_close=True)

    # Assert the trace from the retried query.
    self.assert_trace(
        query_id=retried_query_id,
        query_profile=result.runtime_profile,
        cluster_id="retry_select_success",
        trace_cnt=2,
        missing_spans=["Submitted", "Planning"],
        async_close=True)

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=retry_select_failed {}".format(TRACE_FLAGS),
      cluster_size=3, num_exclusive_coordinators=1, tmp_dir_placeholders=[OUT_DIR],
      disable_log_buffering=True,
      statestored_args="-statestore_heartbeat_frequency_ms=1000")
  def test_retry_select_failed(self):
    """Asserts select queries that are retried but ultimately fail generate the expected
       traces."""
    with self.create_impala_client() as client:
      client.set_configuration({"retry_failed_queries": "true"})

      # Launch a shuffle heavy query, it should be retried.
      handle = client.execute_async("SELECT * FROM tpch.lineitem t1, tpch.lineitem "
          "t2 WHERE t1.l_orderkey = t2.l_orderkey ORDER BY t1.l_orderkey, t2.l_orderkey "
          "LIMIT 1")
      client.wait_for_impala_state(handle, RUNNING, 60)
      query_id = client.handle_id(handle)
      self.cluster.impalads[1].kill()

      # Wait until the retry is running.
      def __wait_until_retried():
        return parse_retry_status(self.query_profile_from_ui(query_id)) == "RETRIED"
      retry(__wait_until_retried, 60, 1, 1, False)

      # Kill another impalad so that another retry is attempted.
      self.cluster.impalads[2].kill()

      # Wait until the query fails.
      client.wait_for_impala_state(handle, ERROR, 60)

      retried_query_profile = client.get_runtime_profile(handle)
      retried_query_id = parse_query_id(retried_query_profile)
      orig_query_profile = self.query_profile_from_ui(query_id)

      client.close_query(handle)

    # Assert the trace from the original query.
    self.assert_trace(
        query_id=query_id,
        query_profile=orig_query_profile,
        cluster_id="retry_select_failed",
        trace_cnt=2,
        err_span="QueryExecution",
        async_close=True)

    # Assert the trace from the retried query.
    self.assert_trace(
        query_id=retried_query_id,
        query_profile=retried_query_profile,
        cluster_id="retry_select_failed",
        trace_cnt=2,
        err_span="QueryExecution",
        missing_spans=["Submitted", "Planning"],
        async_close=True)


@CustomClusterTestSuite.with_args(
    impalad_args="-v=2 --cluster_id=trace_ddl {}".format(TRACE_FLAGS),
    cluster_size=2, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
class TestOtelTraceDDLs(TestOtelTraceBase):
  """Tests that exercise OpenTelemetry tracing behavior on DDLs. These tests are in their
     own class because they require an additional test dimension for async DDLs,"""

  @classmethod
  def setup_class(cls):
    super(TestOtelTraceDDLs, cls).setup_class()

    cls.test_db = "test_otel_trace_ddls_{}_{}".format(
      datetime.now().strftime("%Y%m%d%H%M%S"),
      "".join(choice(ascii_lowercase) for _ in range(7)))
    cls.execute_query_expect_success(cls.client, "CREATE DATABASE {}".format(cls.test_db))

  @classmethod
  def teardown_class(cls):
    cls.execute_query_expect_success(cls.client, "DROP DATABASE {} CASCADE"
        .format(cls.test_db))
    super(TestOtelTraceDDLs, cls).teardown_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestOtelTraceDDLs, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('async_ddl', True, False))

  def test_ddl_createdb(self, vector, unique_name):
    """Asserts a successful create database and drop database generate the expected
       traces."""
    try:
      result = self.execute_query_expect_success(self.client,
          "CREATE DATABASE {}".format(unique_name),
          {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

      self.assert_trace(
          query_id=result.query_id,
          query_profile=result.runtime_profile,
          cluster_id="trace_ddl",
          missing_spans=["AdmissionControl"])
    finally:
      result = self.execute_query_expect_success(self.client,
          "DROP DATABASE IF EXISTS {}".format(unique_name),
          {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})
      self.assert_trace(
          query_id=result.query_id,
          query_profile=result.runtime_profile,
          cluster_id="trace_ddl",
          trace_cnt=2,
          missing_spans=["AdmissionControl"])

  def test_ddl_create_alter_table(self, vector, unique_name):
    """Tests that traces are created for a successful create table, a successful alter
       table, and a failed alter table (adding a column that already exists)."""
    create_result = self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} (id int, string_col string, int_col int)"
        .format(self.test_db, unique_name),
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    alter_success_result = self.execute_query_expect_success(self.client, "ALTER TABLE "
        "{}.{} ADD COLUMNS (new_col string)".format(self.test_db, unique_name),
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    fail_query = "ALTER TABLE {}.{} ADD COLUMNS (new_col string)" \
        .format(self.test_db, unique_name)
    self.execute_query_expect_failure(self.client, fail_query,
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    fail_query_id, fail_profile = self.query_id_from_ui(section="completed_queries",
        match_query=fail_query)

    self.assert_trace(
        query_id=create_result.query_id,
        query_profile=create_result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=3,
        missing_spans=["AdmissionControl"])

    self.assert_trace(
        query_id=alter_success_result.query_id,
        query_profile=alter_success_result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=3,
        missing_spans=["AdmissionControl"])

    self.assert_trace(
        query_id=fail_query_id,
        query_profile=fail_profile,
        cluster_id="trace_ddl",
        trace_cnt=3,
        missing_spans=["AdmissionControl", "QueryExecution"],
        err_span="Planning")

  def test_ddl_createtable_fail(self, vector, unique_name):
    """Asserts a failed create table generates the expected trace."""
    query = "CREATE TABLE {}.{} AS (SELECT * FROM functional.alltypes LIMIT 1)" \
        .format(self.test_db, unique_name)
    self.execute_query_expect_failure(self.client, query,
      {"debug_action": "CLIENT_REQUEST_UPDATE_CATALOG:FAIL",
        "ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})
    query_id, profile = self.query_id_from_ui(section="completed_queries",
        match_query=query)

    self.assert_trace(
        query_id=query_id,
        query_profile=profile,
        cluster_id="trace_ddl",
        trace_cnt=1,
        missing_spans=["AdmissionControl"],
        err_span="QueryExecution")

  def test_ddl_createtable_cte_success(self, vector, unique_name):
    """Asserts create table queries that use a CTE generate the expected traces."""
    result = self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} AS WITH a1 AS (SELECT * FROM functional.alltypes WHERE "
        "tinyint_col=1 LIMIT 10) SELECT id FROM a1".format(self.test_db, unique_name),
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=1,
        missing_spans=["AdmissionControl"])

  def test_compute_stats(self, vector, unique_name):
    """The compute stats queries are a special case. These statements run two separate
       select queries. Locate both select queries on the UI and assert their traces."""

    tbl_name = "{}.{}_alltypes".format(self.test_db, unique_name)

    # Setup a test table to ensure calculating stats on an existing table does not impact
    # other tests.
    self.execute_query_expect_success(self.client, "CREATE TABLE {} PARTITIONED BY "
        "(year, month) AS SELECT * FROM functional.alltypes".format(tbl_name))

    compute_result = self.execute_query_expect_success(self.client,
        "COMPUTE STATS {}".format(tbl_name),
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    query_id_cnt, profile_cnt = self.query_id_from_ui(section="completed_queries",
        match_query="SELECT COUNT(*), `year`, `month` FROM {} GROUP BY "
                    "`year`, `month`".format(tbl_name))

    query_id_ndv, profile_ndv = self.query_id_from_ui(section="completed_queries",
        match_func=lambda q: q['stmt'].startswith("SELECT NDV(id)"))

    self.assert_trace(
        query_id=compute_result.query_id,
        query_profile=compute_result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=4,
        missing_spans=["AdmissionControl"])

    self.assert_trace(
        query_id=query_id_cnt,
        query_profile=profile_cnt,
        cluster_id="trace_ddl",
        trace_cnt=4)

    self.assert_trace(
        query_id=query_id_ndv,
        query_profile=profile_ndv,
        cluster_id="trace_ddl",
        trace_cnt=4)

  def test_compute_incremental_stats(self, vector, unique_name):
    """Asserts compute incremental stats queries generate the expected traces."""
    tbl_name = "{}.{}_alltypes".format(self.test_db, unique_name)

    # Setup a test table to ensure calculating stats on an existing table does not impact
    # other tests.
    self.execute_query_expect_success(self.client, "CREATE TABLE {} PARTITIONED BY "
        "(year, month) AS SELECT * FROM functional.alltypes".format(tbl_name))

    result = self.execute_query_expect_success(self.client,
        "COMPUTE INCREMENTAL STATS {} PARTITION (month=1)".format(tbl_name),
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=2,
        missing_spans=["AdmissionControl"])

  def test_invalidate_metadata(self, vector):
    """Asserts invalidate metadata queries generate the expected traces."""
    result = self.execute_query_expect_success(self.client,
        "INVALIDATE METADATA functional.alltypes",
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=1,
        missing_spans=["AdmissionControl"])
