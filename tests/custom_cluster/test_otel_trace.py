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

from threading import Thread
from time import sleep

from impala.error import HiveServer2Error
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import wait_for_file_line_count, count_lines
from tests.common.impala_connection import ERROR, RUNNING, FINISHED, INITIALIZED, PENDING
from tests.common.test_vector import BEESWAX, HS2, ImpalaTestDimension
from tests.util.otel_trace import parse_trace_file, ATTR_VAL_TYPE_STRING, \
    ATTR_VAL_TYPE_INT, ATTR_VAL_TYPE_BOOL
from tests.util.query_profile_util import parse_db_user, parse_session_id, parse_sql, \
    parse_query_type, parse_query_status, parse_impala_query_state, parse_query_id, \
    parse_retry_status, parse_original_query_id, parse_retried_query_id, \
    parse_num_rows_fetched, parse_admission_result, parse_default_db, \
    parse_num_modified_rows, parse_num_deleted_rows, parse_coordinator
from tests.util.retry import retry

OUT_DIR = "out_dir_traces"
TRACE_FILE = "export-trace.jsonl"
TRACE_FLAGS = "--otel_trace_enabled=true --otel_trace_exporter=file " \
              "--otel_file_flush_interval_ms=500 " \
              "--otel_file_pattern={out_dir_traces}/" + TRACE_FILE

class TestOtelTrace(CustomClusterTestSuite):

  def setup_method(self, method):
    super(TestOtelTrace, self).setup_method(method)
    self.assert_impalad_log_contains("INFO", "join Impala Service pool")
    self.trace_file_path = "{}/{}".format(self.get_tmp_dir(OUT_DIR), TRACE_FILE)
    self.trace_file_count = count_lines(self.trace_file_path, True)

  def assert_trace(self, query_id, query_profile, cluster_id, trace_cnt=1, err_span="",
      missing_spans=[], async_close=False, exact_trace_cnt=False):
    # Parse common values needed in multiple asserts.
    session_id = parse_session_id(query_profile)
    db_user = parse_db_user(query_profile)

    # Wait until all spans are written to the trace file.
    wait_for_file_line_count(
        file_path=self.trace_file_path,
        expected_line_count=trace_cnt + self.trace_file_count,
        max_attempts=60,
        sleep_time_s=1,
        backoff=1,
        exact_match=exact_trace_cnt)

    # Remove missing spans from the expected span count.
    expected_span_count = 6 - len(missing_spans)

    # Parse the trace json files to get the trace for the query.
    trace = parse_trace_file(self.trace_file_path, query_id)
    self.__assert_trace_common(trace, expected_span_count)

    # Retrieve the query status which contains error messages if the query failed.
    query_status = parse_query_status(query_profile)
    query_status = "" if query_status == "OK" else query_status

    impala_query_state = parse_retry_status(query_profile)
    if impala_query_state is None:
      impala_query_state = parse_impala_query_state(query_profile)

    # Determine if the query was retried and if so, get the original query id.
    original_query_id = parse_original_query_id(query_profile)
    original_query_id = "" if original_query_id is None else original_query_id

    # Determine if the query initially failed but has a successful retry under a different
    # query id. If so, get the retried query id.
    retried_query_id = parse_retried_query_id(query_profile)
    retried_query_id = "" if retried_query_id is None else retried_query_id

    # Error message should follow on all spans after the errored span
    in_error = False

    # Retrieve the coordinator from the query profile.
    coordinator = parse_coordinator(query_profile)

    # Parse the query type from the query profile.
    query_type = parse_query_type(query_profile)
    if query_type == "N/A":
      query_type = "UNKNOWN"

    # Assert root span.
    root_span_id = self.__assert_rootspan_attrs(trace.root_span, query_id, session_id,
      cluster_id, db_user, "default-pool", impala_query_state, query_status,
      original_query_id, retried_query_id, coordinator)

    # Assert Init span.
    if "Init" not in missing_spans:
      span_err_msg = ""
      if err_span == "Init":
        span_err_msg = query_status
        in_error = True
      self.__assert_initspan_attrs(trace.child_spans, root_span_id, query_id, session_id,
          cluster_id, db_user, "default-pool", parse_default_db(query_profile),
          parse_sql(query_profile).replace('\n', ' '), original_query_id, coordinator)

    # Assert Submitted span.
    if "Submitted" not in missing_spans:
      span_err_msg = ""
      if err_span == "Submitted" or in_error:
        span_err_msg = query_status
        in_error = True
      self.__assert_submittedspan_attrs(trace.child_spans, root_span_id, query_id)

    # Assert Planning span.
    if "Planning" not in missing_spans:
      status = INITIALIZED
      span_err_msg = ""
      if err_span == "Planning" or in_error:
        span_err_msg = query_status
        status = ERROR
        in_error = True
      self.__assert_planningspan_attrs(trace.child_spans, root_span_id, query_id,
          query_type, span_err_msg, status)

    # Assert AdmissionControl span.
    if "AdmissionControl" not in missing_spans:
      status = PENDING
      span_err_msg = ""
      if err_span == "AdmissionControl" or in_error:
        span_err_msg = query_status
        status = ERROR
        in_error = True
      self.__assert_admissioncontrol_attrs(trace.child_spans, root_span_id, query_id,
        "default-pool", parse_admission_result(query_profile), span_err_msg, status)

    # Assert QueryExecution span.
    if "QueryExecution" not in missing_spans:
      span_err_msg = ""
      if err_span == "QueryExecution" or in_error:
        span_err_msg = query_status
        in_error = True
      self.__assert_query_exec_attrs(trace.child_spans, query_profile, root_span_id,
          query_id, span_err_msg, parse_impala_query_state(query_profile))

    # Assert Close span.
    if "Close" not in missing_spans:
      span_err_msg = ""
      if err_span == "Close" or in_error:
        span_err_msg = query_status
        in_error = True
      self.__assert_close_attrs(trace.child_spans, root_span_id, query_id, span_err_msg,
          parse_impala_query_state(query_profile), async_close)

  def __assert_trace_common(self, trace, expected_child_spans_count):
    """
      Asserts common structure/fields in resource spans and scope spans of the
      OpenTelemetry trace JSON object.
    """

    # Assert the number of child spans in the trace.
    assert len(trace.child_spans) == expected_child_spans_count, \
        "Trace '{}' expected child spans count: {}, actual: {}".format(trace.trace_id,
        expected_child_spans_count, len(trace.child_spans))

    # Each scope span has a scope object which contains the name and version of the
    # OpenTelemetry scope. Assert the scope object sttructure and contents contained
    # within the single span at the path resourceSpan[0].scopeSpans[0].scope.
    assert trace.root_span.scope_name == "org.apache.impala.impalad.query", \
        "Span: '{}' expected: 'org.apache.impala.impalad.query', actual: {}" \
        .format(trace.root_span.span_id, trace.root_span.scope_name)
    assert trace.root_span.scope_version == "1.0.0", "Span: '{}' expected scope " \
        "version '1.0.0', actual: '{}'".format("Root", trace.root_span.scope_version)

    # Assert the scope of each child span.
    for span in trace.child_spans:
      assert span.scope_name == "org.apache.impala.impalad.query", \
          "Span: '{}' expected scope name: 'org.apache.impala.impalad.query', " \
          "actual: {}".format(span.name, span.scope_name)
      assert span.scope_version == "1.0.0", "Span: '{}' expected scope " \
          "version '1.0.0', actual: '{}'".format(span.name, span.scope_version)

  def __assert_scopespan_common(self, span, query_id, is_root, name, attributes_count,
        status, root_span_id=None, err_msg=""):
    """
      Helper function to assert common data points of a single scope span. These spans
      contain the actual root and child spans. Assertions include the span object's
      structure, span properties, and common span attributes.
        - span: The OtelSpan object to assert.
        - query_id: The query id of the span.
        - is_root: Whether the span is a root span.
        - name: The name of the span to assert without the query_id prefix.
        - attributes_count: The expected number of attributes unique to the span. If
                            asserting a child span, adds 7 to this value to account for
                            attributes common across all child spans.
        - status: The expected status of the span. Only used for child spans.
        - root_span_id: The root span id of the span.
    """

    # Read the span trace id and span id from the Impalad logs.
    expected_span_id, expected_trace_id = self.__find_span_log(name, query_id)

    # Assert span properties.
    expected_name = query_id
    actual_kind = span.kind

    if (is_root):
      assert span.parent_span_id is None, "Found parentSpanId on root span"
      assert actual_kind == 2, "Span '{}' expected kind: '{}', actual: '{}'" \
          .format(expected_name, 2, actual_kind)
    else:
      expected_name += " - {}".format(name)

      assert root_span_id is not None
      actual = span.parent_span_id
      assert actual == root_span_id, "Span '{}' expected parentSpanId: '{}', actual: " \
          "'{}'".format(expected_name, root_span_id, actual)

      assert actual_kind == 1, "Span '{}' expected kind: '{}', actual: '{}'" \
          .format(expected_name, 1, actual)

    actual = span.name
    assert actual == expected_name, "Expected span name: '{}', actual: '{}'" \
        .format(expected_name, actual)

    actual = span.trace_id
    assert actual == expected_trace_id, "Span '{}' expected traceId: '{}', " \
        "actual: '{}'".format(expected_name, expected_trace_id, actual)

    actual = span.span_id
    assert actual == expected_span_id, "Span '{}' expected spanId: '{}', " \
        "actual: '{}'".format(expected_name, expected_span_id, actual)

    # Flags must always be 1 which indicates the trace is to be sampled.
    expected_flags = 1
    actual = span.flags
    assert actual == expected_flags, "Span '{}' expected flags: '{}', " \
        "actual: '{}'".format(expected_name, expected_flags, actual)

    # Assert span attributes.
    expected_span_attrs_count = attributes_count if is_root else 7 + attributes_count
    assert len(span.attributes) == expected_span_attrs_count, "Span '{}' attributes " \
        "must contain exactly {} elements, actual: {}".format(expected_name,
        expected_span_attrs_count, len(span.attributes))

    if (is_root):
      self.__assert_attr(expected_name, span.attributes, "ErrorMessage", err_msg)
    else:
      self.__assert_attr(expected_name, span.attributes, "ErrorMsg", err_msg)
      self.__assert_attr(expected_name, span.attributes, "Name", expected_name)
      self.__assert_attr(expected_name, span.attributes, "Running",
        name == "QueryExecution", "boolValue")
      self.__assert_attr(expected_name, span.attributes, "Status", status)

  def __find_span_log(self, span_name, query_id):
    """
      Finds the start span log entry for the given span name and query id in the Impalad
      logs. This log line contains the trace id and span id for the span which are used
      as the expected values when asserting the span properties in the trace file.
    """
    span_regex = r'Started \'{}\' span trace_id="(.*?)" span_id="(.*?)" query_id="{}"' \
        .format(span_name, query_id)
    span_log = self.assert_impalad_log_contains("INFO", span_regex)
    trace_id = span_log.group(1)
    span_id = span_log.group(2)

    return span_id, trace_id

  def __assert_attr(self, span_name, attributes, expected_key, expected_value,
      expected_type="stringValue"):
    """
      Helper function to assert that a specific OpenTelemetry attribute exists in a span.
    """

    assert expected_type in ("stringValue", "boolValue", "intValue"), "Invalid " \
        "expected_type '{}', must be one of 'stringValue', 'boolValue', or 'intValue'" \
        .format(expected_type)

    val = attributes[expected_key]
    assert val is not None, "Span '{}' attribute not found: '{}', actual attributes: {}" \
        .format(span_name, expected_key, attributes)
    assert val.value == expected_value, "Span '{}' attribute '{}' expected: '{}', " \
        "actual: '{}'".format(span_name, expected_key, expected_value, val.value)

    if expected_type == "boolValue":
      expected_type = ATTR_VAL_TYPE_BOOL
    elif expected_type == "intValue":
      expected_type = ATTR_VAL_TYPE_INT
    else:
      expected_type = ATTR_VAL_TYPE_STRING

    assert val.get_type() == expected_type, "Span '{}' attribute '{}' expected to be " \
        "of type '{}', actual: '{}'".format(span_name, expected_key, expected_type,
         val.get_type())

  def __assert_span_events(self, span, expected_events=[]):
    """
      Helper function to assert that a span contains the expected span events.
    """
    assert len(expected_events) == len(span.events), "Span '{}' expected to have " \
        "exactly {} events, actual: {}".format(span.name, len(expected_events),
        len(span.events))

    for event in expected_events:
      assert event in span.events, "Expected '{}' event on span '{}' but " \
        "no such events was found.".format(event, span.name)

  def __assert_rootspan_attrs(self, span, query_id, session_id, cluster_id, user_name,
      request_pool, state, err_msg, original_query_id, retried_query_id, coordinator):
    """
      Helper function that asserts the common attributes in the root span.
    """

    root_span_id, _ = self.__find_span_log("Root", query_id)
    self.__assert_scopespan_common(span, query_id, True, "Root", 14, "", None, err_msg)

    self.__assert_attr(span.name, span.attributes, "QueryId", query_id)
    self.__assert_attr(span.name, span.attributes, "SessionId", session_id)
    self.__assert_attr(span.name, span.attributes, "ClusterId", cluster_id)
    self.__assert_attr(span.name, span.attributes, "UserName", user_name)
    self.__assert_attr(span.name, span.attributes, "RequestPool", request_pool)
    self.__assert_attr(span.name, span.attributes, "State", state)
    self.__assert_attr(span.name, span.attributes, "OriginalQueryId", original_query_id)
    self.__assert_attr(span.name, span.attributes, "RetriedQueryId", retried_query_id)
    self.__assert_attr(span.name, span.attributes, "Coordinator", coordinator)

    return root_span_id

  def __assert_initspan_attrs(self, spans, root_span_id, query_id, session_id, cluster_id,
      user_name, request_pool, default_db, query_string, original_query_id, coordinator):
    """
      Helper function that asserts the common and span-specific attributes in the
      init span.
    """

    # Locate the init span and assert.
    init_span = self.__find_span(spans, "Init", query_id)

    self.__assert_scopespan_common(init_span, query_id, False, "Init", 9, INITIALIZED,
        root_span_id)

    self.__assert_attr(init_span.name, init_span.attributes, "QueryId", query_id)
    self.__assert_attr(init_span.name, init_span.attributes, "SessionId", session_id)
    self.__assert_attr(init_span.name, init_span.attributes, "ClusterId", cluster_id)
    self.__assert_attr(init_span.name, init_span.attributes, "UserName", user_name)
    self.__assert_attr(init_span.name, init_span.attributes, "RequestPool", request_pool)
    self.__assert_attr(init_span.name, init_span.attributes, "DefaultDb", default_db)
    self.__assert_attr(init_span.name, init_span.attributes, "QueryString", query_string)
    self.__assert_attr(init_span.name, init_span.attributes, "OriginalQueryId",
        original_query_id)
    self.__assert_attr(init_span.name, init_span.attributes, "Coordinator", coordinator)

    self.__assert_span_events(init_span)

  def __assert_submittedspan_attrs(self, spans, root_span_id, query_id):
    """
      Helper function that asserts the common attributes in the submitted span.
    """

    submitted_span = self.__find_span(spans, "Submitted", query_id)
    self.__assert_scopespan_common(submitted_span, query_id, False, "Submitted", 0,
        INITIALIZED, root_span_id)

    self.__assert_span_events(submitted_span)

  def __assert_planningspan_attrs(self, spans, root_span_id, query_id, query_type,
      err_msg="", status=INITIALIZED):
    """
      Helper function that asserts the common and span-specific attributes in the
      planning execution span.
    """

    planning_span = self.__find_span(spans, "Planning", query_id)
    self.__assert_scopespan_common(planning_span, query_id, False, "Planning", 1,
        status, root_span_id, err_msg)
    self.__assert_attr(planning_span.name, planning_span.attributes, "QueryType",
        query_type)

    self.__assert_span_events(planning_span)

  def __assert_admissioncontrol_attrs(self, spans, root_span_id, query_id, request_pool,
      adm_result, err_msg, status):
    """
      Helper function that asserts the common and span-specific attributes in the
      admission control span.
    """

    queued = False if adm_result == "Admitted immediately" \
        or adm_result == "Admitted as a trivial query" else True

    adm_ctrl_span = self.__find_span(spans, "AdmissionControl", query_id)
    self.__assert_scopespan_common(adm_ctrl_span, query_id, False, "AdmissionControl", 3,
        status, root_span_id, err_msg)
    self.__assert_attr(adm_ctrl_span.name, adm_ctrl_span.attributes, "Queued",
        queued, "boolValue")
    self.__assert_attr(adm_ctrl_span.name, adm_ctrl_span.attributes, "AdmissionResult",
        adm_result)
    self.__assert_attr(adm_ctrl_span.name, adm_ctrl_span.attributes, "RequestPool",
        request_pool)

    if queued:
      self.__assert_span_events(adm_ctrl_span, ["Queued"])
    else:
      self.__assert_span_events(adm_ctrl_span)

  def __assert_query_exec_attrs(self, spans, query_profile, root_span_id, query_id,
      err_msg, status):
    """
      Helper function that asserts the common and span-specific attributes in the
      query execution span.
    """

    query_exec_span = self.__find_span(spans, "QueryExecution", query_id)
    self.__assert_scopespan_common(query_exec_span, query_id, False, "QueryExecution", 3,
        status, root_span_id, err_msg)
    self.__assert_attr(query_exec_span.name, query_exec_span.attributes,
        "NumModifiedRows", parse_num_modified_rows(query_profile), "intValue")
    self.__assert_attr(query_exec_span.name, query_exec_span.attributes, "NumDeletedRows",
        parse_num_deleted_rows(query_profile), "intValue")
    self.__assert_attr(query_exec_span.name, query_exec_span.attributes, "NumRowsFetched",
        parse_num_rows_fetched(query_profile), "intValue")

    # TODO: IMPALA-14334 - Assert QueryExecution span events

  def __assert_close_attrs(self, spans, root_span_id, query_id, err_msg, status,
        async_close):
    """
      Helper function that asserts the common and span-specific attributes in the
      close span.
    """

    close_span = self.__find_span(spans, "Close", query_id)
    self.__assert_scopespan_common(close_span, query_id, False, "Close", 0, status,
        root_span_id, err_msg)

    expected_events = ["QueryUnregistered"]
    if async_close and "ReleasedAdmissionControlResources" in close_span.events:
      expected_events.append("ReleasedAdmissionControlResources")
    self.__assert_span_events(close_span, expected_events)

  def __find_span(self, spans, name, query_id):
    """
      Helper function to find a span by name in a list of OtelSpan objects.
    """

    for s in spans:
      if s.name.endswith(name):
        return s

    assert False, "Span '{}' not found for query '{}'".format(name, query_id)


@CustomClusterTestSuite.with_args(
    impalad_args="-v=2 --cluster_id=select_dml {}".format(TRACE_FLAGS),
    cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
class TestOtelTraceSelectsDMLs(TestOtelTrace):
  """Tests that exercise OpenTelemetry tracing behavior for select and dml queries."""

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
          "debug_action": "3:PREPARE:FAIL|COORD_BEFORE_EXEC_RPC:SLEEP@5000",
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
    result = self.execute_query_expect_success(self.client, "SELECT * FROM "
        "functional.alltypes LIMIT 5 UNION ALL SELECT * FROM functional.alltypessmall")

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        exact_trace_cnt=True)

  def test_dml_timeout(self):
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

  def test_dml_update(self, unique_database, unique_name):
    # IMPALA-14340: Cannot update a table that has the same name as the database.
    tbl = "{}.{}_tbl".format(unique_database, unique_name)

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
        trace_cnt=4)

  def test_dml_delete(self, unique_database, unique_name):
    # IMPALA-14340: Cannot delete from a table that has the same name as the database.
    tbl = "{}.{}_tbl".format(unique_database, unique_name)

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
        trace_cnt=4)

  def test_dml_delete_join(self, unique_database, unique_name):
    tbl1 = "{}.{}_1".format(unique_database, unique_name)
    self.execute_query_expect_success(self.client, "CREATE TABLE {} STORED AS ICEBERG "
      "TBLPROPERTIES('format-version'='2') AS SELECT id, bool_col, int_col, year, month "
      "FROM functional.alltypes ORDER BY id limit 100".format(tbl1))

    tbl2 = "{}.{}_2".format(unique_database, unique_name)
    self.execute_query_expect_success(self.client, "CREATE TABLE {} STORED AS ICEBERG "
      "TBLPROPERTIES('format-version'='2') AS SELECT id, bool_col, int_col, year, month "
      "FROM functional.alltypessmall ORDER BY id LIMIT 100".format(tbl2))

    result = self.execute_query_expect_success(self.client, "DELETE t1 FROM {} t1 JOIN "
        "{} t2 ON t1.id = t2.id WHERE t2.id < 5".format(tbl1, tbl2))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=5)

  def test_ignored_queries(self, unique_database, unique_name):
    """Asserts queries that should not generate traces do not generate traces."""
    tbl = "{}.{}".format(unique_database, unique_name)
    res_create = self.execute_query_expect_success(self.client,
        "CREATE TABLE {} (a int)".format(tbl))

    # These queries are not expected to have traces created for them.
    ignore_queries = [
      "COMMENT ON DATABASE {} IS 'test'".format(unique_database),
      "DESCRIBE {}".format(tbl),
      "EXPLAIN SELECT * FROM {}".format(tbl),
      "REFRESH FUNCTIONS functional",
      "REFRESH functional.alltypes",
      "SET ALL",
      "SHOW TABLES IN {}".format(unique_database),
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
    # The expected line count is 4 because:
    #     1. unique_database fixture runs drop database
    #     2. unique_database fixture runs create database
    #     3. test runs create table
    #     4. test runs drop table
    wait_for_file_line_count(file_path=self.trace_file_path,
        expected_line_count=4 + self.trace_file_count, max_attempts=10, sleep_time_s=1,
        backoff=1, exact_match=True)

    # Assert the traces for the create/drop table query to ensure both were created.
    self.assert_trace(
        query_id=res_create.query_id,
        query_profile=res_create.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=4,
        missing_spans=["AdmissionControl"])

    self.assert_trace(
        query_id=res_drop.query_id,
        query_profile=res_drop.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=4,
        missing_spans=["AdmissionControl"])

  def test_dml_insert_success(self, unique_database, unique_name):
    self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} (id int, string_col string, int_col int)"
        .format(unique_database, unique_name))

    result = self.execute_query_expect_success(self.client,
        "INSERT INTO {}.{} (id, string_col, int_col) VALUES (1, 'a', 10), (2, 'b', 20), "
        "(3, 'c', 30)".format(unique_database, unique_name))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=4)

  def test_dml_insert_cte_success(self, unique_database, unique_name):
    self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} (id int)".format(unique_database, unique_name))

    result = self.execute_query_expect_success(self.client,
        "WITH a1 AS (SELECT * FROM functional.alltypes WHERE tinyint_col=1 limit 10) "
        "INSERT INTO {}.{} SELECT id FROM a1".format(unique_database, unique_name))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=4)

  def test_dml_insert_overwrite(self, unique_database, unique_name):
    """Test that OpenTelemetry tracing is working by running an insert overwrite query and
       checking that the trace file is created and contains expected spans with the
       expected attributes."""
    self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} AS SELECT * FROM functional.alltypes WHERE id < 500 ".format(
            unique_database, unique_name))

    result = self.execute_query_expect_success(self.client,
        "INSERT OVERWRITE TABLE {}.{} SELECT * FROM functional.alltypes WHERE id > 500 "
        "AND id < 1000".format(unique_database, unique_name))

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="select_dml",
        trace_cnt=4)


class TestOtelTraceSelectQueued(TestOtelTrace):
  """Tests that require setting additional startup flags to assert admission control
     queueing behavior. The cluster must be restarted after each test to apply the
     new flags."""

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=select_queued --default_pool_max_requests=1 {}"
                   .format(TRACE_FLAGS),
      cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
  def test_select_queued(self):
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
    self.client.execute_async("SELECT * FROM functional.alltypes WHERE id = "
        "SLEEP(5000)")

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


class TestOtelTraceSelectRetry(TestOtelTrace):
  """Tests the require ending an Impala daemon and thus the cluster must restart after
     each test."""

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=retry_select_success {}".format(TRACE_FLAGS),
      cluster_size=3, num_exclusive_coordinators=1, tmp_dir_placeholders=[OUT_DIR],
      disable_log_buffering=True,
      statestored_args="-statestore_heartbeat_frequency_ms=60000")
  def test_retry_select_success(self):
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
class TestOtelTraceDDLs(TestOtelTrace):
  """Tests that exercise OpenTelemetry tracing behavior on DDLs. These tests are in their
     own class because they require an additional test dimension for async DDLs"""

  @classmethod
  def add_test_dimensions(cls):
    super(TestOtelTraceDDLs, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('async_ddl', True, False))

  def test_ddl_createdb(self, vector, unique_name):
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

  def test_ddl_create_alter_table(self, vector, unique_database, unique_name):
    """Tests that traces are created for a successful create table, a successful alter
       table, and a failed alter table (adding a column that already exists)."""
    create_result = self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} (id int, string_col string, int_col int)"
        .format(unique_database, unique_name),
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    alter_success_result = self.execute_query_expect_success(self.client, "ALTER TABLE "
        "{}.{} ADD COLUMNS (new_col string)".format(unique_database, unique_name),
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    fail_query = "ALTER TABLE {}.{} ADD COLUMNS (new_col string)" \
        .format(unique_database, unique_name)
    self.execute_query_expect_failure(self.client, fail_query,
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    fail_query_id, fail_profile = self.query_id_from_ui(section="completed_queries",
        match_query=fail_query)

    self.assert_trace(
        query_id=create_result.query_id,
        query_profile=create_result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=5,
        missing_spans=["AdmissionControl"])

    self.assert_trace(
        query_id=alter_success_result.query_id,
        query_profile=alter_success_result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=5,
        missing_spans=["AdmissionControl"])

    self.assert_trace(
        query_id=fail_query_id,
        query_profile=fail_profile,
        cluster_id="trace_ddl",
        trace_cnt=5,
        missing_spans=["AdmissionControl", "QueryExecution"],
        err_span="Planning")

  def test_ddl_createtable_fail(self, vector, unique_name):
    with self.create_client_for_nth_impalad(1, HS2) as second_coord_client:
      try:
        # Create a database to use for this test. Cannot use the unique_database fixture
        # because we want to drop the database after planning but before execution and
        # that fixture drops the database without the "if exists" clause.
        self.execute_query_expect_success(self.client, "CREATE DATABASE {}"
            .format(unique_name))

        with self.create_client_for_nth_impalad(0, HS2) as first_coord_client:
          # In a separate thread, run the create table DDL that will fail.
          fail_query = "CREATE TABLE {}.{} (id int, string_col string, int_col int)" \
              .format(unique_name, unique_name)

          def execute_query_fail():
            self.execute_query_expect_failure(first_coord_client, fail_query,
                {"debug_action": "CRS_DELAY_BEFORE_CATALOG_OP_EXEC:SLEEP@5000",
                    "ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

          thread = Thread(target=execute_query_fail)
          thread.daemon = True
          thread.start()

          # Wait until the create table query is in flight.
          fail_query_id = None
          while fail_query_id is None:
            fail_query_id, profile = self.query_id_from_ui(section="in_flight_queries",
                match_query=fail_query, not_found_ok=True)
            if fail_query_id is not None and len(profile.strip()) > 0 \
                and parse_impala_query_state(profile) == "RUNNING":
              break
            sleep(0.1)

          # Drop the database after planning to cause the create table to fail.
          self.execute_query_expect_success(second_coord_client,
              "DROP DATABASE {} CASCADE".format(unique_name),
              {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

          # Wait until the create table query fails.
          thread.join()
          fail_profile_str = self.query_profile_from_ui(fail_query_id)
      finally:
        self.execute_query_expect_success(second_coord_client,
            "DROP DATABASE IF EXISTS {} CASCADE".format(unique_name))

    # Assert the errored query.
    self.assert_trace(
        query_id=fail_query_id,
        query_profile=fail_profile_str,
        cluster_id="trace_ddl",
        trace_cnt=4,
        missing_spans=["AdmissionControl"],
        err_span="QueryExecution")

  def test_ddl_createtable_cte_success(self, vector, unique_database, unique_name):
    result = self.execute_query_expect_success(self.client,
        "CREATE TABLE {}.{} AS WITH a1 AS (SELECT * FROM functional.alltypes WHERE "
        "tinyint_col=1 LIMIT 10) SELECT id FROM a1".format(unique_database, unique_name),
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=3,
        missing_spans=["AdmissionControl"])

  def test_compute_stats(self, vector, unique_database, unique_name):
    """The compute stats queries are a special case. These statements run two separate
       select queries. Locate both select queries on the UI and assert their traces."""

    tbl_name = "{}.{}_alltypes".format(unique_database, unique_name)

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

  def test_compute_incremental_stats(self, vector, unique_database, unique_name):
    tbl_name = "{}.{}_alltypes".format(unique_database, unique_name)

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

    # Assert the one trace matches the refresh table query.
    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=2,
        missing_spans=["AdmissionControl"])

  def test_invalidate_metadata(self, vector):
    result = self.execute_query_expect_success(self.client,
        "INVALIDATE METADATA functional.alltypes",
        {"ENABLE_ASYNC_DDL_EXECUTION": vector.get_value('async_ddl')})

    self.assert_trace(
        query_id=result.query_id,
        query_profile=result.runtime_profile,
        cluster_id="trace_ddl",
        trace_cnt=1,
        missing_spans=["AdmissionControl"])
