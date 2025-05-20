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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import wait_for_file_line_count
from tests.common.impala_connection import ERROR, RUNNING, FINISHED
from tests.common.test_vector import PROTOCOL, HS2, BEESWAX, ImpalaTestDimension
from tests.util.otel_trace import parse_trace_file, ATTR_VAL_TYPE_STRING, \
    ATTR_VAL_TYPE_INT, ATTR_VAL_TYPE_BOOL
from tests.util.query_profile_util import parse_db_user, parse_session_id, parse_sql, \
    parse_query_type, parse_query_status, parse_impala_query_state, parse_query_id, \
    parse_retry_status, parse_original_query_id, parse_retried_query_id, \
    parse_num_rows_fetched, parse_admission_result
from tests.util.retry import retry


class TestOtelTrace(CustomClusterTestSuite):
  """Tests that exercise OpenTelemetry tracing behavior."""

  OUT_DIR = "out_dir"
  TRACE_FILE = "export-trace.jsonl"

  @classmethod
  def add_test_dimensions(cls):
    super(TestOtelTrace, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension(PROTOCOL, HS2, BEESWAX))

  def setup_method(self, method):
    super(TestOtelTrace, self).setup_method(method)

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=otel_trace --otel_trace_enabled=true "
                   "--otel_trace_exporter=file --otel_file_flush_interval_ms=500 "
                   "--otel_file_pattern={out_dir}/" + TRACE_FILE,
      cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
  def test_query_success(self, vector):
    """Test that OpenTelemetry tracing is working by running a simple query and
    checking that the trace file is created and contains spans."""
    query = "select count(*) from functional.alltypes"
    result = self.execute_query_expect_success(
        self.create_impala_client_from_vector(vector), query)

    self.__assert_trace(result.query_id, result.runtime_profile, "otel_trace")

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=test_invalid_sql "
                   "--otel_trace_enabled=true --otel_trace_exporter=file "
                   "--otel_file_flush_interval_ms=500 "
                   "--otel_file_pattern={out_dir}/" + TRACE_FILE,
      cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
  def test_invalid_sql(self, vector):
    query = "select * from functional.alltypes where field_does_not_exist=1"
    self.execute_query_expect_failure(self.create_impala_client_from_vector(vector),
        query)

    # Retrieve the query id and runtime profile from the UI since the query execute call
    # only returns a HiveServer2Error object and not the query id or profile.
    query_id, profile = self.query_id_from_ui(section="completed_queries",
        match_query=query)

    self.__assert_trace(
        query_id=query_id,
        query_profile=profile,
        cluster_id="test_invalid_sql",
        trace_cnt=1,
        err_span="Planning",
        missing_spans=["AdmissionControl", "QueryExecution"])

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=test_retry_select_success "
                   "--otel_trace_enabled=true --otel_trace_exporter=file "
                   "--otel_file_flush_interval_ms=500 "
                   "--otel_file_pattern={out_dir}/" + TRACE_FILE,
      cluster_size=3, num_exclusive_coordinators=1, tmp_dir_placeholders=[OUT_DIR],
      disable_log_buffering=True,
      statestored_args="-statestore_heartbeat_frequency_ms=60000")
  def test_retry_select_success(self, vector):
    query = "select count(*) from tpch_parquet.lineitem where l_orderkey < 50"
    self.cluster.impalads[1].kill()

    result = self.execute_query_expect_success(
        self.create_impala_client_from_vector(vector), query,
        {"RETRY_FAILED_QUERIES": True})
    retried_query_id = parse_query_id(result.runtime_profile)
    orig_query_profile = self.query_profile_from_ui(result.query_id)

    # Assert the trace from the original query.
    self.__assert_trace(
        query_id=result.query_id,
        query_profile=orig_query_profile,
        cluster_id="test_retry_select_success",
        trace_cnt=2,
        err_span="QueryExecution")

    # Assert the trace from the retried query.
    self.__assert_trace(
        query_id=retried_query_id,
        query_profile=result.runtime_profile,
        cluster_id="test_retry_select_success",
        trace_cnt=2,
        missing_spans=["Submitted", "Planning"])

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=test_retry_select_failed "
                   "--otel_trace_enabled=true --otel_trace_exporter=file "
                   "--otel_file_flush_interval_ms=500 "
                   "--otel_file_pattern={out_dir}/" + TRACE_FILE,
      cluster_size=3, num_exclusive_coordinators=1, tmp_dir_placeholders=[OUT_DIR],
      disable_log_buffering=True,
      statestored_args="-statestore_heartbeat_frequency_ms=1000")
  def test_retry_select_failed(self, vector):
    # Shuffle heavy query.
    query = "select * from tpch.lineitem t1, tpch.lineitem t2 where " \
      "t1.l_orderkey = t2.l_orderkey order by t1.l_orderkey, t2.l_orderkey limit 1"

    vector.set_exec_option("retry_failed_queries", "true")
    client = self.create_impala_client_from_vector(vector)

    # Launch a query, it should be retried.
    handle = self.execute_query_async_using_client(client, query, vector)
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
    self.__assert_trace(
        query_id=query_id,
        query_profile=orig_query_profile,
        cluster_id="test_retry_select_failed",
        trace_cnt=3,
        err_span="QueryExecution")

    # Assert the trace from the retried query.
    self.__assert_trace(
        query_id=retried_query_id,
        query_profile=retried_query_profile,
        cluster_id="test_retry_select_failed",
        trace_cnt=3,
        err_span="QueryExecution",
        missing_spans=["Submitted", "Planning"])

  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=test_select_queued "
                   "--otel_trace_enabled=true --otel_trace_exporter=file "
                   "--otel_file_flush_interval_ms=500 "
                   "--otel_file_pattern={out_dir}/" + TRACE_FILE + " "
                   "--default_pool_max_requests=1",
      cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
  def test_select_queued(self, vector):
    # Launch two queries, the second will be queued until the first completes.
    client = self.create_impala_client_from_vector(vector)

    query = "select * from functional.alltypes where id = 1"
    handle1 = self.execute_query_async_using_client(client,
        "{} and int_col = sleep(5000)".format(query), vector)
    client.wait_for_impala_state(handle1, RUNNING, 60)
    query_id_1 = client.handle_id(handle1)

    handle2 = self.execute_query_async_using_client(client, query, vector)
    query_id_2 = client.handle_id(handle2)

    client.wait_for_impala_state(handle1, FINISHED, 60)
    query_profile_1 = client.get_runtime_profile(handle1)
    client.close_query(handle1)

    client.wait_for_impala_state(handle2, FINISHED, 60)
    query_profile_2 = client.get_runtime_profile(handle2)

    client.close_query(handle2)

    self.__assert_trace(
        query_id=query_id_1,
        query_profile=query_profile_1,
        cluster_id="test_select_queued",
        trace_cnt=3)

    self.__assert_trace(
        query_id=query_id_2,
        query_profile=query_profile_2,
        cluster_id="test_select_queued",
        trace_cnt=3)

  ######################
  # Helper functions.
  ######################
  def __assert_trace(self, query_id, query_profile, cluster_id, trace_cnt=1, err_span="",
      missing_spans=[]):
    # Parse common values needed in multiple asserts.
    session_id = parse_session_id(query_profile)
    db_user = parse_db_user(query_profile)

    # Wait until all spans are written to the trace file.
    trace_file_path = "{}/{}".format(self.get_tmp_dir(self.OUT_DIR), self.TRACE_FILE)
    wait_for_file_line_count(
        file_path=trace_file_path,
        expected_line_count=trace_cnt,
        max_attempts=60,
        sleep_time_s=1,
        backoff=1)

    # Remove missing spans from the expected span count.
    expected_span_count = 6 - len(missing_spans)

    # Parse the trace json files to get the trace for the query.
    trace = parse_trace_file(trace_file_path, query_id)
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

    # Assert root span.
    root_span_id = self.__assert_rootspan_attrs(trace.root_span, query_id, session_id,
      cluster_id, db_user, "default-pool", impala_query_state, query_status,
      original_query_id, retried_query_id)

    # Assert Init span.
    if "Init" not in missing_spans:
      span_err_msg = ""
      if err_span == "Init":
        span_err_msg = query_status
        in_error = True
      self.__assert_initspan_attrs(trace.child_spans, root_span_id, query_id, session_id,
          cluster_id, db_user, "default-pool", "default", parse_sql(query_profile),
          original_query_id)

    # Assert Submitted span.
    if "Submitted" not in missing_spans:
      span_err_msg = ""
      if err_span == "Submitted" or in_error:
        span_err_msg = query_status
        in_error = True
      self.__assert_submittedspan_attrs(trace.child_spans, root_span_id, query_id)

    # Assert Planning span.
    if "Planning" not in missing_spans:
      span_err_msg = ""
      if err_span == "Planning" or in_error:
        span_err_msg = query_status
        in_error = True
      query_type = parse_query_type(query_profile)
      if query_type == "N/A":
        query_type = "UNKNOWN"
      self.__assert_planningspan_attrs(trace.child_spans, root_span_id, query_id,
          query_type, span_err_msg)

    # Assert AdmissionControl span.
    if "AdmissionControl" not in missing_spans:
      self.__assert_admissioncontrol_attrs(trace.child_spans, root_span_id, query_id,
        "default-pool", parse_admission_result(query_profile))

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
          parse_impala_query_state(query_profile))

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

  def __assert_rootspan_attrs(self, span, query_id, session_id, cluster_id, user_name,
      request_pool, state, err_msg, original_query_id, retried_query_id):
    """
      Helper function that asserts the common attributes in the root span.
    """

    root_span_id, _ = self.__find_span_log("Root", query_id)
    self.__assert_scopespan_common(span, query_id, True, "Root", 13, "", None, err_msg)

    self.__assert_attr(span.name, span.attributes, "QueryId", query_id)
    self.__assert_attr(span.name, span.attributes, "SessionId", session_id)
    self.__assert_attr(span.name, span.attributes, "ClusterId", cluster_id)
    self.__assert_attr(span.name, span.attributes, "UserName", user_name)
    self.__assert_attr(span.name, span.attributes, "RequestPool", request_pool)
    self.__assert_attr(span.name, span.attributes, "State", state)
    self.__assert_attr(span.name, span.attributes, "OriginalQueryId", original_query_id)
    self.__assert_attr(span.name, span.attributes, "RetriedQueryId", retried_query_id)

    return root_span_id

  def __assert_initspan_attrs(self, spans, root_span_id, query_id, session_id, cluster_id,
      user_name, request_pool, default_db, query_string, original_query_id):
    """
      Helper function that asserts the common and span-specific attributes in the
      init span.
    """

    # Locate the init span and assert.
    init_span = self.__find_span(spans, "Init", query_id)

    self.__assert_scopespan_common(init_span, query_id, False, "Init", 8, "INITIALIZED",
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

  def __assert_submittedspan_attrs(self, spans, root_span_id, query_id):
    """
      Helper function that asserts the common attributes in the submitted span.
    """

    submitted_span = self.__find_span(spans, "Submitted", query_id)
    self.__assert_scopespan_common(submitted_span, query_id, False, "Submitted", 0,
        "INITIALIZED", root_span_id)

  def __assert_planningspan_attrs(self, spans, root_span_id, query_id, query_type,
      err_msg="", status="INITIALIZED"):
    """
      Helper function that asserts the common and span-specific attributes in the
      planning execution span.
    """

    planning_span = self.__find_span(spans, "Planning", query_id)
    self.__assert_scopespan_common(planning_span, query_id, False, "Planning", 1,
        status, root_span_id, err_msg)
    self.__assert_attr(planning_span.name, planning_span.attributes, "QueryType",
        query_type)

  def __assert_admissioncontrol_attrs(self, spans, root_span_id, query_id, request_pool,
      adm_result, err_msg="", status="PENDING"):
    """
      Helper function that asserts the common and span-specific attributes in the
      admission control span.
    """

    queued = False if adm_result == "Admitted immediately" else True

    adm_ctrl_span = self.__find_span(spans, "AdmissionControl", query_id)
    self.__assert_scopespan_common(adm_ctrl_span, query_id, False, "AdmissionControl", 3,
        status, root_span_id, err_msg)
    self.__assert_attr(adm_ctrl_span.name, adm_ctrl_span.attributes, "Queued",
        queued, "boolValue")
    self.__assert_attr(adm_ctrl_span.name, adm_ctrl_span.attributes, "AdmissionResult",
        adm_result)
    self.__assert_attr(adm_ctrl_span.name, adm_ctrl_span.attributes, "RequestPool",
        request_pool)

  def __assert_query_exec_attrs(self, spans, query_profile, root_span_id, query_id,
      err_msg, status):
    """
      Helper function that asserts the common and span-specific attributes in the
      query execution span.
    """

    query_exec_span = self.__find_span(spans, "QueryExecution", query_id)
    self.__assert_scopespan_common(query_exec_span, query_id, False, "QueryExecution", 3,
        status, root_span_id, err_msg)
    self.__assert_attr(query_exec_span.name, query_exec_span.attributes, "NumDeletedRows",
        0, "intValue")
    self.__assert_attr(query_exec_span.name, query_exec_span.attributes,
        "NumModifiedRows", 0, "intValue")
    self.__assert_attr(query_exec_span.name, query_exec_span.attributes, "NumRowsFetched",
        parse_num_rows_fetched(query_profile), "intValue")

  def __assert_close_attrs(self, spans, root_span_id, query_id,
      err_msg="", status=FINISHED):
    """
      Helper function that asserts the common and span-specific attributes in the
      close span.
    """

    close_span = self.__find_span(spans, "Close", query_id)
    self.__assert_scopespan_common(close_span, query_id, False, "Close", 0, status,
        root_span_id, err_msg)

  def __find_span(self, spans, name, query_id):
    """
      Helper function to find a span by name in a list of OtelSpan objects.
    """

    for s in spans:
      if s.name.endswith(name):
        return s

    assert False, "Span '{}' not found for query '{}'".format(name, query_id)
