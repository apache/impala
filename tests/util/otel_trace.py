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
import json
import logging
import os
import sys
from time import sleep

from tests.common.environ import IMPALA_LOCAL_BUILD_VERSION
from tests.common.file_utils import grep_file_first, wait_for_file_line_count
from tests.common.impala_connection import ERROR, INITIALIZED, PENDING
from tests.util.query_profile_util import (
    parse_admission_result,
    parse_coordinator,
    parse_db_user,
    parse_default_db,
    parse_impala_query_state,
    parse_num_deleted_rows,
    parse_num_modified_rows,
    parse_num_rows_fetched,
    parse_original_query_id,
    parse_query_status,
    parse_query_type,
    parse_retried_query_id,
    parse_retry_status,
    parse_session_id,
    parse_sql,
)

LOG = logging.getLogger(__name__)

# Valid types of OpenTelemetry attribute values.
ATTR_VAL_TYPE_STRING = "string"
ATTR_VAL_TYPE_INT = "int"
ATTR_VAL_TYPE_BOOL = "bool"


class AttributeValue:
  """
    Represents a value of an OpenTelemetry attribute. The key is not stored here, only
    the attribute value. Additionally, the class function get_type() returns the attribute
    value's type.
  """
  def __init__(self, attr):
    assert attr["value"] is not None, "Attribute missing value: {}".format(attr)

    v = attr["value"]
    if "stringValue" in v:
      self.value = v["stringValue"]
    elif "intValue" in v:
      self.value = int(v["intValue"])
    elif "boolValue" in v:
      self.value = v["boolValue"]
    else:
      raise Exception("Unsupported attribute value type: %s" % str(v))

  def get_type(self):
    if isinstance(self.value, bool):
      return ATTR_VAL_TYPE_BOOL
    elif isinstance(self.value, int):
      return ATTR_VAL_TYPE_INT
    else:
      return ATTR_VAL_TYPE_STRING

  def __str__(self):
      """
      Returns a string representation of the AttributeValue object.
      This method is called when print() is used on an instance of this class.
      """
      return "AttributeValue(type='{}', value='{}')".format(
          self.get_type(), str(self.value))


class OtelTrace():
  """
    Represents a single OpenTelemetry trace, which consists of a root span and zero or
    more child spans. Spans are represented by the OtelSpan class. Child spans cannot be
    parent spans to other child spans in this representation.

    Attributes:
      trace_id: The trace ID of this trace.
      root_span: The root span of this trace (an OtelSpan object).
      child_spans: A list of child spans (OtelSpan objects) that belong to this trace.
  """
  def __init__(self, trace_id):
    self.trace_id = trace_id
    self.root_span = None
    self.child_spans = []

  def __str__(self):
    """
    Returns a string representation of the OtelTrace object.
    This method is called when print() is used on an instance of this class.
    """
    s = "OtelTrace(trace_id='{}', root_span={}, child_spans=".format(
        self.trace_id, self.root_span)
    if len(self.child_spans) == 0:
      s += "[])"
    else:
      for cs in self.child_spans:
        s += "  {},\n".format(cs)
      s += "])"

    return s


class OtelSpan:
  """
    Represents a single OpenTelemetry span.

    Attributes:
      scope_name: The name of the scope that produced this span.
      scope_version: The version of the scope that produced this span.
      attributes: A dictionary of attribute key to AttributeValue object.
      events:     A dictionary of event name to event time.
      start_time: The start time of the span in nanoseconds since epoch.
      end_time: The end time of the span in nanoseconds since epoch.
      flags: The OpenTelemetry trace flags of the span (represented as an integer).
      kind: The OpenTelemetry kind of the span (integer).
      name: The OpenTelemetry name of the span.
      parent_span_id: The span ID of the parent span, or None if this is a root span.
      span_id: The span ID of this span.
      trace_id: The trace ID of this span.
      query_id: The query ID associated with this span. This value is extracted from the
                QueryId attribute. Until an attribute with that key is added via the
                add_attribute() method, this value is an empty string.
  """
  def __init__(self):
    self.scope_name = ""
    self.scope_version = ""
    self.attributes = {}
    self.events = {}
    self.start_time = 0
    self.end_time = 0
    self.flags = -1
    self.kind = -1
    self.name = ""
    self.parent_span_id = None
    self.span_id = ""
    self.trace_id = ""
    self.query_id = ""

  def is_root(self):
    return self.parent_span_id is None

  def add_attribute(self, key, value):
    if sys.version_info.major < 3:
      assert isinstance(key, unicode), "key must be a string"  # noqa: F821
      key = str(key)
    else:
      assert isinstance(key, str), "key must be a string"

    assert isinstance(value, AttributeValue), "Value must be an instance of " \
        "AttributeValue, got: {}".format(type(value))

    self.attributes[key] = value
    if key == "QueryId":
      self.query_id = value.value

  def add_event(self, name, time_unix_nano):
    if sys.version_info.major < 3:
      assert isinstance(name, unicode), "Event name must be a string"  # noqa: F821
      name = str(name)
      assert isinstance(time_unix_nano, unicode), \
          "Time value must be a string"  # noqa: F821
      time_unix_nano = str(time_unix_nano)
    else:
      assert isinstance(name, str), "Event name must be a string"
      assert isinstance(time_unix_nano, str), "Time value must be a string"

    try:
      self.events[name] = int(time_unix_nano)
    except ValueError:
        raise ValueError("Could not convert time_unix_nano '{}' to an integer"
            .format(time_unix_nano))

  def __str__(self):
    """
    Returns a string representation of the OtelSpan object.
    This method is called when print() is used on an instance of this class.
    """
    s = "OtelSpan(name='{}', span_id='{}', trace_id='{}', parent_span_id='{}', " \
        "start_time={}, end_time={}, kind={}, flags={}, scope_name='{}', " \
        "scope_version='{}', query_id='{}', attributes={{".format(
            self.name, self.span_id, self.trace_id, self.parent_span_id,
            self.start_time, self.end_time, self.kind, self.flags,
            self.scope_name, self.scope_version, self.query_id)
    for k in self.attributes:
      s += "\n    '{}': {},".format(k, self.attributes[k])
    for k in self.events:
      s += "\n    '{}': {},".format(k, self.events[k])
    s += "\n  })"

    return s


def __parse_attr(attr):
  """Internal helper to parse a single attribute from the json object representing it."""
  assert attr["key"] is not None, "Attribute missing key: {}".format(attr)
  return attr["key"], AttributeValue(attr)


def __parse_line(line):
  """Internal helper to parse a single line of the trace file, which is expected to be
     a json object representing one or more resource spans. Returns a list of OtelSpan
     objects parsed from the line.
  """
  obj = json.loads(line.strip())
  assert obj is not None, "Failed to parse line in json:\n{}".format(line)

  parsed_spans = []
  res_idx = -1
  scope_idx = -1
  span_idx = -1
  attr_idx = -1

  try:
    resource_spans = obj["resourceSpans"]

    # Expected resource span attribute keys/values.
    expected_resource_attrs = {
        "service.name": "Impala",
        "service.version": IMPALA_LOCAL_BUILD_VERSION,
        "telemetry.sdk.version": os.environ.get("IMPALA_OPENTELEMETRY_CPP_VERSION"),
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.language": "cpp"}

    # loop through each resource span
    for res_idx, res_span in enumerate(resource_spans):
      # Assert resource attributes.
      for attr in res_span["resource"]["attributes"]:
        k, v = __parse_attr(attr)
        expected_value = expected_resource_attrs.get(k)
        assert expected_value is not None, "Unexpected resource attribute key: '{}'" \
            .format(k)
        assert v.value == expected_value, "Unexpected value '{}' for resource " \
            "attribute '{}', expected '{}'".format(v.value, k, expected_value)

      # Parse each scope span.
      scope_spans = res_span["scopeSpans"]
      for scope_idx, scope_span in enumerate(scope_spans):
        scope_name = scope_span["scope"]["name"]
        scope_version = scope_span["scope"]["version"]

        # Parse each span.
        for span_idx, span in enumerate(scope_span["spans"]):
          s = OtelSpan()
          s.scope_name = scope_name
          s.scope_version = scope_version
          s.start_time = int(span["startTimeUnixNano"])
          s.end_time = int(span["endTimeUnixNano"])
          s.name = span["name"]
          s.flags = int(span["flags"])
          s.kind = int(span["kind"])
          s.span_id = span["spanId"]
          s.trace_id = span["traceId"]
          if "parentSpanId" in span:
            s.parent_span_id = span["parentSpanId"]

          # Parse each span attribute list.
          for attr_idx, attr in enumerate(span["attributes"]):
            key, value = __parse_attr(attr)
            s.add_attribute(key, value)

          # Parse each span event list.
          if "events" in span:
            for event in span["events"]:
              s.add_event(event["name"], event["timeUnixNano"])

          parsed_spans.append(s)
  except Exception as e:
    sys.stderr.write("Failed to parse json:\n{}".format(line))
    sys.stderr.write("Resource Span Index: {}\n".format(res_idx))
    sys.stderr.write("Scope Span Index: {}\n".format(scope_idx))
    sys.stderr.write("Span Index: {}\n".format(span_idx))
    sys.stderr.write("Attribute Index: {}\n".format(attr_idx))
    sys.stderr.flush()
    raise e

  return parsed_spans


def parse_trace_file(file_path, query_id):
  """
    Parses the OpenTelemetry trace file located at 'file_path' and returns the OtelTrace
    object for the trace that contains the given 'query_id'. Fails an assertion if no
    trace with the given query ID is found, or if the trace does not have a root span.
  """
  traces_by_trace_id = {}
  traces_by_query_id = {}
  parsed_spans = []

  max_retries = 3
  retry_count = 0

  while retry_count < max_retries:
    try:
      with open(file_path, "r") as f:
        lines = f.readlines()
        for line in lines:
          if not line.endswith('\n'):
            # Line does not end with a newline, thus the entire trace has not yet been
            # written to the file. Retry by restarting the loop
            parsed_spans = []
            retry_count += 1
            print("Line doesn't end with newline, retrying (attempt {} of {})"
                .format(retry_count, max_retries))
            sleep(1)
            break
          parsed_spans.extend(__parse_line(line))
        else:
          # Successfully read all lines, exit the retry loop.
          break
    except Exception as e:
      retry_count += 1
      if retry_count >= max_retries:
        raise
      print("Error reading trace file, retrying (attempt {} of {}): {}"
          .format(retry_count, max_retries, e))

  # Build a map of query_id -> OtelTrace for easy lookup.
  # First, locate all root spans
  for s in parsed_spans:
    if s.trace_id not in traces_by_trace_id:
      traces_by_trace_id[s.trace_id] = OtelTrace(s.trace_id)

    if s.is_root():
      traces_by_trace_id[s.trace_id].root_span = s
      traces_by_query_id[s.query_id] = traces_by_trace_id[s.trace_id]
    else:
      traces_by_trace_id[s.trace_id].child_spans.append(s)

  assert len(traces_by_query_id) > 0, "No root span(s) in the file: {}".format(file_path)
  assert query_id in traces_by_query_id, "Could not find trace for query: {}" \
      .format(query_id)

  query_trace = traces_by_query_id[query_id]
  assert query_trace is not None, "Trace was None for query: {}".format(query_id)
  assert query_trace.root_span is not None, "Trace for query '{}' has no root span" \
      .format(query_id)

  return query_trace


def assert_trace(log_file_path, trace_file_path, trace_file_count, query_id,
    query_profile, cluster_id, trace_cnt=1, err_span="", missing_spans=[],
    async_close=False, exact_trace_cnt=False):
  # Parse common values needed in multiple asserts.
  session_id = parse_session_id(query_profile)
  db_user = parse_db_user(query_profile)

  # Wait until all spans are written to the trace file.
  wait_for_file_line_count(
      file_path=trace_file_path,
      expected_line_count=trace_cnt + trace_file_count,
      max_attempts=60,
      sleep_time_s=1,
      backoff=1,
      exact_match=exact_trace_cnt)

  # Remove missing spans from the expected span count.
  expected_span_count = 6 - len(missing_spans)

  # Parse the trace json files to get the trace for the query.
  trace = parse_trace_file(trace_file_path, query_id)
  __assert_trace_common(trace, expected_span_count)

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
  root_span_id = __assert_rootspan_attrs(trace.root_span, query_id, session_id,
    cluster_id, db_user, "default-pool", impala_query_state, query_status,
    original_query_id, retried_query_id, coordinator, log_file_path)

  # Assert Init span.
  if "Init" not in missing_spans:
    span_err_msg = ""
    if err_span == "Init":
      span_err_msg = query_status
      in_error = True
    __assert_initspan_attrs(trace.child_spans, root_span_id, query_id, session_id,
        cluster_id, db_user, "default-pool", parse_default_db(query_profile),
        parse_sql(query_profile).replace('\n', ' '), original_query_id, coordinator,
        log_file_path)

  # Assert Submitted span.
  if "Submitted" not in missing_spans:
    span_err_msg = ""
    if err_span == "Submitted" or in_error:
      span_err_msg = query_status
      in_error = True
    __assert_submittedspan_attrs(trace.child_spans, root_span_id, query_id, log_file_path)

  # Assert Planning span.
  if "Planning" not in missing_spans:
    status = INITIALIZED
    span_err_msg = ""
    if err_span == "Planning" or in_error:
      span_err_msg = query_status
      status = ERROR
      in_error = True
    __assert_planningspan_attrs(trace.child_spans, root_span_id, query_id,
        query_type, span_err_msg, status, log_file_path)

  # Assert AdmissionControl span.
  if "AdmissionControl" not in missing_spans:
    status = PENDING
    span_err_msg = ""
    if err_span == "AdmissionControl" or in_error:
      span_err_msg = query_status
      status = ERROR
      in_error = True
    __assert_admissioncontrol_attrs(trace.child_spans, root_span_id, query_id,
      "default-pool", parse_admission_result(query_profile), span_err_msg, status,
      log_file_path)

  # Assert QueryExecution span.
  if "QueryExecution" not in missing_spans:
    span_err_msg = ""
    if err_span == "QueryExecution" or in_error:
      span_err_msg = query_status
      in_error = True
    __assert_query_exec_attrs(trace.child_spans, query_profile, root_span_id,
        query_id, span_err_msg, parse_impala_query_state(query_profile), log_file_path)

  # Assert Close span.
  if "Close" not in missing_spans:
    span_err_msg = ""
    if err_span == "Close" or in_error:
      span_err_msg = query_status
      in_error = True
    __assert_close_attrs(trace.child_spans, root_span_id, query_id, span_err_msg,
        parse_impala_query_state(query_profile), async_close, log_file_path)


def __assert_trace_common(trace, expected_child_spans_count):
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


def __assert_scopespan_common(span, query_id, is_root, name, attributes_count,
      status, log_file_path, root_span_id=None, err_msg=""):
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
  expected_span_id, expected_trace_id = __find_span_log(log_file_path, name, query_id)

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
    __assert_attr(expected_name, span.attributes, "ErrorMessage", err_msg)
  else:
    __assert_attr(expected_name, span.attributes, "ErrorMsg", err_msg)
    __assert_attr(expected_name, span.attributes, "Name", expected_name)
    __assert_attr(expected_name, span.attributes, "Running",
      name == "QueryExecution", "boolValue")
    __assert_attr(expected_name, span.attributes, "Status", status)


def __find_span_log(log_file_path, span_name, query_id):
  """
    Finds the start span log entry for the given span name and query id in the Impalad
    logs. This log line contains the trace id and span id for the span which are used
    as the expected values when asserting the span properties in the trace file.
  """
  span_regex = r'Started \'{}\' span trace_id="(.*?)" span_id="(.*?)" query_id="{}"' \
      .format(span_name, query_id)

  max_retries = 10
  retry_count = 0

  LOG.info("Searching for span log entry for span '{}' for query '{}' in log file '{}'"
      .format(span_name, query_id, log_file_path))
  while retry_count < max_retries:
    with open(log_file_path, "r") as f:
      span_log = grep_file_first(f, span_regex)
      if span_log is not None:
        return span_log.group(2), span_log.group(1)

    retry_count += 1
    sleep(1)

  raise Exception("Exceeded maximum retries to find span log entry for span '{}' "
      "and query '{}'".format(span_name, query_id))


def __assert_attr(span_name, attributes, expected_key, expected_value,
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


def __assert_span_events(span, expected_events=[]):
  """
    Helper function to assert that a span contains the expected span events.
  """
  assert len(expected_events) == len(span.events), "Span '{}' expected to have " \
      "exactly {} events, actual: {}".format(span.name, len(expected_events),
      len(span.events))

  for event in expected_events:
    assert event in span.events, "Expected '{}' event on span '{}' but " \
      "no such events was found.".format(event, span.name)


def __assert_rootspan_attrs(span, query_id, session_id, cluster_id, user_name,
    request_pool, state, err_msg, original_query_id, retried_query_id, coordinator,
    log_file_path):
  """
    Helper function that asserts the common attributes in the root span.
  """

  root_span_id, _ = __find_span_log(log_file_path, "Root", query_id)
  __assert_scopespan_common(span, query_id, True, "Root", 14, "", log_file_path, None,
      err_msg)

  __assert_attr(span.name, span.attributes, "QueryId", query_id)
  __assert_attr(span.name, span.attributes, "SessionId", session_id)
  __assert_attr(span.name, span.attributes, "ClusterId", cluster_id)
  __assert_attr(span.name, span.attributes, "UserName", user_name)
  __assert_attr(span.name, span.attributes, "RequestPool", request_pool)
  __assert_attr(span.name, span.attributes, "State", state)
  __assert_attr(span.name, span.attributes, "OriginalQueryId", original_query_id)
  __assert_attr(span.name, span.attributes, "RetriedQueryId", retried_query_id)
  __assert_attr(span.name, span.attributes, "Coordinator", coordinator)

  return root_span_id


def __assert_initspan_attrs(spans, root_span_id, query_id, session_id, cluster_id,
    user_name, request_pool, default_db, query_string, original_query_id, coordinator,
    log_file_path):
  """
    Helper function that asserts the common and span-specific attributes in the
    init span.
  """

  # Locate the init span and assert.
  init_span = __find_span(spans, "Init", query_id)

  __assert_scopespan_common(init_span, query_id, False, "Init", 9, INITIALIZED,
      log_file_path, root_span_id)

  __assert_attr(init_span.name, init_span.attributes, "QueryId", query_id)
  __assert_attr(init_span.name, init_span.attributes, "SessionId", session_id)
  __assert_attr(init_span.name, init_span.attributes, "ClusterId", cluster_id)
  __assert_attr(init_span.name, init_span.attributes, "UserName", user_name)
  __assert_attr(init_span.name, init_span.attributes, "RequestPool", request_pool)
  __assert_attr(init_span.name, init_span.attributes, "DefaultDb", default_db)
  __assert_attr(init_span.name, init_span.attributes, "QueryString", query_string)
  __assert_attr(init_span.name, init_span.attributes, "OriginalQueryId",
      original_query_id)
  __assert_attr(init_span.name, init_span.attributes, "Coordinator", coordinator)

  __assert_span_events(init_span)


def __assert_submittedspan_attrs(spans, root_span_id, query_id, log_file_path):
  """
    Helper function that asserts the common attributes in the submitted span.
  """

  submitted_span = __find_span(spans, "Submitted", query_id)
  __assert_scopespan_common(submitted_span, query_id, False, "Submitted", 0, INITIALIZED,
      log_file_path, root_span_id)

  __assert_span_events(submitted_span)


def __assert_planningspan_attrs(spans, root_span_id, query_id, query_type, err_msg,
    status, log_file_path):
  """
    Helper function that asserts the common and span-specific attributes in the
    planning execution span.
  """

  planning_span = __find_span(spans, "Planning", query_id)
  __assert_scopespan_common(planning_span, query_id, False, "Planning", 1, status,
      log_file_path, root_span_id, err_msg)
  __assert_attr(planning_span.name, planning_span.attributes, "QueryType", query_type)

  __assert_span_events(planning_span)


def __assert_admissioncontrol_attrs(spans, root_span_id, query_id, request_pool,
    adm_result, err_msg, status, log_file_path):
  """
    Helper function that asserts the common and span-specific attributes in the
    admission control span.
  """

  queued = False if adm_result == "Admitted immediately" \
      or adm_result == "Admitted as a trivial query" else True

  adm_ctrl_span = __find_span(spans, "AdmissionControl", query_id)
  __assert_scopespan_common(adm_ctrl_span, query_id, False, "AdmissionControl", 3, status,
      log_file_path, root_span_id, err_msg)
  __assert_attr(adm_ctrl_span.name, adm_ctrl_span.attributes, "Queued", queued,
      "boolValue")
  __assert_attr(adm_ctrl_span.name, adm_ctrl_span.attributes, "AdmissionResult",
      adm_result)
  __assert_attr(adm_ctrl_span.name, adm_ctrl_span.attributes, "RequestPool", request_pool)

  if queued:
    __assert_span_events(adm_ctrl_span, ["Queued"])
  else:
    __assert_span_events(adm_ctrl_span)


def __assert_query_exec_attrs(spans, query_profile, root_span_id, query_id,
    err_msg, status, log_file_path):
  """
    Helper function that asserts the common and span-specific attributes in the
    query execution span.
  """

  query_exec_span = __find_span(spans, "QueryExecution", query_id)
  __assert_scopespan_common(query_exec_span, query_id, False, "QueryExecution", 3, status,
      log_file_path, root_span_id, err_msg)
  __assert_attr(query_exec_span.name, query_exec_span.attributes, "NumModifiedRows",
      parse_num_modified_rows(query_profile), "intValue")
  __assert_attr(query_exec_span.name, query_exec_span.attributes, "NumDeletedRows",
      parse_num_deleted_rows(query_profile), "intValue")
  __assert_attr(query_exec_span.name, query_exec_span.attributes, "NumRowsFetched",
      parse_num_rows_fetched(query_profile), "intValue")

  # TODO: IMPALA-14334 - Assert QueryExecution span events


def __assert_close_attrs(spans, root_span_id, query_id, err_msg, status, async_close,
    log_file_path):
  """
    Helper function that asserts the common and span-specific attributes in the
    close span.
  """

  close_span = __find_span(spans, "Close", query_id)
  __assert_scopespan_common(close_span, query_id, False, "Close", 0, status,
      log_file_path, root_span_id, err_msg)

  expected_events = ["QueryUnregistered"]
  if async_close and "ReleasedAdmissionControlResources" in close_span.events:
    expected_events.append("ReleasedAdmissionControlResources")

  # TODO: IMPALA-14334 - Assert Close span events


def __find_span(spans, name, query_id):
  """
    Helper function to find a span by name in a list of OtelSpan objects.
  """

  for s in spans:
    if s.name.endswith(name):
      return s

  assert False, "Span '{}' not found for query '{}'".format(name, query_id)
