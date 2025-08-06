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
import os
import sys

from time import sleep

from tests.common.environ import IMPALA_LOCAL_BUILD_VERSION

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
