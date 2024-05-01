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

import re
import requests

from datetime import datetime
from tests.util.assert_time import assert_time_str, convert_to_milliseconds
from tests.util.memory import assert_byte_str, convert_to_bytes

DEDICATED_COORD_SAFETY_BUFFER_BYTES = 104857600
EXPECTED_QUERY_COLS = 49

CLUSTER_ID = "CLUSTER_ID"
QUERY_ID = "QUERY_ID"
SESSION_ID = "SESSION_ID"
SESSION_TYPE = "SESSION_TYPE"
HIVESERVER2_PROTOCOL_VERSION = "HIVESERVER2_PROTOCOL_VERSION"
DB_USER = "DB_USER"
DB_USER_CONNECTION = "DB_USER_CONNECTION"
DB_NAME = "DB_NAME"
IMPALA_COORDINATOR = "IMPALA_COORDINATOR"
QUERY_STATUS = "QUERY_STATUS"
QUERY_STATE = "QUERY_STATE"
IMPALA_QUERY_END_STATE = "IMPALA_QUERY_END_STATE"
QUERY_TYPE = "QUERY_TYPE"
NETWORK_ADDRESS = "NETWORK_ADDRESS"
START_TIME_UTC = "START_TIME_UTC"
TOTAL_TIME_MS = "TOTAL_TIME_MS"
QUERY_OPTS_CONFIG = "QUERY_OPTS_CONFIG"
RESOURCE_POOL = "RESOURCE_POOL"
PER_HOST_MEM_ESTIMATE = "PER_HOST_MEM_ESTIMATE"
DEDICATED_COORD_MEM_ESTIMATE = "DEDICATED_COORD_MEM_ESTIMATE"
PER_HOST_FRAGMENT_INSTANCES = "PER_HOST_FRAGMENT_INSTANCES"
BACKENDS_COUNT = "BACKENDS_COUNT"
ADMISSION_RESULT = "ADMISSION_RESULT"
CLUSTER_MEMORY_ADMITTED = "CLUSTER_MEMORY_ADMITTED"
EXECUTOR_GROUP = "EXECUTOR_GROUP"
EXECUTOR_GROUPS = "EXECUTOR_GROUPS"
EXEC_SUMMARY = "EXEC_SUMMARY"
NUM_ROWS_FETCHED = "NUM_ROWS_FETCHED"
ROW_MATERIALIZATION_ROWS_PER_SEC = "ROW_MATERIALIZATION_ROWS_PER_SEC"
ROW_MATERIALIZATION_TIME_MS = "ROW_MATERIALIZATION_TIME_MS"
COMPRESSED_BYTES_SPILLED = "COMPRESSED_BYTES_SPILLED"
EVENT_PLANNING_FINISHED = "EVENT_PLANNING_FINISHED"
EVENT_SUBMIT_FOR_ADMISSION = "EVENT_SUBMIT_FOR_ADMISSION"
EVENT_COMPLETED_ADMISSION = "EVENT_COMPLETED_ADMISSION"
EVENT_ALL_BACKENDS_STARTED = "EVENT_ALL_BACKENDS_STARTED"
EVENT_ROWS_AVAILABLE = "EVENT_ROWS_AVAILABLE"
EVENT_FIRST_ROW_FETCHED = "EVENT_FIRST_ROW_FETCHED"
EVENT_LAST_ROW_FETCHED = "EVENT_LAST_ROW_FETCHED"
EVENT_UNREGISTER_QUERY = "EVENT_UNREGISTER_QUERY"
READ_IO_WAIT_TOTAL_MS = "READ_IO_WAIT_TOTAL_MS"
READ_IO_WAIT_MEAN_MS = "READ_IO_WAIT_MEAN_MS"
BYTES_READ_CACHE_TOTAL = "BYTES_READ_CACHE_TOTAL"
BYTES_READ_TOTAL = "BYTES_READ_TOTAL"
PERNODE_PEAK_MEM_MIN = "PERNODE_PEAK_MEM_MIN"
PERNODE_PEAK_MEM_MAX = "PERNODE_PEAK_MEM_MAX"
PERNODE_PEAK_MEM_MEAN = "PERNODE_PEAK_MEM_MEAN"
SQL = "SQL"
PLAN = "PLAN"
TABLES_QUERIED = "TABLES_QUERIED"


def round_to_3(val):
  # The differences between round in Python 2 and Python 3 do not matter here.
  # pylint: disable=round-builtin
  return round(val, 3)


def assert_query(query_tbl, client, expected_cluster_id, raw_profile=None, impalad=None,
    query_id=None, max_mem_for_admission=None, max_row_size=None):
  """Helper function to assert that the values in the completed query log table
      match the values from the query profile."""

  ret_data = {}

  # If query_id was specified, read the profile from the Impala webserver.
  if query_id is not None:
    assert impalad is not None
    assert raw_profile is None, "cannot specify both query_id and raw_profile"
    resp = requests.get("http://{0}:{1}/query_profile_plain_text?query_id={2}"
        .format(impalad.hostname, impalad.get_webserver_port(), query_id))
    assert resp.status_code == 200, "Response code was: {0}".format(resp.status_code)
    profile_text = resp.text
  else:
    profile_text = raw_profile
    assert query_id is None, "cannot specify both raw_profile and query_id"
    match = re.search(r'Query \(id=(.*?)\)', profile_text)
    assert match is not None
    query_id = match.group(1)

  print("Query Id: {0}".format(query_id))
  profile_lines = profile_text.split("\n")

  # Force Impala to process the inserts to the completed queries table.
  if query_tbl != 'sys.impala_query_live':
    client.execute("refresh " + query_tbl)

  # Assert the query was written correctly to the query log table.
  if max_row_size is not None:
    client.set_configuration_option("MAX_ROW_SIZE", max_row_size)
  sql_results = client.execute("select * from {0} where query_id='{1}'".format(
      query_tbl, query_id))
  assert sql_results.success
  assert len(sql_results.data) == 1, "did not find query in completed queries table"

  # Assert the expected columns were included.
  assert len(sql_results.data) == 1
  assert len(sql_results.column_labels) == EXPECTED_QUERY_COLS
  data = sql_results.data[0].split("\t")
  assert len(data) == len(sql_results.column_labels)

  # Cluster ID
  index = 0
  assert sql_results.column_labels[index] == CLUSTER_ID
  ret_data[CLUSTER_ID] = data[index]
  assert data[index] == expected_cluster_id, "cluster id incorrect"

  # Query ID
  index += 1
  assert sql_results.column_labels[index] == QUERY_ID
  ret_data[QUERY_ID] = data[index]
  assert data[index] == query_id

  # Session ID
  index += 1
  assert sql_results.column_labels[index] == SESSION_ID
  ret_data[SESSION_ID] = data[index]
  session_id = re.search(r'\n\s+Session ID:\s+(.*)\n', profile_text)
  assert session_id is not None
  assert data[index] == session_id.group(1), "session id incorrect"

  # Session Type
  index += 1
  assert sql_results.column_labels[index] == SESSION_TYPE
  ret_data[SESSION_TYPE] = data[index]
  session_type = re.search(r'\n\s+Session Type:\s+(.*)\n', profile_text)
  assert session_type is not None
  assert data[index] == session_type.group(1), "session type incorrect"

  # HS2 Protocol Version
  index += 1
  assert sql_results.column_labels[index] == HIVESERVER2_PROTOCOL_VERSION
  ret_data[HIVESERVER2_PROTOCOL_VERSION] = data[index]
  if session_type.group(1) == "HIVESERVER2":
    hs2_ver = re.search(r'\n\s+HiveServer2 Protocol Version:\s+(.*)', profile_text)
    assert hs2_ver is not None
    assert data[index] == "HIVE_CLI_SERVICE_PROTOCOL_{0}".format(hs2_ver.group(1))
  else:
    assert data[index] == ""

  # Database User
  index += 1
  assert sql_results.column_labels[index] == DB_USER
  ret_data[DB_USER] = data[index]
  user = re.search(r'\n\s+User:\s+(.*?)\n', profile_text)
  assert user is not None
  assert data[index] == user.group(1), "db user incorrect"

  # Connected Database User
  index += 1
  assert sql_results.column_labels[index] == DB_USER_CONNECTION
  ret_data[DB_USER_CONNECTION] = data[index]
  db_user = re.search(r'\n\s+Connected User:\s+(.*?)\n', profile_text)
  assert db_user is not None
  assert data[index] == db_user.group(1), "db user connection incorrect"

  # Database Name
  index += 1
  assert sql_results.column_labels[index] == DB_NAME
  ret_data[DB_NAME] = data[index]
  default_db = re.search(r'\n\s+Default Db:\s+(.*?)\n', profile_text)
  assert default_db is not None
  assert data[index] == default_db.group(1), "database name incorrect"

  # Coordinator
  index += 1
  assert sql_results.column_labels[index] == IMPALA_COORDINATOR
  ret_data[IMPALA_COORDINATOR] = data[index]
  coordinator = re.search(r'\n\s+Coordinator:\s+(.*?)\n', profile_text)
  assert coordinator is not None
  assert data[index] == coordinator.group(1), "impala coordinator incorrect"

  # Query Status (can be multiple lines if the query errored)
  index += 1
  assert sql_results.column_labels[index] == QUERY_STATUS
  ret_data[QUERY_STATUS] = data[index]
  query_status = re.search(r'\n\s+Query Status:\s+(.*?)\n\s+Impala Version', profile_text,
      re.DOTALL)
  assert query_status is not None
  assert data[index] == query_status.group(1), "query status incorrect"

  # Query State
  index += 1
  assert sql_results.column_labels[index] == QUERY_STATE
  ret_data[QUERY_STATE] = data[index]
  query_state = re.search(r'\n\s+Query State:\s+(.*?)\n', profile_text)
  assert query_state is not None
  query_state_value = query_state.group(1)
  assert data[index] == query_state_value, "query state incorrect"

  # Impala Query End State
  index += 1
  assert sql_results.column_labels[index] == IMPALA_QUERY_END_STATE
  ret_data[IMPALA_QUERY_END_STATE] = data[index]
  impala_query_state = re.search(r'\n\s+Impala Query State:\s+(.*?)\n', profile_text)
  assert impala_query_state is not None
  assert data[index] == impala_query_state.group(1), "impala query end state incorrect"

  # Query Type
  index += 1
  assert sql_results.column_labels[index] == QUERY_TYPE
  ret_data[QUERY_TYPE] = data[index]
  if query_state_value == "EXCEPTION":
    assert data[index] == "UNKNOWN", "query type incorrect"
  else:
    query_type = re.search(r'\n\s+Query Type:\s+(.*?)\n', profile_text)
    assert query_type is not None
    assert data[index] == query_type.group(1), "query type incorrect"
    query_type = query_type.group(1)

  # Client Network Address
  index += 1
  assert sql_results.column_labels[index] == NETWORK_ADDRESS
  ret_data[NETWORK_ADDRESS] = data[index]
  network_address = re.search(r'\n\s+Network Address:\s+(.*?)\n', profile_text)
  assert network_address is not None
  assert data[index] == network_address.group(1), "network address incorrect"

  # offset from UTC
  utc_now = datetime.utcnow().replace(microsecond=0, second=0)
  local_now = datetime.now().replace(microsecond=0, second=0)
  utc_offset = utc_now - local_now

  # Start Time
  index += 1
  assert sql_results.column_labels[index] == START_TIME_UTC
  ret_data[START_TIME_UTC] = data[index]
  start_time = re.search(r'\n\s+Start Time:\s+(.*?)\n', profile_text)
  assert start_time is not None
  start_time_obj = datetime.strptime(start_time.group(1)[:-3], "%Y-%m-%d %H:%M:%S.%f")
  start_time_obj_utc = start_time_obj + utc_offset
  assert data[index][:-3] == start_time_obj_utc.strftime("%Y-%m-%d %H:%M:%S.%f"), \
      "start time incorrect"

  # End Time (not in table, but needed for duration calculation)
  end_time = re.search(r'\n\s+End Time:\s+(.*?)\n', profile_text)
  assert end_time is not None
  end_time_obj = datetime.strptime(end_time.group(1)[:-3], "%Y-%m-%d %H:%M:%S.%f")

  # Query Duration (allow values that are within 1 second)
  index += 1
  assert sql_results.column_labels[index] == TOTAL_TIME_MS
  ret_data[TOTAL_TIME_MS] = data[index]
  duration = end_time_obj - start_time_obj
  min_allowed = round_to_3(duration.total_seconds() * 1000 * 0.999)
  max_allowed = round_to_3(duration.total_seconds() * 1000 * 1.001)
  assert min_allowed <= float(data[index]) <= max_allowed, "total time incorrect"

  # Query Options Set By Configuration
  index += 1
  assert sql_results.column_labels[index] == QUERY_OPTS_CONFIG
  ret_data[QUERY_OPTS_CONFIG] = data[index]
  if query_state_value == "EXCEPTION":
    assert data[index] != "", "query options set by config incorrect"
  else:
    query_opts = re.search(r'\n\s+Query Options \(set by configuration\):\s+(.*?)\n',
        profile_text)
    assert query_opts is not None
    assert data[index] == query_opts.group(1), "query opts set by config incorrect"

  # Resource Pool
  index += 1
  assert sql_results.column_labels[index] == RESOURCE_POOL
  ret_data[RESOURCE_POOL] = data[index]
  if query_state_value == "EXCEPTION":
    assert data[index] == "", "resource pool incorrect"
  else:
    if query_type != "DDL":
      req_pool = re.search(r'\n\s+Request Pool:\s+(.*?)\n', profile_text)
      assert req_pool is not None
      assert data[index] == req_pool.group(1), "request pool incorrect"
    else:
      assert data[index] == "", "request pool not empty"

  # Per-host Memory Estimate
  index += 1
  assert sql_results.column_labels[index] == PER_HOST_MEM_ESTIMATE
  ret_data[PER_HOST_MEM_ESTIMATE] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert data[index] == "0", "per-host memory estimate incorrect"
  else:
    # First check the Estimated Per-Host Mem from the query profile. This value may not
    # match though because certain query options can cause this value to diverge from the
    # per-host memory estimate stored in the query history table.
    est_perhost_mem = re.search(r'\n\s+Estimated Per-Host Mem:\s+(\d+)\n', profile_text)
    assert est_perhost_mem is not None
    if est_perhost_mem.group(1) != data[index]:
      # The profile and db values diverged, use the Per-Host Resource Estimates field from
      # the query profile as the expected value. Since query profile value is an estimate,
      # it's not as good to use, but it's all we have available.
      perhost_mem_est = re.search(r'\nPer-Host Resource Estimates:\s+Memory\=(.*?)\n',
          profile_text)
      assert perhost_mem_est is not None
      assert_byte_str(expected_str=perhost_mem_est.group(1), actual_bytes=data[index],
          msg="per-host memory estimate incorrect", unit_combined=True)

  # Dedicated Coordinator Memory Estimate
  # This value is different because it is the minimum of the query option
  # MAX_MEM_ESTIMATE_FOR_ADMISSION or a calculation that includes a 100mb buffer.
  # Thus, callers must specify if the query being asserted had that option set.
  index += 1
  assert sql_results.column_labels[index] == DEDICATED_COORD_MEM_ESTIMATE
  ret_data[DEDICATED_COORD_MEM_ESTIMATE] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert data[index] == "0", "dedicated coordinator memory estimate incorrect"
  elif query_type == "DML":
    assert data[index] == str(DEDICATED_COORD_SAFETY_BUFFER_BYTES), \
        "dedicated coordinator memory estimate incorrect"
  else:
    if max_mem_for_admission is not None:
      # The MAX_MEM_ESTIMATE_FOR_ADMISSION query option was specified, thus that should
      # be the value that was written to the database.
      assert str(max_mem_for_admission) == data[index], \
          "dedicated coordinator memory estimate incorrect"
    else:
      root_mem = re.search(r'\n\nF\d+:PLAN FRAGMENT.*?mem-estimate=(\S+?) mem',
          profile_text, re.DOTALL)
      assert root_mem is not None, "dedicated coordinator memory estimate incorrect"
      buffer = DEDICATED_COORD_SAFETY_BUFFER_BYTES
      assert_byte_str(expected_str=root_mem.group(1),
          actual_bytes=int(data[index]) - buffer,
          msg="dedicated coordinator memory estimate incorrect", unit_combined=True)

  # Per-Host Fragment Instances
  index += 1
  assert sql_results.column_labels[index] == PER_HOST_FRAGMENT_INSTANCES
  ret_data[PER_HOST_FRAGMENT_INSTANCES] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert data[index] == "", "per-host fragment instances incorrect"
  else:
    perhost_frags = re.search(r'\n\s+Per Host Number of Fragment Instances:\s+(.*?)\n',
        profile_text)
    assert perhost_frags is not None
    expected = ",".join(sorted(perhost_frags.group(1).replace("(", "=")
        .replace(")", "").split(" ")))
    assert data[index] == expected, ('per-host fragment instances incorrect.'
        ' expected="{0}" actual="{1}"').format(expected, data[index])

  # Backends Count
  index += 1
  assert sql_results.column_labels[index] == BACKENDS_COUNT
  ret_data[BACKENDS_COUNT] = data[index]
  num_bck = re.search(r'\n\s+\- NumBackends:\s+(\d+)', profile_text)
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert num_bck is None
    assert data[index] == "0", "backends count incorrect"
  else:
    assert num_bck is not None
    assert data[index] == num_bck.group(1), "backends count incorrect"

  # Admission Result
  index += 1
  assert sql_results.column_labels[index] == ADMISSION_RESULT
  ret_data[ADMISSION_RESULT] = data[index]
  adm_result = re.search(r'\n\s+Admission result:\s+(.*?)\n', profile_text)
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert adm_result is None
    assert data[index] == "", "admission result incorrect"
  else:
    assert adm_result is not None
    assert data[index] == adm_result.group(1), "admission result incorrect"

  # Cluster Memory Admitted
  index += 1
  assert sql_results.column_labels[index] == CLUSTER_MEMORY_ADMITTED
  ret_data[CLUSTER_MEMORY_ADMITTED] = data[index]
  clust_mem = re.search(r'\n\s+Cluster Memory Admitted:\s+(.*?)\n', profile_text)
  if query_state_value == "EXCEPTION":
    assert clust_mem is None
  else:
    if query_type != "DDL":
      assert clust_mem is not None
      assert_byte_str(expected_str=clust_mem.group(1), actual_bytes=data[index],
          msg="cluster memory admitted incorrect")
    else:
      assert data[index] == "0", "cluster memory not zero"

  # Executor Group
  index += 1
  assert sql_results.column_labels[index] == EXECUTOR_GROUP
  ret_data[EXECUTOR_GROUP] = data[index]
  exec_group = re.search(r'\n\s+Executor Group:\s+(.*?)\n', profile_text)
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert exec_group is None
    assert data[index] == "", "executor group should not have been found"
  else:
    assert exec_group is not None
    assert data[index] == exec_group.group(1), "executor group incorrect"

  # Executor Groups
  index += 1
  assert sql_results.column_labels[index] == EXECUTOR_GROUPS
  ret_data[EXECUTOR_GROUPS] = data[index]
  exec_groups = re.search(r'\n\s+(Executor group \d+:.*?)\n\s+ImpalaServer', profile_text,
      re.DOTALL)
  if query_state_value == "EXCEPTION":
    assert exec_groups is None, "executor groups should not have been found"
  else:
    assert exec_groups is not None
    dedent_str = re.sub(r'^\s{6}', '', exec_groups.group(1), flags=re.MULTILINE)
    assert data[index] == dedent_str, "executor groups incorrect"

  # Exec Summary
  index += 1
  assert sql_results.column_labels[index] == EXEC_SUMMARY
  ret_data[EXEC_SUMMARY] = data[index]
  exec_sum = re.search(r'\n\s+ExecSummary:\s*\n(.*)\n\s+Errors', profile_text, re.DOTALL)
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert exec_sum is None
    assert data[index] == ""
  else:
    assert exec_sum is not None
    assert data[index] == exec_sum.group(1)

  # Rows Fetched
  index += 1
  assert sql_results.column_labels[index] == NUM_ROWS_FETCHED
  ret_data[NUM_ROWS_FETCHED] = data[index]
  rows_fetched = re.search(r'\n\s+\-\s+NumRowsFetched:\s+\S+\s+\((\d+)\)', profile_text)
  if query_state_value == "EXCEPTION":
    assert rows_fetched is None
  else:
    assert rows_fetched is not None
    assert data[index] == rows_fetched.group(1)

  # Row Materialization Rate
  index += 1
  assert sql_results.column_labels[index] == ROW_MATERIALIZATION_ROWS_PER_SEC
  ret_data[ROW_MATERIALIZATION_ROWS_PER_SEC] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DDL" or query_type == 'DML':
    assert data[index] == "0", "row materialization rate incorrect"
  else:
    row_mat = re.search(r'\n\s+\-\s+RowMaterializationRate:\s+(\S+)\s+([MK])?',
        profile_text)
    assert row_mat is not None
    tolerance = int(data[index]) * 0.005
    expected_row_mat = 0
    if row_mat.group(2) == "K":
      expected_row_mat = int(float(row_mat.group(1)) * 1000)
    elif row_mat.group(2) == "M":
      expected_row_mat = int(float(row_mat.group(1)) * 1000000)
    else:
      expected_row_mat = int(float(row_mat.group(1)))
    assert expected_row_mat - tolerance <= int(data[index]) \
        <= expected_row_mat + tolerance, "row materialization rate incorrect"

  # Row Materialization Time
  index += 1
  assert sql_results.column_labels[index] == ROW_MATERIALIZATION_TIME_MS
  ret_data[ROW_MATERIALIZATION_TIME_MS] = data[index]
  row_mat_tmr = re.search(r'\n\s+\-\s+RowMaterializationTimer:\s+(.*?)\n', profile_text)
  if query_state_value == "EXCEPTION":
    assert row_mat_tmr is None
  elif query_type == "DDL" or query_type == 'DML':
    assert row_mat_tmr is not None
    assert row_mat_tmr.group(1) == "0.000ns", "row materialization timer incorrect"
  else:
    assert row_mat_tmr is not None
    assert_time_str(row_mat_tmr.group(1), data[index],
        "row materialization time incorrect")

  # Compressed Bytes Spilled
  index += 1
  assert sql_results.column_labels[index] == COMPRESSED_BYTES_SPILLED
  ret_data[COMPRESSED_BYTES_SPILLED] = data[index]
  scratch_bytes_total = 0
  for sbw in re.findall(r'\n\s+\-\s+ScratchBytesWritten:.*?\((\d+)\)', profile_text):
    scratch_bytes_total += int(sbw)
  assert int(data[index]) == scratch_bytes_total

  # Parse out only the query timeline.
  timeline = re.search(r'\n\s+Query Timeline:(.*?)\n\s+Frontend', profile_text, re.DOTALL)
  assert timeline is not None, "query timeline not found"
  timeline = timeline.group(1)

  # Event Timeline Planning Finished
  index += 1
  assert sql_results.column_labels[index] == EVENT_PLANNING_FINISHED
  ret_data[EVENT_PLANNING_FINISHED] = data[index]
  if query_state_value == "EXCEPTION":
    assert data[index] == "0.000", "planning finished event incorrect"
  else:
    event = re.search(r'\n\s+\-\s+Planning finished:\s+(\S+)', timeline)
    assert event is not None, "planning finished event missing"
    assert_time_str(event.group(1), data[index], "planning finished event incorrect")

  # Event Timeline Submit for Admission
  index += 1
  assert sql_results.column_labels[index] == EVENT_SUBMIT_FOR_ADMISSION
  ret_data[EVENT_SUBMIT_FOR_ADMISSION] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert data[index] == "0.000", "submit for admission event incorrect"
  else:
    event = re.search(r'\n\s+\-\s+Submit for admission:\s+(\S+)', timeline)
    assert event is not None, "submit for admission event missing"
    assert_time_str(event.group(1), data[index], "submit for admission event incorrect")

  # Event Timeline Completed Admission
  index += 1
  assert sql_results.column_labels[index] == EVENT_COMPLETED_ADMISSION
  ret_data[EVENT_COMPLETED_ADMISSION] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert data[index] == "0.000", "completed admission event incorrect"
  else:
    event = re.search(r'\n\s+\-\s+Completed admission:\s+(\S+)', timeline)
    assert event is not None, "completed admission event missing"
    assert_time_str(event.group(1), data[index], "completed admission event incorrect")

  # Event Timeline All Backends Started
  index += 1
  assert sql_results.column_labels[index] == EVENT_ALL_BACKENDS_STARTED
  ret_data[EVENT_ALL_BACKENDS_STARTED] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert data[index] == "0.000", "all backends started event incorrect"
  else:
    event = re.search(r'\n\s+\-\s+All \d+ execution backends \(\d+ fragment instances\)'
        r' started:\s+(\S+)', timeline)
    assert event is not None, "all backends started event missing"
    assert_time_str(event.group(1), data[index], "all backends started event incorrect")

  # Event Timeline Rows Available
  index += 1
  assert sql_results.column_labels[index] == EVENT_ROWS_AVAILABLE
  ret_data[EVENT_ROWS_AVAILABLE] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DML":
    assert data[index] == "0.000", "rows available event incorrect"
  else:
    event = re.search(r'\n\s+\-\s+Rows available:\s+(\S+)', timeline)
    assert event is not None, "rows available event missing"
    assert_time_str(event.group(1), data[index], "rows available event incorrect")

  # Event Timeline First Row Fetched
  index += 1
  assert sql_results.column_labels[index] == EVENT_FIRST_ROW_FETCHED
  ret_data[EVENT_FIRST_ROW_FETCHED] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DDL" or query_type == "DML":
    assert data[index] == "0.000", "first row fetched event incorrect"
  else:
    event = re.search(r'\n\s+\-\s+First row fetched:\s+(\S+)', timeline)
    assert event is not None, "first row fetched event missing"
    assert_time_str(event.group(1), data[index], "first row fetched event incorrect")

  # Event Timeline Last Row Fetched
  index += 1
  assert sql_results.column_labels[index] == EVENT_LAST_ROW_FETCHED
  ret_data[EVENT_LAST_ROW_FETCHED] = data[index]
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert data[index] == "0.000", "last row fetched event incorrect"
  else:
    event = re.search(r'\n\s+\-\s+Last row fetched:\s+(\S+)', timeline)
    assert event is not None, "last row fetched event missing"
    assert_time_str(event.group(1), data[index], "last row fetched event incorrect")

  # Event Timeline Unregister Query
  index += 1
  assert sql_results.column_labels[index] == EVENT_UNREGISTER_QUERY
  ret_data[EVENT_UNREGISTER_QUERY] = data[index]
  event = re.search(r'\n\s+\-\s+Unregister query:\s+(\S+)', timeline)
  assert event is not None, "unregister query event missing"
  assert_time_str(event.group(1), data[index], "unregister query event incorrect")

  # Read IO Wait Total
  index += 1
  assert sql_results.column_labels[index] == READ_IO_WAIT_TOTAL_MS
  ret_data[READ_IO_WAIT_TOTAL_MS] = data[index]
  total_read_wait = 0
  if (query_state_value != "EXCEPTION" and query_type == "QUERY") or data[index] != "0":
    re_wait_time = re.compile(r'^\s+\-\s+ScannerIoWaitTime:\s+(.*?)$')
    read_waits = assert_scan_node_metrics(re_wait_time, profile_lines)
    for r in read_waits:
      total_read_wait += convert_to_milliseconds(r)

    tolerance = total_read_wait * 0.001

    assert total_read_wait - tolerance <= float(data[index]) <= \
        total_read_wait + tolerance, "read io wait time total incorrect"
  else:
    assert data[index] == "0.000"

  # Read IO Wait Average
  index += 1
  assert sql_results.column_labels[index] == READ_IO_WAIT_MEAN_MS
  ret_data[READ_IO_WAIT_MEAN_MS] = data[index]
  if (query_state_value != "EXCEPTION" and query_type == "QUERY"
      and len(read_waits) != 0) or data[index] != "0.000":
    avg_read_wait = round_to_3(float(total_read_wait / len(read_waits)))
    assert avg_read_wait - tolerance <= float(data[index]) <= avg_read_wait + tolerance, \
        "read io wait time average incorrect"
  else:
    assert data[index] == "0.000"

  # Total Bytes Read From Cache
  index += 1
  assert sql_results.column_labels[index] == BYTES_READ_CACHE_TOTAL
  ret_data[BYTES_READ_CACHE_TOTAL] = data[index]
  if (query_state_value != "EXCEPTION" and query_type == "QUERY") or data[index] != "0":
    re_cache_read = re.compile(r'^\s+\-\s+DataCacheHitBytes:\s+.*?\((\d+)\)$')
    read_from_cache = assert_scan_node_metrics(re_cache_read, profile_lines)

    total_read = 0
    for r in read_from_cache:
      total_read += int(r)
    assert total_read == int(data[index]), "bytes read from cache total incorrect"
  else:
    assert data[index] == "0"

  # Total Bytes Read
  index += 1
  assert sql_results.column_labels[index] == BYTES_READ_TOTAL
  ret_data[BYTES_READ_TOTAL] = data[index]
  bytes_read = re.search(r'\n\s+\-\s+TotalBytesRead:\s+.*?\((\d+)\)\n', profile_text)
  if query_state_value != "EXCEPTION" and query_type == "QUERY":
    assert bytes_read is not None, "total bytes read missing"
  if bytes_read is not None:
    assert data[index] == bytes_read.group(1), "total bytes read incorrect"

  # Calculate all peak memory usage stats by scraping the query profile.
  peak_mem_cnt = 0
  min_peak_mem = 0
  max_peak_mem = 0
  total_peak_mem = 0
  for peak_mem in re.findall(r'\n\s+Per Node Peak Memory Usage:(.*?)\n', profile_text):
    for node in re.findall(r'\s+.*?:\d+\((.*?)\)', peak_mem):
      peak_mem_cnt += 1
      conv = convert_to_bytes(node)
      total_peak_mem += conv
      if conv < min_peak_mem or min_peak_mem == 0:
        min_peak_mem = conv
      if conv > max_peak_mem:
        max_peak_mem = conv
  if query_state_value != "EXCEPTION" and query_type != "DDL":
    assert peak_mem_cnt > 0, "did not find per node peak memory usage"

  # Per Node Peak Memory Usage Min
  index += 1
  assert sql_results.column_labels[index] == PERNODE_PEAK_MEM_MIN
  ret_data[PERNODE_PEAK_MEM_MIN] = data[index]
  tolerance = int(min_peak_mem * 0.005)
  assert min_peak_mem - tolerance <= int(data[index]) <= min_peak_mem + tolerance, \
      "pernode peak memory minimum incorrect"

  # Per Node Peak Memory Usage Max
  index += 1
  assert sql_results.column_labels[index] == PERNODE_PEAK_MEM_MAX
  ret_data[PERNODE_PEAK_MEM_MAX] = data[index]
  tolerance = int(max_peak_mem * 0.005)
  assert max_peak_mem - tolerance <= int(data[index]) <= max_peak_mem + tolerance, \
      "pernode peak memory maximum incorrect"

  # Per Node Peak Memory Usage Mean
  index += 1
  assert sql_results.column_labels[index] == PERNODE_PEAK_MEM_MEAN
  ret_data[PERNODE_PEAK_MEM_MEAN] = data[index]
  mean_peak_mem = 0
  if peak_mem_cnt > 0:
    mean_peak_mem = int(total_peak_mem / peak_mem_cnt)
  tolerance = int(max_peak_mem * 0.005)
  assert mean_peak_mem - tolerance <= int(data[index]) <= mean_peak_mem + tolerance, \
      "pernode peak memory mean incorrect"

  # SQL statement
  index += 1
  assert sql_results.column_labels[index] == SQL
  ret_data[SQL] = data[index]
  sql_stmt = re.search(r'\n\s+Sql Statement:\s+(.*?)\n', profile_text)
  assert sql_stmt is not None
  assert data[index] == sql_stmt.group(1), "sql incorrect"

  # Query Plan
  index += 1
  assert sql_results.column_labels[index] == PLAN
  ret_data[PLAN] = data[index]
  plan = re.search(r'\n\s+Plan:\s*\n(.*)\n\s+Estimated Per-Host Mem', profile_text,
      re.DOTALL)
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert plan is None
    assert data[index] == ""
  else:
    assert plan is not None
    assert data[index] == plan.group(1)

  # Tables Queried
  index += 1
  assert sql_results.column_labels[index] == TABLES_QUERIED
  ret_data[TABLES_QUERIED] = data[index]
  tables = re.search(r'\n\s+Tables Queried:\s+(.*?)\n', profile_text)
  if query_state_value == "EXCEPTION" or query_type == "DDL":
    assert tables is None
    assert data[index] == ""
  else:
    assert tables is not None
    assert data[index] == tables.group(1)

  return ret_data
# function assert_query


def assert_scan_node_metrics(re_metric, profile_lines):
  """Retrieves metrics reported under HDFS_SCAN_NODEs removing any metrics from
      Averaged Fragments. The provided re_metric must be a compiled regular expression
      with at least one capture group. Returns a list of the contents of the first
      capture group in the re_metrics regular expression for all matching metrics."""
  metrics = []

  re_in_scan = re.compile(r'^\s+HDFS_SCAN_NODE')
  re_avg_fgmt = re.compile(r'^(\s+)Averaged Fragment')
  in_scan = False
  in_avg_fgmt = 0
  for line in profile_lines:
    avg_fmt_res = re_avg_fgmt.search(line)
    if avg_fmt_res is not None:
      # Averaged Fragments sometimes have HDFS_SCAN_NODEs which must be skipped.
      in_avg_fgmt = len(avg_fmt_res.group(1))
    elif in_avg_fgmt > 0 and line[in_avg_fgmt + 1] != " ":
      # Found a line at the same indentation as the previous Averaged Fragement, thus
      # we successfully skipped over any HDFS_SCAN_NODEs if they existed.
      in_avg_fgmt = 0
    elif in_avg_fgmt == 0 and re_in_scan.match(line) is not None:
      # Found a HDFS_SCAN_NODE that was not under an Averaged Fragment.
      in_scan = True
    elif in_scan:
      # Search through the HDFS_SCAN_NODE for the metric.
      res = re_metric.search(line)
      if res is not None:
        metrics.append(res.group(1))
        in_scan = False

  return metrics
# function assert_scan_node_metrics
