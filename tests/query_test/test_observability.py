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

from collections import defaultdict
from datetime import datetime
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import (SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon,
                               SkipIfLocal, SkipIfNotHdfsMinicluster)
from tests.util.filesystem_utils import IS_EC
from time import sleep, time
from RuntimeProfile.ttypes import TRuntimeProfileFormat
import logging
import pytest
import re

class TestObservability(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  def test_merge_exchange_num_rows(self):
    """Regression test for IMPALA-1473 - checks that the exec summary for a merging
    exchange with a limit reports the number of rows returned as equal to the limit,
    and that the coordinator fragment portion of the runtime profile reports the number
    of rows returned correctly."""
    query = """select tinyint_col, count(*) from functional.alltypes
        group by tinyint_col order by tinyint_col limit 5"""
    result = self.execute_query(query)
    exchange = result.exec_summary[1]
    assert exchange['operator'] == '05:MERGING-EXCHANGE'
    assert exchange['num_rows'] == 5
    assert exchange['est_num_rows'] == 5
    assert exchange['peak_mem'] > 0

    for line in result.runtime_profile.split('\n'):
      # The first 'RowsProduced' we find is for the coordinator fragment.
      if 'RowsProduced' in line:
        assert '(5)' in line
        break

  def test_broadcast_num_rows(self):
    """Regression test for IMPALA-3002 - checks that the num_rows for a broadcast node
    in the exec summary is correctly set as the max over all instances, not the sum."""
    query = """select distinct a.int_col, a.string_col from functional.alltypes a
        inner join functional.alltypessmall b on (a.id = b.id)
        where a.year = 2009 and b.month = 2"""
    result = self.execute_query(query)
    exchange = result.exec_summary[8]
    assert exchange['operator'] == '04:EXCHANGE'
    assert exchange['num_rows'] == 25
    assert exchange['est_num_rows'] == 25
    assert exchange['peak_mem'] > 0

  def test_report_time(self):
    """ Regression test for IMPALA-6741 - checks that last reporting time exists in
    profiles of fragment instances."""
    query = """select count(distinct a.int_col) from functional.alltypes a
        inner join functional.alltypessmall b on (a.id = b.id + cast(sleep(15) as INT))"""
    handle = self.hs2_client.execute_async(query)

    num_validated = 0
    tree = self.hs2_client.get_runtime_profile(handle, TRuntimeProfileFormat.THRIFT)
    while not self.hs2_client.state_is_finished(handle):
      assert tree, num_validated
      for node in tree.nodes:
        if node.name.startswith('Instance '):
          info_strings_key = 'Last report received time'
          assert info_strings_key in node.info_strings
          report_time_str = node.info_strings[info_strings_key].split(".")[0]
          # Try converting the string to make sure it's in the expected format
          assert datetime.strptime(report_time_str, '%Y-%m-%d %H:%M:%S')
          num_validated += 1
      tree = self.hs2_client.get_runtime_profile(handle, TRuntimeProfileFormat.THRIFT)
      # Let's not hit the backend too hard
      sleep(0.1)
    assert num_validated > 0
    self.hs2_client.close_query(handle)

  @SkipIfS3.hbase
  @SkipIfLocal.hbase
  @SkipIfIsilon.hbase
  @SkipIfABFS.hbase
  @SkipIfADLS.hbase
  def test_scan_summary(self):
    """IMPALA-4499: Checks that the exec summary for scans show the table name."""
    # HDFS table
    query = "select count(*) from functional.alltypestiny"
    result = self.execute_query(query)
    scan_idx = len(result.exec_summary) - 1
    assert result.exec_summary[scan_idx]['operator'] == '00:SCAN HDFS'
    assert result.exec_summary[scan_idx]['detail'] == 'functional.alltypestiny'

    # KUDU table
    query = "select count(*) from functional_kudu.alltypestiny"
    result = self.execute_query(query)
    scan_idx = len(result.exec_summary) - 1
    assert result.exec_summary[scan_idx]['operator'] == '00:SCAN KUDU'
    assert result.exec_summary[scan_idx]['detail'] == 'functional_kudu.alltypestiny'

    # HBASE table
    query = "select count(*) from functional_hbase.alltypestiny"
    result = self.execute_query(query)
    scan_idx = len(result.exec_summary) - 1
    assert result.exec_summary[scan_idx]['operator'] == '00:SCAN HBASE'
    assert result.exec_summary[scan_idx]['detail'] == 'functional_hbase.alltypestiny'

  def test_sink_summary(self, unique_database):
    """IMPALA-1048: Checks that the exec summary contains sinks."""
    # SELECT query.
    query = "select count(*) from functional.alltypes"
    result = self.execute_query(query)
    # Sanity-check the root sink.
    root_sink = result.exec_summary[0]
    assert root_sink['operator'] == 'F01:ROOT'
    assert root_sink['max_time'] >= 0
    assert root_sink['num_rows'] == -1
    assert root_sink['est_num_rows'] == -1
    assert root_sink['peak_mem'] >= 0
    assert root_sink['est_peak_mem'] >= 0
    # Sanity-check the exchange sink.
    found_exchange_sender = False
    for row in result.exec_summary[1:]:
      if 'EXCHANGE SENDER' not in row['operator']:
        continue
      found_exchange_sender = True
      assert re.match("F[0-9]+:EXCHANGE SENDER", row['operator'])
      assert row['max_time'] >= 0
      assert row['num_rows'] == -1
      assert row['est_num_rows'] == -1
      assert row['peak_mem'] >= 0
      assert row['est_peak_mem'] >= 0
    assert found_exchange_sender, result

    # INSERT query.
    query = "create table {0}.tmp as select count(*) from functional.alltypes".format(
        unique_database)
    result = self.execute_query(query)
    # Sanity-check the HDFS writer sink.
    assert result.exec_summary[0]['operator'] == 'F01:HDFS WRITER'
    assert result.exec_summary[0]['max_time'] >= 0
    assert result.exec_summary[0]['num_rows'] == -1
    assert result.exec_summary[0]['est_num_rows'] == -1
    assert result.exec_summary[0]['peak_mem'] >= 0
    assert result.exec_summary[0]['est_peak_mem'] >= 0

  def test_query_states(self):
    """Tests that the query profile shows expected query states."""
    query = "select count(*) from functional.alltypes"
    handle = self.execute_query_async(query,
        {"debug_action": "CRS_BEFORE_ADMISSION:SLEEP@1000"})
    # If ExecuteStatement() has completed and the query is paused in the admission control
    # phase, then the query must be in COMPILED state.
    profile = self.client.get_runtime_profile(handle)
    assert "Query State: COMPILED" in profile
    # After completion of the admission control phase, the query must have at least
    # reached RUNNING state.
    self.client.wait_for_admission_control(handle)
    profile = self.client.get_runtime_profile(handle)
    assert "Query State: RUNNING" in profile or \
      "Query State: FINISHED" in profile, profile

    results = self.client.fetch(query, handle)
    profile = self.client.get_runtime_profile(handle)
    # After fetching the results, the query must be in state FINISHED.
    assert "Query State: FINISHED" in profile, profile

  def test_query_options(self):
    """Test that the query profile shows expected non-default query options, both set
    explicitly through client and those set by planner"""
    # Set mem_limit and runtime_filter_wait_time_ms to non-default and default value.
    query_opts = {'mem_limit': 8589934592, 'runtime_filter_wait_time_ms': 0}
    profile = self.execute_query("select 1", query_opts).runtime_profile
    assert "Query Options (set by configuration): MEM_LIMIT=8589934592" in profile,\
        profile
    assert "CLIENT_IDENTIFIER=" + \
        "query_test/test_observability.py::TestObservability::()::test_query_options" \
        in profile
    # Get the TIMEZONE value.
    server_timezone = None
    for row in self.execute_query("set", query_opts).data:
      name, val, _ = row.split("\t")
      if name == "TIMEZONE":
        server_timezone = val
        break
    assert server_timezone is not None

    # For this query, the planner sets NUM_NODES=1, NUM_SCANNER_THREADS=1,
    # RUNTIME_FILTER_MODE=0 and MT_DOP=0
    expected_str = ("Query Options (set by configuration and planner): "
        "MEM_LIMIT=8589934592,"
        "NUM_NODES=1,NUM_SCANNER_THREADS=1,"
        "RUNTIME_FILTER_MODE=0,MT_DOP=0,TIMEZONE={timezone},{erasure_coding}"
        "CLIENT_IDENTIFIER="
        "query_test/test_observability.py::TestObservability::()::test_query_options"
        "\n")
    expected_str = expected_str.format(
        erasure_coding="ALLOW_ERASURE_CODED_FILES=1," if IS_EC else "",
        timezone=server_timezone)
    assert expected_str in profile, profile

  def test_exec_summary(self):
    """Test that the exec summary is populated correctly in every query state"""
    query = "select count(*) from functional.alltypes"
    handle = self.execute_query_async(query,
        {"debug_action": "CRS_BEFORE_ADMISSION:SLEEP@1000"})
    # If ExecuteStatement() has completed and the query is paused in the admission control
    # phase, then the coordinator has not started yet and exec_summary should be empty.
    exec_summary = self.client.get_exec_summary(handle)
    assert exec_summary is not None and exec_summary.nodes is None
    # After completion of the admission control phase, the coordinator would have started
    # and we should get a populated exec_summary.
    self.client.wait_for_admission_control(handle)
    exec_summary = self.client.get_exec_summary(handle)
    assert exec_summary is not None and exec_summary.nodes is not None

    self.client.fetch(query, handle)
    exec_summary = self.client.get_exec_summary(handle)
    # After fetching the results and reaching finished state, we should still be able to
    # fetch an exec_summary.
    assert exec_summary is not None and exec_summary.nodes is not None

  def test_exec_summary_in_runtime_profile(self):
    """Test that the exec summary is populated in runtime profile correctly in every
    query state"""
    query = "select count(*) from functional.alltypes"
    handle = self.execute_query_async(query,
        {"debug_action": "CRS_BEFORE_ADMISSION:SLEEP@1000"})

    # If ExecuteStatement() has completed and the query is paused in the admission control
    # phase, then the coordinator has not started yet and exec_summary should be empty.
    profile = self.client.get_runtime_profile(handle)
    assert "ExecSummary:" not in profile, profile
    # After completion of the admission control phase, the coordinator would have started
    # and we should get a populated exec_summary.
    self.client.wait_for_admission_control(handle)
    profile = self.client.get_runtime_profile(handle)
    assert "ExecSummary:" in profile, profile

    self.client.fetch(query, handle)
    # After fetching the results and reaching finished state, we should still be able to
    # fetch an exec_summary in profile.
    profile = self.client.get_runtime_profile(handle)
    assert "ExecSummary:" in profile, profile

  @SkipIfLocal.multiple_impalad
  def test_profile_fragment_instances(self):
    """IMPALA-6081: Test that the expected number of fragment instances and their exec
    nodes appear in the runtime profile, even when fragments may be quickly cancelled when
    all results are already returned."""
    results = self.execute_query("""
        with l as (select * from tpch.lineitem UNION ALL select * from tpch.lineitem)
        select STRAIGHT_JOIN count(*) from (select * from tpch.lineitem a LIMIT 1) a
        join (select * from l LIMIT 2000000) b on a.l_orderkey = -b.l_orderkey;""")
    # There are 3 scan nodes and each appears in the profile n+1 times (for n fragment
    # instances + the averaged fragment). n depends on how data is loaded and scheduler's
    # decision.
    n = results.runtime_profile.count("HDFS_SCAN_NODE")
    assert n > 0 and n % 3 == 0
    # There are 3 exchange nodes and each appears in the profile 2 times (for 1 fragment
    # instance + the averaged fragment).
    assert results.runtime_profile.count("EXCHANGE_NODE") == 6
    # The following appear only in the root fragment which has 1 instance.
    assert results.runtime_profile.count("HASH_JOIN_NODE") == 2
    assert results.runtime_profile.count("AGGREGATION_NODE") == 2
    assert results.runtime_profile.count("PLAN_ROOT_SINK") == 2

  def test_query_profile_contains_query_events(self):
    """Test that the expected events show up in a query profile."""
    event_regexes = [r'Query Timeline:',
        r'Query submitted:',
        r'Planning finished:',
        r'Submit for admission:',
        r'Completed admission:',
        r'Ready to start on .* backends:',
        r'All .* execution backends \(.* fragment instances\) started:',
        r'Rows available:',
        r'First row fetched:',
        r'Last row fetched:',
        r'Released admission control resources:']
    query = "select * from functional.alltypes"
    runtime_profile = self.execute_query(query).runtime_profile
    self.__verify_profile_event_sequence(event_regexes, runtime_profile)

  def test_query_profile_contains_instance_events(self):
    """Test that /query_profile_encoded contains an event timeline for fragment
    instances, even when there are errors."""
    event_regexes = [r'Fragment Instance Lifecycle Event Timeline',
                     r'Prepare Finished',
                     r'Open Finished',
                     r'First Batch Produced',
                     r'First Batch Sent',
                     r'ExecInternal Finished']
    query = "select count(*) from functional.alltypes"
    runtime_profile = self.execute_query(query).runtime_profile
    self.__verify_profile_event_sequence(event_regexes, runtime_profile)

  def test_query_profile_contains_node_events(self):
    """Test that ExecNode events show up in a profile."""
    event_regexes = [r'Node Lifecycle Event Timeline',
                     r'Open Started',
                     r'Open Finished',
                     r'First Batch Requested',
                     r'First Batch Returned',
                     r'Last Batch Returned',
                     r'Closed']
    query = "select count(*) from functional.alltypes"
    runtime_profile = self.execute_query(query).runtime_profile
    self.__verify_profile_event_sequence(event_regexes, runtime_profile)

  def __verify_profile_event_sequence(self, event_regexes, runtime_profile):
    """Check that 'event_regexes' appear in a consecutive series of lines in
       'runtime_profile'"""
    lines = runtime_profile.splitlines()
    event_regex_index = 0

    # Check that the strings appear in the above order with no gaps in the profile.
    for line in runtime_profile.splitlines():
      match = re.search(event_regexes[event_regex_index], line)
      if match is not None:
        event_regex_index += 1
        if event_regex_index == len(event_regexes):
          # Found all the lines - we're done.
          return
      else:
        # Haven't found the first regex yet.
        assert event_regex_index == 0, \
            event_regexes[event_regex_index] + " not in " + line + "\n" + runtime_profile
    assert event_regex_index == len(event_regexes), \
        "Didn't find all events in profile: \n" + runtime_profile

  def test_query_profile_contains_all_events(self, unique_database):
    """Test that the expected events show up in a query profile for various queries"""
    # make a data file to load data from
    path = "test-warehouse/{0}.db/data_file".format(unique_database)
    self.filesystem_client.create_file(path, "1")
    use_query = "use {0}".format(unique_database)
    self.execute_query(use_query)
    # all the events we will see for every query
    event_regexes = [
      r'Query Compilation:',
      r'Query Timeline:',
      r'Planning finished'
    ]
    # queries that explore different code paths in Frontend compilation
    queries = [
      'create table if not exists impala_6568 (i int)',
      'select * from impala_6568',
      'explain select * from impala_6568',
      'describe impala_6568',
      'alter table impala_6568 set tblproperties(\'numRows\'=\'10\')',
      "load data inpath '/{0}' into table impala_6568".format(path)
    ]
    # run each query...
    for query in queries:
      runtime_profile = self.execute_query(query).runtime_profile
      # and check that all the expected events appear in the resulting profile
      self.__verify_profile_contains_every_event(event_regexes, runtime_profile, query)

  def __verify_profile_contains_every_event(self, event_regexes, runtime_profile, query):
    """Test that all the expected events show up in a given query profile."""
    for regex in event_regexes:
      assert any(re.search(regex, line) for line in runtime_profile.splitlines()), \
          "Didn't find event '" + regex + "' for query '" + query + \
          "' in profile: \n" + runtime_profile

  def test_compute_stats_profile(self, unique_database):
    """Test that the profile for a 'compute stats' query contains three unique query ids:
    one for the parent 'compute stats' query and one each for the two child queries."""
    table_name = "%s.test_compute_stats_profile" % unique_database
    self.execute_query(
        "create table %s as select * from functional.alltypestiny" % table_name)
    results = self.execute_query("compute stats %s" % table_name)
    # Search for all query ids (max length 33) in the profile.
    matches = re.findall("Query \(id=.{,33}\)", results.runtime_profile)
    query_ids = []
    for query_id in matches:
      if query_id not in query_ids:
        query_ids.append(query_id)
    assert len(query_ids) == 3, results.runtime_profile

  def test_global_resource_counters_in_profile(self):
    """Test that a set of global resource usage counters show up in the profile."""
    query = "select count(*) from functional.alltypes"
    profile = self.execute_query(query).runtime_profile
    expected_counters = ["TotalBytesRead", "TotalBytesSent", "TotalScanBytesSent",
                         "TotalInnerBytesSent", "ExchangeScanRatio",
                         "InnerNodeSelectivityRatio"]
    assert all(counter in profile for counter in expected_counters)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_global_exchange_counters(self):
    """Test that global exchange counters are set correctly."""
    query = """select count(*) from tpch_parquet.orders o inner join tpch_parquet.lineitem
        l on o.o_orderkey = l.l_orderkey group by o.o_clerk limit 10"""
    profile = self.execute_query(query).runtime_profile

    # TimeSeriesCounter should be prefixed with a hyphen.
    assert "  MemoryUsage" not in profile
    assert "- MemoryUsage" in profile

    assert "ExchangeScanRatio: 3.19" in profile

    keys = ["TotalBytesSent", "TotalScanBytesSent", "TotalInnerBytesSent"]
    counters = defaultdict(int)
    for line in profile.splitlines():
      for key in keys:
        if key in line:
          # Match byte count within parentheses
          m = re.search("\(([0-9]+)\)", line)
          assert m, "Cannot match pattern for key %s in line '%s'" % (key, line)
          # Only keep first (query-level) counter
          if counters[key] == 0:
            counters[key] = int(m.group(1))

    # All counters have values
    assert all(counters[key] > 0 for key in keys)

    assert counters["TotalBytesSent"] == (counters["TotalScanBytesSent"] +
                                          counters["TotalInnerBytesSent"])

  def test_query_profile_contains_host_resource_usage(self):
    """Tests that the profile contains a sub-profile with per node resource usage."""
    result = self.execute_query("select count(*), sleep(1000) from functional.alltypes")
    profile = result.runtime_profile
    expected_str = "Per Node Profiles:"
    assert any(expected_str in line for line in profile.splitlines())

  def test_query_profile_host_resource_metrics_off(self):
    """Tests that the query profile does not contain resource usage metrics by default or
       when disabled explicitly."""
    query = "select count(*), sleep(1000) from functional.alltypes"
    for query_opts in [None, {'resource_trace_ratio': 0.0}]:
      profile = self.execute_query(query, query_opts).runtime_profile
      # Assert that no host resource counters exist in the profile
      for line in profile.splitlines():
        assert not re.search("HostCpu.*Percentage", line)
        assert not re.search("HostNetworkRx", line)
        assert not re.search("HostDiskReadThroughput", line)

  def test_query_profile_contains_host_resource_metrics(self):
    """Tests that the query profile contains various CPU and network metrics."""
    query_opts = {'resource_trace_ratio': 1.0}
    query = "select count(*), sleep(1000) from functional.alltypes"
    profile = self.execute_query(query, query_opts).runtime_profile
    # We check for 500ms because a query with 1s duration won't hit the 64 values limit
    # that would trigger resampling.
    expected_strs = ["HostCpuIoWaitPercentage (500.000ms):",
                     "HostCpuSysPercentage (500.000ms):",
                     "HostCpuUserPercentage (500.000ms):",
                     "HostNetworkRx (500.000ms):",
                     "HostNetworkTx (500.000ms):",
                     "HostDiskReadThroughput (500.000ms):",
                     "HostDiskWriteThroughput (500.000ms):"]

    # Assert that all expected counters exist in the profile.
    for expected_str in expected_strs:
      assert any(expected_str in line for line in profile.splitlines()), expected_str

    # Check that there are some values for each counter.
    for line in profile.splitlines():
      if not any(key in line for key in expected_strs):
        continue
      values = line.split(':')[1].strip().split(',')
      assert len(values) > 0

  def _find_ts_counters_in_thrift_profile(self, profile, name):
    """Finds all time series counters in 'profile' with a matching name."""
    counters = []
    for node in profile.nodes:
      for counter in node.time_series_counters or []:
        if counter.name == name:
          counters.append(counter)
    return counters

  @pytest.mark.execute_serially
  def test_thrift_profile_contains_host_resource_metrics(self):
    """Tests that the thrift profile contains time series counters for CPU and network
       resource usage."""
    query_opts = {'resource_trace_ratio': 1.0}
    self.hs2_client.set_configuration(query_opts)
    result = self.hs2_client.execute("select sleep(2000)",
                                     profile_format=TRuntimeProfileFormat.THRIFT)
    thrift_profile = result.profile

    expected_keys = ["HostCpuUserPercentage", "HostNetworkRx", "HostDiskReadThroughput"]
    for key in expected_keys:
      counters = self._find_ts_counters_in_thrift_profile(thrift_profile, key)
      # The query will run on a single node, we will only find the counter once.
      assert len(counters) == 1
      counter = counters[0]
      assert len(counter.values) > 0

  @pytest.mark.execute_serially
  def test_query_profile_thrift_timestamps(self):
    """Test that the query profile start and end time date-time strings have
    nanosecond precision. Nanosecond precision is expected by management API clients
    that consume Impala debug webpages."""
    query = "select sleep(5)"
    result = self.hs2_client.execute(query, profile_format=TRuntimeProfileFormat.THRIFT)
    tree = result.profile

    # tree.nodes[1] corresponds to ClientRequestState::summary_profile_
    # See be/src/service/client-request-state.[h|cc].
    start_time = tree.nodes[1].info_strings["Start Time"]
    end_time = tree.nodes[1].info_strings["End Time"]
    # Start and End Times are of the form "2017-12-07 22:26:52.167711000"
    start_time_sub_sec_str = start_time.split('.')[-1]
    end_time_sub_sec_str = end_time.split('.')[-1]

    assert len(end_time_sub_sec_str) == 9, end_time
    assert len(start_time_sub_sec_str) == 9, start_time

  @pytest.mark.execute_serially
  def test_end_time(self):
    """ Test that verifies that the end time of a query with a coordinator is set once
    the coordinator releases its admission control resources. This ensures that the
    duration of the query will be determined by the time taken to do real work rather
    than the duration for which the query remains open. On the other hand, for queries
    without coordinators, the End Time is set only when UnregisterQuery() is called."""
    # Test the end time of a query with a coordinator.
    query = "select 1"
    handle = self.hs2_client.execute_async(query)
    result = self.hs2_client.fetch(query, handle)
    # Ensure that the query returns a non-empty result set.
    assert result is not None
    # Once the results have been fetched, the query End Time must be set.
    tree = self.hs2_client.get_runtime_profile(handle, TRuntimeProfileFormat.THRIFT)
    end_time = tree.nodes[1].info_strings["End Time"]
    assert end_time
    self.hs2_client.close_query(handle)

    # Test the end time of a query without a coordinator.
    query = "describe functional.alltypes"
    handle = self.hs2_client.execute_async(query)
    result = self.hs2_client.fetch(query, handle)
    # Ensure that the query returns a non-empty result set.
    assert result
    # The query End Time must not be set until the query is unregisterted
    tree = self.hs2_client.get_runtime_profile(handle, TRuntimeProfileFormat.THRIFT)
    end_time = tree.nodes[1].info_strings["End Time"]
    assert len(end_time) == 0, end_time
    # Save the last operation to be able to retrieve the profile after closing the query
    last_op = handle.get_handle()._last_operation
    self.hs2_client.close_query(handle)
    tree = last_op.get_profile(TRuntimeProfileFormat.THRIFT)
    end_time = tree.nodes[1].info_strings["End Time"]
    assert end_time is not None

  def test_query_profile_contains_number_of_fragment_instance(self):
    """Test that the expected section for number of fragment instance in
    a query profile."""
    event_regexes = [r'Per Host Number of Fragment Instances']
    query = "select count (*) from functional.alltypes"
    runtime_profile = self.execute_query(query).runtime_profile
    self.__verify_profile_event_sequence(event_regexes, runtime_profile)
