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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfS3, SkipIfADLS, SkipIfIsilon, SkipIfLocal
from tests.common.impala_cluster import ImpalaCluster
import logging
import pytest
import re
import time

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
    assert result.exec_summary[0]['operator'] == '05:MERGING-EXCHANGE'
    assert result.exec_summary[0]['num_rows'] == 5
    assert result.exec_summary[0]['est_num_rows'] == 5
    assert result.exec_summary[0]['peak_mem'] > 0

    for line in result.runtime_profile.split('\n'):
      # The first 'RowsProduced' we find is for the coordinator fragment.
      if 'RowsProduced' in line:
        assert '(5)' in line
        break

  def test_broadcast_num_rows(self):
    """Regression test for IMPALA-3002 - checks that the num_rows for a broadcast node
    in the exec summaty is correctly set as the max over all instances, not the sum."""
    query = """select distinct a.int_col, a.string_col from functional.alltypes a
        inner join functional.alltypessmall b on (a.id = b.id)
        where a.year = 2009 and b.month = 2"""
    result = self.execute_query(query)
    assert result.exec_summary[5]['operator'] == '04:EXCHANGE'
    assert result.exec_summary[5]['num_rows'] == 25
    assert result.exec_summary[5]['est_num_rows'] == 25
    assert result.exec_summary[5]['peak_mem'] > 0

  @SkipIfS3.hbase
  @SkipIfLocal.hbase
  @SkipIfIsilon.hbase
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

  def test_query_states(self):
    """Tests that the query profile shows expected query states."""
    query = "select count(*) from functional.alltypes"
    handle = self.execute_query_async(query, dict())
    profile = self.client.get_runtime_profile(handle)
    # If ExecuteStatement() has completed but the results haven't been fetched yet, the
    # query must have at least reached RUNNING.
    assert "Query State: RUNNING" in profile or \
      "Query State: FINISHED" in profile, profile

    results = self.client.fetch(query, handle)
    profile = self.client.get_runtime_profile(handle)
    # After fetching the results, the query must be in state FINISHED.
    assert "Query State: FINISHED" in profile, profile

  def test_query_options(self):
    """Test that the query profile shows expected non-default query options, both set
    explicitly through client and those set by planner"""
    # Set a query option explicitly through client
    self.execute_query("set MEM_LIMIT = 8589934592")
    # Make sure explicitly set default values are not shown in the profile
    self.execute_query("set runtime_filter_wait_time_ms = 0")
    runtime_profile = self.execute_query("select 1").runtime_profile
    assert "Query Options (set by configuration): MEM_LIMIT=8589934592" in runtime_profile
    # For this query, the planner sets NUM_NODES=1, NUM_SCANNER_THREADS=1,
    # RUNTIME_FILTER_MODE=0 and MT_DOP=0
    assert "Query Options (set by configuration and planner): MEM_LIMIT=8589934592," \
        "NUM_NODES=1,NUM_SCANNER_THREADS=1,RUNTIME_FILTER_MODE=0,MT_DOP=0\n" \
        in runtime_profile

  @SkipIfLocal.multiple_impalad
  @pytest.mark.xfail(reason="IMPALA-6338")
  def test_profile_fragment_instances(self):
    """IMPALA-6081: Test that the expected number of fragment instances and their exec
    nodes appear in the runtime profile, even when fragments may be quickly cancelled when
    all results are already returned."""
    results = self.execute_query("""
        with l as (select * from tpch.lineitem UNION ALL select * from tpch.lineitem)
        select STRAIGHT_JOIN count(*) from (select * from tpch.lineitem a LIMIT 1) a
        join (select * from l LIMIT 2000000) b on a.l_orderkey = -b.l_orderkey;""")
    # There are 3 scan nodes and each appears in the profile 4 times (for 3 fragment
    # instances + the averaged fragment).
    assert results.runtime_profile.count("HDFS_SCAN_NODE") == 12
    # There are 3 exchange nodes and each appears in the profile 2 times (for 1 fragment
    # instance + the averaged fragment).
    assert results.runtime_profile.count("EXCHANGE_NODE") == 6
    # The following appear only in the root fragment which has 1 instance.
    assert results.runtime_profile.count("HASH_JOIN_NODE") == 2
    assert results.runtime_profile.count("AGGREGATION_NODE") == 2
    assert results.runtime_profile.count("PLAN_ROOT_SINK") == 2

  # IMPALA-6399: Run this test serially to avoid a delay over the wait time in fetching
  # the profile.
  @pytest.mark.execute_serially
  def test_query_profile_thrift_timestamps(self):
    """Test that the query profile start and end time date-time strings have
    nanosecond precision. Nanosecond precision is expected by management API clients
    that consume Impala debug webpages."""
    query = "select sleep(1000)"
    handle = self.client.execute_async(query)
    query_id = handle.get_handle().id
    results = self.client.fetch(query, handle)
    self.client.close()

    MAX_WAIT = 300
    start = time.time()
    end = start + MAX_WAIT
    while time.time() <= end:
      # Sleep before trying to fetch the profile. This helps to prevent a warning when the
      # profile is not yet available immediately. It also makes it less likely to
      # introduce an error below in future changes by forgetting to sleep.
      time.sleep(1)
      tree = self.impalad_test_service.get_thrift_profile(query_id)
      if not tree:
        continue

      # tree.nodes[1] corresponds to ClientRequestState::summary_profile_
      # See be/src/service/client-request-state.[h|cc].
      start_time = tree.nodes[1].info_strings["Start Time"]
      end_time = tree.nodes[1].info_strings["End Time"]
      # Start and End Times are of the form "2017-12-07 22:26:52.167711000"
      start_time_sub_sec_str = start_time.split('.')[-1]
      end_time_sub_sec_str = end_time.split('.')[-1]
      if len(end_time_sub_sec_str) == 0:
        elapsed = time.time() - start
        logging.info("end_time_sub_sec_str hasn't shown up yet, elapsed=%d", elapsed)
        continue

      assert len(end_time_sub_sec_str) == 9, end_time
      assert len(start_time_sub_sec_str) == 9, start_time
      return True

    # If we're here, we didn't get the final thrift profile from the debug web page.
    # This could happen due to heavy system load. The test is then inconclusive.
    # Log a message and fail this run.

    dbg_str = "Debug thrift profile for query {0} not available in {1} seconds".format(
      query_id, MAX_WAIT)
    assert False, dbg_str

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
