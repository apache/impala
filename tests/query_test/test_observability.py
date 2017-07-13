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
    self.execute_query("set mem_limit = 8589934592")
    # For this query, the planner sets NUM_NODES=1, NUM_SCANNER_THREADS=1 and
    # RUNTIME_FILTER_MODE=0
    expected_string = "Query Options (non default): MEM_LIMIT=8589934592,NUM_NODES=1," \
        "NUM_SCANNER_THREADS=1,RUNTIME_FILTER_MODE=0,MT_DOP=0\n"
    assert expected_string in self.execute_query("select 1").runtime_profile

    # Make sure explicitly set default values are not shown in the profile
    self.execute_query("set MAX_IO_BUFFERS = 0")
    assert expected_string in self.execute_query("select 1").runtime_profile
