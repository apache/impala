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
#

from copy import deepcopy
import pytest
import re
import time

from beeswaxd.BeeswaxService import QueryState
from tests.common.environ import build_flavor_timeout
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal, SkipIfIsilon
from tests.common.test_vector import ImpalaTestDimension
from tests.verifiers.metric_verifier import MetricVerifier

# slow_build_timeout is set to 200000 to avoid failures like IMPALA-8064 where the
# runtime filters don't arrive in time.
WAIT_TIME_MS = build_flavor_timeout(60000, slow_build_timeout=200000)

# Some of the queries in runtime_filters consume a lot of memory, leading to
# significant memory reservations in parallel tests.
# Skipping Isilon due to IMPALA-6998. TODO: Remove when there's a holistic revamp of
# what tests to run for non-HDFS platforms
@pytest.mark.execute_serially
@SkipIfLocal.multiple_impalad
@SkipIfIsilon.jira(reason="IMPALA-6998")
class TestRuntimeFilters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRuntimeFilters, cls).add_test_dimensions()
    # Runtime filters are disabled on HBase
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format not in ['hbase'])
    # Exercise both mt and non-mt code paths. Some tests assume 3 finstances, so
    # tests are not expected to work unmodified with higher mt_dop values.
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('mt_dop', 0, 1))
    # Don't test all combinations of file format and mt_dop, only test a few
    # representative formats.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format in ['parquet', 'text', 'kudu']
        or v.get_value('mt_dop') == 0)

  def test_basic_filters(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/runtime_filters', vector,
        test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS' : str(WAIT_TIME_MS)})

  def test_wait_time(self, vector):
    """Test that a query that has global filters does not wait for them if run in LOCAL
    mode"""
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    now = time.time()
    self.run_test_case('QueryTest/runtime_filters_wait', vector)
    duration_s = time.time() - now
    assert duration_s < (WAIT_TIME_MS / 1000), \
        "Query took too long (%ss, possibly waiting for missing filters?)" \
        % str(duration_s)

  @pytest.mark.execute_serially
  def test_wait_time_cancellation(self, vector):
    """Regression test for IMPALA-9065 to ensure that threads waiting for filters
    get woken up and exit promptly when the query is cancelled."""
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    # Make sure the cluster is quiesced before we start this test
    self._verify_no_fragments_running()

    self.change_database(self.client, vector.get_value('table_format'))
    # Set up a query where a scan (plan node 0, scanning alltypes) will wait
    # indefinitely for a filter to arrive. The filter arrival is delayed
    # by adding a wait to the scan of alltypestime (plan node 0).
    QUERY = """select straight_join *
               from alltypes t1
                    join /*+shuffle*/ alltypestiny t2 on t1.id = t2.id"""
    self.client.set_configuration(new_vector.get_value('exec_option'))
    self.client.set_configuration_option("DEBUG_ACTION", "1:OPEN:WAIT")
    self.client.set_configuration_option("RUNTIME_FILTER_WAIT_TIME_MS", "10000000")
    # Run same query with different delays to better exercise call paths.
    for delay_s in [0, 1, 2]:
      handle = self.client.execute_async(QUERY)
      # Wait until all the fragments have started up.
      BE_START_REGEX = 'All [0-9]* execution backends .* started'
      while re.search(BE_START_REGEX, self.client.get_runtime_profile(handle)) is None:
        time.sleep(0.2)
      time.sleep(delay_s)  # Give the query time to get blocked waiting for the filter.
      self.client.close_query(handle)

      # Ensure that cancellation has succeeded and the cluster has quiesced.
      self._verify_no_fragments_running()

  def _verify_no_fragments_running(self):
    """Raise an exception if there are fragments running on the cluster after a
    timeout."""
    for impalad in ImpalaCluster.get_e2e_test_cluster().impalads:
      verifier = MetricVerifier(impalad.service)
      verifier.wait_for_metric("impala-server.num-fragments-in-flight", 0, timeout=10)
      verifier.wait_for_backend_admission_control_state(timeout=10)

  def test_file_filtering(self, vector):
    if 'kudu' in str(vector.get_value('table_format')):
      return
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.change_database(self.client, vector.get_value('table_format'))
    self.execute_query("SET RUNTIME_FILTER_MODE=GLOBAL")
    self.execute_query("SET RUNTIME_FILTER_WAIT_TIME_MS=10000")
    result = self.execute_query("""select STRAIGHT_JOIN * from alltypes inner join
                                (select * from alltypessmall where smallint_col=-1) v
                                on v.year = alltypes.year""",
                                new_vector.get_value('exec_option'))
    assert re.search("Files rejected: 8 \(8\)", result.runtime_profile) is not None
    assert re.search("Splits rejected: [^0] \([^0]\)", result.runtime_profile) is None

  def test_file_filtering_late_arriving_filter(self, vector):
    """Test that late-arriving filters are applied to files when the scanner starts processing
    each scan range."""
    if 'kudu' in str(vector.get_value('table_format')):
      return
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.change_database(self.client, vector.get_value('table_format'))
    self.execute_query("SET RUNTIME_FILTER_MODE=GLOBAL")
    self.execute_query("SET RUNTIME_FILTER_WAIT_TIME_MS=1")
    self.execute_query("SET NUM_SCANNER_THREADS=1")
    # This query is crafted so that both scans execute slowly, but the filter should
    # arrive before the destination scan finishes processing all of its files (there are 8
    # files per executor). When I tested this, the filter reliably arrived after a single
    # input file was processed, but the test will still pass as long as it arrives before
    # the last file starts being processed.
    result = self.execute_query("""select STRAIGHT_JOIN count(*) from alltypes inner join /*+shuffle*/
                                     (select distinct * from alltypessmall
                                      where smallint_col > sleep(100)) v
                                     on v.id = alltypes.id
                                   where alltypes.id < sleep(10);""",
                                   new_vector.get_value('exec_option'))
    assert re.search("Splits rejected: [^0] \([^0]\)", result.runtime_profile) is not None


@SkipIfLocal.multiple_impalad
class TestBloomFilters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestBloomFilters, cls).add_test_dimensions()
    # Bloom filters are disabled on HBase, Kudu
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format not in ['hbase', 'kudu'])

  def test_bloom_filters(self, vector):
    self.run_test_case('QueryTest/bloom_filters', vector)

  def test_bloom_wait_time(self, vector):
    """Test that a query that has global filters does not wait for them if run in LOCAL
    mode"""
    now = time.time()
    self.run_test_case('QueryTest/bloom_filters_wait', vector)
    duration_s = time.time() - now
    assert duration_s < (WAIT_TIME_MS / 1000), \
        "Query took too long (%ss, possibly waiting for missing filters?)" \
        % str(duration_s)


@SkipIfLocal.multiple_impalad
class TestMinMaxFilters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMinMaxFilters, cls).add_test_dimensions()
    # Min-max filters are only implemented for Kudu.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format in ['kudu'])

  def test_min_max_filters(self, vector):
    self.run_test_case('QueryTest/min_max_filters', vector,
        test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})

  def test_decimal_min_max_filters(self, vector):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip("skip decimal min max filter test with various joins")
    self.run_test_case('QueryTest/decimal_min_max_filters', vector,
        test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})

  def test_large_strings(self, cursor, unique_database):
    """Tests that truncation of large strings by min-max filters still gives correct
    results"""
    table1 = "%s.min_max_filter_large_strings1" % unique_database
    cursor.execute(
        "create table %s (string_col string primary key) stored as kudu" % table1)
    # Min-max bounds are truncated at 1024 characters, so construct some strings that are
    # longer than that, as well as some that are very close to the min/max bounds.
    matching_vals =\
        ('b' * 1100, 'b' * 1099 + 'c', 'd' * 1100, 'f'* 1099 + 'e', 'f' * 1100)
    cursor.execute("insert into %s values ('%s'), ('%s'), ('%s'), ('%s'), ('%s')"
        % ((table1,) + matching_vals))
    non_matching_vals = ('b' * 1099 + 'a', 'c', 'f' * 1099 + 'g')
    cursor.execute("insert into %s values ('%s'), ('%s'), ('%s')"
        % ((table1,) + non_matching_vals))

    table2 = "%s.min_max_filter_large_strings2" % unique_database
    cursor.execute(
        "create table %s (string_col string primary key) stored as kudu" % table2)
    cursor.execute("insert into %s values ('%s'), ('%s'), ('%s'), ('%s'), ('%s')"
        % ((table2,) + matching_vals))

    cursor.execute("select count(*) from %s a, %s b where a.string_col = b.string_col"
        % (table1, table2))
    assert cursor.fetchall() == [(len(matching_vals),)]

    # Insert a string that will have the max char (255) trailing after truncation, to
    # test the path where adding 1 to the max bound after trunc overflows.
    max_trail_str = "concat(repeat('h', 1000), repeat(chr(255), 50))"
    cursor.execute("insert into %s values (%s)" % (table1, max_trail_str))
    cursor.execute("insert into %s values (%s)" % (table2, max_trail_str))
    cursor.execute("select count(*) from %s a, %s b where a.string_col = b.string_col"
        % (table1, table2))
    assert cursor.fetchall() == [(len(matching_vals) + 1,)]

    # Insert a string that is entirely the max char to test the path where the max can't
    # have 1 added to it after truncation and the filter is disabled.
    all_max_str = "repeat(chr(255), 1030)"
    cursor.execute("insert into %s values (%s)" % (table1, all_max_str))
    cursor.execute("insert into %s values (%s)" % (table2, all_max_str))
    cursor.execute("select count(*) from %s a, %s b where a.string_col = b.string_col"
        % (table1, table2))
    assert cursor.fetchall() == [(len(matching_vals) + 2,)]


@SkipIfLocal.multiple_impalad
class TestRuntimeRowFilters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRuntimeRowFilters, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])
    # Exercise both mt and non-mt code paths. Some tests assume 3 finstances, so
    # tests are not expected to work unmodified with higher mt_dop values.
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('mt_dop', 0, 4))

  def test_row_filters(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/runtime_row_filters', new_vector,
                       test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})

  def test_row_filter_reservation(self, vector):
    """Test handling of runtime filter memory reservations. Tuned for mt_dop=0."""
    mt_dop = vector.get_value('mt_dop')
    if mt_dop != 0:
        pytest.skip("Memory reservations tuned for mt_dop=0")
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['mt_dop'] = mt_dop
    self.run_test_case('QueryTest/runtime_row_filter_reservations', new_vector,
                       test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS' : str(WAIT_TIME_MS)})
