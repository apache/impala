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

from __future__ import absolute_import, division, print_function
import math
import os
import pytest
import re
import time

from tests.common.environ import build_flavor_timeout, ImpalaTestClusterProperties
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfEC, SkipIfLocal, SkipIfFS
from tests.common.test_dimensions import (
  add_mandatory_exec_option, add_exec_option_dimension)
from tests.verifiers.metric_verifier import MetricVerifier
from tests.util.filesystem_utils import WAREHOUSE

# slow_build_timeout is set to 200000 to avoid failures like IMPALA-8064 where the
# runtime filters don't arrive in time.
WAIT_TIME_MS = build_flavor_timeout(60000, slow_build_timeout=200000)
DEFAULT_VARS = {'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)}

# Dimensions exercised in some of the tests.
DIM_MT_DOP_SMALL = [0, 1]
DIM_MT_DOP_MEDIUM = [0, 4]
DIM_MAX_NUM_FILTERS_AGGREGATED_PER_HOST = [0, 2]

# Check whether the Impala under test in slow build. Query option ASYNC_CODEGEN will
# be enabled when test runs for slow build like ASAN, TSAN, UBSAN, etc. This avoid
# failures like IMPALA-9889 where the runtime filters don't arrive in time due to
# the slowness of codegen.
build_runs_slowly = ImpalaTestClusterProperties.get_instance().runs_slowly()


# Some of the queries in runtime_filters consume a lot of memory, leading to
# significant memory reservations in parallel tests.
# Skipping Isilon due to IMPALA-6998. TODO: Remove when there's a holistic revamp of
# what tests to run for non-HDFS platforms
@pytest.mark.execute_serially
@SkipIfLocal.multiple_impalad
@SkipIfFS.late_filters
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
    add_exec_option_dimension(cls, 'mt_dop', DIM_MT_DOP_SMALL)
    # Exercise max_num_filters_aggregated_per_host.
    add_exec_option_dimension(cls, 'max_num_filters_aggregated_per_host',
      DIM_MAX_NUM_FILTERS_AGGREGATED_PER_HOST)
    # Don't test all combinations of file format and mt_dop, only test a few
    # representative formats.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format in ['parquet', 'text', 'kudu']
        or v.get_value('mt_dop') == 0)
    # Limit 'max_num_filters_aggregated_per_host' exercise for parquet only.
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet'
      or v.get_value('max_num_filters_aggregated_per_host') == 0)
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  def test_basic_filters(self, vector):
    num_filters_per_host = vector.get_exec_option('max_num_filters_aggregated_per_host')
    num_backend = 2  # exclude coordinator
    num_updates = (num_backend + 1 if num_filters_per_host == 0
        else int(math.ceil(num_backend / num_filters_per_host)))
    vars = {
      '$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS),
      '$NUM_FILTER_UPDATES': str(num_updates)
    }
    self.run_test_case('QueryTest/runtime_filters', vector, test_file_vars=vars)

  def test_wait_time(self, vector):
    """Test that a query that has global filters does not wait for them if run in LOCAL
    mode"""
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
    # Make sure the cluster is quiesced before we start this test
    self._verify_no_fragments_running()

    self.change_database(self.client, vector.get_value('table_format'))
    # Set up a query where a scan (plan node 0, scanning alltypes) will wait
    # indefinitely for a filter to arrive. The filter arrival is delayed
    # by adding a wait to the scan of alltypestime (plan node 0).
    QUERY = """select straight_join *
               from alltypes t1
                    join /*+shuffle*/ alltypestiny t2 on t1.id = t2.id"""
    self.client.set_configuration(vector.get_exec_option_dict())
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
    self.change_database(self.client, vector.get_value('table_format'))
    self.execute_query("SET RUNTIME_FILTER_MODE=GLOBAL")
    self.execute_query("SET RUNTIME_FILTER_WAIT_TIME_MS=10000")
    result = self.execute_query("""select STRAIGHT_JOIN * from alltypes inner join
                                (select * from alltypessmall where smallint_col=-1) v
                                on v.year = alltypes.year""",
                                vector.get_exec_option_dict())
    assert re.search(r"Files rejected: 8 \(8\)", result.runtime_profile) is not None
    assert re.search(r"Splits rejected: [^0] \([^0]\)", result.runtime_profile) is None

  def test_file_filtering_late_arriving_filter(self, vector):
    """Test that late-arriving filters are applied to files when the scanner starts processing
    each scan range."""
    if 'kudu' in str(vector.get_value('table_format')):
      return
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
                                   vector.get_exec_option_dict())
    splits_re = r"Splits rejected: [^0] \([^0]\)"
    assert re.search(splits_re, result.runtime_profile) is not None


@SkipIfLocal.multiple_impalad
class TestBloomFilters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestBloomFilters, cls).add_test_dimensions()
    # Exercise max_num_filters_aggregated_per_host.
    add_exec_option_dimension(cls, 'max_num_filters_aggregated_per_host',
      DIM_MAX_NUM_FILTERS_AGGREGATED_PER_HOST)
    # Bloom filters are disabled on HBase
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format not in ['hbase'])
    # Limit 'max_num_filters_aggregated_per_host' exercise for parquet only.
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet'
      or v.get_value('max_num_filters_aggregated_per_host') == 0)
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  def test_bloom_filters(self, vector):
    self.run_test_case('QueryTest/bloom_filters', vector)


@SkipIfLocal.multiple_impalad
class TestBloomFiltersOnParquet(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestBloomFiltersOnParquet, cls).add_test_dimensions()
    # Only enable bloom filter.
    add_mandatory_exec_option(cls, 'enabled_runtime_filter_types', 'BLOOM')
    # Exercise max_num_filters_aggregated_per_host.
    add_exec_option_dimension(cls, 'max_num_filters_aggregated_per_host',
      DIM_MAX_NUM_FILTERS_AGGREGATED_PER_HOST)
    # Test exclusively on parquet
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  def test_iceberg_dictionary_runtime_filter(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-dictionary-runtime-filter', vector,
      unique_database, test_file_vars=DEFAULT_VARS)

  def test_parquet_dictionary_runtime_filter(self, vector, unique_database):
    vector.set_exec_option('PARQUET_READ_STATISTICS', 'false')
    self.run_test_case('QueryTest/parquet-dictionary-runtime-filter', vector,
      unique_database, test_file_vars=DEFAULT_VARS)

  def test_iceberg_partition_runtime_filter(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-partition-runtime-filter', vector,
      unique_database, test_file_vars=DEFAULT_VARS)


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
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)
    # IMPALA-10715. Enable only min/max since the bloom filters will return
    # rows only satisfying the join predicates. This test requires the return
    # of non-qualifying rows to succeed.
    add_mandatory_exec_option(cls, "ENABLED_RUNTIME_FILTER_TYPES", "MIN_MAX")

  def test_min_max_filters(self, vector):
    self.execute_query("SET MINMAX_FILTER_THRESHOLD=0.5")
    self.run_test_case('QueryTest/min_max_filters', vector, test_file_vars=DEFAULT_VARS)

  def test_decimal_min_max_filters(self, vector):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip("skip decimal min max filter test with various joins")
    self.run_test_case('QueryTest/decimal_min_max_filters', vector,
                       test_file_vars=DEFAULT_VARS)

  def test_large_strings(self, cursor, unique_database):
    """Tests that truncation of large strings by min-max filters still gives correct
    results"""
    table1 = "%s.min_max_filter_large_strings1" % unique_database
    cursor.execute(
        "create table %s (string_col string primary key) stored as kudu" % table1)
    # Min-max bounds are truncated at 1024 characters, so construct some strings that are
    # longer than that, as well as some that are very close to the min/max bounds.
    matching_vals =\
        ('b' * 1100, 'b' * 1099 + 'c', 'd' * 1100, 'f' * 1099 + 'e', 'f' * 1100)
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


@SkipIfEC.different_scan_split
@SkipIfLocal.multiple_impalad
class TestOverlapMinMaxFilters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestOverlapMinMaxFilters, cls).add_test_dimensions()
    # Overlap min-max filters are only implemented for parquet.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  def test_overlap_min_max_filters(self, vector, unique_database):
    self.execute_query("SET MINMAX_FILTER_THRESHOLD=0.5")
    # disable min/max filters on partition columns and allow min/max filters
    # on sorted columns (by default).
    self.execute_query("SET MINMAX_FILTER_PARTITION_COLUMNS=false")
    self.run_test_case('QueryTest/overlap_min_max_filters', vector, unique_database,
                       test_file_vars=DEFAULT_VARS)
    self.execute_query("SET MINMAX_FILTER_PARTITION_COLUMNS=true")

  def test_overlap_min_max_filters_on_sorted_columns(self, vector, unique_database):
    self.run_test_case('QueryTest/overlap_min_max_filters_on_sorted_columns', vector,
                       unique_database, test_file_vars=DEFAULT_VARS)

  def test_overlap_min_max_filters_on_partition_columns(self, vector, unique_database):
    self.run_test_case('QueryTest/overlap_min_max_filters_on_partition_columns', vector,
                       unique_database, test_file_vars=DEFAULT_VARS)

  @SkipIfLocal.hdfs_client
  def test_partition_column_in_parquet_data_file(self, vector, unique_database):
    """IMPALA-11147: Test that runtime min/max filters still work on data files that
    contain the partitioning columns."""
    tbl_name = "part_col_in_data_file"
    self.execute_query("CREATE TABLE {0}.{1} (i INT) PARTITIONED BY (d DATE) "
                       "STORED AS PARQUET".format(unique_database, tbl_name))
    tbl_loc = "%s/%s/%s/d=2022-02-22/" % (WAREHOUSE, unique_database, tbl_name)
    src_loc = (os.environ['IMPALA_HOME']
               + '/testdata/data/partition_col_in_parquet.parquet')
    self.filesystem_client.make_dir(tbl_loc)
    self.filesystem_client.copy_from_local(src_loc, tbl_loc)
    self.execute_query("ALTER TABLE {0}.{1} RECOVER PARTITIONS".format(
        unique_database, tbl_name))
    self.execute_query("SET PARQUET_FALLBACK_SCHEMA_RESOLUTION=NAME")
    self.execute_query("SET ENABLED_RUNTIME_FILTER_TYPES=MIN_MAX")
    self.execute_query("select * from {0}.{1} t1, {0}.{1} t2 where t1.d=t2.d and t2.i=2".
        format(unique_database, tbl_name))


class TestInListFilters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInListFilters, cls).add_test_dimensions()
    # Currently, IN-list filters are only implemented for orc.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format in ['orc'])
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  def test_in_list_filters(self, vector):
    vector.set_exec_option('enabled_runtime_filter_types', 'in_list')
    vector.set_exec_option('runtime_filter_wait_time_ms', WAIT_TIME_MS)
    self.run_test_case('QueryTest/in_list_filters', vector)


# Apply Bloom filter, Minmax filter and IN-list filters
class TestAllRuntimeFilters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAllRuntimeFilters, cls).add_test_dimensions()
    # All filters are only implemented for Kudu now.
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format in ['kudu'])
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  def test_all_runtime_filters(self, vector):
    self.execute_query("SET ENABLED_RUNTIME_FILTER_TYPES=ALL")
    # Disable generating min/max filters for sorted columns
    self.execute_query("SET MINMAX_FILTER_SORTED_COLUMNS=false")
    self.execute_query("SET MINMAX_FILTER_PARTITION_COLUMNS=false")
    self.run_test_case('QueryTest/all_runtime_filters', vector,
                       test_file_vars=DEFAULT_VARS)

  def test_diff_runtime_filter_types(self, vector):
    # compare number of probe rows when apply different types of runtime filter
    self.run_test_case('QueryTest/diff_runtime_filter_types', vector,
                       test_file_vars=DEFAULT_VARS)


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
    add_exec_option_dimension(cls, 'mt_dop', DIM_MT_DOP_MEDIUM)
    # Exercise max_num_filters_aggregated_per_host.
    add_exec_option_dimension(cls, 'max_num_filters_aggregated_per_host',
      DIM_MAX_NUM_FILTERS_AGGREGATED_PER_HOST)
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  def test_row_filters(self, vector):
    # Disable generating min/max filters for sorted columns
    self.execute_query("SET MINMAX_FILTER_SORTED_COLUMNS=false")
    self.execute_query("SET MINMAX_FILTER_PARTITION_COLUMNS=false")
    self.execute_query("SET PARQUET_DICTIONARY_RUNTIME_FILTER_ENTRY_LIMIT=0")
    num_filters_per_host = vector.get_exec_option('max_num_filters_aggregated_per_host')
    num_backend = 2  # exclude coordinator
    num_updates = (num_backend + 1 if num_filters_per_host == 0
        else int(math.ceil(num_backend / num_filters_per_host)))
    vars = {
      '$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS),
      '$NUM_FILTER_UPDATES': str(num_updates)
    }
    self.run_test_case('QueryTest/runtime_row_filters', vector, test_file_vars=vars)


@SkipIfLocal.multiple_impalad
class TestRuntimeRowFilterReservation(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRuntimeRowFilterReservation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  def test_row_filter_reservation(self, vector):
    """Test handling of runtime filter memory reservations. Tuned for mt_dop=0."""
    self.run_test_case('QueryTest/runtime_row_filter_reservations', vector,
                       test_file_vars=DEFAULT_VARS)


class TestRuntimeFiltersLateRemoteUpdate(ImpalaTestSuite):
  """Test that distributed runtime filter aggregation still works
  when remote filter update is late to reach the intermediate aggregator."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRuntimeFiltersLateRemoteUpdate, cls).add_test_dimensions()
    # Only run this test in parquet
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format in ['parquet'])
    # Set mandatory query options.
    add_mandatory_exec_option(cls, 'max_num_filters_aggregated_per_host', 2)
    add_mandatory_exec_option(cls, 'runtime_filter_wait_time_ms', WAIT_TIME_MS // 10)
    add_mandatory_exec_option(cls, 'debug_action',
        'REMOTE_FILTER_UPDATE_DELAY:SLEEP@%d' % WAIT_TIME_MS)
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  def test_late_remote_update(self, vector):
    """Test that a query with global filters does not wait for late remote filter
    update."""
    query = ('select count(*) from functional.alltypes p '
             'join [SHUFFLE] functional.alltypestiny b '
             'on p.month = b.int_col and b.month = 1 and b.string_col = "1"')
    now = time.time()
    exec_options = vector.get_value('exec_option')
    result = self.execute_query_expect_success(self.client, query, exec_options)
    assert result.data[0] == '620'
    duration_s = time.time() - now
    assert duration_s < (WAIT_TIME_MS / 1000), \
        "Query took too long (%ss, possibly waiting for late filters?)" \
        % str(duration_s)
