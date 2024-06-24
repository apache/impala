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

from beeswaxd.BeeswaxService import QueryState
from getpass import getuser
from signal import SIGRTMIN
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import DEFAULT_KRPC_PORT
from tests.util.workload_management import assert_query
from time import sleep


class TestQueryLive(CustomClusterTestSuite):
  """Tests to assert the query live table is correctly populated."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  def setup_method(self, method):
    super(TestQueryLive, self).setup_method(method)
    create_match = self.assert_impalad_log_contains("INFO", r'\]\s+(\w+:\w+)\]\s+'
        r'Analyzing query: CREATE EXTERNAL TABLE IF NOT EXISTS sys.impala_query_live',
        timeout_s=60)
    self.assert_impalad_log_contains("INFO", r'Query successfully unregistered: '
        r'query_id={}'.format(create_match.group(1)),
        timeout_s=60)

  def assert_describe_extended(self):
    describe_ext_result = self.execute_query('describe extended sys.impala_query_live')
    assert len(describe_ext_result.data) == 80
    system_table_re = re.compile(r'__IMPALA_SYSTEM_TABLE\s+true')
    assert list(filter(system_table_re.search, describe_ext_result.data))
    external_re = re.compile(r'EXTERNAL\s+TRUE')
    assert list(filter(external_re.search, describe_ext_result.data))
    external_table_re = re.compile(r'Table Type:\s+EXTERNAL_TABLE')
    assert list(filter(external_table_re.search, describe_ext_result.data))

  def assert_impalads(self, profile, present=[0, 1, 2], absent=[]):
    for port_idx in present:
      assert ":" + str(DEFAULT_KRPC_PORT + port_idx) + ":" in profile
    for port_idx in absent:
      assert ":" + str(DEFAULT_KRPC_PORT + port_idx) not in profile

  def assert_only_coordinators(self, profile, coords=[0, 1], execs=[2]):
    self.assert_impalads(profile, coords, execs)
    assert "SYSTEM_TABLE_SCAN_NODE (id=0) [{} instances]".format(len(coords)) in profile

  def assert_fragment_instances(self, profile, expected):
    """Asserts that the per host number of fragment instances is as expected."""
    hosts = ['{}({})'.format(DEFAULT_KRPC_PORT + i, expect)
             for i, expect in enumerate(expected)]
    actual_hosts = re.search(r'Per Host Number of Fragment Instances: (.*)', profile)
    assert actual_hosts is not None
    # Split and remove hostname
    actual_hosts = [host.split(':')[1] for host in actual_hosts.group(1).split(' ')]
    assert len(hosts) == len(actual_hosts)
    for host in hosts:
      if host in actual_hosts:
        actual_hosts.remove(host)
      else:
        assert False, "did not find host {}".format(host)
    assert len(actual_hosts) == 0, "did not find all expected hosts"

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt")
  def test_table_structure(self, vector):
    """Asserts that the live table has the expected columns."""
    self.run_test_case('QueryTest/workload-management-live', vector)

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt")
  def test_query_live(self):
    """Asserts the query live table shows and allows filtering queries."""
    # Use a query that reads data from disk for the 1st one, as more representative and a
    # better fit for assert_query.
    result1 = self.client.execute("select * from functional.alltypes",
        fetch_profile_after_close=True)
    assert_query('sys.impala_query_live', self.client, 'test_query_live',
                 result1.runtime_profile)

    # pending queries
    result2 = self.execute_query(
        "select query_id, cluster_id from sys.impala_query_live order by start_time_utc")
    assert len(result2.data) == 3
    assert "{}\t{}".format(result1.query_id, "test_query_live") == result2.data[0]
    assert "{}\t{}".format(result2.query_id, "test_query_live") == result2.data[2]
    # Expect new metrics for the impala_query_live table scanner.
    assert "ActiveQueryCollectionTime: " in result2.runtime_profile
    assert "PendingQueryCollectionTime: " in result2.runtime_profile

    # query filtering
    result3 = self.execute_query(
        "select query_id from sys.impala_query_live "
        "where total_time_ms > 0.0 order by start_time_utc")
    assert len(result3.data) == 4
    assert result1.query_id == result3.data[0]
    assert result2.query_id == result3.data[2]
    assert result3.query_id == result3.data[3]

    result4 = self.execute_query(
        "select db_name, db_user, count(*) as query_count from sys.impala_query_live "
        "group by db_name, db_user order by db_name")
    assert len(result4.data) == 1
    assert "default\t{}\t5".format(getuser()) == result4.data[0]

    result5 = self.execute_query(
        'select * from sys.impala_query_live where cluster_id = "test_query_live_0"')
    assert len(result5.data) == 0

    result = self.execute_query("""
        select count(*) from functional.alltypestiny a
          inner join functional.alltypes b on a.id = b.id
          inner join functional.alltypessmall c on b.id = c.id
    """)
    result5 = self.execute_query(
        'select tables_queried from sys.impala_query_live where query_id = "'
        + result.query_id + '"')
    assert len(result5.data) == 1
    assert result5.data[0] == \
        "functional.alltypes,functional.alltypestiny,functional.alltypessmall"

    # describe query
    describe_result = self.execute_query('describe sys.impala_query_live')
    assert len(describe_result.data) == 49
    self.assert_describe_extended()

    # show create table
    show_create_tbl = self.execute_query('show create table sys.impala_query_live')
    assert len(show_create_tbl.data) == 1
    assert 'CREATE EXTERNAL TABLE sys.impala_query_live' in show_create_tbl.data[0]
    assert "'__IMPALA_SYSTEM_TABLE'='true'" in show_create_tbl.data[0]

    # cannot compute stats or perform write operations
    compute_stats_result = self.execute_query_expect_failure(self.client,
        'compute stats sys.impala_query_live')
    assert 'AnalysisException: COMPUTE STATS not supported for system table: '\
        'sys.impala_query_live' in str(compute_stats_result)

    create_result = self.execute_query_expect_failure(self.client,
        'create table sys.impala_query_live (i int)')
    assert 'AnalysisException: Table already exists: sys.impala_query_live'\
        in str(create_result)

    insert_result = self.execute_query_expect_failure(self.client,
        'insert into sys.impala_query_live select * from sys.impala_query_live limit 1')
    assert 'UnsupportedOperationException: Cannot create data sink into table of type: '\
        'org.apache.impala.catalog.SystemTable' in str(insert_result)

    update_result = self.execute_query_expect_failure(self.client,
        'update sys.impala_query_live set query_id = ""')
    assert 'AnalysisException: Impala only supports modifying Kudu and Iceberg tables, '\
        'but the following table is neither: sys.impala_query_live'\
        in str(update_result)

    delete_result = self.execute_query_expect_failure(self.client,
        'delete from sys.impala_query_live')
    assert 'AnalysisException: Impala only supports modifying Kudu and Iceberg tables, '\
        'but the following table is neither: sys.impala_query_live'\
        in str(delete_result)

    # Drop table at the end, it's only recreated on impalad startup.
    self.execute_query_expect_success(self.client, 'drop table sys.impala_query_live')

  # Must come directly after "drop table sys.impala_query_live"
  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live "
                                                 "--use_local_catalog=true",
                                    catalogd_args="--enable_workload_mgmt "
                                                  "--catalog_topic_mode=minimal",
                                    default_query_options=[
                                      ('default_transactional_type', 'insert_only')])
  def test_default_transactional(self):
    """Asserts the query live table works when impala is started with
    default_transactional_type=insert_only."""
    result = self.client.execute("select * from functional.alltypes",
        fetch_profile_after_close=True)
    assert_query('sys.impala_query_live', self.client, 'test_query_live',
                 result.runtime_profile)
    self.assert_describe_extended()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live "
                                                 "--use_local_catalog=true",
                                    catalogd_args="--enable_workload_mgmt "
                                                  "--catalog_topic_mode=minimal")
  def test_local_catalog(self):
    """Asserts the query live table works with local catalog mode."""
    result = self.client.execute("select * from functional.alltypes",
        fetch_profile_after_close=True)
    assert_query('sys.impala_query_live', self.client, 'test_query_live',
                 result.runtime_profile)

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt",
                                    cluster_size=3,
                                    num_exclusive_coordinators=2)
  def test_dedicated_coordinators(self):
    """Asserts scans are performed only on coordinators."""
    # Use a query that reads data from disk for the 1st one, as more representative and a
    # better fit for assert_query.
    result = self.client.execute("select * from functional.alltypes",
        fetch_profile_after_close=True)
    assert_query('sys.impala_query_live', self.client, 'test_query_live',
        result.runtime_profile)

    client2 = self.create_client_for_nth_impalad(1)
    query = "select query_id, impala_coordinator from sys.impala_query_live " \
            "order by start_time_utc"
    handle1 = self.execute_query_async(query)
    handle2 = client2.execute_async(query)
    result1 = self.client.fetch(query, handle1)
    result2 = client2.fetch(query, handle2)
    assert len(result1.data) == 4
    assert result1.data == result2.data

    profile1 = self.client.get_runtime_profile(handle1)
    self.assert_only_coordinators(profile1)
    profile2 = client2.get_runtime_profile(handle2)
    self.assert_only_coordinators(profile2)

    self.close_query(handle1)
    client2.close_query(handle2)
    client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt",
                                    cluster_size=3,
                                    num_exclusive_coordinators=2)
  def test_executor_groups(self):
    """Asserts scans are performed only on coordinators with multiple executor groups."""
    # Add a (non-dedicated) coordinator and executor in a different executor group.
    self._start_impala_cluster(options=['--impalad_args=--executor_groups=extra'],
                               cluster_size=1,
                               add_executors=True,
                               expected_num_impalads=4)

    result = self.client.execute(
        "select query_id, impala_coordinator from sys.impala_query_live",
        fetch_profile_after_close=True)
    assert len(result.data) == 1
    self.assert_only_coordinators(result.runtime_profile, coords=[0, 1], execs=[2, 3])

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt")
  def test_query_entries_are_unique(self):
    """Asserts queries in the query live table are unique."""
    # Start a query and close it with a delay between CloseClientRequestState and
    # Unregister. Then query sys.impala_query_live from multiple coordinators.
    client2 = self.create_client_for_nth_impalad(1)

    async_handle = self.execute_query_async("select count(*) from functional.alltypes",
        {"debug_action": "CLOSED_NOT_UNREGISTERED:SLEEP@1000"})
    self.close_query(async_handle)

    query = "select * from sys.impala_query_live order by start_time_utc"
    handle = self.execute_query_async(query)
    handle2 = client2.execute_async(query)
    result = self.client.fetch(query, handle)
    result2 = client2.fetch(query, handle2)
    assert len(result.data) == 3
    assert result.data[0] == result2.data[0]

    def remove_dynamic_fields(fields):
      # Excludes QUERY_STATE, IMPALA_QUERY_END_STATE, QUERY_TYPE, TOTAL_TIME_MS, and
      # everything after QUERY_OPTS_CONFIG as they change over the course of compiling
      # and running the query.
      return fields[:10] + fields[13:15] + fields[16:17]

    # Compare cluster_id and query_id. Not all fields will match as they're queried on a
    # live query at different times. Order is only guaranteed with minicluster on a
    # single machine, where they use the same clock.
    data1a = remove_dynamic_fields(result.data[1].split('\t'))
    data1b = remove_dynamic_fields(result.data[2].split('\t'))
    data2a = remove_dynamic_fields(result2.data[1].split('\t'))
    data2b = remove_dynamic_fields(result2.data[2].split('\t'))
    assert data1a == data2a
    assert data1b == data2b

    self.close_query(handle)
    client2.close_query(handle2)
    client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt",
                                    cluster_size=3,
                                    num_exclusive_coordinators=2)
  def test_missing_coordinator(self):
    """Asserts scans finish if a coordinator disappears mid-schedule. Depends on
    test config of statestore_heartbeat_frequency_ms=50."""
    query = "select query_id, impala_coordinator from sys.impala_query_live"
    handle = self.execute_query_async(query, query_options={
        'debug_action': 'AC_BEFORE_ADMISSION:SLEEP@3000'})

    # Wait for query to compile and assign ranges, then kill impalad during debug delay.
    self.wait_for_any_state(handle, [QueryState.COMPILED], 3)
    self.cluster.impalads[1].kill()

    result = self.client.fetch(query, handle)
    assert len(result.data) == 1
    expected_message = 'is no longer available for system table scan assignment'
    self.assert_impalad_log_contains('WARNING', expected_message)
    assert expected_message in self.client.get_runtime_profile(handle)

    self.close_query(handle)

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt")
  def test_shutdown_coordinator(self):
    """Asserts query fails if a coordinator disappears after scheduling. Depends on
    test config of statestore_heartbeat_frequency_ms=50."""
    query = "select query_id, impala_coordinator from sys.impala_query_live"
    handle = self.execute_query_async(query, query_options={
        'debug_action': 'CRS_BEFORE_COORD_STARTS:SLEEP@3000'})

    # Wait for query to compile.
    self.wait_for_any_state(handle, [QueryState.COMPILED], 3)
    # Ensure enough time for scheduling to assign ranges.
    sleep(1)
    # Kill impalad during debug delay.
    self.cluster.impalads[1].kill()

    try:
      self.client.fetch(query, handle)
      assert False, "fetch should fail"
    except Exception as e:
      assert "Network error: Client connection negotiation failed" in str(e)
    # Beeswax client closes the query on failure.

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt")
  def test_graceful_shutdown_coordinator(self):
    """Asserts query succeeds if another coordinator is shutdown gracefully after
    scheduling. Depends on test config of statestore_heartbeat_frequency_ms=50."""
    query = "select query_id from sys.impala_query_live"
    handle = self.execute_query_async(query, query_options={
        'debug_action': 'CRS_BEFORE_COORD_STARTS:SLEEP@3000'})

    # Wait for query to compile and assign ranges, then gracefully shutdown impalad.
    self.wait_for_any_state(handle, [QueryState.COMPILED], 3)
    self.cluster.impalads[1].kill(SIGRTMIN)

    self.wait_for_any_state(handle, [QueryState.FINISHED], 10)
    # Allow time for statestore update to propagate. Shutdown grace period is 120s.
    sleep(1)
    # Coordinator in graceful shutdown should not be scheduled in new queries.
    shutdown = self.execute_query(query)

    result = self.client.fetch(query, handle)
    assert len(result.data) == 1
    assert result.query_id == result.data[0]
    self.client.get_runtime_profile(handle)
    self.assert_impalads(self.client.get_runtime_profile(handle))
    self.close_query(handle)

    assert len(shutdown.data) == 2
    self.assert_impalads(shutdown.runtime_profile, present=[0, 2], absent=[1])

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True,
                                    cluster_size=3,
                                    num_exclusive_coordinators=2)
  def test_multi_table_union(self):
    """Asserts only system table scan fragments are scheduled to coordinators."""
    utc_timestamp = self.execute_query('select utc_timestamp()')
    assert len(utc_timestamp.data) == 1
    start_time = utc_timestamp.data[0]

    # Wait for the query to be written to the log to ensure there's something in it."
    logged = self.execute_query_expect_success(self.client,
        'select count(*) from functional.alltypes')
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 2, 30, allow_greater=True)

    query = """select query_id from
        (select query_id, start_time_utc from sys.impala_query_live live
          where start_time_utc > "{0}" union
         select query_id, start_time_utc from sys.impala_query_log
          where start_time_utc > "{0}")
        as history order by start_time_utc""".format(start_time)
    result = self.client.execute(query, fetch_profile_after_close=True)
    assert len(result.data) == 3
    assert utc_timestamp.query_id == result.data[0]
    assert logged.query_id == result.data[1]
    assert result.query_id == result.data[2]

    # Unions run in a single fragment, and on the union of selected scan nodes (even
    # though some may not have actual scan ranges to evaluate). So all nodes are involved
    # in scans.
    self.assert_fragment_instances(result.runtime_profile, [3, 2, 2])

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--cluster_id=test_query_live",
                                    catalogd_args="--enable_workload_mgmt",
                                    cluster_size=3,
                                    num_exclusive_coordinators=2)
  def test_multi_table_join(self, unique_database):
    """Asserts only system table scan fragments are scheduled to coordinators."""
    self.execute_query('create table {}.users (user string)'.format(unique_database))
    self.execute_query('insert into {}.users values ("alice"), ("bob"), ("{}")'
                       .format(unique_database, getuser()))

    result = self.client.execute('select straight_join count(*) from {}.users, '
        'sys.impala_query_live where user = db_user and start_time_utc > utc_timestamp()'
        .format(unique_database), fetch_profile_after_close=True)
    assert len(result.data) == 1
    assert '1' == result.data[0]

    # HDFS scan runs on 1 node, System Table scan on 2
    assert 2 == result.runtime_profile.count('HDFS_SCAN_NODE')
    assert 3 == result.runtime_profile.count('SYSTEM_TABLE_SCAN_NODE')

    # impala_query_live is assigned to build side, so executor has HDFS scan fragment and
    # aggregation, coordinators have System Table scan fragments, and initial coordinator
    # has the root fragment.
    self.assert_fragment_instances(result.runtime_profile, [2, 1, 1])
