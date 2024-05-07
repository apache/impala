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
from collections import defaultdict
from datetime import datetime
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfLocal, SkipIfNotHdfsMinicluster
from tests.util.filesystem_utils import IS_EC, WAREHOUSE
from tests.util.parse_util import get_duration_us_from_str
from time import sleep, time
from RuntimeProfile.ttypes import TRuntimeProfileFormat
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

  @SkipIfFS.hbase
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
        "RUNTIME_FILTER_MODE=OFF,MT_DOP=0,TIMEZONE={timezone},"
        "CLIENT_IDENTIFIER="
        "query_test/test_observability.py::TestObservability::()::test_query_options,"
        "SPOOL_QUERY_RESULTS=0"
        "\n")
    expected_str = expected_str.format(timezone=server_timezone)
    assert expected_str in profile, profile

  def test_profile(self):
    """Test that expected fields are populated in the profile."""
    query = """select count(distinct a.int_col) from functional.alltypes a
        inner join functional.alltypessmall b on (a.id = b.id + cast(sleep(15) as INT))"""
    result = self.execute_query(query)

    assert "Query Type: QUERY" in result.runtime_profile
    assert "Query State: " in result.runtime_profile
    assert "Default Db: default" in result.runtime_profile
    tables = re.search(r'\n\s+Tables Queried:\s+(.*?)\n', result.runtime_profile)
    assert tables is not None
    assert sorted(tables.group(1).split(",")) \
        == ["functional.alltypes", "functional.alltypessmall"]

  def test_exec_summary(self):
    """Test that the exec summary is populated correctly in every query state"""
    query = "select count(*) from functional.alltypes"
    handle = self.execute_query_async(query,
        {"debug_action": "AC_BEFORE_ADMISSION:SLEEP@1000"})
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
    # Verify the query is complete.
    assert exec_summary.progress.num_completed_scan_ranges == \
        exec_summary.progress.total_scan_ranges
    assert exec_summary.progress.num_completed_fragment_instances == \
        exec_summary.progress.total_fragment_instances

  def test_exec_summary_in_runtime_profile(self):
    """Test that the exec summary is populated in runtime profile correctly in every
    query state"""
    query = "select count(*) from functional.alltypes"
    handle = self.execute_query_async(query,
        {"debug_action": "AC_BEFORE_ADMISSION:SLEEP@1000"})

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
    query_opts = {'report_skew_limit': -1}
    results = self.execute_query("""
        with l as (select * from tpch.lineitem UNION ALL select * from tpch.lineitem)
        select STRAIGHT_JOIN count(*) from (select * from tpch.lineitem a LIMIT 1) a
        join (select * from l LIMIT 2000000) b on a.l_orderkey = -b.l_orderkey;""",
        query_opts)
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

  def test_query_profile_contains_query_compilation_static_events(self):
    """Test that the expected events show up in a query profile. These lines are static
    and should appear in this exact order."""
    event_regexes = [
        r'Analysis finished:',
        r'Authorization finished (.*):',
        r'Value transfer graph computed:',
        r'Single node plan created:',
        r'Runtime filters computed:',
        r'Distributed plan created:']
    query = "select * from functional.alltypes"
    runtime_profile = self.execute_query(query).runtime_profile
    self.__verify_profile_event_sequence(event_regexes, runtime_profile)

  def test_query_profile_contains_query_compilation_metadata_load_events(self,
        cluster_properties):
    """Test that the Metadata load started and finished events appear in the query
    profile when Catalog cache is evicted."""
    invalidate_query = "invalidate metadata functional.alltypes"
    select_query = "select * from functional.alltypes"
    self.execute_query(invalidate_query).runtime_profile
    runtime_profile = self.execute_query(select_query).runtime_profile
    # Depending on whether this is a catalog-v2 cluster or not some of the metadata
    # loading events are different
    if not cluster_properties.is_catalog_v2_cluster():
      load_event_regexes = [r'Query Compilation:', r'Metadata load started:',
          r'Metadata load finished. loaded-tables=.*/.* load-requests=.* '
            r'catalog-updates=.*:',
          r'Analysis finished:']
    else:
      load_event_regexes = [
        r'Frontend:',
        r'Referenced Tables:',
        r'CatalogFetch.ColumnStats.Hits',
        r'CatalogFetch.ColumnStats.Misses',
        r'CatalogFetch.ColumnStats.Requests',
        r'CatalogFetch.ColumnStats.Time',
        r'CatalogFetch.Config.Hits',
        r'CatalogFetch.Config.Misses',
        r'CatalogFetch.Config.Requests',
        r'CatalogFetch.Config.Time',
        r'CatalogFetch.DatabaseList.Hits',
        r'CatalogFetch.DatabaseList.Misses',
        r'CatalogFetch.DatabaseList.Requests',
        r'CatalogFetch.DatabaseList.Time',
        r'CatalogFetch.PartitionLists.Hits',
        r'CatalogFetch.PartitionLists.Misses',
        r'CatalogFetch.PartitionLists.Requests',
        r'CatalogFetch.PartitionLists.Time',
        r'CatalogFetch.Partitions.Hits',
        r'CatalogFetch.Partitions.Misses',
        r'CatalogFetch.Partitions.Requests',
        r'CatalogFetch.Partitions.Time',
        r'CatalogFetch.RPCs.Bytes',
        r'CatalogFetch.RPCs.Requests',
        r'CatalogFetch.RPCs.Time',
        r'CatalogFetch.StorageLoad.Time',
        r'CatalogFetch.TableList.Hits',
        r'CatalogFetch.TableList.Misses',
        r'CatalogFetch.TableList.Requests',
        r'CatalogFetch.TableList.Time',
        r'CatalogFetch.Tables.Hits',
        r'CatalogFetch.Tables.Misses',
        r'CatalogFetch.Tables.Requests',
        r'CatalogFetch.Tables.Time']
    self.__verify_profile_event_sequence(load_event_regexes, runtime_profile)

  def test_query_profile_contains_query_compilation_metadata_cached_event(self):
    """Test that the Metadata cache available event appears in the query profile when
    the table is cached."""
    refresh_query = "refresh functional.alltypes"
    select_query = "select * from functional.alltypes"
    self.execute_query(refresh_query).runtime_profile
    runtime_profile = self.execute_query(select_query).runtime_profile
    event_regexes = [r'Query Compilation:',
        r'Metadata of all .* tables cached:',
        r'Analysis finished:']
    self.__verify_profile_event_sequence(event_regexes, runtime_profile)

  def test_query_profile_contains_query_compilation_lineage_event(self):
    """Test that the lineage information appears in the profile in the right place. This
    event depends on whether the lineage_event_log_dir is configured."""
    impalad = self.impalad_test_service
    lineage_event_log_dir_value = impalad.get_flag_current_value("lineage_event_log_dir")
    assert lineage_event_log_dir_value is not None
    if lineage_event_log_dir_value == "":
      event_regexes = [
        r'Distributed plan created:',
        r'Planning finished:']
    else:
      event_regexes = [
        r'Distributed plan created:',
        r'Lineage info computed:',
        r'Planning finished:']
    # Disable auto-scaling as it can produce multiple event sequences in event_regexes
    self.execute_query_expect_success(self.client, "set enable_replan=0")
    query = "select * from functional.alltypes"
    runtime_profile = self.execute_query(query).runtime_profile
    self.__verify_profile_event_sequence(event_regexes, runtime_profile)

  def test_query_profile_contains_query_timeline_events(self):
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
    # Use no-limits pool so that it cannot get queued in admission control (which would
    # add an extra event to the above timeline).
    query_opts = {'request_pool': 'root.no-limits'}
    runtime_profile = self.execute_query(query, query_opts).runtime_profile
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
    path = "{1}/{0}.db/data_file".format(unique_database, WAREHOUSE)
    self.filesystem_client.create_file(path, "1")
    use_query = "use {0}".format(unique_database)
    self.execute_query(use_query)
    # all the events we will see for every query
    event_regexes = [
      r'Query Compilation:',
      r'Query Timeline:',
      r'Planning finished'
    ]
    # Events for CreateTable DDLs.
    create_table_ddl_event_regexes = [
      r'Catalog Server Operation:',
      r'Got metastoreDdlLock:',
      r'Created table in Metastore:',
      r'Created table in catalog cache:',
      r'DDL finished:',
    ]
    # Events for AlterTable DDLs.
    alter_table_ddl_event_regexes = [
      r'Catalog Server Operation:',
      r'Got catalog version read lock:',
      r'Got catalog version write lock and table write lock:',
      r'Altered table in Metastore:',
      r'Fetched table from Metastore:',
      r'Loaded table schema:',
      r'DDL finished:'
    ]
    # Events for LOAD statement.
    load_data_ddl_event_regexes = [
      r'Catalog Server Operation:',
      r'Got catalog version read lock:',
      r'Got catalog version write lock and table write lock:',
      r'Fired Metastore events:',
      r'Fetched table from Metastore:',
      r'Loaded file metadata for 1 partitions:',
      r'Finished updateCatalog request:'
    ]
    # queries that explore different code paths in Frontend compilation
    queries = [
      'create table if not exists impala_6568 (i int)',
      'select * from impala_6568',
      'explain select * from impala_6568',
      'describe impala_6568',
      'alter table impala_6568 set tblproperties(\'numRows\'=\'10\')',
      "load data inpath '{0}' into table impala_6568".format(path)
    ]
    # run each query...
    for query in queries:
      runtime_profile = self.execute_query(query).runtime_profile
      # and check that all the expected events appear in the resulting profile
      self.__verify_profile_contains_every_event(event_regexes, runtime_profile, query)
      # Verify catalogOp timeline
      if query.startswith("create table"):
        self.__verify_profile_contains_every_event(
            create_table_ddl_event_regexes, runtime_profile, query)
      elif query.startswith("alter table"):
        self.__verify_profile_contains_every_event(
            alter_table_ddl_event_regexes, runtime_profile, query)
      elif query.startswith("load data"):
        self.__verify_profile_contains_every_event(
            load_data_ddl_event_regexes, runtime_profile, query)

  def __verify_profile_contains_every_event(self, event_regexes, runtime_profile, query):
    """Test that all the expected events show up in a given query profile."""
    for regex in event_regexes:
      assert any(re.search(regex, line) for line in runtime_profile.splitlines()), \
          "Didn't find event '" + regex + "' for query '" + query + \
          "' in profile: \n" + runtime_profile

  def test_create_table_profile_events(self, unique_database):
    """Test that specific DDL timeline event labels exist in the profile. Note that
       labels of HMS events are not used so this test is expected to pass with or without
       event processor enabled"""
    # Create normal table
    stmt = "create table %s.t1(id int) partitioned by (p int)" % unique_database
    self.__verify_event_labels_in_profile(stmt, [
        "Got metastoreDdlLock",
        "Got Metastore client",
        "Created table in Metastore",
        "Created table in catalog cache",
        "DDL finished"
    ])
    # Create Kudu table
    stmt = "create table %s.t2(id int, name string, primary key(id))" \
           " partition by hash(id) partitions 3 stored as kudu" % unique_database
    self.__verify_event_labels_in_profile(stmt, [
        "Got Metastore client",
        "Got Kudu client",
        "Got kuduDdlLock",
        "Checked table existence in Kudu",
        "Created table in Kudu",
        "Got metastoreDdlLock",
        "Got Metastore client",
        "Checked table existence in Metastore",
        "Created table in Metastore",
        "Created table in catalog cache",
        "DDL finished",
    ])
    # Create Iceberg table
    stmt = "create table %s.t3(id int) stored as iceberg" % unique_database
    self.__verify_event_labels_in_profile(stmt, [
        "Got Metastore client",
        "Checked table existence in Metastore",
        "Created table using Iceberg Catalog HIVE_CATALOG",
        "Created table in catalog cache",
        "DDL finished",
    ])
    # INSERT into table
    stmt = "insert into %s.t1 partition(p) values (0,0), (1,1)" % unique_database
    self.__verify_event_labels_in_profile(stmt, [
        "Got Metastore client",
        "Added 2 partitions in Metastore",
        "Loaded file metadata for 2 partitions",
        "Finished updateCatalog request"
    ])
    # Compute stats
    stmt = "compute stats %s.t1" % unique_database
    self.__verify_event_labels_in_profile(stmt, [
        "Got Metastore client",
        "Updated column stats",
        "Altered 2 partitions in Metastore",
        "Altered table in Metastore",
        "Fetched table from Metastore",
        "Loaded file metadata for 2 partitions",
        "DDL finished"
    ])
    # REFRESH
    stmt = "refresh %s.t1" % unique_database
    self.__verify_event_labels_in_profile(stmt, [
      "Got Metastore client",
      "Fetched table from Metastore",
      "Loaded file metadata for 2 partitions",
      "Finished resetMetadata request"
    ])
    # Drop table
    stmt = "drop table %s.t1" % unique_database
    self.__verify_event_labels_in_profile(stmt, [
        "Got Metastore client",
        "Dropped table in Metastore",
        "Deleted table in catalog cache",
        "DDL finished"
    ])

  def __verify_event_labels_in_profile(self, stmt, event_labels):
    profile = self.execute_query(stmt).runtime_profile
    for label in event_labels:
      assert label in profile, "Missing '%s' in runtime profile:\n%s" % (label, profile)

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

    # periodic_counter_update_period_ms is 500 ms by default. Use sleep(50) with limit 10
    # to ensure that there is at least one sample in sampling counters.
    query = """select count(*), sleep(50) from tpch_parquet.orders o
        inner join tpch_parquet.lineitem l on o.o_orderkey = l.l_orderkey
        group by o.o_clerk limit 10"""
    profile = self.execute_query(query).runtime_profile

    # TimeSeriesCounter should be prefixed with a hyphen.
    assert "  MemoryUsage" not in profile
    assert "- MemoryUsage" in profile

    # Usually 4.63. Ignoring last digit to deflake test (IMPALA-12500).
    assert "ExchangeScanRatio: 4.6" in profile

    keys = ["TotalBytesSent", "TotalScanBytesSent", "TotalInnerBytesSent"]
    counters = defaultdict(int)
    for line in profile.splitlines():
      for key in keys:
        if key in line:
          # Match byte count within parentheses
          m = re.search("\(([0-9]+)\)", line)

          # If a match was not found, then the value of the key should be 0
          if not m:
            assert key + ": 0" in line, "Invalid format for key %s" % key
            assert counters[key] != 0, "Query level counter for key %s cannot be 0" % key
          elif counters[key] == 0:
            # Only keep first (query-level) counter
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
    """Tests that the query profile does not contain resource usage metrics
       when disabled explicitly."""
    query = "select count(*), sleep(1000) from functional.alltypes"
    for query_opts in [{'resource_trace_ratio': 0.0}]:
      profile = self.execute_query(query, query_opts).runtime_profile
      # Assert that no host resource counters exist in the profile
      for line in profile.splitlines():
        assert not re.search("HostCpu.*Percentage", line)
        assert not re.search("HostNetworkRx", line)
        assert not re.search("HostDiskReadThroughput", line)

  def test_query_profile_contains_host_resource_metrics(self):
    """Tests that the query profile contains various CPU and network metrics
       by default or when enabled explicitly."""
    query = "select count(*), sleep(1000) from functional.alltypes"
    for query_opts in [{}, {'resource_trace_ratio': 1.0}]:
      profile = self.execute_query(query, query_opts).runtime_profile
      # We check for 50ms because a query with 1s duration won't hit the 64 values limit
      # that would trigger resampling.
      expected_strs = ["HostCpuIoWaitPercentage (50.000ms):",
                     "HostCpuSysPercentage (50.000ms):",
                     "HostCpuUserPercentage (50.000ms):",
                     "HostNetworkRx (50.000ms):",
                     "HostNetworkTx (50.000ms):",
                     "HostDiskReadThroughput (50.000ms):",
                     "HostDiskWriteThroughput (50.000ms):"]

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
  def test_query_end_time(self):
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

  def test_dml_end_time(self, unique_database):
    """Same as the above test but verifies the end time of DML is set only after the work
    in catalogd is done."""
    stmt = "create table %s.alltypestiny like functional.alltypestiny" % unique_database
    self.execute_query(stmt)
    # Warm up the table
    self.execute_query("describe %s.alltypestiny" % unique_database)
    stmt = "insert overwrite %s.alltypestiny partition(year, month)" \
           " select * from functional.alltypestiny" % unique_database
    # Use debug_action to inject a delay in catalogd. The INSERT usually finishes in
    # 300ms without the delay.
    delay_s = 5
    self.hs2_client.set_configuration_option(
        "debug_action", "catalogd_insert_finish_delay:SLEEP@%d" % (delay_s * 1000))
    start_ts = time()
    handle = self.hs2_client.execute_async(stmt)
    self.hs2_client.clear_configuration()
    end_time_str = ""
    duration_str = ""
    while len(end_time_str) == 0:
      sleep(1)
      tree = self.hs2_client.get_runtime_profile(handle, TRuntimeProfileFormat.THRIFT)
      end_time_str = tree.nodes[1].info_strings["End Time"]
      duration_str = tree.nodes[1].info_strings["Duration"]
      # End time should not show up earlier than the delay.
      if time() - start_ts < delay_s:
        assert len(end_time_str) == 0, "End time show up too early: {end_str}. " \
                                       "{delay_s} second delay expected since " \
                                       "{start_str} ({start_ts:.6f})".format(
          end_str=end_time_str, delay_s=delay_s, start_ts=start_ts,
          start_str=datetime.utcfromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M:%S.%f'))
    self.hs2_client.close_query(handle)
    duration_us = get_duration_us_from_str(duration_str)
    assert duration_us > delay_s * 1000000

  def test_query_profile_contains_number_of_fragment_instance(self):
    """Test that the expected section for number of fragment instance in
    a query profile."""
    event_regexes = [r'Per Host Number of Fragment Instances']
    query = "select count (*) from functional.alltypes"
    runtime_profile = self.execute_query(query).runtime_profile
    self.__verify_profile_event_sequence(event_regexes, runtime_profile)

  def test_query_profile_contains_executor_group(self):
    """Test that the profile contains an info string with the executor group that was
    picked by admission control."""
    query = "select count (*) from functional.alltypes"
    runtime_profile = self.execute_query(query).runtime_profile
    assert "Executor Group:" in runtime_profile

  def test_query_profile_storage_load_time_filesystem(self, unique_database,
      cluster_properties):
    """Test that when a query needs load metadata for table(s), the
    storage load time should be in the profile. Tests file systems."""
    table_name = 'ld_prof'
    self.execute_query(
        "create table {0}.{1}(col1 int)".format(unique_database, table_name))
    self.__check_query_profile_storage_load_time(unique_database, table_name,
        cluster_properties)

  @SkipIfFS.hbase
  @pytest.mark.execute_serially
  def test_query_profile_storage_load_time(self, cluster_properties):
    """Test that when a query needs load metadata for table(s), the
    storage load time should be in the profile. Tests kudu and hbase."""
    # KUDU table
    self.__check_query_profile_storage_load_time("functional_kudu", "alltypes",
        cluster_properties)

    # HBASE table
    self.__check_query_profile_storage_load_time("functional_hbase", "alltypes",
        cluster_properties)

  def __check_query_profile_storage_load_time(self, db_name, table_name,
      cluster_properties):
    """Check query profile for storage load time with a given database."""
    self.execute_query("invalidate metadata {0}.{1}".format(db_name, table_name))
    query = "select count (*) from {0}.{1}".format(db_name, table_name)
    runtime_profile = self.execute_query(query).runtime_profile
    if cluster_properties.is_catalog_v2_cluster():
      storageLoadTime = "StorageLoad.Time"
    else:
      storageLoadTime = "storage-load-time"
    assert storageLoadTime in runtime_profile
    # Call the second time, no metastore loading needed.
    # Only check this part in Catalog V1 because of V2's random behavior
    if not cluster_properties.is_catalog_v2_cluster():
      runtime_profile = self.execute_query(query).runtime_profile
      assert storageLoadTime not in runtime_profile

  def __verify_hashtable_stats_profile(self, runtime_profile):
    assert "Hash Table" in runtime_profile
    assert "Probes:" in runtime_profile
    assert "Travel:" in runtime_profile
    assert "HashCollisions:" in runtime_profile
    assert "Resizes:" in runtime_profile
    nprobes = re.search('Probes:.*\((\d+)\)', runtime_profile)
    # Probes and travel can be 0. The number can be an integer or float with K.
    assert nprobes and len(nprobes.groups()) == 1 and nprobes.group(1) >= 0
    ntravel = re.search('Travel:.*\((\d+)\)', runtime_profile)
    assert ntravel and len(ntravel.groups()) == 1 and ntravel.group(1) >= 0

  def test_query_profle_hashtable(self):
    """Test that the profile for join/aggregate contains hash table related
    information."""
    # Join
    query = """select a.int_col, a.string_col from functional.alltypes a
        inner join functional.alltypessmall b on a.id = b.id"""
    result = self.execute_query(query)
    assert result.success
    self.__verify_hashtable_stats_profile(result.runtime_profile)
    # Group by
    query = """select year, count(*) from
        functional.alltypesagg where int_col < 7 and year = 2010 group by year"""
    result = self.execute_query(query)
    assert result.success
    self.__verify_hashtable_stats_profile(result.runtime_profile)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_skew_reporting_in_runtime_profile(self):
    """Test that the skew summary and skew details are reported in runtime profile
    correctly"""
    query = """select ca_state, count(*) from tpcds_parquet.store_sales,
            tpcds_parquet.customer, tpcds_parquet.customer_address
            where ss_customer_sk = c_customer_sk and
            c_current_addr_sk = ca_address_sk
            group by ca_state
            order by ca_state
            """
    "Set up the skew threshold to 0.0"
    query_opts = {'report_skew_limit': 0.0}
    results = self.execute_query(query, query_opts)
    assert results.success

    "When the skew summary is seen, look for the details"
    skews_found = 'skew\(s\) found at:.*HASH_JOIN.*HASH_JOIN.*HDFS_SCAN_NODE'
    if len(re.findall(skews_found, results.runtime_profile, re.M)) == 1:

      "Expect to see skew details twice at the hash join nodes."
      probe_rows_at_hj = 'HASH_JOIN_NODE.*\n.*Skew details: ProbeRows'
      assert len(re.findall(probe_rows_at_hj, results.runtime_profile, re.M)) == 2

      "Expect to see skew details once at the scan node."
      probe_rows_at_hdfs_scan = 'HDFS_SCAN_NODE.*\n.*Skew details: RowsRead'
      assert len(re.findall(probe_rows_at_hdfs_scan, results.runtime_profile, re.M)) == 1

  def test_query_profile_contains_auto_scaling_events(self):
    """Test that the info about two compilation events appears in the profile."""
    query = "select * from functional.alltypes"
    # Specifically turn on test_replan to impose a 2-executor group environment in FE.
    query_opts = {'enable_replan': 1, 'test_replan': 1}
    results = self.execute_query(query, query_opts)
    assert results.success
    runtime_profile = results.runtime_profile
    assert len(re.findall('Analysis finished:', runtime_profile, re.M)) == 2
    assert len(re.findall('Single node plan created:', runtime_profile, re.M)) == 2
    assert len(re.findall('Distributed plan created:', runtime_profile, re.M)) == 2

  @SkipIfNotHdfsMinicluster.plans
  def test_reduced_cardinality_by_filter(self):
    """IMPALA-12702: Check that ExecSummary shows the reduced cardinality estimation."""
    query_opts = {'compute_processing_cost': True}
    query = """select STRAIGHT_JOIN count(*) from
        (select l_orderkey from tpch_parquet.lineitem) a
        join (select o_orderkey, o_custkey from tpch_parquet.orders) l1
          on a.l_orderkey = l1.o_orderkey
        where l1.o_custkey < 1000"""
    result = self.execute_query(query, query_opts)
    scan = result.exec_summary[10]
    assert '00:SCAN' in scan['operator']
    assert scan['num_rows'] == 39563
    assert scan['est_num_rows'] == 575771
    assert scan['detail'] == 'tpch_parquet.lineitem'
    runtime_profile = result.runtime_profile
    assert "cardinality=575.77K(filtered from 6.00M)" in runtime_profile

  def test_query_profile_contains_get_inflight_profile_counter(self):
    """Test that counter for getting inflight profiles appears in the profile"""
    # This query runs 15s
    query = "select count(*) from functional.alltypes where bool_col = sleep(50)"
    handle = self.execute_query_async(query)
    query_id = handle.get_handle().id

    cluster = ImpalaCluster.get_e2e_test_cluster()
    impalad = cluster.get_first_impalad()
    profile_urls = [
      "query_profile?query_id=",
      "query_profile_encoded?query_id=",
      "query_profile_json?query_id=",
      "query_profile_plain_text?query_id=",
    ]
    for url in profile_urls:
      impalad.service.read_debug_webpage(url + query_id)
    profile = self.client.get_runtime_profile(handle)
    assert "GetInFlightProfileTimeStats:" in profile
    assert "ClientFetchLockWaitTimer:" in profile
    # Make sure the counter actually records the requests
    samples_search = re.search(r"GetInFlightProfileTimeStats:.*samples: (\d+)", profile)
    num_samples = int(samples_search.group(1))
    assert num_samples > 0


class TestQueryStates(ImpalaTestSuite):
  """Test that the 'Query State' and 'Impala Query State' are set correctly in the
  runtime profile."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  def test_query_states(self):
    """Tests that the query profile shows expected query states."""
    query = "select count(*) from functional.alltypes where bool_col = sleep(10)"
    handle = self.execute_query_async(query,
        {"debug_action": "AC_BEFORE_ADMISSION:SLEEP@1000"})
    # If ExecuteStatement() has completed and the query is paused in the admission control
    # phase, then the query must be in COMPILED state.
    profile = self.client.get_runtime_profile(handle)
    assert self.__is_line_in_profile("Query State: COMPILED", profile)
    assert self.__is_line_in_profile("Impala Query State: PENDING", profile)
    # After completion of the admission control phase, the query must have at least
    # reached RUNNING state.
    self.client.wait_for_admission_control(handle)
    profile = self.client.get_runtime_profile(handle)
    assert self.__is_line_in_profile("Query State: RUNNING", profile), profile
    assert self.__is_line_in_profile("Impala Query State: RUNNING", profile), profile

    self.client.fetch(query, handle)
    profile = self.client.get_runtime_profile(handle)
    # After fetching the results, the query must be in state FINISHED.
    assert self.__is_line_in_profile("Query State: FINISHED", profile), profile
    assert self.__is_line_in_profile("Impala Query State: FINISHED", profile), profile

  def test_error_query_state(self):
    """Tests that the query profile shows the proper error state."""
    query = "select * from functional.alltypes limit 10"
    handle = self.execute_query_async(query, {"abort_on_error": "1",
                                              "debug_action": "0:GETNEXT:FAIL"})

    def assert_finished():
      profile = self.client.get_runtime_profile(handle)
      return self.__is_line_in_profile("Query State: FINISHED", profile) and \
             self.__is_line_in_profile("Impala Query State: FINISHED", profile)

    self.assert_eventually(300, 1, assert_finished,
      lambda: self.client.get_runtime_profile(handle))

    try:
      self.client.fetch(query, handle)
      assert False
    except ImpalaBeeswaxException:
      pass

    profile = self.client.get_runtime_profile(handle)
    assert self.__is_line_in_profile("Query State: EXCEPTION", profile), profile
    assert self.__is_line_in_profile("Impala Query State: ERROR", profile), profile

  def __is_line_in_profile(self, line, profile):
    """Returns true if the given 'line' is in the given 'profile'. A single line of the
    profile must exactly match the given 'line' (excluding whitespaces)."""
    return re.search("^\s*{0}\s*$".format(line), profile, re.M)
