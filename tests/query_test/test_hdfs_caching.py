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

# Validates limit on scan nodes

from __future__ import absolute_import, division, print_function
from builtins import range
import pytest
import re
import time
from subprocess import check_call

from tests.common.environ import build_flavor_timeout, IS_DOCKERIZED_TEST_CLUSTER
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.skip import SkipIfFS, SkipIfDockerizedCluster
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.filesystem_utils import get_fs_path, IS_EC
from tests.util.shell_util import exec_process


# End to end test that hdfs caching is working.
@SkipIfFS.hdfs_caching  # missing coverage: verify SET CACHED gives error
class TestHdfsCaching(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsCaching, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('exec_option')['batch_size'] == 0)
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == "text")

  # The tpch nation table is cached as part of data loading. We'll issue a query
  # against this table and verify the metric is updated correctly.
  @pytest.mark.execute_serially
  def test_table_is_cached(self, vector):
    cached_read_metric = "impala-server.io-mgr.cached-bytes-read"
    query_string = "select count(*) from tpch.nation"
    expected_bytes_delta = 2199
    impala_cluster = ImpalaCluster.get_e2e_test_cluster()

    # Collect the cached read metric on all the impalads before running the query
    cached_bytes_before = list()
    for impalad in impala_cluster.impalads:
      cached_bytes_before.append(impalad.service.get_metric_value(cached_read_metric))

    # Execute the query.
    result = self.execute_query(query_string)
    assert(len(result.data) == 1)
    assert(result.data[0] == '25')

    # Read the metrics again.
    cached_bytes_after = list()
    for impalad in impala_cluster.impalads:
      cached_bytes_after.append(impalad.service.get_metric_value(cached_read_metric))

    # Verify that the cached bytes increased by the expected number on exactly one of
    # the impalads.
    num_metrics_increased = 0
    assert(len(cached_bytes_before) == len(cached_bytes_after))
    for i in range(0, len(cached_bytes_before)):
      assert(cached_bytes_before[i] == cached_bytes_after[i] or\
             cached_bytes_before[i] + expected_bytes_delta == cached_bytes_after[i])
      if cached_bytes_after[i] > cached_bytes_before[i]:
        num_metrics_increased = num_metrics_increased + 1

    if IS_DOCKERIZED_TEST_CLUSTER:
      assert num_metrics_increased == 0, "HDFS caching is disabled in dockerised cluster."
    elif IS_EC:
      assert num_metrics_increased == 0, "HDFS caching is disabled with erasure coding."
    elif num_metrics_increased != 1:
      # Test failed, print the metrics
      for i in range(0, len(cached_bytes_before)):
        print("%d %d" % (cached_bytes_before[i], cached_bytes_after[i]))
      assert(False)

  def test_cache_cancellation(self, vector):
    """ This query runs on some mix of cached and not cached tables. The query has
        a limit so it exercises the cancellation paths. Regression test for
        IMPALA-1019. """
    num_iters = 100
    query_string = """
      with t1 as (select int_col x, bigint_col y from functional.alltypes limit 2),
           t2 as (select int_col x, bigint_col y from functional.alltypestiny limit 2),
           t3 as (select int_col x, bigint_col y from functional.alltypessmall limit 2)
      select * from t1, t2, t3 where t1.x = t2.x and t2.x = t3.x """

    # Run this query for some iterations since it is timing dependent.
    for x in range(1, num_iters):
      result = self.execute_query(query_string)
      assert(len(result.data) == 2)


# A separate class has been created for "test_hdfs_caching_fallback_path" to make it
# run as a part of exhaustive tests which require the workload to be 'functional-query'.
# TODO: Move this to TestHdfsCaching once we make exhaustive tests run for other workloads
@SkipIfFS.hdfs_caching
class TestHdfsCachingFallbackPath(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @SkipIfFS.hdfs_encryption
  def test_hdfs_caching_fallback_path(self, vector, unique_database, testid_checksum):
    """ This tests the code path of the query execution where the hdfs cache read fails
    and the execution falls back to the normal read path. To reproduce this situation we
    rely on IMPALA-3679, where zcrs are not supported with encryption zones. This makes
    sure ReadFromCache() fails and falls back to ReadRange() to read the scan range."""

    if self.exploration_strategy() != 'exhaustive' or\
        vector.get_value('table_format').file_format != 'text':
      pytest.skip()

    # Create a new encryption zone and copy the tpch.nation table data into it.
    encrypted_table_dir = get_fs_path("/test-warehouse/" + testid_checksum)
    create_query_sql = "CREATE EXTERNAL TABLE %s.cached_nation like tpch.nation "\
        "LOCATION '%s'" % (unique_database, encrypted_table_dir)
    check_call(["hdfs", "dfs", "-mkdir", encrypted_table_dir], shell=False)
    check_call(["hdfs", "crypto", "-createZone", "-keyName", "testKey1", "-path",\
        encrypted_table_dir], shell=False)
    check_call(["hdfs", "dfs", "-cp", get_fs_path("/test-warehouse/tpch.nation/*.tbl"),\
        encrypted_table_dir], shell=False)
    # Reduce the scan range size to force the query to have multiple scan ranges.
    exec_options = vector.get_value('exec_option')
    exec_options['max_scan_range_length'] = 1024
    try:
      self.execute_query_expect_success(self.client, create_query_sql)
      # Cache the table data
      self.execute_query_expect_success(self.client, "ALTER TABLE %s.cached_nation set "
         "cached in 'testPool'" % unique_database)
      # Wait till the whole path is cached. We set a deadline of 20 seconds for the path
      # to be cached to make sure this doesn't loop forever in case of caching errors.
      caching_deadline = time.time() + 20
      while not is_path_fully_cached(encrypted_table_dir):
        if time.time() > caching_deadline:
          pytest.fail("Timed out caching path: " + encrypted_table_dir)
        time.sleep(2)
      self.execute_query_expect_success(self.client, "invalidate metadata "
          "%s.cached_nation" % unique_database);
      result = self.execute_query_expect_success(self.client, "select count(*) from "
          "%s.cached_nation" % unique_database, exec_options)
      assert(len(result.data) == 1)
      assert(result.data[0] == '25')
    except Exception as e:
      pytest.fail("Failure in test_hdfs_caching_fallback_path: " + str(e))
    finally:
      check_call(["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", encrypted_table_dir],\
          shell=False)


@SkipIfFS.hdfs_caching
class TestHdfsCachingDdl(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsCachingDdl, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and \
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    self.cleanup_db("cachedb")
    self.client.execute("create database cachedb")

  def teardown_method(self, method):
    self.cleanup_db("cachedb")

  @pytest.mark.execute_serially
  @SkipIfDockerizedCluster.accesses_host_filesystem
  def test_caching_ddl(self, vector):
    # Get the number of cache requests before starting the test
    num_entries_pre = get_num_cache_requests()
    self.run_test_case('QueryTest/hdfs-caching', vector)

    # After running this test case we should be left with 10 cache requests.
    # In this case, 1 for each table + 7 more for each cached partition + 1
    # for the table with partitions on both HDFS and local file system.
    assert num_entries_pre == get_num_cache_requests() - 10

    self.client.execute("drop table cachedb.cached_tbl_part")
    self.client.execute("drop table cachedb.cached_tbl_nopart")
    self.client.execute("drop table cachedb.cached_tbl_local")
    self.client.execute("drop table cachedb.cached_tbl_ttl")

    # Dropping the tables should cleanup cache entries leaving us with the same
    # total number of entries.
    assert num_entries_pre == get_num_cache_requests()

  @pytest.mark.execute_serially
  def test_caching_ddl_drop_database(self, vector):
    """IMPALA-2518: DROP DATABASE CASCADE should properly drop all impacted cache
        directives"""
    num_entries_pre = get_num_cache_requests()
    # Populates the `cachedb` database with some cached tables and partitions
    self.client.execute("use cachedb")
    self.client.execute("create table cached_tbl_nopart (i int) cached in 'testPool'")
    self.client.execute("insert into cached_tbl_nopart select 1")
    self.client.execute("create table cached_tbl_part (i int) partitioned by (j int) \
                         cached in 'testPool'")
    self.client.execute("insert into cached_tbl_part (i,j) select 1, 2")
    # We expect the number of cached entities to grow
    assert num_entries_pre < get_num_cache_requests()
    self.client.execute("use default")
    self.client.execute("drop database cachedb cascade")
    # We want to see the number of cached entities return to the original count
    assert num_entries_pre == get_num_cache_requests()

  @pytest.mark.execute_serially
  def test_cache_reload_validation(self, vector):
    """This is a set of tests asserting that cache directives modified
       outside of Impala are picked up after reload, cf IMPALA-1645"""

    num_entries_pre = get_num_cache_requests()
    create_table = ("create table cachedb.cached_tbl_reload "
        "(id int) cached in 'testPool' with replication = 8")
    self.client.execute(create_table)

    # Access the table once to load the metadata
    self.client.execute("select count(*) from cachedb.cached_tbl_reload")

    create_table = ("create table cachedb.cached_tbl_reload_part (i int) "
        "partitioned by (j int) cached in 'testPool' with replication = 8")
    self.client.execute(create_table)

    # Add two partitions
    self.client.execute("alter table cachedb.cached_tbl_reload_part add partition (j=1)")
    self.client.execute("alter table cachedb.cached_tbl_reload_part add partition (j=2)")

    assert num_entries_pre + 4 == get_num_cache_requests(), \
      "Adding the tables should be reflected by the number of cache directives."

    # Modify the cache directive outside of Impala and reload the table to verify
    # that changes are visible
    drop_cache_directives_for_path("/test-warehouse/cachedb.db/cached_tbl_reload")
    drop_cache_directives_for_path("/test-warehouse/cachedb.db/cached_tbl_reload_part")
    drop_cache_directives_for_path(
        "/test-warehouse/cachedb.db/cached_tbl_reload_part/j=1")
    change_cache_directive_repl_for_path(
        "/test-warehouse/cachedb.db/cached_tbl_reload_part/j=2", 3)

    # Create a bogus cached table abusing an existing cache directive ID, IMPALA-1750
    dirid = get_cache_directive_for_path("/test-warehouse/cachedb.db/cached_tbl_reload_part/j=2")
    self.client.execute(("create table cachedb.no_replication_factor (id int) " \
                         "tblproperties(\"cache_directive_id\"=\"%s\")" % dirid))
    self.run_test_case('QueryTest/hdfs-caching-validation', vector)
    # Temp fix for IMPALA-2510. Due to IMPALA-2518, when the test database is dropped,
    # the cache directives are not removed for table 'cached_tbl_reload_part'.
    drop_cache_directives_for_path(
        "/test-warehouse/cachedb.db/cached_tbl_reload_part/j=2")

  @pytest.mark.execute_serially
  def test_external_drop(self):
    """IMPALA-3040: Tests that dropping a partition in Hive leads to the removal of the
       cache directive after a refresh statement in Impala."""
    num_entries_pre = get_num_cache_requests()
    self.client.execute("use cachedb")
    self.client.execute("create table test_external_drop_tbl (i int) partitioned by "
                        "(j int) cached in 'testPool'")
    self.client.execute("insert into test_external_drop_tbl (i,j) select 1, 2")
    # 1 directive for the table and 1 directive for the partition.
    assert num_entries_pre + 2 == get_num_cache_requests()
    self.hive_client.drop_partition("cachedb", "test_external_drop_tbl", ["2"], True)
    self.client.execute("refresh test_external_drop_tbl")
    # The directive on the partition is removed.
    assert num_entries_pre + 1 == get_num_cache_requests()
    self.client.execute("drop table test_external_drop_tbl")
    # We want to see the number of cached entities return to the original count.
    assert num_entries_pre == get_num_cache_requests()

def drop_cache_directives_for_path(path):
  """Drop the cache directive for a given path"""
  rc, stdout, stderr = exec_process("hdfs cacheadmin -removeDirectives -path %s" % path)
  assert rc == 0, \
      "Error removing cache directive for path %s (%s, %s)" % (path, stdout, stderr)

def is_path_fully_cached(path):
  """Returns true if all the bytes of the path are cached, false otherwise"""
  rc, stdout, stderr = exec_process("hdfs cacheadmin -listDirectives -stats -path %s" % path)
  assert rc == 0
  caching_stats = stdout.strip("\n").split("\n")[-1].split()
  # Compare BYTES_NEEDED and BYTES_CACHED, the output format is as follows
  # "ID POOL REPL EXPIRY PATH BYTES_NEEDED BYTES_CACHED FILES_NEEDED FILES_CACHED"
  return len(caching_stats) > 0 and caching_stats[5] == caching_stats[6]


def get_cache_directive_for_path(path):
  rc, stdout, stderr = exec_process("hdfs cacheadmin -listDirectives -path %s" % path)
  assert rc == 0
  dirid = re.search('^\s+?(\d+)\s+?testPool\s+?.*?$', stdout, re.MULTILINE).group(1)
  return dirid

def change_cache_directive_repl_for_path(path, repl):
  """Drop the cache directive for a given path"""
  dirid = get_cache_directive_for_path(path)
  rc, stdout, stderr = exec_process(
    "hdfs cacheadmin -modifyDirective -id %s -replication %s" % (dirid, repl))
  assert rc == 0, \
      "Error modifying cache directive for path %s (%s, %s)" % (path, stdout, stderr)

def get_num_cache_requests():
  """Returns the number of outstanding cache requests. Due to race conditions in the
    way cache requests are added/dropped/reported (see IMPALA-3040), this function tries
    to return a stable result by making several attempts to stabilize it within a
    reasonable timeout."""
  def get_num_cache_requests_util():
    rc, stdout, stderr = exec_process("hdfs cacheadmin -listDirectives -stats")
    assert rc == 0, 'Error executing hdfs cacheadmin: %s %s' % (stdout, stderr)
    # remove blank new lines from output count
    lines = [line for line in stdout.split('\n') if line.strip()]
    count = None
    for line in lines:
      if line.startswith("Found "):
        # the line should say "Found <int> entries"
        # if we find this line we parse the number of entries
        # from this line.
        count = int(re.search(r'\d+', line).group())
        break
    # if count is available we return it else we just
    # return the total number of lines
    if count is not None:
      return count
    else:
      return len(stdout.split('\n'))

  # IMPALA-3040: This can take time, especially under slow builds like ASAN.
  wait_time_in_sec = build_flavor_timeout(5, slow_build_timeout=20)
  num_stabilization_attempts = 0
  max_num_stabilization_attempts = 10
  num_requests = None
  LOG.info("{0} Entered get_num_cache_requests()".format(time.time()))
  while num_stabilization_attempts < max_num_stabilization_attempts:
    new_requests = get_num_cache_requests_util()
    if new_requests == num_requests: break
    LOG.info("{0} Waiting to stabilise: num_requests={1} new_requests={2}".format(
        time.time(), num_requests, new_requests))
    num_requests = new_requests
    num_stabilization_attempts = num_stabilization_attempts + 1
    time.sleep(wait_time_in_sec)
  LOG.info("{0} Final num requests: {1}".format(time.time(), num_requests))
  return num_requests
