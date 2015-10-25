# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates limit on scan nodes
#
import logging
import os
import pytest
from copy import copy
from subprocess import call
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import *
from tests.common.test_vector import *
from tests.common.impala_cluster import ImpalaCluster
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.skip import SkipIfS3, SkipIfIsilon
from tests.util.shell_util import exec_process

# End to end test that hdfs caching is working.
@SkipIfS3.caching # S3: missing coverage: verify SET CACHED gives error
@SkipIfIsilon.caching
class TestHdfsCaching(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsCaching, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('exec_option')['batch_size'] == 0)
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == "text")

  # The tpch nation table is cached as part of data loading. We'll issue a query
  # against this table and verify the metric is updated correctly.
  @pytest.mark.execute_serially
  def test_table_is_cached(self, vector):
    cached_read_metric = "impala-server.io-mgr.cached-bytes-read"
    query_string = "select count(*) from tpch.nation"
    expected_bytes_delta = 2199
    impala_cluster = ImpalaCluster()

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

    if num_metrics_increased != 1:
      # Test failed, print the metrics
      for i in range(0, len(cached_bytes_before)):
        print "%d %d" % (cached_bytes_before[i], cached_bytes_after[i])
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
    for x in xrange(1, num_iters):
      result = self.execute_query(query_string)
      assert(len(result.data) == 2)

@SkipIfS3.caching
@SkipIfIsilon.caching
class TestHdfsCachingDdl(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsCachingDdl, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and \
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    self.cleanup_db("cachedb")
    self.client.execute("create database cachedb")

  def teardown_method(self, method):
    self.cleanup_db("cachedb")

  @pytest.mark.execute_serially
  def test_caching_ddl(self, vector):

    # Get the number of cache requests before starting the test
    num_entries_pre = get_num_cache_requests()
    self.run_test_case('QueryTest/hdfs-caching', vector)

    # After running this test case we should be left with 9 cache requests.
    # In this case, 1 for each table + 7 more for each cached partition + 1
    # for the table with partitions on both HDFS and local file system.
    assert num_entries_pre == get_num_cache_requests() - 9

    self.client.execute("drop table cachedb.cached_tbl_part")
    self.client.execute("drop table cachedb.cached_tbl_nopart")
    self.client.execute("drop table cachedb.cached_tbl_local")

    # Dropping the tables should cleanup cache entries leaving us with the same
    # total number of entries
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

def drop_cache_directives_for_path(path):
  """Drop the cache directive for a given path"""
  rc, stdout, stderr = exec_process("hdfs cacheadmin -removeDirectives -path %s" % path)
  assert rc == 0, \
      "Error removing cache directive for path %s (%s, %s)" % (path, stdout, stderr)

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
  """Returns the number of outstanding cache requests"""
  rc, stdout, stderr = exec_process("hdfs cacheadmin -listDirectives -stats")
  assert rc == 0, 'Error executing hdfs cacheadmin: %s %s' % (stdout, stderr)
  return len(stdout.split('\n'))
