#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates limit on scan nodes
#
import logging
import pytest
from copy import copy
from subprocess import call
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import *
from tests.common.test_vector import *
from tests.common.impala_cluster import ImpalaCluster
from tests.common.test_dimensions import create_exec_option_dimension
from tests.util.shell_util import exec_process

# End to end test that hdfs caching is working.
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

  @pytest.mark.execute_serially
  @pytest.mark.xfail(run=False, reason="IMPALA-1037. This test is flaky")
  def test_caching_ddl(self, vector):
    self.client.execute("drop table if exists functional.cached_tbl_part")
    self.client.execute("drop table if exists functional.cached_tbl_nopart")

    # Get the number of cache requests before starting the test
    num_entries_pre = get_num_cache_requests()
    self.run_test_case('QueryTest/hdfs-caching', vector)

    # After running this test case we should be left with 6 cache requests.
    # In this case, 1 for each table + 4 more for each cached partition.
    assert num_entries_pre == get_num_cache_requests() - 6

    self.client.execute("drop table functional.cached_tbl_part")
    self.client.execute("drop table functional.cached_tbl_nopart")

    # Dropping the tables should cleanup cache entries leaving us with the same
    # total number of entries
    assert num_entries_pre == get_num_cache_requests()

def get_num_cache_requests():
  """Returns the number of outstanding cache requests"""
  rc, stdout, stderr = exec_process("hdfs cacheadmin -listDirectives -stats")
  assert rc == 0, 'Error executing hdfs cacheadmin: %s %s' % (stdout, stderr)
  return len(stdout.split('\n'))
