#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates limit on scan nodes
#
import logging
import pytest
from copy import copy
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import *
from tests.common.impala_cluster import ImpalaCluster

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

