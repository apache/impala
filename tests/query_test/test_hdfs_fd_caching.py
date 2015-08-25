# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
from tests.common.skip import SkipIfS3
from tests.util.shell_util import exec_process



class TestHdfsFdCaching(ImpalaTestSuite):
  """
  This test suite tests the behavior of HDFS file descriptor caching by evaluating the
  metrics exposed by the Impala daemon.
  """

  NUM_ROWS = 10000

  @classmethod
  def file_format_constraint(cls, v):
    return v.get_value('table_format').file_format in ["parquet"]

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsFdCaching, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(cls.file_format_constraint)

  @classmethod
  def get_workload(cls):
    return 'functional-query'


  def setup_method(self, method):
    self.cleanup_db("cachefd")
    self.client.execute("create database cachefd")
    self.client.execute("create table cachefd.simple(id int, col1 int, col2 int) "
                        "stored as parquet")
    buf = "insert into cachefd.simple values"
    self.client.execute(buf + ", ".join(["({0},{0},{0})".format(x) for x in range(self.NUM_ROWS)]))

  def teardown_method(self, methd):
    self.cleanup_db("cachedfd")


  @pytest.mark.execute_serially
  def test_simple_scan(self, vector):
    """Tests that in the default configuration, file handle caching is disabled and no
    file handles are cached."""

    num_handles_before = self.cached_handles()
    assert 0 == num_handles_before
    self.execute_query("select * from cachefd.simple limit 1", vector=vector)
    num_handles_after = self.cached_handles()
    assert 0 == num_handles_after
    assert num_handles_after == num_handles_before
    assert 0 == self.outstanding_handles()

    # No change when reading the table again
    for x in range(10):
      self.execute_query("select * from cachefd.simple limit 1", vector=vector)

    assert num_handles_after == num_handles_before
    assert 0 == self.outstanding_handles()

  def cached_handles(self):
    return self.get_agg_metric("impala-server.io.mgr.num-cached-file-handles")

  def outstanding_handles(self):
    return self.get_agg_metric("impala-server.io.mgr.num-file-handles-outstanding")

  def get_agg_metric(self, key, fun=sum):
    cluster = ImpalaCluster()
    return fun([s.service.get_metric_value(key) for s
                in cluster.impalads])
