#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
#
# Tests for HDFS caching
#
import logging
import pytest
from subprocess import call
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_exec_option_dimension
from tests.util.shell_util import exec_process

class TestHdfsCaching(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsCaching, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and \
        v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.execute_serially
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
