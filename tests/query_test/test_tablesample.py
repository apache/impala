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

# Tests the TABLESAMPLE clause.

from __future__ import absolute_import, division, print_function
import pytest
import subprocess

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import ImpalaTestDimension

class TestTableSample(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTableSample, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('repeatable', *[True, False]))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('filtered', *[True, False]))
    # Tablesample is only supported on HDFS tables.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
      v.get_value('table_format').file_format != 'kudu' and
      v.get_value('table_format').file_format != 'hbase')
    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on core testing time by limiting the file formats.
      cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet' or
        v.get_value('table_format').file_format == 'text')

  def test_tablesample(self, vector):
    # Do not use a .test to avoid making this test flaky.
    # 1. Queries without the repeatable clause are non-deterministic.
    # 2. The results of queries without a repeatable clause could change due to
    # changes in data loading that affect the number or size of files.
    repeatable = vector.get_value('repeatable')
    filtered = vector.get_value('filtered')

    where_clause = ""
    if filtered:
      where_clause = "where month between 1 and 6"

    ImpalaTestSuite.change_database(self.client, vector.get_value('table_format'))
    result = self.client.execute("select count(*) from alltypes %s" % where_clause)
    baseline_count = int(result.data[0])
    prev_count = None
    for perc in [5, 20, 50, 100]:
      rep_sql = ""
      if repeatable: rep_sql = " repeatable(1)"
      sql_stmt = "select count(*) from alltypes tablesample system(%s)%s %s" \
                 % (perc, rep_sql, where_clause)
      handle = self.client.execute_async(sql_stmt)
      # IMPALA-6352: flaky test, possibly due to a hung thread. Wait for 500 sec before
      # failing and logging the backtraces of all impalads.
      is_finished = self.client.wait_for_finished_timeout(handle, 500)
      assert is_finished, 'Query Timed out. Dumping backtrace of all threads in ' \
                          'impalads:\nthreads in the impalad1: %s \nthreads in the ' \
                          'impalad2: %s \nthreads in the impalad3: %s' % \
                        (subprocess.check_output(
                          "gdb -ex \"set pagination 0\" -ex \"thread apply all bt\"  "
                          "--batch -p $(pgrep impalad | sed -n 1p)", shell=True),
                         subprocess.check_output(
                          "gdb -ex \"set pagination 0\" -ex \"thread apply all bt\"  "
                          "--batch -p $(pgrep impalad | sed -n 2p)", shell=True),
                         subprocess.check_output(
                          "gdb -ex \"set pagination 0\" -ex \"thread apply all bt\"  "
                          "--batch -p $(pgrep impalad | sed -n 3p)", shell=True))
      result = self.client.fetch(sql_stmt, handle)
      self.client.close_query(handle)
      count = int(result.data[0])
      if perc < 100:
        assert count < baseline_count
      else:
        assert count == baseline_count
      if prev_count and repeatable:
        # May not necessarily be true for non-repeatable samples
        assert count > prev_count
      prev_count = count
