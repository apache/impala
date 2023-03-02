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
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import ImpalaTestDimension

class TestLimit(ImpalaTestSuite):
  LIMIT_VALUES = [1, 2, 3, 4, 5, 10, 100, 5000]
  LIMIT_VALUES_CORE = [1, 5, 10, 5000]
  QUERIES = ["select * from lineitem limit %d"]

  # TODO: we should be able to run count(*) in setup rather than hardcoding the values
  # but I have no idea how to do this with this framework.
  TOTAL_ROWS = 6001215

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLimit, cls).add_test_dimensions()

    # Add two more dimensions
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_dimension(
          ImpalaTestDimension('limit_value', *TestLimit.LIMIT_VALUES_CORE))
    else:
      cls.ImpalaTestMatrix.add_dimension(
          ImpalaTestDimension('limit_value', *TestLimit.LIMIT_VALUES))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('query', *TestLimit.QUERIES))

    # Don't run with large limits and tiny batch sizes.  This generates excessive
    # network traffic and makes the machine run very slowly.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('limit_value') < 100 or v.get_value('exec_option')['batch_size'] == 0)
    # TPCH is not generated in hbase format.
    # TODO: Add test coverage for hbase.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format != "hbase")

  def test_limit(self, vector):
    # We can't validate the rows that are returned since that is non-deterministic.
    # This is why this is a python test rather than a .test.
    limit = vector.get_value('limit_value')
    expected_num_rows = min(limit, TestLimit.TOTAL_ROWS)
    query_string = vector.get_value('query') % limit
    result = self.execute_query(query_string, vector.get_value('exec_option'),
                                table_format=vector.get_value('table_format'))
    assert(len(result.data) == expected_num_rows)


class TestLimitBase(ImpalaTestSuite):
  def exec_query_validate(self, query, exec_options, should_succeed, expected_rows,
                          expected_error):
    """Executes a query and validates the results"""
    try:
      result = self.execute_query(query, exec_options)
      assert should_succeed, 'Query was expected to fail'
      assert len(result.data) == expected_rows,\
          'Wrong number of rows returned %d' % len(result.data)
    except ImpalaBeeswaxException as e:
      assert not should_succeed, 'Query was not expected to fail: %s' % e
      if (expected_error not in str(e)):
        print(str(e))
      assert expected_error in str(e)
