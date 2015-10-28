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
import pytest
import sys
import re
from copy import copy
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfLocal

class TestQueryMemLimitScaling(ImpalaTestSuite):
  """Test class to do functional validation of per query memory limits. """
  QUERY = ["select * from lineitem where l_orderkey = -1",
           "select min(l_orderkey) from lineitem",
           "select * from lineitem order by l_orderkey limit 1"]

  # These query take 400mb-1gb if no mem limits are set
  MEM_LIMITS = ["-1", "400m", "150m"]

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryMemLimitScaling, cls).add_test_dimensions()
    # add mem_limit as a test dimension.
    new_dimension = TestDimension('mem_limit', *TestQueryMemLimitScaling.MEM_LIMITS)
    cls.TestMatrix.add_dimension(new_dimension)
    if cls.exploration_strategy() != 'exhaustive':
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format in ['parquet'])

  # Test running with different mem limits to exercise the dynamic memory
  # scaling functionality.
  def test_mem_usage_scaling(self, vector):
    mem_limit = copy(vector.get_value('mem_limit'))
    table_format = vector.get_value('table_format')
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = mem_limit
    for query in self.QUERY:
      self.execute_query(query, exec_options, table_format=table_format)

class TestExprMemUsage(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExprMemUsage, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    if cls.exploration_strategy() != 'exhaustive':
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format in ['parquet'])

  def test_scanner_mem_usage(self, vector):
    exec_options = vector.get_value('exec_option')
    # This value was picked empircally based on the query.
    exec_options['mem_limit'] = '300m'
    self.execute_query_expect_success(self.client,
      "select count(*) from lineitem where lower(l_comment) = 'hello'", exec_options,
      table_format=vector.get_value('table_format'))


class TestLowMemoryLimits(ImpalaTestSuite):
  '''Super class for the memory limit tests with the TPC-H and TPC-DS queries'''
  EXPECTED_ERROR_MSG = "Memory limit exceeded"

  def low_memory_limit_test(self, vector, tpch_query, limit):
    mem = vector.get_value('mem_limit')
    # Mem consumption can be +-30MBs, depending on how many scanner threads are
    # running. Adding this extra mem in order to reduce false negatives in the tests.
    limit = limit + 30

    # If memory limit larger than the minimum threshold, then it is not expected to fail.
    expects_error = mem < limit;
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['mem_limit'] = str(mem) + "m"
    try:
      self.run_test_case(tpch_query, new_vector)
    except ImpalaBeeswaxException as e:
      if not expects_error: raise
      assert TestLowMemoryLimits.EXPECTED_ERROR_MSG in str(e)


class TestTpchMemLimitError(TestLowMemoryLimits):
  # TODO: After we stabilize the mem usage test, we should move this test to exhaustive.
  # The mem limits that will be used.
  MEM_IN_MB = [20, 140, 180, 275, 450, 700, 980]

  # Different values of mem limits and minimum mem limit (in MBs) each query is expected
  # to run without problem. Those values were determined by manual testing.
  MIN_MEM_FOR_TPCH = { 'Q1' : 140, 'Q2' : 120, 'Q3' : 240, 'Q4' : 125, 'Q5' : 235,\
                       'Q6' : 25, 'Q7' : 265, 'Q8' : 250, 'Q9' : 400, 'Q10' : 240,\
                       'Q11' : 110, 'Q12' : 125, 'Q13' : 110, 'Q14' : 105, 'Q15' : 125,\
                       'Q16' : 125, 'Q17' : 130, 'Q18' : 425, 'Q19' : 240, 'Q20' : 250,\
                       'Q21' : 620, 'Q22' : 125}

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpchMemLimitError, cls).add_test_dimensions()

    cls.TestMatrix.add_dimension(
      TestDimension('mem_limit', *TestTpchMemLimitError.MEM_IN_MB))

    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  def test_low_mem_limit_q1(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q1', self.MIN_MEM_FOR_TPCH['Q1']);

  def test_low_mem_limit_q2(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q2', self.MIN_MEM_FOR_TPCH['Q2']);

  def test_low_mem_limit_q3(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q3', self.MIN_MEM_FOR_TPCH['Q3']);

  def test_low_mem_limit_q4(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q4', self.MIN_MEM_FOR_TPCH['Q4']);

  def test_low_mem_limit_q5(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q5', self.MIN_MEM_FOR_TPCH['Q5']);

  def test_low_mem_limit_q6(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q6', self.MIN_MEM_FOR_TPCH['Q6']);

  def test_low_mem_limit_q7(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q7', self.MIN_MEM_FOR_TPCH['Q7']);

  def test_low_mem_limit_q8(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q8', self.MIN_MEM_FOR_TPCH['Q8']);

  def test_low_mem_limit_q9(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q9', self.MIN_MEM_FOR_TPCH['Q9']);

  @SkipIfLocal.mem_usage_different
  def test_low_mem_limit_q10(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q10', self.MIN_MEM_FOR_TPCH['Q10']);

  def test_low_mem_limit_q11(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q11', self.MIN_MEM_FOR_TPCH['Q11']);

  def test_low_mem_limit_q12(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q12', self.MIN_MEM_FOR_TPCH['Q12']);

  def test_low_mem_limit_q13(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q13', self.MIN_MEM_FOR_TPCH['Q13']);

  def test_low_mem_limit_q14(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q14', self.MIN_MEM_FOR_TPCH['Q14']);

  def test_low_mem_limit_q15(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q15', self.MIN_MEM_FOR_TPCH['Q15']);

  def test_low_mem_limit_q16(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q16', self.MIN_MEM_FOR_TPCH['Q16']);

  def test_low_mem_limit_q17(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q17', self.MIN_MEM_FOR_TPCH['Q17']);

  def test_low_mem_limit_q18(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q18', self.MIN_MEM_FOR_TPCH['Q18']);

  def test_low_mem_limit_q19(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q19', self.MIN_MEM_FOR_TPCH['Q19']);

  def test_low_mem_limit_q20(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q20', self.MIN_MEM_FOR_TPCH['Q20']);

  @SkipIfLocal.mem_usage_different
  def test_low_mem_limit_q21(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q21', self.MIN_MEM_FOR_TPCH['Q21']);

  def test_low_mem_limit_q22(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q22', self.MIN_MEM_FOR_TPCH['Q22']);


class TestTpcdsMemLimitError(TestLowMemoryLimits):
  # The mem limits that will be used.
  MEM_IN_MB = [20, 100, 110, 150]

  # Different values of mem limits and minimum mem limit (in MBs) each query is expected
  # to run without problem. Those values were determined by manual testing.
  MIN_MEM_FOR_TPCDS = { 'q53' : 110}

  @classmethod
  def get_workload(self):
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsMemLimitError, cls).add_test_dimensions()

    cls.TestMatrix.add_dimension(
      TestDimension('mem_limit', *TestTpcdsMemLimitError.MEM_IN_MB))

    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  def test_low_mem_limit_q53(self, vector):
    self.low_memory_limit_test(vector, 'tpcds-q53', self.MIN_MEM_FOR_TPCDS['q53']);
