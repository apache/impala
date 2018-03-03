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
#
import pytest
from copy import copy

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.test_vector import ImpalaTestDimension
from tests.verifiers.metric_verifier import MetricVerifier

# Substrings of the expected error messages when the mem limit is too low
MEM_LIMIT_EXCEEDED_MSG = "Memory limit exceeded"
MEM_LIMIT_TOO_LOW_FOR_RESERVATION = ("minimum memory reservation is greater than memory "
  "available to the query for buffer reservations")
MEM_LIMIT_ERROR_MSGS = [MEM_LIMIT_EXCEEDED_MSG, MEM_LIMIT_TOO_LOW_FOR_RESERVATION]

@SkipIfLocal.mem_usage_different
class TestQueryMemLimitScaling(ImpalaTestSuite):
  """Test class to do functional validation of per query memory limits. """
  QUERY = ["select * from lineitem where l_orderkey = -1",
           "select min(l_orderkey) from lineitem",
           "select * from lineitem order by l_orderkey limit 1"]

  # These query take 400mb-1gb if no mem limits are set
  MEM_LIMITS = ["-1", "400m", "150m"]

  @classmethod
  def get_workload(self):
    # Note: this workload doesn't run exhaustively. See IMPALA-3947 before trying to move
    # this test to exhaustive.
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryMemLimitScaling, cls).add_test_dimensions()
    # add mem_limit as a test dimension.
    new_dimension = ImpalaTestDimension('mem_limit',
                                        *TestQueryMemLimitScaling.MEM_LIMITS)
    cls.ImpalaTestMatrix.add_dimension(new_dimension)
    if cls.exploration_strategy() != 'exhaustive':
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
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
    # Note: this workload doesn't run exhaustively. See IMPALA-3947 before trying to move
    # this test to exhaustive.
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExprMemUsage, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    if cls.exploration_strategy() != 'exhaustive':
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
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

  def low_memory_limit_test(self, vector, tpch_query, limit, xfail_mem_limit=None):
    mem = vector.get_value('mem_limit')
    # Mem consumption can be +-30MBs, depending on how many scanner threads are
    # running. Adding this extra mem in order to reduce false negatives in the tests.
    limit = limit + 30

    # If memory limit larger than the minimum threshold, then it is not expected to fail.
    expects_error = mem < limit
    new_vector = copy(vector)
    exec_options = new_vector.get_value('exec_option')
    exec_options['mem_limit'] = str(mem) + "m"

    # Reduce the page size to better exercise page boundary logic.
    exec_options['default_spillable_buffer_size'] = "256k"
    try:
      self.run_test_case(tpch_query, new_vector)
    except ImpalaBeeswaxException as e:
      if not expects_error and not xfail_mem_limit: raise
      found_expected_error = False
      for error_msg in MEM_LIMIT_ERROR_MSGS:
        if error_msg in str(e): found_expected_error = True
      assert found_expected_error, str(e)
      if not expects_error and xfail_mem_limit:
        pytest.xfail(xfail_mem_limit)


@SkipIfLocal.mem_usage_different
class TestTpchMemLimitError(TestLowMemoryLimits):
  # The mem limits that will be used.
  MEM_IN_MB = [20, 140, 180, 220, 275, 450, 700]

  # Different values of mem limits and minimum mem limit (in MBs) each query is expected
  # to run without problem. These were determined using the query_runtime_info.json file
  # produced by the stress test (i.e. concurrent_select.py).
  MIN_MEM_FOR_TPCH = { 'Q1' : 125, 'Q2' : 125, 'Q3' : 112, 'Q4' : 137, 'Q5' : 137,\
                       'Q6' : 25, 'Q7' : 200, 'Q8' : 125, 'Q9' : 200, 'Q10' : 162,\
                       'Q11' : 112, 'Q12' : 150, 'Q13' : 125, 'Q14' : 125, 'Q15' : 125,\
                       'Q16' : 137, 'Q17' : 137, 'Q18' : 196, 'Q19' : 112, 'Q20' : 162,\
                       'Q21' : 187, 'Q22' : 125}

  @classmethod
  def get_workload(self):
    # Note: this workload doesn't run exhaustively. See IMPALA-3947 before trying to move
    # this test to exhaustive.
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpchMemLimitError, cls).add_test_dimensions()

    cls.ImpalaTestMatrix.add_dimension(
      ImpalaTestDimension('mem_limit', *TestTpchMemLimitError.MEM_IN_MB))

    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  def test_low_mem_limit_q1(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q1', self.MIN_MEM_FOR_TPCH['Q1'])

  def test_low_mem_limit_q2(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q2', self.MIN_MEM_FOR_TPCH['Q2'])

  def test_low_mem_limit_q3(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q3', self.MIN_MEM_FOR_TPCH['Q3'])

  def test_low_mem_limit_q4(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q4', self.MIN_MEM_FOR_TPCH['Q4'])

  def test_low_mem_limit_q5(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q5', self.MIN_MEM_FOR_TPCH['Q5'])

  def test_low_mem_limit_q6(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q6', self.MIN_MEM_FOR_TPCH['Q6'])

  def test_low_mem_limit_q7(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q7', self.MIN_MEM_FOR_TPCH['Q7'])

  def test_low_mem_limit_q8(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q8', self.MIN_MEM_FOR_TPCH['Q8'])

  def test_low_mem_limit_q9(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q9', self.MIN_MEM_FOR_TPCH['Q9'],
            xfail_mem_limit="IMPALA-3328: TPC-H Q9 memory limit test is flaky")

  def test_low_mem_limit_q10(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q10', self.MIN_MEM_FOR_TPCH['Q10'])

  def test_low_mem_limit_q11(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q11', self.MIN_MEM_FOR_TPCH['Q11'])

  def test_low_mem_limit_q12(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q12', self.MIN_MEM_FOR_TPCH['Q12'])

  def test_low_mem_limit_q13(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q13', self.MIN_MEM_FOR_TPCH['Q13'])

  def test_low_mem_limit_q14(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q14', self.MIN_MEM_FOR_TPCH['Q14'])

  def test_low_mem_limit_q15(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q15', self.MIN_MEM_FOR_TPCH['Q15'])

  def test_low_mem_limit_q16(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q16', self.MIN_MEM_FOR_TPCH['Q16'])

  def test_low_mem_limit_q17(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q17', self.MIN_MEM_FOR_TPCH['Q17'])

  def test_low_mem_limit_q18(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q18', self.MIN_MEM_FOR_TPCH['Q18'])

  def test_low_mem_limit_q19(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q19', self.MIN_MEM_FOR_TPCH['Q19'])

  def test_low_mem_limit_q20(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q20', self.MIN_MEM_FOR_TPCH['Q20'])

  def test_low_mem_limit_q21(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q21', self.MIN_MEM_FOR_TPCH['Q21'])

  def test_low_mem_limit_q22(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q22', self.MIN_MEM_FOR_TPCH['Q22'])

  @pytest.mark.execute_serially
  def test_low_mem_limit_no_fragments(self, vector):
    self.low_memory_limit_test(vector, 'tpch-q14', self.MIN_MEM_FOR_TPCH['Q14'])
    self.low_memory_limit_test(vector, 'tpch-q18', self.MIN_MEM_FOR_TPCH['Q18'])
    self.low_memory_limit_test(vector, 'tpch-q20', self.MIN_MEM_FOR_TPCH['Q20'])
    for impalad in ImpalaCluster().impalads:
      verifier = MetricVerifier(impalad.service)
      verifier.wait_for_metric("impala-server.num-fragments-in-flight", 0)

@SkipIfLocal.mem_usage_different
class TestTpchPrimitivesMemLimitError(TestLowMemoryLimits):
  """
  Memory usage tests using targeted-perf queries to exercise specific operators.
  """

  # The mem limits that will be used.
  MEM_IN_MB = [20, 100, 120, 200]

  # Different values of mem limits and minimum mem limit (in MBs) each query is expected
  # to run without problem. Determined by manual binary search.
  MIN_MEM = { 'primitive_broadcast_join_3': 115, 'primitive_groupby_bigint_highndv': 110,
              'primitive_orderby_all': 120}

  @classmethod
  def get_workload(self):
    # Note: this workload doesn't run exhaustively. See IMPALA-3947 before trying to move
    # this test to exhaustive.
    return 'targeted-perf'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpchPrimitivesMemLimitError, cls).add_test_dimensions()

    cls.ImpalaTestMatrix.add_dimension(
      ImpalaTestDimension('mem_limit', *cls.MEM_IN_MB))

    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  def run_primitive_query(self, vector, query_name):
    self.low_memory_limit_test(vector, query_name, self.MIN_MEM[query_name])

  def test_low_mem_limit_broadcast_join_3(self, vector):
    """Test hash join memory requirements."""
    self.run_primitive_query(vector, 'primitive_broadcast_join_3')

  def test_low_mem_limit_groupby_bigint_highndv(self, vector):
    """Test grouping aggregation memory requirements."""
    self.run_primitive_query(vector, 'primitive_groupby_bigint_highndv')

  def test_low_mem_limit_orderby_all(self, vector):
    """Test sort and analytic memory requirements."""
    self.run_primitive_query(vector, 'primitive_orderby_all')


@SkipIfLocal.mem_usage_different
class TestTpcdsMemLimitError(TestLowMemoryLimits):
  # The mem limits that will be used.
  MEM_IN_MB = [20, 100, 116, 150]

  # Different values of mem limits and minimum mem limit (in MBs) each query is expected
  # to run without problem. Those values were determined by manual testing.
  MIN_MEM_FOR_TPCDS = { 'q53' : 116}

  @classmethod
  def get_workload(self):
    # Note: this workload doesn't run exhaustively. See IMPALA-3947 before trying to move
    # this test to exhaustive.
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsMemLimitError, cls).add_test_dimensions()

    cls.ImpalaTestMatrix.add_dimension(
      ImpalaTestDimension('mem_limit', *TestTpcdsMemLimitError.MEM_IN_MB))

    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  def test_low_mem_limit_q53(self, vector):
    self.low_memory_limit_test(
        vector, 'tpcds-decimal_v2-q53', self.MIN_MEM_FOR_TPCDS['q53'])
