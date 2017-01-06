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

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension

class TestScratchLimit(ImpalaTestSuite):
  """
  This class tests the functionality of setting the scratch limit as a query option
  """

  spill_query = """
      select o_orderdate, o_custkey, o_comment
      from tpch.orders
      order by o_orderdate
      """

  # Block manager memory limit that is low enough to
  # force Impala to spill to disk when executing 'spill_query'
  max_block_mgr_memory = "64m"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScratchLimit, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_with_high_scratch_limit(self, vector):
    """
    Query runs to completion with a scratch limit well above
    its required scratch space which in this case is 128m.
    """
    exec_option = vector.get_value('exec_option')
    exec_option['max_block_mgr_memory'] = self.max_block_mgr_memory
    exec_option['scratch_limit'] = '500m'
    self.execute_query_expect_success(self.client, self.spill_query, exec_option)

  def test_with_low_scratch_limit(self, vector):
    """
    Query throws the appropriate exception with a scratch limit well below
    its required scratch space which in this case is 128m.
    """
    exec_option = vector.get_value('exec_option')
    exec_option['max_block_mgr_memory'] = self.max_block_mgr_memory
    exec_option['scratch_limit'] = '24m'
    expected_error = 'Scratch space limit of %s bytes exceeded'
    scratch_limit_in_bytes = 24 * 1024 * 1024
    try:
      self.execute_query(self.spill_query, exec_option)
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert expected_error % scratch_limit_in_bytes in str(e)

  def test_with_zero_scratch_limit(self, vector):
    """
    Query throws the appropriate exception with a scratch limit of
    zero which means no scratch space can be allocated.
    """
    exec_option = vector.get_value('exec_option')
    exec_option['max_block_mgr_memory'] = self.max_block_mgr_memory
    exec_option['scratch_limit'] = '0'
    self.execute_query_expect_failure(self.spill_query, exec_option)

  def test_with_unlimited_scratch_limit(self, vector):
    """
    Query runs to completion with a scratch Limit of -1 means default/no limit.
    """
    exec_option = vector.get_value('exec_option')
    exec_option['max_block_mgr_memory'] = self.max_block_mgr_memory
    exec_option['scratch_limit'] = '-1'
    self.execute_query_expect_success(self.client, self.spill_query, exec_option)

  def test_without_specifying_scratch_limit(self, vector):
    """
    Query runs to completion with the default setting of no scratch limit.
    """
    exec_option = vector.get_value('exec_option')
    exec_option['max_block_mgr_memory'] = self.max_block_mgr_memory
    self.execute_query_expect_success(self.client, self.spill_query, exec_option)

  def test_with_zero_scratch_limit_no_memory_limit(self, vector):
    """
    Query runs to completion without spilling as there is no limit on block memory manger.
    Scratch limit of zero ensures spilling is disabled.
    """
    exec_option = vector.get_value('exec_option')
    exec_option['scratch_limit'] = '0'
    self.execute_query_expect_success(self.client, self.spill_query, exec_option)
