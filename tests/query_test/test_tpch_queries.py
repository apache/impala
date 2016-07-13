# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Functional tests running the TPCH workload.
import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension

class TestTpchQuery(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpchQuery, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    # The tpch tests take a long time to execute so restrict the combinations they
    # execute over.
    # TODO: the planner tests are based on text and need this.
    if cls.exploration_strategy() == 'core':
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format in ['text', 'kudu'])

  def get_test_file_prefix(self, vector):
    if vector.get_value('table_format').file_format in ['kudu']:
      return 'tpch-kudu-q'
    else:
      return 'tpch-q'

  def idfn(val):
    return "TPC-H: Q{0}".format(val)

  @pytest.mark.parametrize("query", xrange(1, 23), ids=idfn)
  def test_tpch(self, vector, query):
    self.run_test_case('{0}{1}'.format(self.get_test_file_prefix(vector), query),
        vector)

