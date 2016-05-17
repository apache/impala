# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
# Validates table stored on the LocalFileSystem.
#
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension

class TestLocalFileSystem(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLocalFileSystem, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and \
        v.get_value('table_format').compression_codec == 'none')

  def test_local_filesystem(self, vector, unique_database):
    self.run_test_case('QueryTest/local-filesystem', vector, unique_database)
