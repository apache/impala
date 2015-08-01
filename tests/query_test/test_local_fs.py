# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
# Validates table stored on the LocalFileSystem.
#
import pytest
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

  def setup_method(self, method):
    self.cleanup_db("local_fs_db")
    self.client.execute("create database local_fs_db")

  def teardown_method(self, method):
    self.cleanup_db("local_fs_db")

  @pytest.mark.execute_serially
  def test_local_filesystem(self, vector):
    self.run_test_case('QueryTest/local-filesystem', vector)
