#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala insert tests
#
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_exec_option_dimension

class TestDelimitedText(ImpalaTestSuite):
  """
  Tests delimited text files with different tuple delimiters, field delimiters
  and escape characters.
  """

  TEST_DB_NAME = "delim_text_test_db"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDelimitedText, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # Only run on delimited text with no compression.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text')
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    # cleanup and create a fresh test database
    self.cleanup_db(self.TEST_DB_NAME)
    self.client.refresh()
    self.execute_query("create database %s" % (self.TEST_DB_NAME))

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)

  @pytest.mark.execute_serially
  def test_delimited_text(self, vector):
    self.run_test_case('QueryTest/delimited-text', vector)
