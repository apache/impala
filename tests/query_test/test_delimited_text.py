# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala tests for different tuple delimiters, field delimiters,
# and escape characters.
#
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension


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
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def setup_method(self, method):
    # cleanup and create a fresh test database
    self.__cleanup()
    self.execute_query("create database %s" % self.TEST_DB_NAME)

  def teardown_method(self, method):
    self.__cleanup()

  def __cleanup(self):
    self.client.execute('use default')
    self.client.execute('drop table if exists %s.cbn' % self.TEST_DB_NAME)
    self.client.execute('drop table if exists %s.dhp' % self.TEST_DB_NAME)
    self.client.execute('drop table if exists %s.tecn' % self.TEST_DB_NAME)
    self.client.execute('drop database if exists %s' % self.TEST_DB_NAME)

  def test_delimited_text(self, vector):
    self.run_test_case('QueryTest/delimited-text', vector)

  @pytest.mark.execute_serially
  def test_delimited_text_latin_chars(self, vector):
    """Verifies Impala is able to properly handle delimited text that contains
    extended ASCII/latin characters. Marked as running serial because of shared
    cleanup/setup"""
    self.run_test_case('QueryTest/delimited-latin-text', vector, encoding="latin-1")
