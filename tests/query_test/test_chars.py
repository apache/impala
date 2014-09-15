#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
import logging
import pytest
from copy import copy
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestStringQueries(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def setup_method(self, method):
    self.__cleanup_char_tables()
    self.__create_char_tables()

  def teardown_method(self, method):
    self.__cleanup_char_tables()

  def __cleanup_char_tables(self):
    self.client.execute('drop table if exists functional.test_char_tmp');
    self.client.execute('drop table if exists functional.test_varchar_tmp');

  def __create_char_tables(self):
    self.client.execute(
        'create table if not exists functional.test_varchar_tmp (vc varchar(5))')
    self.client.execute(
        'create table if not exists functional.test_char_tmp (c char(5))')

  @classmethod
  def add_test_dimensions(cls):
    super(TestStringQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
      create_exec_option_dimension(disable_codegen_options=[True]))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['text'] and
        v.get_value('table_format').compression_codec in ['none'])

  def test_varchar(self, vector):
    self.run_test_case('QueryTest/chars', vector)
