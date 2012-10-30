#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala insert tests
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestInsertQueries(ImpalaTestSuite):
  @classmethod
  def get_dataset(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertQueries, cls).add_test_dimensions()
    # Insert is currently only supported for text and trevni
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' or\
        v.get_value('table_format').file_format == 'trevni')
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.execute_serially
  def test_insert(self, vector):
    self.run_test_case('QueryTest/insert', vector)

  @pytest.mark.execute_serially
  def test_insert_overwrite(self, vector):
    self.run_test_case('QueryTest/insert_overwrite', vector)

  @pytest.mark.execute_serially
  def test_insert_null(self, vector):
    self.run_test_case('QueryTest/insert_null', vector)

  @pytest.mark.execute_serially
  def test_insert_overflow(self, vector):
    self.run_test_case('QueryTest/overflow', vector)
