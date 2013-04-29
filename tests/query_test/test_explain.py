#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Functional tests running EXPLAIN statements.
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestExplain(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExplain, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('exec_option')['batch_size'] == 0 and\
        v.get_value('exec_option')['disable_codegen'] == False and\
        v.get_value('exec_option')['num_nodes'] != 1)

  def test_explain(self, vector):
    self.run_test_case('QueryTest/explain', vector)
