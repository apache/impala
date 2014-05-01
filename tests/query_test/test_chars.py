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

  @classmethod
  def add_test_dimensions(cls):
    super(TestStringQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

  def test_varchar(self, vector):
    self.run_test_case('QueryTest/chars', vector)
