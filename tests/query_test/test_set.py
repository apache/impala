#!/usr/bin/env python
# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
#
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
#
# Tests for SET <query option>

import logging
import pytest
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_dimensions import *
from tests.common.impala_test_suite import ImpalaTestSuite, SINGLE_NODE_ONLY

class TestSet(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSet, cls).add_test_dimensions()
    # This test only needs to be run once.
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def test_set(self, vector):
    self.run_test_case('QueryTest/set', vector)

  def test_set_negative(self, vector):
    # Test that SET with an invalid config option name fails
    try:
      self.execute_query('set foo=bar')
      assert False, 'Expected to fail'
    except ImpalaBeeswaxException as e:
      assert "Ignoring invalid configuration option: foo" in str(e)

    # Test that SET with an invalid config option value fails
    try:
      self.execute_query('set parquet_compression_codec=bar')
      assert False, 'Expected to fail'
    except ImpalaBeeswaxException as e:
      assert "Invalid parquet compression codec: bar" in str(e)

  @pytest.mark.execute_serially
  def test_set_mem_limit(self, vector):
    # Test that SET actually does change the mem_limit
    self.execute_query('select 1')
    self.execute_query('set mem_limit=1');
    try:
      self.execute_query('select 1')
      assert False, 'Expected to fail'
    except ImpalaBeeswaxException as e:
      assert "Memory limit exceeded" in str(e)
    self.execute_query('set mem_limit=0');
    self.execute_query('select 1')
