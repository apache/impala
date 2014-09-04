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

import logging
import pytest
from copy import deepcopy
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import (TestDimension,
    create_single_exec_option_dimension,
    create_parquet_dimension)

class TestSpillingBase(CustomClusterTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

  @classmethod
  def setup_class(cls):
    # Reduce the IO read size. This reduces the memory required to trigger spilling.
    options = ["--impalad_args=--read_size=1000000"]
    cls._start_impala_cluster(options)
    cls.client = cls.cluster.get_first_impalad().service.create_beeswax_client()

  def setup_method(self, method):
    pass

class TestSpilling(TestSpillingBase):
  @classmethod
  def add_test_dimensions(cls):
    super(TestSpilling, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_parquet_dimension('tpch'))

  def test_spilling(self, vector):
    new_vector = deepcopy(vector)
    self.run_test_case('QueryTest/spilling', new_vector)

