# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Functional tests running the TPCH and TPCDS workload twice to test tuple cache.
from __future__ import absolute_import, division, print_function
import pytest

from tests.common.environ import IS_TUPLE_CACHE_CORRECT_CHECK
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.test_file_parser import load_tpc_queries_name_sorted

MT_DOP_VALUES = [0, 4]


def run_tuple_cache_test(self, vector, query, mtdop):
  vector.get_value('exec_option')['enable_tuple_cache'] = True
  # Use a long runtime filter wait time (1 minute) to ensure filters arrive before
  # generating the tuple cache for correctness check.
  if IS_TUPLE_CACHE_CORRECT_CHECK:
    vector.get_value('exec_option')['runtime_filter_wait_time_ms'] = 60000
    vector.get_value('exec_option')['enable_tuple_cache_verification'] = True
  vector.get_value('exec_option')['mt_dop'] = mtdop
  # Run twice to test write and read the tuple cache.
  self.run_test_case(query, vector)
  self.run_test_case(query, vector)


@SkipIf.not_tuple_cache
class TestTupleCacheTpchQuery(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheTpchQuery, cls).add_test_dimensions()
    if cls.exploration_strategy() != 'exhaustive':
      cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
      cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.parametrize("query", load_tpc_queries_name_sorted('tpch'))
  @pytest.mark.parametrize("mtdop", MT_DOP_VALUES)
  def test_tpch(self, vector, query, mtdop):
    run_tuple_cache_test(self, vector, query, mtdop)


@SkipIf.not_tuple_cache
class TestTupleCacheTpcdsQuery(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheTpcdsQuery, cls).add_test_dimensions()
    if cls.exploration_strategy() != 'exhaustive':
      cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
      cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.parametrize("query", load_tpc_queries_name_sorted('tpcds'))
  @pytest.mark.parametrize("mtdop", MT_DOP_VALUES)
  def test_tpcds(self, vector, query, mtdop):
    run_tuple_cache_test(self, vector, query, mtdop)
