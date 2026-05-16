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

import pytest
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfBuildType
from tests.util.test_file_parser import load_tpc_queries_name_sorted


# Basic test running TPCDS with tcmalloc's aggressive decommit turned off.
# This doesn't make any sense with sanitizer builds.
@SkipIfBuildType.sanitizer
@CustomClusterTestSuite.with_args(cluster_size=3,
    start_args="--tcmalloc_aggressive_decommit false")
class TestAggressiveDecommitOffTpcds(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
      v.get_value('table_format').file_format == 'parquet'
      and v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.execute_serially
  @pytest.mark.parametrize("query", load_tpc_queries_name_sorted('tpcds'))
  def test_tpcds(self, vector, query):
    self.run_test_case(query, vector)
    # When aggressive decommit is off, tcmalloc.pageheap-free-bytes should be non-zero
    for i in range(3):
      pageheap_free_bytes = self.cluster.impalads[i].service.get_metric_value(
          'tcmalloc.pageheap-free-bytes')
      assert pageheap_free_bytes > 0
