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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)

class TestStatsExtrapolation(CustomClusterTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestStatsExtrapolation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @CustomClusterTestSuite.with_args(impalad_args=('--enable_stats_extrapolation=true'))
  def test_stats_extrapolation(self, vector, unique_database):
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['explain_level'] = 2
    self.run_test_case('QueryTest/stats-extrapolation', vector, unique_database)
