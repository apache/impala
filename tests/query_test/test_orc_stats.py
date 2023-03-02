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

from __future__ import absolute_import, division, print_function
from tests.common.impala_test_suite import ImpalaTestSuite


class TestOrcStats(ImpalaTestSuite):
  """
  This suite tests runtime optimizations based on ORC statistics.
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestOrcStats, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'orc')

  def test_orc_stats(self, vector, unique_database):
    # The test makes assumptions about the number of rows read from the orc-reader
    # inside a fragment, so we ensure that the tests run in a single fragment.
    vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/orc-stats', vector, use_db=unique_database)
