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
from tests.common.skip import SkipIfEC, SkipIfLocal, SkipIfFS
from tests.common.test_dimensions import create_parquet_dimension


class TestResourceLimits(ImpalaTestSuite):
  """Test resource limit functionality."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestResourceLimits, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_parquet_dimension(cls.get_workload()))

  @SkipIfLocal.multiple_impalad
  @SkipIfEC.parquet_file_size
  def test_thread_limits(self, vector):
    # Remove option from vector to allow test file to override it per query.
    del vector.get_value('exec_option')['num_nodes']
    self.run_test_case('QueryTest/thread-limits', vector)

  @SkipIfLocal.multiple_impalad
  def test_resource_limits(self, vector):
    self.run_test_case('QueryTest/query-resource-limits', vector)

  @SkipIfFS.hbase
  @SkipIfLocal.multiple_impalad
  def test_resource_limits_hbase(self, vector):
    self.run_test_case('QueryTest/query-resource-limits-hbase', vector)

  @SkipIfLocal.multiple_impalad
  def test_resource_limits_kudu(self, vector):
    self.run_test_case('QueryTest/query-resource-limits-kudu', vector)
