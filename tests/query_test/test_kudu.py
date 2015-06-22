# Copyright 2012 Cloudera Inc.
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

from tests.common.impala_test_suite import ImpalaTestSuite
import pytest

class TestKuduOperations(ImpalaTestSuite):
  """
  This suite tests the different modification operations when using a kudu table.
  """

  @classmethod
  def file_format_constraint(cls, v):
    return v.get_value('table_format').file_format in ["parquet"]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduOperations, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(cls.file_format_constraint)

  def setup_method(self, method):
    self.cleanup_db("kududb_test")
    self.client.execute("create database kududb_test")

  def teardown_method(self, methd):
    self.cleanup_db("kududb_test")

  def test_kudu_scan_node(self, vector):
    self.run_test_case('QueryTest/kudu-scan-node', vector, use_db="functional_kudu")

  @pytest.mark.execute_serially
  def test_insert_update_delete(self, vector):
    self.run_test_case('QueryTest/kudu_crud', vector, use_db="kududb_test")
