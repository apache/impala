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

# Targeted Impala HBase Tests

import pytest

from tests.common.impala_test_suite import ImpalaTestSuite

class TestHBaseQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHBaseQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(\
        lambda v: v.get_value('table_format').file_format == 'hbase')

  def test_hbase_scan_node(self, vector):
    self.run_test_case('QueryTest/hbase-scan-node', vector)

  def test_hbase_row_key(self, vector):
    self.run_test_case('QueryTest/hbase-rowkeys', vector)

  def test_hbase_filters(self, vector):
    self.run_test_case('QueryTest/hbase-filters', vector)

  def test_hbase_subquery(self, vector):
    self.run_test_case('QueryTest/hbase-subquery', vector)

  def test_hbase_inline_views(self, vector):
    self.run_test_case('QueryTest/hbase-inline-view', vector)

  def test_hbase_top_n(self, vector):
    self.run_test_case('QueryTest/hbase-top-n', vector)

  def test_hbase_limits(self, vector):
    self.run_test_case('QueryTest/hbase-limit', vector)

  @pytest.mark.execute_serially
  def test_hbase_inserts(self, vector):
    self.run_test_case('QueryTest/hbase-inserts', vector)
