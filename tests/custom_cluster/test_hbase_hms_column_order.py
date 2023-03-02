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
#
# Tests for IMPALA-1658

from __future__ import absolute_import, division, print_function
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestHBaseHmsColumnOrder(CustomClusterTestSuite):
  '''If use_hms_column_order_for_hbase_tables=false, then catalogd orders HBase columns
     by family/qualifier (IMPALA-886). This is incompatible with other file formats and
     HBase tables in Hive, where the order  comes from HMS and is defined during CREATE
     TABLE. For the sake of backward compatibility the old behavior is kept as default.
     This test checks that the correct order is used if
     use_hms_column_order_for_hbase_tables=true
  '''

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'hbase')

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(
      catalogd_args="--use_hms_column_order_for_hbase_tables=true")
  def test_hbase_hms_column_order(self, vector, unique_database):
    self.run_test_case('QueryTest/hbase-hms-column-order', vector, unique_database)
