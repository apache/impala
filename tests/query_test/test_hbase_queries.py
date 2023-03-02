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

from __future__ import absolute_import, division, print_function
import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS

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

  @SkipIfFS.hive
  def test_hbase_col_filter(self, vector, unique_database):
    """IMPALA-7929: test query with table created with hive and mapped to hbase. The key
    column doesn't have qualifier and the query with predicate on key column name should
    not fail"""
    table_name = "{0}.hbase_col_filter_testkeyx".format(unique_database)
    cr_table = """CREATE TABLE {0} (k STRING, c STRING) STORED BY
               'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES
               ('hbase.columns.mapping'=':key,cf:c', 'serialization.format'='1')
               TBLPROPERTIES ('hbase.table.name'=\'{1}\',
               'storage_handler'='org.apache.hadoop.hive.hbase.HBaseStorageHandler')
               """.format(table_name, table_name)
    add_data = """INSERT INTO TABLE {0} VALUES ('row1', 'c1'), ('row2', 'c2'),
             ('row3', 'c2'), ('row4', 'c4')""".format(table_name)
    del_table = "DROP TABLE IF EXISTS {0}".format(table_name)

    try:
      self.run_stmt_in_hive(cr_table, username='hdfs')
      self.run_stmt_in_hive(add_data, username='hdfs')
      self.client.execute("invalidate metadata {0}".format(table_name))
      self.run_test_case('QueryTest/hbase-col-filter', vector, unique_database)
    finally:
      self.run_stmt_in_hive(del_table)
