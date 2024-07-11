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
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestIcebergAlwaysAllowMergeOnRead(CustomClusterTestSuite):
  """Tests for checking the behaviour of startup flag
   'iceberg_always_allow_merge_on_read_operations'."""

  TABLE_NAME = "iceberg_copy_on_write"

  def create_test_table(self, db):
    self.execute_query("CREATE TABLE {0}.{1} (i int) STORED BY ICEBERG "
        "tblproperties('format-version'='2', 'write.delete.mode'='copy-on-write', "
        "'write.update.mode'='copy-on-write');".format(db, self.TABLE_NAME))
    self.execute_query("INSERT INTO {0}.{1} values(1),(2);".format(db, self.TABLE_NAME))

  def delete_statement(self, db):
    return "DELETE FROM {0}.{1} WHERE i=1;".format(db, self.TABLE_NAME)

  def update_statement(self, db):
    return "UPDATE {0}.{1} SET i=3 WHERE i=2;".format(db, self.TABLE_NAME)

  def analysis_exception(self, db, operation):
    return "AnalysisException: Unsupported '{0}': 'copy-on-write' for " \
        "Iceberg table: {1}.{2}".format(operation, db, self.TABLE_NAME)

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(
      impalad_args='--iceberg_always_allow_merge_on_read_operations=true')
  @pytest.mark.execute_serially
  def test_enable_merge_on_read(self, unique_database):
    """If the flag is enabled, Impala can execute DELETE, UPDATE and MERGE operations
    even if the table property is 'copy-on-write'."""
    # TODO IMPALA-12732: add test case for MERGE.
    self.create_test_table(unique_database)
    self.execute_query_expect_success(self.client, self.delete_statement(unique_database))
    self.execute_query_expect_success(self.client, self.update_statement(unique_database))

  @CustomClusterTestSuite.with_args(
      impalad_args='--iceberg_always_allow_merge_on_read_operations=false')
  @pytest.mark.execute_serially
  def test_disable_merge_on_read(self, unique_database):
    """If the flag is disabled, Impala cannot write delete files with 'merge-on-read'
    strategy if the table property is 'copy-on-write'."""
    self.create_test_table(unique_database)
    result = self.execute_query_expect_failure(self.client,
                    self.delete_statement(unique_database))
    assert self.analysis_exception(unique_database, "write.delete.mode") in str(result)
    result = self.execute_query_expect_failure(self.client,
                    self.update_statement(unique_database))
    assert self.analysis_exception(unique_database, "write.update.mode") in str(result)
