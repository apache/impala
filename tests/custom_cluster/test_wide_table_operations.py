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
from builtins import range
import os
import pytest
from subprocess import call
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf

TBL_NAME = "widetable_2000_cols_partitioned"
NUM_PARTS = 50000


@SkipIf.not_hdfs
class TestWideTableOperations(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive since it takes more than 20 mins')
    super(TestWideTableOperations, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      jvm_args="-Xmx2g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="
               + os.getenv("LOG_DIR", "/tmp"))
  def test_wide_table_operations(self, vector, unique_database):
    """Regression test for IMPALA-11812. Test DDL/DML operations on wide table.
    Use a small heap size (2GB) to make sure memory consumption is optimized.
    Each FieldSchema instance takes 24 bytes in a small heap (<32GB). Without the fix,
    catalogd will hold at least 50,000 (parts) * 2,000 (cols) = 100,000,000 FieldSchema
    instances in memory for execDdl or table loading, which already takes more than 2GB
    and will results in OOM failures."""
    # Create partition dirs and files locally
    tmp_dir = "/tmp/" + TBL_NAME
    os.mkdir(tmp_dir)
    for i in range(NUM_PARTS):
      part_dir = tmp_dir + "/p=" + str(i)
      data_file = part_dir + "/data.txt"
      os.mkdir(part_dir)
      with open(data_file, 'w') as local_file:
        local_file.write("true")
    # Upload files to HDFS
    hdfs_dir = self._get_table_location("functional." + TBL_NAME, vector)
    call(["hdfs", "dfs", "-rm", "-r", "-skipTrash", hdfs_dir])
    # Use 1 replica to save space, 8 threads to speed up
    call(["hdfs", "dfs", "-Ddfs.replication=1", "-put", "-t", "8", tmp_dir, hdfs_dir])
    # Create a new table so we don't need to drop partitions at the end.
    # It will be dropped when 'unique_database' is dropped.
    create_tbl_ddl =\
        "create external table {db}.{tbl} like functional.{tbl} " \
        "location '{location}'".format(
            db=unique_database, tbl=TBL_NAME, location=hdfs_dir)
    self.execute_query_expect_success(
        self.client, create_tbl_ddl.format(db=unique_database, tbl=TBL_NAME))

    # Recover partitions first. This takes 10mins for 50k partitions.
    recover_stmt = "alter table {db}.{tbl} recover partitions"
    # Invalidate the table to test initial metadata loading
    invalidate_stmt = "invalidate metadata {db}.{tbl}"
    # Test initial table loading and get all partitions
    show_parts_stmt = "show partitions {db}.{tbl}"
    try:
      self.execute_query_expect_success(
          self.client, recover_stmt.format(db=unique_database, tbl=TBL_NAME))
      self.execute_query_expect_success(
          self.client, invalidate_stmt.format(db=unique_database, tbl=TBL_NAME))
      res = self.execute_query_expect_success(
          self.client, show_parts_stmt.format(db=unique_database, tbl=TBL_NAME))
      # Last line is 'Total'
      assert len(res.data) == NUM_PARTS + 1
    finally:
      call(["rm", "-rf", tmp_dir])
      call(["hdfs", "dfs", "-rm", "-r", "-skipTrash", hdfs_dir])
