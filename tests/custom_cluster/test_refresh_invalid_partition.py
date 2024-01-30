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

from __future__ import absolute_import
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestRefreshInvalidPartition(CustomClusterTestSuite):
  @classmethod
  def get_workload(self):
      return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--topic_update_log_gc_frequency=10")
  def test_refresh_invalid_partition_with_sync_ddl(self, vector, unique_database):
    """
    Regression test for IMPALA-12448. Avoid getting stuck when refreshing a
    non-existent partition with sync_ddl.
    """
    table_name_1 = unique_database + '.' + "partition_test_table_1"
    table_name_2 = unique_database + '.' + "partition_test_table_2"
    self.client.execute(
        'create table %s (y int) partitioned by (x int)' % table_name_1)
    self.client.execute(
        'create table %s (y int) partitioned by (x int)' % table_name_2)

    self.client.execute('insert into table %s partition (x=1) values(1)'
                        % table_name_1)
    self.client.execute('insert into table %s partition (x=1) values(1)'
                        % table_name_2)

    assert '1\t1' == self.client.execute(
        'select * from %s' % table_name_1).get_data()
    assert '1\t1' == self.client.execute(
        'select * from %s' % table_name_2).get_data()

    """
    Run it multiple times so that at least one topic update log GC is triggered.
    """
    i = 15
    while i > 0:
      self.execute_query('refresh %s' % table_name_1,
        query_options={"SYNC_DDL": "true"})
      i -= 1

    """Refresh a non-existent partition with sync_ddl."""
    self.execute_query_expect_success(self.client, 'refresh %s partition (x=999)'
        % table_name_2, query_options={"SYNC_DDL": "true", "EXEC_TIME_LIMIT_S": "30"})

  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_update_frequency_ms=5000")
  def test_refresh_missing_partition(self, unique_database):
    client1 = self.cluster.impalads[1].service.create_beeswax_client()
    client2 = self.cluster.impalads[2].service.create_beeswax_client()
    self.client.execute('create table {}.tbl (i int) partitioned by (p int)'
        .format(unique_database))
    self.execute_query(
        'insert into {}.tbl partition(p) values (0,0), (1,1)'.format(unique_database),
        query_options={"SYNC_DDL": "true"})
    self.execute_query_expect_success(
        self.client,
        'alter table {}.tbl drop partition(p=0)'.format(unique_database),
        {"SYNC_DDL": "false"})
    self.execute_query_expect_success(
        client1,
        'refresh {}.tbl partition(p=0)'.format(unique_database),
        {"SYNC_DDL": "true"})
    show_parts_stmt = 'show partitions {}.tbl'.format(unique_database)
    res = self.execute_query_expect_success(client2, show_parts_stmt)
    # First line is the header. Only one partition should be shown so the
    # result has two lines.
    assert len(res.data) == 2
    res = self.execute_query_expect_success(client1, show_parts_stmt)
    assert len(res.data) == 2
    res = self.execute_query_expect_success(self.client, show_parts_stmt)
    assert len(res.data) == 2
