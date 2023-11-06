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
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestPreLoadTableTypes(CustomClusterTestSuite):
  """
  Test catalogd runs with --pull_table_types_and_comments=true keeps the table/view
  types and comments loaded.
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestPreLoadTableTypes, cls).setup_class()

  def verify_existing_tables(self, hs2_client):
    results = hs2_client.get_tables()
    types_and_comments = {}
    for _, db, tbl, tbl_type, comment in results:
      types_and_comments[db + '.' + tbl] = (tbl_type, comment)
    assert types_and_comments['functional.alltypestiny'] == ('TABLE', 'Tiny table')
    assert types_and_comments['functional.alltypes_view'] == ('VIEW', 'View on alltypes')
    assert types_and_comments['functional.alltypes'] == ('TABLE', '')
    assert types_and_comments['functional_orc_def.materialized_view'] == \
        ('MATERIALIZED_VIEW', '')

  def verify_table_types_and_comments(self, unique_database):
    n = self.get_impalad_cluster_size()
    hs2_clients = [self.create_client_for_nth_impalad(nth=i, protocol='hs2')
                   for i in range(n)]

    # Verify table types and comments at startup
    for i in range(n):
      self.verify_existing_tables(hs2_clients[i])

    # Test INVALIDATE METADATA on unloaded tables/views
    self.execute_query("invalidate metadata functional.alltypestiny")
    self.execute_query("invalidate metadata functional.alltypes_view")
    self.execute_query("invalidate metadata functional_orc_def.materialized_view")
    self.verify_existing_tables(self.hs2_client)
    # Test again on multiple coordinators. Use sync_ddl=true on the last command to assure
    # all coordinators get the updates.
    self.execute_query("invalidate metadata functional.alltypestiny")
    self.execute_query("invalidate metadata functional.alltypes_view")
    self.execute_query("invalidate metadata functional_orc_def.materialized_view",
                       {'sync_ddl': True})
    for i in range(n):
      self.verify_existing_tables(hs2_clients[i])

    # Verify table types and comments after global INVALIDATE METADATA
    self.execute_query("invalidate metadata", {'sync_ddl': True})
    for i in range(n):
      self.verify_existing_tables(hs2_clients[i])

    # Verify table types and comments on loaded table/view
    self.execute_query("describe functional.alltypestiny")
    self.execute_query("describe functional.alltypes_view")
    self.execute_query("describe functional_orc_def.materialized_view",
                       {'sync_ddl': True})
    for i in range(n):
      self.verify_existing_tables(hs2_clients[i])

    # Test INVALIDATE METADATA on loaded table/view
    self.execute_query("invalidate metadata functional.alltypestiny")
    self.execute_query("invalidate metadata functional.alltypes_view")
    self.execute_query("invalidate metadata functional_orc_def.materialized_view",
                       {'sync_ddl': True})
    for i in range(n):
      self.verify_existing_tables(hs2_clients[i])

    # Verify types and comments on newly created table/view
    table_name = "__hs2_table_comment_test"
    view_name = "__hs2_view_comment_test"
    self.execute_query(
        "create table {0}.{1} (a int) comment 'table comment'"
        .format(unique_database, table_name))
    self.execute_query(
        "create view {0}.{1} comment 'view comment' as select * from {0}.{2}"
        .format(unique_database, view_name, table_name), {'sync_ddl': True})
    for i in range(n):
      results = hs2_clients[i].get_tables(database=unique_database)
      for _, db, tbl, tbl_type, comment in results:
        if tbl == table_name:
          assert tbl_type == 'TABLE'
          assert comment == 'table comment'
        elif tbl == view_name:
          assert tbl_type == 'VIEW'
          assert comment == 'view comment'

  @CustomClusterTestSuite.with_args(
    catalogd_args="--pull_table_types_and_comments=true --catalog_topic_mode=full "
                  "--hms_event_polling_interval_s=0",
    impalad_args="--use_local_catalog=false")
  def test_legacy_catalog_mode_without_event_processor(self, unique_database):
    self.verify_table_types_and_comments(unique_database)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--pull_table_types_and_comments --catalog_topic_mode=minimal "
                  "--hms_event_polling_interval_s=0",
    impalad_args="--use_local_catalog=true")
  def test_local_catalog_mode_without_event_processor(self, unique_database):
    self.verify_table_types_and_comments(unique_database)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--pull_table_types_and_comments --catalog_topic_mode=mixed "
                  "--hms_event_polling_interval_s=0",
    start_args="--per_impalad_args=--use_local_catalog=false;--use_local_catalog=true")
  def test_mixed_catalog_mode_without_event_processor(self, unique_database):
    self.verify_table_types_and_comments(unique_database)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--pull_table_types_and_comments=true --catalog_topic_mode=full "
                  "--hms_event_polling_interval_s=1",
    impalad_args="--use_local_catalog=false")
  def test_legacy_catalog_mode_with_event_processor(self, unique_database):
    self.verify_table_types_and_comments(unique_database)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--pull_table_types_and_comments --catalog_topic_mode=minimal "
                  "--hms_event_polling_interval_s=1",
    impalad_args="--use_local_catalog=true")
  def test_local_catalog_mode_with_event_processor(self, unique_database):
    self.verify_table_types_and_comments(unique_database)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--pull_table_types_and_comments --catalog_topic_mode=mixed "
                  "--hms_event_polling_interval_s=1",
    start_args="--per_impalad_args=--use_local_catalog=false;--use_local_catalog=true")
  def test_mixed_catalog_mode_with_event_processor(self, unique_database):
    self.verify_table_types_and_comments(unique_database)
