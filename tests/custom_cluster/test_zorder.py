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

import shlex
import pprint
import re

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import SkipIfLocal
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.util.test_file_parser import QueryTestSectionReader, remove_comments

# Temporary classes for testing Z-ordering frontend features.
# These classes are based on tests/metadata/test_ddl.py
# TODO: merge the testfiles and delete this test when Z-order tests do not
# require custom cluster anymore.
class TestZOrder(CustomClusterTestSuite):
  VALID_SECTION_NAMES = ["CREATE_TABLE", "CREATE_VIEW", "QUERY", "RESULTS"]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestZOrder, cls).add_test_dimensions()

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @SkipIfLocal.hdfs_client
  @UniqueDatabase.parametrize(sync_ddl=True, num_dbs=2)
  def test_alter_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False

    # Create an unpartitioned table to get a filesystem directory that does not
    # use the (key=value) format. The directory is automatically cleanup up
    # by the unique_database fixture.
    self.client.execute("create table {0}.part_data (i int)".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/part_data".format(unique_database))
    self.filesystem_client.create_file(
        "test-warehouse/{0}.db/part_data/data.txt".format(unique_database),
        file_data='1984')
    self.run_test_case('QueryTest/alter-table-zorder', vector, use_db=unique_database)

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-zorder', vector, use_db=unique_database)

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_like_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-like-table-zorder', vector,
        use_db=unique_database)

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_like_file(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-like-file-zorder', vector,
        use_db=unique_database)

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_as_select(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-as-select-zorder', vector,
        use_db=unique_database)
