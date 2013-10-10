#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
#

from tests.common.impala_test_suite import ImpalaTestSuite
import time
import pytest

class TestInsertBehaviour(ImpalaTestSuite):
  """Tests for INSERT behaviour that isn't covered by checking query results"""

  @pytest.mark.excute_serially
  def test_insert_removes_staging_files(self):
    insert_staging_dir = \
      "test-warehouse/functional.db/insert_overwrite_nopart/.impala_insert_staging"
    self.hdfs_client.delete_file_dir(insert_staging_dir, recursive=True)
    self.client.execute("""INSERT OVERWRITE
functional.insert_overwrite_nopart SELECT int_col FROM functional.tinyinttable""")
    ls = self.hdfs_client.list_dir(insert_staging_dir)
    assert len(ls['FileStatuses']['FileStatus']) == 0

  @pytest.mark.excute_serially
  def test_insert_preserves_hidden_files(self):
    """Test that INSERT OVERWRITE preserves hidden files in the root table directory"""
    table_dir = "test-warehouse/functional.db/insert_overwrite_nopart/"
    hidden_file_locations = [".hidden", "_hidden"]
    dir_locations = ["dir", ".hidden_dir"]
    for dir in dir_locations:
      self.hdfs_client.make_dir(table_dir + dir)
    for file in hidden_file_locations:
      self.hdfs_client.create_file(table_dir + file, '', overwrite=True)

    self.client.execute("""INSERT OVERWRITE
functional.insert_overwrite_nopart SELECT int_col FROM functional.tinyinttable""")

    for file in hidden_file_locations:
      try:
        self.hdfs_client.get_file_dir_status(table_dir + file)
      except:
        err_msg = "Hidden file '%s' was unexpectedly deleted by INSERT OVERWRITE"
        pytest.fail(err_msg % (table_dir + file))

    for dir in dir_locations:
      try:
        self.hdfs_client.get_file_dir_status(table_dir + file)
      except:
        err_msg = "Directory '%s' was unexpectedly deleted by INSERT OVERWRITE"
        pytest.fail(err_msg % (table_dir + dir))
