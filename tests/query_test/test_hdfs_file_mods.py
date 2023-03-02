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

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal
from tests.common.test_vector import ImpalaTestDimension
from subprocess import call
from tests.util.filesystem_utils import FILESYSTEM_PREFIX

# Modifications to test
MODIFICATION_TYPES=["delete_files", "delete_directory", "move_file", "append"]

@SkipIfLocal.hdfs_client
class TestHdfsFileMods(ImpalaTestSuite):
  """
  This test suite tests that modifications to HDFS files don't crash Impala.
  In particular, this interacts with existing functionality such as the
  file handle cache and IO retries.
  """

  @classmethod
  def file_format_constraint(cls, v):
    return v.get_value('table_format').file_format in ["text"]

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsFileMods, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('modification_type',\
        *MODIFICATION_TYPES))
    cls.ImpalaTestMatrix.add_constraint(cls.file_format_constraint)

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def setup_ext_table(self, vector, unique_database, new_table_location):
    # Use HDFS commands to clone the table's files at the hdfs level
    old_table_location = "{0}/test-warehouse/tinytable".format(FILESYSTEM_PREFIX)
    call(["hdfs", "dfs", "-mkdir", new_table_location])
    self.filesystem_client.copy(old_table_location + "/*", new_table_location,
        overwrite=True)

    # Create an external table with the new files (tinytable has two string columns)
    create_table = "create external table {0}.t1 (a string, b string) "\
        + "row format delimited fields terminated by \',\' location \'{1}\'"
    self.client.execute(create_table.format(unique_database, new_table_location))

  @pytest.mark.execute_serially
  def test_file_modifications(self, vector, unique_database):
    """Tests file modifications on an external table."""

    new_table_location = "{0}/test-warehouse/{1}".format(FILESYSTEM_PREFIX,\
        unique_database)
    self.setup_ext_table(vector, unique_database, new_table_location)

    # Query the table. If file handle caching is enabled, this will fill the cache.
    count_query = "select count(*) from {0}.t1".format(unique_database)
    original_result = self.execute_query_expect_success(self.client, count_query)
    assert(original_result.data[0] == '3')

    # Do the modification based on the test settings
    modification_type = vector.get_value('modification_type')
    if (modification_type == 'delete_files'):
      # Delete the data file (not the directory)
      call(["hdfs", "dfs", "-rm", "-skipTrash", new_table_location + "/*"])
    elif (modification_type == 'delete_directory'):
      # Delete the whole directory (including data file)
      call(["hdfs", "dfs", "-rm", "-r", "-skipTrash", new_table_location])
    elif (modification_type == 'move_file'):
      # Move the file underneath the directory
      call(["hdfs", "dfs", "-mv", new_table_location + "/data.csv", \
          new_table_location + "/data.csv.moved"])
    elif (modification_type == 'append'):
      # Append a copy of the hdfs file to itself (duplicating all entries)
      local_tmp_location = "/tmp/{0}.data.csv".format(unique_database)
      call(["hdfs", "dfs", "-copyToLocal", new_table_location + "/data.csv", \
           local_tmp_location])
      call(["hdfs", "dfs", "-appendToFile", local_tmp_location, \
           new_table_location + "/data.csv"])
      call(["rm", local_tmp_location])
    else:
      assert(false)

    # The query might fail, but nothing should crash.
    try:
      self.execute_query(count_query)
    except ImpalaBeeswaxException as e:
      pass

    # Invalidate metadata
    invalidate_metadata_sql = "invalidate metadata {0}.t1".format(unique_database)
    self.execute_query_expect_success(self.client, invalidate_metadata_sql)

    # Verify that nothing crashes and the query should succeed
    new_result = self.execute_query_expect_success(self.client, count_query)
    if (modification_type == 'move_file'):
      assert(new_result.data[0] == '3')
    elif (modification_type == 'delete_files' or \
          modification_type == 'delete_directory'):
      assert(new_result.data[0] == '0')
    elif (modification_type == 'append'):
      # Allow either the old count or the new count to tolerate delayed consistency.
      assert(new_result.data[0] == '6' or new_result.data[0] == '3')

    # Drop table
    drop_table_sql = "drop table {0}.t1".format(unique_database)
    self.execute_query_expect_success(self.client, drop_table_sql)

    # Cleanup directory (which may already be gone)
    call(["hdfs", "dfs", "-rm", "-r", "-skipTrash", new_table_location])

