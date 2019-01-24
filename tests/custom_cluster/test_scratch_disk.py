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
# Tests for query expiration.

import os
import pytest
import shutil
import stat
import tempfile

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

class TestScratchDir(CustomClusterTestSuite):

  # Query with order by requires spill to disk if intermediate results don't fit in mem
  spill_query = """
      select o_orderdate, o_custkey, o_comment
      from tpch.orders
      order by o_orderdate
      """
  # Query without order by can be executed without spilling to disk.
  in_mem_query = """
      select o_orderdate, o_custkey, o_comment from tpch.orders
      """
  # Buffer pool limit that is low enough to force Impala to spill to disk when executing
  # spill_query.
  buffer_pool_limit = "45m"

  def count_nonempty_dirs(self, dirs):
    count = 0
    for dir_name in dirs:
      if os.path.exists(dir_name) and len(os.listdir(dir_name)) > 0:
        count += 1
    return count

  def get_dirs(dirs):
    return ','.join(dirs)

  def generate_dirs(self, num, writable=True, non_existing=False):
    result = []
    for i in xrange(num):
      dir_path = tempfile.mkdtemp()
      if non_existing:
        shutil.rmtree(dir_path)
      elif not writable:
        os.chmod(dir_path, stat.S_IREAD)
      if not non_existing:
        self.created_dirs.append(dir_path)
      result.append(dir_path)
      print "Generated dir" + dir_path
    return result

  def setup_method(self, method):
    # Don't call the superclass method to prevent starting Impala before each test. In
    # this file, each test is responsible for doing that because we want to generate
    # the parameter string to start-impala-cluster in each test method.
    self.created_dirs = []

  def teardown_method(self, method):
    for dir_path in self.created_dirs:
      shutil.rmtree(dir_path, ignore_errors=True)

  @pytest.mark.execute_serially
  def test_multiple_dirs(self, vector):
    """ 5 empty directories are created in the /tmp directory and we verify that only
        one of those directories is used as scratch disk. Only one should be used as
        scratch because all directories are on same disk."""
    normal_dirs = self.generate_dirs(5)
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(normal_dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=false'])
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=1)
    exec_option = vector.get_value('exec_option')
    exec_option['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_expect_success(client, self.spill_query, exec_option)
    assert self.count_nonempty_dirs(normal_dirs) == 1

  @pytest.mark.execute_serially
  def test_no_dirs(self, vector):
    """ Test we can execute a query with no scratch dirs """
    self._start_impala_cluster(['--impalad_args=-logbuflevel=-1 -scratch_dirs='])
    self.assert_impalad_log_contains("WARNING",
        "Running without spill to disk: no scratch directories provided\.")
    exec_option = vector.get_value('exec_option')
    exec_option['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    # Expect spill to disk to fail
    self.execute_query_expect_failure(client, self.spill_query, exec_option)
    # Should be able to execute in-memory query
    self.execute_query_expect_success(client, self.in_mem_query, exec_option)

  @pytest.mark.execute_serially
  def test_non_writable_dirs(self, vector):
    """ Test we can execute a query with only bad non-writable scratch """
    non_writable_dirs = self.generate_dirs(5, writable=False)
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(
      ','.join(non_writable_dirs))])
    self.assert_impalad_log_contains("ERROR", "Running without spill to disk: could "
        + "not use any scratch directories in list:.*. See previous "
        + "warnings for information on causes.")
    self.assert_impalad_log_contains("WARNING", "Could not remove and recreate directory "
            + ".*: cannot use it for scratch\. Error was: .*", expected_count=5)
    exec_option = vector.get_value('exec_option')
    exec_option['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    # Expect spill to disk to fail
    self.execute_query_expect_failure(client, self.spill_query, exec_option)
    # Should be able to execute in-memory query
    self.execute_query_expect_success(client, self.in_mem_query, exec_option)
    assert self.count_nonempty_dirs(non_writable_dirs) == 0

  @pytest.mark.execute_serially
  def test_non_existing_dirs(self, vector):
    """ Test that non-existing directories are not created or used """
    non_existing_dirs = self.generate_dirs(5, non_existing=True)
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(
      ','.join(non_existing_dirs))])
    self.assert_impalad_log_contains("ERROR", "Running without spill to disk: could "
        + "not use any scratch directories in list:.*. See previous "
        + "warnings for information on causes.")
    self.assert_impalad_log_contains("WARNING", "Cannot use directory .* for scratch: "
        + "Encountered exception while verifying existence of directory path",
        expected_count=5)
    exec_option = vector.get_value('exec_option')
    exec_option['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    # Expect spill to disk to fail
    self.execute_query_expect_failure(client, self.spill_query, exec_option)
    # Should be able to execute in-memory query
    self.execute_query_expect_success(client, self.in_mem_query, exec_option)
    assert self.count_nonempty_dirs(non_existing_dirs) == 0

  @pytest.mark.execute_serially
  def test_write_error_failover(self, vector):
    """ Test that we can fail-over to writable directories if other directories
        have permissions changed or are removed after impalad startup."""
    dirs = self.generate_dirs(3);
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=true'])
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=len(dirs))
    exec_option = vector.get_value('exec_option')
    exec_option['buffer_pool_limit'] = self.buffer_pool_limit
    # Trigger errors when writing the first two directories.
    shutil.rmtree(dirs[0]) # Remove the first directory.
    # Make all subdirectories in the second directory non-writable.
    for dirpath, dirnames, filenames in os.walk(dirs[1]):
      os.chmod(dirpath, stat.S_IREAD)

    # Should still be able to spill to the third directory.
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_expect_success(client, self.spill_query, exec_option)
