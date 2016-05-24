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
# Tests for query expiration.

import pytest
import threading
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.custom_cluster_test_suite import NUM_SUBSCRIBERS, CLUSTER_SIZE
from time import sleep, time
import stat
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
import shutil
import os
import random
import tempfile

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
  # Memory limit that is low enough to get Impala to spill to disk when executing
  # spill_query and high enough that we can execute in_mem_query without spilling.
  mem_limit = "200m"

  def count_nonempty_dirs(self, dirs):
    count = 0
    for dir_name in dirs:
      if os.path.exists(dir_name) and len(os.listdir(dir_name)) > 0:
        count += 1
    return count

  def get_dirs(dirs):
    return ','.join(dirs)

  @classmethod
  def setup_class(cls):
    super(TestScratchDir, cls).setup_class()
    cls.normal_dirs = cls.generate_dirs(5)
    cls.non_writable_dirs = cls.generate_dirs(5, writable=False)
    cls.non_existing_dirs = cls.generate_dirs(5, non_existing=True)

  @classmethod
  def teardown_class(cls):
    for dir_path in cls.normal_dirs:
      shutil.rmtree(dir_path)
    for dir_path in cls.non_writable_dirs:
      shutil.rmtree(dir_path)

  @classmethod
  def generate_dirs(cls, num, writable=True, non_existing=False):
    result = []
    for i in xrange(num):
      dir_path = tempfile.mkdtemp()
      if non_existing:
        shutil.rmtree(dir_path)
      elif not writable:
        os.chmod(dir_path, stat.S_IREAD)
      result.append(dir_path)
    return result

  def setup_method(self, method):
    # We are overriding this method to prevent starting Impala before each test. In this
    # file, each test is responsible for doing that because we want to use class
    # variables like cls.normal_dirs to generate the parameter string to
    # start-impala-cluster, which are generated in setup_class (so using the with_args
    # decorator does not work).
    pass

  def teardown_method(self, method):
    pass

  @pytest.mark.execute_serially
  def test_multiple_dirs(self, vector):
    """ 5 empty directories are created in the /tmp directory and we verify that only
        one of those directories is used as scratch disk. Only one should be used as
        scratch because all directories are on same disk."""
    self._start_impala_cluster([
      '--impalad_args="-scratch_dirs={0}"'.format(','.join(self.normal_dirs))])
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=1)
    exec_option = vector.get_value('exec_option')
    exec_option['mem_limit'] = self.mem_limit
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_expect_success(client, self.spill_query, exec_option)
    assert self.count_nonempty_dirs(self.normal_dirs) == 1

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-scratch_dirs=")
  def test_no_dirs(self, vector):
    """ Test we can execute a query with no scratch dirs """
    self._start_impala_cluster(['--impalad_args="-scratch_dirs="'])
    self.assert_impalad_log_contains("WARNING",
        "Running without spill to disk: no scratch directories provided\.")
    exec_option = vector.get_value('exec_option')
    exec_option['mem_limit'] = self.mem_limit
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    # Expect spill to disk to fail
    self.execute_query_expect_failure(client, self.spill_query, exec_option)
    # Should be able to execute in-memory query
    self.execute_query_expect_success(client, self.in_mem_query, exec_option)

  @pytest.mark.execute_serially
  def test_non_writable_dirs(self, vector):
    """ Test we can execute a query with only bad non-writable scratch """
    self._start_impala_cluster([
      '--impalad_args="-scratch_dirs={0}"'.format(','.join(self.non_writable_dirs))])
    self.assert_impalad_log_contains("ERROR", "Running without spill to disk: could "
        + "not use any scratch directories in list:.*. See previous "
        + "warnings for information on causes.")
    self.assert_impalad_log_contains("WARNING", "Could not remove and recreate directory "
            + ".*: cannot use it for scratch\. Error was: .*", expected_count=5)
    exec_option = vector.get_value('exec_option')
    exec_option['mem_limit'] = self.mem_limit
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    # Expect spill to disk to fail
    self.execute_query_expect_failure(client, self.spill_query, exec_option)
    # Should be able to execute in-memory query
    self.execute_query_expect_success(client, self.in_mem_query, exec_option)
    assert self.count_nonempty_dirs(self.non_writable_dirs) == 0

  @pytest.mark.execute_serially
  def test_non_existing_dirs(self, vector):
    """ Test that non-existing directories are not created or used """
    self._start_impala_cluster([
      '--impalad_args="-scratch_dirs={0}"'.format(','.join(self.non_existing_dirs))])
    self.assert_impalad_log_contains("ERROR", "Running without spill to disk: could "
        + "not use any scratch directories in list:.*. See previous "
        + "warnings for information on causes.")
    self.assert_impalad_log_contains("WARNING", "Cannot use directory .* for scratch: "
        + "Encountered exception while verifying existence of directory path",
        expected_count=5)
    exec_option = vector.get_value('exec_option')
    exec_option['mem_limit'] = self.mem_limit
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    # Expect spill to disk to fail
    self.execute_query_expect_failure(client, self.spill_query, exec_option)
    # Should be able to execute in-memory query
    self.execute_query_expect_success(client, self.in_mem_query, exec_option)
    assert self.count_nonempty_dirs(self.non_existing_dirs) == 0
