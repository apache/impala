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

def non_existing_dirs(num):
  dirs = ["/tmp/nonexisting_%d" % i for i in range(num)]
  for dir_name in dirs:
    if os.path.exists(dir_name):
      shutil.rmtree(dir_name)
    assert not os.path.exists(dir_name)
  return dirs

def non_writable_dirs(num):
  dirs = ["/tmp/nonwritable_%d" % i for i in range(num)]
  for dir_name in dirs:
    if os.path.exists(dir_name):
      shutil.rmtree(dir_name)
    os.mkdir(dir_name)
    os.chmod(dir_name, stat.S_IREAD)
  return dirs

def normal_dirs(num):
  dirs = ["/tmp/existing_%d" % i for i in range(num)]
  for dir_name in dirs:
    if os.path.exists(dir_name):
      shutil.rmtree(dir_name)
    os.mkdir(dir_name)
  return dirs

class TestScratchDir(CustomClusterTestSuite):

  CustomClusterTestSuite.dirs = []
  query =  "select o_orderdate, o_custkey, o_comment from tpch.orders order by o_orderdate"

  def count_nonempty_dirs(self):
    count = 0
    for dir_name in CustomClusterTestSuite.dirs:
      if os.path.exists(dir_name) and len(os.listdir(dir_name)) > 0:
        count += 1
    return count

  def get_dirs(dirs):
    CustomClusterTestSuite.dirs = dirs
    return ','.join(CustomClusterTestSuite.dirs)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args('-scratch_dirs=%s' % get_dirs(normal_dirs(5)))
  def test_multiple_dirs(self, vector):
    """ 5 empty directories are created in the /tmp directory and we verify that only
        one of those directories is used as scratch disk """
    exec_option = vector.get_value('exec_option')
    exec_option['mem_limit'] = "200m"
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_expect_success(client, self.query, exec_option)
    assert self.count_nonempty_dirs() == 1

""" In these tests we pass in non-existing or non-writable directories. However, this
    causes the test to hang (which is expected behavior) because impala fails to start.
    One way out of this is to start a timer and check that Impala did not start after x
    seconds. Ishaan thinks this is a bad idea.

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args('-scratch_dirs=%s' %
      get_dirs(normal_dirs(5) + non_existing_dirs(1)))
  def test_non_existing(self, vector):
    pytest.xfail("non existing scratch directory")
    exec_option = vector.get_value('exec_option')
    exec_option['mem_limit'] = "300m"
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_expect_success(client, self.query, exec_option)
    assert self.count_nonempty_dirs() == 1

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args('-scratch_dirs=%s' %
      get_dirs(normal_dirs(5) + non_writable_dirs(1)))
  def test_non_writable_dirs(self, vector):
    pytest.xfail("scratch directory is non writable")
    exec_option = vector.get_value('exec_option')
    exec_option['mem_limit'] = "300m"
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_expect_success(client, self.query, exec_option)
    assert self.count_nonempty_dirs() == 1
"""
