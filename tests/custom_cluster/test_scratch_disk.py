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
import re
import shutil
import stat
import tempfile

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.verifiers.metric_verifier import MetricVerifier
from tests.common.skip import SkipIf

class TestScratchDir(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestScratchDir, cls).setup_class()

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
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=false',
      '--impalad_args=--disk_spill_compression_codec=zstd',
      '--impalad_args=--disk_spill_punch_holes=true'])
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
    result = self.execute_query_expect_success(client, self.in_mem_query, exec_option)
    # IMPALA-10565: Since scratch_dirs is empty, we expect planner to disable result
    # spooling.
    query_options_by_planner = ".*set by configuration and planner" \
                               ".*SPOOL_QUERY_RESULTS=0"
    assert re.search(query_options_by_planner, result.runtime_profile)

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
    # IMPALA-9856: Disable query result spooling so that in_mem_query does not spill to
    # disk.
    exec_option['spool_query_results'] = '0'
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
    # IMPALA-9856: Disable query result spooling so that in_mem_query does not spill to
    # disk.
    exec_option['spool_query_results'] = '0'
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
    dirs = self.generate_dirs(3)
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=true',
      '--impalad_args=--disk_spill_compression_codec=zstd',
      '--impalad_args=--disk_spill_punch_holes=true'])
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

  @pytest.mark.execute_serially
  def test_scratch_dirs_default_priority(self, vector):
    """ 5 empty directories are created in the /tmp directory and we verify that all
        of those directories are used as scratch disk. By default, all directories
        should have the same priority and so all should be used in a round robin
        manner."""
    normal_dirs = self.generate_dirs(5)
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(normal_dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=true'], cluster_size=1,
      expected_num_impalads=1)
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=len(normal_dirs))
    vector.get_value('exec_option')['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    verifier = MetricVerifier(impalad.service)
    verifier.wait_for_metric("impala-server.num-fragments-in-flight", 2)
    metrics0 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-0')
    metrics1 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-1')
    metrics2 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-2')
    metrics3 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-3')
    metrics4 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-4')
    assert (metrics0 > 0 and metrics1 > 0 and metrics2 > 0 and metrics3 > 0
            and metrics4 > 0)
    results = client.fetch(self.spill_query, handle)
    assert results.success
    client.close_query(handle)
    client.close()

  @pytest.mark.execute_serially
  def test_scratch_dirs_prioritized_spill(self, vector):
    """ 5 empty directories are created in the /tmp directory and we verify that only
        the directories with highest priority are used as scratch disk."""
    normal_dirs = self.generate_dirs(5)
    normal_dirs[0] = '{0}::{1}'.format(normal_dirs[0], 1)
    normal_dirs[1] = '{0}::{1}'.format(normal_dirs[1], 0)
    normal_dirs[2] = '{0}::{1}'.format(normal_dirs[2], 1)
    normal_dirs[3] = '{0}::{1}'.format(normal_dirs[3], 0)
    normal_dirs[4] = '{0}::{1}'.format(normal_dirs[4], 1)
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(normal_dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=true'], cluster_size=1,
      expected_num_impalads=1)
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=len(normal_dirs))
    vector.get_value('exec_option')['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    verifier = MetricVerifier(impalad.service)
    verifier.wait_for_metric("impala-server.num-fragments-in-flight", 2)
    metrics0 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-0')
    metrics1 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-1')
    metrics2 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-2')
    metrics3 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-3')
    metrics4 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-4')
    # dir1 and dir3 have highest priority and will be used as scratch disk.
    assert (metrics1 > 0 and metrics3 > 0 and metrics0 == 0 and metrics2 == 0
            and metrics4 == 0)
    results = client.fetch(self.spill_query, handle)
    assert results.success
    client.close_query(handle)
    client.close()

  @pytest.mark.execute_serially
  @SkipIf.not_hdfs
  def test_scratch_dirs_remote_spill(self, vector):
    # Test one remote directory with one its local buffer directory.
    normal_dirs = self.generate_dirs(1)
    # Use local hdfs for testing. Could be changed to S3.
    normal_dirs.append('hdfs://localhost:20500/tmp')
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(normal_dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=true'],
      cluster_size=1,
      expected_num_impalads=1)
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=len(normal_dirs) - 1)
    vector.get_value('exec_option')['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    verifier = MetricVerifier(impalad.service)
    verifier.wait_for_metric("impala-server.num-fragments-in-flight", 2)
    metrics0 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-0')
    # Dir0 is the remote directory.
    assert (metrics0 > 0)
    results = client.fetch(self.spill_query, handle)
    assert results.success
    client.close_query(handle)
    client.close()

  @pytest.mark.execute_serially
  @SkipIf.not_hdfs
  def test_scratch_dirs_mix_local_and_remote_dir_spill_local_only(self, vector):
    '''Two local directories, the first one is always used as local buffer for
       remote directories. Set the second directory big enough so that only spills
       to local in the test'''
    normal_dirs = self.generate_dirs(2)
    normal_dirs[0] = '{0}::{1}'.format(normal_dirs[0], 1)
    normal_dirs[1] = '{0}:2GB:{1}'.format(normal_dirs[1], 0)
    normal_dirs.append('hdfs://localhost:20500/tmp')
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(normal_dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=true'],
      cluster_size=1,
      expected_num_impalads=1)
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=len(normal_dirs) - 1)
    vector.get_value('exec_option')['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    verifier = MetricVerifier(impalad.service)
    verifier.wait_for_metric("impala-server.num-fragments-in-flight", 2)
    metrics0 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-0')
    metrics1 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-1')
    # Local directory always ranks before the remote one, so dir0 is the local directory.
    # Only spill to dir0 because it has enough space for the spilling.
    assert (metrics0 > 0 and metrics1 == 0)
    results = client.fetch(self.spill_query, handle)
    assert results.success
    client.close_query(handle)
    client.close()

  @pytest.mark.execute_serially
  @SkipIf.not_hdfs
  def test_scratch_dirs_mix_local_and_remote_dir_spill_both(self, vector):
    '''Two local directories, the first one is always used as local buffer for
       remote directories. Set the second directory small enough so that it spills
       to both directories in the test'''
    normal_dirs = self.generate_dirs(2)
    normal_dirs[0] = '{0}:32MB:{1}'.format(normal_dirs[0], 0)
    normal_dirs[1] = '{0}:4MB:{1}'.format(normal_dirs[1], 1)
    normal_dirs.append('hdfs://localhost:20500/tmp')
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(normal_dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=true'],
      cluster_size=1,
      expected_num_impalads=1)
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=len(normal_dirs) - 1)
    vector.get_value('exec_option')['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    verifier = MetricVerifier(impalad.service)
    verifier.wait_for_metric("impala-server.num-fragments-in-flight", 2)
    metrics0 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-0')
    metrics1 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-1')
    # Local directory always ranks before the remote one, so dir0 is the local directory.
    # The query spills to both dir0 and dir1. By default the remote file is 16MB each,
    # so the value of metrics1 should be at least one file size.
    assert (metrics0 == 4 * 1024 * 1024 and metrics1 % (16 * 1024 * 1024) == 0)
    results = client.fetch(self.spill_query, handle)
    assert results.success
    client.close_query(handle)
    client.close()

  @pytest.mark.execute_serially
  @SkipIf.not_hdfs
  def test_scratch_dirs_remote_spill_with_options(self, vector):
    # One local buffer directory and one remote directory.
    normal_dirs = self.generate_dirs(1)
    normal_dirs[0] = '{0}:16MB:{1}'.format(normal_dirs[0], 0)
    normal_dirs.append('hdfs://localhost:20500/tmp')
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(normal_dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=true',
      '--impalad_args=--remote_tmp_file_size=8M',
      '--impalad_args=--remote_tmp_file_block_size=1m'],
      cluster_size=1, expected_num_impalads=1)
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=len(normal_dirs) - 1)
    vector.get_value('exec_option')['buffer_pool_limit'] = self.buffer_pool_limit
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    verifier = MetricVerifier(impalad.service)
    verifier.wait_for_metric("impala-server.num-fragments-in-flight", 2)
    metrics0 = self.get_metric('tmp-file-mgr.scratch-space-bytes-used.dir-0')
    # The query spills to the remote directories and creates remote files,
    # so that the size is bigger than 0, and be integer times of remote file size.
    assert (metrics0 > 0 and metrics0 % (8 * 1024 * 1024) == 0)
    results = client.fetch(self.spill_query, handle)
    assert results.success
    client.close_query(handle)
    client.close()

  @pytest.mark.execute_serially
  @SkipIf.not_hdfs
  def test_scratch_dirs_remote_spill_concurrent(self, vector):
    '''Concurrently execute multiple queries that trigger the spilling to the remote
    directory to test if there is a deadlock issue.'''
    normal_dirs = self.generate_dirs(1)
    normal_dirs[0] = '{0}:16MB:{1}'.format(normal_dirs[0], 0)
    normal_dirs.append('hdfs://localhost:20500/tmp')
    num = 5
    self._start_impala_cluster([
      '--impalad_args=-logbuflevel=-1 -scratch_dirs={0}'.format(','.join(normal_dirs)),
      '--impalad_args=--allow_multiple_scratch_dirs_per_device=true',
      '--impalad_args=--remote_tmp_file_size=8M',
      '--impalad_args=--remote_tmp_file_block_size=1m'],
      cluster_size=num, num_coordinators=num, expected_num_impalads=num)
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
                                    expected_count=len(normal_dirs) - 1)
    vector.get_value('exec_option')['buffer_pool_limit'] = self.buffer_pool_limit
    impalad_name = 'impalad'
    client_name = 'client'
    handle_name = 'handle'
    for i in range(num):
        impalad = self.cluster.impalads[i - 1]
        locals()[client_name + str(i)] = impalad.service.create_beeswax_client()

    for i in range(num):
        client = locals()[client_name + str(i)]
        locals()[handle_name + str(i)] = self.execute_query_async_using_client(client,
                self.spill_query, vector)

    total_size = 0
    for i in range(num):
        impalad = self.cluster.impalads[i - 1]
        client = locals()[client_name + str(i)]
        handle = locals()[handle_name + str(i)]
        results = client.fetch(self.spill_query, handle)
        assert results.success
        metrics = impalad.service.get_metric_value(
            'tmp-file-mgr.scratch-space-bytes-used-high-water-mark')
        total_size = total_size + metrics
        client.close_query(handle)
        client.close()

    # assert that we did use the scratch space and should be integer times of the
    # remote file size.
    assert (total_size > 0 and total_size % (8 * 1024 * 1024) == 0)
