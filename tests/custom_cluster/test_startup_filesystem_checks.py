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
# Tests for the behavior of startup_filesystem_check_directories

from __future__ import absolute_import, division, print_function
import logging
import pytest
import os
import tempfile

from impala_py_lib.helpers import find_all_files, is_core_dump
from tests.common.file_utils import assert_file_in_dir_contains
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.filesystem_utils import get_fs_path

LOG = logging.getLogger('test_startup_filesystem_checks')


class TestStartupFilesystemChecks(CustomClusterTestSuite):
  """
  Tests for the behavior of startup_filesystem_check_directories.

  All test cases in this testsuite
  are expected to fail cluster startup and will swallow exceptions thrown during
  setup_method().
  """

  # Use get_fs_path because testdata in Ozone requires a volume prefix and does not
  # accept underscore as a bucket name (the first element after volume prefix).
  NONEXISTENT_PATH = get_fs_path("/nonexistent-path")
  NONDIRECTORY_PATH = \
      get_fs_path("/test-warehouse/alltypes/year=2009/month=1/090101.txt")
  VALID_SUBDIRECTORY = get_fs_path("/test-warehouse")
  # Test multiple valid directories along with an empty entry
  MULTIPLE_VALID_DIRECTORIES = ",".join([
    "/",
    get_fs_path("/test-warehouse/zipcode_incomes"),
    "",
    get_fs_path("/test-warehouse/alltypes")]
  )
  LOG_DIR = tempfile.mkdtemp(prefix="test_startup_filesystem_checks_",
                             dir=os.getenv("LOG_DIR"))
  MINIDUMP_PATH = tempfile.mkdtemp()

  IMPALAD_ARGS = "--startup_filesystem_check_directories={0} --minidump_path={1}"

  pre_test_cores = None

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impala_log_dir=LOG_DIR,
      impalad_args=IMPALAD_ARGS.format(NONEXISTENT_PATH, MINIDUMP_PATH))
  def test_nonexistent_path(self, unique_name):
    # parse log file for expected exception
    assert_file_in_dir_contains(TestStartupFilesystemChecks.LOG_DIR,
        "Invalid path specified for startup_filesystem_check_directories: " +
        "{0} does not exist.".format(TestStartupFilesystemChecks.NONEXISTENT_PATH))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impala_log_dir=LOG_DIR,
      impalad_args=IMPALAD_ARGS.format(NONDIRECTORY_PATH, MINIDUMP_PATH))
  def test_nondirectory_path(self, unique_name):
    # parse log file for expected exception
    assert_file_in_dir_contains(TestStartupFilesystemChecks.LOG_DIR,
        "Invalid path specified for startup_filesystem_check_directories: " +
        "{0} is not a directory.".format(TestStartupFilesystemChecks.NONDIRECTORY_PATH))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impala_log_dir=LOG_DIR,
      impalad_args=IMPALAD_ARGS.format(VALID_SUBDIRECTORY, MINIDUMP_PATH))
  def test_valid_subdirectory(self, unique_name):
    # parse log file for expected log message showing success
    assert_file_in_dir_contains(TestStartupFilesystemChecks.LOG_DIR,
        "Successfully listed {0}".format(TestStartupFilesystemChecks.VALID_SUBDIRECTORY))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impala_log_dir=LOG_DIR,
      impalad_args=IMPALAD_ARGS.format(MULTIPLE_VALID_DIRECTORIES, MINIDUMP_PATH))
  def test_multiple_valid_dirs(self, unique_name):
    valid_directories = TestStartupFilesystemChecks.MULTIPLE_VALID_DIRECTORIES.split(",")
    for valid_dir in valid_directories:
      if len(valid_dir) == 0:
        continue
      # parse log file for expected log message showing success
      assert_file_in_dir_contains(TestStartupFilesystemChecks.LOG_DIR,
        "Successfully listed {0}".format(valid_dir))

  def setup_method(self, method):
    # Make a note of any core files that already exist
    possible_cores = find_all_files('*core*')
    self.pre_test_cores = set([f for f in possible_cores if is_core_dump(f)])

    # Explicitly override CustomClusterTestSuite.setup_method() to
    # allow it to exception, since this testsuite is for cases where
    # startup fails
    try:
      super(TestStartupFilesystemChecks, self).setup_method(method)
    except Exception:
      self._stop_impala_cluster()

  def teardown_method(self, method):
    try:
      # The core dumps expected to be generated by this test should be cleaned up
      possible_cores = find_all_files('*core*')
      post_test_cores = set([f for f in possible_cores if is_core_dump(f)])

      for f in (post_test_cores - self.pre_test_cores):
        LOG.info("Cleaned up {core} created by tests".format(core=f))
        os.remove(f)

      # Explicitly override CustomClusterTestSuite.teardown_method() to
      # allow it to exception, since it relies on setup_method() having
      # completed successfully
      super(TestStartupFilesystemChecks, self).teardown_method(method)
    except Exception:
      self._stop_impala_cluster()
