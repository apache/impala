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
import logging
import time
import pytest
import os

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


LOG = logging.getLogger(__name__)
class TestLoggingCore(CustomClusterTestSuite):
  """Test existence of certain log lines under some scenario."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def _test_max_errors(self, max_error_logs_per_instance, max_errors, expect_downgraded):
    """Test that number of non-fatal error printed to INFO log is limited by
    max_errors and max_error_logs_per_instance."""

    query = ("select id, bool_col, tinyint_col, smallint_col "
        "from functional.alltypeserror order by id")
    client = self.create_impala_client()

    self.execute_query_expect_success(client, query, {'max_errors': max_errors})
    self.assert_impalad_log_contains("INFO", "Error parsing row",
        max_error_logs_per_instance if expect_downgraded else 8)
    self.assert_impalad_log_contains("INFO",
        "printed {0} non-fatal error to log level 1".format(max_error_logs_per_instance),
        1 if expect_downgraded else 0)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1,
      impalad_args="--max_error_logs_per_instance=2",
      disable_log_buffering=True)
  def test_max_errors(self):
    self._test_max_errors(2, 4, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1,
      impalad_args="--max_error_logs_per_instance=3",
      disable_log_buffering=True)
  def test_max_errors_0(self):
    self._test_max_errors(3, 0, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1,
      impalad_args="--max_error_logs_per_instance=2",
      disable_log_buffering=True)
  def test_max_errors_no_downgrade(self):
    self._test_max_errors(2, -1, False)


class TestLogFlushPermissionDenied(CustomClusterTestSuite):
    """Test logging of failures to open log files with cause Permission denied."""
    LOG_FLUSH_FAILURES_DIR = "log_flush_failures_dir"

    @classmethod
    def get_workload(cls):
        return 'functional-query'

    def setup_method(self, method):
        # Override parent
        super(TestLogFlushPermissionDenied, self).setup_method(method)
        tmp_dir = self.get_tmp_dir(self.LOG_FLUSH_FAILURES_DIR)
        self.orig_permissions = os.stat(tmp_dir).st_mode
        os.chmod(tmp_dir, 0)

    def teardown_method(self, method):
        # Override parent
        os.chmod(self.get_tmp_dir(self.LOG_FLUSH_FAILURES_DIR), self.orig_permissions)
        super(TestLogFlushPermissionDenied, self).teardown_method(method)

    def __test_permission_denied(self, log_dir):
        self.assert_impalad_log_contains("INFO",
          r"Could not open log file: {0}.*, cause: Permission denied".format(log_dir), 2)

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--lineage_event_log_dir={" + LOG_FLUSH_FAILURES_DIR + "}",
        tmp_dir_placeholders=[LOG_FLUSH_FAILURES_DIR])
    def test_lineage_log_failure(self):
      self.__test_permission_denied(self.get_tmp_dir(self.LOG_FLUSH_FAILURES_DIR))

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
            impalad_args="--audit_event_log_dir={" + LOG_FLUSH_FAILURES_DIR + "}",
            tmp_dir_placeholders=[LOG_FLUSH_FAILURES_DIR])
    def test_audit_log_failure(self):
      self.__test_permission_denied(self.get_tmp_dir(self.LOG_FLUSH_FAILURES_DIR))

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
      impalad_args="--profile_log_dir={" + LOG_FLUSH_FAILURES_DIR + "}",
      tmp_dir_placeholders=[LOG_FLUSH_FAILURES_DIR])
    def test_profiles_failure(self):
      time.sleep(5)
      self.__test_permission_denied(self.get_tmp_dir(self.LOG_FLUSH_FAILURES_DIR))
