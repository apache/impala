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
# Client tests for Query Event Hooks

from __future__ import absolute_import, division, print_function
import os
import pytest
import tempfile

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestHooks(CustomClusterTestSuite):
  """
  Tests for FE QueryEventHook invocations.
  """
  DUMMY_HOOK = "org.apache.impala.testutil.DummyQueryEventHook"
  MINIDUMP_PATH = tempfile.mkdtemp()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impala_log_dir=tempfile.mkdtemp(prefix="test_hooks_", dir=os.getenv("LOG_DIR")),
      impalad_args="--query_event_hook_classes={0} "
                   "--minidump_path={1} -logbuflevel=-1"
                   .format(DUMMY_HOOK, MINIDUMP_PATH),
      catalogd_args="--minidump_path={0}".format(MINIDUMP_PATH))
  def test_query_event_hooks_execute(self, unique_database):
    """
    Tests that the post query execution hook actually executes by using a
    dummy hook implementation.
    """
    # Dummy hook should log something (See org.apache.impala.testutil.DummyQueryEventHook)
    self.assert_impalad_log_contains("INFO",
        "{0}.onImpalaStartup".format(self.DUMMY_HOOK), expected_count=-1)
    # Run a test query that triggers a lineage event.
    self.execute_query_expect_success(
        self.client, "select count(*) from functional.alltypes")
    # onQueryComplete() is invoked by the lineage logger.
    self.assert_impalad_log_contains("INFO",
        "{0}.onQueryComplete".format(self.DUMMY_HOOK), expected_count=-1)


class TestHooksStartupFail(CustomClusterTestSuite):
  """
  Tests for failed startup due to bad QueryEventHook startup or registration

  All test cases in this testsuite are expected to fail cluster startup and will
  swallow exceptions thrown during setup_method().
  """
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestHooksStartupFail, cls).setup_class()

  FAILING_HOOK = "org.apache.impala.testutil.AlwaysErrorQueryEventHook"
  NONEXIST_HOOK = "captain.hook"
  LOG_DIR1 = tempfile.mkdtemp(prefix="test_hooks_startup_fail_", dir=os.getenv("LOG_DIR"))
  LOG_DIR2 = tempfile.mkdtemp(prefix="test_hooks_startup_fail_", dir=os.getenv("LOG_DIR"))
  MINIDUMP_PATH = tempfile.mkdtemp()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      expect_cores=True,
      cluster_size=1,
      impalad_timeout_s=5,
      impala_log_dir=LOG_DIR1,
      impalad_args="--query_event_hook_classes={0} "
                   "--minidump_path={1}"
                   .format(FAILING_HOOK, MINIDUMP_PATH),
      catalogd_args="--minidump_path={0}".format(MINIDUMP_PATH))
  def test_hook_startup_fail(self):
    """
    Tests that exception during QueryEventHook.onImpalaStart will prevent
    successful daemon startup.
    """
    # Parse log file for expected exception.
    self.assert_impalad_log_contains("INFO",
                                     "Exception during onImpalaStartup from "
                                     "QueryEventHook class {0}"
                                     .format(self.FAILING_HOOK), expected_count=-1)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      expect_cores=True,
      cluster_size=1,
      impalad_timeout_s=5,
      impala_log_dir=LOG_DIR2,
      impalad_args="--query_event_hook_classes={0} "
                   "--minidump_path={1}"
                   .format(NONEXIST_HOOK, MINIDUMP_PATH),
      catalogd_args="--minidump_path={0}".format(MINIDUMP_PATH))
  def test_hook_instantiation_fail(self):
    """
    Tests that failure to instantiate a QueryEventHook will prevent
    successful daemon startup.
    """
    # Parse log file for expected exception.
    self.assert_impalad_log_contains("INFO",
                                     "Unable to instantiate query event hook class {0}"
                                     .format(self.NONEXIST_HOOK), expected_count=-1)
