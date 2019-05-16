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

import os
import time
import pytest
import tempfile
import logging

from getpass import getuser
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import assert_file_in_dir_contains

LOG = logging.getLogger(__name__)


class TestHooks(CustomClusterTestSuite):
  """
  Tests for FE QueryEventHook invocations.

  All test cases in this test suite share an impala log dir, so keep that in mind
  if parsing any logs during your test assertions.
  """
  DUMMY_HOOK = "org.apache.impala.testutil.DummyQueryEventHook"
  HOOK_POSTEXEC_FILE = "/tmp/{0}.onQueryComplete".format(DUMMY_HOOK)
  HOOK_START_FILE = "/tmp/{0}.onImpalaStartup".format(DUMMY_HOOK)
  MINIDUMP_PATH = tempfile.mkdtemp()
  IMPALA_LOG_DIR = tempfile.mkdtemp(prefix="test_hooks_", dir=os.getenv("LOG_DIR"))

  def teardown(self):
    try:
      os.remove(TestHooks.HOOK_START_FILE)
    except OSError:
      pass

  def setup_method(self, method):
    # Explicitly override CustomClusterTestSuite.setup_method() to
    # clean up the dummy hook's onQueryComplete file
    try:
      os.remove(TestHooks.HOOK_START_FILE)
      os.remove(TestHooks.HOOK_POSTEXEC_FILE)
    except OSError:
      pass
    super(TestHooks, self).setup_method(method)

  def teardown_method(self, method):
    # Explicitly override CustomClusterTestSuite.teardown_method() to
    # clean up the dummy hook's onQueryComplete file
    super(TestHooks, self).teardown_method(method)
    try:
      os.remove(TestHooks.HOOK_START_FILE)
      os.remove(TestHooks.HOOK_POSTEXEC_FILE)
    except OSError:
      pass

  @pytest.mark.xfail(run=False, reason="IMPALA-8589")
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impala_log_dir=IMPALA_LOG_DIR,
      impalad_args="--query_event_hook_classes={0} "
                   "--minidump_path={1}"
                   .format(DUMMY_HOOK, MINIDUMP_PATH),
      catalogd_args="--minidump_path={0}".format(MINIDUMP_PATH))
  def test_query_event_hooks_execute(self, unique_database):
    """
    Tests that the post query execution hook actually executes by using a
    dummy hook implementation that writes a file and testing for existence
    of that file.

    This test will perform queries needed to induce a hook event but should
    clean up the db before completion.
    """
    user = getuser()
    impala_client = self.create_impala_client()

    # create closure over variables so we don't have to repeat them
    def query(sql):
      return impala_client.execute(sql, user=user)

    # hook should be invoked at daemon startup
    assert self.__wait_for_file(TestHooks.HOOK_START_FILE, timeout_s=10)

    query("create table {0}.recipes (recipe_id int, recipe_name string)"
          .format(unique_database))
    query("insert into {0}.recipes (recipe_id, recipe_name) values "
          "(1,'Tacos'), (2,'Tomato Soup'), (3,'Grilled Cheese')".format(unique_database))

    # TODO (IMPALA-8572): need to end the session to trigger
    # query unregister and therefore hook invocation.  can possibly remove
    # after IMPALA-8572 completes
    impala_client.close()

    # dummy hook should create a file
    assert self.__wait_for_file(TestHooks.HOOK_POSTEXEC_FILE, timeout_s=10)

  def __wait_for_file(self, filepath, timeout_s=10):
    if timeout_s < 0: return False
    LOG.info("Waiting {0} s for file {1}".format(timeout_s, filepath))
    for i in range(0, timeout_s):
      LOG.info("Poll #{0} for file {1}".format(i, filepath))
      if os.path.isfile(filepath):
        LOG.info("Found file {0}".format(filepath))
        return True
      else:
        time.sleep(1)
    LOG.info("File {0} not found".format(filepath))
    return False


class TestHooksStartupFail(CustomClusterTestSuite):
  """
  Tests for failed startup due to bad QueryEventHook startup or registration

  All test cases in this testsuite@pytest.mark.xfail(run=False, reason="IMPALA-8589")
  are expected to fail cluster startup and will swallow exceptions thrown during
  setup_method().
  """

  FAILING_HOOK = "org.apache.impala.testutil.AlwaysErrorQueryEventHook"
  NONEXIST_HOOK = "captain.hook"
  LOG_DIR1 = tempfile.mkdtemp(prefix="test_hooks_startup_fail_", dir=os.getenv("LOG_DIR"))
  LOG_DIR2 = tempfile.mkdtemp(prefix="test_hooks_startup_fail_", dir=os.getenv("LOG_DIR"))
  MINIDUMP_PATH = tempfile.mkdtemp()

  @pytest.mark.xfail(run=False, reason="IMPALA-8589")
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impala_log_dir=LOG_DIR1,
      impalad_args="--query_event_hook_classes={0} "
                   "--minidump_path={1}"
                   .format(FAILING_HOOK, MINIDUMP_PATH),
      catalogd_args="--minidump_path={0}".format(MINIDUMP_PATH))
  def test_hook_startup_fail(self):
    """
    Tests that exception during QueryEventHook.onImpalaStart will prevent
    successful daemon startup.

    This is done by parsing the impala log file for a specific message
    so is kind of brittle and maybe even prone to flakiness, depending
    on how the log file is flushed.
    """
    # parse log file for expected exception
    assert_file_in_dir_contains(TestHooksStartupFail.LOG_DIR1,
                                "Exception during onImpalaStartup from "
                                "QueryEventHook class {0}"
                                .format(TestHooksStartupFail.FAILING_HOOK))

  @pytest.mark.xfail(run=False, reason="IMPALA-8589")
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impala_log_dir=LOG_DIR2,
      impalad_args="--query_event_hook_classes={0} "
                   "--minidump_path={1}"
                   .format(NONEXIST_HOOK, MINIDUMP_PATH),
      catalogd_args="--minidump_path={0}".format(MINIDUMP_PATH))
  def test_hook_instantiation_fail(self):
    """
    Tests that failure to instantiate a QueryEventHook will prevent
    successful daemon startup.

    This is done by parsing the impala log file for a specific message
    so is kind of brittle and maybe even prone to flakiness, depending
    on how the log file is flushed.
    """
    # parse log file for expected exception
    assert_file_in_dir_contains(TestHooksStartupFail.LOG_DIR2,
                                "Unable to instantiate query event hook class {0}"
                                .format(TestHooksStartupFail.NONEXIST_HOOK))

  def setup_method(self, method):
    # Explicitly override CustomClusterTestSuite.setup_method() to
    # allow it to exception, since this test suite is for cases where
    # startup fails
    try:
      super(TestHooksStartupFail, self).setup_method(method)
    except Exception:
      self._stop_impala_cluster()

  def teardown_method(self, method):
    # Explicitly override CustomClusterTestSuite.teardown_method() to
    # allow it to exception, since it relies on setup_method() having
    # completed successfully
    try:
      super(TestHooksStartupFail, self).teardown_method(method)
    except Exception:
      self._stop_impala_cluster()
