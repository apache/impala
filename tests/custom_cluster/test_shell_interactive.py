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

from multiprocessing.pool import ThreadPool
from random import randint

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_vector import ImpalaTestVector
from tests.common.test_dimensions import create_client_protocol_dimension
from tests.shell.util import (get_shell_cmd, get_impalad_port, spawn_shell,
                              wait_for_query_state)


class TestShellInteractive(CustomClusterTestSuite):

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-default_pool_max_requests 1")
  def test_admission_status(self):
    """Test whether the admission status gets printed if a query gets queued when
    either live_summary or live_progress is set to true"""
    expected_admission_status = "Query queued. Latest queuing reason: " \
                                "number of running queries 1 is at or over limit 1"
    # Start a long running query so that the next one gets queued.
    sleep_query_handle = self.client.execute_async("select sleep(10000)")
    self.client.wait_for_admission_control(sleep_query_handle)

    # Iterate over test vector within test function to avoid restarting cluster.
    for vector in\
        [ImpalaTestVector([value]) for value in create_client_protocol_dimension()]:
      proc = spawn_shell(get_shell_cmd(vector))
      proc.sendline("set enable_trivial_query_for_admission=false;")
      # Check with only live_summary set to true.
      proc.expect("{0}] default>".format(get_impalad_port(vector)))
      proc.sendline("set live_summary=true;")
      proc.sendline("select 1;")
      proc.expect(expected_admission_status)
      proc.sendcontrol('c')
      proc.expect("Cancelling Query")
      # Check with only live_progress set to true.
      proc.sendline("set live_summary=false;")
      proc.sendline("set live_progress=true;")
      proc.sendline("select 1;")
      proc.expect(expected_admission_status)

  _test_retry_query =\
        "select count(*) from functional.alltypes where bool_col = sleep(50)"
  _query_retry_options = "set retry_failed_queries=true;"

  @pytest.mark.execute_serially
  def test_query_retries_profile_and_summary_cmd(self):
    """Tests transparent query retries via impala-shell. Validates the output of the
    'profile [all | latest | original];' commands in impala-shell."""
    query = "select count(*) from functional.alltypes where bool_col = sleep(50)"
    vector = ImpalaTestVector([ImpalaTestVector.Value("protocol", "hs2")])
    proc = self.__trigger_retry_shell(vector, query)

    # Expect the correct results
    proc.expect("3650", timeout=300)

    # Check the output of 'profile all'
    proc.sendline("profile all;")
    proc.expect("Query Runtime Profile:")
    proc.expect("Query State: FINISHED")
    proc.expect("Failed Query Runtime Profile\(s\):")
    proc.expect("Query State: EXCEPTION")
    proc.expect("Retry Status: RETRIED")

    # Check the output of 'profile latest' and 'profile'. The output of both cmds
    # should be equivalent.
    for profile_cmd in ["profile latest;", "profile;"]:
      proc.sendline(profile_cmd)
      proc.expect("Query Runtime Profile:")
      proc.expect("Query State: FINISHED")
      # Validate that the output does not contain info about the failed profile.
      self.__proc_not_expect(proc, "Failed Query Runtime Profile\(s\):")
      self.__proc_not_expect(proc, "Query State: EXCEPTION")
      self.__proc_not_expect(proc, "Retry Status: RETRIED")

    # Check the output of 'profile original'
    proc.sendline("profile original;")
    proc.expect("Query Runtime Profile:")
    proc.expect("Query State: EXCEPTION")
    proc.expect("Retry Status: RETRIED")
    self.__proc_not_expect(proc, "Failed Query Runtime Profile\(s\):")
    self.__proc_not_expect(proc, "Query State: FINISHED")

    # Check the output of 'summary all'
    proc.sendline("summary all;")
    proc.expect("Query Summary:")
    # The retried query runs on 2 instances.
    proc.expect("00:SCAN HDFS\w*| 2\w*| 2")
    proc.expect("Failed Query Summary:")
    # The original query runs on 3 instances.
    proc.expect("00:SCAN HDFS\w*| 3\w*| 3")

    # Check the output of 'summary latest' and 'summary'. The output of both cmds
    # should be equivalent.
    for summary_cmd in ["summary latest;", "summary;"]:
      proc.sendline(summary_cmd)
      # The retried query runs on 2 instances.
      proc.expect("00:SCAN HDFS\w*| 2\w*| 2")

    # Check the output of 'summary original'
    proc.sendline("summary original")
    # The original query runs on 3 instances.
    proc.expect("00:SCAN HDFS\w*| 3\w*| 3")

  @pytest.mark.execute_serially
  def test_query_retries_show_profiles(self):
    """Tests transparent query retries via impala-shell. Validates that the output of the
    impala-shell when the '-p' option is specified prints out both the original and
    retried runtime profiles."""
    query = "select count(*) from functional.alltypes where bool_col = sleep(50)"
    vector = ImpalaTestVector([ImpalaTestVector.Value("protocol", "hs2")])
    proc = self.__trigger_retry_shell(vector, query, shell_params=['-p'])

    proc.expect("3650", timeout=300)
    proc.expect("Query Runtime Profile:")
    proc.expect("Query State: FINISHED")

  def __proc_not_expect(self, proc, pattern):
    """Helper method for pexpect.except to assert that a pattern is not present."""
    proc.expect("^((?!{0}).)*$".format(pattern))

  def __trigger_retry_shell(self, vector, query, shell_params=[]):
    """Runs a query via the impala-shell and triggers a query retry."""
    vector = ImpalaTestVector([ImpalaTestVector.Value("protocol", "hs2")])
    pool = ThreadPool(processes=1)
    proc = spawn_shell(get_shell_cmd(vector) + shell_params)
    proc.expect("{0}] default>".format(get_impalad_port(vector)))
    proc.sendline("set retry_failed_queries=true;")
    pool.apply_async(lambda: proc.sendline(query + ";"))
    wait_for_query_state(vector, query, "RUNNING")
    self.cluster.impalads[
        randint(1, ImpalaTestSuite.get_impalad_cluster_size() - 1)].kill()
    return proc
