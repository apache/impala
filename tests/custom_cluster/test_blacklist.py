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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

import pytest
import re

from tests.common.skip import SkipIfNotHdfsMinicluster
from time import sleep


# Tests that verify the behavior of the executor blacklist.
@SkipIfNotHdfsMinicluster.tuned_for_minicluster
class TestBlacklist(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestBlacklist, cls).setup_class()

  @pytest.mark.execute_serially
  def test_kill_impalad(self, cursor):
    """Test that verifies that when an impalad is killed, it is properly blacklisted."""
    # Run a query and verify that no impalads are blacklisted yet.
    result = self.execute_query("select count(*) from tpch.lineitem")
    assert re.search("Blacklisted Executors: (.*)", result.runtime_profile) is None, \
        result.runtime_profile

    # Kill an impalad
    killed_impalad = self.cluster.impalads[2]
    killed_impalad.kill()

    # Run a query which should fail as the impalad hasn't been blacklisted yet.
    try:
      self.execute_query("select count(*) from tpch.lineitem")
      assert False, "Query was expected to fail"
    except Exception as e:
      assert "Exec() rpc failed" in str(e)

    # Run another query which should succeed and verify the impalad was blacklisted.
    result = self.execute_query("select count(*) from tpch.lineitem")
    match = re.search("Blacklisted Executors: (.*)", result.runtime_profile)
    assert match.group(1) == "%s:%s" % \
        (killed_impalad.hostname, killed_impalad.service.be_port), result.runtime_profile

    # Sleep for long enough for the statestore to remove the impalad from the cluster
    # membership, i.e. Statestore::FailedExecutorDetectionTime() + some padding
    sleep(12)
    # Run another query and verify nothing was blacklisted and only 2 backends were
    # scheduled on.
    result = self.execute_query("select count(*) from tpch.lineitem")
    assert re.search("Blacklisted Executors: (.*)", result.runtime_profile) is None, \
        result.runtime_profile
    assert re.search("NumBackends: 2", result.runtime_profile), result.runtime_profile

  @pytest.mark.execute_serially
  def test_restart_impalad(self, cursor):
    """Test that verifies the behavior when an impalad is killed, blacklisted, and then
    restarted."""
    # Run a query and verify that no impalads are blacklisted yet.
    result = self.execute_query("select count(*) from tpch.lineitem")
    assert re.search("Blacklisted Executors: (.*)", result.runtime_profile) is None, \
        result.runtime_profile

    # Kill an impalad
    killed_impalad = self.cluster.impalads[2]
    killed_impalad.kill()

    # Run a query which should fail as the impalad hasn't been blacklisted yet.
    try:
      self.execute_query("select count(*) from tpch.lineitem")
      assert False, "Query was expected to fail"
    except Exception as e:
      assert "Exec() rpc failed" in str(e)

    # Run another query which should succeed and verify the impalad was blacklisted.
    result = self.execute_query("select count(*) from tpch.lineitem")
    match = re.search("Blacklisted Executors: (.*)", result.runtime_profile)
    assert match.group(1) == "%s:%s" % \
        (killed_impalad.hostname, killed_impalad.service.be_port), result.runtime_profile

    # Restart the impalad.
    killed_impalad.start()

    # Sleep for long enough for the statestore to update the membership to include the
    # restarted impalad, ImpaladProcess.start() won't return until the Impalad says its
    # ready to accept connections, at which point it will have already registered with the
    # statestore, so we don't need to sleep very long.
    sleep(2)

    # Run another query and verify nothing was blacklisted and all 3 backends were
    # scheduled on.
    result = self.execute_query("select count(*) from tpch.lineitem")
    assert re.search("Blacklisted Executors: (.*)", result.runtime_profile) is None, \
        result.runtime_profile
    assert re.search("NumBackends: 3", result.runtime_profile), result.runtime_profile
