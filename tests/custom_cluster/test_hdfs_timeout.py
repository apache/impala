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
import re
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfNotHdfsMinicluster
from subprocess import check_call
from tests.util.filesystem_utils import IS_OZONE
from tests.util.shell_util import exec_process


@SkipIfNotHdfsMinicluster.tuned_for_minicluster
class TestHdfsTimeouts(CustomClusterTestSuite):
  """Test to verify that HDFS operations time out correctly."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--hdfs_operation_timeout_sec=5 --max_cached_file_handles=0")
  def test_hdfs_open_timeout(self, vector):
    """This verifies that hdfsOpenFile times out appropriately. It tests this by
       halting the NameNode, running a query that needs to do hdfsOpenFile,
       and verifying that it times out and throws an error."""

    # Find the NameNode's pid via pgrep. This would raise an error if it did not
    # find a pid, so there is at least one match.
    data_api_name = 'OzoneManager' if IS_OZONE else 'namenode.NameNode'
    rc, pgrep_output, stderr = exec_process("pgrep -f {}".format(data_api_name))
    assert rc == 0, \
        "Error finding NameNode pid\nstdout={0}\nstderr={1}".format(pgrep_output, stderr)
    # In our test environment, this should only match one pid
    assert(pgrep_output.count("\n") == 1)
    namenode_pid = pgrep_output.strip()

    # Run a query successfully. This fetches metadata from the NameNode,
    # and since this will be cached, a subsequent run will not ask the NameNode
    # for metadata. This means a subsequent execution will only talk to the NameNode
    # for file open.
    self.execute_query_expect_success(self.client,
        "select count(*) from functional.alltypes", vector=vector)

    # Stop the NameNode and execute the query again. Since the file handle cache is off,
    # the query will do hdfsOpenFile calls and talk to the NameNode. Since the NameNode
    # is stopped, those calls will hang, testing the timeout functionality.
    ex = None
    result = None
    try:
      # Stop the NameNode
      check_call(["kill", "-STOP", namenode_pid])
      start_time = time.time()
      result = self.execute_query("select count(*) from functional.alltypes",
          vector=vector)
      end_time = time.time()
    except Exception as e:
      ex = e
    finally:
      end_time = time.time()
      # Always resume the NameNode
      check_call(["kill", "-CONT", namenode_pid])

    # The query should have failed, which raises an exception
    if ex is None:
      assert False, "Query should have failed, but instead returned {0}".format(result)

    # The exception should contain the appropriate error message
    error_pattern = "hdfsOpenFile\(\) for.*at backend.*"
    "failed to finish before the 5 second timeout"
    assert len(re.findall(error_pattern, str(ex))) > 0

    # The timeout is 5 seconds and seems to be enforced within about 20 seconds, so it
    # would be unusual if this query does not finish in 60 seconds.
    assert (end_time - start_time) < 60.0

    # Execute the query a final time to verify that the system recovers.
    self.execute_query_expect_success(self.client,
        "select count(*) from functional.alltypes", vector=vector)
