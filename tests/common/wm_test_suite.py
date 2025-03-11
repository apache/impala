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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class WorkloadManagementTestSuite(CustomClusterTestSuite):
  """Base class for tests that exercise workload management behavior.
  The setup_method waits for workload management init completion, while the
  teardown_method waits until impala-server.completed-queries.queued metric
  reaches 0."""

  def setup_method(self, method):
    super(WorkloadManagementTestSuite, self).setup_method(method)
    self.wait_for_wm_init_complete()

  def teardown_method(self, method):
    self.wait_for_wm_idle()
    super(WorkloadManagementTestSuite, self).teardown_method(method)

  def get_client(self, protocol):
    """Retrieves the default Impala client for the specified protocol. This client is
       automatically closed after the test completes."""
    return self.default_impala_client(protocol=protocol)
