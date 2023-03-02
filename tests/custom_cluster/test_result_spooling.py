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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.util.web_pages_util import (
    get_num_completed_backends,
    get_mem_admitted_backends_debug_page)


class TestDedicatedCoordinator(CustomClusterTestSuite):
  """A custom cluster test that tests result spooling against a cluster with a dedicated
  coordinator."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDedicatedCoordinator, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # There's no reason to test this on other file formats/compression codecs right now
    cls.ImpalaTestMatrix.add_dimension(
      create_uncompressed_text_dimension(cls.get_workload()))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=2, num_exclusive_coordinators=1)
  def test_dedicated_coordinator(self, vector):
    """Test the following when result spooling is enabled on a cluster with a dedicated
    coordinator when all results are spooled: (1) all backends are shutdown besides the
    coordinator and (2) all non-coordinator memory is released."""
    num_rows = 2000
    query = "select id from functional_parquet.alltypes order by id limit {0}".format(
        num_rows)
    vector.get_value('exec_option')['spool_query_results'] = 'true'

    # Amount of time to wait for the query to reach the finished state before throwing a
    # Timeout exception.
    timeout = 10

    handle = self.execute_query_async(query, vector.get_value('exec_option'))
    try:
      # Wait for the query to finish (all rows are spooled). Assert that the executor
      # has been shutdown and its memory has been released.
      self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], timeout)
      self.assert_eventually(timeout, 0.5,
          lambda: re.search("RowsSent:.*({0})".format(num_rows),
          self.client.get_runtime_profile(handle)))
      assert "NumCompletedBackends: 1 (1)" in self.client.get_runtime_profile(handle)
      mem_admitted = get_mem_admitted_backends_debug_page(self.cluster)
      assert mem_admitted['executor'][0] == 0
      assert mem_admitted['coordinator'] > 0
      assert get_num_completed_backends(self.cluster.impalads[0].service,
               handle.get_handle().id) == 1

      # Fetch all results from the query and assert that the coordinator and the executor
      # have been shutdown and their memory has been released.
      self.client.fetch(query, handle)
      assert "NumCompletedBackends: 2 (2)" in self.client.get_runtime_profile(handle)
      mem_admitted = get_mem_admitted_backends_debug_page(self.cluster)
      assert mem_admitted['executor'][0] == 0
      assert mem_admitted['coordinator'] == 0
      assert get_num_completed_backends(self.cluster.impalads[0].service,
               handle.get_handle().id) == 2
    finally:
      self.client.close_query(handle)
