#!/usr/bin/env impala-python
#
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

import re


class TestSharedCatalogd(CustomClusterTestSuite):
  """Test sharing catalogd across Impala clusters"""

  def setup_method(self, method):
    super(TestSharedCatalogd, self).setup_method(method)
    self.coordinator = self.cluster.impalads[0]

  @CustomClusterTestSuite.with_args(impalad_args="-cluster_membership_topic_id=cluster1")
  def test_disjiont_clusters(self, unique_database):
    """Tests that two Impala clusters can share catalogd and statestore."""
    # Start a new cluster of 3 impalads using a new cluster id.
    self._start_impala_cluster(["--impalad_args=-cluster_membership_topic_id=cluster2"],
                               cluster_size=3,
                               num_coordinators=3,
                               add_impalads=True,
                               expected_num_impalads=3,
                               expected_subscribers=7)
    cluster2 = self.cluster.impalads[3:]
    assert len(cluster2) == 3
    cluster2_client = self.create_client_for_nth_impalad(3)
    result = cluster2_client.execute("select count(*) from functional.alltypes",
                                     fetch_profile_after_close=True)
    # Verify the query runs in the new cluster
    match = re.search(r'Per Host Number of Fragment Instances: (.*)\n',
                      result.runtime_profile)
    assert match is not None
    # Examples:
    # nodes = ['quanlong-OptiPlex-BJ:27005(1)', 'quanlong-OptiPlex-BJ:27004(1)',
    #          'quanlong-OptiPlex-BJ:27003(2)']
    # port_fis = ['27005(1)', '27004(1)', '27003(2)']
    nodes = match.group(1).split()
    assert len(nodes) == 3
    port_fis = [n.split(':')[1] for n in nodes]
    assert "27003(2)" in port_fis
    assert "27004(1)" in port_fis
    assert "27005(1)" in port_fis

    # Create a table in cluster1 with sync_ddl=true. The table should be visible
    # in cluster2 immediately.
    self.execute_query("create table {}.tbl(i int)".format(unique_database),
                       query_options={"sync_ddl": True})
    result = cluster2_client.execute("describe {}.tbl".format(unique_database))
    assert result.data == ['i\tint\t']
