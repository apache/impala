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
from builtins import range
import pytest
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestIncrementalMetadataUpdate(CustomClusterTestSuite):
  """ Validates incremental metadata updates across catalogd and coordinators."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestIncrementalMetadataUpdate, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_update_frequency_ms=5000")
  def test_long_statestore_update_frequency(self, unique_database):
    """Test multiple inserts on the same table executed by different impalads. Increase
    statestore_update_frequency_ms so inserts can finished in the same time window of
    a catalog topic update. Different impalads will have different versions of the table
    returned by DML responses. They should be able to correctly apply the incremental
    partition updates from catalogd."""
    self.execute_query(
        "create table %s.multi_inserts_tbl (id int) partitioned by (p int)"
        % unique_database)
    # Refresh the table with sync_ddl=true so table metadata is loaded in all impalads.
    self.execute_query("refresh %s.multi_inserts_tbl" % unique_database,
                       query_options={"sync_ddl": True})
    # Run 3 inserts using different impalads.
    for i in range(3):
      impalad_client = self.create_client_for_nth_impalad(i)
      # Set SYNC_DDL for the last query, so metadata should in sync along all impalads
      # at the end.
      impalad_client.set_configuration_option("sync_ddl", i == 2)
      impalad_client.execute(
          "insert into {0}.multi_inserts_tbl partition(p) values ({1}, {1})"
          .format(unique_database, i))

    # Verify each impalad has the latest table metadata.
    for i in range(3):
      impalad_client = self.create_client_for_nth_impalad(i)
      res = impalad_client.execute(
          "show partitions %s.multi_inserts_tbl" % unique_database)
      assert len(res.data) == 4, "missing partitions"
