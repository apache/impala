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
import pytest
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestAutomaticCatalogInvalidation(CustomClusterTestSuite):
  """ Test that tables are cached in the catalogd after usage for the configured time
      and invalidated afterwards."""
  query = "select count(*) from functional.alltypes"
  # The following columns string presents in the catalog object iff the table loaded.
  metadata_cache_string = "columns (list) = list&lt;struct&gt;"
  url = "http://localhost:25020/catalog_object?object_type=TABLE&" \
        "object_name=functional.alltypes"

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def _get_catalog_object(self):
    """ Return the catalog object of functional.alltypes serialized to string. """
    return self.cluster.catalogd.service.read_debug_webpage(
        "catalog_object?object_type=TABLE&object_name=functional.alltypes")

  def _run_test(self, cursor):
    cursor.execute(self.query)
    cursor.fetchall()
    # The table is cached after usage.
    assert self.metadata_cache_string in self._get_catalog_object()
    timeout = time.time() + 20
    while True:
      time.sleep(1)
      # The table is eventually evicted.
      if self.metadata_cache_string not in self._get_catalog_object():
        return
      assert time.time() < timeout

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args="--invalidate_tables_timeout_s=5",
      impalad_args="--invalidate_tables_timeout_s=5")
  def test_v1_catalog(self, cursor):
    self._run_test(cursor)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      catalogd_args="--invalidate_tables_timeout_s=5 --catalog_topic_mode=minimal",
      impalad_args="--invalidate_tables_timeout_s=5 --use_local_catalog")
  def test_local_catalog(self, cursor):
    self._run_test(cursor)
