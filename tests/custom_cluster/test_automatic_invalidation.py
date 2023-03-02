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
import os
import pytest
import time
from subprocess import call
from tests.common.environ import ImpalaTestClusterProperties
from tests.util.filesystem_utils import IS_HDFS, IS_LOCAL


from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestAutomaticCatalogInvalidation(CustomClusterTestSuite):
  """ Test that tables are cached in the catalogd after usage for the configured time
      and invalidated afterwards."""
  query = "select count(*) from functional.alltypes"
  # The following columns string presents in the catalog object iff the table loaded.
  metadata_cache_string = "columns (list) = list&lt;struct&gt;"
  url = "http://localhost:25020/catalog_object?object_type=TABLE&" \
        "object_name=functional.alltypes"

  # The test will run a query and assumes the table is loaded when the query finishes.
  # The timeout should be larger than the time of the query.
  timeout = 20 if ImpalaTestClusterProperties.get_instance().runs_slowly() or\
               (not IS_HDFS and not IS_LOCAL) else 10
  timeout_flag = "--invalidate_tables_timeout_s=" + str(timeout)

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
    # Wait 5 * table TTL for the invalidation to take effect.
    max_wait_time = time.time() + self.timeout * 5
    while True:
      time.sleep(1)
      # The table is eventually evicted.
      if self.metadata_cache_string not in self._get_catalog_object():
        return
      assert time.time() < max_wait_time

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args=timeout_flag, impalad_args=timeout_flag)
  def test_v1_catalog(self, cursor):
    self._run_test(cursor)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      catalogd_args=timeout_flag + " --catalog_topic_mode=minimal",
      impalad_args=timeout_flag + " --use_local_catalog")
  def test_local_catalog(self, cursor):
    self._run_test(cursor)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args="--invalidate_tables_timeout_s=1",
                                    impalad_args="--invalidate_tables_timeout_s=1")
  def test_invalid_table(self):
    """ Regression test for IMPALA-7606. Tables failed to be loaded don't have a
        last used time and shouldn't be considered for invalidation."""
    self.execute_query_expect_failure(self.client, "select * from functional.bad_serde")
    # The table expires after 1 second. Sleeping for another logbufsecs=5 seconds to wait
    # for the log to be flushed. Wait 4 more seconds to reduce flakiness.
    time.sleep(10)
    assert "Unexpected exception thrown while attempting to automatically invalidate "\
        "tables" not in open(os.path.join(self.impala_log_dir, "catalogd.INFO")).read()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--invalidate_tables_on_memory_pressure "
                  "--invalidate_tables_gc_old_gen_full_threshold=0 "
                  "--invalidate_tables_fraction_on_memory_pressure=1",
    impalad_args="--invalidate_tables_on_memory_pressure")
  def test_memory_pressure(self):
    """ Test that memory-based invalidation kicks out all the tables after an GC."""
    self.execute_query(self.query)
    # This triggers a full GC as of openjdk 1.8.
    call(["jmap", "-histo:live", str(self.cluster.catalogd.get_pid())])
    # Sleep for logbufsecs=5 seconds to wait for the log to be flushed. Wait 5 more
    # seconds to reduce flakiness.
    time.sleep(10)
    assert self.metadata_cache_string not in self._get_catalog_object()
