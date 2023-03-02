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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestDisableCatalogDataOps(CustomClusterTestSuite):
  """Test Catalog behavior when --disable_catalog_data_ops_debug_only is set."""

  # TODO(vercegovac): make the test more precise by starting from an empty database
  # and adding specifically one java-udf and one avro table.
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      catalogd_args="--logbuflevel=-1 --disable_catalog_data_ops_debug_only=true")
  def test_disable_catalog_data_ops(self):
    # Expects that all Java UDF loading messages are for skip and that none of them load.
    self.assert_catalogd_log_contains(
      'INFO', "Skip loading Java functions: catalog data ops disabled.",
      expected_count=-1)
    self.assert_catalogd_log_contains(
      'INFO', "Loading Java functions for database:", expected_count=0)

    # Indirectly issues a load to a single avro table. Expects to skip the schema load.
    self.client.execute("select count(*) from functional_avro.tinytable")
    self.assert_catalogd_log_contains('INFO',
      "Avro schema, .*://.*.json, not loaded from fs: catalog data ops disabled.")
