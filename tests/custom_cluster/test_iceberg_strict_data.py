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

SELECT_STATEMENT = "SELECT COUNT(1) FROM " \
  "functional_parquet.iceberg_multiple_storage_locations"
EXCEPTION = "IcebergTableLoadingException: " \
  "Error loading metadata for Iceberg table"


class TestIcebergStrictDataFileLocation(CustomClusterTestSuite):
  """Tests for checking the behaviour of startup flag
   'iceberg_restrict_data_file_location'."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(
      catalogd_args='--iceberg_restrict_data_file_location=true')
  @pytest.mark.execute_serially
  def test_restricted_location(self, vector):
    """If the flag is enabled, tables with multiple storage locations will fail
    to load their datafiles."""
    result = self.execute_query_expect_failure(self.client, SELECT_STATEMENT)
    assert EXCEPTION in str(result)

  @CustomClusterTestSuite.with_args(
      catalogd_args='--iceberg_restrict_data_file_location=false')
  @pytest.mark.execute_serially
  def test_disabled(self, vector):
    """If the flag is disabled, and tables with multiple storage locations
    are configured properly, the tables load successfully."""
    result = self.execute_query_expect_success(self.client, SELECT_STATEMENT)
    assert '9' in result.data
