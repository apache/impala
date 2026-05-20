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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import create_iceberg_table_from_directory


class TestIcebergWithPuffinStatsStartupFlag(CustomClusterTestSuite):
  """Tests for checking the behaviour of the startup flag
  'enable_reading_puffin_stats'."""

  @CustomClusterTestSuite.with_args(
      catalogd_args='--enable_reading_puffin_stats=false')
  @pytest.mark.execute_serially
  def test_disable_reading_puffin(self, unique_database):
    self._read_ndv_stats_expect_result(unique_database, [-1, -1])

  def _read_ndv_stats_expect_result(self, unique_database, expected_ndv_stats):
    tbl_name = "iceberg_with_puffin_stats"
    create_iceberg_table_from_directory(self.client, unique_database, tbl_name, "parquet")

    full_tbl_name = "{}.{}".format(unique_database, tbl_name)
    show_col_stats_stmt = "show column stats {}".format(full_tbl_name)
    query_result = self.execute_query(show_col_stats_stmt)

    rows = query_result.get_data().split("\n")
    ndvs = [int(row.split()[2]) for row in rows]
    assert ndvs == expected_ndv_stats
