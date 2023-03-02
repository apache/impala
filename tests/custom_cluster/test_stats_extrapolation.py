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
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)

class TestStatsExtrapolation(CustomClusterTestSuite):
  """Minimal end-to-end test for the --enable_stats_extrapolation impalad flag. This test
  primarly checks that the flag is propagated to the FE. More testing is done in FE unit
  tests and metadata/test_stats_extrapolation.py."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestStatsExtrapolation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--enable_stats_extrapolation=true")
  def test_stats_extrapolation(self, vector, unique_database):
    # Test row count extrapolation
    self.client.execute("set explain_level=2")
    explain_result = self.client.execute("explain select * from functional.alltypes")
    assert "extrapolated-rows=7.30K" in " ".join(explain_result.data)
    # Test COMPUTE STATS TABLESAMPLE
    part_test_tbl = unique_database + ".alltypes"
    self.clone_table("functional.alltypes", part_test_tbl, True, vector)
    # Since our test tables are small, set the minimum sample size to 0 to make sure
    # we exercise the sampling code paths.
    self.client.execute("set COMPUTE_STATS_MIN_SAMPLE_SIZE=0")
    self.client.execute(
        "compute stats {0} tablesample system (13)".format(part_test_tbl))
    # Check that table stats were set.
    table_stats = self.client.execute("show table stats {0}".format(part_test_tbl))
    col_names = [fs.name.upper() for fs in table_stats.schema.fieldSchemas]
    extrap_rows_idx = col_names.index("EXTRAP #ROWS")
    for row in table_stats.data:
      assert int(row.split("\t")[extrap_rows_idx]) >= 0
    # Check that column stats were set.
    col_stats = self.client.execute("show column stats {0}".format(part_test_tbl))
    col_names = [fs.name.upper() for fs in col_stats.schema.fieldSchemas]
    ndv_col_idx = col_names.index("#DISTINCT VALUES")
    for row in col_stats.data:
      assert int(row.split("\t")[ndv_col_idx]) >= 0
