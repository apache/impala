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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfEC
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.common.test_vector import HS2


class TestStatsExtrapolation(ImpalaTestSuite):
  """Test stats extrapolation and compute stats tablesample. Stats extrapolation is
  enabled via table property and not via the impalad startup flag so these tests can be
  run as regular tests (non-custom-cluster) and in parallel with other tests."""

  @classmethod
  def default_test_protocol(cls):
    return HS2

  @classmethod
  def add_test_dimensions(cls):
    super(TestStatsExtrapolation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @SkipIfEC.contain_full_explain
  def test_stats_extrapolation(self, vector, unique_database):
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['explain_level'] = 2
    self.run_test_case('QueryTest/stats-extrapolation', vector, unique_database)

  def test_compute_stats_tablesample(self, vector, unique_database):
    """COMPUTE STATS TABLESAMPLE is inherently non-deterministic due to its use of
    SAMPLED_NDV() so we test it specially. The goal of this test is to ensure that
    COMPUTE STATS TABLESAMPLE computes in-the-right-ballpark stats and successfully
    stores them in the HMS."""

    # Since our test tables are small, set the minimum sample size to 0 to make sure
    # we exercise the sampling code paths.
    self.client.set_configuration_option('compute_stats_min_sample_size', '0')

    # Test partitioned table.
    part_test_tbl = unique_database + ".alltypes"
    self.clone_table("functional.alltypes", part_test_tbl, True, vector)
    # Clone to use as a baseline. We run the regular COMPUTE STATS on this table.
    part_test_tbl_base = unique_database + ".alltypes_base"
    self.clone_table(part_test_tbl, part_test_tbl_base, True, vector)
    self.client.execute("compute stats {0}".format(part_test_tbl_base))
    # Enable stats extrapolation on both tables to match SHOW output.
    self.__set_extrapolation_tblprop(part_test_tbl)
    self.__set_extrapolation_tblprop(part_test_tbl_base)
    self.__run_sampling_test(part_test_tbl, "", part_test_tbl_base, 1, 3)
    self.__run_sampling_test(part_test_tbl, "", part_test_tbl_base, 10, 7)
    self.__run_sampling_test(part_test_tbl, "", part_test_tbl_base, 20, 13)
    self.__run_sampling_test(part_test_tbl, "", part_test_tbl_base, 100, 99)

    # Test unpartitioned table.
    nopart_test_tbl = unique_database + ".alltypesnopart"
    self.client.execute(
        "create table {0} as select * from functional.alltypes".format(nopart_test_tbl))
    # Clone to use as a baseline. We run the regular COMPUTE STATS on this table.
    nopart_test_tbl_base = unique_database + ".alltypesnopart_base"
    self.clone_table(nopart_test_tbl, nopart_test_tbl_base, False, vector)
    self.client.execute("compute stats {0}".format(nopart_test_tbl_base))
    # Enable stats extrapolation on both tables to match SHOW output.
    self.__set_extrapolation_tblprop(nopart_test_tbl)
    self.__set_extrapolation_tblprop(nopart_test_tbl_base)
    self.__run_sampling_test(nopart_test_tbl, "", nopart_test_tbl_base, 1, 3)
    self.__run_sampling_test(nopart_test_tbl, "", nopart_test_tbl_base, 10, 7)
    self.__run_sampling_test(nopart_test_tbl, "", nopart_test_tbl_base, 20, 13)
    self.__run_sampling_test(nopart_test_tbl, "", nopart_test_tbl_base, 100, 99)

    # Test empty table.
    empty_test_tbl = unique_database + ".empty_tbl"
    self.clone_table("functional.alltypes", empty_test_tbl, False, vector)
    self.__set_extrapolation_tblprop(empty_test_tbl)
    self.__run_sampling_test(empty_test_tbl, "", empty_test_tbl, 10, 7)

    # Test column subset.
    column_subset_tbl = unique_database + ".column_subset"
    columns = "(int_col, string_col)"
    self.clone_table("functional.alltypes", column_subset_tbl, True, vector)
    self.__set_extrapolation_tblprop(column_subset_tbl)
    self.__run_sampling_test(column_subset_tbl, columns, part_test_tbl_base, 1, 3)
    self.__run_sampling_test(column_subset_tbl, columns, part_test_tbl_base, 10, 7)
    self.__run_sampling_test(column_subset_tbl, columns, part_test_tbl_base, 20, 13)
    self.__run_sampling_test(column_subset_tbl, columns, part_test_tbl_base, 100, 99)

    # Test no columns.
    no_column_tbl = unique_database + ".no_columns"
    columns = "()"
    self.clone_table("functional.alltypes", no_column_tbl, True, vector)
    self.__set_extrapolation_tblprop(no_column_tbl)
    self.__run_sampling_test(no_column_tbl, columns, part_test_tbl_base, 10, 7)

    # Test wide table. Should not crash or error. This takes a few minutes so restrict
    # to exhaustive.
    if self.exploration_strategy() == "exhaustive":
      wide_test_tbl = unique_database + ".wide"
      self.clone_table("functional.widetable_1000_cols", wide_test_tbl, False, vector)
      self.__set_extrapolation_tblprop(wide_test_tbl)
      self.client.execute(
        "compute stats {0} tablesample system(10)".format(wide_test_tbl))

  def test_compute_stats_tablesample_iceberg(self, unique_database):
    self.client.set_configuration_option('compute_stats_min_sample_size', '0')

    ice_tbls = ['iceberg_non_partitioned', 'iceberg_partitioned',
                'iceberg_v2_delete_equality_partitioned',
                'iceberg_v2_positional_not_all_data_files_have_delete_files',
                'iceberg_v2_positional_not_all_data_files_have_delete_files_orc']

    # Array of sampling parameters: [(percentage, seed), ...]
    sampling_params = [(1, 3), (10, 7), (20, 13), (100, 99)]

    for tbl in ice_tbls:
      orig_tbl = 'functional_parquet.' + tbl
      cloned_tbl_base = unique_database + '.' + tbl + "_base"
      cloned_tbl_sample = unique_database + '.' + tbl + "_sample"
      self.clone_iceberg_table(orig_tbl, cloned_tbl_base)
      self.clone_iceberg_table(orig_tbl, cloned_tbl_sample)
      self.__set_extrapolation_tblprop(cloned_tbl_sample)
      self.client.execute("compute stats {0}".format(cloned_tbl_base))

      for sampling_param in sampling_params:
        percentage = sampling_param[0]
        seed = sampling_param[1]
        self.__run_sampling_test(
            cloned_tbl_sample, "", cloned_tbl_base, percentage, seed,
            is_precise_table_stats=True)

  def clone_iceberg_table(self, src_tbl, dst_tbl):
    # We are cloning external, non-HiveCatalog tables here.
    self.client.execute("create table {0} like {1}".format(dst_tbl, src_tbl))

  def __set_extrapolation_tblprop(self, tbl):
    """Alters the given table to enable stats extrapolation via tblproperty."""
    self.client.execute("alter table {0} set "
      "tblproperties('impala.enable.stats.extrapolation'='true')".format(tbl))

  def __run_sampling_test(self, tbl, cols, expected_tbl, perc, seed,
                          is_precise_table_stats=False):
    """Drops stats on 'tbl' and then runs COMPUTE STATS TABLESAMPLE on 'tbl' with the
    given column restriction clause, sampling percent and random seed. Checks that
    the resulting table and column stats are reasonably close to those of
    'expected_tbl'. For table formats like ICEBERG the table-level stats are always
    precise, this can be indicated by the parameter 'is_extrapolated_table_stats'"""
    self.client.execute("drop stats {0}".format(tbl))
    self.client.execute(
        "compute stats {0}{1} tablesample system ({2}) repeatable ({3})".format(
            tbl, cols, perc, seed))
    self.__check_table_stats(tbl, expected_tbl, perc, is_precise_table_stats)
    self.__check_column_stats(cols, tbl, perc, expected_tbl)

  def __check_table_stats(self, tbl, expected_tbl, perc, is_precise=False):
    """Checks that the row counts reported in SHOW TABLE STATS on 'tbl' are within 2x
    of those reported for 'expected_tbl'. Assumes that COMPUTE STATS was previously run
    on 'expected_table' and that COMPUTE STATS TABLESAMPLE was run on 'tbl'."""
    actual = self.client.execute("show table stats {0}".format(tbl))
    expected = self.client.execute("show table stats {0}".format(expected_tbl))
    assert len(actual.data) == len(expected.data)
    assert len(actual.column_labels) == len(expected.column_labels)
    col_names = actual.column_labels
    rows_col_idx = col_names.index("#ROWS")
    extrap_rows_label = "#ROWS" if is_precise else "EXTRAP #ROWS"
    extrap_rows_col_idx = col_names.index(extrap_rows_label)
    for i in range(0, len(actual.data)):
      act_cols = actual.data[i].split("\t")
      exp_cols = expected.data[i].split("\t")
      assert int(exp_cols[rows_col_idx]) >= 0
      # The expected_tbl is expected to have valid extrapolated #rows for every partition.
      assert int(act_cols[extrap_rows_col_idx]) >= 0
      diff_perc = 0.0 if is_precise else self.get_diff_perc(perc)
      self.appx_equals(
        int(act_cols[extrap_rows_col_idx]), int(exp_cols[rows_col_idx]), diff_perc)
      # Only the table-level row count is stored. The partition row counts
      # are extrapolated.
      if act_cols[0] == "Total":
        self.appx_equals(
          int(act_cols[rows_col_idx]), int(exp_cols[rows_col_idx]), diff_perc)
      elif len(actual.data) > 1:
        # Partition row count is expected to not be set.
        assert int(act_cols[rows_col_idx]) == -1

  def __check_column_stats(self, cols, tbl, perc, expected_tbl):
    """Checks that the NDVs in SHOW COLUMNS STATS on 'tbl' are within 2x of those
    reported for 'expected_tbl'. Assumes that COMPUTE STATS was previously run
    on 'expected_table' and that COMPUTE STATS TABLESAMPLE was run on 'tbl'."""
    actual = self.client.execute("show column stats {0}".format(tbl))
    expected = self.client.execute("show column stats {0}".format(expected_tbl))
    assert len(actual.data) == len(expected.data)
    assert len(actual.column_labels) == len(expected.column_labels)
    col_names = actual.column_labels
    ndv_col_idx = col_names.index("#DISTINCT VALUES")
    for i in range(0, len(actual.data)):
      act_cols = actual.data[i].split("\t")
      exp_cols = expected.data[i].split("\t")
      assert int(exp_cols[ndv_col_idx]) >= 0
      # Only compare the NDVs for columns which were included in the COMPUTE STATS.
      # The other non partitioning columns are expected to have NDV not set, since the
      # caller drops the stats before calling COMPUTE STATS.
      if cols == "" or act_cols[0] in cols:
        assert int(act_cols[ndv_col_idx]) >= 0
        self.appx_equals(int(act_cols[ndv_col_idx]), int(exp_cols[ndv_col_idx]),
                         self.get_diff_perc(perc))

  def get_diff_perc(self, percentage):
    # Calculates the the parameter 'diff_perc' we give to 'appx_equals()'.
    # 'diff_perc' should be proportional to the sample percentage. We use percentage * 0.9
    # to give enough margin for misestimation.
    assert 0 <= percentage <= 100
    fraction = percentage / 100.0
    return 1.0 - fraction * 0.9
