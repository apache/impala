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

from subprocess import check_call

import glob
import os
import shutil
import sys
import tempfile

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import create_iceberg_table_from_directory


class TestIcebergWithPuffinStatsStartupFlag(CustomClusterTestSuite):
  """Tests for checking the behaviour of the startup flag
  'disable_reading_puffin_stats'."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(
      catalogd_args='--disable_reading_puffin_stats=true')
  @pytest.mark.execute_serially
  def test_disable_reading_puffin(self, unique_database):
    self._read_ndv_stats_expect_result(unique_database, [-1, -1])

  @CustomClusterTestSuite.with_args(
      catalogd_args='--disable_reading_puffin_stats=false')
  @pytest.mark.execute_serially
  def test_enable_reading_puffin(self, unique_database):
    self._read_ndv_stats_expect_result(unique_database, [2, 2])

  def _read_ndv_stats_expect_result(self, unique_database, expected_ndv_stats):
    tbl_name = "iceberg_with_puffin_stats"
    create_iceberg_table_from_directory(self.client, unique_database, tbl_name, "parquet")

    full_tbl_name = "{}.{}".format(unique_database, tbl_name)
    show_col_stats_stmt = "show column stats {}".format(full_tbl_name)
    query_result = self.execute_query(show_col_stats_stmt)

    rows = query_result.get_data().split("\n")
    ndvs = [int(row.split()[2]) for row in rows]
    assert ndvs == expected_ndv_stats


class TestIcebergTableWithPuffinStats(CustomClusterTestSuite):
  """Tests related to Puffin statistics files for Iceberg tables."""

  CREATE_TBL_STMT_TEMPLATE = """CREATE TABLE {} (
        int_col1 INT,
        int_col2 INT,
        bigint_col BIGINT,
        float_col FLOAT,
        double_col DOUBLE,
        decimal_col DECIMAL,
        date_col DATE,
        string_col STRING,
        timestamp_col TIMESTAMP,
        bool_col BOOLEAN) STORED BY ICEBERG"""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestIcebergTableWithPuffinStats, cls).add_test_dimensions()

  @CustomClusterTestSuite.with_args(
      catalogd_args='--disable_reading_puffin_stats=false')
  def test_puffin_stats(self, vector, unique_database):
    """Tests that Puffin stats are correctly read. The stats we use in this test do not
    necessarily reflect the actual state of the table."""
    tbl_name = unique_database + ".ice_puffin_tbl"
    create_tbl_stmt = self.CREATE_TBL_STMT_TEMPLATE.format(tbl_name)
    self.execute_query(create_tbl_stmt)

    # Set stats in HMS so we can check that we fall back to that if we don't have Puffin
    # stats.
    set_stats_stmt = \
        "alter table {} set column stats timestamp_col ('numDVs'='2000')".format(tbl_name)
    self.execute_query(set_stats_stmt)

    tbl_loc = self._get_table_location(tbl_name, vector)

    tbl_properties = self._get_properties("Table Parameters:", tbl_name)
    uuid = tbl_properties["uuid"]
    metadata_json_path = tbl_properties["metadata_location"]

    self._copy_files_to_puffin_tbl(tbl_name, tbl_loc, uuid)

    self._check_all_stats_in_1_file(tbl_name, tbl_loc, metadata_json_path)
    self._check_all_stats_in_2_files(tbl_name, tbl_loc, metadata_json_path)
    self._check_duplicate_stats_in_1_file(tbl_name, tbl_loc, metadata_json_path)
    self._check_duplicate_stats_in_2_files(tbl_name, tbl_loc, metadata_json_path)
    self._check_one_file_current_one_not(tbl_name, tbl_loc, metadata_json_path)
    self._check_not_all_blobs_current(tbl_name, tbl_loc, metadata_json_path)
    self._check_missing_file(tbl_name, tbl_loc, metadata_json_path)
    self._check_one_file_corrupt_one_not(tbl_name, tbl_loc, metadata_json_path)
    self._check_all_files_corrupt(tbl_name, tbl_loc, metadata_json_path)
    self._check_file_contains_invalid_field_id(tbl_name, tbl_loc, metadata_json_path)
    self._check_stats_for_unsupported_type(tbl_name, tbl_loc, metadata_json_path)
    self._check_invalid_and_corrupt_sketches(tbl_name, tbl_loc, metadata_json_path)
    self._check_metadata_ndv_ok_file_corrupt(tbl_name, tbl_loc, metadata_json_path)
    self._check_multiple_field_ids_for_blob(tbl_name, tbl_loc, metadata_json_path)

    # Disable reading Puffin stats with a table property.
    disable_puffin_reading_tbl_prop_stmt = "alter table {} set tblproperties( \
        'impala.iceberg_disable_reading_puffin_stats'='true')".format(tbl_name)
    self.execute_query(disable_puffin_reading_tbl_prop_stmt)
    # Refresh 'metadata_json_path'.
    tbl_properties = self._get_properties("Table Parameters:", tbl_name)
    metadata_json_path = tbl_properties["metadata_location"]

    self._check_reading_puffin_stats_disabled_by_tbl_prop(
        tbl_name, tbl_loc, metadata_json_path)

  def _copy_files_to_puffin_tbl(self, tbl_name, tbl_loc, uuid):
    version_info = sys.version_info
    if version_info.major >= 3 and version_info.minor >= 2:
      with tempfile.TemporaryDirectory() as tmpdir:
        self._copy_files_to_puffin_tbl_impl(tbl_name, tbl_loc, uuid, tmpdir)
    else:
      try:
        tmpdir = tempfile.mkdtemp()
        self._copy_files_to_puffin_tbl_impl(tbl_name, tbl_loc, uuid, tmpdir)
      finally:
        shutil.rmtree(tmpdir)

  def _copy_files_to_puffin_tbl_impl(self, tbl_name, tbl_loc, uuid, tmpdir):
    metadata_dir = os.path.join(os.getenv("IMPALA_HOME"), "testdata/ice_puffin")
    tbl_loc_placeholder = "TABLE_LOCATION_PLACEHOLDER"
    uuid_placeholder = "UUID_PLACEHOLDER"

    tmp_metadata_dir = tmpdir + "/dir"
    tmp_generated_metadata_dir = os.path.join(tmp_metadata_dir, "generated")
    shutil.copytree(metadata_dir, tmp_metadata_dir)

    sed_location_pattern = "s|{}|{}|g".format(tbl_loc_placeholder, tbl_loc)
    sed_uuid_pattern = "s/{}/{}/g".format(uuid_placeholder, uuid)
    metadata_json_files = glob.glob(tmp_generated_metadata_dir + "/*metadata.json")
    for metadata_json in metadata_json_files:
      check_call(["sed", "-i", "-e", sed_location_pattern,
                  "-e", sed_uuid_pattern, metadata_json])

    # Move all files from the 'generated' subdirectory to the parent directory so that
    # all files end up in the same directory on HDFS.
    mv_cmd = "mv {generated_dir}/* {parent_dir} && rmdir {generated_dir}".format(
        generated_dir=tmp_generated_metadata_dir, parent_dir=tmp_metadata_dir)
    check_call(["bash", "-c", mv_cmd])

    # Copy the files to HDFS.
    self.filesystem_client.copy_from_local(glob.glob(tmp_metadata_dir + "/*"),
        tbl_loc + "/metadata")

  def _check_all_stats_in_1_file(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "all_stats_in_1_file.metadata.json", [1, 2, 3, 4, 5, 6, 7, 8, 9, -1])

  def _check_all_stats_in_2_files(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "stats_divided.metadata.json", [1, 2, 3, 4, 5, 6, 7, 8, 9, -1])

  def _check_duplicate_stats_in_1_file(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "duplicate_stats_in_1_file.metadata.json",
        [1, 2, -1, -1, -1, -1, -1, -1, 2000, -1])

  def _check_duplicate_stats_in_2_files(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "duplicate_stats_in_2_files.metadata.json",
        [1, 2, 3, -1, -1, -1, -1, -1, 2000, -1])

  def _check_one_file_current_one_not(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "one_file_current_one_not.metadata.json",
        [1, 2, -1, -1, -1, -1, -1, -1, 2000, -1])

  def _check_not_all_blobs_current(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "not_all_blobs_current.metadata.json",
        [1, 2, -1, 4, -1, -1, -1, -1, 2000, -1])

  def _check_missing_file(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "missing_file.metadata.json", [-1, -1, 3, 4, -1, -1, -1, -1, 2000, -1])

  def _check_one_file_corrupt_one_not(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "one_file_corrupt_one_not.metadata.json",
        [-1, -1, 3, 4, -1, -1, -1, -1, 2000, -1])

  def _check_all_files_corrupt(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "all_files_corrupt.metadata.json", [-1, -1, -1, -1, -1, -1, -1, -1, 2000, -1])

  def _check_file_contains_invalid_field_id(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "file_contains_invalid_field_id.metadata.json",
        [1, -1, -1, -1, -1, -1, -1, -1, 2000, -1])

  def _check_stats_for_unsupported_type(self, tbl_name, tbl_loc,
          metadata_json_path):
    # Ndv stats are not supported for BOOLEAN in HMS, so we don't take it into account
    # even if it is present in a Puffin file.
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "stats_for_unsupported_type.metadata.json",
        [2, -1, -1, -1, -1, -1, -1, -1, 2000, -1])

  def _check_invalid_and_corrupt_sketches(self, tbl_name, tbl_loc,
          metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "invalidAndCorruptSketches.metadata.json",
        [1, -1, 3, -1, 5, -1, -1, -1, 2000, -1])

  def _check_metadata_ndv_ok_file_corrupt(self, tbl_name, tbl_loc, metadata_json_path):
    # The Puffin file is corrupt but it shouldn't cause an error since we don't actually
    # read it because we read the NDV value from the metadata.json file.
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "metadata_ndv_ok_stats_file_corrupt.metadata.json",
        [1, 2, -1, -1, -1, -1, -1, -1, 2000, -1])

  def _check_multiple_field_ids_for_blob(self, tbl_name, tbl_loc, metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "multiple_field_ids.metadata.json",
        [-1, -1, -1, -1, -1, -1, -1, -1, 2000, -1])

  def _check_reading_puffin_stats_disabled_by_tbl_prop(self, tbl_name, tbl_loc,
          metadata_json_path):
    self._check_scenario(tbl_name, tbl_loc, metadata_json_path,
        "all_stats_in_1_file.metadata.json", [-1, -1, -1, -1, -1, -1, -1, -1, 2000, -1])

  def _check_scenario(self, tbl_name, tbl_loc, current_metadata_json_path,
      new_metadata_json_name, expected_ndvs):
    self._change_metadata_json_file(tbl_name, tbl_loc, current_metadata_json_path,
        new_metadata_json_name)

    invalidate_metadata_stmt = "invalidate metadata {}".format(tbl_name)
    self.execute_query(invalidate_metadata_stmt)
    show_col_stats_stmt = "show column stats {}".format(tbl_name)
    res = self.execute_query(show_col_stats_stmt)

    ndvs = self._get_ndvs_from_query_result(res)
    assert expected_ndvs == ndvs

  def _change_metadata_json_file(self, tbl_name, tbl_loc, current_metadata_json_path,
      new_metadata_json_name):
    # Overwrite the current metadata.json file with the given file.
    new_metadata_json_path = os.path.join(tbl_loc, "metadata", new_metadata_json_name)
    self.filesystem_client.copy(new_metadata_json_path, current_metadata_json_path, True)

  def _get_ndvs_from_query_result(self, query_result):
    rows = query_result.get_data().split("\n")
    return [int(row.split()[2]) for row in rows]
