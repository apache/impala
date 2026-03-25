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
from tests.common.environ import ImpalaTestClusterProperties
from tests.common.skip import SkipIf, IS_OZONE
import pytest


@pytest.mark.execute_serially
@CustomClusterTestSuite.with_args(catalogd_args='--logbuflevel=-1')
@SkipIf.not_dfs
class TestFileMetadataStats(CustomClusterTestSuite):
  """
  Test enhanced file metadata statistics logging in catalogd (IMPALA-13122).
  This test verifies that the catalogd logs detailed file statistics including:
  - Number of files and blocks
  - File size statistics (min/avg/max)
  - Total file size
  - Modification and access times (min/max)
  - Number of unique host:disk pairs (HDFS/Ozone only)
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_file_metadata_stats_partitioned_table(self, unique_database):
    """Test that file metadata statistics are logged when loading a partitioned table."""

    # Create a partitioned table
    tbl_name = "{}.test_file_stats_partitioned".format(unique_database)
    self.execute_query("create table {} (id int, name string) "
                       "partitioned by (year int, month int)".format(tbl_name))

    # Insert some data to create files
    self.execute_query("insert into {} partition(year=2023, month=1) "
                       "values (1, 'test1'), (2, 'test2')".format(tbl_name))
    self.execute_query("insert into {} partition(year=2023, month=2) "
                       "values (3, 'test3'), (4, 'test4')".format(tbl_name))
    self.execute_query("insert into {} partition(year=2024, month=1) "
                       "values (5, 'test5'), (6, 'test6')".format(tbl_name))

    # Force a refresh to trigger file metadata loading and logging
    self.execute_query("refresh {}".format(tbl_name))

    # Verify the log contains expected file metadata statistics
    # The log should contain information about files, blocks, sizes, and times
    log_regex = r"Loaded file and block metadata for.*{}.*Files: \d+".format(
        tbl_name.replace(".", r"\."))
    self.assert_catalogd_log_contains("INFO", log_regex, expected_count=-1, timeout_s=10)

    # Verify file size statistics are logged (min/avg/max pattern)
    size_regex = r"File sizes \(min/avg/max\):"
    self.assert_catalogd_log_contains("INFO", size_regex, expected_count=-1, timeout_s=10)

    # Verify modification times are logged
    modtime_regex = r"Modification times \(min/max\):"
    self.assert_catalogd_log_contains("INFO", modtime_regex, expected_count=-1,
        timeout_s=10)

  def test_file_metadata_stats_unpartitioned_table(self, unique_database):
    """Test that file metadata statistics are logged for unpartitioned tables."""
    # Create an unpartitioned table
    tbl_name = "{}.test_file_stats_unpartitioned".format(unique_database)
    self.execute_query("create table {} (id int, name string, value double)"
                       .format(tbl_name))

    # Insert data to create files
    self.execute_query("insert into {} values "
                       "(1, 'a', 1.1), (2, 'b', 2.2), (3, 'c', 3.3)".format(tbl_name))
    self.execute_query("insert into {} values "
                       "(4, 'd', 4.4), (5, 'e', 5.5)".format(tbl_name))

    # Refresh to trigger metadata loading
    self.execute_query("refresh {}".format(tbl_name))

    # Verify comprehensive statistics are logged
    log_regex = r"Loaded file and block metadata for.*{}.*Files: \d+.*Blocks: \d+".\
        format(tbl_name.replace(".", r"\."))
    self.assert_catalogd_log_contains("INFO", log_regex, expected_count=-1, timeout_s=10)

    # Verify total size is logged
    total_size_regex = r"Total size:.*B"
    self.assert_catalogd_log_contains("INFO", total_size_regex, expected_count=-1,
        timeout_s=10)

  def test_file_metadata_stats_external_table(self):
    """Test file metadata statistics for external tables."""
    # Use existing test data from functional database
    tbl_name = "functional.alltypes"

    # Invalidate metadata to force a fresh load
    self.execute_query("invalidate metadata {}".format(tbl_name))

    # Execute a query to trigger table loading
    self.execute_query("select count(*) from {}".format(tbl_name))

    # Verify detailed file statistics are logged
    # alltypes is partitioned with 24 partitions (2 years * 12 months)
    log_regex = (r"Loaded file and block metadata for.*functional\.alltypes.*"
                 r"Files: 24.*File sizes \(min/avg/max\):")
    self.assert_catalogd_log_contains("INFO", log_regex, expected_count=-1, timeout_s=15)

  def test_file_metadata_stats_host_disk_pairs(self):
    """Test that host and host:disk pair statistics are logged for HDFS tables."""
    # Use a table that has data on HDFS
    tbl_name = "functional.alltypessmall"

    # Invalidate to trigger fresh load
    self.execute_query("invalidate metadata {}".format(tbl_name))
    self.execute_query("select count(*) from {}".format(tbl_name))

    # For HDFS tables, we should see host statistics logged
    hosts_regex = r"Hosts: \d+"
    self.assert_catalogd_log_contains("INFO", hosts_regex, expected_count=-1,
        timeout_s=15)

    # For HDFS tables with disk IDs available, host:disk pair stats are logged
    # With erasure coding or Ozone, disk IDs may not be available, so skip this check
    cluster_properties = ImpalaTestClusterProperties.get_instance()
    if not (cluster_properties.is_erasure_coding_enabled() or IS_OZONE):
      host_disk_regex = r"Host:Disk pairs: \d+"
      self.assert_catalogd_log_contains("INFO", host_disk_regex, expected_count=-1,
          timeout_s=15)
