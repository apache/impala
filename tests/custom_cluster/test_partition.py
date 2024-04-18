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
import logging
import pytest
import shutil
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import (
  create_single_exec_option_dimension,
  create_uncompressed_text_dimension)

LOG = logging.getLogger(__name__)


class TestPartition(CustomClusterTestSuite):
  """Tests to validate partitioning"""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestPartition, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # There's no reason to test this on other file formats/compression codecs right now
    cls.ImpalaTestMatrix.add_dimension(
      create_uncompressed_text_dimension(cls.get_workload()))

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal")
  def test_partition_location_in_local_catalog_mode(self, vector, unique_database):
    try:
      self.run_test_case('QueryTest/partition-location', vector,
          use_db=unique_database)
    finally:
      # Delete generated data files in local file system.
      local_file_dir = '/tmp/%s.db/' % unique_database
      try:
        shutil.rmtree(local_file_dir)
      except OSError as e:
        LOG.info("Cannot remove directory %s, %s " % (local_file_dir, e.strerror))

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full")
  def test_partition_location_in_legacy_mode(self, vector, unique_database):
    try:
      self.run_test_case('QueryTest/partition-location', vector,
          use_db=unique_database)
    finally:
      # Delete generated data files in local file system.
      local_file_dir = '/tmp/%s.db/' % unique_database
      try:
        shutil.rmtree(local_file_dir)
      except OSError as e:
        LOG.info("Cannot remove directory %s, %s " % (local_file_dir, e.strerror))


class TestPartitionDeletion(CustomClusterTestSuite):
  """Tests catalogd sends deletion updates (i.e. isDeleted=true) for dropped partitions.
     Use a normal catalog update frequency (2s) instead of the default one in custom
     cluster tests (50ms) so the race conditions of IMPALA-13009 could happen."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestPartitionDeletion, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    # It doesn't matter what the file format is. So just test on text/none.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'text'
         and v.get_value('table_format').compression_codec == 'none'))

  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_update_frequency_ms=2000",
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full --hms_event_polling_interval_s=0")
  def test_legacy_catalog_no_event_processing(self, unique_database):
    self._test_partition_deletion(unique_database)

  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_update_frequency_ms=2000",
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full --hms_event_polling_interval_s=1")
  def test_legacy_catalog_with_event_processing(self, unique_database):
    self._test_partition_deletion(unique_database)

  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_update_frequency_ms=2000",
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0")
  def test_local_catalog_no_event_processing(self, unique_database):
    self._test_partition_deletion(unique_database)

  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_update_frequency_ms=2000",
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=1")
  def test_local_catalog_with_event_processing(self, unique_database):
    self._test_partition_deletion(unique_database)

  def _test_partition_deletion(self, unique_database):
    tbl = unique_database + ".part_tbl"
    self.client.execute("create table {}(i int) partitioned by (p int)".format(tbl))
    self.client.execute("alter table {} add partition(p=0)".format(tbl))

    #############################################################
    # Test 1: DropPartition + Invalidate + Load
    #
    # Add and drop different partitions
    for i in range(1, 4):
      self.client.execute("alter table {} add partition(p={})".format(tbl, i))
      # Wait 1s so catalogd has chance to propagate new partitions before we drop them.
      time.sleep(1)
      # If the following 3 statements are executed in a catalog topic update cycle, it
      # covers the bug of IMPALA-13009.
      self.client.execute("alter table {} drop partition(p>0)".format(tbl))
      self.client.execute("invalidate metadata {}".format(tbl))
      # Trigger metadata loading on this table
      self.client.execute("describe {}".format(tbl))

    res = self.client.execute("show partitions {}".format(tbl))
    assert self.has_value("p=0", res.data)
    # The last line is the total summary
    assert len(res.data) == 2

    # Check catalogd has sent deletions for dropped partitions if their updates have been
    # sent before.
    update_log_regex = "Collected . partition update.*HDFS_PARTITION:{}:.*p={}"
    deletion_log_regex = "Collected . partition deletion.*HDFS_PARTITION:{}:.*p={}"
    for i in range(1, 4):
      update_found = self.assert_catalogd_log_contains("INFO",
          update_log_regex.format(tbl, i), dry_run=True)
      if update_found:
        self.assert_catalogd_log_contains("INFO", deletion_log_regex.format(tbl, i))

    # Restart impalad and check the partitions on it
    self.cluster.impalads[0].restart()
    self.client = self.create_impala_client()
    new_res = self.client.execute("show partitions {}".format(tbl))
    assert new_res.data == res.data
    self.assert_impalad_log_contains("WARNING", "stale partition", expected_count=0)
    self.assert_impalad_log_contains("ERROR", "stale partition", expected_count=0)

    #############################################################
    # Test 2: UpdatePartition + Invalidate + Load
    #
    # Updates the partition, invalidates the table and then reloads it. Checks the dropped
    # version of the partition in the removed table version won't interfere with the
    # update. Run this 5 times so they could happen inside a catalog update cycle.
    self.client.execute("alter table {} add partition(p=5)".format(tbl))
    for num in range(5):
      self.client.execute("refresh {} partition(p=5)".format(tbl))
      self.client.execute("invalidate metadata " + tbl)
      res = self.client.execute("show partitions " + tbl)
      assert self.has_value("p=0", res.data)
      assert self.has_value("p=5", res.data)
      # The last line is the total summary
      assert len(res.data) == 3

    # Restart impalad and check the partitions on it
    self.cluster.impalads[0].restart()
    self.client = self.create_impala_client()
    new_res = self.client.execute("show partitions {}".format(tbl))
    assert new_res.data == res.data
    self.assert_impalad_log_contains("WARNING", "stale partition", expected_count=0)
    self.assert_impalad_log_contains("ERROR", "stale partition", expected_count=0)

    #############################################################
    # Test 3: DropPartition + DropTable + CreateTable + Load
    #
    # Check no leaks if the partition and table are dropped sequentially.
    self.client.execute("alter table {} drop partition(p=0)".format(tbl))
    self.client.execute("drop table " + tbl)
    time.sleep(2)
    # Re-create and reload the HdfsTable so Impalad will see a HdfsTable with an empty
    # partition map. Any leaked stale partitions in the catalog topic will be reported.
    self.client.execute("create table {}(i int) partitioned by (p int)".format(tbl))
    self.client.execute("describe " + tbl)
    # Restart impalad and check the partitions on it
    self.cluster.impalads[0].restart()
    self.client = self.create_impala_client()
    res = self.client.execute("show partitions {}".format(tbl))
    assert len(res.data) == 1
    self.assert_impalad_log_contains("WARNING", "stale partition", expected_count=0)
    self.assert_impalad_log_contains("ERROR", "stale partition", expected_count=0)
