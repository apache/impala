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

import logging
import pytest
import shutil

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
