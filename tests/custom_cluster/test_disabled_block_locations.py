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
from os import getenv
from tests.common.file_utils import create_iceberg_table_from_directory
from tests.common.skip import SkipIf
from tests.util.filesystem_utils import IS_HDFS

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

CORE_SITE_CONFIG_DIR = getenv('IMPALA_HOME') + '/fe/src/test/resources/' +\
    'core-site-disabled-block-locations'


class TestDisabledBlockLocations(CustomClusterTestSuite):
  @classmethod
  def setup_class(cls):
    super(TestDisabledBlockLocations, cls).setup_class()

  def load_table(self, database, table_name, format="parquet",
      table_location="${IMPALA_HOME}/testdata/data/iceberg_test/iceberg_v3"):
    create_iceberg_table_from_directory(self.client, database,
        table_name, format, table_location)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(custom_core_site_dir=CORE_SITE_CONFIG_DIR)
  def test_no_block_locations(self, vector):
    self.run_test_case('QueryTest/no-block-locations', vector)
    if IS_HDFS:
      self.run_test_case('QueryTest/no-block-locations-hdfs-only', vector)

  @SkipIf.not_dfs
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(custom_core_site_dir=CORE_SITE_CONFIG_DIR)
  def test_iceberg_smoke(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-create', vector, unique_database)
    self.run_test_case('QueryTest/iceberg-external', vector, unique_database)
    self.run_test_case('QueryTest/iceberg-insert', vector, unique_database)
    self.run_test_case('QueryTest/iceberg-delete', vector, unique_database)
    self.run_test_case('QueryTest/iceberg-delete-partitioned', vector, unique_database)

  @SkipIf.not_dfs
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(custom_core_site_dir=CORE_SITE_CONFIG_DIR)
  def test_iceberg_v3_smoke(self, vector, unique_database):
    self.load_table(unique_database, "iceberg_v3_row_lineage")
    self.load_table(unique_database, "iceberg_v3_row_lineage_orc", format="orc")
    self.run_test_case('QueryTest/iceberg-v3-row-lineage', vector, unique_database)

    self.load_table(unique_database, "iceberg_v3_deletion_vectors")
    self.run_test_case('QueryTest/iceberg-v3-delete', vector, unique_database)
    self.run_test_case('QueryTest/iceberg-v3-delete-partition-sort',
        vector, unique_database)

    self.run_test_case('QueryTest/iceberg-v3-optimize', vector, unique_database)

    self.execute_query("drop table {}.{}".format(
                       unique_database, "iceberg_v3_deletion_vectors"))
    self.load_table(unique_database, "iceberg_v3_deletion_vectors")
    self.run_test_case('QueryTest/iceberg-v3-delete-existing-dv', vector, unique_database)

    self.load_table(unique_database, "iceberg_v2_delete_equality",
        table_location="${IMPALA_HOME}/testdata/data/iceberg_test/hadoop_catalog/ice")
    self.run_test_case('QueryTest/iceberg-v3-delete-v2-equality-upgrade',
        vector, unique_database)

    self.run_test_case('QueryTest/iceberg-v3-update', vector, unique_database)
