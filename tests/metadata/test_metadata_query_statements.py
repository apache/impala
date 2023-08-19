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

# Impala tests for queries that query metadata and set session settings

from __future__ import absolute_import, division, print_function
import pytest
import re

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfCatalogV2
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.util.filesystem_utils import get_fs_path
from tests.util.event_processor_utils import EventProcessorUtils

# TODO: For these tests to pass, all table metadata must be created exhaustively.
# the tests should be modified to remove that requirement.
class TestMetadataQueryStatements(ImpalaTestSuite):

  CREATE_DATA_SRC_STMT = ("CREATE DATA SOURCE %s LOCATION '" +
      get_fs_path("/test-warehouse/data-sources/test-data-source.jar") +
      "' CLASS 'org.apache.impala.extdatasource.AllTypesDataSource' API_VERSION 'V1'")
  DROP_DATA_SRC_STMT = "DROP DATA SOURCE IF EXISTS %s"
  TEST_DATA_SRC_NAMES = ["show_test_ds1", "show_test_ds2"]
  AVRO_SCHEMA_LOC = get_fs_path("/test-warehouse/avro_schemas/functional/alltypes.json")

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMetadataQueryStatements, cls).add_test_dimensions()
    sync_ddl_opts = [0, 1]
    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on test runtime by only running with SYNC_DDL=0
      sync_ddl_opts = [0]

    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=ALL_NODES_ONLY,
        disable_codegen_options=[False],
        batch_sizes=[0],
        sync_ddl=sync_ddl_opts))
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_use(self, vector):
    self.run_test_case('QueryTest/use', vector)

  def test_show(self, vector):
    self.run_test_case('QueryTest/show', vector)

  @SkipIfFS.incorrent_reported_ec
  def test_show_stats(self, vector):
    self.run_test_case('QueryTest/show-stats', vector, "functional")

  def test_describe_path(self, vector, unique_database):
    self.run_test_case('QueryTest/describe-path', vector, unique_database)

  # Missing Coverage: Describe formatted compatibility between Impala and Hive when the
  # data doesn't reside in hdfs.
  @SkipIfFS.hive
  def test_describe_formatted(self, vector, unique_database):
    # IMPALA-10176: test_describe_formatted is broken, so disable it for now
    pytest.skip()
    # For describe formmated, we try to match Hive's output as closely as possible.
    # However, we're inconsistent with our handling of NULLs vs theirs - Impala sometimes
    # specifies 'NULL' where Hive uses an empty string, and Hive somtimes specifies 'null'
    # with padding where Impala uses a sequence of blank spaces - and for now
    # we want to leave it that way to not affect users who rely on this output.
    def compare_describe_formatted(impala_results, hive_results):
      for impala, hive in zip(re.split(',|\n', impala_results),
          re.split(',|\n', hive_results)):

        if impala != hive:
          # If they don't match, check if it's because of the inconsistent null handling.
          impala = impala.replace(' ', '').lower()
          hive = hive.replace(' ', '').lower()
          if not ((impala == "'null'" and hive ==  "''") or
              (impala == "''" and hive == "'null'")):
            return False
      return True

    # Describe a partitioned table.
    self.exec_and_compare_hive_and_impala_hs2("describe formatted functional.alltypes",
        compare=compare_describe_formatted)
    self.exec_and_compare_hive_and_impala_hs2(
        "describe formatted functional_text_gzip.alltypes",
        compare=compare_describe_formatted)

    # Describe an unpartitioned table.
    self.exec_and_compare_hive_and_impala_hs2("describe formatted tpch.lineitem",
        compare=compare_describe_formatted)
    self.exec_and_compare_hive_and_impala_hs2("describe formatted functional.jointbl",
        compare=compare_describe_formatted)

    # Create and describe an unpartitioned and partitioned Avro table created
    # by Impala without any column definitions.
    # TODO: Instead of creating new tables here, change one of the existing
    # Avro tables to be created without any column definitions.
    self.client.execute("create database if not exists %s" % unique_database)
    self.client.execute((
        "create table %s.%s with serdeproperties ('avro.schema.url'='%s') stored as avro"
        % (unique_database, "avro_alltypes_nopart", self.AVRO_SCHEMA_LOC)))
    self.exec_and_compare_hive_and_impala_hs2("describe formatted avro_alltypes_nopart",
        compare=compare_describe_formatted)

    self.client.execute((
        "create table %s.%s partitioned by (year int, month int) "
        "with serdeproperties ('avro.schema.url'='%s') stored as avro"
        % (unique_database, "avro_alltypes_part", self.AVRO_SCHEMA_LOC)))
    self.exec_and_compare_hive_and_impala_hs2("describe formatted avro_alltypes_part",
        compare=compare_describe_formatted)

    self.exec_and_compare_hive_and_impala_hs2(\
        "describe formatted functional.alltypes_view_sub",
        compare=compare_describe_formatted)

    # test for primary / foreign constraints
    self.exec_and_compare_hive_and_impala_hs2(\
        "describe formatted functional.child_table",
        compare=compare_describe_formatted)

    self.exec_and_compare_hive_and_impala_hs2(\
        "describe formatted functional.parent_table_2",
        compare=compare_describe_formatted)

    self.exec_and_compare_hive_and_impala_hs2(\
        "describe formatted tpcds.store_returns",
        compare=compare_describe_formatted)

  @pytest.mark.execute_serially # due to data src setup/teardown
  def test_show_data_sources(self, vector):
    try:
      self.__create_data_sources()
      self.run_test_case('QueryTest/show-data-sources', vector)
    finally:
      self.__drop_data_sources()

  def __drop_data_sources(self):
    for name in self.TEST_DATA_SRC_NAMES:
      self.client.execute(self.DROP_DATA_SRC_STMT % (name,))

  def __create_data_sources(self):
    self.__drop_data_sources()
    for name in self.TEST_DATA_SRC_NAMES:
      self.client.execute(self.CREATE_DATA_SRC_STMT % (name,))

  @SkipIfFS.hive
  @pytest.mark.execute_serially  # because of use of hardcoded database
  def test_describe_db(self, vector, cluster_properties):
    self.__test_describe_db_cleanup()
    try:
      self.client.execute("create database impala_test_desc_db1")
      self.client.execute("create database impala_test_desc_db2 "
                          "comment 'test comment'")
      self.client.execute("create database impala_test_desc_db3 "
                          "location '" + get_fs_path("/testdb") + "'")
      self.client.execute("create database impala_test_desc_db4 comment 'test comment' "
                          "location \"" + get_fs_path("/test2.db") + "\"")
      self.client.execute("create database impala_test_desc_db5 comment 'test comment' "
                          "managedlocation \"" + get_fs_path("/test2.db") + "\"")
      self.run_stmt_in_hive("create database hive_test_desc_db comment 'test comment' "
                           "with dbproperties('pi' = '3.14', 'e' = '2.82')")
      self.run_stmt_in_hive("create database hive_test_desc_db2 comment 'test comment' "
                           "managedlocation '" + get_fs_path("/test2.db") + "'")
      if cluster_properties.is_event_polling_enabled():
        # Using HMS event processor - wait until the database shows up.
        assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
        EventProcessorUtils.wait_for_event_processing(self)
        self.confirm_db_exists("hive_test_desc_db")
      else:
        # Invalidate metadata to pick up hive-created db.
        self.client.execute("invalidate metadata")
      self.run_test_case('QueryTest/describe-db', vector)
    finally:
      self.__test_describe_db_cleanup()

  def __test_describe_db_cleanup(self):
    self.cleanup_db('hive_test_desc_db')
    self.cleanup_db('hive_test_desc_db2')
    self.cleanup_db('impala_test_desc_db1')
    self.cleanup_db('impala_test_desc_db2')
    self.cleanup_db('impala_test_desc_db3')
    self.cleanup_db('impala_test_desc_db4')
    self.cleanup_db('impala_test_desc_db5')
