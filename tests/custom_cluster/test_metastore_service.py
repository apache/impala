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
import pytest
from hive_metastore.ttypes import Database
from hive_metastore.ttypes import FieldSchema
from hive_metastore.ttypes import FindNextCompactRequest
from hive_metastore.ttypes import GetTableRequest
from hive_metastore.ttypes import GetPartitionsByNamesRequest
from hive_metastore.ttypes import TruncateTableRequest
from hive_metastore.ttypes import Table
from hive_metastore.ttypes import StorageDescriptor
from hive_metastore.ttypes import SerDeInfo
from hive_metastore.ttypes import UpdateTransactionalStatsRequest
from hive_metastore.ttypes import WriteNotificationLogBatchRequest

from tests.util.event_processor_utils import EventProcessorUtils
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.filesystem_utils import IS_HDFS, IS_OZONE

class TestMetastoreService(CustomClusterTestSuite):
    """
    Tests for the Catalog Metastore service. Each test in this class should
    start a hms_server using the catalogd flag --start_hms_server=true
    """

    part_tbl = ImpalaTestSuite.get_random_name("test_metastore_part_tbl")
    unpart_tbl = ImpalaTestSuite.get_random_name("test_metastore_unpart_tbl")
    acid_part_tbl = ImpalaTestSuite.get_random_name("test_metastore_acid_part_tbl")
    unpart_acid_tbl = ImpalaTestSuite.get_random_name("test_metastore_unpart_acid_tbl")
    default_unknowntbl = ImpalaTestSuite.get_random_name("test_metastore_default_tbl")

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899"
    )
    def test_passthrough_apis(self):
        """
        This test exercises some of the Catalog HMS APIs which are directly
        passed through to the backing HMS service. This is by no means an
        exhaustive set but merely used to as a sanity check to make sure that a
        hive_client can connect to the Catalog's metastore service and is
        able to execute calls to the backing HMS service.
        """
        catalog_hms_client = None
        db_name = ImpalaTestSuite.get_random_name("test_passthrough_apis_db")
        try:
            catalog_hms_client, hive_transport = ImpalaTestSuite.create_hive_client(5899)
            assert catalog_hms_client is not None
            # get_databases
            databases = catalog_hms_client.get_all_databases()
            assert databases is not None
            assert len(databases) > 0
            assert "functional" in databases
            # get_database
            database = catalog_hms_client.get_database("functional")
            assert database is not None
            assert "functional" == database.name
            # get_tables
            tables = catalog_hms_client.get_tables("functional", "*")
            assert tables is not None
            assert len(tables) > 0
            assert "alltypes" in tables
            # get table
            table = catalog_hms_client.get_table("functional", "alltypes")
            assert table is not None
            assert "alltypes" == table.tableName
            assert table.sd is not None
            # get partitions
            partitions = catalog_hms_client.get_partitions("functional", "alltypes", -1)
            assert partitions is not None
            assert len(partitions) > 0
            # get partition names
            part_names = catalog_hms_client.get_partition_names("functional", "alltypes",
                                                                -1)
            assert part_names is not None
            assert len(part_names) > 0
            assert "year=2009/month=1" in part_names
            # notification APIs
            event_id = EventProcessorUtils.get_current_notification_id(catalog_hms_client)
            assert event_id is not None
            assert event_id > 0
            # DDLs
            catalog_hms_client.create_database(self.__get_test_database(db_name))
            database = catalog_hms_client.get_database(db_name)
            assert database is not None
            assert db_name == database.name
            tbl_name = ImpalaTestSuite.get_random_name(
                "test_passthrough_apis_tbl")
            cols = [["c1", "int", "col 1"], ["c2", "string", "col 2"]]
            part_cols = [["part", "string", "part col"]]
            catalog_hms_client.create_table(self.__get_test_tbl(db_name, tbl_name,
                cols, part_cols))
            table = catalog_hms_client.get_table(db_name, tbl_name)
            assert table is not None
            assert tbl_name == table.tableName
            self.__compare_cols(cols, table.sd.cols)
            self.__compare_cols(part_cols, table.partitionKeys)
        finally:
            if catalog_hms_client is not None:
                catalog_hms_client.drop_database(db_name, True, True)
                catalog_hms_client.shutdown()

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--enable_sync_to_latest_event_on_ddls=false"
    )
    def test_get_table_req_with_fallback(self):
      """
      Test the get_table_req APIs with fallback to HMS enabled. These calls
      succeed even if catalog throws exceptions since we fallback to HMS.
      """
      catalog_hms_client = None
      db_name = ImpalaTestSuite.get_random_name(
          "test_get_table_req_with_fallback_db")
      try:
        catalog_hms_client, hive_transport = ImpalaTestSuite.create_hive_client(5899)
        assert catalog_hms_client is not None

        # Test simple get_table_req without stats.
        get_table_request = GetTableRequest()
        get_table_request.dbName = "functional"
        get_table_request.tblName = "alltypes"
        get_table_request.getFileMetadata = True
        get_table_response = catalog_hms_client.get_table_req(get_table_request)
        assert get_table_response.table.dbName == "functional"
        assert get_table_response.table.tableName == "alltypes"
        # Request did not ask for stats, verify colStats are not populated.
        assert get_table_response.table.colStats is None
        # assert that fileMetadata is in the response
        self.__assert_filemd(get_table_response.table.fileMetadata,
                             get_table_response.table.dictionary)

        # Test get_table_request with stats and engine set to Impala.
        get_table_request.getColumnStats = True
        get_table_request.engine = "impala"
        get_table_request.getFileMetadata = False
        get_table_response = catalog_hms_client.get_table_req(get_table_request)
        assert get_table_response.table.dbName == "functional"
        assert get_table_response.table.tableName == "alltypes"
        assert get_table_response.table.colStats is not None
        # Verify the column stats objects are populated for
        # non-clustering columns.
        assert len(get_table_response.table.colStats.statsObj) > 0
        # file-metadata is not requested and hence should be not be set
        # assert that fileMetadata is in the response
        self.__assert_no_filemd(get_table_response.table.fileMetadata,
                             get_table_response.table.dictionary)

        # Create table in Hive and test this is properly falling back to HMS.
        # Create table via Hive.
        catalog_hms_client.create_database(self.__get_test_database(db_name))
        database = catalog_hms_client.get_database(db_name)
        assert database is not None
        assert db_name == database.name
        tbl_name = ImpalaTestSuite.get_random_name(
            "test_get_table_req_with_fallback_tbl")
        cols = [["c1", "int", "col 1"], ["c2", "string", "col 2"]]
        catalog_hms_client.create_table(self.__get_test_tbl(db_name, tbl_name, cols))

        get_table_request.dbName = db_name
        get_table_request.tblName = tbl_name
        get_table_request.getColumnStats = True
        get_table_request.getFileMetadata = True

        # Engine is not specified, this should throw an exception even if
        # fallback_to_hms_on_errors is true.
        expected_exception = "isGetColumnStats() is true in the request but " \
            "engine is not specified."
        try:
          catalog_hms_client.get_table_req(get_table_request)
        except Exception as e:
          if expected_exception is not None:
            assert expected_exception in str(e)

        # Set engine and request impala, this should fallback to HMS since
        # the table is not in catalog cache. File metadata should be set even if the
        # request is served via HMS fallback.
        get_table_request.engine = "Impala"
        get_table_response = catalog_hms_client.get_table_req(get_table_request)
        assert get_table_response.table.dbName == db_name
        assert get_table_response.table.tableName == tbl_name
        assert get_table_response.table.fileMetadata is not None
        # The table does not have any data so we expect the fileMetadata.data to be None
        assert get_table_response.table.fileMetadata.data is None
        # even if there are no files, the object dictionary should be not null and empty
        assert get_table_response.table.dictionary is not None
        assert len(get_table_response.table.dictionary.values) == 0
      finally:
        if catalog_hms_client is not None:
            catalog_hms_client.shutdown()
        if self.__get_database_no_throw(db_name) is not None:
          self.hive_client.drop_database(db_name, True, True)

    def __get_database_no_throw(self, db_name):
      try:
        return self.hive_client.get_database(db_name)
      except Exception:
        return None

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--fallback_to_hms_on_errors=false "
                      "--hms_event_polling_interval_s=0 "
                      "--enable_sync_to_latest_event_on_ddls=false"
    )
    def test_get_table_req_without_fallback(self):
      """
      Test the get_table_req APIs with fallback to HMS enabled. These calls
      throw exceptions since we do not fallback to HMS if the db/table is not
      in Catalog cache. We specifically disable events polling because this test
      exercises the error code paths when table is not found in the catalogd. With
      events processing turned on, it leads to flakiness.
      """
      catalog_hms_client = None
      db_name = ImpalaTestSuite.get_random_name(
          "test_get_table_req_without_fallback_db")
      new_db_name = None
      try:
        catalog_hms_client, hive_transport = ImpalaTestSuite.create_hive_client(5899)
        assert catalog_hms_client is not None

        # Create table via Hive.
        self.hive_client.create_database(self.__get_test_database(db_name))
        database = self.hive_client.get_database(db_name)
        assert database is not None
        assert db_name == database.name
        tbl_name = ImpalaTestSuite.get_random_name("test_get_table_req_tbl")
        cols = [["c1", "int", "col 1"], ["c2", "string", "col 2"]]
        self.hive_client.create_table(self.__get_test_tbl(db_name, tbl_name, cols))

        # Test get_table_request with stats and engine set to Impala.
        # Test simple get_table_req without stats.
        get_table_request = GetTableRequest()
        get_table_request.dbName = db_name
        get_table_request.tblName = tbl_name
        get_table_request.getColumnStats = True
        # Engine is not set, this should throw an exception without falling
        # back to HMS.
        expected_exception_str = "Column stats are requested " \
            "but engine is not set in the request."
        self.__call_get_table_req_expect_exception(catalog_hms_client,
            get_table_request, expected_exception_str)
        # Verify DB not found exception is thrown. Engine does not matter,
        # we only return Impala table level column stats but this field is
        # required to be consistent with Hive's implementation.
        get_table_request.engine = "Impala"
        expected_exception_str = "Database " + db_name + " not found"
        self.__call_get_table_req_expect_exception(catalog_hms_client,
            get_table_request, expected_exception_str)

        # Create database in Impala
        new_db_name = ImpalaTestSuite.get_random_name(
            "test_get_table_req_without_fallback_db")
        query = "create database " + new_db_name
        self.execute_query_expect_success(self.client, query)
        new_tbl_name = ImpalaTestSuite.get_random_name(
            "test_get_table_req_without_fallback_tbl")
        new_cols = [["c1", "int", "col 1"], ["c2", "string", "col 2"]]
        # DDLs currently only pass-through to HMS so this call will not create a table
        # in catalogd.
        catalog_hms_client.create_table(self.__get_test_tbl(new_db_name, new_tbl_name,
            new_cols))
        # Verify table not found exception is thrown
        get_table_request.dbName = new_db_name
        get_table_request.tblName = new_tbl_name
        expected_exception_str = "Table " + new_db_name + "." + new_tbl_name \
                                 + " not found"
        self.__call_get_table_req_expect_exception(catalog_hms_client,
            get_table_request, expected_exception_str)

        # Create a table in Impala and verify that the get_table_req gets the
        # table and stats for non-clustering columns immediately.
        impala_tbl_name = ImpalaTestSuite.get_random_name(
            "test_get_table_req_without_fallback_impala_tbl")
        query = "create table " + new_db_name + "." + impala_tbl_name + \
            "(id int) partitioned by (year int)"
        self.execute_query_expect_success(self.client, query)
        get_table_request.dbName = new_db_name
        get_table_request.tblName = impala_tbl_name
        get_table_request.getColumnStats = True
        # Engine does not matter, we only return Impala table level column
        # stats but this field is required to be consistent with Hive's
        # implementation.
        get_table_request.engine = "impala"
        get_table_response = catalog_hms_client.get_table_req(get_table_request)
        assert get_table_response.table.dbName == new_db_name
        assert get_table_response.table.tableName == impala_tbl_name
        assert get_table_response.table.colStats is not None
        assert get_table_response.table.colStats.statsObj is not None
        # Verify there is a ColumnStatisticsObj only for non-clustering columns
        # (id in this case).
        assert len(get_table_response.table.colStats.statsObj) == 1
        assert get_table_response.table.colStats.statsObj[0].colName == "id"

        # Verify a non-default catalog in the request throws an exception.
        get_table_request.catName = "testCatalog"
        expected_exception_str = "Catalog service does not support " \
            "non-default catalogs"
        self.__call_get_table_req_expect_exception(catalog_hms_client,
            get_table_request, expected_exception_str)
        # fetch file-metadata of a non-partitioned table
        get_table_request = GetTableRequest()
        get_table_request.dbName = "functional"
        get_table_request.tblName = "tinytable"
        get_table_request.getFileMetadata = True
        get_table_response = catalog_hms_client.get_table_req(get_table_request)
        assert get_table_response.table.dbName == "functional"
        assert get_table_response.table.tableName == "tinytable"
        self.__assert_filemd(get_table_response.table.fileMetadata,
                             get_table_response.table.dictionary)

        # fetch file-metadata along with stats
        get_table_request = GetTableRequest()
        get_table_request.dbName = "functional"
        get_table_request.tblName = "tinytable"
        get_table_request.getFileMetadata = True
        get_table_request.getColumnStats = True
        get_table_request.engine = "impala"
        get_table_response = catalog_hms_client.get_table_req(get_table_request)
        assert get_table_response.table.dbName == "functional"
        assert get_table_response.table.tableName == "tinytable"
        self.__assert_filemd(get_table_response.table.fileMetadata,
                             get_table_response.table.dictionary)
        assert get_table_response.table.colStats is not None
        assert get_table_response.table.colStats.statsObj is not None
      finally:
        if catalog_hms_client is not None:
          catalog_hms_client.shutdown()
        if self.__get_database_no_throw(db_name) is not None:
          self.hive_client.drop_database(db_name, True, True)
        if self.__get_database_no_throw(new_db_name) is not None:
          self.hive_client.drop_database(new_db_name, True, True)

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--fallback_to_hms_on_errors=false"
    )
    def test_get_partitions_by_names(self):
      catalog_hms_client = None
      try:
        catalog_hms_client, hive_transport = ImpalaTestSuite.create_hive_client(5899)
        assert catalog_hms_client is not None
        valid_part_names = ["year=2009/month=1", "year=2009/month=2", "year=2009/month=3",
           "year=2009/month=4"]
        self.__run_partitions_by_names_tests(catalog_hms_client, "functional_parquet",
          "alltypestiny", valid_part_names)
      finally:
        if catalog_hms_client is not None:
          catalog_hms_client.shutdown()

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--fallback_to_hms_on_errors=true"
    )
    def test_fallback_get_partitions_by_names(self, unique_database):
      """
      Test makes sure that the fall-back path is working in case of errors in catalogd's
      implementation of get_partitions_by_name_req.
      """
      catalog_client = None
      try:
        catalog_client, hive_transport = ImpalaTestSuite.create_hive_client(5899)
        assert catalog_client is not None
        assert self.hive_client is not None
        self.__create_test_tbls_from_hive(unique_database)
        valid_part_names = ["year=2009/month=1", "year=2009/month=2", "year=2009/month=3",
                            "year=2009/month=4"]
        self.__run_partitions_by_names_tests(catalog_client, unique_database,
          TestMetastoreService.part_tbl, valid_part_names, True)
      finally:
        if catalog_client is not None:
          catalog_client.shutdown()

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--fallback_to_hms_on_errors=true "
                      "--invalidate_hms_cache_on_ddls=true "
                      "--enable_sync_to_latest_event_on_ddls=false"
    )
    def test_cache_invalidated_on_nontransactional_table_ddls(self):
        db_name = ImpalaTestSuite.get_random_name(
            "test_cache_invalidated_on_nontransactional_table_ddls_db")
        tbl_name = ImpalaTestSuite.get_random_name(
            "test_cache_invalidated_on_nontransactional_table_ddls_tbl")
        self.__test_non_transactional_table_cache_helper(db_name, tbl_name, True)

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--fallback_to_hms_on_errors=true "
                      "--invalidate_hms_cache_on_ddls=false "
                      "--enable_sync_to_latest_event_on_ddls=false"
    )
    def test_cache_valid_on_nontransactional_table_ddls(self):
        db_name = ImpalaTestSuite.get_random_name(
            "test_cache_valid_on_nontransactional_table_ddls_db")
        tbl_name = ImpalaTestSuite.get_random_name(
            "test_cache_valid_on_nontransactional_table_ddls_tbl")
        self.__test_non_transactional_table_cache_helper(db_name, tbl_name, False)

    def __test_non_transactional_table_cache_helper(self, db_name,
                                                    tbl_name, invalidateCache):
        """
        This tests the following DDLs after creating table and loading it in cache:
        1. Drop partition:
              If invalidateCache is True, then table should be removed from cache
              and then subsequent get_partitions_by_names_req should load table from HMS
              If invalidateCache is False, the get_partitions_by_names_req
              should be served from the already cached table
        2. Truncate table remove partition:
              If invalidateCache is True, then table should be removed from cache
              and then subsequent get_partitions_by_names_req should load table from HMS
              and the file metadata for the truncated partition should be none
              If invalidateCache is False, the get_partitions_by_names_req
              should be served from the already cached table which has stale
              partition file metadata
        2. Alter table rename :
              If invalidateCache is True, this should remove the old table from the cache
              and add new table. Subsequent get_table req on old table should throw an
              exception. If invalidateCache is False, the old table should still be
              present in the cache and subsequent get_table req should serve old table
              from the cache
        3. Alter table add column:
              If invalidateCache is True, then table should be invalidated from cache
              and then subsequent get_table_req should reload table from HMS
              If invalidateCache is False, the should be served from the already
              cached table which has the stale column info
        4. drop table:
              If invalidateCache is True, the table is removed from the cache
              If invalidateCache is False, the table is *not* removed from the cache
        """
        catalog_hms_client = None
        cur_get_table_response = None
        prev_get_table_response = None
        try:
            catalog_hms_client, hive_transport = ImpalaTestSuite.create_hive_client(5899)
            assert catalog_hms_client is not None

            create_database_query = "create database " + db_name
            self.execute_query_expect_success(self.client, create_database_query)

            database = catalog_hms_client.get_database(db_name)
            assert database is not None
            assert db_name == database.name

            create_tbl_query = "create table " + db_name + "." + tbl_name + \
                               "(c1 int) partitioned by (part_col int) "
            self.execute_query_expect_success(self.client, create_tbl_query)

            insert_queries = [
                "insert into {0}.{1} PARTITION (part_col=1) VALUES (1)".format(
                    db_name, tbl_name),
                "insert into {0}.{1} PARTITION (part_col=2) VALUES (2)".format(
                    db_name, tbl_name),
                "insert into {0}.{1} PARTITION (part_col=3) VALUES (3)".format(
                    db_name, tbl_name)
            ]
            for query in insert_queries:
                self.execute_query_expect_success(self.client, query)

            get_table_request = self.__create_get_table_hms_request(
                db_name, tbl_name, True, False, "impala")
            cur_get_table_response = catalog_hms_client.get_table_req(
                get_table_request)
            assert cur_get_table_response.table is not None
            assert cur_get_table_response.table.dbName == db_name
            assert cur_get_table_response.table.tableName == tbl_name

            partitions_response = catalog_hms_client.get_partition_names(
                db_name, tbl_name, -1)
            assert partitions_response is not None
            assert len(partitions_response) == 3

            # drop a partition
            catalog_hms_client.drop_partition_by_name(
                db_name, tbl_name, "part_col=3", True)

            # save cur_get_table_response in prev_get_table_response
            # before calling get_table_req HMS api
            prev_get_table_response = cur_get_table_response

            new_get_table_request = self.__create_get_table_hms_request(
                db_name, tbl_name, True, False, "impala")
            cur_get_table_response = catalog_hms_client.get_table_req(
                new_get_table_request)
            assert cur_get_table_response.table is not None

            part_col_names = ["part_col=1", "part_col=2", "part_col=3"]
            get_parts_req = GetPartitionsByNamesRequest()
            get_parts_req.db_name = db_name
            get_parts_req.tbl_name = tbl_name
            get_parts_req.names = part_col_names
            get_parts_req.getFileMetadata = True
            parts_response = catalog_hms_client.get_partitions_by_names_req(
                get_parts_req)
            if invalidateCache:
                # drop_partition_by_name hms api should invalidate
                # table from cache and reload new table from HMS
                len(parts_response.partitions) == 2
            else:
                # table should be served from the cache
                # and the cached table has 3 partitions
                len(parts_response.partitions) == 3

            # Truncate table by removing a partition
            part_to_truncate = ["part_col=2"]
            truncate_table_req = TruncateTableRequest()
            truncate_table_req.dbName = db_name
            truncate_table_req.tableName = tbl_name
            truncate_table_req.partNames = part_to_truncate
            catalog_hms_client.truncate_table_req(truncate_table_req)

            # Check partition's file metadata after truncating it
            # for table invalidated from cache, partition
            # file metadata is none whereas it is not none
            # for stale table present in cache
            get_parts_req = GetPartitionsByNamesRequest()
            get_parts_req.db_name = db_name
            get_parts_req.tbl_name = tbl_name
            get_parts_req.names = part_to_truncate
            get_parts_req.getFileMetadata = True
            parts_response = catalog_hms_client.get_partitions_by_names_req(
                get_parts_req)
            assert len(parts_response.partitions) == 1
            for part in parts_response.partitions:
                assert part.fileMetadata is not None
                if invalidateCache:
                    assert part.fileMetadata.data is None
                else:
                    assert part.fileMetadata.data is not None

            # alter current table by renaming
            # the table name
            table_to_update = self.__deepCopyTable(cur_get_table_response.table)
            new_table_name = tbl_name + "_new"

            # update table name
            table_to_update.tableName = new_table_name
            # alter the table
            catalog_hms_client.alter_table(db_name, tbl_name, table_to_update)
            prev_get_table_response = cur_get_table_response
            expected_exception = None
            # create get request for old table
            old_table_get_request = self.__create_get_table_hms_request(
                db_name, tbl_name, True, False, "impala")
            if invalidateCache:
                # get_table_req on old table should throw exception
                # because it neither exists in cache nor in HMS
                try:
                    cur_get_table_response = catalog_hms_client.get_table_req(
                        old_table_get_request)
                except Exception as e:
                    expected_exception = e
                assert expected_exception is not None
            else:
                # old table still exists in cache
                cur_get_table_response = catalog_hms_client.get_table_req(
                    old_table_get_request)
                assert expected_exception is None

            # make sure that new table exists in cache
            self.execute_query_expect_success(
                self.client, "invalidate metadata " + db_name + "." + new_table_name)

            # get_table_req on new table should return table
            new_get_table_request = self.__create_get_table_hms_request(
                db_name, new_table_name, True, False, "impala")

            cur_get_table_response = catalog_hms_client.get_table_req(
                new_get_table_request)
            assert cur_get_table_response.table is not None
            assert cur_get_table_response.table.sd is not None

            table_to_update = self.__deepCopyTable(cur_get_table_response.table)

            updated_cols = [["c1", "int", ""], ["c2", "int", ""]]
            # alter new table by adding new column
            table_to_update.sd.cols = self.__create_field_schemas(updated_cols)
            catalog_hms_client.alter_table(db_name, new_table_name, table_to_update)
            # get table
            cur_get_table_response = catalog_hms_client.get_table_req(
                new_get_table_request)

            if invalidateCache:
                # new get_table_req should load the table from HMS again because the table
                # was invalidated from catalogD cache
                self.__compare_cols(updated_cols,
                                    cur_get_table_response.table.sd.cols, False)
            else:
                # new_get_table_req should load the existing table from cache
                # so the expected cols should be old ones
                expected_cols = [["c1", "int", ""]]
                self.__compare_cols(
                    expected_cols, cur_get_table_response.table.sd.cols, False)

            prev_get_table_response = cur_get_table_response
            # drop table
            catalog_hms_client.drop_table(db_name, new_table_name, True)
            if invalidateCache:
                # new get_table_req should throw an exception
                # since the table does not exist in cache as well as in HMS
                expected_exception = None
                try:
                    cur_get_table_response = catalog_hms_client.get_table_req(
                        new_get_table_request)
                except Exception as e:
                    expected_exception = e
                assert expected_exception is not None
            else:
                # new get_table_req should return the
                # table from the cache
                cur_get_table_response = catalog_hms_client.get_table_req(
                    new_get_table_request)
                assert cur_get_table_response.table is not None
                assert prev_get_table_response.table == cur_get_table_response.table
        finally:
            if catalog_hms_client is not None:
                catalog_hms_client.shutdown()
            if self.__get_database_no_throw(db_name) is not None:
                self.hive_client.drop_database(db_name, True, True)

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--fallback_to_hms_on_errors=true "
                      "--invalidate_hms_cache_on_ddls=true "
                      "--enable_sync_to_latest_event_on_ddls=false "
                      "--hms_event_polling_interval_s=1"
    )
    def test_cache_invalidate_incomplete_table(self):
        """
        Tests the cache invalidation on an incomplete table
        i.e a table not fully loaded in cache
        """
        db_name = "test_cache_invalidate_incomplete_table_db"
        tbl_name = "test_cache_invalidate_incomplete_table_tbl"
        catalog_hms_client = None
        try:
            catalog_hms_client, hive_transport = ImpalaTestSuite.create_hive_client(5899)
            assert catalog_hms_client is not None

            create_database_query = "create database " + db_name
            self.execute_query_expect_success(self.client, create_database_query)

            database = catalog_hms_client.get_database(db_name)
            assert database is not None
            assert db_name == database.name

            create_tbl_query = "create table " + db_name + "." + tbl_name + \
                               "(c1 int) partitioned by (part_col int) "
            self.execute_query_expect_success(self.client, create_tbl_query)

            insert_queries = [
                "insert into {0}.{1} PARTITION (part_col=1) VALUES (1)".format(
                    db_name, tbl_name),
                "insert into {0}.{1} PARTITION (part_col=2) VALUES (2)".format(
                    db_name, tbl_name)
            ]
            for query in insert_queries:
                self.execute_query_expect_success(self.client, query)
            get_table_request = self.__create_get_table_hms_request(
                db_name, tbl_name, True, False, "impala")
            cur_get_table_response = catalog_hms_client.get_table_req(
                get_table_request)
            assert cur_get_table_response.table is not None
            assert cur_get_table_response.table.dbName == db_name
            assert cur_get_table_response.table.tableName == tbl_name

            partitions_response = catalog_hms_client.get_partition_names(
                db_name, tbl_name, -1)
            assert partitions_response is not None
            assert len(partitions_response) == 2

            invalidate_tbl_query = "invalidate metadata " + \
                                   db_name + "." + tbl_name
            # now invalidate the table metadata so that we have
            # Incomplete table in the cache
            self.execute_query_expect_success(self.client,
                                              invalidate_tbl_query)

            # drop a partition on an Incomplete table
            catalog_hms_client.drop_partition_by_name(
                db_name, tbl_name, "part_col=2", True)

            new_get_table_request = self.__create_get_table_hms_request(
                db_name, tbl_name, True, False, "impala")
            cur_get_table_response = catalog_hms_client.get_table_req(
                new_get_table_request)
            assert cur_get_table_response.table is not None

            part_col_names = ["part_col=1", "part_col=2"]
            get_parts_req = GetPartitionsByNamesRequest()
            get_parts_req.db_name = db_name
            get_parts_req.tbl_name = tbl_name
            get_parts_req.names = part_col_names
            parts_response = catalog_hms_client.get_partitions_by_names_req(
                get_parts_req)
            assert len(parts_response.partitions) == 1
            # invalidate the table again before dropping it
            self.execute_query_expect_success(self.client, invalidate_tbl_query)
            catalog_hms_client.drop_table(db_name, tbl_name, True)
            # new get_table_req should throw an exception
            # since the table does not exist in cache as well as in HMS
            expected_exception = None
            try:
                cur_get_table_response = catalog_hms_client.get_table_req(
                    new_get_table_request)
            except Exception as e:
                expected_exception = e
            assert expected_exception is not None
        finally:
            if catalog_hms_client is not None:
                catalog_hms_client.shutdown()
            if self.__get_database_no_throw(db_name) is not None:
                self.hive_client.drop_database(db_name, True, True)

    def __create_get_table_hms_request(self, db_name, tbl_name, get_column_stats=False,
                                       get_file_metadata=False, engine="impala"):
        new_get_table_request = GetTableRequest()
        new_get_table_request.dbName = db_name
        new_get_table_request.tblName = tbl_name
        new_get_table_request.getColumnStats = get_column_stats
        new_get_table_request.getFileMetadata = get_file_metadata
        new_get_table_request.engine = engine
        return new_get_table_request

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--fallback_to_hms_on_errors=true "
                      "--invalidate_hms_cache_on_ddls=true "
                      "--enable_sync_to_latest_event_on_ddls=false "
                      "--hms_event_polling_interval_s=5"
    )
    def test_table_create_drop_seq(self):
        """
        The test covers two scenarios:
        1. Drop table from metastore server and
           immediately create it before drop and
           create events are processed by event
           processor
        2. Create table and immediately drop it
           before the events are processed by event
           processor
        """
        catalog_hms_client = None
        cur_get_table_response = None
        db_name = ImpalaTestSuite.get_random_name(
            "test_table_create_drop_seq_db_")
        tbl_name = ImpalaTestSuite.get_random_name(
            "test_table_create_drop_seq_tbl_")
        try:
            catalog_hms_client, hive_transport = \
                ImpalaTestSuite.create_hive_client(5899)
            assert catalog_hms_client is not None
            create_db_query = "create database " + db_name
            self.execute_query_expect_success(self.client, create_db_query)

            database = catalog_hms_client.get_database(db_name)
            assert database is not None
            assert db_name == database.name

            full_table_name = db_name + "." + tbl_name
            # create managed table
            create_tbl_query = "create table " + full_table_name + \
                               "(c1 int) partitioned by (part_col int) "
            self.execute_query_expect_success(self.client, create_tbl_query)

            get_table_request = self.__create_get_table_hms_request(
                db_name, tbl_name, True, False, "impala")
            cur_get_table_response = catalog_hms_client.get_table_req(
                get_table_request)
            assert cur_get_table_response.table is not None
            assert cur_get_table_response.table.dbName == db_name
            assert cur_get_table_response.table.tableName == tbl_name

            # Test scenario 1 - drop table and immediately create it

            # wait for event processor to sync till latest HMS event
            # before issuing drop table
            EventProcessorUtils.wait_for_event_processing(self, 20)
            # get skipped events count so far
            skipped_events_count_before = \
                EventProcessorUtils.get_num_skipped_events()
            # drop table
            catalog_hms_client.drop_table(db_name, tbl_name, True)
            # immediately create same table again via Impala client
            # before drop_event is processed by event processor
            self.execute_query_expect_success(self.client, create_tbl_query)
            # wait for event processor to process all events
            EventProcessorUtils.wait_for_event_processing(self, 20)

            # When DROP_TABLE event is processed by event metastore,
            # event id for drop table < event id of create table
            # As a result of it, table drop should be skipped
            # CREATE_TABLE event too should be skipped since
            # table already exists
            skipped_events_count_after = \
                EventProcessorUtils.get_num_skipped_events()
            assert skipped_events_count_after == \
                   skipped_events_count_before + 2

            # drop table, we will recreate it in test 2
            catalog_hms_client.drop_table(db_name, tbl_name, True)
            EventProcessorUtils.wait_for_event_processing(self, 20)

            # Test scenario 2 - create table and immediately drop it

            # get skipped events count so far
            skipped_events_count_before = \
                EventProcessorUtils.get_num_skipped_events()

            # create managed table
            create_tbl_query = "create table " + full_table_name + \
                               "(c1 int) partitioned by (part_col int) "
            self.execute_query_expect_success(self.client, create_tbl_query)

            # Immediately drop the table before CREATE_TABLE
            # event is processed by event processor
            catalog_hms_client.drop_table(db_name, tbl_name, True)

            # wait for event processor to process all events
            EventProcessorUtils.wait_for_event_processing(self, 20)

            # When event processor processes
            # 1. CREATE_TABLE: It should be skipped since
            # drop table's event id  > create_table event id
            # 2. DROP_TABLE: It should be skipped too since table
            # does not exist in cache anymore.
            skipped_events_count_after = \
                EventProcessorUtils.get_num_skipped_events()
            assert skipped_events_count_after == \
                   skipped_events_count_before + 2

        finally:
            if catalog_hms_client is not None:
                catalog_hms_client.shutdown()
            if self.__get_database_no_throw(db_name) is not None:
                self.hive_client.drop_database(db_name, True, True)

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--fallback_to_hms_on_errors=true "
                      "--invalidate_hms_cache_on_ddls=true "
                      "--enable_sync_to_latest_event_on_ddls=false "
                      "--hms_event_polling_interval_s=5"
    )
    def test_database_create_drop_seq(self):
        """
        The test covers two scenarios:
        1. Drop db from metastore server and
           immediately create it before drop and
           create events are processed by event
           processor
        2. Create db and immediately drop it
           before the events are processed by event
           processor
        """
        catalog_hms_client = None
        db_name = ImpalaTestSuite.get_random_name(
            "test_database_create_drop_seq_db_")
        try:
            catalog_hms_client, hive_transport = \
                ImpalaTestSuite.create_hive_client(5899)
            assert catalog_hms_client is not None
            create_db_query = "create database " + db_name
            self.execute_query_expect_success(self.client,
                                              create_db_query)

            database = catalog_hms_client.get_database(db_name)
            assert database is not None
            assert db_name == database.name

            # Test scenario 1 - drop database and immediately create it

            # wait for event processor to sync till latest HMS event
            # before issuing drop database
            EventProcessorUtils.wait_for_event_processing(self, 20)
            # get skipped events count so far
            skipped_events_count_before = \
                EventProcessorUtils.get_num_skipped_events()
            catalog_hms_client.drop_database(db_name, False, False)
            # immediately create new database with same name via
            # Impala shell
            self.execute_query_expect_success(self.client,
                                              create_db_query)
            # wait for event processor to process all events
            EventProcessorUtils.wait_for_event_processing(self, 20)

            skipped_events_count_after = \
                EventProcessorUtils.get_num_skipped_events()
            assert skipped_events_count_after == \
                   skipped_events_count_before + 2

            # drop db. We will recreate it in test 2
            catalog_hms_client.drop_database(db_name, False, False)
            EventProcessorUtils.wait_for_event_processing(self, 20)

            skipped_events_count_before = \
                EventProcessorUtils.get_num_skipped_events()

            # Test scenario 2: create database and immediately drop it
            self.execute_query_expect_success(self.client,
                                              create_db_query)
            # immediately drop db before event processor
            # processes create_database event
            catalog_hms_client.drop_database(db_name, False, False)

            EventProcessorUtils.wait_for_event_processing(self, 20)

            # When event processor processes
            # 1. CREATE_DATABASE: It should be skipped since
            # drop db's event id  > create_db event id
            # 2. DROP_DB: It should be skipped too since db
            # does not exist in cache anymore.
            skipped_events_count_after = \
                EventProcessorUtils.get_num_skipped_events()
            assert skipped_events_count_after == \
                   skipped_events_count_before + 2
        finally:
            if catalog_hms_client is not None:
                catalog_hms_client.shutdown()
            if self.__get_database_no_throw(db_name) is not None:
                self.hive_client.drop_database(db_name, True, True)

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        impalad_args="--use_local_catalog=true",
        catalogd_args="--catalog_topic_mode=minimal "
                      "--start_hms_server=true "
                      "--hms_port=5899 "
                      "--fallback_to_hms_on_errors=true "
    )
    def test_hms_apis(self):
        """
        The test verifies if the following HMS apis are reachable from impala (so that
        MetastoreServiceHandler in impala can talk to HMS through these APIs):
        1.  find_next_compact2
        2.  add_write_notification_log_in_batch
        3.  update_transaction_statistics
        """
        catalog_hms_client = None
        tbl_name = ImpalaTestSuite.get_random_name(
            "test_table_compaction_seq_tbl_")
        try:
            catalog_hms_client, hive_transport = \
                ImpalaTestSuite.create_hive_client(5899)
            assert catalog_hms_client is not None
            # Test 1: verify find_next_compact2 api in HMS
            # create managed table
            self.run_stmt_in_hive("create transactional table default.{0} (c1 int)"
              .format(tbl_name))
            self.run_stmt_in_hive("alter table default.{0} compact 'minor'"
              .format(tbl_name))
            compactRequest = FindNextCompactRequest()
            compactRequest.workerId = "myworker"
            compactRequest.workerVersion = "4.0.0"
            optionalCi = catalog_hms_client.find_next_compact2(compactRequest)
            # If the above call is successful then find_next_compact2 api is reachable.
            # Test 2: verify add_write_notification_log_in_batch api in HMS
            rqstList = list()
            logBatchRequest = \
                WriteNotificationLogBatchRequest("hive", "default", tbl_name, rqstList)
            catalog_hms_client.add_write_notification_log_in_batch(logBatchRequest)
            # If the above call is successful then HMS api
            # add_write_notification_log_in_batch is reachable.

            # Test 3: verify update_transaction_statistics api in HMS
            stats_obj = UpdateTransactionalStatsRequest()
            stats_obj.tableId = 1
            stats_obj.insertCount = 1
            stats_obj.updatedCount = 1
            stats_obj.deletedCount = 1
            catalog_hms_client.update_transaction_statistics(stats_obj)
            # If the above call is successful the HMS api
            # update_transaction_statistics is reachable

            catalog_hms_client.drop_table("default", tbl_name, True)
        finally:
            if catalog_hms_client is not None:
                catalog_hms_client.shutdown()

    def __create_test_tbls_from_hive(self, db_name):
      """Util method to create test tables from hive in the given database. It creates
      4 tables (partitioned and unpartitioned) for non-acid and acid cases and returns
      the valid partition names for the partitioned tables."""
      # create a partitioned table
      # Creating a table from hive and inserting into it takes very long. Hence we create
      # a external table pointing to an existing table location
      tbl_location = self.hive_client.get_table("functional", "alltypessmall").sd.location
      self.run_stmt_in_hive(
        "create external table {0}.{1} like functional.alltypessmall location '{2}'"
          .format(db_name, TestMetastoreService.part_tbl, tbl_location))
      self.run_stmt_in_hive(
        "msck repair table {0}.{1}".format(db_name, TestMetastoreService.part_tbl))
      # TODO create a acid partitioned table
      tbl_location = self.hive_client.get_table("functional", "tinytable").sd.location
      self.run_stmt_in_hive(
        "create external table {0}.{1} like functional.tinytable location '{2}'"
          .format(db_name, TestMetastoreService.unpart_tbl, tbl_location))
      self.run_stmt_in_hive(
        "create table default.{0} (c1 int)"
          .format(TestMetastoreService.default_unknowntbl))

    @classmethod
    def __run_get_table_tests(cls, catalog_hms_client, db_name, tbl_name,
                              fallback_expected=False):
      """
      Issues get_table_req for various cases and validates the response.
      """
      # Test simple get_table_req without stats.
      get_table_request = GetTableRequest()
      get_table_request.dbName = db_name
      get_table_request.tblName = tbl_name
      get_table_response = catalog_hms_client.get_table_req(get_table_request)
      assert get_table_response.table.dbName == db_name
      assert get_table_response.table.tableName == tbl_name
      # Request did not ask for stats, verify colStats are not populated.
      assert get_table_response.table.colStats is None

      # Test get_table_request with stats and engine set to Impala.
      get_table_request.getColumnStats = True
      get_table_request.engine = "impala"
      get_table_response = catalog_hms_client.get_table_req(get_table_request)
      assert get_table_response.table.dbName == db_name
      assert get_table_response.table.tableName == tbl_name
      assert get_table_response.table.colStats is not None
      # Verify the column stats objects are populated for
      # non-clustering columns.
      assert len(get_table_response.table.colStats.statsObj) > 0

      # We only return Impala table level column stats but engine field is
      # required to be consistent with Hive's implementation.
      get_table_request.engine = "hive"
      get_table_response = catalog_hms_client.get_table_req(get_table_request)
      assert get_table_response.table.dbName == db_name
      assert get_table_response.table.tableName == tbl_name
      assert get_table_response.table.colStats is not None
      # Verify the column stats objects are populated for
      # non-clustering columns.
      assert len(get_table_response.table.colStats.statsObj) > 0

      # request for tables which are unknown to catalogd should fallback to HMS
      # if fallback_expected if True
      get_table_request.dbName = TestMetastoreService.test_db_name
      get_table_request.tblName = TestMetastoreService.unpart_tbl
      get_table_request.getColumnStats = True

      # Engine is not specified, this should throw an exception even if
      # fallback_to_hms_on_errors is true.
      expected_exception = "isGetColumnStats() is true in the request but " \
                           "engine is not specified."
      try:
        catalog_hms_client.get_table_req(get_table_request)
      except Exception as e:
        if expected_exception is not None:
          assert expected_exception in str(e)

      # Set engine and request impala, this should fallback to HMS since
      # the table is not in catalog cache.
      get_table_request.engine = "Impala"
      if fallback_expected:
        get_table_response = catalog_hms_client.get_table_req(get_table_request)
        assert get_table_response.table.dbName == db_name
        assert get_table_response.table.tableName == tbl_name
      else:
        expected_exception = None
        try:
          catalog_hms_client.get_table_req(get_table_request)
        except Exception as e:
          expected_exception = e
        assert expected_exception is not None

    def __run_partitions_by_names_tests(self, catalog_hms_client, db_name, tbl_name,
                                        valid_part_names, expect_fallback=False):
      """This method runs all the test cases for fetching partitions. The method
      expects that the partition keys are year and month for the given table and
      that there are atleast 3 partitions in the table."""
      # Test get_partition_by_name_req
      get_parts_req = GetPartitionsByNamesRequest()
      get_parts_req.db_name = db_name
      get_parts_req.tbl_name = tbl_name
      # must have more than 3 partitions to test the filtering below
      assert len(valid_part_names) > 3
      part_names = valid_part_names[:3]
      part_name_format = "year={0}/month={1}"
      get_parts_req.names = part_names
      response = catalog_hms_client.get_partitions_by_names_req(get_parts_req)
      assert response.partitions is not None
      assert len(response.partitions) == 3
      self.__validate_partitions(part_names, response, part_name_format)
      # request partitions with file-metadata
      get_parts_req = GetPartitionsByNamesRequest()
      get_parts_req.db_name = db_name
      get_parts_req.tbl_name = tbl_name
      get_parts_req.names = part_names
      get_parts_req.getFileMetadata = True
      response = catalog_hms_client.get_partitions_by_names_req(get_parts_req)
      assert response.partitions is not None
      assert len(response.partitions) == 3
      self.__validate_partitions(part_names, response, part_name_format,
                                 True)
      # request contains unavailable partitions
      get_parts_req = GetPartitionsByNamesRequest()
      get_parts_req.db_name = db_name
      get_parts_req.tbl_name = tbl_name
      part_names = [x for x in valid_part_names]
      part_names[0] = "year=2009/month=13"
      get_parts_req.names = part_names
      get_parts_req.getFileMetadata = True
      assert get_parts_req.names is not None
      response = catalog_hms_client.get_partitions_by_names_req(get_parts_req)
      assert response.partitions is not None
      assert len(response.partitions) == 3
      # remove the bad partname for comparison below
      part_names.pop(0)
      self.__validate_partitions(part_names, response, part_name_format, True)
      # request contains empty partition names
      get_parts_req = GetPartitionsByNamesRequest()
      get_parts_req.db_name = db_name
      get_parts_req.tbl_name = tbl_name
      get_parts_req.names = []
      response = catalog_hms_client.get_partitions_by_names_req(get_parts_req)
      assert response.partitions is not None
      assert len(response.partitions) == 0
      # Negative test cases
      # invalid partition keys
      get_parts_req = GetPartitionsByNamesRequest()
      get_parts_req.db_name = db_name
      get_parts_req.tbl_name = tbl_name
      get_parts_req.names = ["invalid-key=1"]
      response = catalog_hms_client.get_partitions_by_names_req(get_parts_req)
      # in this case we replicate what HMS does which is to return 0 partitions
      assert len(response.partitions) == 0
      # empty table name
      get_parts_req = GetPartitionsByNamesRequest()
      get_parts_req.db_name = db_name
      get_parts_req.tbl_name = ""
      get_parts_req.names = []
      if expect_fallback:
        self.__get_parts_by_names_expect_exception(catalog_hms_client, get_parts_req,
                                                   "NoSuchObjectException")
      else:
        self.__get_parts_by_names_expect_exception(catalog_hms_client, get_parts_req,
                                                 "Table name is empty or null")
      # empty db name
      get_parts_req = GetPartitionsByNamesRequest()
      get_parts_req.db_name = ""
      get_parts_req.tbl_name = tbl_name
      get_parts_req.names = []
      if expect_fallback:
        self.__get_parts_by_names_expect_exception(catalog_hms_client, get_parts_req,
                                                   "NoSuchObjectException")
      else:
        self.__get_parts_by_names_expect_exception(catalog_hms_client, get_parts_req,
                                                 "Database name is empty or null")
      # table does not exist
      get_parts_req = GetPartitionsByNamesRequest()
      get_parts_req.db_name = db_name
      get_parts_req.tbl_name = "table-does-not-exist"
      get_parts_req.names = []
      if expect_fallback:
        self.__get_parts_by_names_expect_exception(catalog_hms_client, get_parts_req,
                                                   "InvalidObjectException")
      else:
        self.__get_parts_by_names_expect_exception(catalog_hms_client, get_parts_req,
          "Table {0}.table-does-not-exist not found".format(
            db_name))

    @classmethod
    def __validate_partitions(cls, part_names, get_parts_by_names_result, name_format,
                              expect_files=False):
      """
      Validates that the given list of partitions contains all the partitions from the
      given list of partition names.
      """
      test_part_names = list(part_names)
      if expect_files:
        assert get_parts_by_names_result.dictionary is not None
        # obj_dict will only be populated when the table is on HDFS or OZONE
        # where block locations are available.
        if IS_HDFS or IS_OZONE:
          assert len(get_parts_by_names_result.dictionary.values) > 0
      else:
        assert get_parts_by_names_result.dictionary is None
      partitions = get_parts_by_names_result.partitions
      for part in partitions:
        assert part is not None
        # we create the part name here since partition object only has keys
        assert len(part.values) == 2
        name = name_format.format(part.values[0], part.values[1])
        assert name in test_part_names
        if expect_files:
          assert part.fileMetadata is not None
          assert part.fileMetadata.data is not None
        else:
          assert part.fileMetadata is None
        test_part_names.remove(name)
      assert len(test_part_names) == 0

    def __assert_filemd(self, filemetadata, obj_dict):
      """
      Util method which asserts that the given file-metadata is valid.
      """
      assert filemetadata is not None
      assert filemetadata.data is not None
      assert obj_dict is not None
      # obj_dict will only be populated when the table is on HDFS or OZONE
      # where block locations are available.
      if IS_HDFS or IS_OZONE:
        assert len(obj_dict.values) > 0

    def __assert_no_filemd(self, filemetadata, obj_dict):
      """
      Util method which asserts that the given file-metadata not valid. Used to verify
      that the HMS response objects do not contain the file-metadata.
      """
      assert filemetadata is None
      assert obj_dict is None

    @classmethod
    def __call_get_table_req_expect_exception(cls, client,
        get_table_request, expected_exception_str=None):
      exception_recieved = None
      try:
        client.get_table_req(get_table_request)
      except Exception as e:
        exception_recieved = e
        if expected_exception_str is not None:
          assert expected_exception_str in str(e)
      assert exception_recieved is not None

    @classmethod
    def __get_parts_by_names_expect_exception(self, catalog_hms_client,
        request, expected_exception_str=None):
      """
      Calls get_partitions_by_names_req on the HMS client and expects
      it to call with the exception msg as provided in expected_exception_str
      """
      exception = None
      try:
        catalog_hms_client.get_partitions_by_names_req(request)
      except Exception as e:
        exception = e
        if expected_exception_str is not None:
          assert expected_exception_str in str(e)
      assert exception is not None

    def __compare_cols(self, cols, fieldSchemas, compareComments=True):
        """
        Compares the given list of fieldSchemas with the expected cols
        """
        assert len(cols) == len(fieldSchemas)
        for i in range(len(cols)):
            assert cols[i][0] == fieldSchemas[i].name
            assert cols[i][1] == fieldSchemas[i].type
            if compareComments:
                assert cols[i][2] == fieldSchemas[i].comment

    def __get_test_database(self, db_name,
        description="test_db_for_metastore_service"):
        db = Database()
        db.name = db_name
        db.desription = description
        return db

    def __get_test_tbl(self, db_name, tbl_name, cols, part_cols=None):
        tbl = Table()
        tbl.dbName = db_name
        tbl.tableName = tbl_name
        sd = StorageDescriptor()
        tbl.sd = sd
        sd.cols = self.__create_field_schemas(cols)
        sd.inputFormat = \
          "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        sd.outputFormat = \
          "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        if part_cols is not None:
            tbl.partitionKeys = self.__create_field_schemas(part_cols)
        serde = SerDeInfo()
        serde.name = ""
        serde.serializationLib = \
          "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        tbl.sd.serdeInfo = serde
        return tbl

    def __create_field_schemas(self, cols):
        fieldSchemas = []
        for col in cols:
            f = FieldSchema()
            f.name = col[0]
            f.type = col[1]
            f.comment = col[2]
            fieldSchemas.append(f)
        return fieldSchemas

    def __deepCopyTable(self, other):
        tbl = Table()
        tbl.tableName = other.tableName
        tbl.dbName = other.dbName
        tbl.owner = other.owner
        tbl.createTime = other.createTime
        tbl.lastAccessTime = other.lastAccessTime
        tbl.retention = other.retention
        tbl.sd = other.sd
        tbl.partitionKeys = other.partitionKeys
        tbl.parameters = other.parameters
        tbl.viewOriginalText = other.viewOriginalText
        tbl.viewExpandedText = other.viewExpandedText
        tbl.tableType = other.tableType
        tbl.privileges = other.privileges
        tbl.temporary = other.temporary
        tbl.rewriteEnabled = other.rewriteEnabled
        tbl.creationMetadata = other.creationMetadata
        tbl.catName = other.catName
        tbl.ownerType = other.ownerType
        tbl.writeId = other.writeId
        tbl.isStatsCompliant = other.isStatsCompliant
        tbl.colStats = other.colStats
        tbl.accessType = other.accessType
        tbl.requiredReadCapabilities = other.requiredReadCapabilities
        tbl.requiredWriteCapabilities = other.requiredWriteCapabilities
        tbl.id = other.id
        tbl.fileMetadata = other.fileMetadata
        tbl.dictionary = other.dictionary
        return tbl
