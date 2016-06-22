# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Impala tests for queries that query metadata and set session settings

import pytest

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfIsilon, SkipIfS3, SkipIfLocal
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.util.filesystem_utils import get_fs_path

# TODO: For these tests to pass, all table metadata must be created exhaustively.
# the tests should be modified to remove that requirement.
class TestMetadataQueryStatements(ImpalaTestSuite):

  CREATE_DATA_SRC_STMT = ("CREATE DATA SOURCE %s LOCATION '" +
      get_fs_path("/test-warehouse/data-sources/test-data-source.jar") +
      "' CLASS 'com.cloudera.impala.extdatasource.AllTypesDataSource' API_VERSION 'V1'")
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

    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=ALL_NODES_ONLY,
        disable_codegen_options=[False],
        batch_sizes=[0],
        sync_ddl=sync_ddl_opts))
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def test_use(self, vector):
    self.run_test_case('QueryTest/use', vector)

  def test_show(self, vector):
    self.run_test_case('QueryTest/show', vector)

  def test_show_stats(self, vector):
    self.run_test_case('QueryTest/show-stats', vector, "functional")

  def test_describe_path(self, vector, unique_database):
    self.run_test_case('QueryTest/describe-path', vector, unique_database)

  # Missing Coverage: Describe formatted compatibility between Impala and Hive when the
  # data doesn't reside in hdfs.
  @SkipIfIsilon.hive
  @SkipIfS3.hive
  @SkipIfLocal.hive
  def test_describe_formatted(self, vector, unique_database):
    # Describe a partitioned table.
    self.exec_and_compare_hive_and_impala_hs2("describe formatted functional.alltypes")
    self.exec_and_compare_hive_and_impala_hs2(
        "describe formatted functional_text_lzo.alltypes")
    # Describe an unpartitioned table.
    self.exec_and_compare_hive_and_impala_hs2("describe formatted tpch.lineitem")
    self.exec_and_compare_hive_and_impala_hs2("describe formatted functional.jointbl")

    # Create and describe an unpartitioned and partitioned Avro table created
    # by Impala without any column definitions.
    # TODO: Instead of creating new tables here, change one of the existing
    # Avro tables to be created without any column definitions.
    self.client.execute("create database if not exists %s" % unique_database)
    self.client.execute((
        "create table %s.%s with serdeproperties ('avro.schema.url'='%s') stored as avro"
        % (unique_database, "avro_alltypes_nopart", self.AVRO_SCHEMA_LOC)))
    self.exec_and_compare_hive_and_impala_hs2("describe formatted avro_alltypes_nopart")

    self.client.execute((
        "create table %s.%s partitioned by (year int, month int) "
        "with serdeproperties ('avro.schema.url'='%s') stored as avro"
        % (unique_database, "avro_alltypes_part", self.AVRO_SCHEMA_LOC)))
    self.exec_and_compare_hive_and_impala_hs2("describe formatted avro_alltypes_part")

    try:
      # Describe a view
      self.exec_and_compare_hive_and_impala_hs2(\
          "describe formatted functional.alltypes_view_sub")
    except AssertionError:
      pytest.xfail("Investigate minor difference in displaying null vs empty values")

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

  @SkipIfS3.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  @pytest.mark.execute_serially # because of invalidate metadata
  def test_describe_db(self, vector):
    self.__test_describe_db_cleanup()
    try:
      self.client.execute("create database impala_test_desc_db1")
      self.client.execute("create database impala_test_desc_db2 "
                          "comment 'test comment'")
      self.client.execute("create database impala_test_desc_db3 "
                          "location '" + get_fs_path("/testdb") + "'")
      self.client.execute("create database impala_test_desc_db4 comment 'test comment' "
                          "location \"" + get_fs_path("/test2.db") + "\"")
      self.run_stmt_in_hive("create database hive_test_desc_db comment 'test comment' "
                           "with dbproperties('pi' = '3.14', 'e' = '2.82')")
      self.run_stmt_in_hive("alter database hive_test_desc_db set owner user test")
      self.client.execute("invalidate metadata")
      self.run_test_case('QueryTest/describe-db', vector)
    finally:
      self.__test_describe_db_cleanup()

  def __test_describe_db_cleanup(self):
    self.cleanup_db('hive_test_desc_db')
    self.cleanup_db('impala_test_desc_db1')
    self.cleanup_db('impala_test_desc_db2')
    self.cleanup_db('impala_test_desc_db3')
    self.cleanup_db('impala_test_desc_db4')

  # Missing Coverage: ddl by hive being visible to Impala for data not residing in hdfs.
  @SkipIfIsilon.hive
  @SkipIfS3.hive
  @SkipIfLocal.hive
  @pytest.mark.execute_serially # because of invalidate metadata
  def test_impala_sees_hive_tables_and_dbs(self, vector):
    self.client.set_configuration(vector.get_value('exec_option'))
    DB_NAME = 'hive_test_db'
    TBL_NAME = 'testtbl'
    self.cleanup_db(DB_NAME)
    try:
      self.__run_test_impala_sees_hive_tables_and_dbs(DB_NAME, TBL_NAME)
    finally:
      self.cleanup_db(DB_NAME)

  def __run_test_impala_sees_hive_tables_and_dbs(self, db_name, tbl_name):
    assert db_name not in self.all_db_names()

    self.run_stmt_in_hive("create database %s" % db_name)

    # Run 'invalidate metadata <table name>' when the parent database does not exist.
    try:
      self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
      assert 0, 'Expected to fail'
    except ImpalaBeeswaxException as e:
      assert "TableNotFoundException: Table not found: %s.%s"\
          % (db_name, tbl_name) in str(e)

    assert db_name not in self.all_db_names()

    # Create a table external to Impala.
    self.run_stmt_in_hive("create table %s.%s (i int)" % (db_name, tbl_name))

    # Impala does not know about this database or table.
    assert db_name not in self.all_db_names()

    # Run 'invalidate metadata <table name>'. It should add the database and table
    # in to Impala's catalog.
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
    assert db_name in self.all_db_names()

    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name in result.data
    assert len(result.data) == 1

    self.client.execute("create table %s.%s (j int)" % (db_name, tbl_name + "_test"))
    self.run_stmt_in_hive("drop table %s.%s" % (db_name, tbl_name + "_test"))

    # Re-create the table in Hive. Use the same name, but different casing.
    self.run_stmt_in_hive("create table %s.%s (i bigint)" % (db_name, tbl_name + "_TEST"))
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name + "_Test"))
    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name + "_test" in result.data
    assert tbl_name + "_Test" not in result.data
    assert tbl_name + "_TEST" not in result.data

    # Verify this table is the version created in Hive (the column should be BIGINT)
    result = self.client.execute("describe %s.%s" % (db_name, tbl_name + '_test'))
    assert 'bigint' in result.data[0]

    self.client.execute("drop table %s.%s" % (db_name, tbl_name + "_TEST"))

    # Make sure we can actually use the table
    self.client.execute(("insert overwrite table %s.%s "
                        "select 1 from functional.alltypes limit 5"
                         % (db_name, tbl_name)))
    result = self.execute_scalar("select count(*) from %s.%s" % (db_name, tbl_name))
    assert int(result) == 5

    # Should be able to call invalidate metadata multiple times on the same table.
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
    self.client.execute("refresh %s.%s"  % (db_name, tbl_name))
    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name in result.data

    # Can still use the table.
    result = self.execute_scalar("select count(*) from %s.%s" % (db_name, tbl_name))
    assert int(result) == 5

    # Run 'invalidate metadata <table name>' when no table exists with that name.
    try:
      self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name + '2'))
      assert 0, 'Expected to fail'
    except ImpalaBeeswaxException as e:
      assert "TableNotFoundException: Table not found: %s.%s"\
          % (db_name, tbl_name + '2') in str(e)

    result = self.client.execute("show tables in %s" % db_name);
    assert len(result.data) == 1
    assert tbl_name in result.data

    # Create another table
    self.run_stmt_in_hive("create table %s.%s (i int)" % (db_name, tbl_name + '2'))
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name + '2'))
    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name + '2' in result.data
    assert tbl_name in result.data

    # Drop the table, and then verify invalidate metadata <table name> removes the
    # table from the catalog.
    self.run_stmt_in_hive("drop table %s.%s " % (db_name, tbl_name))
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name + '2' in result.data
    assert tbl_name not in result.data

    # Should be able to call invalidate multiple times on the same table when the table
    # does not exist.
    try:
      self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
      assert 0, 'Expected to fail'
    except ImpalaBeeswaxException as e:
      assert "TableNotFoundException: Table not found: %s.%s"\
          % (db_name, tbl_name) in str(e)

    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name + '2' in result.data
    assert tbl_name not in result.data

    # Drop the parent database (this will drop all tables). Then invalidate the table
    self.run_stmt_in_hive("drop database %s CASCADE" % db_name)
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name + '2'))
    result = self.client.execute("show tables in %s" % db_name);
    assert len(result.data) == 0

    # Requires a refresh to see the dropped database
    assert db_name in self.all_db_names()

    self.client.execute("invalidate metadata")
    assert db_name not in self.all_db_names()
