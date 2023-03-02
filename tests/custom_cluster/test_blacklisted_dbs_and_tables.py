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


class TestBlacklistedDbsAndTables(CustomClusterTestSuite):
  """Test for db and table blacklist."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  def __expect_error_in_result(self, stmt, expected_err):
    # Drop db/table/view statements won't fail if they contains IF EXISTS. Instead,
    # the error message is returned as results.
    result = self.execute_query(stmt)
    assert expected_err in result.get_data()

  def __expect_error_in_query(self, stmt, expected_err):
    err = self.execute_query_expect_failure(self.client, stmt)
    assert expected_err in str(err)

  def __check_db_not_visible(self, db):
    result = self.hive_client.get_database(db)
    assert result is not None
    self.__expect_error_in_query("describe database %s" % db,
                                 "Database does not exist: %s" % db)
    # Check blacklisted dbs are not shown when querying the database list
    result = self.execute_query("show databases")
    assert db not in result.data

  def __check_table_not_visible(self, db, table):
    result = self.hive_client.get_table(db, table)
    assert result is not None
    self.__expect_error_in_query("describe %s.%s" % (db, table),
                                 "Could not resolve path: '%s.%s'" % (db, table))
    self.__expect_error_in_query("invalidate metadata %s.%s" % (db, table),
                                 "Table not found: %s.%s" % (db, table))
    # Check blacklisted tables are not shown when querying the table list
    result = self.execute_query("show tables in %s" % db)
    assert table not in result.data

  def __check_create_drop_table(self, use_fully_qualified_table_name=True):
    tbl_prefix = "functional." if use_fully_qualified_table_name else ""
    # Check creating table/view with blacklisted table name
    self.__expect_error_in_query(
      "create table {0}alltypes (i int)".format(tbl_prefix),
      "Invalid table/view name: functional.alltypes")
    self.__expect_error_in_query(
      "create table {0}alltypes as select 1".format(tbl_prefix),
      "Invalid table/view name: functional.alltypes")
    self.__expect_error_in_query(
      "create table {0}alltypes like functional.alltypestiny".format(tbl_prefix),
      "Invalid table/view name: functional.alltypes")
    self.__expect_error_in_query(
      "create view {0}alltypes as select 1".format(tbl_prefix),
      "Invalid table/view name: functional.alltypes")

    # Check dropping table/view with blacklisted table name
    self.__expect_error_in_result(
      "drop table if exists {0}alltypes".format(tbl_prefix),
      "Can't drop blacklisted table: functional.alltypes")
    self.__expect_error_in_result(
      "drop view if exists {0}alltypes".format(tbl_prefix),
      "Can't drop blacklisted table: functional.alltypes")

    # Check renaming table/view to blacklisted table name
    self.__expect_error_in_query(
      "alter table functional.alltypestiny rename to functional.alltypes",
      "Invalid table/view name: functional.alltypes")
    self.__expect_error_in_query(
      "alter view functional.alltypes_view rename to functional.alltypes",
      "Invalid table/view name: functional.alltypes")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--blacklisted_dbs=functional_rc,functional_seq "
                 "--blacklisted_tables=functional.alltypes,functional_parquet.alltypes",
    catalogd_args="--blacklisted_dbs=functional_rc,functional_seq "
                  "--blacklisted_tables=functional.alltypes,functional_parquet.alltypes")
  def test_blacklisted_dbs_and_tables(self, vector):
    self.__check_db_not_visible("functional_rc")
    self.__check_db_not_visible("functional_seq")
    self.__check_table_not_visible("functional", "alltypes")
    self.__check_table_not_visible("functional_parquet", "alltypes")

    # Check blacklisted dbs/tables not appear after INVALIDATE METADATA
    self.execute_query("INVALIDATE METADATA")
    self.__check_db_not_visible("functional_rc")
    self.__check_db_not_visible("functional_seq")
    self.__check_table_not_visible("functional", "alltypes")
    self.__check_table_not_visible("functional_parquet", "alltypes")

    # Check creating/dropping blacklisted database
    self.__expect_error_in_query(
      "create database functional_rc",
      "Invalid db name: functional_rc")
    self.__expect_error_in_query(
      "create database if not exists functional_rc",
      "Invalid db name: functional_rc")
    self.__expect_error_in_query(
      "drop database functional_rc",
      "Database does not exist: functional_rc")
    self.__expect_error_in_result(
      "drop database if exists functional_rc",
      "Can't drop blacklisted database: functional_rc")

    self.__check_create_drop_table(use_fully_qualified_table_name=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--blacklisted_tables=alltypes_def,functional.alltypes",
    catalogd_args="--blacklisted_tables=alltypes_def,functional.alltypes")
  def test_resolving_default_database(self, vector):
    # Check that "alltypes_def" is resolved as "default.alltypes_def"
    table = self.hive_client.get_table("functional", "alltypes")
    table.dbName = "default"
    table.tableName = "alltypes_def"
    self.hive_client.create_table(table)
    self.__check_table_not_visible("default", "alltypes_def")
    self.hive_client.drop_table("default", "alltypes_def", True)

    # Check non fully qualified table names are recognized correctly
    self.change_database(self.client, db_name="functional")
    self.__check_create_drop_table(use_fully_qualified_table_name=False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--blacklisted_dbs=functional_rc "
                  "--blacklisted_tables=functional.alltypes",
    impalad_args="--blacklisted_dbs=functional_seq "
                  "--blacklisted_tables=functional.alltypestiny")
  def test_inconsistent_blacklist(self, vector):
    """Test the error handling when blacklists are accidentally set differently between
    coordinators and the catalogd"""
    self.__expect_error_in_query(
      "create database functional_rc",
      "Can't create blacklisted database: functional_rc")
    self.__expect_error_in_query(
      "refresh functions functional_rc",
      "Can't refresh functions in blacklisted database: functional_rc")
    self.__expect_error_in_result(
      "drop database if exists functional_rc",
      "Can't drop blacklisted database: functional_rc")
    self.__expect_error_in_query(
      "create table functional.alltypes (i int)",
      "Can't create blacklisted table: functional.alltypes")
    self.__expect_error_in_query(
      "create table if not exists functional.alltypes (i int)",
      "Can't create blacklisted table: functional.alltypes")
    self.__expect_error_in_query(
      "create table functional.alltypes as select 1",
      "Can't create blacklisted table: functional.alltypes")
    self.__expect_error_in_query(
      "create table functional.alltypes like functional.alltypestiny",
      "Can't create blacklisted table: functional.alltypes")
    self.__expect_error_in_query(
      "create view functional.alltypes as select 1",
      "Can't create view with blacklisted table name: functional.alltypes")
    self.__expect_error_in_query(
      "alter table functional.alltypesagg rename to functional.alltypes",
      "Can't rename to blacklisted table name: functional.alltypes")
    self.__expect_error_in_query(
      "alter view functional.alltypes_view rename to functional.alltypes",
      "Can't rename to blacklisted table name: functional.alltypes")
