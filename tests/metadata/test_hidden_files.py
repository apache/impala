# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

from subprocess import check_call

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.util.filesystem_utils import WAREHOUSE, IS_S3

class TestHiddenFiles(ImpalaTestSuite):
  """
  Tests that files with special prefixes/suffixes are considered 'hidden' when
  loading table metadata and running queries.
  """

  # The .test file run in these tests relies this table name.
  TBL_NAME = "test_hidden_files"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHiddenFiles, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))
    # Only run in exhaustive mode on hdfs since this test takes a long time.
    if cls.exploration_strategy() != 'exhaustive' and not IS_S3:
      cls.TestMatrix.clear()

  def __prepare_test_table(self, db_name, tbl_name):
    """Creates a test table with two partitions, and copies files into the HDFS
    directories of the two partitions. The goal is to have both an empty and non-empty
    partition with hidden files."""

    self.client.execute(
      "create table %s.%s like functional.alltypes" % (db_name, tbl_name))
    self.client.execute(
      "alter table %s.%s add partition (year=2010, month=1)" % (db_name, tbl_name))
    self.client.execute(
      "alter table %s.%s add partition (year=2010, month=2)" % (db_name, tbl_name))

    ALLTYPES_LOC = "%s/alltypes" % WAREHOUSE
    TEST_TBL_LOC = "%s/%s.db/%s" % (WAREHOUSE, db_name, tbl_name)
    # Copy a visible file into one of the partitions.
    check_call(["hadoop", "fs", "-cp",
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/100101.txt" % TEST_TBL_LOC], shell=False)
    # Add hidden files to the non-empty partition. Use upper case hidden suffixes.
    check_call(["hadoop", "fs", "-cp",
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/.100101.txt" % TEST_TBL_LOC], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/_100101.txt" % TEST_TBL_LOC], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/100101.txt.COPYING" % TEST_TBL_LOC], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/100101.txt.TMP" % TEST_TBL_LOC], shell=False)
    # Add hidden files to the empty partition. Use lower case hidden suffixes.
    check_call(["hadoop", "fs", "-cp",
          "%s/year=2010/month=2/100201.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=2/.100201.txt" % TEST_TBL_LOC], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "%s/year=2010/month=2/100201.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=2/_100201.txt" % TEST_TBL_LOC], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "%s/year=2010/month=2/100201.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=2/100201.txt.copying" % TEST_TBL_LOC], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "%s/year=2010/month=2/100201.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=2/100201.txt.tmp" % TEST_TBL_LOC], shell=False)

  def test_hidden_files_load(self, vector, unique_database):
    """Tests that an incremental refresh ignores hidden files."""
    self.__prepare_test_table(unique_database, self.TBL_NAME)
    self.client.execute("invalidate metadata %s.%s" % (unique_database, self.TBL_NAME))
    self.run_test_case('QueryTest/hidden-files', vector, unique_database)

  # This test runs on one dimension. Therefore, running in it parallel is safe, given no
  # other method in this test class is run.
  def test_hidden_files_refresh(self, vector, unique_database):
    """Tests that an incremental refresh ignores hidden files."""
    self.__prepare_test_table(unique_database, self.TBL_NAME)
    self.client.execute("refresh %s.%s" % (unique_database, self.TBL_NAME))
    self.run_test_case('QueryTest/hidden-files', vector, unique_database)
