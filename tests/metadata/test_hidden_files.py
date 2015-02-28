# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from subprocess import check_call

TEST_DB = 'hidden_files_db'
TEST_TBL = 'hf'

@pytest.mark.skipif(os.getenv("TARGET_FILESYSTEM") == "s3", reason="Disabled on s3")
class TestHiddenFiles(ImpalaTestSuite):
  """
  Tests that files with special prefixes/suffixes are considered 'hidden' when
  loading table metadata and running queries.
  """

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHiddenFiles, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))
    # Only run in exhaustive mode since this test takes a long time.
    #if cls.exploration_strategy() != 'exhaustive':
    #  cls.TestMatrix.clear()

  def setup_method(self, method):
    self.cleanup_db(TEST_DB)
    self.client.execute("create database " + TEST_DB);
    self.client.execute(
      "create table %s.%s like functional.alltypes" % (TEST_DB, TEST_TBL))
    self.client.execute(
      "alter table %s.%s add partition (year=2010, month=1)" % (TEST_DB, TEST_TBL))
    self.client.execute(
      "alter table %s.%s add partition (year=2010, month=2)" % (TEST_DB, TEST_TBL))
    self.__populate_test_table()

  def teardown_method(self, method):
    self.cleanup_db(TEST_DB)

  def __populate_test_table(self):
    """Copy files into the HDFS directories of two partitions of the table.
    The goal is to have both an empty and non-empty partition with hidden files."""

    tbl_loc = "/test-warehouse/%s.db/%s" % (TEST_DB, TEST_TBL)

    # Copy a visible file into one of the partitions.
    check_call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=1/100101.txt",
          "%s/year=2010/month=1/100101.txt" % tbl_loc], shell=False)
    # Add hidden files to the non-empty partition. Use upper case hidden suffixes.
    check_call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=1/100101.txt",
          "%s/year=2010/month=1/.100101.txt" % tbl_loc], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=1/100101.txt",
          "%s/year=2010/month=1/_100101.txt" % tbl_loc], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=1/100101.txt",
          "%s/year=2010/month=1/100101.txt.COPYING" % tbl_loc], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=1/100101.txt",
          "%s/year=2010/month=1/100101.txt.TMP" % tbl_loc], shell=False)
    # Add hidden files to the empty partition. Use lower case hidden suffixes.
    check_call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=2/100201.txt",
          "%s/year=2010/month=2/.100201.txt" % tbl_loc], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=2/100201.txt",
          "%s/year=2010/month=2/_100201.txt" % tbl_loc], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=2/100201.txt",
          "%s/year=2010/month=2/100201.txt.copying" % tbl_loc], shell=False)
    check_call(["hadoop", "fs", "-cp",
          "/test-warehouse/alltypes/year=2010/month=2/100201.txt",
          "%s/year=2010/month=2/100201.txt.tmp" % tbl_loc], shell=False)

  @pytest.mark.execute_serially
  def test_hidden_files_load(self, vector):
    """Tests that an incremental refresh ignores hidden files."""
    self.client.execute("invalidate metadata %s.hf" % TEST_DB)
    self.run_test_case('QueryTest/hidden-files', vector)

  @pytest.mark.execute_serially
  def test_hidden_files_refresh(self, vector):
    """Tests that an incremental refresh ignores hidden files."""
    self.client.execute("refresh %s.hf" % TEST_DB)
    self.run_test_case('QueryTest/hidden-files', vector)
