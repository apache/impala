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
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    # Only run in exhaustive mode on hdfs since this test takes a long time.
    if cls.exploration_strategy() != 'exhaustive' and not IS_S3:
      cls.ImpalaTestMatrix.clear()

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
    self.filesystem_client.copy(
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/100101.txt" % TEST_TBL_LOC, overwrite=True)
    # Add hidden files to the non-empty partition. Use upper case hidden suffixes.
    self.filesystem_client.copy(
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/.100101.txt" % TEST_TBL_LOC, overwrite=True)
    self.filesystem_client.copy(
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/_100101.txt" % TEST_TBL_LOC, overwrite=True)
    self.filesystem_client.copy(
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/100101.txt.COPYING" % TEST_TBL_LOC, overwrite=True)
    self.filesystem_client.copy(
          "%s/year=2010/month=1/100101.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=1/100101.txt.TMP" % TEST_TBL_LOC, overwrite=True)
    # Add hidden files to the empty partition. Use lower case hidden suffixes.
    self.filesystem_client.copy(
          "%s/year=2010/month=2/100201.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=2/.100201.txt" % TEST_TBL_LOC, overwrite=True)
    self.filesystem_client.copy(
          "%s/year=2010/month=2/100201.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=2/_100201.txt" % TEST_TBL_LOC, overwrite=True)
    self.filesystem_client.copy(
          "%s/year=2010/month=2/100201.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=2/100201.txt.copying" % TEST_TBL_LOC, overwrite=True)
    self.filesystem_client.copy(
          "%s/year=2010/month=2/100201.txt" % ALLTYPES_LOC,
          "%s/year=2010/month=2/100201.txt.tmp" % TEST_TBL_LOC, overwrite=True)

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
