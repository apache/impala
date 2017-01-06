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

# Functional tests for LOAD DATA statements.

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.common.skip import SkipIfLocal
from tests.util.filesystem_utils import WAREHOUSE

TEST_TBL_PART = "test_load"
TEST_TBL_NOPART = "test_load_nopart"
STAGING_PATH = 'test-warehouse/test_load_staging'
ALLTYPES_PATH = "test-warehouse/alltypes/year=2010/month=1/100101.txt"
MULTIAGG_PATH = 'test-warehouse/alltypesaggmultifiles/year=2010/month=1/day=1'
HIDDEN_FILES = ["{0}/3/.100101.txt".format(STAGING_PATH),
                "{0}/3/_100101.txt".format(STAGING_PATH)]

@SkipIfLocal.hdfs_client
class TestLoadData(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLoadData, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def _clean_test_tables(self):
    self.client.execute("drop table if exists functional.{0}".format(TEST_TBL_NOPART))
    self.client.execute("drop table if exists functional.{0}".format(TEST_TBL_PART))
    self.filesystem_client.delete_file_dir(STAGING_PATH, recursive=True)

  def teardown_method(self, method):
    self._clean_test_tables()

  def setup_method(self, method):
    # Defensively clean the data dirs if they exist.
    self._clean_test_tables()

    # Create staging directories for load data inpath. The staging directory is laid out
    # as follows:
    #   - It has 6 sub directories, numbered 1-6
    #   - The directories are populated with files from a subset of partitions in
    #     existing partitioned tables.
    #   - Sub Directories 1-4 have single files copied from alltypes/
    #   - Sub Directories 5-6 have multiple files (4) copied from alltypesaggmultifiles
    #   - Sub Directory 3 also has hidden files, in both supported formats.
    #   - All sub-dirs contain a hidden directory
    for i in xrange(1, 6):
      stagingDir = '{0}/{1}'.format(STAGING_PATH, i)
      self.filesystem_client.make_dir(stagingDir, permission=777)
      self.filesystem_client.make_dir('{0}/_hidden_dir'.format(stagingDir),
                                      permission=777)
    # Copy single file partitions from alltypes.
    for i in xrange(1, 4):
      self.filesystem_client.copy(ALLTYPES_PATH,
                               "{0}/{1}/100101.txt".format(STAGING_PATH, i))
    # Copy multi file partitions from alltypesaggmultifiles.
    file_names = self.filesystem_client.ls(MULTIAGG_PATH)
    for i in xrange(4, 6):
      for file_ in file_names:
        self.filesystem_client.copy(
            "{0}/{1}".format(MULTIAGG_PATH, file_),
            '{0}/{1}/{2}'.format(STAGING_PATH, i, file_))

    # Create two hidden files, with a leading . and _
    for file_ in HIDDEN_FILES:
      self.filesystem_client.copy(ALLTYPES_PATH, file_)

    # Create both the test tables.
    self.client.execute("create table functional.{0} like functional.alltypes"
        " location '{1}/{0}'".format(TEST_TBL_PART, WAREHOUSE))
    self.client.execute("create table functional.{0} like functional.alltypesnopart"
        " location '{1}/{0}'".format(TEST_TBL_NOPART, WAREHOUSE))

  def test_load(self, vector):
    self.run_test_case('QueryTest/load', vector)
    # The hidden files should not have been moved as part of the load operation.
    for file_ in HIDDEN_FILES:
      assert self.filesystem_client.exists(file_), "{0} does not exist".format(file_)
