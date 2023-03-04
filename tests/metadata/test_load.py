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

from __future__ import absolute_import, division, print_function
from builtins import range
import time
from beeswaxd.BeeswaxService import QueryState
from copy import deepcopy
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_client_protocol_dimension,
    create_exec_option_dimension,
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.common.skip import SkipIfLocal
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import get_fs_path, WAREHOUSE, IS_HDFS

TEST_TBL_PART = "test_load"
TEST_TBL_NOPART = "test_load_nopart"
TEST_TBL_NOPART_EXT = "test_load_nopart_ext"
STAGING_PATH = '%s/test_load_staging' % WAREHOUSE
ALLTYPES_PATH = "%s/alltypes/year=2010/month=1/100101.txt" % WAREHOUSE
MULTIAGG_PATH = '%s/alltypesaggmultifiles/year=2010/month=1/day=1' % WAREHOUSE
HIDDEN_FILES = ["{0}/3/.100101.txt".format(STAGING_PATH),
                "{0}/3/_100101.txt".format(STAGING_PATH)]
# A path outside WAREHOUSE, which will be a different bucket for Ozone/ofs.
TMP_STAGING_PATH = get_fs_path('/tmp/test_load_staging')

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
    for i in range(1, 6):
      stagingDir = '{0}/{1}'.format(STAGING_PATH, i)
      self.filesystem_client.make_dir(stagingDir, permission=777)
      self.filesystem_client.make_dir('{0}/_hidden_dir'.format(stagingDir),
                                      permission=777)
    # Copy single file partitions from alltypes.
    for i in range(1, 4):
      self.filesystem_client.copy(ALLTYPES_PATH,
                               "{0}/{1}/100101.txt".format(STAGING_PATH, i))
    # Copy multi file partitions from alltypesaggmultifiles.
    file_names = self.filesystem_client.ls(MULTIAGG_PATH)
    for i in range(4, 6):
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


@SkipIfLocal.hdfs_client
class TestLoadDataExternal(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLoadDataExternal, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def _clean_test_tables(self):
    self.client.execute("drop table if exists functional.{0}".format(TEST_TBL_NOPART_EXT))
    self.filesystem_client.delete_file_dir(TMP_STAGING_PATH, recursive=True)

  def teardown_method(self, method):
    self._clean_test_tables()

  def setup_method(self, method):
    # Defensively clean the data dirs if they exist.
    self._clean_test_tables()

    self.filesystem_client.make_dir(TMP_STAGING_PATH)
    self.filesystem_client.copy(ALLTYPES_PATH, "{0}/100101.txt".format(TMP_STAGING_PATH))

    self.client.execute("create table functional.{0} like functional.alltypesnopart"
        " location '{1}/{0}'".format(TEST_TBL_NOPART_EXT, WAREHOUSE))

  def test_load(self, vector):
    self.execute_query_expect_success(self.client, "load data inpath '{0}/100101.txt'"
        " into table functional.{1}".format(TMP_STAGING_PATH, TEST_TBL_NOPART_EXT))
    result = self.execute_scalar(
        "select count(*) from functional.{0}".format(TEST_TBL_NOPART_EXT))
    assert(result == '310')


@SkipIfLocal.hdfs_client
class TestAsyncLoadData(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAsyncLoadData, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    # Test all clients: hs2, hs2-http and beeswax
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    # Test two exec modes per client
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('enable_async_load_data_execution', True, False))
    # Disable codegen = false
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        disable_codegen_options=[False]))

  # This test subjects the load into either sync or async compilation of the load
  # query at the backend through beewax or hs2 clients. The objective is to assure
  # the load query completes successfully.
  def test_async_load(self, vector, unique_database):
    enable_async_load_data = vector.get_value('enable_async_load_data_execution')
    protocol = vector.get_value('protocol')
    client = self.create_impala_client(protocol=protocol)
    is_hs2 = protocol in ['hs2', 'hs2-http']
    running_state = "RUNNING_STATE" if is_hs2 else QueryState.RUNNING
    finished_state = "FINISHED_STATE" if is_hs2 else QueryState.FINISHED

    # Form a fully qualified table name with '-' in protocol 'hs2-http' dropped as
    # '-' is not allowed in Impala table name even delimited with ``.
    qualified_table_name = '{0}.{1}_{2}_{3}'.format(unique_database, TEST_TBL_NOPART,
        protocol if protocol != 'hs2-http' else 'hs2http', enable_async_load_data)

    # Form a staging path that is protocol and enable_async_load_data dependent to
    # allow parallel creating distinct HDFS directories for each test object.
    staging_path = "{0}_{1}_{2}".format(STAGING_PATH, protocol, enable_async_load_data)

    # Put some data into the staging path
    self.filesystem_client.delete_file_dir(staging_path, recursive=True)
    self.filesystem_client.make_dir(staging_path, permission=777)
    self.filesystem_client.copy(ALLTYPES_PATH, "{0}/100101.txt".format(staging_path))

    # Create a table with the staging path
    self.client.execute("create table {0} like functional.alltypesnopart \
        location \'{1}\'".format(qualified_table_name, staging_path))

    try:

      # The load data is going to need the metadata of the table. To avoid flakiness
      # about metadata loading, this selects from the table first to get the metadata
      # loaded.
      self.execute_query_expect_success(client,
          "select count(*) from {0}".format(qualified_table_name))

      # Configure whether to use async LOAD and add an appropriate delay of 3 seconds
      new_vector = deepcopy(vector)
      new_vector.get_value('exec_option')['enable_async_load_data_execution'] = \
           enable_async_load_data
      delay = "CRS_DELAY_BEFORE_LOAD_DATA:SLEEP@3000"
      new_vector.get_value('exec_option')['debug_action'] = "{0}".format(delay)
      load_stmt = "load data inpath \'{1}\' \
          into table {0}".format(qualified_table_name, staging_path)
      exec_start = time.time()
      handle = self.execute_query_async_using_client(client, load_stmt, new_vector)
      exec_end = time.time()
      exec_time = exec_end - exec_start
      exec_end_state = client.get_state(handle)

      # Wait for the statement to finish with a timeout of 20 seconds
      # (30 seconds without shortcircuit reads)
      wait_time = 20 if IS_HDFS else 30
      wait_start = time.time()
      self.wait_for_state(handle, finished_state, wait_time, client=client)
      wait_end = time.time()
      wait_time = wait_end - wait_start
      self.close_query_using_client(client, handle)
      if enable_async_load_data:
        # In async mode:
        #  The compilation of LOAD is processed in the exec step without delay. And the
        #  processing of the LOAD plan is in wait step with delay. The wait time should
        #  definitely take more time than 3 seconds.
        assert(exec_end_state == running_state)
        assert(wait_time >= 3)
      else:
        # In sync mode:
        #  The entire LOAD is processed in the exec step with delay. exec_time should be
        #  more than 3 seconds. Since the load query is submitted async, it is possible
        #  that the exec state returned is still in RUNNING state due to the the wait-for
        #  thread executing ClientRequestState::Wait() does not have time to set the
        #  exec state from RUNNING to FINISH.
        assert(exec_end_state == running_state or exec_end_state == finished_state)
        assert(exec_time >= 3)
    finally:
      client.close()

    self.client.execute("drop table if exists {0}".format(qualified_table_name))
    self.filesystem_client.delete_file_dir(staging_path, recursive=True)
