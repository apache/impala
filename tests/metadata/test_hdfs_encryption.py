# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfS3, SkipIfIsilon
from tests.common.test_dimensions import create_exec_option_dimension
from tests.util.shell_util import exec_process

TEST_DB = 'test_encryption_db'

# PyWebHdfs takes absolute paths without a leading slash
PYWEBHDFS_TMP_DIR = 'tmp/test_encryption_load_data'
TMP_DIR = '/%s' % (PYWEBHDFS_TMP_DIR)


@SkipIfS3.load_data
@SkipIfIsilon.hdfs_encryption
@pytest.mark.execute_serially
class TestHdfsEncryption(ImpalaTestSuite):
  ''' Tests LOAD DATA commands work between HDFS encryption zones.

      A test directory is created with data to be loaded into a test table.
      The test directory and/or the table directory are created as encryption
      zones with different keys, and the LOAD DATA command is executed. The
      tests operate on both partitioned and non-partitioned tables.
  '''
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsEncryption, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

    PARTITIONED = [True, False]
    # For 'core', just test loading from a directory that is encrypted.
    KEY_LOAD_DIR = ["testkey1"]
    KEY_TBL_DIR = [None]

    if cls.exploration_strategy() == 'exhaustive':
      KEY_LOAD_DIR = [None, "testkey1", "testkey2"]
      KEY_TBL_DIR = [None, "testkey1", "testkey2"]

    cls.TestMatrix.add_dimension(TestDimension('partitioned', *PARTITIONED))
    cls.TestMatrix.add_dimension(TestDimension('key_load_dir', *KEY_LOAD_DIR))
    cls.TestMatrix.add_dimension(TestDimension('key_tbl_dir', *KEY_TBL_DIR))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('key_load_dir') is not None or\
        v.get_value('key_tbl_dir') is not None)

  def setup_method(self, method):
    self.__cleanup()
    self.client.execute('create database if not exists %s' % TEST_DB)
    self.client.execute('use %s' % TEST_DB)
    self.hdfs_client.make_dir(PYWEBHDFS_TMP_DIR)

  def teardown_method(self, method):
    self.__cleanup()

  def __cleanup(self):
    self.client.execute('use default')
    self.client.execute('drop table if exists %s.tbl' % TEST_DB)
    self.client.execute('drop database if exists %s' % TEST_DB)
    self.hdfs_client.delete_file_dir(PYWEBHDFS_TMP_DIR, recursive=True)

  def test_load_data(self, vector):
    key_tbl_dir = vector.get_value('key_tbl_dir')
    key_load_dir = vector.get_value('key_load_dir')

    if vector.get_value('partitioned'):
      src_file = "/test-warehouse/alltypes/year=2010/month=1/100101.txt"
      src_tbl_schema = "functional.alltypes"
    else:
      src_file = "/test-warehouse/tinytable/data.csv"
      src_tbl_schema = "functional.tinytable"

    if key_load_dir is not None:
      rc, stdout, stderr = exec_process(
          'hdfs crypto -createZone -keyName %s -path %s' % (key_load_dir, TMP_DIR))
      assert rc == 0, 'Error executing hdfs crypto: %s %s' % (stdout, stderr)

    # hdfs_client doesn't support copy
    rc, stdout, stderr = exec_process('hdfs dfs -cp %s %s' % (src_file, TMP_DIR))
    assert rc == 0, 'Error executing hdfs cp: %s %s' % (stdout, stderr)

    self.client.execute('create table tbl like %s' % (src_tbl_schema))

    if key_tbl_dir is not None:
      rc, stdout, stderr = exec_process(
          'hdfs crypto -createZone -keyName %s -path /test-warehouse/%s.db/tbl' %
          (key_tbl_dir, TEST_DB))
      assert rc == 0, 'Error executing hdfs crypto: %s %s' % (stdout, stderr)

    if vector.get_value('partitioned'):
      # insert a single value to create the partition spec
      self.client.execute('insert into tbl partition (year=2010, month=1) '\
          'values (0,true,0,0,0,0,0,0,NULL,NULL,NULL)')
      self.client.execute('load data inpath \'%s\' into table tbl '\
          'partition(year=2010, month=1)' % (TMP_DIR))
    else:
      self.client.execute('load data inpath \'%s\' into table tbl ' % (TMP_DIR))
