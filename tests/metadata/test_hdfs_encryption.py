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
import getpass
import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.common.test_vector import ImpalaTestDimension
from tests.util.shell_util import exec_process

TEST_DB = 'test_encryption_db'

# PyWebHdfs takes absolute paths without a leading slash
PYWEBHDFS_TMP_DIR = 'tmp/test_encryption_load_data'
TMP_DIR = '/%s' % (PYWEBHDFS_TMP_DIR)


@SkipIfFS.hdfs_encryption
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
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

    PARTITIONED = [True, False]
    # For 'core', just test loading from a directory that is encrypted.
    KEY_LOAD_DIR = ["testkey1"]
    KEY_TBL_DIR = [None]

    if cls.exploration_strategy() == 'exhaustive':
      KEY_LOAD_DIR = [None, "testkey1", "testkey2"]
      KEY_TBL_DIR = [None, "testkey1", "testkey2"]

    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('partitioned', *PARTITIONED))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('key_load_dir', *KEY_LOAD_DIR))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('key_tbl_dir', *KEY_TBL_DIR))
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('key_load_dir') is not None or\
        v.get_value('key_tbl_dir') is not None)

  def setup_method(self, method):
    self.__cleanup()
    self.client.execute('create database if not exists %s' % TEST_DB)
    self.client.execute('use %s' % TEST_DB)
    self.hdfs_client.make_dir(PYWEBHDFS_TMP_DIR)
    # Few tests depend on the .Trash directory being present. In case it doesn't
    # exist, we create a random text file and delete it so that hdfs recreates
    # the hierarchy of trash
    if not self.hdfs_client.exists("/user/{0}/.Trash/".format(getpass.getuser())):
      self.hdfs_client.create_file("test-warehouse/random",file_data="random")
      rc, stdout, stderr = exec_process("hadoop fs -rm /test-warehouse/random")
      assert rc == 0, 'Error re-creating trash: %s %s' % (stdout, stderr)

  def teardown_method(self, method):
    self.__cleanup()
    # Clean up trash directory so that further tests aren't affected
    rc, stdout, stderr = exec_process(
            "hadoop fs -rmr /user/{0}/.Trash/".format(getpass.getuser()))
    assert rc == 0, 'Error deleting Trash: %s %s' % (stdout, stderr)

  def create_encryption_zone(self, key, path):
    """Creates an encryption zone using key 'key' on path 'path'"""
    rc, stdout, stderr = exec_process(
            "hdfs crypto -createZone -keyName %s -path %s" % (key, path))
    assert rc == 0, 'Error creating encryption zone: %s %s' % (stdout, stderr)

  def __cleanup(self):
    self.client.execute('use default')
    self.client.execute('drop table if exists %s.tbl purge' % TEST_DB)
    self.client.execute('drop table if exists %s.t1 purge' % TEST_DB)
    self.cleanup_db(TEST_DB)
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

  @pytest.mark.execute_serially
  def test_drop_partition_encrypt(self):
    """Verifies if alter <tbl> drop partition purge works in case
    where the Trash dir and partition dir are in different encryption
    zones. Check IMPALA-2310 for details"""
    self.client.execute("create table {0}.t1(i int) partitioned\
      by (j int)".format(TEST_DB))
    # Add three partitions (j=1), (j=2), (j=3) to table t1
    self.client.execute("alter table {0}.t1 add partition(j=1)".format(TEST_DB));
    self.client.execute("alter table {0}.t1 add partition(j=2)".format(TEST_DB));
    self.client.execute("alter table {0}.t1 add partition(j=3)".format(TEST_DB));
    # Clean up the trash directory to create an encrypted zone
    rc, stdout, stderr = exec_process(
            "hadoop fs -rm -r /user/{0}/.Trash/*".format(getpass.getuser()))
    assert rc == 0, 'Error deleting Trash: %s %s' % (stdout, stderr)
    # Create the necessary encryption zones
    self.create_encryption_zone("testkey1", "/test-warehouse/{0}.db/t1/j=1"\
            .format(TEST_DB))
    self.create_encryption_zone("testkey2", "/test-warehouse/{0}.db/t1/j=2"\
            .format(TEST_DB))
    self.create_encryption_zone("testkey1", "/test-warehouse/{0}.db/t1/j=3"\
            .format(TEST_DB))

    # HDFS 2.8+ behavior is to create individual trash per encryption zone;
    # don't create an encryption zone on .Trash in that case, otherwise
    # recursive trash is created.
    has_own_trash = self.hdfs_client.exists(
        "/test-warehouse/{0}.db/t1/j=1/.Trash".format(TEST_DB))
    if not has_own_trash:
      self.create_encryption_zone("testkey2", "/user/{0}/.Trash/".format(\
              getpass.getuser()))

    # Load sample data into the partition directories
    self.hdfs_client.create_file("test-warehouse/{0}.db/t1/j=1/j1.txt"\
            .format(TEST_DB), file_data='j1')
    self.hdfs_client.create_file("test-warehouse/{0}.db/t1/j=2/j2.txt"\
            .format(TEST_DB), file_data='j2')
    self.hdfs_client.create_file("test-warehouse/{0}.db/t1/j=3/j3.txt"\
            .format(TEST_DB), file_data='j3')

    # Drop the partition (j=1) without purge and make sure partition directory still
    # exists. This behavior is expected due to the difference in encryption zones
    # between the .Trash and the warehouse directory (prior to HDFS 2.8)
    if not has_own_trash:
      self.execute_query_expect_failure(self.client, "alter table {0}.t1 drop \
              partition(j=1)".format(TEST_DB));
      assert self.hdfs_client.exists("test-warehouse/{0}.db/t1/j=1/j1.txt".format(TEST_DB))
      assert self.hdfs_client.exists("test-warehouse/{0}.db/t1/j=1".format(TEST_DB))
    else:
      # HDFS 2.8+ behavior succeeds the query and creates trash; the partition removal
      # ends up destroying the directories which moves this back to the user's trash
      self.client.execute("alter table {0}.t1 drop partition(j=1)".format(TEST_DB));
      assert self.hdfs_client.exists(
        "/user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=1/j1.txt"\
        .format(getpass.getuser(), TEST_DB))
      assert not self.hdfs_client.exists("test-warehouse/{0}.db/t1/j=1/j1.txt".format(TEST_DB))
      assert not self.hdfs_client.exists("test-warehouse/{0}.db/t1/j=1".format(TEST_DB))

    # Drop the partition j=2 (with purge) and make sure the partition directory is deleted
    self.client.execute("alter table {0}.t1 drop partition(j=2) purge".format(TEST_DB))
    assert not self.hdfs_client.exists("test-warehouse/{0}.db/t1/j=2/j2.txt".format(TEST_DB))
    assert not self.hdfs_client.exists("test-warehouse/{0}.db/t1/j=2".format(TEST_DB))
    # Drop the partition j=3 (with purge) and make sure the partition is deleted
    # This is the case where the trash directory and partition data directory
    # are in different encryption zones. Using purge should delete the partition
    # data pemanently by skipping trash
    self.client.execute("alter table {0}.t1 drop partition(j=3) purge".format(TEST_DB))
    assert not self.hdfs_client.exists("test-warehouse/{0}.db/t1/j=3/j3.txt".format(TEST_DB))
    assert not self.hdfs_client.exists("test-warehouse/{0}.db/t1/j=3".format(TEST_DB))

  @pytest.mark.execute_serially
  def test_drop_table_encrypt(self):
    """Verifies if drop <table> purge works in a case where Trash directory and table
    directory in different encryption zones"""
    self.client.execute("create table {0}.t3(i int)".format(TEST_DB))

    # Clean up the trash directory to create an encrypted zone
    rc, stdout, stderr = exec_process(
            "hadoop fs -rmr /user/{0}/.Trash/*".format(getpass.getuser()))
    assert rc == 0, 'Error deleting Trash: %s %s' % (stdout, stderr)
    # Create table directory and trash directory in different encryption zones
    self.create_encryption_zone("testkey1", "/test-warehouse/{0}.db/t3".format(TEST_DB))
    self.create_encryption_zone("testkey2", "/user/{0}/.Trash/".format(getpass.getuser()))
    self.client.execute("drop table {0}.t3 purge".format(TEST_DB))
    assert not self.hdfs_client.exists("test-warehouse/{0}.db/t3".format(TEST_DB))
