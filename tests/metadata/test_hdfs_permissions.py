#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
import os
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.skip import SkipIfS3


TEST_DB = 'read_only_table_test_db'

@SkipIfS3.insert
class TestHdfsPermissions(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsPermissions, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def setup_method(self, method):
    self.__cleanup()
    self.client.execute('create database if not exists %s' % TEST_DB)

  def teardown_method(self, method):
    self.__cleanup()

  def __cleanup(self):
    self.client.execute('use default')
    self.client.execute('drop table if exists %s.read_only_tbl' % TEST_DB)
    self.client.execute('drop database if exists %s' % TEST_DB)
    self.hdfs_client.delete_file_dir('test-warehouse/read-only-test', recursive=True)

  def test_insert_into_read_only_table(self, vector):
    self.client.execute('use %s' % TEST_DB)

    # Create a directory that is read-only
    self.hdfs_client.make_dir('test-warehouse/read-only-test', permission=444)
    self.client.execute('create external table read_only_tbl (i int) location '\
        '\'/test-warehouse/read-only-test\'')
    try:
      self.client.execute('insert into table read_only_tbl select 1')
      assert 0, 'Expected INSERT INTO read-only table to fail'
    except Exception, e:
      assert 'does not have WRITE access to at least one HDFS path: hdfs:' in str(e)

    # Should still be able to query this table without any errors.
    assert '0' == self.execute_scalar('select count(*) from read_only_tbl')

    # Now re-create the directory with write privileges.
    self.hdfs_client.delete_file_dir('test-warehouse/read-only-test', recursive=True)
    self.hdfs_client.make_dir('test-warehouse/read-only-test', permission=777)

    self.client.execute('refresh read_only_tbl')
    self.client.execute('insert into table read_only_tbl select 1')
    assert '1' == self.execute_scalar('select count(*) from read_only_tbl')
    self.client.execute('drop table if exists read_only_tbl')

    # Verify with a partitioned table
    try:
      self.client.execute('insert into table functional_seq.alltypes '\
          'partition(year, month) select * from functional.alltypes limit 0')
      assert 0, 'Expected INSERT INTO read-only partition to fail'
    except Exception, e:
      assert 'does not have WRITE access to at least one HDFS path: hdfs:' in str(e)
