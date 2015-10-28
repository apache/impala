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
from tests.common.skip import SkipIfS3, SkipIfLocal
from tests.util.filesystem_utils import WAREHOUSE

TEST_TBL = 'read_only_tbl'
TBL_LOC = '%s/%s' % (WAREHOUSE, TEST_TBL)


@SkipIfS3.insert
@SkipIfLocal.hdfs_client
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
    self._cleanup()

  def teardown_method(self, method):
    self._cleanup()

  def _cleanup(self):
    self.client.execute('drop table if exists %s' % TEST_TBL)
    self.hdfs_client.delete_file_dir('test-warehouse/%s' % TEST_TBL, recursive=True)

  def test_insert_into_read_only_table(self, vector):
    # Create a directory that is read-only
    self.hdfs_client.make_dir('test-warehouse/%s' % TEST_TBL, permission=444)
    self.client.execute("create external table %s (i int) location '%s'"
        % (TEST_TBL, TBL_LOC))
    try:
      self.client.execute('insert into table %s select 1' % TEST_TBL)
      assert False, 'Expected INSERT INTO read-only table to fail'
    except Exception, e:
      assert 'does not have WRITE access to at least one HDFS path: hdfs:' in str(e)

    # Should still be able to query this table without any errors.
    assert self.execute_scalar('select count(*) from %s' % TEST_TBL) == "0"

    # Now re-create the directory with write privileges.
    self.hdfs_client.delete_file_dir('test-warehouse/%s' % TEST_TBL, recursive=True)
    self.hdfs_client.make_dir('test-warehouse/%s' % TEST_TBL, permission=777)

    self.client.execute('refresh  %s' % TEST_TBL)
    self.client.execute('insert into table %s select 1' % TEST_TBL)
    assert self.execute_scalar('select count(*) from %s' % TEST_TBL) == "1"
    self.client.execute('drop table if exists %s' % TEST_TBL)

    # Verify with a partitioned table
    try:
      self.client.execute('insert into table functional_seq.alltypes '\
          'partition(year, month) select * from functional.alltypes limit 0')
      assert False, 'Expected INSERT INTO read-only partition to fail'
    except Exception, e:
      assert 'does not have WRITE access to at least one HDFS path: hdfs:' in str(e)
