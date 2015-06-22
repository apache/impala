#!/usr/bin/env python
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

import pytest
import sys
import os
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfS3
from tests.util.filesystem_utils import WAREHOUSE, IS_DEFAULT_FS, get_fs_path

class TestCTASCleanup(ImpalaTestSuite):
  """Test class to verify if Impala cleanup newly created table after CTAS failure."""

  TEST_DB_NAME = "test_ctas_cleanup_db"

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCTASCleanup, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)
    self.execute_query("create database if not exists {0} location '{1}/{0}.db'"
        .format(self.TEST_DB_NAME, WAREHOUSE))

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)

  @SkipIfS3.insert
  def test_ctas_cleanup(self, vector):
    TBL_NAME = "test_ctas_tbl"
    self.execute_query_expect_failure(self.client,
      "create table %s.%s as select * from functional_text_gzip.bad_text_gzip" %
      (self.TEST_DB_NAME,TBL_NAME))
    path = "test-warehouse/%s.db/%s" % (self.TEST_DB_NAME, TBL_NAME)
    try:
      self.hdfs_client.get_file_dir_status(path)
      pytest.fail("CTAS doesn't delete HDFS directory after failure")
    except Exception, e:
      pass
    # Verify the new table is cleaned up from catalog cache.
    self.execute_query_expect_failure(self.client,
        "select * from %s.%s" % (self.TEST_DB_NAME,TBL_NAME))

    # Verify that the table does not exist in HMS.
    self.client.execute("invalidate metadata")
    self.client.execute("use %s" % self.TEST_DB_NAME)
    if TBL_NAME in self.client.execute("show tables").data:
      pytest.fail("CTAS doesn't cleanup metadata properly after failure")
