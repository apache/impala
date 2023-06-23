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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
  create_single_exec_option_dimension,
  create_uncompressed_text_dimension)
from tests.util.filesystem_utils import get_fs_path


class TestTestcaseBuilder(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTestcaseBuilder, cls).add_test_dimensions()
    # This test only needs to be run once.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
      create_uncompressed_text_dimension(cls.get_workload()))

  def test_query_without_from(self):
    self._test_export_and_import(0, 0, 0, "SELECT 5 * 20")

  def test_query_with_tbls(self, unique_database):
    """Verify the basic usage. Use a unique database so the import won't impact the
    metadata used by other tests"""
    self.client.execute(
        "create table {0}.alltypes like functional.alltypes".format(unique_database))
    self.client.execute(
        "create view {0}.alltypes_view as select * from {0}.alltypes"
        .format(unique_database))
    # Test SELECT on a view. The view will be expanded and the underlying table will also
    # be exported.
    self._test_export_and_import(1, 1, 1,
        "select count(*) from {0}.alltypes_view".format(unique_database))

  def _test_export_and_import(self, num_dbs, num_tbls, num_views, query):
    tmp_path = get_fs_path("/tmp")
    # Make sure /tmp dir exists
    if not self.filesystem_client.exists(tmp_path):
      self.filesystem_client.make_dir(tmp_path)
    # Generate Testcase Data for query without table reference
    testcase_generate_query = "COPY TESTCASE TO '%s' %s" % (tmp_path, query)
    result = self.execute_query_expect_success(self.client, testcase_generate_query)
    assert len(result.data) == 1, "Testcase builder wrong result: {0}".format(result.data)

    # Check file exists
    testcase_path = str(result.data)[1: -1]
    index = testcase_path.index(tmp_path)
    hdfs_path = testcase_path[index:-1]
    assert self.filesystem_client.exists(hdfs_path), \
        "File not generated {0}".format(hdfs_path)

    try:
      # Test load testcase works
      testcase_load_query = "COPY TESTCASE FROM {0}".format(testcase_path)
      result = self.execute_query_expect_success(self.client, testcase_load_query)
      expected_msg = "{0} db(s), {1} table(s) and {2} view(s) imported for query".format(
          num_dbs, num_tbls, num_views)
      assert expected_msg in result.get_data()
    finally:
      # Delete testcase file from tmp
      status = self.filesystem_client.delete_file_dir(hdfs_path)
      assert status, "Delete generated testcase file failed with {0}".format(status)
