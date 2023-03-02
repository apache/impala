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

# Validates table stored on the LocalFileSystem.
#
from __future__ import absolute_import, division, print_function
import pytest
from subprocess import check_call, call

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.filesystem_utils import get_secondary_fs_path

@SkipIf.no_secondary_fs
class TestMultipleFilesystems(ImpalaTestSuite):
  """
  Tests that tables and queries can span multiple filesystems.
  """

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMultipleFilesystems, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and \
        v.get_value('table_format').compression_codec == 'none')

  def _populate_secondary_fs_partitions(self, db_name):
    # This directory may already exist. So we needn't mind if this call fails.
    call(["hadoop", "fs", "-mkdir", get_secondary_fs_path("/multi_fs_tests/")], shell=False)
    check_call(["hadoop", "fs", "-mkdir",
                get_secondary_fs_path("/multi_fs_tests/%s.db/" % db_name)], shell=False)
    self.filesystem_client.copy("/test-warehouse/alltypes_parquet/",
        get_secondary_fs_path("/multi_fs_tests/%s.db/" % db_name), overwrite=True)
    self.filesystem_client.copy("/test-warehouse/tinytable/", get_secondary_fs_path(
        "/multi_fs_tests/%s.db/" % db_name), overwrite=True)

  @pytest.mark.execute_serially
  def test_multiple_filesystems(self, vector, unique_database):
    try:
      self._populate_secondary_fs_partitions(unique_database)
      self.run_test_case('QueryTest/multiple-filesystems', vector, use_db=unique_database)
    finally:
      # We delete this from the secondary filesystem here because the database was created
      # in HDFS but the queries will create this path in the secondary FS as well. So
      # dropping the database will not delete the directory in the secondary FS.
      check_call(["hadoop", "fs", "-rm", "-r",
          get_secondary_fs_path("/multi_fs_tests/%s.db/" % unique_database)], shell=False)
