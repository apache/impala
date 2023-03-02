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

# This test suite validates the functionality that allows users to create an external
# table associated with a single file.

from __future__ import absolute_import, division, print_function
from tests.common.file_utils import copy_files_to_hdfs_dir
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf
from tests.util.filesystem_utils import WAREHOUSE


@SkipIf.sfs_unsupported
class TestSFS(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSFS, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'none')
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('exec_option')['disable_codegen'] is False)

  def test_sfs(self, vector, unique_database):
    files_for_external_tables = ["testdata/data/sfs_d1.parq", "testdata/data/sfs_d2.txt",
                                 "testdata/data/sfs_d3.parq", "testdata/data/sfs_d4.txt"]
    files_for_managed_tables = ["testdata/data/sfs_d3.parq", "testdata/data/sfs_d4.txt"]
    hdfs_dir_for_external_tables = "{0}/{1}.db/".format(WAREHOUSE, unique_database)
    hdfs_dir_for_managed_tables =\
        "{0}/managed/{1}.db/".format(WAREHOUSE, unique_database)

    copy_files_to_hdfs_dir(files_for_external_tables, hdfs_dir_for_external_tables)
    copy_files_to_hdfs_dir(files_for_managed_tables, hdfs_dir_for_managed_tables)

    self.run_test_case('QueryTest/sfs', vector, unique_database)
