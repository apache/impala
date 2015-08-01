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
#
# Tests Impala properly handles errors when reading and writing data.

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfS3
import pytest

class TestDataErrors(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestDataErrors, cls).add_test_dimensions()

  @classmethod
  def get_workload(self):
    return 'functional-query'


@SkipIfS3.qualified_path
class TestHdfsScanNodeErrors(TestDataErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsScanNodeErrors, cls).add_test_dimensions()
    # Only run on delimited text with no compression.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format != 'hbase' and
        v.get_value('table_format').file_format != 'parquet')

  def test_hdfs_scan_node_errors(self, vector):
    # TODO: Run each test with abort_on_error=0 and abort_on_error=1.
    vector.get_value('exec_option')['abort_on_error'] = 0
    if (vector.get_value('table_format').file_format != 'text'):
      pytest.xfail("Expected results differ across file formats")
    self.run_test_case('DataErrorsTest/hdfs-scan-node-errors', vector)


@SkipIfS3.qualified_path
class TestHdfsSeqScanNodeErrors(TestHdfsScanNodeErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsSeqScanNodeErrors, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'seq')

  def test_hdfs_seq_scan_node_errors(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('DataErrorsTest/hdfs-sequence-scan-errors', vector)


@SkipIfS3.qualified_path
class TestHdfsRcFileScanNodeErrors(TestHdfsScanNodeErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsRcFileScanNodeErrors, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'rc')

  def test_hdfs_rcfile_scan_node_errors(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('DataErrorsTest/hdfs-rcfile-scan-node-errors', vector)


class TestHBaseDataErrors(TestDataErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHBaseDataErrors, cls).add_test_dimensions()

    # Only run on hbase.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'hbase' and\
        v.get_value('table_format').compression_codec == 'none')

  def test_hbase_scan_node_errors(self, vector):
    pytest.xfail("hbasealltypeserror doesn't seem to return any errors")

    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('DataErrorsTest/hbase-scan-node-errors', vector)

  def test_hbase_insert_errors(self, vector):
    pytest.xfail("hbasealltypeserror doesn't seem to return any errors")
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('DataErrorsTest/hbase-insert-errors', vector)
