# encoding=utf-8
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
from tests.common.skip import SkipIfS3, SkipIfLocal
import random
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
@SkipIfLocal.qualified_path
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

@SkipIfS3.insert
class TestTimestampErrors(TestDataErrors):
  """
  Create test table with various valid/invalid timestamp values, then run
  scan and aggregation queries to make sure Impala doesn't crash.
    - value doesn't have date
    - value contains non-ascii char
    - value contains unicode char
    - value is outside boost gregorian date range.
  """
  TEST_DATABASE = "test_timestamp" + str(random.randint(0, 10**5))

  @classmethod
  def setup_class(cls):
    super(TestTimestampErrors, cls).setup_class()
    cls.cleanup_db(cls.TEST_DATABASE)
    cls.client.execute("CREATE DATABASE IF NOT EXISTS " + cls.TEST_DATABASE)

  @classmethod
  def teardown_class(cls):
    cls.cleanup_db(cls.TEST_DATABASE)
    super(TestTimestampErrors, cls).teardown_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestTimestampErrors, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text')

  def _setup_test_table(self, table_name):
    create_stmt = "CREATE TABLE " + table_name + " (col string)"
    insert_stmt = "INSERT INTO TABLE " + table_name + " values" + \
        "('1999-03-24 07:21:02'), ('2001-ån-02 12:12:15')," + \
        "('1997-1131 02:09:32'), ('1954-12-03 15:10:02')," + \
        "('12:10:02'), ('1001-04-23 21:08:19'), ('15:03:09')"
    alter_stmt = "ALTER TABLE " + table_name + " CHANGE col col timestamp"
    self.client.execute(create_stmt)
    self.client.execute(insert_stmt)
    self.client.execute(alter_stmt)

  @pytest.mark.execute_serially
  def test_timestamp_scan_agg_errors(self, vector):
    table_name = "%s.%s_%s" % (self.TEST_DATABASE, 'scan_agg_timestamp', \
        str(random.randint(0, 10**5)))
    self._setup_test_table(table_name)
    vector.get_value('exec_option')['abort_on_error'] = 0
    result = self.client.execute("SELECT AVG(col) FROM " + table_name)
    assert result.data == ['1977-01-27 11:15:32']
    result = self.client.execute("SELECT * FROM " + table_name + " ORDER BY col")
    assert len(result.data) == 7
    assert result.data == ['1954-12-03 15:10:02', '1999-03-24 07:21:02', \
        '12:10:02', '15:03:09', 'NULL', 'NULL', 'NULL']
    result = self.client.execute("SELECT COUNT(DISTINCT col) FROM " + table_name)
    assert result.data == ['4']
