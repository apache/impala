# encoding=utf-8
#
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
#
# Tests Impala properly handles errors when reading and writing data.

from __future__ import absolute_import, division, print_function
import pytest
import subprocess

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf, SkipIfFS
from tests.common.test_dimensions import create_exec_option_dimension
from tests.util.filesystem_utils import get_fs_path
from tests.util.test_file_parser import QueryTestSectionReader


class TestDataErrors(ImpalaTestSuite):
  # batch_size of 1 can expose some interesting corner cases at row batch boundaries.
  BATCH_SIZES = [0, 1]

  @classmethod
  def add_test_dimensions(cls):
    super(TestDataErrors, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension(batch_sizes=cls.BATCH_SIZES))


  @classmethod
  def get_workload(self):
    return 'functional-query'


# Regression test for IMP-633. Added as a part of IMPALA-5198.
@SkipIf.not_dfs
class TestHdfsFileOpenFailErrors(ImpalaTestSuite):
  @pytest.mark.execute_serially
  def test_hdfs_file_open_fail(self):
    absolute_location = get_fs_path("/test-warehouse/file_open_fail")
    create_stmt = \
        "create table file_open_fail (x int) location '" + absolute_location + "'"
    insert_stmt = "insert into file_open_fail values(1)"
    select_stmt = "select * from file_open_fail"
    drop_stmt = "drop table if exists file_open_fail purge"
    self.client.execute(drop_stmt)
    self.client.execute(create_stmt)
    self.client.execute(insert_stmt)
    self.filesystem_client.delete_file_dir(absolute_location, recursive=True)
    assert not self.filesystem_client.exists(absolute_location)
    try:
      self.client.execute(select_stmt)
    except ImpalaBeeswaxException as e:
      assert "Failed to open HDFS file" in str(e)
    self.client.execute(drop_stmt)


# Test for IMPALA-5331 to verify that the libHDFS API hdfsGetLastExceptionRootCause()
# works.
@SkipIf.not_hdfs
class TestHdfsUnknownErrors(ImpalaTestSuite):
  @pytest.mark.execute_serially
  def test_hdfs_safe_mode_error_255(self, unique_database):
    pytest.xfail("IMPALA-6109: Putting HDFS name node into safe mode trips up HBase")
    create_stmt = "create table {0}.safe_mode_fail (x int)".format(unique_database)
    insert_stmt = "insert into {0}.safe_mode_fail values (1)".format(unique_database)
    self.execute_query_expect_success(self.client, create_stmt)
    self.execute_query_expect_success(self.client, insert_stmt)
    try:
      # Check that we're not in safe mode.
      output, error = subprocess.Popen(
          ['hdfs', 'dfsadmin', '-safemode', 'get'],
              stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
      assert error is "", "Couldn't get status of Safe mode. Error: %s" % (error)
      assert "Safe mode is OFF" in output
      # Turn safe mode on.
      output, error = subprocess.Popen(
          ['hdfs', 'dfsadmin', '-safemode', 'enter'],
              stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
      assert error is "", "Couldn't turn Safe mode ON. Error: %s" % (error)
      assert "Safe mode is ON" in output

      # We shouldn't be able to write to HDFS when it's in safe mode.
      ex = self.execute_query_expect_failure(self.client, insert_stmt)

      # Confirm that it is an Unknown error with error code 255.
      assert "Unknown error 255" in str(ex)
      # Confirm that we were able to get the root cause.
      assert "Name node is in safe mode" in str(ex)
    finally:
      # Leave safe mode.
      output, error = subprocess.Popen(
          ['hdfs', 'dfsadmin', '-safemode', 'leave'],
              stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
      assert error is "", "Couldn't turn Safe mode OFF. Error: %s" % (error)
      assert "Safe mode is OFF" in output


@SkipIfFS.qualified_path
class TestHdfsScanNodeErrors(TestDataErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsScanNodeErrors, cls).add_test_dimensions()
    # Only run on delimited text with no compression.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format != 'hbase' and
        v.get_value('table_format').file_format != 'parquet')

  def test_hdfs_scan_node_errors(self, vector):
    # TODO: Run each test with abort_on_error=0 and abort_on_error=1.
    vector.get_value('exec_option')['abort_on_error'] = 0
    if (vector.get_value('table_format').file_format != 'text'):
      pytest.xfail("Expected results differ across file formats")
    self.run_test_case('DataErrorsTest/hdfs-scan-node-errors', vector)


@SkipIfFS.qualified_path
class TestHdfsSeqScanNodeErrors(TestHdfsScanNodeErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsSeqScanNodeErrors, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'seq')

  def test_hdfs_seq_scan_node_errors(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('DataErrorsTest/hdfs-sequence-scan-errors', vector)


@SkipIfFS.qualified_path
class TestHdfsRcFileScanNodeErrors(TestHdfsScanNodeErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsRcFileScanNodeErrors, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'rc')

  def test_hdfs_rcfile_scan_node_errors(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('DataErrorsTest/hdfs-rcfile-scan-node-errors', vector)


@SkipIfFS.qualified_path
class TestHdfsJsonScanNodeErrors(TestHdfsScanNodeErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsJsonScanNodeErrors, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'json')

  def test_hdfs_json_scan_node_errors(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = 0
    table_format = vector.get_value('table_format')
    db_name = QueryTestSectionReader.get_db_name(table_format)
    self.run_test_case('DataErrorsTest/hdfs-json-scan-node-errors', vector,
        use_db=db_name)


class TestAvroErrors(TestDataErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestAvroErrors, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'avro' and
        v.get_value('table_format').compression_codec == 'snap')

  def test_avro_errors(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('DataErrorsTest/avro-errors', vector)


class TestHBaseDataErrors(TestDataErrors):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHBaseDataErrors, cls).add_test_dimensions()

    # Only run on hbase.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
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


class TestTimestampErrors(TestDataErrors):
  """
  Create test table with various valid/invalid timestamp values, then run
  scan and aggregation queries to make sure Impala doesn't crash.
    - value doesn't have date
    - value contains non-ascii char
    - value contains unicode char
    - value is outside boost gregorian date range.
  """
  @classmethod
  def add_test_dimensions(cls):
    super(TestTimestampErrors, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text')

  def _setup_test_table(self, fq_tbl_name):
    create_stmt = "CREATE TABLE " + fq_tbl_name + " (col string)"
    insert_stmt = "INSERT INTO TABLE " + fq_tbl_name + " values" + \
        "('1999-03-24 07:21:02'), ('2001-ån-02 12:12:15')," + \
        "('1997-1131 02:09:32'), ('1954-12-03 15:10:02')," + \
        "('12:10:02'), ('1001-04-23 21:08:19'), ('15:03:09')"
    alter_stmt = "ALTER TABLE " + fq_tbl_name + " CHANGE col col timestamp"
    self.client.execute(create_stmt)
    self.client.execute(insert_stmt)
    self.client.execute(alter_stmt)

  def test_timestamp_scan_agg_errors(self, vector, unique_database):
    FQ_TBL_NAME = "%s.%s" % (unique_database, 'scan_agg_timestamp')
    self._setup_test_table(FQ_TBL_NAME)
    vector.get_value('exec_option')['abort_on_error'] = 0
    result = self.client.execute("SELECT AVG(col) FROM " + FQ_TBL_NAME)
    assert result.data == ['1977-01-27 11:15:32']
    result = self.client.execute("SELECT * FROM " + FQ_TBL_NAME + " ORDER BY col")
    assert len(result.data) == 7
    assert result.data == ['1954-12-03 15:10:02', '1999-03-24 07:21:02', \
        'NULL', 'NULL', 'NULL', 'NULL', 'NULL']
    result = self.client.execute("SELECT COUNT(DISTINCT col) FROM " + FQ_TBL_NAME)
    assert result.data == ['2']
