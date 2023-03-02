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
from subprocess import check_call

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.filesystem_utils import get_fs_path

class TestRewrittenFile(ImpalaTestSuite):
  """Tests that we gracefully handle when a file in HDFS is rewritten outside of Impala
  without issuing "invalidate metadata"."""

  FILE_NAME = "alltypes.parq"
  # file size = 17.8 KB
  SHORT_FILE = get_fs_path("/test-warehouse/alltypesagg_parquet/year=2010/month=1/" \
      "day=__HIVE_DEFAULT_PARTITION__/*.parq")
  SHORT_FILE_NUM_ROWS = 1000
  # file size = 43.3 KB
  LONG_FILE = get_fs_path("/test-warehouse/alltypesagg_parquet/year=2010/month=1/" \
      "day=9/*.parq")
  LONG_FILE_NUM_ROWS = 1000

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRewrittenFile, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # TODO: add more file formats
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def __overwrite_file_and_query(self, db_name, table_name, old_file, new_file,
    expected_error, expected_new_count):
    """Rewrites 'old_file' with 'new_file' without invalidating metadata and verifies that
    querying the table results in the expected error. 'expected_error' only needs to be a
    substring of the full error message."""
    table_location = self.__get_test_table_location(db_name)
    self.__create_test_table(db_name, table_name, table_location)

    # First copy in 'old_file' and refresh the cached file metadata.
    self.__copy_file_to_test_table(old_file, table_location)
    self.client.execute("refresh %s.%s" % (db_name, table_name))

    # Then overwrite 'old_file' with 'new_file', and don't invalidate metadata.
    self.__copy_file_to_test_table(new_file, table_location)

    # Query the table and check for expected error.
    try:
      result = self.client.execute("select * from %s.%s" % (db_name, table_name))
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert expected_error in str(e)

    # Refresh the table and make sure we get results
    self.client.execute("refresh %s.%s" % (db_name, table_name))
    result = self.client.execute("select count(*) from %s.%s" % (db_name, table_name))
    assert result.data == [str(expected_new_count)]

  @SkipIfFS.read_past_eof
  def test_new_file_shorter(self, vector, unique_database):
    """Rewrites an existing file with a new shorter file."""
    # Full error is something like:
    #   Metadata for file '...' appears stale. Try running "refresh
    #   unique_database_name.new_file_shorter" to reload the file metadata.
    # IMPALA-2512: Error message could also be something like
    #   Query aborted:Disk I/O error on ...:27001: Error seeking ...
    #   between 0 and ... for '...'
    # TODO: find a better way to detect stale file meta and remove skip markers.
    table_name = "new_file_shorter"
    self.__overwrite_file_and_query(unique_database, table_name,
      self.LONG_FILE, self.SHORT_FILE, 'appears stale.', self.SHORT_FILE_NUM_ROWS)

  def test_new_file_longer(self, vector, unique_database):
    """Rewrites an existing file with a new longer file."""
    # Full error is something like:
    # "File '..' has an invalid Parquet version number: ff4C
    # Please check that it is a valid Parquet file. his error can also occur due to
    # stale metadata. If you believe this is a valid Parquet file, try running
    # "refresh ...".
    table_name = "new_file_longer"
    self.__overwrite_file_and_query(unique_database, table_name,
      self.SHORT_FILE, self.LONG_FILE, 'invalid Parquet version number',
      self.LONG_FILE_NUM_ROWS)

  def test_delete_file(self, vector, unique_database):
    """Deletes an existing file without refreshing metadata."""
    table_name = "delete_file"
    table_location = self.__get_test_table_location(unique_database)
    self.__create_test_table(unique_database, table_name, table_location)

    # Copy in a file and refresh the cached file metadata.
    self.__copy_file_to_test_table(self.LONG_FILE, table_location)
    self.client.execute("refresh %s.%s" % (unique_database, table_name))

    # Delete the file without refreshing metadata.
    check_call(["hadoop", "fs", "-rm", table_location + '/*'], shell=False)

    # Query the table and check for expected error.
    try:
      result = self.client.execute("select * from %s.%s" % (unique_database, table_name))
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert 'No such file or directory' in str(e)

    # Refresh the table and make sure we get results
    self.client.execute("refresh %s.%s" % (unique_database, table_name))
    result = self.client.execute("select count(*) from %s.%s"\
      % (unique_database, table_name))
    assert result.data == ['0']

  def __get_test_table_location(self, db_name):
    return get_fs_path("/test-warehouse/%s" % db_name)

  def __create_test_table(self, db_name, table_name, table_location):
    self.client.execute("""
      CREATE TABLE %s.%s LIKE functional.alltypesnopart STORED AS PARQUET
      LOCATION '%s'
    """ % (db_name, table_name, table_location))

  def __copy_file_to_test_table(self, src_path, table_location):
    """Copies the provided path to the test table, overwriting any previous file."""
    dst_path = "%s/%s" % (table_location, self.FILE_NAME)
    self.filesystem_client.copy(src_path, dst_path, overwrite=True)
