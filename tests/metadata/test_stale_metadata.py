# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

import random
from subprocess import check_call
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.impala_test_suite import create_single_exec_option_dimension
from tests.util.filesystem_utils import get_fs_path

class TestRewrittenFile(ImpalaTestSuite):
  """Tests that we gracefully handle when a file in HDFS is rewritten outside of Impala
  without issuing "invalidate metadata"."""

  # Create a unique database name so we can run multiple instances of this test class in
  # parallel
  DATABASE = "test_written_file_" + str(random.randint(0, 10**10))

  TABLE_NAME = "alltypes_rewritten_file"
  TABLE_LOCATION = get_fs_path("/test-warehouse/%s" % DATABASE)
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
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # TODO: add more file formats
    cls.TestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  @classmethod
  def setup_class(cls):
    super(TestRewrittenFile, cls).setup_class()
    cls.cleanup_db(cls.DATABASE)
    cls.client.execute("create database if not exists " + cls.DATABASE)

  @classmethod
  def teardown_class(cls):
    cls.cleanup_db(cls.DATABASE)
    super(TestRewrittenFile, cls).teardown_class()

  def teardown_method(self, method):
    self.__drop_test_table()

  def __overwrite_file_and_query(self, vector, old_file, new_file, expected_error,
      expected_new_count):
    """Rewrites 'old_file' with 'new_file' without invalidating metadata and verifies that
    querying the table results in the expected error. 'expected_error' only needs to be a
    substring of the full error message."""
    self.__create_test_table()

    # First copy in 'old_file' and refresh the cached file metadata.
    self.__copy_file_to_test_table(old_file)
    self.client.execute("refresh %s" % self.__full_table_name())

    # Then overwrite 'old_file' with 'new_file', and don't invalidate metadata.
    self.__copy_file_to_test_table(new_file)

    # Query the table and check for expected error.
    try:
      result = self.client.execute("select * from %s" % self.__full_table_name())
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert expected_error in str(e)

    # Refresh the table and make sure we get results
    self.client.execute("refresh %s" % self.__full_table_name())
    result = self.client.execute("select count(*) from %s" % self.__full_table_name())
    assert result.data == [str(expected_new_count)]

  def test_new_file_shorter(self, vector):
    """Rewrites an existing file with a new shorter file."""
    # Full error is something like:
    #   Metadata for file '...' appears stale. Try running "refresh
    #   test_written_file_xxx.alltypes_rewritten_file" to reload the file metadata.
    self.__overwrite_file_and_query(vector, self.LONG_FILE, self.SHORT_FILE,
        'appears stale.', self.SHORT_FILE_NUM_ROWS)

  def test_new_file_longer(self, vector):
    """Rewrites an existing file with a new longer file."""
    # Full error is something like:
    #   File '...' has an invalid version number: ff4C
    #   This could be due to stale metadata. Try running "refresh
    #   test_written_file_xxx.alltypes_rewritten_file".
    self.__overwrite_file_and_query(vector, self.SHORT_FILE, self.LONG_FILE,
        'invalid version number', self.LONG_FILE_NUM_ROWS)

  def test_delete_file(self, vector):
    """Deletes an existing file without refreshing metadata."""
    self.__create_test_table()

    # Copy in a file and refresh the cached file metadata.
    self.__copy_file_to_test_table(self.LONG_FILE)
    self.client.execute("refresh %s" % self.__full_table_name())

    # Delete the file without refreshing metadata.
    check_call(["hadoop", "fs", "-rm", self.TABLE_LOCATION + '/*'], shell=False)

    # Query the table and check for expected error.
    try:
      result = self.client.execute("select * from %s" % self.__full_table_name())
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert 'No such file or directory' in str(e)

    # Refresh the table and make sure we get results
    self.client.execute("refresh %s" % self.__full_table_name())
    result = self.client.execute("select count(*) from %s" % self.__full_table_name())
    assert result.data == ['0']

  def __create_test_table(self):
    self.__drop_test_table()
    self.client.execute("""
      CREATE TABLE %s LIKE functional.alltypesnopart STORED AS PARQUET
      LOCATION '%s'
    """ % (self.__full_table_name(), self.TABLE_LOCATION))

  def __drop_test_table(self):
    self.client.execute("DROP TABLE IF EXISTS %s" % self.__full_table_name())

  def __copy_file_to_test_table(self, src_path):
    """Copies the provided path to the test table, overwriting any previous file."""
    dst_path = "%s/%s" % (self.TABLE_LOCATION, self.FILE_NAME)
    check_call(["hadoop", "fs", "-cp", "-f", src_path, dst_path], shell=False)

  def __full_table_name(self):
    return "%s.%s" % (self.DATABASE, self.TABLE_NAME)
