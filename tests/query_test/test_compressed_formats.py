# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

import os
import pytest
from os.path import join
from subprocess import call
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfS3, SkipIfIsilon

# (file extension, table suffix) pairs
compression_formats = [
  ('.bz2',     'bzip'),
  ('.deflate', 'def'),
  ('.gz',      'gzip'),
  ('.snappy',  'snap'),
 ]


# Missing Coverage: Compressed data written by Hive is queriable by Impala on a non-hdfs
# filesystem.
@SkipIfS3.hive
@SkipIfIsilon.hive
class TestCompressedFormats(ImpalaTestSuite):
  """
  Tests that we support compressed RC, sequence and text files and that unsupported
  formats fail gracefully (see IMPALA-14: Files with .gz extension reported as 'not
  supported').
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCompressedFormats, cls).add_test_dimensions()
    cls.TestMatrix.clear()
    cls.TestMatrix.add_dimension(\
        TestDimension('file_format', *['rc', 'seq', 'text']))
    cls.TestMatrix.add_dimension(\
        TestDimension('compression_format', *compression_formats))
    if cls.exploration_strategy() == 'core':
      # Don't run on core.  This test is very slow and we are unlikely
      # to regress here.
      cls.TestMatrix.add_constraint(lambda v: False);

  @pytest.mark.execute_serially
  def test_compressed_formats(self, vector):
    file_format = vector.get_value('file_format')
    extension, suffix = vector.get_value('compression_format')
    if file_format in ['rc', 'seq']:
      # Test that compressed RC/sequence files are supported.
      db_suffix = '_%s_%s' % (file_format, suffix)
      self._copy_and_query_compressed_file(
       'tinytable', db_suffix, suffix, '000000_0', extension)
    elif file_format is 'text':
      # TODO: How about LZO?
      if suffix in ['gzip', 'snap', 'bzip']:
        # Test that {gzip,snappy,bzip}-compressed text files are supported.
        db_suffix = '_%s_%s' % (file_format, suffix)
        self._copy_and_query_compressed_file(
          'tinytable', db_suffix, suffix, '000000_0', extension)
      else:
        # Deflate-compressed (['def']) text files (or at least text files with a
        # compressed extension) have not been tested yet.
        pytest.skip("Skipping the text/def tests")
    else:
      assert False, "Unknown file_format: %s" % file_format

  # TODO: switch to using hive metastore API rather than hive shell.
  def _copy_and_query_compressed_file(self, table_name, db_suffix, compression_codec,
                                     file_name, extension, expected_error=None):
    # We want to create a test table with a compressed file that has a file
    # extension. We'll do this by making a copy of an existing table with hive.
    base_dir = '/test-warehouse'
    src_table = 'functional%s.%s' % (db_suffix, table_name)
    src_table_dir = "%s%s" % (table_name, db_suffix)
    src_table_dir = join(base_dir, src_table_dir)
    src_file = join(src_table_dir, file_name)

    # Make sure destination table uses suffix, even if use_suffix=False, so
    # unique tables are created for each compression format
    dest_table = '%s_%s_copy' % (table_name, compression_codec)
    dest_table_dir = join(base_dir, dest_table)
    dest_file = join(dest_table_dir, file_name + extension)

    drop_cmd = 'DROP TABLE IF EXISTS %s;' % (dest_table)
    hive_cmd = drop_cmd + 'CREATE TABLE %s LIKE %s;' % (dest_table, src_table)

    # Create the table
    call(["hive", "-e", hive_cmd]);
    call(["hadoop", "fs", "-cp", src_file, dest_file])
    # Try to read the compressed file with extension
    query = 'select count(*) from %s' % dest_table
    try:
      # Need to invalidate the metadata because the table was created external to Impala.
      self.client.execute("invalidate metadata %s" % dest_table)
      result = self.execute_scalar(query)
      # Fail iff we expected an error
      assert expected_error is None, 'Query is expected to fail'
    except Exception as e:
      error_msg = str(e)
      print error_msg
      if expected_error is None or expected_error not in error_msg:
        print "Unexpected error:\n%s", error_msg
        raise
    finally:
      call(["hive", "-e", drop_cmd]);


@SkipIfS3.insert
class TestTableWriters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTableWriters, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # This class tests many formats, but doesn't use the contraints
    # Each format is tested within one test file, we constrain to text/none
    # as each test file only needs to be run once.
    cls.TestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format =='text' and
        v.get_value('table_format').compression_codec == 'none'))

  def test_seq_writer(self, vector):
    # TODO debug this test, same as seq writer.
    # This caused by a zlib failure. Suspected cause is too small a buffer
    # passed to zlib for compression; similar to IMPALA-424
    pytest.skip()
    self.run_test_case('QueryTest/seq-writer', vector)

  def test_avro_writer(self, vector):
    self.run_test_case('QueryTest/avro-writer', vector)

  def test_text_writer(self, vector):
    # TODO debug this test, same as seq writer.
    pytest.skip()
    self.run_test_case('QueryTest/text-writer', vector)
