#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

from subprocess import call
from tests.common.test_vector import *
from tests.common.impala_test_suite import ImpalaTestSuite

expected_error_string = 'Compressed text files are not supported'
compressed_text_extensions = ['snappy', 'deflate', 'gz', 'bz2']

# This is test to validate that unsupported formats fail gracefully
class TestUnsupportedFormats(ImpalaTestSuite):
  @classmethod
  def get_dataset(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUnsupportedFormats, cls).add_test_dimensions()

    cls.TestMatrix.clear()
    # Add compressed text dimension more dimensions
    cls.TestMatrix.add_dimension(\
        TestDimension('test_format', *compressed_text_extensions))

  # TODO: switch to using hive metastore API rather than hive shell.
  def test_compressed_text(self, vector):
    # We want to create a test table in hive and add a single empty file with the
    # compressed extension.
    extension = vector.get_value('test_format')
    table_name = 'compressed_text_%s' % extension
    table_dir = '/test-warehouse/' + table_name
    drop_cmd = 'DROP TABLE IF EXISTS %s;' % table_name
    hive_cmd = drop_cmd + 'CREATE TABLE %s(col int);' % table_name
    text_file = '%s/file.%s' % (table_dir, extension)
    
    # Create the table
    call(["hive", "-e", hive_cmd]);
    # Place the dummy file
    call(["hadoop", "fs", "-touchz", text_file])
    query = 'select count(*) from %s' % table_name
    try:
      # Need to refresh
      self.client.refresh()
      result = self.execute_scalar(query)
      call(["hive", "-e", drop_cmd]);
      assert(0), 'Query is expected to fail'
    except Exception as e:
      call(["hive", "-e", drop_cmd]);
      error_msg = str(e)
      if not expected_error_string in error_msg:
        print "Unexpected error:\n%s", error_msg
        raise


