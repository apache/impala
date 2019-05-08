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

import math
import pytest
import struct
import subprocess
from os.path import join
from subprocess import call

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon, SkipIfLocal
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import get_fs_path

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
@SkipIfABFS.hive
@SkipIfADLS.hive
@SkipIfIsilon.hive
@SkipIfLocal.hive
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
    cls.ImpalaTestMatrix.clear()
    cls.ImpalaTestMatrix.add_dimension(\
        ImpalaTestDimension('file_format', *['rc', 'seq', 'text']))
    cls.ImpalaTestMatrix.add_dimension(\
        ImpalaTestDimension('compression_format', *compression_formats))
    if cls.exploration_strategy() == 'core':
      # Don't run on core.  This test is very slow and we are unlikely
      # to regress here.
      cls.ImpalaTestMatrix.add_constraint(lambda v: False);

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
    self.run_stmt_in_hive(hive_cmd)
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
      self.run_stmt_in_hive(drop_cmd)

class TestUnsupportedTableWriters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUnsupportedTableWriters, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # This class tests different formats, but doesn't use constraints.
    # The constraint added below is only to make sure that the test file runs once.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format =='text' and
        v.get_value('table_format').compression_codec == 'none'))

  def test_error_message(self, vector, unique_database):
    # Tests that an appropriate error message is displayed for unsupported writers like
    # compressed text, avro and sequence.
    self.run_test_case('QueryTest/unsupported-writers', vector, unique_database)

@pytest.mark.execute_serially
class TestLargeCompressedFile(ImpalaTestSuite):
  """
  Tests that Impala handles compressed files in HDFS larger than 1GB.
  This test creates a 2GB test data file and loads it into a table.
  """
  TABLE_NAME = "large_compressed_file"
  TABLE_LOCATION = get_fs_path("/test-warehouse/large_compressed_file")
  """
  Name the file with ".snappy" extension to let scanner treat it as
  a snappy block compressed file.
  """
  FILE_NAME = "largefile.snappy"
  # Maximum uncompressed size of an outer block in a snappy block compressed file.
  CHUNK_SIZE = 1024 * 1024 * 1024
  # Limit the max file size to 2GB or too much memory may be needed when
  # uncompressing the buffer. 2GB is sufficient to show that we support
  # size beyond maximum 32-bit signed value.
  MAX_FILE_SIZE = 2 * CHUNK_SIZE

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLargeCompressedFile, cls).add_test_dimensions()

    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip("skipping if it's not exhaustive test.")
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format =='text' and
        v.get_value('table_format').compression_codec == 'snap'))

  def teardown_method(self, method):
    self.__drop_test_table()

  def __generate_file(self, file_name, file_size):
    """Generate file with random data and a specified size."""

    # Read the payload compressed using snappy. The compressed payload
    # is generated from a string of 50176 bytes.
    payload_size = 50176
    hdfs_cat = subprocess.Popen(["hadoop", "fs", "-cat",
        "/test-warehouse/compressed_payload.snap"], stdout=subprocess.PIPE)
    compressed_payload = hdfs_cat.stdout.read()
    compressed_size = len(compressed_payload)
    hdfs_cat.stdout.close()
    hdfs_cat.wait()

    # The layout of a snappy-block compressed file is one or more
    # of the following nested structure which is called "chunk" in
    # the code below:
    #
    # - <big endian 32-bit value encoding the uncompresed size>
    # - one or more blocks of the following structure:
    #   - <big endian 32-bit value encoding the compressed size>
    #   - <raw bits compressed by snappy algorithm>

    # Number of nested structures described above.
    num_chunks = int(math.ceil(file_size / self.CHUNK_SIZE))
    # Number of compressed snappy blocks per chunk.
    num_blocks_per_chunk = self.CHUNK_SIZE / (compressed_size + 4)
    # Total uncompressed size of a nested structure.
    total_chunk_size = num_blocks_per_chunk * payload_size

    hdfs_put = subprocess.Popen(["hadoop", "fs", "-put", "-f", "-", file_name],
        stdin=subprocess.PIPE, bufsize=-1)
    for i in range(num_chunks):
      hdfs_put.stdin.write(struct.pack('>i', total_chunk_size))
      for j in range(num_blocks_per_chunk):
        hdfs_put.stdin.write(struct.pack('>i', compressed_size))
        hdfs_put.stdin.write(compressed_payload)
    hdfs_put.stdin.close()
    hdfs_put.wait()

  def test_query_large_file(self, vector):
    self.__create_test_table();
    dst_path = "%s/%s" % (self.TABLE_LOCATION, self.FILE_NAME)
    file_size = self.MAX_FILE_SIZE
    self.__generate_file(dst_path, file_size)
    self.client.execute("refresh %s" % self.TABLE_NAME)

    # Query the table
    result = self.client.execute("select * from %s limit 1" % self.TABLE_NAME)

  def __create_test_table(self):
    self.__drop_test_table()
    self.client.execute("CREATE TABLE %s (col string) " \
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '%s'"
        % (self.TABLE_NAME, self.TABLE_LOCATION))

  def __drop_test_table(self):
    self.client.execute("DROP TABLE IF EXISTS %s" % self.TABLE_NAME)

class TestBzip2Streaming(ImpalaTestSuite):
  MAX_SCAN_RANGE_LENGTHS = [0, 5]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestBzip2Streaming, cls).add_test_dimensions()

    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip("skipping if it's not exhaustive test.")
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('max_scan_range_length', *cls.MAX_SCAN_RANGE_LENGTHS))
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'bzip')

  def test_bzip2_streaming(self, vector):
    self.run_test_case('QueryTest/text-bzip-scan', vector)
