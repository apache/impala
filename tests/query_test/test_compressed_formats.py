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
from builtins import range
import math
import os
import pytest
import random
import struct
import subprocess
from os.path import join

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.test_result_verifier import verify_query_result_is_equal
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import get_fs_path, WAREHOUSE


# (file extension, table suffix) pairs
compression_formats = [
  ('.bz2', 'bzip'),
  ('.deflate', 'def'),
  ('.gz', 'gzip'),
  ('.snappy', 'snap'),
 ]

compression_extensions = ['.bz2', '.deflate', '.gz', '.snappy']


class TestCompressedFormatsBase(ImpalaTestSuite):
  """
  Base class to provide utility functions for testing support for compressed
  data files.
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCompressedFormatsBase, cls).add_test_dimensions()

  def _copy_and_query_compressed_file(self, unique_database, table_name, db_suffix,
      dest_extension, expected_error=None):
    """
    This is a utility function to test the behavior for compressed file formats
    with different file extensions. It creates a new table in the unique_database
    as a copy of the provided table_name from the functional schema with the
    specified db_suffix. It copies the first file from the sourcce table to the new
    table as file_basename + dest_extension. It then runs a query on the
    new table. Unless expected_error is set, it expects the query to run successfully.
    """
    # Calculate locations for the source table
    base_dir = WAREHOUSE
    src_table = "functional{0}.{1}".format(db_suffix, table_name)
    src_table_dir = join(base_dir, table_name + db_suffix)
    file_basename = self.filesystem_client.ls(src_table_dir)[0]
    src_file = join(src_table_dir, file_basename)

    # Calculate locations for the destination table
    dest_table_dir = "{2}/{0}.db/{1}".format(unique_database, table_name, WAREHOUSE)
    dest_table = "{0}.{1}".format(unique_database, table_name)
    dest_file = join(dest_table_dir, file_basename + dest_extension)

    # Use a specific location to avoid any interactions with Hive behavior changes.
    drop_cmd = "DROP TABLE IF EXISTS {0};".format(dest_table)
    create_cmd = "CREATE TABLE {0} LIKE {1} LOCATION \'{2}\';".format(
      dest_table, src_table, dest_table_dir)

    # Create the table and copy in the data file
    self.client.execute(drop_cmd)
    self.client.execute(create_cmd)
    self.filesystem_client.copy(src_file, dest_file, overwrite=True)
    # Try to read the compressed file with extension
    query = "select count(*) from {0};".format(dest_table)
    try:
      # Need to refresh the metadata to see the file copied in.
      self.client.execute("refresh {0}".format(dest_table))
      result = self.execute_scalar(query)
      # Fail iff we expected an error
      assert expected_error is None, 'Query is expected to fail'
      assert result and int(result) > 0
    except Exception as e:
      error_msg = str(e)
      print(error_msg)
      if expected_error is None or expected_error not in error_msg:
        print("Unexpected error:\n{0}".format(error_msg))
        raise
    finally:
      self.client.execute(drop_cmd)
      self.filesystem_client.delete_file_dir(dest_file)


class TestCompressedNonText(TestCompressedFormatsBase):
  """Tests behavior for compressed non-text formats (avro, rc, seq)."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestCompressedNonText, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.clear()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('file_format', *['rc', 'seq', 'avro']))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('compression_format', *compression_formats))

  def test_insensitivity_to_extension(self, vector, unique_database):
    """
    Avro, RC, and Sequence files do not use the file extension to determine the
    type of compression. This verifies that they work regardless of the
    extension.
    """
    file_format = vector.get_value('file_format')
    right_extension, suffix = vector.get_value('compression_format')
    # Avro is only loaded in a subset of the compression types. Bail out for
    # the ones that are not loaded.
    if file_format == 'avro' and suffix not in ['def', 'snap']:
      pytest.xfail('Avro is only created for Deflate and Snappy compression codecs')
    db_suffix = '_{0}_{1}'.format(file_format, suffix)

    # Pick one wrong extension randomly
    wrong_extensions = [ext for ext in compression_extensions if ext != right_extension]
    random.shuffle(wrong_extensions)
    wrong_extension = wrong_extensions[0]
    # Test with the "right" extension that matches the file's compression, one wrong
    # extension, and no extension. By default, Hive does not use a file extension.
    for ext in [right_extension, wrong_extension, ""]:
      self._copy_and_query_compressed_file(
        unique_database, 'tinytable', db_suffix, ext)


class TestCompressedText(TestCompressedFormatsBase):
  """
  Tests behavior for compressed text files, which determine the compression codec from
  the file extension.
  """

  @classmethod
  def add_test_dimensions(cls):
    super(TestCompressedText, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.clear()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('file_format', *['text', 'json']))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('compression_format', *compression_formats))

  def test_correct_extension(self, vector, unique_database):
    """
    Text files use the file extension to determine the type of compression.
    By default, Hive creates files with the appropriate file extension.
    This verifies the positive case that the correct extension works.

    This is a somewhat trivial test. However, it runs with the core exploration
    strategy and runs on all filesystems. It verifies formats on core that are
    otherwise limited to exhaustive. That is important for coverage on non-HDFS
    filesystems like s3.
    """
    file_format = vector.get_value('file_format')
    extension, suffix = vector.get_value('compression_format')
    db_suffix = '_{0}_{1}'.format(file_format, suffix)
    self._copy_and_query_compressed_file(
      unique_database, 'tinytable', db_suffix, extension)


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
        (v.get_value('table_format').file_format == 'text' and
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
        (v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'snap'))

  def teardown_method(self, method):
    self.__drop_test_table()

  def __generate_file(self, file_name, file_size):
    """Generate file with random data and a specified size."""

    # Read the payload compressed using snappy. The compressed payload
    # is generated from a string of 50176 bytes.
    payload_size = 50176
    hdfs_cat = subprocess.Popen(["hadoop", "fs", "-cat",
        "%s/compressed_payload.snap" % WAREHOUSE], stdout=subprocess.PIPE)
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
    num_blocks_per_chunk = self.CHUNK_SIZE // (compressed_size + 4)
    # Total uncompressed size of a nested structure.
    total_chunk_size = num_blocks_per_chunk * payload_size

    hdfs_put = subprocess.Popen(["hdfs", "dfs", "-put", "-d", "-f", "-", file_name],
        stdin=subprocess.PIPE, bufsize=-1)
    for i in range(num_chunks):
      hdfs_put.stdin.write(struct.pack('>i', total_chunk_size))
      for j in range(num_blocks_per_chunk):
        hdfs_put.stdin.write(struct.pack('>i', compressed_size))
        hdfs_put.stdin.write(compressed_payload)
    hdfs_put.stdin.close()
    hdfs_put.wait()

  def test_query_large_file(self, vector):
    self.__create_test_table()
    dst_path = "%s/%s" % (self.TABLE_LOCATION, self.FILE_NAME)
    file_size = self.MAX_FILE_SIZE
    self.__generate_file(dst_path, file_size)
    self.client.execute("refresh %s" % self.TABLE_NAME)

    # Query the table
    self.client.execute("select * from %s limit 1" % self.TABLE_NAME)

  def __create_test_table(self):
    self.__drop_test_table()
    self.client.execute("CREATE TABLE %s (col string) "
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
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'bzip')

  def test_bzip2_streaming(self, vector):
    self.run_test_case('QueryTest/text-bzip-scan', vector)


class TestReadZtsdLibCompressedFile(ImpalaTestSuite):
  """
  Test that file compressed by zstd standard library can be read by Impala
  """
  COMPRESSED_TABLE_NAME = "zstdlib_compressed_table"
  UNCOMPRESSED_TABLE_NAME = "uncompressed_table"
  COMPRESSED_TABLE_LOCATION = get_fs_path("/test-warehouse/zstdlib_compressed_file")
  UNCOMPRESSED_TABLE_LOCATION = get_fs_path("/test-warehouse/uncompressed_file")
  COMPRESSED_LOCAL_FILE_PATH = "testdata/data/text_large_zstd.zst"
  UNCOMPRESSED_LOCAL_FILE_PATH = "testdata/data/text_large_zstd.txt"

  IMPALA_HOME = os.environ['IMPALA_HOME']

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestReadZtsdLibCompressedFile, cls).add_test_dimensions()

    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'zstd'))

  def __generate_file(self, local_file_location, table_location):
    """
    Make directory in HDFS and copy zstd standard library compressed file to HDFS
    Lib compressed file extension has to be zst to be readable.
    Copy original uncompressed file to HDFS as well for row comparison.
    """
    source_local_file = os.path.join(self.IMPALA_HOME, local_file_location)
    subprocess.check_call(["hadoop", "fs", "-put", source_local_file, table_location])

  def __create_test_table(self, table_name, location):
    self.client.execute("DROP TABLE IF EXISTS %s" % table_name)
    self.client.execute("CREATE TABLE %s (col string) LOCATION '%s'"
        % (table_name, location))

  def test_query_large_file(self):
    self.__create_test_table(self.COMPRESSED_TABLE_NAME,
        self.COMPRESSED_TABLE_LOCATION)
    self.__create_test_table(self.UNCOMPRESSED_TABLE_NAME,
         self.UNCOMPRESSED_TABLE_LOCATION)
    self.__generate_file(self.COMPRESSED_LOCAL_FILE_PATH,
        self.COMPRESSED_TABLE_LOCATION)
    self.__generate_file(self.UNCOMPRESSED_LOCAL_FILE_PATH,
        self.UNCOMPRESSED_TABLE_LOCATION)
    self.client.execute("refresh %s" % self.COMPRESSED_TABLE_NAME)
    self.client.execute("refresh %s" % self.UNCOMPRESSED_TABLE_NAME)

    # Read from compressed table
    result = self.client.execute("select count(*) from %s" % self.COMPRESSED_TABLE_NAME)
    result_uncompressed = self.client.execute("select count(*) from %s" %
        self.UNCOMPRESSED_TABLE_NAME)
    assert int(result.get_data()) == int(result_uncompressed.get_data())

    # Read top 10k rows from compressed table and uncompressed table, compare results
    base_result = self.execute_query_expect_success(self.client,
        "select * from {0} order by col limit 10000".format(self.UNCOMPRESSED_TABLE_NAME))
    test_result = self.execute_query_expect_success(self.client,
        "select * from {0} order by col limit 10000".format(self.COMPRESSED_TABLE_NAME))
    verify_query_result_is_equal(test_result.data, base_result.data)
