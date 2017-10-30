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

from copy import copy
import itertools
import math
import os
import pytest
import random
import shutil
import tempfile
import time
from subprocess import check_call
from tests.common.test_dimensions import create_exec_option_dimension_from_dict
from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.util.filesystem_utils import WAREHOUSE, get_fs_path
from tests.util.test_file_parser import QueryTestSectionReader

# Random fuzz testing of HDFS scanners. Existing tables for any HDFS file format
# are corrupted in random ways to flush out bugs with handling of corrupted data.
class TestScannersFuzzing(ImpalaTestSuite):
  # Use abort_on_error = False to ensure we scan all the files.
  ABORT_ON_ERROR_VALUES = [False]

  # Only run on all nodes - num_nodes=1 would not provide additional coverage.
  NUM_NODES_VALUES = [0]

  # Limit memory to avoid causing other concurrent tests to fail.
  MEM_LIMITS = ['512m']

  # Test the codegen and non-codegen paths.
  DISABLE_CODEGEN_VALUES = [True, False]

  # Test a range of batch sizes to exercise different corner cases.
  BATCH_SIZES = [0, 1, 16, 10000]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersFuzzing, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({
          'abort_on_error' : cls.ABORT_ON_ERROR_VALUES,
          'num_nodes' : cls.NUM_NODES_VALUES,
          'mem_limit' : cls.MEM_LIMITS}))
    # TODO: enable for more table formats once they consistently pass the fuzz test.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ('avro', 'parquet') or
        (v.get_value('table_format').file_format == 'text' and
          v.get_value('table_format').compression_codec in ('none', 'lzo')))


  def test_fuzz_alltypes(self, vector, unique_database):
    table_format = vector.get_value('table_format')
    src_db = QueryTestSectionReader.get_db_name(table_format)
    table_name = "alltypes"
    self.run_fuzz_test(vector, src_db, table_name, unique_database, table_name)

  def test_fuzz_decimal_tbl(self, vector, unique_database):
    table_format = vector.get_value('table_format')
    table_name = "decimal_tbl"
    if table_format.file_format == 'avro':
      table_name = "avro_decimal_tbl"
      if table_format.compression_codec != 'snap' or \
          table_format.compression_type != 'block':
        pytest.skip()
    elif table_format.file_format == 'text' and \
        table_format.compression_codec != 'none':
      # decimal_tbl is not present for these file formats
      pytest.skip()

    src_db = QueryTestSectionReader.get_db_name(table_format)
    self.run_fuzz_test(vector, src_db, table_name, unique_database, table_name, 10)

  def test_fuzz_nested_types(self, vector, unique_database):
    table_format = vector.get_value('table_format')
    table_name = "complextypestbl"
    src_db = QueryTestSectionReader.get_db_name(table_format)

    if table_format.file_format != 'parquet': pytest.skip()
    self.run_fuzz_test(vector, src_db, table_name, unique_database, table_name, 10)

  def test_fuzz_uncompressed_parquet(self, vector, unique_database):
    """Parquet tables in default schema are compressed, so in order
       to do the fuzz_test on an uncompressed parquet table, this test
       clones from an existing parquet table into a new table with
       no compression.
    """
    table_format = vector.get_value('table_format')
    if vector.get_value('table_format').compression_codec != 'none': pytest.skip()
    if table_format.file_format != 'parquet': pytest.skip()

    """Even when the compression_codec is none, the default compression type is snappy
       so compression codec is changed explicitly to be none.
    """
    self.execute_query("set compression_codec=none")

    tbl_list = ["alltypes", "decimal_tbl"]
    for orig_tbl_name in tbl_list:
      src_table_name = "parquet_uncomp_src_" + orig_tbl_name
      fuzz_table_name = "parquet_uncomp_dst_" + orig_tbl_name
      fq_tbl_name = unique_database + "." + src_table_name
      create_tbl = ("create table {0} stored as parquet as select * from"
          " functional_parquet.{1}".format(fq_tbl_name, orig_tbl_name))
      self.execute_query(create_tbl)
      self.run_fuzz_test(vector, unique_database, src_table_name, unique_database,
          fuzz_table_name, 10)

  # TODO: add test coverage for additional data types like char and varchar

  def run_fuzz_test(self, vector, src_db, src_table, fuzz_db, fuzz_table, num_copies=1):
    """ Do some basic fuzz testing: create a copy of an existing table with randomly
    corrupted files and make sure that we don't crash or behave in an unexpected way.
    'unique_database' is used for the table, so it will be cleaned up automatically.
    If 'num_copies' is set, create that many corrupted copies of each input file.
    SCANNER_FUZZ_SEED can be set in the environment to reproduce the result (assuming that
    input files are the same).
    SCANNER_FUZZ_KEEP_FILES can be set in the environment to keep the generated files.
    """
    # Create and seed a new random number generator for reproducibility.
    rng = random.Random()
    random_seed = os.environ.get("SCANNER_FUZZ_SEED") or time.time()
    LOG.info("Using random seed %d", random_seed)
    rng.seed(long(random_seed))

    tmp_table_dir = tempfile.mkdtemp(prefix="tmp-scanner-fuzz-%s" % fuzz_table,
        dir=os.path.join(os.environ['IMPALA_HOME'], "testdata"))

    self.execute_query("create table %s.%s like %s.%s" % (fuzz_db, fuzz_table,
        src_db, src_table))
    fuzz_table_location = get_fs_path("/test-warehouse/{0}.db/{1}".format(
        fuzz_db, fuzz_table))

    LOG.info("Generating corrupted version of %s in %s. Local working directory is %s",
        fuzz_table, fuzz_db, tmp_table_dir)

    # Find the location of the existing table and get the full table directory structure.
    fq_table_name = src_db + "." + src_table
    table_loc = self._get_table_location(fq_table_name, vector)
    check_call(['hdfs', 'dfs', '-copyToLocal', table_loc + "/*", tmp_table_dir])

    partitions = self.walk_and_corrupt_table_data(tmp_table_dir, num_copies, rng)
    for partition in partitions:
      self.execute_query('alter table {0}.{1} add partition ({2})'.format(
          fuzz_db, fuzz_table, ','.join(partition)))

    # Copy all of the local files and directories to hdfs.
    to_copy = ["%s/%s" % (tmp_table_dir, file_or_dir)
               for file_or_dir in os.listdir(tmp_table_dir)]
    check_call(['hdfs', 'dfs', '-copyFromLocal'] + to_copy + [fuzz_table_location])

    if "SCANNER_FUZZ_KEEP_FILES" not in os.environ:
      shutil.rmtree(tmp_table_dir)

    # Querying the corrupted files should not DCHECK or crash.
    self.execute_query("refresh %s.%s" % (fuzz_db, fuzz_table))
    # Execute a query that tries to read all the columns and rows in the file.
    # Also execute a count(*) that materializes no columns, since different code
    # paths are exercised.
    queries = [
        'select count(*) from (select distinct * from {0}.{1}) q'.format(
            fuzz_db, fuzz_table),
        'select count(*) from {0}.{1} q'.format(fuzz_db, fuzz_table)]

    for query, batch_size, disable_codegen in \
        itertools.product(queries, self.BATCH_SIZES, self.DISABLE_CODEGEN_VALUES):
      query_options = copy(vector.get_value('exec_option'))
      query_options['batch_size'] = batch_size
      query_options['disable_codegen'] = disable_codegen
      query_options['disable_codegen_rows_threshold'] = 0
      try:
        result = self.execute_query(query, query_options = query_options)
        LOG.info('\n'.join(result.log))
      except Exception as e:
        if 'memory limit exceeded' in str(e).lower():
          # Memory limit error should fail query.
          continue
        msg = "Should not throw error when abort_on_error=0: '{0}'".format(e)
        LOG.error(msg)
        # Parquet and compressed text can fail the query for some parse errors.
        # E.g. corrupt Parquet footer (IMPALA-3773) or a corrupt LZO index file
        # (IMPALA-4013).
        table_format = vector.get_value('table_format')
        if table_format.file_format != 'parquet' \
            and not (table_format.file_format == 'text' and
            table_format.compression_codec != 'none'):
          raise

  def walk_and_corrupt_table_data(self, tmp_table_dir, num_copies, rng):
    """ Walks a local copy of a HDFS table directory. Returns a list of partitions, each
    as a list of "key=val" pairs. Ensures there is 'num_copies' copies of each file,
    and corrupts each of the copies.
    """
    partitions = []
    # Iterate over the partitions and files we downloaded.
    for subdir, dirs, files in os.walk(tmp_table_dir):
      if '_impala_insert_staging' in subdir: continue
      if len(dirs) != 0: continue # Skip non-leaf directories

      rel_subdir = os.path.relpath(subdir, tmp_table_dir)
      if rel_subdir != ".":
        # Create metadata for any directory partitions.
        partitions.append(self.partitions_from_path(rel_subdir))

      # Corrupt all of the files that we find.
      for filename in files:
        filepath = os.path.join(subdir, filename)
        copies = [filepath]
        for copy_num in range(1, num_copies):
          copypath = os.path.join(subdir, "copy{0}_{1}".format(copy_num, filename))
          shutil.copyfile(filepath, copypath)
          copies.append(copypath)
        for filepath in copies:
          self.corrupt_file(filepath, rng)
    return partitions

  def partitions_from_path(self, relpath):
    """ Return a list of "key=val" parts from partitions inferred from the directory path.
    """
    reversed_partitions = []
    while relpath != '':
      relpath, suffix  = os.path.split(relpath)
      reversed_partitions.append(suffix)
    return reversed(reversed_partitions)

  def corrupt_file(self, path, rng):
    """ Corrupt the file at 'path' in the local file system in a randomised way using the
    random number generator 'rng'. Rewrites the file in-place.
    Logs a message to describe how the file was corrupted, so the error is reproducible.
    """
    with open(path, "rb") as f:
      data = bytearray(f.read())

    num_corruptions = rng.randint(0, int(math.log(len(data))))
    for _ in xrange(num_corruptions):
      flip_offset = rng.randint(0, len(data) - 1)
      flip_val = rng.randint(0, 255)
      LOG.info("corrupt file: Flip byte in {0} at {1} from {2} to {3}".format(
          path, flip_offset, data[flip_offset], flip_val))
      data[flip_offset] = flip_val

    if rng.random() < 0.4:
      truncation = rng.randint(0, len(data))
      LOG.info("corrupt file: Truncate {0} to {1}".format(path, truncation))
      data = data[:truncation]

    with open(path, "wb") as f:
      f.write(data)
