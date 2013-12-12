#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This test suite validates the scanners by running queries against ALL file formats and
# their permutations (e.g. compression codec/compression type). This works by exhaustively
# generating the table format test vectors for this specific test suite. This way, other
# tests can run with the normal exploration strategy and the overall test runtime doesn't
# explode.

import logging
import pytest
from copy import deepcopy
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.util.test_file_parser import *
from tests.common.test_dimensions import create_single_exec_option_dimension

class TestScannersAllTableFormats(ImpalaTestSuite):
  BATCH_SIZES = [0, 1, 16]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersAllTableFormats, cls).add_test_dimensions()
    # Exhaustively generate all table format vectors. This can still be overridden
    # using the --table_formats flag.
    cls.TestMatrix.add_dimension(cls.create_table_info_dimension('exhaustive'))
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestScannersAllTableFormats.BATCH_SIZES))

  def test_scanners(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/scanners', new_vector)


# Test case to verify the scanners work properly when the table metadata (specifically the
# number of columns in the table) does not match the number of columns in the data file.
class TestUnmatchedSchema(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUnmatchedSchema, cls).add_test_dimensions()
    # TODO: Does it add anything to enumerate all the supported compression codecs
    # for each table format?
    cls.TestMatrix.add_dimension(cls.create_table_info_dimension('exhaustive'))
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # Avro has a more advanced schema evolution process which is covered in more depth
    # in the test_avro_schema_evolution test suite.
    cls.TestMatrix.add_constraint(\
        lambda v: v.get_value('table_format').file_format != 'avro')

  def __get_table_location(self, table_name, vector):
    result = self.execute_query_using_client(self.client,
        "describe formatted %s" % table_name, vector)
    for row in result.data:
      if 'Location:' in row:
        return row.split('\t')[1]
    # This should never happen.
    assert 0, 'Unable to get location for table: ' + table_name

  def __create_test_table(self, vector):
    """
    Creates the test table

    Cannot be done in a setup method because we need access to the current test vector
    """
    self.__drop_test_table(vector)
    self.execute_query_using_client(self.client,
        "create external table jointbl_test like jointbl", vector)

    # Update the location of the new table to point the same location as the old table
    location = self.__get_table_location('jointbl', vector)
    self.execute_query_using_client(self.client,
        "alter table jointbl_test set location '%s'" % location, vector)

  def __drop_test_table(self, vector):
    self.execute_query_using_client(self.client,
        "drop table if exists jointbl_test", vector)

  def test_unmatched_schema(self, vector):
    table_format = vector.get_value('table_format')
    # jointbl has no columns with unique values. When loaded in hbase, the table looks
    # different, as hbase collapses duplicates.
    if table_format.file_format == 'hbase':
      pytest.skip()
    self.__create_test_table(vector)
    self.run_test_case('QueryTest/test-unmatched-schema', vector)
    self.__drop_test_table(vector)


# Tests that scanners can read a single-column, single-row, 10MB table
class TestWideRow(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestWideRow, cls).add_test_dimensions()
    # I can't figure out how to load a huge row into hbase
    cls.TestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format != 'hbase')

  def test_wide_row(self, vector):
    new_vector = deepcopy(vector)
    # Use a 5MB scan range, so we will have to perform 5MB of sync reads
    new_vector.get_value('exec_option')['max_scan_range_length'] = 5 * 1024 * 1024
    # We need > 10 MB of memory because we're creating extra buffers:
    # - 10 MB table / 5 MB scan range = 2 scan ranges, each of which may allocate ~20MB
    # - Sync reads will allocate ~5MB of space
    # The 80MB value used here was determined empirically by raising the limit until the query
    # succeeded for all file formats -- I don't know exactly why we need this much.
    # TODO: figure out exact breakdown of memory usage (IMPALA-681)
    new_vector.get_value('exec_option')['mem_limit'] = 80 * 1024 * 1024
    self.run_test_case('QueryTest/wide-row', new_vector)

class TestParquet(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquet, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet(self, vector):
    self.run_test_case('QueryTest/parquet', vector)
