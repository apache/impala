#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates limit on scan nodes
#
import logging
import pytest
from tests.common.test_vector import *
from tests.util.test_file_parser import QueryTestSectionReader
from tests.common.impala_test_suite import ImpalaTestSuite

# TODO: add larger limits when IMP-660 is fixed
limit_values = [0, 1, 2, 3, 4, 5, 10] 
queries = ["select * from tpch.lineitem$TABLE limit %d"]
# TODO: we should be able to run count(*) in setup rather than hardcoding the values
# but I have no idea how to do this with this framework.
total_rows = 6001215

class TestLimit(ImpalaTestSuite):
  @classmethod
  def get_dataset(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLimit, cls).add_test_dimensions()

    # Add two more dimensions
    cls.TestMatrix.add_dimension(TestDimension('limit_value', *limit_values))
    cls.TestMatrix.add_dimension(TestDimension('query', *queries))

  def test_limit(self, vector):
    # We can't validate the rows that are returned since that is non-deterministic.
    # This is why this is a python test rather than a .test.
    limit = vector.get_value('limit_value')
    expected_num_rows = min(limit, total_rows)
    query_string = vector.get_value('query') % limit
    query_string = QueryTestSectionReader.replace_table_suffix(
        query_string, vector.get_value('table_format'))
    result = self.execute_query(query_string, vector.get_value('exec_option'))
    assert(len(result.data) == expected_num_rows)
