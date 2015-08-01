# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted tests for decimal type.
#
import logging
import pytest
from copy import copy
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestDecimalQueries(ImpalaTestSuite):
  BATCH_SIZES = [0, 1]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDecimalQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestDecimalQueries.BATCH_SIZES))

    # On CDH4, hive does not support decimal so we can't run these tests against
    # the other file formats. Enable them on C5.
    cls.TestMatrix.add_constraint(lambda v:\
        (v.get_value('table_format').file_format == 'text' and
         v.get_value('table_format').compression_codec == 'none') or
         v.get_value('table_format').file_format == 'parquet')

  def test_queries(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/decimal', new_vector)

# TODO: when we have a good way to produce Avro decimal data (e.g. upgrade Hive), we can
# run Avro through the same tests as above instead of using avro_decimal_tbl.
class TestAvroDecimalQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAvroDecimalQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'avro' and
         v.get_value('table_format').compression_codec == 'snap'))

  def test_avro_queries(self, vector):
    self.run_test_case('QueryTest/decimal_avro', vector)
