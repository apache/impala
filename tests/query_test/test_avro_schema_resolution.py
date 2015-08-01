# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

# This test requires that testdata/avro_schema_resolution/create_table.sql has been run
class TestAvroSchemaResolution(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAvroSchemaResolution, cls).add_test_dimensions()
    # avro/snap is the only table format with a schema_resolution_test table
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'avro' and\
        v.get_value('table_format').compression_codec == 'snap')

  def test_avro_schema_resolution(self, vector):
    self.run_test_case('QueryTest/avro-schema-resolution', vector)
