# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

import pytest
from subprocess import call
from subprocess import check_call
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

  def test_avro_c_lib_unicode_nulls(self, vector):
    """Test for IMPALA-1136 and IMPALA-2161 and unicode characters in the
    schema that were not handled correctly by the Avro C library.
    """
    result = self.execute_query("select * from functional_avro_snap.avro_unicode_nulls")
    comparison = self.execute_query("select * from functional.liketbl")

    # If we were not able to properly parse the Avro file schemas, then the result
    # would be empty.
    assert len(comparison.data) == len(result.data)
    for x in range(len(result.data)):
      assert comparison.data[x] == result.data[x]

  def test_avro_codegen_decoder(self, vector):
    """Test for IMPALA-2798, verify if Impala returns correct result if table schema
    doesn't match file schema.
    """
    self.run_test_case('QueryTest/avro-schema-resolution', vector)

  def test_avro_stale_schema(self, vector, unique_database):
    """Test for IMPALA-3314 and IMPALA-3513. Impalad shouldn't crash with stale avro
    metadata. Instead, should provide a meaningful error message.
    """
    # Create a table with default fileformat and later change it to avro using
    # alter sql. The query runs with stale metadata and a warning should be raised.
    # Invalidating metadata should cause the Avro schema to be properly set upon the
    # next metadata load.
    self.run_test_case('QueryTest/avro-stale-schema', vector, unique_database)
