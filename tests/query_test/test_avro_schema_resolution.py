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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfCatalogV2

# This test requires that testdata/avro_schema_resolution/create_table.sql has been run
class TestAvroSchemaResolution(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAvroSchemaResolution, cls).add_test_dimensions()
    # avro/snap is the only table format with a schema_resolution_test table
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'avro' and\
        v.get_value('table_format').compression_codec == 'snap')

  def test_avro_schema_resolution(self, vector, unique_database):
    self.run_test_case('QueryTest/avro-schema-resolution', vector, unique_database)

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

  @SkipIfCatalogV2.catalog_v1_test()
  def test_avro_schema_changes(self, vector, unique_database):
    """Test for IMPALA-3314 and IMPALA-3513: Impalad shouldn't crash with stale Avro
    metadata. Instead, should provide a meaningful error message.
    Test for IMPALA-3092: Impala should be able to query an Avro table after ALTER TABLE
    ... ADD COLUMN ...
    Test for IMPALA-3776: Fix describe formatted when changing Avro schema.
    """
    self.run_test_case('QueryTest/avro-schema-changes', vector, unique_database)
