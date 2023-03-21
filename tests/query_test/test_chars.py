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
from copy import deepcopy

import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (create_exec_option_dimension,
    create_client_protocol_dimension, hs2_parquet_constraint, hs2_text_constraint)
from tests.util.filesystem_utils import get_fs_path

class TestStringQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestStringQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension(disable_codegen_options=[False, True]))
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['text', 'json']
        and v.get_value('table_format').compression_codec in ['none'])
    # Run these queries through both beeswax and HS2 to get coverage of CHAR/VARCHAR
    # returned via both protocols.
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_constraint(hs2_text_constraint)

  def test_chars(self, vector):
    self.run_test_case('QueryTest/chars', vector)

  def test_chars_tmp_tables(self, vector, unique_database):
    if vector.get_value('protocol') in ['hs2', 'hs2-http']:
      pytest.skip("HS2 does not return row counts for inserts")
    # Tests that create temporary tables and require a unique database.
    self.run_test_case('QueryTest/chars-tmp-tables', vector, unique_database)

  # Regression tests for IMPALA-10753.
  def test_chars_values_stmt(self, vector, unique_database):
    if vector.get_value('protocol') in ['hs2', 'hs2-http']:
      pytest.skip("HS2 does not return row counts for inserts")
    vector = deepcopy(vector)
    vector.get_value('exec_option')['values_stmt_avoid_lossy_char_padding'] = True
    self.run_test_case('QueryTest/chars-values-stmt-no-lossy-char-padding',
        vector, unique_database)

    vector.get_value('exec_option')['values_stmt_avoid_lossy_char_padding'] = False
    self.run_test_case('QueryTest/chars-values-stmt-lossy-char-padding',
        vector, unique_database)

class TestCharFormats(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCharFormats, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension(disable_codegen_options=[False, True]))
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format in ['avro'] and
        v.get_value('table_format').compression_codec in ['snap']) or
        v.get_value('table_format').file_format in ['parquet'] or
        v.get_value('table_format').file_format in ['orc'] or
        (v.get_value('table_format').file_format in ['text', 'json']
         and v.get_value('table_format').compression_codec in ['none']))
    # Run these queries through both beeswax and HS2 to get coverage of CHAR/VARCHAR
    # returned via both protocols.
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_constraint(hs2_parquet_constraint)

  def test_char_format(self, vector):
    self.run_test_case('QueryTest/chars-formats', vector)

  def test_string_literal(self, vector):
    self.run_test_case('QueryTest/string-literals', vector)
