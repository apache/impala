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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (create_exec_option_dimension,
    create_client_protocol_dimension, hs2_parquet_constraint)


class TestUtf8StringFunctions(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUtf8StringFunctions, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension(disable_codegen_options=[False, True]))
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'] and
        v.get_value('table_format').compression_codec in ['none'])
    # Run these queries through both beeswax and HS2 to get coverage of CHAR/VARCHAR
    # returned via both protocols.
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_constraint(hs2_parquet_constraint)

  def test_string_functions(self, vector):
    self.run_test_case('QueryTest/utf8-string-functions', vector)
