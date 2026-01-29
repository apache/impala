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

# Tests the UNPIVOT clause.

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_parquet_dimension,
    default_client_protocol_dimension,
)


class TestUnpivotClause(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    cls.ImpalaTestMatrix.add_dimension(default_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        disable_codegen_options=[False], batch_sizes=[1, 3]))

  def test_unpivot_clause(self, unique_database, vector):
    self.run_test_case('QueryTest/unpivot-clause', vector, unique_database)


class TestUnpivotClauseOnLargeTable(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    cls.ImpalaTestMatrix.add_dimension(default_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        disable_codegen_options=[False], batch_sizes=[0]))

  def test_unpivot_clause(self, unique_database, vector):
    self.run_test_case('QueryTest/unpivot-clause-large', vector, unique_database)
