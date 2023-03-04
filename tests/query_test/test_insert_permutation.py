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

# Targeted Impala insert tests

from __future__ import absolute_import, division, print_function
from builtins import map
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_uncompressed_text_dimension)

class TestInsertQueriesWithPermutation(ImpalaTestSuite):
  """
  Tests for the column permutation feature of INSERT statements
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertQueriesWithPermutation, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value. This is needed should we decide
    # to run the insert tests in parallel (otherwise there will be two tests inserting
    # into the same table at the same time for the same file format).
    # TODO: When we do decide to run these tests in parallel we could create unique temp
    # tables for each test case to resolve the concurrency problems.
    # TODO: do we need to run with multiple file formats? This seems to be really
    # targeting FE behavior.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0]))
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_insert_permutation(self, vector):
    list(map(self.cleanup_db, ["insert_permutation_test"]))
    self.run_test_case('QueryTest/insert_permutation', vector)

  def teardown_method(self, method):
    list(map(self.cleanup_db, ["insert_permutation_test"]))
