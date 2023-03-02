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
import pytest
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_vector import ImpalaTestDimension
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import create_exec_option_dimension

class TestLocalTzConversion(CustomClusterTestSuite):
  """Tests for --use_local_tz_for_unix_timestamp_conversions"""

  @classmethod
  def add_test_dimensions(cls):
    super(TestLocalTzConversion, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False, True], batch_sizes=[0]))
    # Test with and without expr rewrites to cover regular expr evaluations
    # as well as constant folding, in particular, timestamp literals.
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('enable_expr_rewrites', *[0,1]))

  @classmethod
  def add_custom_cluster_constraints(cls):
    # Do not call the super() implementation because this class needs to relax the
    # set of constraints.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @SkipIfFS.hbase
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--use_local_tz_for_unix_timestamp_conversions=true")
  def test_timestamp_functions(self, vector):
    """Tests timestamp functions with --use_local_tz_for_unix_timestamp_conversions=true
       TODO: this test can be probably removed as a query option is created for
             use_local_tz_for_unix_timestamp_conversions in IMPALA-10171
    """
    vector.get_value('exec_option')['enable_expr_rewrites'] = \
        vector.get_value('enable_expr_rewrites')

    # Tests for UTC timestamp functions, i.e. functions that do not depend on the
    # behavior of the flag --use_local_tz_for_unix_timestamp_conversions. These tests
    # are also executed in test_exprs.py to ensure the same behavior when running
    # without the gflag set.
    self.run_test_case('QueryTest/utc-timestamp-functions', vector)

    # Tests for local timestamp functions, i.e. functions that depend on the
    # behavior of the flag --use_local_tz_for_unix_timestamp_conversions.
    self.run_test_case('QueryTest/local-timestamp-functions', vector)

    # Test that scanning of different file formats is not affected by flag
    # use_local_tz_for_unix_timestamp_conversions.
    self.run_test_case('QueryTest/file-formats-with-local-tz-conversion', vector)
