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
from tests.common.skip import SkipIfApacheHive

ST_POINT_SIGNATURE = "BINARY\tst_point(STRING)\tJAVA\ttrue"
SHOW_FUNCTIONS = "show functions in _impala_builtins"


class TestGeospatialLibrary(CustomClusterTestSuite):
  """Tests the geospatial_library backend flag"""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(start_args='--geospatial_library=NONE')
  @SkipIfApacheHive.feature_not_supported
  @pytest.mark.execute_serially
  def test_disabled(self):
    result = self.execute_query(SHOW_FUNCTIONS)
    assert ST_POINT_SIGNATURE not in result.data

  @SkipIfApacheHive.feature_not_supported
  @pytest.mark.execute_serially
  def test_enabled(self):
    result = self.execute_query(SHOW_FUNCTIONS)
    assert ST_POINT_SIGNATURE in result.data
