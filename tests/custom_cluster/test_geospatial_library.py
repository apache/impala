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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfApacheHive

# In WKB mode ST functions register with GEOMETRY where ESRI mode uses BINARY.
ST_POINT_SIGNATURE = "BINARY\tst_point(STRING)\tJAVA\ttrue"
ST_X_SIGNATURE_BUILTIN = "DOUBLE\tst_x(BINARY)\tBUILTIN\ttrue"
ST_POINT_WKB_SIGNATURE = "GEOMETRY\tst_point(STRING)\tJAVA\ttrue"
ST_X_WKB_SIGNATURE = "DOUBLE\tst_x(GEOMETRY)\tJAVA\ttrue"
SHOW_FUNCTIONS = "show functions in _impala_builtins"


class TestGeospatialLibrary(CustomClusterTestSuite):
  """Tests the geospatial_library backend flag in the non-default modes."""

  @CustomClusterTestSuite.with_args(start_args='--geospatial_library=NONE')
  def test_disabled(self):
    result = self.execute_query(SHOW_FUNCTIONS)
    assert ST_POINT_SIGNATURE not in result.data
    assert ST_X_SIGNATURE_BUILTIN not in result.data
    assert ST_POINT_WKB_SIGNATURE not in result.data
    assert ST_X_WKB_SIGNATURE not in result.data

  @SkipIfApacheHive.feature_not_supported
  @CustomClusterTestSuite.with_args(start_args='--geospatial_library=WKB_EXPERIMENTAL')
  def test_wkb_experimental(self):
    # WKB_EXPERIMENTAL registers ST functions with GEOMETRY signatures, unlike the BINARY
    # signatures used by HIVE_ESRI mode.
    result = self.execute_query(SHOW_FUNCTIONS)
    assert ST_POINT_SIGNATURE not in result.data
    assert ST_X_SIGNATURE_BUILTIN not in result.data
    assert ST_POINT_WKB_SIGNATURE in result.data
    assert ST_X_WKB_SIGNATURE in result.data

  @SkipIfApacheHive.feature_not_supported
  @CustomClusterTestSuite.with_args(start_args='--geospatial_library=HIVE_ESRI')
  def test_hive_esri(self):
    # HIVE_ESRI registers ST functions with BINARY signatures (and native C++ builtins),
    # unlike the GEOMETRY signatures of the default WKB_EXPERIMENTAL mode.
    result = self.execute_query(SHOW_FUNCTIONS)
    assert ST_POINT_SIGNATURE in result.data
    assert ST_X_SIGNATURE_BUILTIN in result.data
    assert ST_POINT_WKB_SIGNATURE not in result.data
    assert ST_X_WKB_SIGNATURE not in result.data


@CustomClusterTestSuite.with_args(start_args='--geospatial_library=HIVE_ESRI')
class TestEsriHiveMode(CustomClusterTestSuite):
  """Tests HIVE_ESRI mode in detail."""

  def test_esri_geospatial_functions(self, vector):
    self.run_test_case('QueryTest/geospatial-esri', vector)

  def test_esri_geospatial_functions_extra(self, vector):
    self.run_test_case('QueryTest/geospatial-esri-extra', vector)

  def test_esri_specific_overloads(self, vector):
    # HIVE_ESRI-only overloads that are planned to be dropped in future.
    self.run_test_case('QueryTest/geospatial-esri-specific-overloads', vector)

  def test_esri_srid(self, vector):
    # SRID-dependent tests; SRID is not preserved in WKB mode.
    self.run_test_case('QueryTest/geospatial-esri-srid', vector)

  def test_esri_high_dimension(self, vector):
    # 3D/4D geometry tests (ST_Z, ST_M, ST_Is3D, etc.), only supported in ESRI mode.
    self.run_test_case('QueryTest/geospatial-esri-high-dimension', vector)

  def test_esri_geospatial_planner(self, vector):
    # Planner tests for the Java geo UDFs; kept out of the JUnit PlannerTest.
    self.run_test_case('QueryTest/geospatial-esri-planner', vector)

  def test_esri_wkb_serialization(self, vector):
    # Test WKB round-trip / malformed-input handling.
    self.run_test_case('QueryTest/geospatial-wkb-serialization', vector)

  def test_relations_table(self, vector):
    self.run_test_case('QueryTest/geospatial-relations-table', vector)
