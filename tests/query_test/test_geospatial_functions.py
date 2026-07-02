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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfApacheHive
from tests.common.test_dimensions import create_single_exec_option_dimension


@SkipIfApacheHive.feature_not_supported
class TestGeospatialFuctions(ImpalaTestSuite):
  """Tests the geospatial builtin functions in the default WKB_EXPERIMENTAL mode."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestGeospatialFuctions, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Currently only a text table has geospatial data, skip other fileformats.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text')

  def test_esri_geospatial_functions(self, vector):
    # tests generated from
    # https://github.com/Esri/spatial-framework-for-hadoop/tree/master/hive/test
    self.run_test_case('QueryTest/geospatial-esri', vector)

  def test_esri_geospatial_functions_extra(self, vector):
    # manually added tests
    self.run_test_case('QueryTest/geospatial-esri-extra', vector)

  def test_wkb_serialization(self, vector):
    # WKB serialization round-trip and error cases
    self.run_test_case('QueryTest/geospatial-wkb-serialization', vector)

  def test_relations_table(self, vector):
    self.run_test_case('QueryTest/geospatial-relations-table', vector)

  def test_geometry_type(self, vector, unique_database):
    # tests for GEOMETRY type specific behavior
    self.run_test_case('QueryTest/geometry-type', vector, use_db=unique_database)
