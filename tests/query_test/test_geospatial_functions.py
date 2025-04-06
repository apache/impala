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
from tests.common.skip import SkipIfApacheHive
from tests.common.test_dimensions import create_single_exec_option_dimension


class TestGeospatialFuctions(ImpalaTestSuite):
  """Tests the geospatial builtin functions"""

  @classmethod
  def add_test_dimensions(cls):
    super(TestGeospatialFuctions, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Tests do not use tables at the moment, skip other fileformats than Parquet.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  @SkipIfApacheHive.feature_not_supported
  def test_esri_geospatial_functions(self, vector):
    # tests generated from
    # https://github.com/Esri/spatial-framework-for-hadoop/tree/master/hive/test
    self.run_test_case('QueryTest/geospatial-esri', vector)
    # manual tests added
    self.run_test_case('QueryTest/geospatial-esri-extra', vector)

  @SkipIfApacheHive.feature_not_supported
  def test_esri_geospatial_planner(self, vector):
    # These tests are not among planner tests because with default flags
    # geospatial builtin functions are not loaded.
    self.run_test_case('QueryTest/geospatial-esri-planner', vector)
