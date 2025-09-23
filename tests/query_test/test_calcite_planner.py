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
import logging

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (add_mandatory_exec_option)

LOG = logging.getLogger(__name__)


class TestCalcitePlanner(ImpalaTestSuite):

  @classmethod
  def setup_class(cls):
    super(TestCalcitePlanner, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestCalcitePlanner, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'planner', 'CALCITE')
    add_mandatory_exec_option(cls, 'fallback_planner', 'CALCITE')
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')

  def test_calcite_frontend(self, vector, unique_database):
    self.run_test_case('QueryTest/calcite', vector, use_db=unique_database)

  def test_semicolon(self, cursor):
    cursor.execute("select 4;")


class TestFallbackPlanner(ImpalaTestSuite):

  @classmethod
  def setup_class(cls):
    super(TestFallbackPlanner, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestFallbackPlanner, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')

  def test_fallback_planner(self, vector, unique_database):
    self.run_test_case('QueryTest/fallback_planner', vector, use_db=unique_database)
