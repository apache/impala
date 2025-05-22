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
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import (add_mandatory_exec_option)

LOG = logging.getLogger(__name__)


class TestCalcitePlanner(CustomClusterTestSuite):

  @classmethod
  def setup_class(cls):
    super(TestCalcitePlanner, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestCalcitePlanner, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'use_calcite_planner', 'true')

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      start_args="--env_vars=USE_CALCITE_PLANNER=true",
      impalad_args="--use_local_catalog=false",
      catalogd_args="--catalog_topic_mode=full")
  def test_calcite_frontend(self, vector, unique_database):
    """Calcite planner does not work in local catalog mode yet."""
    self.run_test_case('QueryTest/calcite', vector, use_db=unique_database)
