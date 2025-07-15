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
from os import getenv

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

CORE_SITE_CONFIG_DIR = getenv('IMPALA_HOME') + '/fe/src/test/resources/' +\
    'core-site-disabled-block-locations'


class TestDisabledBlockLocations(CustomClusterTestSuite):
  @classmethod
  def setup_class(cls):
    super(TestDisabledBlockLocations, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(custom_core_site_dir=CORE_SITE_CONFIG_DIR)
  def test_no_block_locations(self, vector):
    self.run_test_case('QueryTest/no-block-locations', vector)
