#!/usr/bin/env impala-python
#
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


@CustomClusterTestSuite.with_args(
  catalogd_args="--truncate_external_tables_with_hms=false",
  cluster_size=1)
class TestNoDeleteStatsInTruncate(CustomClusterTestSuite):
  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestNoDeleteStatsInTruncate, cls).setup_class()

  def test_stats_remain_after_truncate(self, unique_database, vector):
    vector.get_value('exec_option')['delete_stats_in_truncate'] = False
    self.run_test_case('QueryTest/truncate-table-no-delete-stats', vector,
        use_db=unique_database)
