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
import re

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.filesystem_utils import get_fs_path
from tests.util.parse_util import parse_duration_string_ms


class TestObservability(CustomClusterTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  def test_host_profile_jvm_gc_metrics(self, unique_database):
    self.execute_query_expect_success(self.client,
        "create function {0}.gc(int) returns int location '{1}' \
        symbol='org.apache.impala.JavaGcUdfTest'".format(
            unique_database, get_fs_path('/test-warehouse/impala-hive-udfs.jar')))
    profile = self.execute_query_expect_success(self.client,
        "select {0}.gc(int_col) from functional.alltypes limit 1000".format(
            unique_database)).runtime_profile

    gc_count_regex = "GcCount:.*\((.*)\)"
    gc_count_match = re.search(gc_count_regex, profile)
    assert gc_count_match, profile
    assert int(gc_count_match.group(1)) > 0, profile

    gc_time_millis_regex = "GcTimeMillis: (.*)"
    gc_time_millis_match = re.search(gc_time_millis_regex, profile)
    assert gc_time_millis_match, profile
    assert parse_duration_string_ms(gc_time_millis_match.group(1)) > 0
