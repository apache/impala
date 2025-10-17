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
  @pytest.mark.execute_serially
  def test_host_profile_jvm_gc_metrics(self, unique_database):
    self.execute_query_expect_success(self.client,
        "create function {0}.gc(int) returns int location '{1}' \
        symbol='org.apache.impala.JavaGcUdfTest'".format(
            unique_database, get_fs_path('/test-warehouse/impala-hive-udfs.jar')))
    profile = self.execute_query_expect_success(self.client,
        "select {0}.gc(int_col) from functional.alltypes limit 1000".format(
            unique_database)).runtime_profile

    gc_count_regex = r"GcCount:.*\((.*)\)"
    gc_count_match = re.search(gc_count_regex, profile)
    assert gc_count_match, profile
    assert int(gc_count_match.group(1)) > 0, profile

    gc_time_millis_regex = "GcTimeMillis: (.*)"
    gc_time_millis_match = re.search(gc_time_millis_regex, profile)
    assert gc_time_millis_match, profile
    assert parse_duration_string_ms(gc_time_millis_match.group(1)) > 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      catalogd_args="--catalog_topic_mode=minimal",
      impalad_args="--use_local_catalog=true",
      disable_log_buffering=True)
  def test_query_id_in_logs(self, unique_database):
    res = self.execute_query("create table %s.tbl (i int)" % unique_database)
    self.assert_catalogd_log_contains(
        "INFO", "{}] execDdl request: CREATE_TABLE {}.tbl issued by"
        .format(res.query_id, unique_database))

    res = self.execute_query("explain select * from %s.tbl" % unique_database)
    self.assert_catalogd_log_contains(
        "INFO", r"{}] Loading metadata for: {}.tbl \(needed by coordinator\)"
        .format(res.query_id, unique_database))

    res = self.execute_query(
        "create table %s.tbl2 as select * from functional.alltypes" % unique_database)
    self.assert_catalogd_log_contains(
        "INFO", "%s] Loading metadata for table: functional.alltypes" % res.query_id)
    self.assert_catalogd_log_contains(
        "INFO", "%s] Remaining items in queue: 0. Loads in progress: 1" % res.query_id,
        expected_count=-1)
    self.assert_catalogd_log_contains(
        "INFO", r"{}] Loading metadata for: functional.alltypes \(needed by coordinator\)"
        .format(res.query_id))
    self.assert_catalogd_log_contains(
        "INFO", "{}] execDdl request: CREATE_TABLE_AS_SELECT {}.tbl2 issued by"
        .format(res.query_id, unique_database))
    self.assert_catalogd_log_contains(
        "INFO", "{}] updateCatalog request: Update catalog for {}.tbl2"
        .format(res.query_id, unique_database))
    self.assert_catalogd_log_contains(
        "INFO", r"{}] Loading metadata for: {}.tbl2 \(Load for INSERT\)"
        .format(res.query_id, unique_database))
