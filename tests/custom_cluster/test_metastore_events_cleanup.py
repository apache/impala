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
import os

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf

IMPALA_HOME = os.getenv('IMPALA_HOME')
HIVE_SITE_EVENTS_CLEANUP = IMPALA_HOME + '/fe/src/test/resources/hive-site-events-cleanup'


class TestTableLoadingWithEventsCleanUp(CustomClusterTestSuite):

  @pytest.mark.execute_serially
  @SkipIf.is_test_jdk
  @CustomClusterTestSuite.with_args(hive_conf_dir=HIVE_SITE_EVENTS_CLEANUP)
  def test_table_load_with_events_cleanup(self, unique_database):
    """Regression test for IMPALA-11028"""
    self.execute_query_expect_success(self.client, "create table {}.{}"
                                                   "(id int)".format(unique_database,
      "t1"))
    self.execute_query_expect_success(self.client, "create table {}.{}"
                                                   "(id int)".format(unique_database,
      "t2"))
    self.execute_query_expect_success(self.client, "select sleep(120000)")
    self.execute_query_expect_success(self.client, "create table {}.{}"
                                                   "(id int)".format(unique_database,
      "t3"))
    self.execute_query_expect_success(self.client, "select * from "
                                                   "{}.{}".format(unique_database, "t1"))
