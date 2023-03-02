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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS


@SkipIfFS.variable_listing_times
class TestDebugActions(ImpalaTestSuite):

  @pytest.mark.execute_serially
  def test_catalogd_debug_actions(self, unique_database):
    self.client.execute("refresh tpcds.store_sales")
    self.client.execute(
      "create table {0}.test like functional.alltypes".format(unique_database))
    self.client.execute("insert into {0}.test partition (year,month) "
      "select * from functional.alltypes".format(unique_database))
    self.client.execute("compute stats {0}.test".format(unique_database))
    self.__run_debug_action("refresh tpcds.store_sales",
      debug_action="catalogd_refresh_hdfs_listing_delay:SLEEP@50", delta=2000)
    self.__run_debug_action("refresh tpcds.store_sales",
      debug_action="catalogd_refresh_hdfs_listing_delay:JITTER@50@0.75", delta=1000)
    self.__run_debug_action(
      "alter table {0}.test recover partitions".format(unique_database),
      debug_action="catalogd_table_recover_delay:SLEEP@3000", delta=2000)
    # the variance of compute stats statement could itself be within few hundred
    # millisecs hence adding additional delay of 4000 doesn't necessarily slow down the
    # query by 4000 ms always.
    self.__run_debug_action("compute stats {0}.test".format(unique_database),
      debug_action="catalogd_update_stats_delay:SLEEP@4000", delta=3000)

  def __run_debug_action(self, query, debug_action, delta):
    """Test makes sure that the given debug_action is set is indeed causing the query
    to run slower."""
    time_taken_before = self.exec_and_time(query)
    time_taken_after = self.exec_and_time(query, {"debug_action": debug_action})
    assert (time_taken_after - time_taken_before) > delta
