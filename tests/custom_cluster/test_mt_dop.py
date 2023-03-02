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
import os
import pytest
from copy import deepcopy

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout
from tests.common.skip import SkipIfNotHdfsMinicluster

WAIT_TIME_MS = build_flavor_timeout(60000, slow_build_timeout=100000)

# The path to resources directory which contains the admission control config files
# (used for max mt dop test).
RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test", "resources")


def impalad_admission_ctrl_maxmtdop_args():
  fs_allocation_path = os.path.join(RESOURCES_DIR, "fair-scheduler-maxmtdop.xml")
  llama_site_path = os.path.join(RESOURCES_DIR, "llama-site-maxmtdop.xml")
  return "--llama_site_path={0} --fair_scheduler_allocation_path={1}".format(
      llama_site_path, fs_allocation_path)


class TestMtDopFlags(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMtDopFlags, cls).add_test_dimensions()

  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_mt_dop_runtime_filters_one_node(self, vector):
    """Runtime filter tests, which assume 3 fragment instances, can also be run on a single
    node cluster to test multiple filter sources/destinations per backend."""
    # Runtime filter test with RUNTIME_PROFILE seconds modified to reflect
    # the different filter aggregation pattern with mt_dop.
    vector.get_value('exec_option')['mt_dop'] = 3
    self.run_test_case('QueryTest/runtime_filters_mt_dop', vector,
        test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})
    vector.get_value('table_format').file_format = 'parquet'
    self.run_test_case('QueryTest/runtime_filters_mt_dop', vector,
        test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})

    # Also run with kudu to test min-max filters. Need to modify table_format directly
    # so that the Kudu RUNTIME_PROFILE section is correctly used.
    vector.get_value('table_format').file_format = 'kudu'
    self.run_test_case('QueryTest/runtime_filters_mt_dop', vector,
        test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})

  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_mt_dop_union_empty_table(self, unique_database):
    """ Regression test for IMPALA-11803: When used in DEBUG build,
    impalad crashed while running union on an empty table with MT_DOP>1.
    This test verifies the fix on the same."""
    self.client.execute("set mt_dop=2")
    self.client.execute("select count(*) from (select f2 from"
                        " functional.emptytable union all select id from"
                        " functional.alltypestiny) t")

class TestMaxMtDop(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMaxMtDop, cls).add_test_dimensions()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_maxmtdop_args())
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_max_mt_dop(self, vector):
    self.run_test_case('QueryTest/max-mt-dop', vector)
