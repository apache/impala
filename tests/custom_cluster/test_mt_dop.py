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

import os
import pytest

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
  def add_test_dimensions(cls):
    super(TestMtDopFlags, cls).add_test_dimensions()

  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_mt_dop_runtime_filters_one_node(self, vector):
    """Runtime filter tests, which assume 3 fragment instances, can also be run on a single
    node cluster to test multiple filter sources/destinations per backend."""
    # Remove 'num_nodes' option so we can set it at .test file.
    # Revisit this if 'num_nodes' dimension size increase.
    vector.unset_exec_option('num_nodes')
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
  def test_mt_dop_union_empty_table(self):
    """ Regression test for IMPALA-11803: When used in DEBUG build,
    impalad crashed while running union on an empty table with MT_DOP>1.
    This test verifies the fix on the same."""
    self.client.execute("set mt_dop=2")
    self.client.execute("select count(*) from (select f2 from"
                        " functional.emptytable union all select id from"
                        " functional.alltypestiny) t")

  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_mt_dop_union_limit_scan_range_submission(self, unique_database):
    """Regression test for a hang with mt_dop > 1 on a single backend: UNION nodes
    open their children lazily, so if a LIMIT makes one fragment instance stop
    pulling rows before it opens one of the union's scan node children, that
    instance never submits its scan ranges for that child. Another co-located
    instance that does open the child then waits forever for the missing
    submission in ScanRangeSharedState::GetNextScanRange()."""
    # Use mt_dop=0 for the inserts so each INSERT statement creates one file.
    self.client.execute("set mt_dop=0")
    # 'repro0' must consist of a single scan range (one text file) with at least 10
    # distinct keys. The instance that reads it reaches the aggregation limit and
    # then stops without opening the second union child.
    self.client.execute("create table {0}.repro0 (k int) stored as textfile".format(
        unique_database))
    self.client.execute("insert into {0}.repro0 values (0),(1),(2),(3),(4),(5),(6),"
                        "(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),"
                        "(20),(21),(22),(23),(24)".format(unique_database))
    # 'repro1' must consist of two scan ranges (two text files) so that the fragment
    # gets two co-located instances, but must have fewer than 10 distinct keys so
    # that the instance which opens it cannot satisfy the limit itself.
    self.client.execute("create table {0}.repro1 (k int) stored as textfile".format(
        unique_database))
    self.client.execute("insert into {0}.repro1 values (1),(2),(3)".format(
        unique_database))
    self.client.execute("insert into {0}.repro1 values (4),(5),(6)".format(
        unique_database))
    # Verify the file counts that this test relies on.
    assert len(self.execute_query(
        "show files in {0}.repro0".format(unique_database)).data) == 1
    assert len(self.execute_query(
        "show files in {0}.repro1".format(unique_database)).data) == 2
    self.client.execute("set mt_dop=2")
    self.client.execute("set exec_time_limit_s=10")
    # One instance reads repro0's single scan range, produces enough groups to hit the
    # LIMIT, and stops without opening the repro1 scan. The other instance gets no
    # repro0 ranges, so it opens repro1 and then waits for the first instance's scan
    # range submission, which shouldn't hang.
    result = self.execute_query("select k from (select k from {0}.repro0 union all"
                                " select k from {0}.repro1) t group by 1 limit 10"
                                .format(unique_database))
    assert len(result.data) == 10


class TestMaxMtDop(CustomClusterTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestMaxMtDop, cls).add_test_dimensions()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_maxmtdop_args())
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_max_mt_dop(self, vector):
    self.run_test_case('QueryTest/max-mt-dop', vector)
