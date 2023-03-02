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
from tests.common.skip import SkipIfBuildType

class TestImpala3798(CustomClusterTestSuite):
  """Regression test for IMPALA-3798, which is a hang that occurs when an Avro file is not
  filtered by a runtime filter, but its header split is (this only occurs when the filter
  arrives after file filtering is attempted, but before per-split filtering is).

  The debug flag --skip_file_runtime_filtering disables per-file filtering, mimicing the
  race that leads to the hang.
  """
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--skip_file_runtime_filtering=true")
  def test_sequence_file_filtering_race(self, vector):
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    client.execute("SET RUNTIME_FILTER_MODE=GLOBAL")
    client.execute("SET RUNTIME_FILTER_WAIT_TIME_MS=10000")

    # Set scan range length shorter than file length to ensure more than one split per
    # file (which is necessary to trigger IMPALA-3798).
    client.execute("SET MAX_SCAN_RANGE_LENGTH=1024")
    # To trigger the bug, there must be a partition filter that eliminates at least one
    # file. In this case, we choose a filter that eliminates all files, since there is no
    # int_col = 3 in alltypes.
    client.execute("select STRAIGHT_JOIN * from functional_avro.alltypes a join " +
                   "[SHUFFLE] functional_avro.alltypes b on a.month = b.id " +
                   "and b.int_col = -3")
