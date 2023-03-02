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
from tests.common.skip import SkipIf


@SkipIf.not_dfs
class TestHedgedReads(CustomClusterTestSuite):
  """ Exercises the hedged reads code path.
      NOTE: We unfortunately cannot force hedged reads on a minicluster, but we enable
      this test to at least make sure that the code path doesn't break."""
  @CustomClusterTestSuite.with_args("--use_hdfs_pread=true")
  def test_hedged_reads(self, vector):
    QUERY = "select * from tpch_parquet.lineitem limit 100"
    self.client.execute(QUERY)
