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
from tests.common.skip import SkipIfBuildType

class TestAlwaysFalseFilter(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestAlwaysFalseFilter, cls).setup_class()

  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--skip_file_runtime_filtering=true")
  def test_skip_split(self, cursor):
    """IMPALA-5789: Test that always false filter filters out splits when file-level
    filtering is disabled."""
    cursor.execute("SET RUNTIME_FILTER_MODE=GLOBAL")
    cursor.execute("SET RUNTIME_FILTER_WAIT_TIME_MS=10000")
    query = """select STRAIGHT_JOIN * from alltypes inner join
            (select * from alltypessmall where smallint_col=-1) v
            on v.year = alltypes.year"""
    # Manually iterate through file formats instead of creating a test matrix to prevent
    # the cluster from restarting multiple times.
    for table_suffix in ['', '_avro', '_parquet', '_rc', '_seq']:
      cursor.execute("use functional" + table_suffix)
      cursor.execute(query)
      # Fetch all rows to finalize the query profile.
      cursor.fetchall()
      profile = cursor.get_profile()
      assert re.search("Files rejected: [^0] \([^0]\)", profile) is None
      assert re.search("Splits rejected: 8 \(8\)", profile) is not None
