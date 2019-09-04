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

import re

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import extend_exec_option_dimension


class TestFetch(ImpalaTestSuite):
  """Tests that are independent of whether result spooling is enabled or not."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestFetch, cls).add_test_dimensions()
    # Result fetching should be independent of file format, so only test against
    # Parquet files.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_rows_sent_counters(self, vector):
    """Validate that ClientFetchWaitTimer, NumRowsFetched, RowMaterializationRate,
    and RowMaterializationTimer are set to valid values in the ImpalaServer section
    of the runtime profile."""
    num_rows = 10
    result = self.execute_query("select sleep(100) from functional.alltypes limit {0}"
        .format(num_rows), vector.get_value('exec_option'))
    assert re.search("ClientFetchWaitTimer: [1-9]", result.runtime_profile)
    assert "NumRowsFetched: {0} ({0})".format(num_rows) in result.runtime_profile
    assert re.search("RowMaterializationRate: [1-9]", result.runtime_profile)
    # The query should take at least 1s to materialize all rows since it should sleep for
    # 1 second during materialization.
    assert re.search("RowMaterializationTimer: [1-9]s", result.runtime_profile)


class TestFetchAndSpooling(ImpalaTestSuite):
  """Tests that apply when result spooling is enabled or disabled."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestFetchAndSpooling, cls).add_test_dimensions()
    # Result fetching should be independent of file format, so only test against
    # Parquet files.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')
    extend_exec_option_dimension(cls, 'spool_query_results', 'true')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_rows_sent_counters(self, vector):
    """Validate that RowsSent and RowsSentRate are set to valid values in
    the PLAN_ROOT_SINK section of the runtime profile."""
    num_rows = 10
    result = self.execute_query("select id from functional.alltypes limit {0}"
        .format(num_rows), vector.get_value('exec_option'))
    assert "RowsSent: {0} ({0})".format(num_rows) in result.runtime_profile
    assert re.search("RowsSentRate: [1-9]", result.runtime_profile)
