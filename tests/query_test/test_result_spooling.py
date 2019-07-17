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

from tests.common.impala_test_suite import ImpalaTestSuite


class TestResultSpooling(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_result_spooling(self):
    """Tests that setting SPOOL_QUERY_RESULTS = true for simple queries returns the
    correct number of results."""
    query_opts = {"spool_query_results": "true"}
    query = "select * from functional.alltypes limit 10"
    result = self.execute_query_expect_success(self.client, query, query_opts)
    assert(len(result.data) == 10)
