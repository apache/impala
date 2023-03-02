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


class TestRE2MaxMem(CustomClusterTestSuite):
    """test if re2 max_mem parameter is set using the global flag in imapalad"""
    @classmethod
    def get_workload(cls):
        return 'tpch'

    def _test_re2_max_mem(self, expect_fail, dfa_out_of_mem):
        """"test to see given an amount of memory (in Bytes) does the re2 print an error for
        DFA run out of memory when compiling and pattern matching a long regexp"""

        client = self.create_impala_client()

        query = (
          "select c_comment from tpch.customer where regexp_like(c_comment,"
          "repeat('([a-c].*[t-w]|[t].?[h]|[^xyz]|.*?(\\\\d+))\\\\w', 1000),'i');")

        # RE2 regex compilation storage is also dependent on max_mem parameter, for small
        # values the regex pattern will fail as invalid pattern although it can be valid
        # given a higher max_mem for RE2.
        # See: https://github.com/google/re2/blob/3e9622e/re2/re2.h#L619-L648
        if expect_fail:
            # if we expect the regex compilation to fail we can ignore
            # DFA out of memory issue at that will be brought up when
            # RE2 does not having enough storage to store compiled regex
            self.execute_query_expect_failure(client, query)
        else:
            self.execute_query_expect_success(client, query)
            self.assert_impalad_log_contains(
              "ERROR", "DFA out of memory", -1 if dfa_out_of_mem else 0)

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(cluster_size=1,
      impalad_args="--re2_mem_limit=1KB")
    def test_dfa_out_of_mem(self):
        self._test_re2_max_mem(True, True)

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(cluster_size=1)
    def test_re2_max_mem_not_specified(self):
        # default max_mem set by re2's regex engine is 8 MiB
        self._test_re2_max_mem(False, True)

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(cluster_size=1,
        impalad_args="--re2_mem_limit=200MB")
    def test_dfa_not_out_of_mem(self):
        self._test_re2_max_mem(False, False)
