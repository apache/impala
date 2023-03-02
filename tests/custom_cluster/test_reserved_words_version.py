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


class TestReservedWordsVersion(CustomClusterTestSuite):
  def __test_common(self):
    self.execute_query_expect_success(self.client, "select 1 as year")
    self.execute_query_expect_success(self.client, "select 1 as avg")

    assert "Hint: reserved words have to be escaped when used as an identifier, e.g. " \
      "`iceberg`" in str(self.execute_query_expect_failure(self.client,
                                                          "create database iceberg"))
    self.execute_query_expect_success(self.client, "create database `iceberg`")
    self.execute_query_expect_success(self.client, "drop database `iceberg`")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--reserved_words_version=3.0.0")
  def test_3_0(self):
    assert "Hint: reserved words have to be escaped when used as an identifier, e.g. " \
      "`at`" in str(self.execute_query_expect_failure(self.client, "select 1 as at"))
    self.execute_query_expect_success(self.client, "select 1 as `at`")
    self.__test_common()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--reserved_words_version=2.11.0")
  def test_2_11(self):
    self.execute_query_expect_success(self.client, "select 1 as at")

    self.__test_common()
