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


class TestStatestoreRpcErrors(CustomClusterTestSuite):
  """Tests for statestore RPC handling."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestStatestoreRpcErrors, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      " --debug_actions=REGISTER_SUBSCRIBER_FIRST_ATTEMPT:FAIL@1.0")
  def test_register_subscriber_rpc_error(self, vector):
    self.assert_impalad_log_contains("INFO",
        "Injected RPC error.*Debug Action: REGISTER_SUBSCRIBER_FIRST_ATTEMPT")

    # Ensure cluster has started up by running a query.
    result = self.execute_query("select count(*) from functional_parquet.alltypes")
    assert result.success, str(result)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      " --debug_actions=GET_PROTOCOL_VERSION_FIRST_ATTEMPT:FAIL@1.0")
  def test_get_protocol_version_rpc_error(self, vector):
    self.assert_impalad_log_contains("INFO",
        "Injected RPC error.*Debug Action: GET_PROTOCOL_VERSION_FIRST_ATTEMPT")

    # Ensure cluster has started up by running a query.
    result = self.execute_query("select count(*) from functional_parquet.alltypes")
    assert result.success, str(result)


class TestCatalogRpcErrors(CustomClusterTestSuite):
  """Tests for catalog RPC handling."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestCatalogRpcErrors, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      " --debug_actions=CATALOG_RPC_FIRST_ATTEMPT:FAIL@1.0")
  def test_register_subscriber_rpc_error(self, vector, unique_database):
    """Validate that RPCs to the catalogd are retried by injecting a failure into the
    first RPC attempt for any catalogd RPC. Run a variety of queries that require
    catalogd interaction to ensure all RPCs are retried."""
    # Validate create table queries.
    result = self.execute_query("create table {0}.tmp (col int)".format(unique_database))
    assert result.success

    # Validate insert queries.
    result = self.execute_query("insert into table {0}.tmp values (1)"
        .format(unique_database))
    assert result.success

    # Validate compute stats queries.
    result = self.execute_query("compute stats {0}.tmp".format(unique_database))
    assert result.success

    # Validate refresh table queries.
    result = self.execute_query("refresh {0}.tmp".format(unique_database))
    assert result.success

    # Validate drop table queries.
    result = self.execute_query("drop table {0}.tmp".format(unique_database))
    assert result.success

    # Validate select queries against pre-existing, but not-loaded tables.
    result = self.execute_query("select count(*) from functional_parquet.alltypes")
    assert result.success, str(result)

    # The 6 queries above each should have triggered the DEBUG_ACTION, so assert that
    # the DEBUG_ACTION was triggered 8 times (an extra 2 for the DROP and CREATE DATABASE
    # queries needed to make the unique_database).
    self.assert_impalad_log_contains("INFO",
        "Injected RPC error.*Debug Action: CATALOG_RPC_FIRST_ATTEMPT", 8)
