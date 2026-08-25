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

from unittest.mock import Mock, call, patch

import pytest

from tests.common.base_test_suite import BaseTestSuite
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.custom_cluster.test_iceberg_rest_catalog import IcebergRestCatalogTests
from tests.custom_cluster.test_iceberg_rest_catalog import RestServerProperties


class TestIcebergRestCatalogCleanup(BaseTestSuite):

  def test_table_fixture_uses_rest_api(self):
    server = Mock(port=9084)
    missing_table = Mock(status_code=404)
    existing_namespace = Mock(status_code=409)
    created_table = Mock(status_code=200)

    with patch('tests.custom_cluster.test_iceberg_rest_catalog.requests.delete',
               return_value=missing_table) as delete, \
        patch('tests.custom_cluster.test_iceberg_rest_catalog.requests.post',
              side_effect=[existing_namespace, created_table]) as post:
      IcebergRestCatalogTests._create_rest_test_table(
          server, 'impala_rest_dml_test.insert_test')

    delete.assert_called_once_with(
        'http://localhost:9084/v1/namespaces/impala_rest_dml_test/tables/insert_test',
        params={'purgeRequested': 'true'}, timeout=30)
    assert post.call_args_list == [
        call('http://localhost:9084/v1/namespaces',
             json={'namespace': ['impala_rest_dml_test'], 'properties': {}},
             timeout=30),
        call('http://localhost:9084/v1/namespaces/impala_rest_dml_test/tables',
             json={
                 'name': 'insert_test',
                 'schema': {
                     'type': 'struct',
                     'schema-id': 0,
                     'fields': [
                         {'id': 1, 'name': 'i', 'required': False, 'type': 'int'}],
                 },
                 'properties': {'format-version': '2'},
             }, timeout=30)]
    missing_table.raise_for_status.assert_not_called()
    existing_namespace.raise_for_status.assert_not_called()
    created_table.raise_for_status.assert_called_once_with()

  @pytest.mark.parametrize('failed_servers', [[], [0], [1], [0, 1]])
  def test_teardown_stops_all_servers_and_runs_parent(self, failed_servers):
    suite = IcebergRestCatalogTests()
    suite.servers = [Mock(), Mock()]
    errors = [RuntimeError('first shutdown failed'),
              RuntimeError('second shutdown failed')]
    for index in failed_servers:
      suite.servers[index].stop_rest_server.side_effect = errors[index]
    method = Mock()

    with patch.object(CustomClusterTestSuite, 'teardown_method') as parent_teardown:
      if failed_servers:
        with pytest.raises(RuntimeError) as exc:
          suite.teardown_method(method)
        assert exc.value is errors[failed_servers[0]]
      else:
        suite.teardown_method(method)

    for server in suite.servers:
      server.stop_rest_server.assert_called_once_with(60)
    parent_teardown.assert_called_once_with(method)

  @pytest.mark.parametrize('failure_stage', ['server', 'parent', 'clients'])
  def test_setup_preserves_original_error_after_shutdown_failure(self, failure_stage):
    suite = IcebergRestCatalogTests()
    servers = [Mock(), Mock()]
    setup_error = RuntimeError('setup failed')
    servers[0].stop_rest_server.side_effect = RuntimeError('shutdown failed')

    @RestServerProperties({'port': 9084}, {'port': 9085})
    def method():
      pass

    with patch('tests.custom_cluster.test_iceberg_rest_catalog.IcebergRestServer',
               side_effect=servers), \
        patch.object(CustomClusterTestSuite, 'setup_method') as parent_setup, \
        patch.object(suite, 'create_impala_clients') as create_clients:
      if failure_stage == 'server':
        servers[1].start_rest_server.side_effect = setup_error
      elif failure_stage == 'parent':
        parent_setup.side_effect = setup_error
      else:
        create_clients.side_effect = setup_error
      with pytest.raises(RuntimeError) as exc:
        suite.setup_method(method)

    assert exc.value is setup_error
    for server in servers:
      server.stop_rest_server.assert_called_once_with(10)
