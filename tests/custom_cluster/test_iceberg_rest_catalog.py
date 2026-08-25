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
from urllib.parse import quote

import pytest
import requests

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite, HIVE_CONF_DIR
from tests.common.iceberg_rest_server import IcebergRestServer
from tests.util.filesystem_utils import get_fs_path
from tests.util.hdfs_util import HadoopFsCommandLineClient


IMPALA_HOME = os.environ['IMPALA_HOME']
NO_CATALOGD_STARTARGS = '--no_catalogd'
REST_STANDALONE_IMPALAD_ARGS = """--use_local_catalog=true --catalogd_deployed=false
    --catalog_config_dir={}/testdata/configs/catalog_configs/iceberg_rest_config"""\
        .format(IMPALA_HOME)
MULTICATALOG_IMPALAD_ARGS = """--use_local_catalog=true
    --catalog_config_dir={}/testdata/configs/catalog_configs/iceberg_rest_config"""\
        .format(IMPALA_HOME)
MULTIPLE_REST_IMPALAD_ARGS = """--use_local_catalog=true
    --catalog_config_dir={}/testdata/configs/catalog_configs/multicatalog_rest_config"""\
        .format(IMPALA_HOME)
MULTIPLE_REST_WITHOUT_CATALOGD_IMPALAD_ARGS = """--use_local_catalog=true
    --catalogd_deployed=false \
    --catalog_config_dir={}/testdata/configs/catalog_configs/multicatalog_rest_config"""\
        .format(IMPALA_HOME)
MULTICATALOG_CATALOGD_ARGS = "--catalog_topic_mode=minimal"
WRITE_TEST_TABLE = "impala_rest_dml_test.insert_test"
SECONDARY_WRITE_TEST_TABLE = "impala_rest_dml_test_secondary.insert_test"
WRITE_TEST_CATALOG_LOCATION = IcebergRestServer.DEFAULT_CATALOG_LOCATION
SECONDARY_WRITE_TEST_CATALOG_LOCATION = \
    '/test-warehouse/iceberg_test/secondary_hadoop_catalog'


def RestServerProperties(*server_configs):
  """
  Annotation to specify configurations for multiple REST servers to be started.
  Each argument is a dictionary with optional 'port', 'catalog_location', and
  'test_table' keys.
  Example:
  @RestServerProperties({'port': 9085},
      {'port': 9086, 'catalog_location': '/tmp/cat2', 'test_table': 'db.tbl'})
  """
  def decorator(func):
    func.rest_server_configs = list(server_configs)
    return func
  return decorator


class IcebergRestCatalogTests(CustomClusterTestSuite):
  """Base class for Iceberg REST Catalog tests."""
  def setup_method(self, method):
    args = method.__dict__
    if HIVE_CONF_DIR in args:
      raise Exception("Cannot specify HIVE_CONF_DIR because the tests of this class are "
          "running without Hive.")
    self.servers = []
    self.rest_test_tables = []

    server_configs = getattr(method, 'rest_server_configs', None)

    if server_configs:
      for config in server_configs:
        port = config.get('port', IcebergRestServer.DEFAULT_REST_SERVER_PORT)
        catalog_location = config.get('catalog_location',
            IcebergRestServer.DEFAULT_CATALOG_LOCATION)
        test_table = config.get('test_table')
        print("Starting REST server with annotation properties: "
              "Port=%s, Catalog Location=%s" % (port, catalog_location))
        server = IcebergRestServer(port, catalog_location)
        self.servers.append(server)
        if test_table:
          self.rest_test_tables.append((server, test_table))

    try:
      for server in self.servers:
        server.start_rest_server(300)
      for server, table_name in self.rest_test_tables:
        self._create_rest_test_table(server, table_name)
      super(IcebergRestCatalogTests, self).setup_method(method)
      # At this point we can create the Impala clients that we will need.
      self.create_impala_clients()
    except Exception:
      try:
        self._stop_rest_servers(10)
      except Exception:
        # stop_rest_server() logs shutdown errors. Preserve the setup failure.
        pass
      raise

  def teardown_method(self, method):
    try:
      self._stop_rest_servers()
    finally:
      super(IcebergRestCatalogTests, self).teardown_method(method)

  def _stop_rest_servers(self, timeout_s=60):
    first_error = None
    for server, table_name in getattr(self, 'rest_test_tables', []):
      try:
        self._drop_rest_test_table(server, table_name)
      except Exception as e:
        if first_error is None:
          first_error = e
    for server in self.servers:
      try:
        server.stop_rest_server(timeout_s)
      except Exception as e:
        if first_error is None:
          first_error = e
    if first_error is not None:
      raise first_error

  @staticmethod
  def _rest_table_url(server, table_name):
    namespace, table = table_name.split('.', 1)
    return "http://localhost:{}/v1/namespaces/{}/tables/{}".format(
        server.port, quote(namespace, safe=''), quote(table, safe=''))

  @classmethod
  def _drop_rest_test_table(cls, server, table_name):
    response = requests.delete(cls._rest_table_url(server, table_name),
        params={'purgeRequested': 'true'}, timeout=30)
    if response.status_code != 404:
      response.raise_for_status()

  @classmethod
  def _create_rest_test_table(cls, server, table_name):
    namespace, table = table_name.split('.', 1)
    cls._drop_rest_test_table(server, table_name)
    namespace_response = requests.post(
        "http://localhost:{}/v1/namespaces".format(server.port),
        json={'namespace': [namespace], 'properties': {}}, timeout=30)
    if namespace_response.status_code != 409:
      namespace_response.raise_for_status()

    create_response = requests.post(
        "http://localhost:{}/v1/namespaces/{}/tables".format(
            server.port, quote(namespace, safe='')),
        json={
            'name': table,
            'schema': {
                'type': 'struct',
                'schema-id': 0,
                'fields': [
                    {'id': 1, 'name': 'i', 'required': False, 'type': 'int'}],
            },
            'properties': {'format-version': '2'},
        }, timeout=30)
    create_response.raise_for_status()


class TestIcebergRestCatalogWithHms(IcebergRestCatalogTests):
  """Test suite for Iceberg REST Catalog. HMS running while tests are running"""
  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE})
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTICATALOG_IMPALAD_ARGS,
     catalogd_args=MULTICATALOG_CATALOGD_ARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_multicatalog(self, vector):
    self.run_test_case('QueryTest/iceberg-multicatalog',
                       vector, use_db="ice")
    self.execute_query_expect_success(
        self.client, "insert into {} values (1)".format(WRITE_TEST_TABLE))
    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(WRITE_TEST_TABLE))
    assert result.data == ['1']

  @RestServerProperties(
    {'port': 9084},
    {'port': 9085,
     'catalog_location': '/test-warehouse/iceberg_test/secondary_hadoop_catalog'}
  )
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTIPLE_REST_IMPALAD_ARGS,
     catalogd_args=MULTICATALOG_CATALOGD_ARGS)
  @pytest.mark.execute_serially
  def test_multiple_rest_catalogs(self, vector):
    self.run_test_case('QueryTest/iceberg-multiple-rest-catalogs',
                       vector, use_db="ice")

  @RestServerProperties({'port': 9084})
  @CustomClusterTestSuite.with_args(
     impalad_args="{} --blacklisted_dbs=ice".format(MULTICATALOG_IMPALAD_ARGS),
     catalogd_args=MULTICATALOG_CATALOGD_ARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_multicatalog_blacklisted_db(self, vector):
    self.run_test_case('QueryTest/iceberg-rest-catalog-blacklist-db', vector,
        use_db="default")

  @RestServerProperties({'port': 9084})
  @CustomClusterTestSuite.with_args(
      impalad_args="{} --blacklisted_tables=ice.airports_parquet"
                   .format(REST_STANDALONE_IMPALAD_ARGS),
      catalogd_args=MULTICATALOG_CATALOGD_ARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_multicatalog_blacklisted_tables(self, vector):
    self.run_test_case('QueryTest/iceberg-rest-catalog-blacklist-tables',
        vector, use_db="ice")


class TestIcebergRestCatalogDmlStandalone(IcebergRestCatalogTests):
  """REST DML coverage for topologies without HMS."""

  @classmethod
  def need_default_clients(cls):
    """There will be no HMS, so we shouldn't create the Hive client."""
    return False

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_insert_into(self):
    table_name = WRITE_TEST_TABLE
    data_path = get_fs_path(
        "/test-warehouse/iceberg_test/hadoop_catalog/"
        "impala_rest_dml_test/insert_test/data")
    filesystem_client = HadoopFsCommandLineClient()

    self.execute_query_expect_success(
        self.client, "insert into {} values (1)".format(table_name))
    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(table_name))
    assert result.data == ['1']
    assert self._snapshot_count(table_name) == 1
    assert len(filesystem_client.ls(data_path)) == 1

    committed_files = set(filesystem_client.ls(data_path))
    unknown_state = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitStateUnknownException@'
        'simulated REST commit state unknown'}
    error = self.execute_query_expect_failure(
        self.client, "insert into {} values (3)".format(table_name),
        query_options=unknown_state)
    assert "simulated REST commit state unknown" in str(error)

    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(table_name))
    assert result.data == ['1']
    assert self._snapshot_count(table_name) == 1
    files_after_unknown_state = set(filesystem_client.ls(data_path))
    orphan_files = files_after_unknown_state - committed_files
    assert len(orphan_files) == 1
    assert filesystem_client.delete_file_dir(
        data_path + "/" + orphan_files.pop())

    commit_failure = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitFailedException@'
        'simulated REST commit failure'}
    error = self.execute_query_expect_failure(
        self.client, "insert into {} values (2)".format(table_name),
        query_options=commit_failure)
    assert "simulated REST commit failure" in str(error)

    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(table_name))
    assert result.data == ['1']
    assert self._snapshot_count(table_name) == 1
    assert len(filesystem_client.ls(data_path)) == 1

    unsupported_statements = [
        "insert overwrite {} values (2)".format(table_name),
        "truncate table {}".format(table_name),
        "delete from {} where i = 1".format(table_name),
        "update {} set i = 2 where i = 1".format(table_name),
        "merge into {0} target using (select 1 i) source "
        "on target.i = source.i when matched then update set i = 2".format(
            table_name),
        "optimize table {}".format(table_name),
    ]
    for statement in unsupported_statements:
      error = self.execute_query_expect_failure(self.client, statement)
      assert "AnalysisException: Only INSERT INTO is supported" in str(error)

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE},
      {'port': 9085,
       'catalog_location': SECONDARY_WRITE_TEST_CATALOG_LOCATION,
       'test_table': SECONDARY_WRITE_TEST_TABLE})
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTIPLE_REST_WITHOUT_CATALOGD_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_multiple_rest_catalogs_route_insert(self):
    self.execute_query_expect_success(
        self.client, "insert into {} values (1)".format(WRITE_TEST_TABLE))
    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(WRITE_TEST_TABLE))
    assert result.data == ['1']

    self.execute_query_expect_success(
        self.client, "insert into {} values (2)".format(SECONDARY_WRITE_TEST_TABLE))
    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(SECONDARY_WRITE_TEST_TABLE))
    assert result.data == ['2']
    assert self._snapshot_count(WRITE_TEST_TABLE) == 1
    assert self._snapshot_count(SECONDARY_WRITE_TEST_TABLE) == 1

  def _snapshot_count(self, table_name):
    result = self.execute_query_expect_success(
        self.client, "select count(*) from {}.snapshots".format(table_name))
    return int(result.data[0])


class TestIcebergRestCatalogNoHms(IcebergRestCatalogTests):
  """Test suite for Iceberg REST Catalog. HMS is stopped while tests are running"""

  @classmethod
  def need_default_clients(cls):
    """There will be no HMS, so we shouldn't create the Hive client."""
    return False

  @classmethod
  def setup_class(cls):
    super(TestIcebergRestCatalogNoHms, cls).setup_class()

    try:
      cls._stop_hive_service()
    except Exception as e:
      cls.cleanup_infra_services()
      raise e

  @classmethod
  def teardown_class(cls):
    cls.cleanup_infra_services()
    return super(TestIcebergRestCatalogNoHms, cls).teardown_class()

  @classmethod
  def cleanup_infra_services(cls):
    cls._start_hive_service(None)

  @RestServerProperties({'port': 9084})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_basic(self, vector):
    self.run_test_case('QueryTest/iceberg-rest-catalog', vector, use_db="ice")

  @RestServerProperties(
    {'port': 9084},
    {'port': 9085,
     'catalog_location': '/test-warehouse/iceberg_test/secondary_hadoop_catalog'}
  )
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTIPLE_REST_WITHOUT_CATALOGD_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS)
  @pytest.mark.execute_serially
  def test_multiple_rest_catalogs_without_catalogd(self, vector):
    self.run_test_case('QueryTest/iceberg-multiple-rest-catalogs',
                       vector, use_db="ice")

  @RestServerProperties(
    {'port': 9084},
    {'port': 9085}
  )
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTIPLE_REST_WITHOUT_CATALOGD_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS)
  @pytest.mark.execute_serially
  def test_multiple_rest_catalogs_with_ambiguous_tables(self, vector):
    self.run_test_case('QueryTest/iceberg-multiple-rest-catalogs-ambiguous-name',
                       vector, use_db="ice")

  @RestServerProperties({'port': 9084})
  @CustomClusterTestSuite.with_args(
     impalad_args="{} --blacklisted_dbs=ice".format(REST_STANDALONE_IMPALAD_ARGS),
     start_args=NO_CATALOGD_STARTARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_basic_blacklisted_db(self, vector):
    self.run_test_case('QueryTest/iceberg-rest-catalog-blacklist-db', vector,
        use_db="default")

  @RestServerProperties({'port': 9084})
  @CustomClusterTestSuite.with_args(
      impalad_args="{} --blacklisted_tables=ice.airports_parquet"
                   .format(REST_STANDALONE_IMPALAD_ARGS),
      start_args=NO_CATALOGD_STARTARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_basic_blacklisted_tables(self, vector):
    self.run_test_case('QueryTest/iceberg-rest-catalog-blacklist-tables',
        vector, use_db="ice")
