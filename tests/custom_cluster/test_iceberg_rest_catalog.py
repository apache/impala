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

import http.client
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote

import pytest
import requests

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite, HIVE_CONF_DIR
from tests.common.iceberg_rest_server import IcebergRestServer
from tests.common.iceberg_rest_test_tables import (
    DELETE_PARTITIONED_TEST_TABLES, DELETE_TEST_TABLES, INSERT_TEST_TABLE,
    MERGE_PARTITION_SORT_TEST_TABLES, MERGE_TEST_TABLES, OPTIMIZE_TEST_TABLES,
    PARTITIONED_INSERT_TEST_TABLE, UPDATE_PARTITIONS_TEST_TABLES, UPDATE_TEST_TABLES,
    create_table_request)
from tests.common.impala_connection import IMPALA_CONNECTION_EXCEPTION, RUNNING
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
# Two REST catalogs sharing a name: neither has a unique name, so DML and DDL are
# disabled for both.
DUPLICATE_NAME_REST_IMPALAD_ARGS = """--use_local_catalog=true
    --catalog_config_dir={}/testdata/configs/catalog_configs/duplicate_name_rest_config"""\
        .format(IMPALA_HOME)
DUPLICATE_NAME_REST_WITHOUT_CATALOGD_IMPALAD_ARGS = \
    "--catalogd_deployed=false " + DUPLICATE_NAME_REST_IMPALAD_ARGS
MULTICATALOG_CATALOGD_ARGS = "--catalog_topic_mode=minimal"
WRITE_TEST_TABLE = "impala_rest_dml_test.insert_test"
PARTITIONED_WRITE_TEST_TABLE = "impala_rest_dml_test.partitioned_insert_test"
SECONDARY_WRITE_TEST_TABLE = "impala_rest_dml_test_secondary.insert_test"
# A table that the REST catalog serves but Impala cannot load: Impala has no TIME type.
UNLOADABLE_TEST_TABLE = "impala_rest_dml_test.unloadable_test"
UNLOADABLE_TEST_DEFINITION = {
    'fields': [{'id': 1, 'name': 't', 'required': False, 'type': 'time'}],
    'properties': {'format-version': '2'}}
WRITE_TEST_CATALOG_LOCATION = IcebergRestServer.DEFAULT_CATALOG_LOCATION
SECONDARY_WRITE_TEST_CATALOG_LOCATION = \
    '/test-warehouse/iceberg_test/secondary_hadoop_catalog'
UPDATE_TEST_NS = "impala_rest_update_test"
MERGE_TEST_NS = "impala_rest_merge_test"
# Enables credential vending against the HDFS-backed in-tree REST server (which vends
# nothing), verifying the plumbing is a no-op for the common case.
REST_VENDED_CREDENTIALS_IMPALAD_ARGS = """--use_local_catalog=true
    --catalogd_deployed=false
    --catalog_config_dir={}/testdata/configs/catalog_configs/iceberg_rest_vended_config"""\
        .format(IMPALA_HOME)


def RestServerProperties(*server_configs):
  """
  Annotation to specify configurations for multiple REST servers to be started.
  Each argument is a dictionary with optional 'port', 'catalog_location',
  'test_table', 'partitioned_test_table' and 'test_tables' keys. 'test_tables' is a
  (namespace, {table name: definition}) pair, see iceberg_rest_test_tables. The test
  tables are created through the REST API before the test and dropped after it, see
  _create_rest_test_table().
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
    # (server, table name, table definition) of the tables to create.
    self.rest_test_tables = []

    server_configs = getattr(method, 'rest_server_configs', None)

    if server_configs:
      for config in server_configs:
        port = config.get('port', IcebergRestServer.DEFAULT_REST_SERVER_PORT)
        catalog_location = config.get('catalog_location',
            IcebergRestServer.DEFAULT_CATALOG_LOCATION)
        test_table = config.get('test_table')
        partitioned_test_table = config.get('partitioned_test_table')
        print("Starting REST server with annotation properties: "
              "Port=%s, Catalog Location=%s" % (port, catalog_location))
        server = IcebergRestServer(port, catalog_location)
        self.servers.append(server)
        if test_table:
          self.rest_test_tables.append((server, test_table, INSERT_TEST_TABLE))
        if partitioned_test_table:
          self.rest_test_tables.append(
              (server, partitioned_test_table, PARTITIONED_INSERT_TEST_TABLE))
        if 'test_tables' in config:
          namespace, tables = config['test_tables']
          for table, definition in tables.items():
            self.rest_test_tables.append(
                (server, "{}.{}".format(namespace, table), definition))

    try:
      for server in self.servers:
        server.start_rest_server(300)
      for server, table_name, definition in self.rest_test_tables:
        self._create_rest_test_table(server, table_name, definition=definition)
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
    for server, table_name, _ in getattr(self, 'rest_test_tables', []):
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

  @staticmethod
  def _drop_requests(server, table_name):
    """The DELETE requests for 'table_name' that 'server' logged."""
    namespace, table = table_name.split('.', 1)
    path = "namespaces/{}/tables/{} ".format(namespace, table)
    with open(server.log_path) as log:
      return [line for line in log if "Request: DELETE" in line and path in line]

  def _check_drop_request(self, server, table_name, stmt, purge):
    """Runs 'stmt', which drops 'table_name', and checks that it sent one DELETE to
    'server', with purgeRequested=true exactly if 'purge'. The test server deletes the
    table directory either way, so only the request shows whether PURGE reached it."""
    requests_before = len(self._drop_requests(server, table_name))
    result = self.execute_query_expect_success(self.client, stmt)
    assert result.data == ['Table has been dropped.']
    new_requests = self._drop_requests(server, table_name)[requests_before:]
    assert len(new_requests) == 1
    assert ("purgeRequested=true" in new_requests[0]) == purge

  def _run_rest_drop_table_test(self, server, table_name):
    """DROP TABLE on a REST catalog table runs in the coordinator through the REST
    catalog: the table disappears from the REST server and from Impala. Names are
    matched in lower case, as everywhere else in Impala. Works the same with and
    without catalogd."""
    db_name, tbl_name = table_name.split('.', 1)
    table_path = get_fs_path("{}/{}/{}".format(
        server.catalog_location, db_name, tbl_name))
    filesystem_client = HadoopFsCommandLineClient()

    self.execute_query_expect_success(
        self.client, "insert into {} values (1)".format(table_name))
    assert len(filesystem_client.ls(table_path + "/data")) == 1

    # The test server, backed by HadoopCatalog, deletes the table directory with or
    # without PURGE; the request shows that PURGE reached the server.
    self._check_drop_request(server, table_name,
        "drop table {} purge".format(table_name.upper()), purge=True)
    assert requests.get(self._rest_table_url(server, table_name),
        timeout=30).status_code == 404
    assert not filesystem_client.exists(table_path)
    result = self.execute_query_expect_success(
        self.client, "show tables in {}".format(db_name))
    assert tbl_name not in result.data
    error = self.execute_query_expect_failure(
        self.client, "select * from {}".format(table_name))
    assert "Could not resolve table reference" in str(error)

    # IF EXISTS on a table of a REST catalog that analysis does not find is a no-op in
    # the coordinator, neither the REST catalog nor catalogd is asked.
    result = self.execute_query_expect_success(
        self.client, "drop table if exists {}".format(table_name))
    assert result.data == ['Table does not exist.']
    self.execute_query_expect_success(
        self.client, "drop table if exists {}.{}".format(db_name, "no_such_table"))
    result = self.execute_query_expect_success(
        self.client, "drop view if exists {}".format(table_name))
    assert result.data == ['View does not exist.']

    # IF EXISTS is routed to the REST catalog once the table is there again.
    self._create_rest_test_table(server, table_name)
    result = self.execute_query_expect_success(
        self.client, "drop view if exists {}".format(table_name))
    assert result.data == ['Drop view is not allowed on a table.']
    self._check_drop_request(server, table_name,
        "drop table if exists {}".format(table_name.upper()), purge=False)
    assert requests.get(self._rest_table_url(server, table_name),
        timeout=30).status_code == 404

    # Impala cannot resolve a mixed-case name on the REST server, so IF EXISTS on it
    # must leave the table alone, although it is listed.
    mixed_case_table = "{}.MixedCase".format(db_name)
    self._create_rest_test_table(server, mixed_case_table)
    self.rest_test_tables.append((server, mixed_case_table, INSERT_TEST_TABLE))
    self.execute_query_expect_success(
        self.client, "drop table if exists {} purge".format(mixed_case_table))
    assert requests.get(self._rest_table_url(server, mixed_case_table),
        timeout=30).status_code == 200

  def _check_drop_without_unique_name(self):
    """Both REST catalogs are named 'rest', so neither can be addressed by name. Every
    DROP TABLE on their tables must be rejected for that reason and must not be sent to
    catalogd instead."""
    server = self.servers[0]
    expected = "REST catalog does not have a unique name"

    error = self.execute_query_expect_failure(
        self.client, "drop table {}".format(WRITE_TEST_TABLE))
    assert expected in str(error)

    self._create_rest_test_table(
        server, UNLOADABLE_TEST_TABLE, definition=UNLOADABLE_TEST_DEFINITION)
    self.rest_test_tables.append(
        (server, UNLOADABLE_TEST_TABLE, UNLOADABLE_TEST_DEFINITION))
    for stmt in ["drop table {}", "drop table if exists {}"]:
      error = self.execute_query_expect_failure(
          self.client, stmt.format(UNLOADABLE_TEST_TABLE))
      assert expected in str(error)
    assert requests.get(self._rest_table_url(server, UNLOADABLE_TEST_TABLE),
        timeout=30).status_code == 200

    # A table that analysis did not find is a no-op with IF EXISTS, as in catalogd.
    db_name = WRITE_TEST_TABLE.split('.')[0]
    result = self.execute_query_expect_success(
        self.client, "drop table if exists {}.no_such_table".format(db_name))
    assert result.data == ['Table does not exist.']

  @classmethod
  def _create_rest_test_table(cls, server, table_name, partitioned=False,
      definition=None):
    """Creates 'table_name' from 'definition', see iceberg_rest_test_tables. Without a
    definition, the table gets an INT column 'i', and a partitioned table also gets an
    INT column 'p' and an identity partition spec on it."""
    if definition is None:
      definition = PARTITIONED_INSERT_TEST_TABLE if partitioned else INSERT_TEST_TABLE
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
        json=create_table_request(table, definition), timeout=30)
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
    # MERGE into the REST table using an HMS table as the source.
    self.execute_query_expect_success(self.client,
        "merge into {} t using (select id from functional.alltypestiny where id < 3) s "
        "on t.i = s.id when matched then delete "
        "when not matched then insert values (s.id)".format(WRITE_TEST_TABLE))
    result = self.execute_query_expect_success(
        self.client, "select i from {} order by i".format(WRITE_TEST_TABLE))
    assert result.data == ['0', '2']

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE})
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTICATALOG_IMPALAD_ARGS,
     catalogd_args=MULTICATALOG_CATALOGD_ARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_drop_table(self):
    """With catalogd deployed, DROP TABLE on a REST table must still run in the
    coordinator: catalogd does not know the table."""
    self._run_rest_drop_table_test(self.servers[0], WRITE_TEST_TABLE)
    # A database that only catalogd knows still routes there.
    result = self.execute_query_expect_success(
        self.client, "drop table if exists default.no_such_table")
    assert result.data == ['Table does not exist.']

    # The coordinators cache nothing for REST tables: a drop through one coordinator is
    # visible on another at once, and SYNC_DDL has nothing to wait for.
    self._create_rest_test_table(self.servers[0], WRITE_TEST_TABLE)
    other_client = self.create_client_for_nth_impalad(1)
    try:
      self.execute_query_expect_success(
          other_client, "select * from {}".format(WRITE_TEST_TABLE))
      self.execute_query_expect_success(
          self.client, "drop table {}".format(WRITE_TEST_TABLE),
          query_options={'sync_ddl': 1})
      error = self.execute_query_expect_failure(
          other_client, "select * from {}".format(WRITE_TEST_TABLE))
      assert "Could not resolve table reference" in str(error)
    finally:
      other_client.close()

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION})
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTICATALOG_IMPALAD_ARGS,
     catalogd_args=MULTICATALOG_CATALOGD_ARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_drop_while_unreachable(self, unique_database):
    """In a database that catalogd and a REST catalog both have, DROP TABLE cannot tell
    which catalog holds a table while the REST server is unreachable. It fails then,
    also for a table of catalogd, and works again once the server is back."""
    server = self.servers[0]
    hms_table = "{}.hms_tbl".format(unique_database)
    rest_table = "{}.rest_tbl".format(unique_database)
    self.execute_query_expect_success(
        self.client, "create table {} (i int)".format(hms_table))
    self._create_rest_test_table(server, rest_table)
    self.rest_test_tables.append((server, rest_table, INSERT_TEST_TABLE))

    server.stop_rest_server()
    try:
      error = self.execute_query_expect_failure(
          self.client, "drop table {}".format(hms_table))
      assert "failed to list the tables of {}".format(unique_database) in str(error)
    finally:
      server.start_rest_server(300)
    self.execute_query_expect_success(self.client, "drop table {}".format(hms_table))
    self.execute_query_expect_success(
        self.client, "drop table {} purge".format(rest_table))
    requests.delete("http://localhost:{}/v1/namespaces/{}".format(
        server.port, unique_database), timeout=30).raise_for_status()

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE},
      {'port': 9085, 'catalog_location': SECONDARY_WRITE_TEST_CATALOG_LOCATION})
  @CustomClusterTestSuite.with_args(
     impalad_args=DUPLICATE_NAME_REST_IMPALAD_ARGS,
     catalogd_args=MULTICATALOG_CATALOGD_ARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_drop_without_unique_name(self):
    """With catalogd deployed, DROP TABLE on a REST catalog without a unique name is
    rejected, whether the table loads or fails to load."""
    self._check_drop_without_unique_name()

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


class TestIcebergRestCatalogStandalone(IcebergRestCatalogTests):
  """REST DML and DDL coverage for topologies without HMS."""

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
        "truncate table {}".format(table_name),
    ]
    for statement in unsupported_statements:
      error = self.execute_query_expect_failure(self.client, statement)
      assert ("AnalysisException: Only INSERT INTO, INSERT OVERWRITE, UPDATE, DELETE, "
              "MERGE and OPTIMIZE are supported") in str(error)

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_tables': (UPDATE_TEST_NS, UPDATE_TEST_TABLES)})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_update(self, vector):
    """Test UPDATE on Iceberg REST catalog tables (format-version 3)."""
    ns = UPDATE_TEST_NS
    self.run_test_case('QueryTest/iceberg-rest-update', vector, use_db=ns)

    table_name = "{}.update_basic".format(ns)
    data_path = get_fs_path(
        "/test-warehouse/iceberg_test/hadoop_catalog/"
        "{}/update_basic/data".format(ns))
    filesystem_client = HadoopFsCommandLineClient()

    # State left behind by the last section of iceberg-rest-update.test.
    expected_rows = ['10\tsame', '20\tsame', '30\tsame']
    committed_files = set(filesystem_client.ls(data_path))
    unknown_state = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitStateUnknownException@'
        'simulated REST commit state unknown'}
    error = self.execute_query_expect_failure(
        self.client,
        "update {} set s = 'fail' where i = 10".format(table_name),
        query_options=unknown_state)
    assert "simulated REST commit state unknown" in str(error)

    result = self.execute_query_expect_success(
        self.client, "select * from {} order by i".format(table_name))
    assert result.data == expected_rows
    # Files are kept when the commit state is unknown.
    orphan_files = set(filesystem_client.ls(data_path)) - committed_files
    assert len(orphan_files) > 0
    for orphan_file in orphan_files:
      assert filesystem_client.delete_file_dir(data_path + "/" + orphan_file)

    commit_failure = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitFailedException@'
        'simulated REST commit failure'}
    error = self.execute_query_expect_failure(
        self.client,
        "update {} set s = 'fail' where i = 10".format(table_name),
        query_options=commit_failure)
    assert "simulated REST commit failure" in str(error)

    result = self.execute_query_expect_success(
        self.client, "select * from {} order by i".format(table_name))
    assert result.data == expected_rows
    # Files written by a failed commit are cleaned up.
    assert set(filesystem_client.ls(data_path)) == committed_files

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_tables': (UPDATE_TEST_NS, UPDATE_PARTITIONS_TEST_TABLES)})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_update_partitions(self, vector):
    """Test UPDATE on partitioned Iceberg REST catalog tables (format-version 3)."""
    self.run_test_case(
        'QueryTest/iceberg-rest-update-partitions', vector, use_db=UPDATE_TEST_NS)

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_tables': (UPDATE_TEST_NS, DELETE_TEST_TABLES)})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_delete(self, vector):
    """Test DELETE on Iceberg REST catalog tables (format-version 3)."""
    ns = UPDATE_TEST_NS
    self.run_test_case('QueryTest/iceberg-rest-delete', vector, use_db=ns)

    table_name = "{}.ice_delete".format(ns)
    data_path = get_fs_path(
        "/test-warehouse/iceberg_test/hadoop_catalog/"
        "{}/ice_delete/data".format(ns))
    filesystem_client = HadoopFsCommandLineClient()

    # State left behind by the last ice_delete section of iceberg-rest-delete.test.
    expected_rows = ['10\tten']
    committed_files = set(filesystem_client.ls(data_path))
    unknown_state = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitStateUnknownException@'
        'simulated REST commit state unknown'}
    error = self.execute_query_expect_failure(
        self.client, "delete from {} where i = 10".format(table_name),
        query_options=unknown_state)
    assert "simulated REST commit state unknown" in str(error)

    result = self.execute_query_expect_success(
        self.client, "select * from {}".format(table_name))
    assert result.data == expected_rows
    # Files are kept when the commit state is unknown.
    orphan_files = set(filesystem_client.ls(data_path)) - committed_files
    assert len(orphan_files) > 0
    for orphan_file in orphan_files:
      assert filesystem_client.delete_file_dir(data_path + "/" + orphan_file)

    commit_failure = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitFailedException@'
        'simulated REST commit failure'}
    error = self.execute_query_expect_failure(
        self.client, "delete from {} where i = 10".format(table_name),
        query_options=commit_failure)
    assert "simulated REST commit failure" in str(error)

    result = self.execute_query_expect_success(
        self.client, "select * from {}".format(table_name))
    assert result.data == expected_rows
    # Files written by a failed commit are cleaned up.
    assert set(filesystem_client.ls(data_path)) == committed_files

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_tables': (UPDATE_TEST_NS, DELETE_PARTITIONED_TEST_TABLES)})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_delete_partitioned(self, vector):
    """Test DELETE on partitioned Iceberg REST catalog tables (format-version 3)."""
    self.run_test_case(
        'QueryTest/iceberg-rest-delete-partitioned', vector, use_db=UPDATE_TEST_NS)

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_tables': (MERGE_TEST_NS, MERGE_TEST_TABLES)})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_merge(self, vector):
    """Test MERGE on Iceberg REST catalog tables."""
    ns = MERGE_TEST_NS
    self.run_test_case('QueryTest/iceberg-rest-merge', vector, use_db=ns)

    table_name = "{}.s14_target".format(ns)
    data_path = get_fs_path(
        "/test-warehouse/iceberg_test/hadoop_catalog/"
        "{}/s14_target/data".format(ns))
    filesystem_client = HadoopFsCommandLineClient()
    # Writes both a data file and a deletion vector.
    merge = ("merge into {} t using (values (2 id, 'fail' val), (4, 'new')) s "
             "on t.id = s.id when matched then update set val = s.val "
             "when not matched then insert values (s.id, s.val)").format(table_name)
    select = "select * from {} order by id".format(table_name)

    # State left behind by the last s14_target section of iceberg-rest-merge.test.
    expected_rows = ['1\tone', '2\tagain', '3\tagain']
    committed_files = set(filesystem_client.ls(data_path))
    unknown_state = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitStateUnknownException@'
        'simulated REST commit state unknown'}
    error = self.execute_query_expect_failure(
        self.client, merge, query_options=unknown_state)
    assert "simulated REST commit state unknown" in str(error)

    result = self.execute_query_expect_success(self.client, select)
    assert result.data == expected_rows
    # Files are kept when the commit state is unknown.
    orphan_files = set(filesystem_client.ls(data_path)) - committed_files
    assert len(orphan_files) > 0
    for orphan_file in orphan_files:
      assert filesystem_client.delete_file_dir(data_path + "/" + orphan_file)

    commit_failure = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitFailedException@'
        'simulated REST commit failure'}
    error = self.execute_query_expect_failure(
        self.client, merge, query_options=commit_failure)
    assert "simulated REST commit failure" in str(error)

    result = self.execute_query_expect_success(self.client, select)
    assert result.data == expected_rows
    # Files written by a failed commit are cleaned up.
    assert set(filesystem_client.ls(data_path)) == committed_files

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_tables': (MERGE_TEST_NS, MERGE_PARTITION_SORT_TEST_TABLES)})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_merge_partition_sort(self, vector):
    """Test MERGE on partitioned and sorted Iceberg REST catalog tables
    (format-version 2)."""
    self.run_test_case(
        'QueryTest/iceberg-rest-merge-partition-sort', vector, use_db=MERGE_TEST_NS)

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_tables': (UPDATE_TEST_NS, OPTIMIZE_TEST_TABLES)})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_optimize(self, vector):
    """Test OPTIMIZE on Iceberg REST catalog tables."""
    ns = UPDATE_TEST_NS
    self.run_test_case('QueryTest/iceberg-rest-optimize', vector, use_db=ns)

    table_name = "{}.ice_optimize".format(ns)
    data_path = get_fs_path(
        "/test-warehouse/iceberg_test/hadoop_catalog/"
        "{}/ice_optimize/data".format(ns))
    filesystem_client = HadoopFsCommandLineClient()

    # State left behind by the last ice_optimize section of iceberg-rest-optimize.test.
    # A full table OPTIMIZE rewrites even a table that has a single data file.
    expected_rows = ['1\tone\ttrue', '3\tthree\ttrue']
    committed_files = set(filesystem_client.ls(data_path))
    unknown_state = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitStateUnknownException@'
        'simulated REST commit state unknown'}
    error = self.execute_query_expect_failure(
        self.client, "optimize table {}".format(table_name),
        query_options=unknown_state)
    assert "simulated REST commit state unknown" in str(error)

    result = self.execute_query_expect_success(
        self.client, "select * from {}".format(table_name))
    assert sorted(result.data) == expected_rows
    # Files are kept when the commit state is unknown.
    orphan_files = set(filesystem_client.ls(data_path)) - committed_files
    assert len(orphan_files) == 1
    for orphan_file in orphan_files:
      assert filesystem_client.delete_file_dir(data_path + "/" + orphan_file)

    commit_failure = {'debug_action':
        'ICEBERG_COMMIT:EXCEPTION@CommitFailedException@'
        'simulated REST commit failure'}
    error = self.execute_query_expect_failure(
        self.client, "optimize table {}".format(table_name),
        query_options=commit_failure)
    assert "simulated REST commit failure" in str(error)

    result = self.execute_query_expect_success(
        self.client, "select * from {}".format(table_name))
    assert sorted(result.data) == expected_rows
    # Files written by a failed commit are cleaned up.
    assert set(filesystem_client.ls(data_path)) == committed_files

    # Without catalogd, nothing orders the commits of a REST table but Iceberg's own
    # validation. An OPTIMIZE must keep the rows that a concurrent INSERT commits, and
    # must fail if a concurrent DELETE removes rows from the files that it rewrites.
    # The debug action of the commit failure is still set in the session.
    self.client.clear_configuration()
    table_name = "{}.ice_optimize_conflict".format(ns)
    self.execute_query_expect_success(
        self.client, "insert into {} values (1), (2)".format(table_name))
    self._run_concurrently_with_optimize(
        table_name, "insert into {} values (3)".format(table_name))
    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(table_name))
    assert sorted(result.data) == ['1', '2', '3']

    error = self._run_concurrently_with_optimize(
        table_name, "delete from {} where i = 1".format(table_name),
        expect_failure=True)
    assert "found new delete for replaced data file" in error
    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(table_name))
    assert sorted(result.data) == ['2', '3']

  def _run_concurrently_with_optimize(self, table_name, concurrent_stmt,
      expect_failure=False):
    """Runs 'concurrent_stmt' while an OPTIMIZE of 'table_name' waits before its commit.
    Checks that the OPTIMIZE fails iff 'expect_failure' is true, and that a failed
    OPTIMIZE leaves no files behind. Returns the error of a failed OPTIMIZE."""
    namespace, table = table_name.split('.', 1)
    data_path = get_fs_path("{}/{}/{}/data".format(
        WRITE_TEST_CATALOG_LOCATION, namespace, table))
    filesystem_client = HadoopFsCommandLineClient()
    files_before = set(filesystem_client.ls(data_path))

    # The delay is after the transaction is created from the table loaded at planning,
    # so the commit has to refresh the table and validate the rewrite again.
    self.client.set_configuration(
        {'debug_action': 'CATALOGD_ICEBERG_CONFLICT:SLEEP@5000'})
    handle = self.client.execute_async("optimize table {}".format(table_name))
    self.client.clear_configuration()
    self.client.wait_for_impala_state(handle, RUNNING, 30)
    self.execute_query_expect_success(self.client, concurrent_stmt)
    if not expect_failure:
      assert self.client.wait_for_finished_timeout(handle, 30)
      self.client.close_query(handle)
      return None
    with pytest.raises(IMPALA_CONNECTION_EXCEPTION) as error:
      self.client.wait_for_finished_timeout(handle, 30)

    # Every file written since the OPTIMIZE started belongs to 'concurrent_stmt'.
    result = self.execute_query_expect_success(
        self.client, "select file_path from {}.`files`".format(table_name))
    table_files = set(path.rsplit('/', 1)[-1] for path in result.data)
    new_files = set(filesystem_client.ls(data_path)) - files_before
    assert new_files
    assert new_files <= table_files
    return str(error.value)

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_drop_table(self):
    self._run_rest_drop_table_test(self.servers[0], WRITE_TEST_TABLE)
    self._check_drop_table_in_missing_db()

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_drop_unloadable_table(self):
    """With a single REST catalog, a table that fails to load can still be dropped."""
    server = self.servers[0]
    self._create_rest_test_table(
        server, UNLOADABLE_TEST_TABLE, definition=UNLOADABLE_TEST_DEFINITION)
    self.rest_test_tables.append(
        (server, UNLOADABLE_TEST_TABLE, UNLOADABLE_TEST_DEFINITION))
    error = self.execute_query_expect_failure(
        self.client, "select * from {}".format(UNLOADABLE_TEST_TABLE))
    assert "Could not load table" in str(error)
    result = self.execute_query_expect_success(
        self.client, "drop table {}".format(UNLOADABLE_TEST_TABLE))
    assert result.data == ['Table has been dropped.']
    assert requests.get(self._rest_table_url(server, UNLOADABLE_TEST_TABLE),
        timeout=30).status_code == 404

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE})
  @CustomClusterTestSuite.with_args(
     impalad_args="{} --blacklisted_tables={}".format(
         REST_STANDALONE_IMPALAD_ARGS, WRITE_TEST_TABLE),
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_drop_blacklisted_table(self):
    """Like catalogd, the coordinator never drops a blacklisted table, not even with
    IF EXISTS and PURGE: analysis hides it, and answers IF EXISTS as catalogd does."""
    server = self.servers[0]
    result = self.execute_query_expect_success(
        self.client, "drop table if exists {} purge".format(WRITE_TEST_TABLE))
    assert result.data == ["Can't drop blacklisted table: {}".format(WRITE_TEST_TABLE)]
    result = self.execute_query_expect_success(
        self.client, "drop table if exists {} purge".format(WRITE_TEST_TABLE.upper()))
    assert result.data == ["Can't drop blacklisted table: {}".format(WRITE_TEST_TABLE)]
    error = self.execute_query_expect_failure(
        self.client, "drop table {} purge".format(WRITE_TEST_TABLE))
    assert "Table does not exist" in str(error)
    assert requests.get(self._rest_table_url(server, WRITE_TEST_TABLE),
        timeout=30).status_code == 200

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
  def test_multiple_rest_catalogs_route_writes(self):
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

    # A MERGE whose source is in another REST catalog commits only to the target's.
    # It deletes the matched row 1 and inserts the unmatched row 2.
    self.execute_query_expect_success(
        self.client, "insert into {} values (1)".format(SECONDARY_WRITE_TEST_TABLE))
    self.execute_query_expect_success(self.client,
        "merge into {} t using {} s on t.i = s.i when matched then delete "
        "when not matched then insert values (s.i)".format(
            WRITE_TEST_TABLE, SECONDARY_WRITE_TEST_TABLE))
    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(WRITE_TEST_TABLE))
    assert result.data == ['2']
    assert self._snapshot_count(WRITE_TEST_TABLE) == 2
    assert self._snapshot_count(SECONDARY_WRITE_TEST_TABLE) == 2

    # DDL is routed by the same catalog name as DML: each table is dropped through
    # the REST catalog that owns it.
    self.execute_query_expect_success(
        self.client, "drop table {}".format(SECONDARY_WRITE_TEST_TABLE))
    assert requests.get(self._rest_table_url(self.servers[1],
        SECONDARY_WRITE_TEST_TABLE), timeout=30).status_code == 404
    assert requests.get(self._rest_table_url(self.servers[0], WRITE_TEST_TABLE),
        timeout=30).status_code == 200
    self.execute_query_expect_success(
        self.client, "drop table {}".format(WRITE_TEST_TABLE))
    assert requests.get(self._rest_table_url(self.servers[0], WRITE_TEST_TABLE),
        timeout=30).status_code == 404

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE,
       'partitioned_test_table': PARTITIONED_WRITE_TEST_TABLE})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_STANDALONE_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_insert_overwrite(self, vector):
    self.run_test_case('QueryTest/iceberg-rest-insert-overwrite', vector,
        use_db="impala_rest_dml_test")

    # Without catalogd, nothing orders the commits of a REST table but Iceberg's own
    # validation. An overwrite planned before a concurrent write commits must fail
    # instead of erasing that write.
    table_name = WRITE_TEST_TABLE
    self.execute_query_expect_success(
        self.client, "insert overwrite {} values (1)".format(table_name))
    self._check_overwrite_conflict(
        table_name, "insert into {} values (2)".format(table_name))
    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(table_name))
    assert sorted(result.data) == ['1', '2']

    self._check_overwrite_conflict(
        table_name, "insert overwrite {} values (3)".format(table_name))
    result = self.execute_query_expect_success(
        self.client, "select i from {}".format(table_name))
    assert result.data == ['3']

  def _check_overwrite_conflict(self, table_name, concurrent_stmt):
    """Runs 'concurrent_stmt' while a slow INSERT OVERWRITE on 'table_name' is
    executing, and checks that the overwrite fails to commit and leaves no files."""
    namespace, table = table_name.split('.', 1)
    data_path = get_fs_path("{}/{}/{}/data".format(
        WRITE_TEST_CATALOG_LOCATION, namespace, table))
    filesystem_client = HadoopFsCommandLineClient()
    files_before = set(filesystem_client.ls(data_path))

    handle = self.client.execute_async(
        "insert overwrite {} select sleep(5000)".format(table_name))
    time.sleep(1)
    self.execute_query_expect_success(self.client, concurrent_stmt)
    with pytest.raises(IMPALA_CONNECTION_EXCEPTION) as error:
      self.client.wait_for_finished_timeout(handle, 30)
    assert "Found conflicting files" in str(error.value)

    # Every file written since the overwrite started belongs to 'concurrent_stmt'.
    result = self.execute_query_expect_success(
        self.client, "select file_path from {}.`files`".format(table_name))
    table_files = set(path.rsplit('/', 1)[-1] for path in result.data)
    new_files = set(filesystem_client.ls(data_path)) - files_before
    assert new_files
    assert new_files <= table_files

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE},
      {'port': 9085,
       'catalog_location': SECONDARY_WRITE_TEST_CATALOG_LOCATION})
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTIPLE_REST_WITHOUT_CATALOGD_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_multiple_rest_catalogs_drop_unloadable_table(self):
    """A table that fails to load is dropped through the REST catalog that lists it,
    even when another catalog, visited first, has a database of the same name."""
    secondary = self.servers[1]
    self._create_rest_test_table(
        secondary, UNLOADABLE_TEST_TABLE, definition=UNLOADABLE_TEST_DEFINITION)
    self.rest_test_tables.append(
        (secondary, UNLOADABLE_TEST_TABLE, UNLOADABLE_TEST_DEFINITION))
    result = self.execute_query_expect_success(
        self.client, "drop table {}".format(UNLOADABLE_TEST_TABLE))
    assert result.data == ['Table has been dropped.']
    assert requests.get(self._rest_table_url(secondary, UNLOADABLE_TEST_TABLE),
        timeout=30).status_code == 404
    assert requests.get(self._rest_table_url(self.servers[0], WRITE_TEST_TABLE),
        timeout=30).status_code == 200
    self._check_drop_table_in_missing_db()

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE},
      {'port': 9085, 'catalog_location': SECONDARY_WRITE_TEST_CATALOG_LOCATION})
  @CustomClusterTestSuite.with_args(
     impalad_args=DUPLICATE_NAME_REST_WITHOUT_CATALOGD_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_rest_catalog_drop_without_unique_name(self):
    """Without catalogd, DROP TABLE on a REST catalog without a unique name is
    rejected, whether the table loads or fails to load."""
    self._check_drop_without_unique_name()

  @RestServerProperties(
      {'port': 9084, 'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE},
      {'port': 9085, 'catalog_location': SECONDARY_WRITE_TEST_CATALOG_LOCATION,
       'test_table': WRITE_TEST_TABLE})
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTIPLE_REST_WITHOUT_CATALOGD_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_multiple_rest_catalogs_drop_ambiguous_table(self):
    """A table that two REST catalogs both list is ambiguous: DROP TABLE is rejected,
    also with IF EXISTS, and neither catalog drops it."""
    for stmt in ["drop table {}", "drop table if exists {} purge"]:
      error = self.execute_query_expect_failure(
          self.client, stmt.format(WRITE_TEST_TABLE))
      assert "Ambiguous table name" in str(error)
    for server in self.servers:
      assert requests.get(self._rest_table_url(server, WRITE_TEST_TABLE),
          timeout=30).status_code == 200

  def _check_drop_table_in_missing_db(self):
    """Without catalogd, a database that no REST catalog serves belongs to the first
    REST catalog, with one REST catalog as well as with several. IF EXISTS is a no-op
    in the coordinator, as for any missing REST table. Without IF EXISTS, analysis
    reports the missing database."""
    result = self.execute_query_expect_success(
        self.client, "drop table if exists no_such_db.no_such_table")
    assert result.data == ['Table does not exist.']
    error = self.execute_query_expect_failure(
        self.client, "drop table no_such_db.no_such_table")
    assert "Database does not exist: no_such_db" in str(error)

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

  @RestServerProperties({'port': 9084})
  @CustomClusterTestSuite.with_args(
     impalad_args=REST_VENDED_CREDENTIALS_IMPALAD_ARGS,
     start_args=NO_CATALOGD_STARTARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_vended_credentials_enabled(self, vector):
    # With vending enabled but no credentials actually vended, load and scan must still
    # succeed via the normal path.
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


class LostCommitResponseProxy(object):
  """HTTP proxy in front of the REST server. When armed for a table, it forwards the
  next commit request of that table to the REST server, then closes the connection
  without returning the server's response, as if the network failed after the commit
  was applied. Every other request is forwarded unchanged."""

  def __init__(self, target_port):
    proxy = self
    self.target_port = target_port
    self.armed_table = None
    self.dropped_responses = []
    self.commit_requests = 0

    class Handler(BaseHTTPRequestHandler):
      def log_message(self, _format, *_args):
        pass

      def _forward(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length) if length else None
        headers = dict((k, v) for k, v in self.headers.items()
                       if k.lower() not in ('host', 'connection', 'content-length'))
        conn = http.client.HTTPConnection('localhost', proxy.target_port, timeout=60)
        conn.request(self.command, self.path, body=body, headers=headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()

        match = re.match(r'^/v1/namespaces/[^/]+/tables/([^/?]+)$', self.path)
        if self.command == 'POST' and match:
          proxy.commit_requests += 1
          if match.group(1) == proxy.armed_table:
            proxy.armed_table = None
            proxy.dropped_responses.append(response.status)
            self.close_connection = True
            return

        self.send_response(response.status)
        for k, v in response.getheaders():
          if k.lower() not in ('transfer-encoding', 'connection', 'content-length'):
            self.send_header(k, v)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

      do_GET = do_POST = do_DELETE = do_HEAD = _forward

    self.server = ThreadingHTTPServer(('localhost', 0), Handler)
    self.port = self.server.server_address[1]
    self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
    self.thread.start()

  def stop(self):
    self.server.shutdown()
    self.server.server_close()


LOST_RESPONSE_NS = "impala_rest_lost_response_test"
LOST_RESPONSE_CATALOG_CONFIG_DIR = os.path.join(
    IMPALA_HOME, 'logs', 'iceberg_rest_lost_response_config')


class TestIcebergRestCatalogLostCommitResponse(IcebergRestCatalogTests):
  """Commits whose response is lost on the way back from the REST catalog."""

  @classmethod
  def need_default_clients(cls):
    return False

  def setup_method(self, method):
    # impalad connects to the REST catalog at startup, so the proxy and its catalog
    # config must exist before the cluster starts.
    self.proxy = LostCommitResponseProxy(IcebergRestServer.DEFAULT_REST_SERVER_PORT)
    if not os.path.isdir(LOST_RESPONSE_CATALOG_CONFIG_DIR):
      os.makedirs(LOST_RESPONSE_CATALOG_CONFIG_DIR)
    with open(os.path.join(LOST_RESPONSE_CATALOG_CONFIG_DIR, 'rest.properties'),
              'w') as f:
      f.write("connector.name=iceberg\n"
              "iceberg.catalog.type=rest\n"
              "iceberg.rest-catalog.name=rest-1\n"
              "iceberg.rest-catalog.uri=http://localhost:{}\n".format(self.proxy.port))
    try:
      super(TestIcebergRestCatalogLostCommitResponse, self).setup_method(method)
    except Exception:
      self.proxy.stop()
      raise

  def teardown_method(self, method):
    try:
      super(TestIcebergRestCatalogLostCommitResponse, self).teardown_method(method)
    finally:
      self.proxy.stop()

  @RestServerProperties(
      {'port': IcebergRestServer.DEFAULT_REST_SERVER_PORT,
       'catalog_location': WRITE_TEST_CATALOG_LOCATION,
       'test_tables': (LOST_RESPONSE_NS, {'lost_response': {
           'fields': [{'id': 1, 'name': 'i', 'required': False, 'type': 'int'},
                      {'id': 2, 'name': 's', 'required': False, 'type': 'string'}],
           'properties': {'format-version': '3'}}})})
  @CustomClusterTestSuite.with_args(
     impalad_args="--use_local_catalog=true --catalogd_deployed=false "
                  "--catalog_config_dir={}".format(LOST_RESPONSE_CATALOG_CONFIG_DIR),
     start_args=NO_CATALOGD_STARTARGS,
     cluster_size=1)
  @pytest.mark.execute_serially
  def test_update_with_lost_commit_response(self):
    """The REST catalog applies the commit of an UPDATE, but its response is lost. The
    commit state is unknown to Impala, so the files the committed snapshot references
    must not be deleted."""
    table_name = "{}.lost_response".format(LOST_RESPONSE_NS)
    data_path = get_fs_path("{}/{}/lost_response/data".format(
        WRITE_TEST_CATALOG_LOCATION, LOST_RESPONSE_NS))
    filesystem_client = HadoopFsCommandLineClient()

    self.execute_query_expect_success(
        self.client, "insert into {} values (1, 'one'), (2, 'two')".format(table_name))
    assert self._snapshot_count(table_name) == 1
    files_before = set(filesystem_client.ls(data_path))

    self.proxy.armed_table = 'lost_response'
    self.execute_query_expect_failure(
        self.client, "update {} set s = 'updated' where i = 1".format(table_name))
    self.proxy.armed_table = None

    # The REST catalog did apply the commit, only its response was lost.
    assert self.proxy.dropped_responses == [200]
    # The files written by the UPDATE are kept.
    assert set(filesystem_client.ls(data_path)) - files_before
    assert self._snapshot_count(table_name) == 2

    # So the committed snapshot must still be readable.
    result = self.execute_query_expect_success(
        self.client, "select * from {} order by i".format(table_name))
    assert result.data == ['1\tupdated', '2\ttwo']

  def _snapshot_count(self, table_name):
    result = self.execute_query_expect_success(
        self.client, "select count(*) from {}.snapshots".format(table_name))
    return int(result.data[0])
