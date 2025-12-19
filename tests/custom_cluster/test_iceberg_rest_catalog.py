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
import os
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite, HIVE_CONF_DIR
from tests.common.iceberg_rest_server import IcebergRestServer


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


def RestServerProperties(*server_configs):
  """
  Annotation to specify configurations for multiple REST servers to be started.
  Each argument should be a dictionary with 'port' and 'catalog_location' keys.
  Example:
  @RestServerProperties({'port': 9085}, {'port': 9086, 'catalog_location': '/tmp/cat2'})
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

    server_configs = getattr(method, 'rest_server_configs', None)

    if server_configs:
      for config in server_configs:
        port = config.get('port', IcebergRestServer.DEFAULT_REST_SERVER_PORT)
        catalog_location = config.get('catalog_location',
            IcebergRestServer.DEFAULT_CATALOG_LOCATION)
        print("Starting REST server with annotation properties: "
              "Port=%s, Catalog Location=%s" % (port, catalog_location))
        self.servers.append(IcebergRestServer(port, catalog_location))

    try:
      for server in self.servers:
        server.start_rest_server(300)
      super(IcebergRestCatalogTests, self).setup_method(method)
      # At this point we can create the Impala clients that we will need.
      self.create_impala_clients()
    except Exception as e:
      for server in self.servers:
        server.stop_rest_server(10)
      raise e

  def teardown_method(self, method):
    for server in self.servers:
      server.stop_rest_server()
    super(IcebergRestCatalogTests, self).teardown_method(method)


class TestIcebergRestCatalogWithHms(IcebergRestCatalogTests):
  """Test suite for Iceberg REST Catalog. HMS running while tests are running"""
  @RestServerProperties({'port': 9084})
  @CustomClusterTestSuite.with_args(
     impalad_args=MULTICATALOG_IMPALAD_ARGS,
     catalogd_args=MULTICATALOG_CATALOGD_ARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_multicatalog(self, vector):
    self.run_test_case('QueryTest/iceberg-multicatalog',
                       vector, use_db="ice")

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
