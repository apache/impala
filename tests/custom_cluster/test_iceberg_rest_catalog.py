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

REST_SERVER_PORT = 9084
IMPALA_HOME = os.environ['IMPALA_HOME']
START_ARGS = 'start_args'
IMPALAD_ARGS = """--use_local_catalog=true --catalogd_deployed=false
    --catalog_config_dir={}/testdata/configs/catalog_configs/iceberg_rest_config"""\
        .format(IMPALA_HOME)


class TestIcebergRestCatalog(CustomClusterTestSuite):
  """Test suite for Iceberg REST Catalog."""

  @classmethod
  def need_default_clients(cls):
    """There will be no HMS, so we shouldn't create the Hive client."""
    return False

  @classmethod
  def setup_class(cls):
    super(TestIcebergRestCatalog, cls).setup_class()
    try:
      cls.iceberg_rest_server = IcebergRestServer()
      cls.iceberg_rest_server.start_rest_server(300)
    except Exception as e:
      cls.iceberg_rest_server.stop_rest_server(10)
      raise e

    try:
      cls._stop_hive_service()
    except Exception as e:
      cls.cleanup_infra_services()
      raise e

  @classmethod
  def teardown_class(cls):
    cls.cleanup_infra_services()
    return super(TestIcebergRestCatalog, cls).teardown_class()

  @classmethod
  def cleanup_infra_services(cls):
    cls.iceberg_rest_server.stop_rest_server(10)
    cls._start_hive_service(None)

  def setup_method(self, method):
    args = method.__dict__
    if HIVE_CONF_DIR in args:
      raise Exception("Cannot specify HIVE_CONF_DIR because the tests of this class are "
          "running without Hive.")
    # Invoke start-impala-cluster.py with '--no_catalogd'
    start_args = "--no_catalogd"
    if START_ARGS in args:
      start_args = args[START_ARGS] + " " + start_args
    args[START_ARGS] = start_args

    super(TestIcebergRestCatalog, self).setup_method(method)
    # At this point we can create the Impala clients that we will need.
    self.create_impala_clients()

  @CustomClusterTestSuite.with_args(impalad_args=IMPALAD_ARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_basic(self, vector):
    self.run_test_case('QueryTest/iceberg-rest-catalog', vector, use_db="ice")
