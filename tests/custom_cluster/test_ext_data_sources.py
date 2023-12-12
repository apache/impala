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
import os
import subprocess

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestExtDataSources(CustomClusterTestSuite):
  """Impala query tests for external data sources."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_data_source_tables(self, vector, unique_database):
    """Start Impala cluster in LocalCatalog Mode"""
    self.run_test_case('QueryTest/data-source-tables', vector, use_db=unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_jdbc_data_source(self, vector, unique_database):
    """Start Impala cluster in LocalCatalog Mode"""
    self.run_test_case('QueryTest/jdbc-data-source', vector, use_db=unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args='--data_source_batch_size=2048')
  def test_data_source_big_batch_size(self, vector, unique_database):
    """Run test with batch size greater than default size 1024"""
    self.run_test_case('QueryTest/data-source-tables', vector, use_db=unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args='--data_source_batch_size=512')
  def test_data_source_small_batch_size(self, vector, unique_database):
    """Run test with batch size less than default size 1024"""
    self.run_test_case('QueryTest/data-source-tables', vector, use_db=unique_database)


class TestMySqlExtJdbcTables(CustomClusterTestSuite):
  """Impala query tests for external jdbc tables on MySQL server."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def _setup_mysql_test_env(cls):
    # Download MySQL docker image and jdbc driver, start MySQL server, create database
    # and tables, create user account, load testing data, copy jdbc driver to HDFS, etc.
    script = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/setup-mysql-env.sh')
    run_cmd = [script]
    try:
      subprocess.check_call(run_cmd, close_fds=True)
    except subprocess.CalledProcessError as e:
      if e.returncode == 10:
        pytest.skip("These tests requireadd the docker to be added to sudoer's group")
      elif e.returncode == 20:
        pytest.skip("Can't connect to local MySQL server")
      elif e.returncode == 30:
        pytest.skip("File /var/run/mysqld/mysqld.sock not found")
      else:
        assert False, "Failed to setup MySQL testing environment"

  @classmethod
  def _remove_mysql_test_env(cls):
    # Tear down MySQL server, remove its docker image, etc.
    script = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/clean-mysql-env.sh')
    run_cmd = [script]
    subprocess.check_call(run_cmd, close_fds=True)

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('These tests only run in exhaustive')
    cls._setup_mysql_test_env()
    super(TestMySqlExtJdbcTables, cls).setup_class()

  @classmethod
  def teardown_class(cls):
    cls._remove_mysql_test_env()
    super(TestMySqlExtJdbcTables, cls).teardown_class()

  @pytest.mark.execute_serially
  def test_mysql_ext_jdbc_tables(self, vector, unique_database):
    """Run tests for external jdbc tables on MySQL"""
    self.run_test_case('QueryTest/mysql-ext-jdbc-tables', vector, use_db=unique_database)
