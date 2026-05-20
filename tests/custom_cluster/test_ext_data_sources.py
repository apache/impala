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
import re
import time
import pytest
import requests
import subprocess

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout
from tests.common.skip import SkipIfApacheHive
from tests.common.test_dimensions import create_exec_option_dimension
from tests.util.filesystem_utils import FILESYSTEM_PREFIX
from time import sleep


class TestExtDataSources(CustomClusterTestSuite):
  """Impala query tests for external data sources."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestExtDataSources, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
      exec_single_node_option=[100]))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal")
  def test_data_source_tables(self, vector, unique_database, unique_name):
    """Start Impala cluster in LocalCatalog Mode"""
    self.run_test_case('QueryTest/data-source-tables', vector, use_db=unique_database,
                       test_file_vars={'$UNIQUE_DATASOURCE': unique_name})

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
  def test_data_source_big_batch_size(self, vector, unique_database, unique_name):
    """Run test with batch size greater than default size 1024"""
    self.run_test_case('QueryTest/data-source-tables', vector, use_db=unique_database,
        test_file_vars={'$UNIQUE_DATASOURCE': unique_name})

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args='--data_source_batch_size=512')
  def test_data_source_small_batch_size(self, vector, unique_database, unique_name):
    """Run test with batch size less than default size 1024"""
    self.run_test_case('QueryTest/data-source-tables', vector, use_db=unique_database,
        test_file_vars={'$UNIQUE_DATASOURCE': unique_name})

  @SkipIfApacheHive.data_connector_not_supported
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="--statestore_update_frequency_ms=1000")
  def test_restart_catalogd(self):
    """Restart Catalog server after creating a data source. Verify that the data source
    object is persistent across restarting of Catalog server."""
    DROP_DATA_SOURCE_QUERY = "DROP DATA SOURCE IF EXISTS test_restart_persistent"
    CREATE_DATA_SOURCE_QUERY = "CREATE DATA SOURCE test_restart_persistent " \
        "LOCATION '/test-warehouse/data-sources/jdbc-data-source.jar' " \
        "CLASS 'org.apache.impala.extdatasource.jdbc.PersistentJdbcDataSource' " \
        "API_VERSION 'V1'"
    SHOW_DATA_SOURCE_QUERY = "SHOW DATA SOURCES LIKE 'test_restart_*'"

    # Create a data source and verify that the object is created successfully.
    self.execute_query_expect_success(self.client, DROP_DATA_SOURCE_QUERY)
    self.execute_query_expect_success(self.client, CREATE_DATA_SOURCE_QUERY)
    result = self.execute_query(SHOW_DATA_SOURCE_QUERY)
    assert result.success, str(result)
    assert "PersistentJdbcDataSource" in result.get_data()

    # Restart Catalog server.
    self.cluster.catalogd.restart()
    wait_time_s = build_flavor_timeout(90, slow_build_timeout=180)
    self.cluster.statestored.service.wait_for_metric_value('statestore.live-backends',
        expected_value=4, timeout=wait_time_s)

    # Verify that the data source object is still available after restarting Catalog
    # server.
    result = self.execute_query(SHOW_DATA_SOURCE_QUERY)
    assert result.success, str(result)
    assert "PersistentJdbcDataSource" in result.get_data()
    # Remove the data source
    self.execute_query_expect_success(self.client, DROP_DATA_SOURCE_QUERY)
    result = self.execute_query(SHOW_DATA_SOURCE_QUERY)
    assert result.success, str(result)
    assert "PersistentJdbcDataSource" not in result.get_data()

  @SkipIfApacheHive.data_connector_not_supported
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000",
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false "
                  "--enable_reload_events=true",
    start_args="--enable_catalogd_ha")
  def test_catalogd_ha_failover(self):
    """The test case for cluster started with catalogd HA enabled."""
    DROP_DATA_SOURCE_QUERY = "DROP DATA SOURCE IF EXISTS test_failover_persistent"
    CREATE_DATA_SOURCE_QUERY = "CREATE DATA SOURCE test_failover_persistent " \
        "LOCATION '/test-warehouse/data-sources/jdbc-data-source.jar' " \
        "CLASS 'org.apache.impala.extdatasource.jdbc.FailoverInSyncJdbcDataSource' " \
        "API_VERSION 'V1'"
    SHOW_DATA_SOURCE_QUERY = "SHOW DATA SOURCES LIKE 'test_failover_*'"
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Create a data source and verify that the object is created successfully.
    self.execute_query_expect_success(self.client, DROP_DATA_SOURCE_QUERY)
    self.execute_query_expect_success(self.client, CREATE_DATA_SOURCE_QUERY)
    result = self.execute_query(SHOW_DATA_SOURCE_QUERY)
    assert result.success, str(result)
    assert "FailoverInSyncJdbcDataSource" in result.get_data()

    # Kill active catalogd
    catalogds[0].kill()
    # Wait for long enough for the statestore to detect the failure of active catalogd
    # and assign active role to standby catalogd.
    catalogd_service_2.wait_for_metric_value(
        "catalog-server.active-status", expected_value=True, timeout=30)
    assert(catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Wait until coordinator receive failover notification.
    coordinator_service = self.cluster.impalads[0].service
    expected_catalog_service_port = catalogd_service_2.get_catalog_service_port()
    received_failover_notification = False
    retry_count = 30
    while (retry_count > 0):
      active_catalogd_address = \
          coordinator_service.get_metric_value("catalog.active-catalogd-address")
      _, catalog_service_port = active_catalogd_address.split(":")
      if (int(catalog_service_port) == expected_catalog_service_port):
        received_failover_notification = True
        break
      retry_count -= 1
      sleep(1)
    assert received_failover_notification, \
        "Coordinator did not receive notification of Catalog service failover."

    # Verify that the data source object is available in the catalogd of HA pair.
    result = self.execute_query(SHOW_DATA_SOURCE_QUERY)
    assert result.success, str(result)
    assert "FailoverInSyncJdbcDataSource" in result.get_data()
    # Remove the data source
    self.execute_query_expect_success(self.client, DROP_DATA_SOURCE_QUERY)
    result = self.execute_query(SHOW_DATA_SOURCE_QUERY)
    assert result.success, str(result)
    assert "FailoverInSyncJdbcDataSource" not in result.get_data()


class TestPostgresJdbcTables(CustomClusterTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestPostgresJdbcTables, cls).setup_class()

  @pytest.mark.execute_serially
  def test_postgres_jdbc_tables(self, vector, unique_database):
    driver_url = FILESYSTEM_PREFIX +\
        "/test-warehouse/data-sources/jdbc-drivers/postgresql-jdbc.jar"
    sql = """
    DROP TABLE IF EXISTS {0}.country_postgres;
    CREATE EXTERNAL TABLE {0}.country_postgres (
      id INT,
      name STRING,
      bool_col BOOLEAN,
      tinyint_col     SMALLINT,
      smallint_col    SMALLINT,
      int_col         INT,
      bigint_col      BIGINT,
      float_col       FLOAT,
      double_col      DOUBLE,
      date_col        DATE,
      string_col      STRING,
      timestamp_col   TIMESTAMP)
    STORED BY JDBC
    TBLPROPERTIES (
      "database.type"="POSTGRES",
      "jdbc.url"="jdbc:postgresql://localhost:5432/functional",
      "jdbc.auth"="AuthMech=0",
      "jdbc.driver"="org.postgresql.Driver",
      "driver.url"="{1}",
      "dbcp.username"="hiveuser",
      "dbcp.password"="password",
      "table"="country");

    DROP TABLE IF EXISTS {0}.quoted_col;
    CREATE EXTERNAL TABLE {0}.quoted_col
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        `freeze`      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY JDBC
    TBLPROPERTIES (
      "database.type"="POSTGRES",
      "jdbc.url"="jdbc:postgresql://localhost:5432/functional",
      "jdbc.auth"="AuthMech=0",
      "jdbc.driver"="org.postgresql.Driver",
      "driver.url"="{1}",
      "dbcp.username"="hiveuser",
      "dbcp.password"="password",
      "table"="quoted_col"
    );

    DROP TABLE IF EXISTS {0}.country_keystore_postgres;
    CREATE EXTERNAL TABLE {0}.country_keystore_postgres (
      id INT,
      name STRING,
      bool_col BOOLEAN,
      tinyint_col     SMALLINT,
      smallint_col    SMALLINT,
      int_col         INT,
      bigint_col      BIGINT,
      float_col       FLOAT,
      double_col      DOUBLE,
      date_col        DATE,
      string_col      STRING,
      timestamp_col   TIMESTAMP)
    STORED BY JDBC
    TBLPROPERTIES (
      "database.type"="POSTGRES",
      "jdbc.url"="jdbc:postgresql://localhost:5432/functional",
      "jdbc.auth"="AuthMech=0",
      "jdbc.driver"="org.postgresql.Driver",
      "driver.url"="{1}",
      "dbcp.username"="hiveuser",
      "dbcp.password"="password",
      "table"="country");

    DROP TABLE IF EXISTS {0}.country_postgres_query;
    CREATE EXTERNAL TABLE {0}.country_postgres_query (
      id INT,
      name STRING,
      bool_col BOOLEAN,
      tinyint_col     SMALLINT,
      smallint_col    SMALLINT,
      int_col         INT,
      bigint_col      BIGINT,
      float_col       FLOAT,
      double_col      DOUBLE,
      date_col        DATE,
      string_col      STRING,
      timestamp_col   TIMESTAMP)
    STORED BY JDBC
    TBLPROPERTIES (
      "database.type"="POSTGRES",
      "jdbc.url"="jdbc:postgresql://localhost:5432/functional",
      "jdbc.auth"="AuthMech=0",
      "jdbc.driver"="org.postgresql.Driver",
      "driver.url"="{1}",
      "dbcp.username"="hiveuser",
      "dbcp.password"="password",
      "query"="select id,name,bool_col,tinyint_col,smallint_col,
        int_col,bigint_col,float_col,double_col,date_col,string_col,
        timestamp_col from country");

    DROP TABLE IF EXISTS {0}.country_keystore_postgres_query;
    CREATE EXTERNAL TABLE {0}.country_keystore_postgres_query (
      id INT,
      name STRING,
      bool_col BOOLEAN,
      tinyint_col     SMALLINT,
      smallint_col    SMALLINT,
      int_col         INT,
      bigint_col      BIGINT,
      float_col       FLOAT,
      double_col      DOUBLE,
      date_col        DATE,
      string_col      STRING,
      timestamp_col   TIMESTAMP)
    STORED BY JDBC
    TBLPROPERTIES (
      "database.type"="POSTGRES",
      "jdbc.url"="jdbc:postgresql://localhost:5432/functional",
      "jdbc.auth"="AuthMech=0",
      "jdbc.driver"="org.postgresql.Driver",
      "driver.url"="{1}",
      "dbcp.username"="hiveuser",
      "dbcp.password"="password",
      "query"="select id,name,bool_col,tinyint_col,smallint_col,
        int_col,bigint_col,float_col,double_col,date_col,string_col,
        timestamp_col from country");
    """.format(unique_database, driver_url)

    '''
    try:
      self.client.execute(sql)
    except Exception as e:
      print("\n[DEBUG] Failed to create JDBC table")
      print("[DEBUG] Exception type:", type(e))
      print("[DEBUG] Exception message:", str(e))
      print("[DEBUG] Traceback:\n" + "".join(traceback.format_tb(e.__traceback__)))
      pytest.xfail(reason="Can't create JDBC table.")
    '''
    # Split into statements and execute one-by-one.
    stmts = [s.strip() for s in sql.split(';')]
    for i, stmt in enumerate(stmts):
      if not stmt:
        continue
      # Optional: skip pure comment lines (if any)
      if stmt.startswith('--') or stmt.startswith('/*'):
        continue

      # Log the statement (truncate for readability)
      truncated = (stmt[:200] + '...') if len(stmt) > 200 else stmt
      print("\n[DEBUG] Executing statement #%d:\n%s\n" % (i + 1, truncated))

      try:
        # Use run_stmt_in_hive as before (this is what the test harness uses).
        self.client.execute(stmt + ';')
      except Exception as e:
        print("\n[DEBUG] Statement #%d failed." % (i + 1))
        print("[DEBUG] Exception type:", type(e))
        print("[DEBUG] Exception message:", str(e))
        raise

    self.client.execute("INVALIDATE METADATA {0}.country_postgres"
                        .format(unique_database))
    self.client.execute("INVALIDATE METADATA {0}.country_keystore_postgres"
                        .format(unique_database))
    self.client.execute("INVALIDATE METADATA {0}.country_postgres_query"
                        .format(unique_database))
    self.client.execute("INVALIDATE METADATA {0}"
                        ".country_keystore_postgres_query"
                        .format(unique_database))
    self.client.execute("DESCRIBE {0}.country_postgres_query"
                        .format(unique_database))
    self.client.execute("DESCRIBE {0}"
                        ".country_keystore_postgres_query"
                        .format(unique_database))
    self.run_test_case('QueryTest/hive-jdbc-postgres-tables',
                       vector, use_db=unique_database)

  def test_invalid_postgres_jdbc_table(self, unique_database):
    sql_both_set = """
    CREATE EXTERNAL TABLE {0}.invalid_both_props (
      id INT,
      name STRING
    )
    STORED BY JDBC
    TBLPROPERTIES (
      "database.type"="POSTGRES",
      "jdbc.url"="jdbc:postgresql://localhost:5432/functional",
      "jdbc.auth"="AuthMech=0",
      "jdbc.driver"="org.postgresql.Driver",
      "driver.url"="/test-warehouse/data-sources/jdbc-drivers/postgresql-jdbc.jar",
      "dbcp.username"="hiveuser",
      "dbcp.password"="password",
      "table"="country",
      "query"="SELECT * FROM country");
    """.format(unique_database)

    self.execute_query(sql_both_set)
    ex = self.execute_query_expect_failure(
      self.client,
      "select count(*) from {0}.invalid_both_props".format(unique_database))
    assert "Only one of 'table' or 'query' should be set" in str(ex)

    sql_none_set = """
    CREATE EXTERNAL TABLE {0}.invalid_no_props (
      id INT,
      name STRING
    )
    STORED BY JDBC
    TBLPROPERTIES (
      "database.type"="POSTGRES",
      "jdbc.url"="jdbc:postgresql://localhost:5432/functional",
      "jdbc.auth"="AuthMech=0",
      "jdbc.driver"="org.postgresql.Driver",
      "driver.url"="/test-warehouse/data-sources/jdbc-drivers/postgresql-jdbc.jar",
      "dbcp.username"="hiveuser",
      "dbcp.password"="password");
    """.format(unique_database)

    self.execute_query(sql_none_set)
    ex = self.execute_query_expect_failure(
      self.client,
      "select count(*) from {0}.invalid_no_props".format(unique_database))
    assert "either 'table' or 'query' property must be set" in str(ex)


class TestHivePostgresJdbcTables(CustomClusterTestSuite):
  """Tests for hive jdbc postgres tables. """

  @classmethod
  def setup_class(cls):
    super(TestHivePostgresJdbcTables, cls).setup_class()

  @pytest.mark.execute_serially
  def test_postgres_hive_jdbc_tables(self, vector, unique_database):
    """Run tests for external hive jdbc tables."""
    hive_sql = """
    DROP TABLE IF EXISTS {0}.country_postgres;
    CREATE EXTERNAL TABLE {0}.country_postgres
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        string_col      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password",
        "hive.sql.table" = "country"
    );

    DROP TABLE IF EXISTS {0}.quoted_col;
    CREATE EXTERNAL TABLE {0}.quoted_col
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        `freeze`      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password",
        "hive.sql.table" = "quoted_col"
    );

    DROP TABLE IF EXISTS {0}.country_keystore_postgres;
    CREATE EXTERNAL TABLE {0}.country_keystore_postgres
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        string_col      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password.keystore" =
        "jceks://hdfs/test-warehouse/data-sources/test.jceks",
        "hive.sql.dbcp.password.key" = "hiveuser",
        "hive.sql.table" = "country"
    );

    DROP TABLE IF EXISTS {0}.country_postgres_query;
    CREATE EXTERNAL TABLE {0}.country_postgres_query
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        string_col      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password",
        "hive.sql.query" = "select id,name,bool_col,tinyint_col,smallint_col,
        int_col,bigint_col,float_col,double_col,date_col,string_col,
        timestamp_col from country"
    );

    DROP TABLE IF EXISTS {0}.country_keystore_postgres_query;
    CREATE EXTERNAL TABLE {0}.country_keystore_postgres_query
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        string_col      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password.keystore" =
        "jceks://hdfs/test-warehouse/data-sources/test.jceks",
        "hive.sql.dbcp.password.key" = "hiveuser",
        "hive.sql.query" = "select id,name,bool_col,tinyint_col,smallint_col,
        int_col,bigint_col,float_col,double_col,date_col,string_col,
        timestamp_col from country"
    );
    """.format(unique_database)
    try:
      self.run_stmt_in_hive(hive_sql)
    except Exception:
      pytest.xfail(reason="Can't create hive jdbc table.")
    self.client.execute("INVALIDATE METADATA {0}.country_postgres".
                        format(unique_database))
    self.client.execute("INVALIDATE METADATA {0}.country_keystore_postgres".
                        format(unique_database))
    self.client.execute("INVALIDATE METADATA {0}.country_postgres_query".
                        format(unique_database))
    self.client.execute("INVALIDATE METADATA {0}.country_keystore_postgres_query".
                        format(unique_database))
    # Describing postgres hive jdbc table in Impala.
    self.client.execute("DESCRIBE {0}.country_postgres_query".format(unique_database))
    self.client.execute("DESCRIBE {0}.country_keystore_postgres_query"
        .format(unique_database))

    # Select statements are verified in hive-jdbc-postgres-tables.test.
    self.run_test_case('QueryTest/hive-jdbc-postgres-tables', vector,
                       use_db=unique_database)

  def test_invalid_postgres_hive_jdbc_table(self, unique_database):
    """Negative tests for hive jdbc tables with postgres"""

    # Both hive.sql.table and hive.sql.query are set
    sql_both_set = """
    CREATE EXTERNAL TABLE {0}.invalid_both_props (
        id INT,
        name STRING
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password",
        "hive.sql.table" = "country",
        "hive.sql.query" = "SELECT * FROM country"
    )
    """.format(unique_database)

    try:
      self.run_stmt_in_hive(sql_both_set)
      # This test expect querying from Impala will fail.
      # However, it is not possible to test this because the Hive CREATE TABLE
      # query will fail when both "hive.sql.table" and "hive.sql.query" are set.
      # ex = self.execute_query_expect_failure(
      #   "select count(*) from {0}.invalid_both_props".format(unique_database))
      # assert "Only one of 'hive.sql.table' or 'hive.sql.query' should be set" in str(ex)
    except Exception as ex:
      assert "Caught exception while initializing the SqlSerDe: null" in str(ex)

    # Neither hive.sql.table nor hive.sql.query is set
    sql_none_set = """
    CREATE EXTERNAL TABLE {0}.invalid_no_props (
        id INT,
        name STRING
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password"
    )
    """.format(unique_database)

    try:
      self.run_stmt_in_hive(sql_none_set)
      # This test expect querying from Impala will fail.
      # However, it is not possible to test this because the Hive CREATE TABLE
      # query will fail when none of "hive.sql.table" and "hive.sql.query" are set.
      # ex = self.execute_query_expect_failure(
      #   "select count(*) from {0}.invalid_no_props".format(unique_database))
      # assert "Either 'hive.sql.table' or 'hive.sql.query' must be set" in str(ex)
    except Exception as ex:
      assert ("Caught exception while trying to get columns: "
              "Both parameters are null") in str(ex)


class TestMySqlExtJdbcTables(CustomClusterTestSuite):
  """Impala query tests for external jdbc tables on MySQL server.
  It also includes tests for external hive jdbc tables on mysql."""

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
        pytest.skip("These tests required the docker to be added to sudoer's group")
      elif e.returncode == 20:
        pytest.skip("Can't connect to local MySQL server")
      elif e.returncode == 30:
        pytest.skip("File /var/run/mysqld/mysqld.sock not found")
      else:
        # The mysql docker container creation and mysqld can fail due to multiple
        # reasons. This could be an Intermittent issue and need to re-run the test.
        pytest.xfail(reason="Failed to setup MySQL testing environment")

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

  @pytest.mark.execute_serially
  def test_mysql_hive_jdbc_tables(self, vector, unique_database):
    """ Run tests for external hive jdbc tables on mysql"""
    hive_sql = """
    ADD JAR hdfs:///test-warehouse/data-sources/jdbc-drivers/mysql-jdbc.jar;

    DROP TABLE IF EXISTS {0}.country_mysql;
    CREATE EXTERNAL TABLE {0}.country_mysql
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        string_col      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "com.mysql.cj.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password",
        "hive.sql.table" = "country"
    );

    DROP TABLE IF EXISTS {0}.quoted_col;
    CREATE EXTERNAL TABLE {0}.quoted_col
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        `freeze`      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "com.mysql.cj.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password",
        "hive.sql.table" = "quoted_col"
    );

    DROP TABLE IF EXISTS {0}.country_keystore_mysql;
    CREATE EXTERNAL TABLE {0}.country_keystore_mysql
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        string_col      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "com.mysql.cj.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password.keystore" =
        "jceks://hdfs/test-warehouse/data-sources/test.jceks",
        "hive.sql.dbcp.password.key" = "hiveuser",
        "hive.sql.table" = "country"
    );

    DROP TABLE IF EXISTS {0}.country_mysql_query;
    CREATE EXTERNAL TABLE {0}.country_mysql_query
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        string_col      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "com.mysql.cj.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password",
        "hive.sql.query" = "select * from country"
    );

    DROP TABLE IF EXISTS {0}.country_keystore_mysql_query;
    CREATE EXTERNAL TABLE {0}.country_keystore_mysql_query
    (
        id INT,
        name STRING,
        bool_col BOOLEAN,
        tinyint_col     SMALLINT,
        smallint_col    SMALLINT,
        int_col         INT,
        bigint_col      BIGINT,
        float_col       FLOAT,
        double_col      DOUBLE,
        date_col        DATE,
        string_col      STRING,
        timestamp_col   TIMESTAMP
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "com.mysql.cj.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password.keystore" =
        "jceks://hdfs/test-warehouse/data-sources/test.jceks",
        "hive.sql.dbcp.password.key" = "hiveuser",
        "hive.sql.query" = "select * from country"
    );
    """.format(unique_database)
    try:
      self.run_stmt_in_hive(hive_sql)
    except Exception:
      pytest.xfail(reason="Can't create hive jdbc table.")
    self.client.execute("INVALIDATE METADATA {0}.country_mysql"
                        .format(unique_database))
    self.client.execute("INVALIDATE METADATA {0}.country_keystore_mysql"
                        .format(unique_database))
    self.client.execute("INVALIDATE METADATA {0}.country_mysql_query"
                        .format(unique_database))
    self.client.execute("INVALIDATE METADATA {0}.country_keystore_mysql_query"
                        .format(unique_database))
    # Describing mysql hive jdbc table in Impala.
    self.client.execute("DESCRIBE {0}.country_mysql".format(unique_database))
    self.client.execute("DESCRIBE {0}.country_keystore_mysql".format(unique_database))
    self.client.execute("DESCRIBE {0}.country_mysql_query".format(unique_database))
    self.client.execute("DESCRIBE {0}.country_keystore_mysql_query"
        .format(unique_database))

    # Select statements are verified in hive-jdbc-mysql-tables.test.
    self.run_test_case('QueryTest/hive-jdbc-mysql-tables', vector,
                       use_db=unique_database)

  @pytest.mark.execute_serially
  def test_invalid_mysql_hive_jdbc_table_properties(self, unique_database):
    """Negative tests for hive jdbc tables with hive"""
    add_jar_stmt =\
      "ADD JAR hdfs:///test-warehouse/data-sources/jdbc-drivers/mysql-jdbc.jar;"
    self.run_stmt_in_hive(add_jar_stmt)

    # Both hive.sql.table and hive.sql.query are set
    sql_both_set = """
    CREATE EXTERNAL TABLE {0}.invalid_both_props_mysql (
        id INT,
        name STRING
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "com.mysql.cj.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password",
        "hive.sql.table" = "country",
        "hive.sql.query" = "SELECT id, name FROM country"
    )
    """.format(unique_database)

    try:
      self.run_stmt_in_hive(sql_both_set)
      # This test expect querying from Impala will fail.
      # However, it is not possible to test this because the Hive CREATE TABLE
      # query will fail when both "hive.sql.table" and "hive.sql.query" are set.
      # ex = self.execute_query_expect_failure(
      #   "select count(*) from {0}.invalid_both_props_mysql".format(unique_database))
      # assert "Only one of 'hive.sql.table' or 'hive.sql.query' should be set" in str(ex)
    except Exception as ex:
      assert "Caught exception while initializing the SqlSerDe: null" in str(ex)

    # Neither hive.sql.table nor hive.sql.query is set
    sql_none_set = """
    CREATE EXTERNAL TABLE {0}.invalid_no_props_mysql (
        id INT,
        name STRING
    )
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "com.mysql.cj.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/functional",
        "hive.sql.dbcp.username" = "hiveuser",
        "hive.sql.dbcp.password" = "password"
    )
    """.format(unique_database)

    try:
      self.run_stmt_in_hive(sql_none_set)
      # This test expect querying from Impala will fail.
      # However, it is not possible to test this because the Hive CREATE TABLE
      # query will fail when none of "hive.sql.table" and "hive.sql.query" are set.
      # ex = self.execute_query_expect_failure(
      #   "select count(*) from {0}.invalid_no_props".format(unique_database))
      # assert "Either 'hive.sql.table' or 'hive.sql.query' must be set" in str(ex)
    except Exception as ex:
      assert ("Cannot load JDBC driver class 'com.mysql.cj.jdbc.Driver'") in str(ex)


class TestImpalaExtJdbcTables(CustomClusterTestSuite):
  """Impala query tests for external jdbc tables in Impala cluster."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestImpalaExtJdbcTables, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
      exec_single_node_option=[100]))

  @classmethod
  def _download_impala_jdbc_driver(cls):
    # Download Impala jdbc driver and copy jdbc driver to HDFS.
    script = os.path.join(
      os.environ['IMPALA_HOME'], 'testdata/bin/download-impala-jdbc-driver.sh')
    run_cmd = [script]
    try:
      subprocess.check_call(run_cmd, close_fds=True)
    except subprocess.CalledProcessError:
      assert False, "Failed to download Impala JDBC driver"

  @classmethod
  def setup_class(cls):
    cls._download_impala_jdbc_driver()
    super(TestImpalaExtJdbcTables, cls).setup_class()

  @classmethod
  def teardown_class(cls):
    super(TestImpalaExtJdbcTables, cls).teardown_class()

  @pytest.mark.execute_serially
  def test_impala_ext_jdbc_tables(self, vector, unique_database):
    """Run tests for external jdbc tables in Impala cluster"""
    self.run_test_case(
        'QueryTest/impala-ext-jdbc-tables', vector, use_db=unique_database)
    # Verify the settings of query options with Queries Web page on Impala coordinator
    response = requests.get("http://localhost:25000/queries?json")
    response_json = response.text
    assert "SET MAX_ERRORS=10000" in response_json, \
        "No matching option MAX_ERRORS found in the queries site."
    assert "SET MEM_LIMIT=1000000000" in response_json, \
        "No matching option MEM_LIMIT found in the queries site."
    assert "SET ENABLED_RUNTIME_FILTER_TYPES=\\\"BLOOM,MIN_MAX\\\"" in response_json or \
        "SET ENABLED_RUNTIME_FILTER_TYPES='BLOOM,MIN_MAX'" in response_json, \
        "No matching option ENABLED_RUNTIME_FILTER_TYPES found in the queries site."
    assert "SET QUERY_TIMEOUT_S=600" in response_json, \
        "No matching option QUERY_TIMEOUT_S found in the queries site."
    assert "SET REQUEST_POOL=\\\"default-pool\\\"" in response_json, \
        "No matching option REQUEST_POOL found in the queries site."
    assert "SET DEBUG_ACTION" not in response_json, \
        "Matching option DEBUG_ACTION found in the queries site."

  @pytest.mark.execute_serially
  def test_impala_ext_jdbc_tables_predicates(self, vector, unique_database):
    """Run tests for external jdbc tables in Impala cluster for new predicates"""
    self.run_test_case(
        'QueryTest/impala-ext-jdbc-tables-predicates', vector, use_db=unique_database)


# Queries used by MT_DOP tests and benchmarks.
# Single-table grouped aggregate with a date filter on tpch_jdbc.lineitem.
# Tests parallel scan throughput and per-instance row distribution.
_JDBC_LINEITEM_AGG_QUERY = """
SELECT
    l.l_returnflag,
    l.l_linestatus,
    COUNT(*) AS order_count,
    SUM(l.l_quantity) AS total_quantity,
    AVG(l.l_quantity) AS avg_quantity,
    MIN(l.l_discount) AS min_discount,
    MAX(l.l_discount) AS max_discount,
    MIN(l.l_extendedprice) AS min_price,
    MAX(l.l_extendedprice) AS max_price
FROM tpch_jdbc.lineitem l
WHERE l.l_shipdate >= '1995-01-01'
GROUP BY
    l.l_returnflag,
    l.l_linestatus
ORDER BY total_quantity DESC
""".strip()

# Tests that multi-scan parallelism produces consistent results.
_JDBC_JOIN_QUERY = """
SELECT
    c.c_nationkey,
    MIN(o.o_totalprice) AS min_order_price,
    MAX(o.o_totalprice) AS max_order_price,
    MIN(l.l_discount) AS min_discount,
    MAX(l.l_discount) AS max_discount,
    MIN(ps.ps_supplycost) AS min_supply_cost,
    MAX(ps.ps_supplycost) AS max_supply_cost,
    COUNT(*) AS total_rows
FROM tpch_jdbc.customer c
JOIN tpch_jdbc.orders o
    ON c.c_custkey = o.o_custkey
JOIN tpch_jdbc.lineitem l
    ON o.o_orderkey = l.l_orderkey
JOIN tpch_jdbc.partsupp ps
    ON l.l_partkey = ps.ps_partkey
   AND l.l_suppkey = ps.ps_suppkey
WHERE o.o_orderdate >= '1994-01-01'
GROUP BY c.c_nationkey
ORDER BY total_rows DESC
""".strip()


class TestJdbcMtDop(CustomClusterTestSuite):
  """Correctness and profile tests for MT_DOP parallelism on JDBC external data sources.

  Validates:
    - Correctness: queries with MT_DOP=0/1/4 produce the same results.
    - Profile: with MT_DOP=N>1, the expected number of fragment instances are active
      and each returns at least one row (all instances participate in the scan).
    - Counter: NumExternalDataSourceGetNext grows with MT_DOP, confirming that
      multiple scanner threads are fetching concurrently.
  """

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('These tests only run in exhaustive')
    super(TestJdbcMtDop, cls).setup_class()

  def _run(self, query, mt_dop):
    self.client.set_configuration_option("mt_dop", mt_dop)
    return self.client.execute(query)

  def _run_and_get_profile(self, query, mt_dop):
    return self._run(query, mt_dop).runtime_profile

  def _fragment_instance_rows(self, profile):
    """Return the list of RowsProduced values from all fragment instances."""
    return [int(m) for m in re.findall(r'RowsProduced: (\d+)', profile)]

  def _get_next_total(self, profile):
    """Sum of NumExternalDataSourceGetNext across all instances."""
    return sum(int(m) for m in
               re.findall(r'NumExternalDataSourceGetNext: (\d+)', profile))

  # Correctness
  @pytest.mark.execute_serially
  def test_lineitem_agg_correctness(self):
    """Grouped lineitem aggregate must return identical results for all MT_DOP values."""
    baseline = self._run(_JDBC_LINEITEM_AGG_QUERY, mt_dop=0).data
    assert baseline == self._run(_JDBC_LINEITEM_AGG_QUERY, mt_dop=1).data, \
        "Lineitem agg mismatch between mt_dop=0 and mt_dop=1"
    assert baseline == self._run(_JDBC_LINEITEM_AGG_QUERY, mt_dop=4).data, \
        "Lineitem agg mismatch between mt_dop=0 and mt_dop=4"

  @pytest.mark.execute_serially
  def test_join_correctness(self):
    """Four-table join must return identical results for all MT_DOP values."""
    baseline = self._run(_JDBC_JOIN_QUERY, mt_dop=0).data
    assert baseline == self._run(_JDBC_JOIN_QUERY, mt_dop=1).data, \
        "Join query mismatch between mt_dop=0 and mt_dop=1"
    assert baseline == self._run(_JDBC_JOIN_QUERY, mt_dop=4).data, \
        "Join query mismatch between mt_dop=0 and mt_dop=4"

  # Profile / counters

  @pytest.mark.execute_serially
  @pytest.mark.parametrize("mt_dop", [0, 1, 4])
  def test_instance_count(self, mt_dop):
    """With MT_DOP=N the JDBC scan fragment should run with max(1,N) instances."""
    profile = self._run_and_get_profile(_JDBC_LINEITEM_AGG_QUERY, mt_dop)
    expected = max(1, mt_dop)

    # New profile format
    matches = re.findall(r'NumFragmentInstances:\s+(\d+)', profile)

    assert matches, (
        "Could not find NumFragmentInstances in profile.\nProfile:\n%s"
        % profile
    )

    actual = max(int(x) for x in matches)

    assert actual >= expected, (
        "Expected >= %d fragment instances for mt_dop=%d, got %d\nProfile:\n%s"
        % (expected, mt_dop, actual, profile))

  @pytest.mark.execute_serially
  @pytest.mark.parametrize("mt_dop", [1, 4])
  def test_all_instances_returned_rows(self, mt_dop):
    """Every scanner instance should have returned at least one row."""
    profile = self._run_and_get_profile(_JDBC_LINEITEM_AGG_QUERY, mt_dop)
    rows_per_instance = self._fragment_instance_rows(profile)
    assert rows_per_instance, "No RowsProduced counters found in profile"
    active = [r for r in rows_per_instance if r > 0]
    assert len(active) >= mt_dop, (
        "Expected at least %d active instances (RowsProduced > 0), got %d.\n"
        "RowsProduced per instance: %s\nProfile:\n%s"
        % (mt_dop, len(active), rows_per_instance, profile))

  @pytest.mark.execute_serially
  @pytest.mark.parametrize("mt_dop", [1, 4])
  def test_get_next_counter_scales_with_mt_dop(self, mt_dop):
    """Total GetNext calls should not decrease as MT_DOP grows."""
    calls_1 = self._get_next_total(
        self._run_and_get_profile(_JDBC_LINEITEM_AGG_QUERY, mt_dop=1))
    calls_n = self._get_next_total(
        self._run_and_get_profile(_JDBC_LINEITEM_AGG_QUERY, mt_dop=mt_dop))
    assert calls_1 > 0, "Expected non-zero GetNext calls with mt_dop=1"
    if mt_dop > 1:
      assert calls_n >= calls_1, (
          "Expected total GetNext calls to be >= mt_dop=1 (%d) with mt_dop=%d, "
          "got %d" % (calls_1, mt_dop, calls_n))

  @pytest.mark.execute_serially
  def test_shared_scan_handle(self):
    """All instances in the same fragment should share the same scan handle."""
    profile = self._run_and_get_profile(_JDBC_LINEITEM_AGG_QUERY, mt_dop=4)
    handles = re.findall(r'ScanHandle:\s+([0-9a-f-]{36})', profile)
    if handles:
      assert len(set(handles)) == 1, (
          "Expected a single shared scan handle across instances, "
          "got multiple: %s" % set(handles))


class TestJdbcMtDopBenchmark(CustomClusterTestSuite):
  """Benchmark tests for JDBC MT_DOP parallelism.

  Uses two representative TPC-H workloads:
    - _JDBC_LINEITEM_AGG_QUERY: single-table grouped aggregate, exercises the
      parallel scan path directly (most sensitive to MT_DOP).
    - _JDBC_JOIN_QUERY: four-table join, exercises coordination overhead across
      multiple concurrent JDBC scans.

  Each test records performance metrics from the Impala runtime profile and
  asserts that parallelism does not produce excessive overhead.
  """

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('These tests only run in exhaustive')
    super(TestJdbcMtDopBenchmark, cls).setup_class()

  def _run(self, query, mt_dop):
    self.client.set_configuration_option("mt_dop", mt_dop)
    return self.client.execute(query)

  def _get_next_total(self, profile):
    return sum(int(x) for x in
               re.findall(r'NumExternalDataSourceGetNext: (\d+)', profile))

  def _rows_per_instance(self, profile):
    return [int(x) for x in re.findall(r'RowsProduced: (\d+)', profile)]

  # Latency benchmarks
  @pytest.mark.execute_serially
  def test_benchmark_lineitem_agg_latency(self):
    """MT_DOP=4 wall-clock time for the lineitem aggregate must not exceed 2x MT_DOP=1.

    Three warm-up runs are executed first to amortise JVM / connection-pool
    startup costs before the timed measurement.
    """
    for _ in range(3):
      self._run(_JDBC_LINEITEM_AGG_QUERY, mt_dop=1)
    t0 = time.time()
    self._run(_JDBC_LINEITEM_AGG_QUERY, mt_dop=1)
    latency_1 = time.time() - t0

    for _ in range(3):
      self._run(_JDBC_LINEITEM_AGG_QUERY, mt_dop=4)
    t0 = time.time()
    self._run(_JDBC_LINEITEM_AGG_QUERY, mt_dop=4)
    latency_4 = time.time() - t0

    speedup = latency_1 / latency_4 if latency_4 > 0 else float('inf')
    print("\n[BENCHMARK] lineitem_agg latency: mt_dop=1 %.3fs  mt_dop=4 %.3fs  "
          "speedup=%.2fx" % (latency_1, latency_4, speedup))
    assert latency_4 <= latency_1 * 2, (
        "mt_dop=4 (%.3fs) is more than 2x slower than mt_dop=1 (%.3fs); "
        "parallelism overhead looks excessive" % (latency_4, latency_1))

  @pytest.mark.execute_serially
  def test_benchmark_join_latency(self):
    """MT_DOP=4 wall-clock time for the four-table join must not exceed 2x MT_DOP=1."""
    for _ in range(3):
      self._run(_JDBC_JOIN_QUERY, mt_dop=1)
    t0 = time.time()
    self._run(_JDBC_JOIN_QUERY, mt_dop=1)
    latency_1 = time.time() - t0

    for _ in range(3):
      self._run(_JDBC_JOIN_QUERY, mt_dop=4)
    t0 = time.time()
    self._run(_JDBC_JOIN_QUERY, mt_dop=4)
    latency_4 = time.time() - t0

    speedup = latency_1 / latency_4 if latency_4 > 0 else float('inf')
    print("\n[BENCHMARK] join latency:         mt_dop=1 %.3fs  mt_dop=4 %.3fs  "
          "speedup=%.2fx" % (latency_1, latency_4, speedup))
    assert latency_4 <= latency_1 * 2, (
        "join mt_dop=4 (%.3fs) is more than 2x slower than mt_dop=1 (%.3fs)"
        % (latency_4, latency_1))

  # GetNext throughput benchmarks
  @pytest.mark.execute_serially
  @pytest.mark.parametrize("mt_dop", [1, 2, 4])
  def test_benchmark_lineitem_agg_get_next_throughput(self, mt_dop):
    """Record GetNext call count and rate for the lineitem aggregate at each MT_DOP.

    The aggregate call count must be non-zero; the rate is logged for trend
    analysis but not bounded (connection latency varies per environment).
    """
    t0 = time.time()
    result = self._run(_JDBC_LINEITEM_AGG_QUERY, mt_dop=mt_dop)
    elapsed = time.time() - t0
    total_calls = self._get_next_total(result.runtime_profile)
    rate = total_calls / elapsed if elapsed > 0 else 0
    print("\n[BENCHMARK] lineitem_agg GetNext: mt_dop=%d  calls=%d  "
          "elapsed=%.3fs  rate=%.1f calls/s" % (mt_dop, total_calls, elapsed, rate))
    assert total_calls > 0, \
        "Expected non-zero GetNext calls for lineitem_agg mt_dop=%d" % mt_dop

  @pytest.mark.execute_serially
  @pytest.mark.parametrize("mt_dop", [1, 2, 4])
  def test_benchmark_join_get_next_throughput(self, mt_dop):
    """Record GetNext call count and rate for the four-table join at each MT_DOP."""
    t0 = time.time()
    result = self._run(_JDBC_JOIN_QUERY, mt_dop=mt_dop)
    elapsed = time.time() - t0
    total_calls = self._get_next_total(result.runtime_profile)
    rate = total_calls / elapsed if elapsed > 0 else 0
    print("\n[BENCHMARK] join GetNext:         mt_dop=%d  calls=%d  "
          "elapsed=%.3fs  rate=%.1f calls/s" % (mt_dop, total_calls, elapsed, rate))
    assert total_calls > 0, \
        "Expected non-zero GetNext calls for join query mt_dop=%d" % mt_dop

  @pytest.mark.execute_serially
  @pytest.mark.parametrize("mt_dop", [2, 4])
  def test_benchmark_lineitem_agg_instance_participation(self, mt_dop):
    """Verify that multiple fragment instances participate in execution."""
    profile = self._run(
        _JDBC_LINEITEM_AGG_QUERY, mt_dop=mt_dop).runtime_profile

    rows = self._rows_per_instance(profile)

    assert rows, "No RowsProduced counters found in profile"

    active = [r for r in rows if r > 0]

    print(
        "\n[BENCHMARK] lineitem_agg row dist: "
        "mt_dop=%d rows=%s active=%d"
        % (mt_dop, rows, len(active))
    )

    assert len(active) >= mt_dop, (
        "Expected at least %d active instances, got %d.\nRows: %s"
        % (mt_dop, len(active), rows))

  @pytest.mark.execute_serially
  @pytest.mark.parametrize("mt_dop", [2, 4])
  def test_benchmark_join_instance_participation(self, mt_dop):
    """Verify that multiple fragment instances participate in execution."""
    profile = self._run(
        _JDBC_JOIN_QUERY, mt_dop=mt_dop).runtime_profile

    rows = self._rows_per_instance(profile)

    assert rows, "No RowsProduced counters found in profile"

    active = [r for r in rows if r > 0]

    print(
        "\n[BENCHMARK] join row dist: "
        "mt_dop=%d rows=%s active=%d"
        % (mt_dop, rows, len(active))
    )

    assert len(active) >= mt_dop, (
        "Expected at least %d active instances, got %d.\nRows: %s"
        % (mt_dop, len(active), rows))
