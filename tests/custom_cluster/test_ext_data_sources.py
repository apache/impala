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
