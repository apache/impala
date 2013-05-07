// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetSchemasReq;
import org.apache.hive.service.cli.thrift.TGetTablesReq;
import org.junit.Test;

import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.authorization.ImpalaInternalAdminUser;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.service.Frontend;
import com.cloudera.impala.thrift.TMetadataOpRequest;
import com.cloudera.impala.thrift.TMetadataOpResponse;
import com.cloudera.impala.thrift.TMetadataOpcode;
import com.cloudera.impala.thrift.TSessionState;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class AuthorizationTest {
  // Policy file has defined current user to have:
  //   ALL permission on 'tpch' database and 'newdb' database
  //   ALL permission on 'functional_seq_snap' database
  //   SELECT permissions on all tables in 'tpcds' database
  //   SELECT permissions on 'functional.alltypesagg' (no INSERT permissions)
  //   INSERT permissions on 'functional.alltypes' (no SELECT permissions)
  //   INSERT permissions on all tables in 'functional_parquet' database
  private final static String AUTHZ_POLICY_FILE = "/test-warehouse/authz-policy.ini";
  private final static User USER = new User(System.getProperty("user.name"));
  private final AnalysisContext analysisContext;
  private final AnalysisContext adminUserAnalysisContext;
  private final Frontend fe;

  public AuthorizationTest() throws IOException {
    AuthorizationConfig authzConfig =
        new AuthorizationConfig("server1", AUTHZ_POLICY_FILE);
    Catalog catalog = new Catalog(true, false, authzConfig);
    analysisContext = new AnalysisContext(catalog, Catalog.DEFAULT_DB, USER);
    adminUserAnalysisContext = new AnalysisContext(catalog,
        Catalog.DEFAULT_DB, ImpalaInternalAdminUser.getInstance());
    fe = new Frontend(true, authzConfig);
  }

  @Test
  public void TestSelect() throws AuthorizationException, AnalysisException {
    // Can select from table that user has privileges on.
    AuthzOk("select * from functional.alltypesagg");

    // Constant select.
    AuthzOk("select 1");

    // Unqualified table name.
    AuthzError("select * from alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: default.alltypes");

    // Select with no privileges.
    AuthzError("select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // Select without referencing a column.
    AuthzError("select 1 from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // Select from non-existent table.
    AuthzError("select 1 from functional.notbl",
        "User '%s' does not have privileges to execute 'SELECT' on: functional.notbl");

    // Select from non-existent db.
    AuthzError("select 1 from nodb.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: nodb.alltypes");

    // Table within inline view is authorized properly.
    AuthzError("select a.* from (select * from functional.alltypes) a",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");
    // Table within inline view is authorized properly (user has permission).
    AuthzOk("select a.* from (select * from functional.alltypesagg) a");
  }

  @Test
  public void TestUnion() throws AuthorizationException, AnalysisException {
    AuthzOk("select * from functional.alltypesagg union all " +
        "select * from functional.alltypesagg");

    AuthzError("select * from functional.alltypesagg union all " +
        "select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
         "functional.alltypes");
  }

  @Test
  public void TestInsert() throws AuthorizationException, AnalysisException {
    AuthzOk("insert into functional_parquet.alltypes " +
        "partition(month,year) select * from functional_seq_snap.alltypes");

    // Insert + inline view (user has permissions).
    AuthzOk("insert into functional.alltypes partition(month,year) " +
        "select b.* from functional.alltypesagg a join (select * from " +
        "functional_seq_snap.alltypes) b on (a.int_col = b.int_col)");

    // User doesn't have INSERT permissions in the target table.
    AuthzError("insert into functional.alltypesagg select 1",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.alltypesagg");

    // User doesn't have SELECT permissions on source table.
    AuthzError("insert into functional.alltypes " +
        "select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // User doesn't have permissions on source table within inline view.
    AuthzError("insert into functional.alltypes " +
        "select * from functional.alltypesagg a join (select * from " +
        "functional_seq.alltypes) b on (a.int_col = b.int_col)",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional_seq.alltypes");
  }

  @Test
  public void TestWithClause() throws AuthorizationException, AnalysisException {
    // User has SELECT privileges on table in WITH-clause view.
    AuthzOk("with t as (select * from functional.alltypesagg) select * from t");
    // User doesn't have SELECT privileges on table in WITH-clause view.
    AuthzError("with t as (select * from functional.alltypes) select * from t",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // User has SELECT privileges on table in WITH-clause view in INSERT.
    AuthzOk("with t as (select * from functional_seq_snap.alltypes) " +
        "insert into functional_parquet.alltypes partition(month,year) select * from t");
    // User doesn't have SELECT privileges on table in WITH-clause view in INSERT.
    AuthzError("with t as (select * from functional_parquet.alltypes) " +
        "insert into functional_parquet.alltypes partition(month,year) select * from t",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
         "functional_parquet.alltypes");
  }

  @Test
  public void TestExplain() throws AuthorizationException, AnalysisException {
    AuthzOk("explain select * from functional.alltypesagg");
    AuthzOk("explain insert into functional_parquet.alltypes " +
        "partition(month,year) select * from functional_seq_snap.alltypes");

    // Select without permissions.
    AuthzError("explain select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // Insert with no select permissions on source table.
    AuthzError("explain insert into functional_parquet.alltypes " +
        "partition(month,year) select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // Insert, user doesn't have permissions on source table.
    AuthzError("explain insert into functional.alltypes " +
        "select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");
  }

  @Test
  public void TestUseDb() throws AnalysisException, AuthorizationException {
    // Positive cases (user has privileges on these tables).
    AuthzOk("use functional");
    AuthzOk("use tpcds");
    AuthzOk("use tpch");

    AuthzError("use functional_seq",
        "User '%s' does not have privileges to access: functional_seq.*");
    AuthzError("use default",
        "User '%s' does not have privileges to access: default.*");
    // Database does not exist, user does not have access.
    AuthzError("use nodb",
        "User '%s' does not have privileges to access: nodb.*");

    // Database does not exist, user has access:
    try {
      AuthzOk("use newdb");
      fail("Expected AnalysisException");
    } catch (AnalysisException e) {
      Assert.assertEquals(e.getMessage(), "Database does not exist: newdb");
    }
  }

  @Test
  public void TestResetMetadata() throws AnalysisException, AuthorizationException {
    // Positive cases (user has privileges on these tables).
    AuthzOk("invalidate metadata functional.alltypesagg");
    AuthzOk("refresh functional.alltypesagg");

    // TODO: Use a real user to run this positive test case once
    // AuthorizationChecker supports LocalGroupAuthorizationProvider.
    adminUserAnalysisContext.analyze("invalidate metadata");

    AuthzError("invalidate metadata",
        "User '%s' does not have privileges to access: server");
    AuthzError("invalidate metadata unknown_db.alltypessmall",
        "User '%s' does not have privileges to access: unknown_db.alltypessmall");
    AuthzError("invalidate metadata functional_seq.alltypessmall",
        "User '%s' does not have privileges to access: functional_seq.alltypessmall");
    AuthzError("invalidate metadata functional.unknown_table",
        "User '%s' does not have privileges to access: functional.unknown_table");
    AuthzError("invalidate metadata functional.alltypessmall",
        "User '%s' does not have privileges to access: functional.alltypessmall");
    AuthzError("refresh functional.alltypessmall",
        "User '%s' does not have privileges to access: functional.alltypessmall");
  }

  @Test
  public void TestCreateTable() throws AnalysisException, AuthorizationException {
    AuthzOk("create table tpch.new_table (i int)");
    AuthzOk("create table tpch.new_lineitem like tpch.lineitem");
    // Create table IF NOT EXISTS, user has permission and table exists.
    AuthzOk("create table if not exists tpch.lineitem (i int)");
    try {
      AuthzOk("create table tpch.lineitem (i int)");
      fail("Expected analysis error.");
    } catch (AnalysisException e) {
      Assert.assertEquals(e.getMessage(), "Table already exists: tpch.lineitem");
    }
    // Create table IF NOT EXISTS, user does not have permission and table exists.
    AuthzError("create table if not exists functional_seq.alltypes (i int)",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "functional_seq.alltypes");

    // User has permission to create at given location.
    AuthzOk("create table tpch.new_table (i int) location " +
        "'hdfs://localhost:20500/test-warehouse/new_table'");

    // No permissions on source table.
    AuthzError("create table tpch.new_lineitem like tpch_seq.lineitem",
        "User '%s' does not have privileges to access: tpch_seq.lineitem");

    // No permissions on target table.
    AuthzError("create table tpch_rc.new like tpch.lineitem",
        "User '%s' does not have privileges to execute 'CREATE' on: tpch_rc.new");

    // Unqualified table name.
    AuthzError("create table new_table (i int)",
        "User '%s' does not have privileges to execute 'CREATE' on: default.new_table");

    // Table already exists (user does not have permission).
    AuthzError("create table functional.alltypes (i int)",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "functional.alltypes");

    // Database does not exist, user does not have access.
    AuthzError("create table nodb.alltypes (i int)",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "nodb.alltypes");

    // User does not have permission to create table at the specified location..
    AuthzError("create table tpch.new_table (i int) location " +
        "'hdfs://localhost:20500/test-warehouse/alltypes'",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/alltypes");
  }

  @Test
  public void TestCreateDatabase() throws AnalysisException, AuthorizationException {
    // User has permissions to create database.
    AuthzOk("create database newdb");

    // Create database with location specified explicitly (user has permission).
    AuthzOk("create database newdb location " +
        "'hdfs://localhost:20500/test-warehouse/new_table'");

    // Database already exists (no permissions).
    AuthzError("create database functional",
        "User '%s' does not have privileges to execute 'CREATE' on: functional");

    // No existent db (no permissions).
    AuthzError("create database nodb",
        "User '%s' does not have privileges to execute 'CREATE' on: nodb");
  }

  @Test
  public void TestDropDatabase() throws AnalysisException, AuthorizationException {
    // User has permissions.
    AuthzOk("drop database tpch");
    // User has permissions, database does not exists and IF EXISTS specified
    AuthzOk("drop database if exists newdb");
    // User has permission, database does not exists, IF EXISTS not specified.
    try {
      AuthzOk("drop database newdb");
      fail("Expected analysis error");
    } catch (AnalysisException e) {
      Assert.assertEquals(e.getMessage(), "Database does not exist: newdb");
    }

    // Database exists, user doesn't have permission to drop.
    AuthzError("drop database functional",
        "User '%s' does not have privileges to execute 'DROP' on: functional");
    AuthzError("drop database if exists functional",
        "User '%s' does not have privileges to execute 'DROP' on: functional");

    // Database does not exist, user doesn't have permission to drop.
    AuthzError("drop database nodb",
        "User '%s' does not have privileges to execute 'DROP' on: nodb");
    AuthzError("drop database if exists nodb",
        "User '%s' does not have privileges to execute 'DROP' on: nodb");
  }

  @Test
  public void TestDropTable() throws AnalysisException, AuthorizationException {
    // Drop table (user has permission).
    AuthzOk("drop table tpch.lineitem");

    // Drop table (user does not have permission).
    AuthzError("drop table functional.alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: functional.alltypes");

    // Drop table with unqualified table name.
    AuthzError("drop table alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: default.alltypes");

    // Drop table with non-existent database.
    AuthzError("drop table nodb.alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: nodb.alltypes");

    // Drop table with non-existent table.
    AuthzError("drop table functional.notbl",
        "User '%s' does not have privileges to execute 'DROP' on: functional.notbl");
  }

  @Test
  public void AlterTable() throws AnalysisException, AuthorizationException {
    // User has permissions to modify tables.
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes ADD COLUMNS (c1 int)");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes REPLACE COLUMNS (c1 int)");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes CHANGE int_col c1 int");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes DROP int_col");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes RENAME TO functional_seq_snap.t1");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes SET FILEFORMAT PARQUETFILE");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'hdfs://localhost:20500/test-warehouse/new_table'");

    // Alter table and set location to a path the user does not have access to.
    AuthzError("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'hdfs://localhost:20500/test-warehouse/no_access'",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/no_access");

    AuthzError("ALTER TABLE functional.alltypes SET FILEFORMAT PARQUETFILE",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("ALTER TABLE functional.alltypes ADD COLUMNS (c1 int)",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("ALTER TABLE functional.alltypes REPLACE COLUMNS (c1 int)",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("ALTER TABLE functional.alltypes CHANGE int_col c1 int",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("ALTER TABLE functional.alltypes DROP int_col",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("ALTER TABLE functional.alltypes rename to functional_seq_snap.t1",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");

    // No privileges on target.
    AuthzError("ALTER TABLE functional_seq_snap.alltypes rename to functional.alltypes",
        "User '%s' does not have privileges to execute 'CREATE' on: functional.alltypes");

    // Rename table that does not exist (no permissions).
    AuthzError("ALTER TABLE functional.notbl rename to functional_seq_snap.newtbl",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.notbl");

    // Rename table in db that does not exist (no permissions).
    AuthzError("ALTER TABLE nodb.alltypes rename to functional_seq_snap.newtbl",
        "User '%s' does not have privileges to execute 'ALTER' on: nodb.alltypes");

    // Alter table that does not exist (no permissions).
    AuthzError("ALTER TABLE functional.notbl ADD COLUMNS (c1 int)",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.notbl");

    // Alter table in db that does not exist (no permissions).
    AuthzError("ALTER TABLE nodb.alltypes ADD COLUMNS (c1 int)",
        "User '%s' does not have privileges to execute 'ALTER' on: nodb.alltypes");

    // Unqualified table name.
    AuthzError("ALTER TABLE alltypes ADD COLUMNS (c1 int)",
        "User '%s' does not have privileges to execute 'ALTER' on: default.alltypes");
  }

  @Test
  public void TestDescribe() throws AuthorizationException, AnalysisException {
    AuthzOk("describe functional.alltypesagg");
    AuthzOk("describe functional.alltypes");

    // Unqualified table name.
    AuthzError("describe alltypes",
        "User '%s' does not have privileges to access: default.alltypes");
    // Database doesn't exist.
    AuthzError("describe nodb.alltypes",
        "User '%s' does not have privileges to access: nodb.alltypes");
  }

  @Test
  public void TestLoad() throws AuthorizationException, AnalysisException {
    // User has permission on table and URI.
    AuthzOk("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem'" +
    		" into table functional.alltypes partition(month=10, year=2009)");

    // User does not have permission on table.
    AuthzError("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem'" +
        " into table functional.alltypesagg",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.alltypes");

    // User does not have permission on URI.
    AuthzError("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.part'" +
        " into table functional.alltypes partition(month=10, year=2009)",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/tpch.part");

    // URI does not exist and user does not have permission.
    AuthzError("load data inpath 'hdfs://localhost:20500/test-warehouse/nope'" +
        " into table functional.alltypes partition(month=10, year=2009)",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/nope");

    // Table/Db does not exist, user does not have permission.
    AuthzError("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem'" +
        " into table functional.notable",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.notable");
    AuthzError("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem'" +
        " into table nodb.alltypes",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "nodb.alltypes");
  }

  @Test
  public void TestShowPermissions() throws AuthorizationException, AnalysisException {
    AuthzOk("show tables in functional");
    AuthzOk("show databases");

    // Database exists, user does not have access.
    AuthzError("show tables in functional_rc",
        "User '%s' does not have privileges to access: functional_rc.*");

    // Database does not exist, user does not have access.
    AuthzError("show tables in nodb",
        "User '%s' does not have privileges to access: nodb.*");

    AuthzError("show tables",
        "User '%s' does not have privileges to access: default.*");

    // Database does not exist, user has access.
    try {
      AuthzOk("show tables in newdb");
      fail("Expected AnalysisException");
    } catch (AnalysisException e) {
      Assert.assertEquals(e.getMessage(), "Database does not exist: newdb");
    }
  }

  @Test
  public void TestShowDbResultsFiltered() throws ImpalaException {
    // These are the only dbs that should show up because they are the only
    // dbs the user has any permissions on.
    List<String> expectedDbs = Lists.newArrayList("functional", "functional_parquet",
        "functional_seq_snap", "tpcds", "tpch");

    List<String> dbs = fe.getDbNames("*", USER);
    Assert.assertEquals(expectedDbs, dbs);

    dbs = fe.getDbNames(null, USER);
    Assert.assertEquals(expectedDbs, dbs);
  }

  @Test
  public void TestShowTableResultsFiltered() throws ImpalaException {
    // The user only has permission on these tables in the functional databases.
    List<String> expectedTbls = Lists.newArrayList("alltypes", "alltypesagg");

    List<String> tables = fe.getTableNames("functional", "*", USER);
    Assert.assertEquals(expectedTbls, tables);

    tables = fe.getTableNames("functional", null, USER);
    Assert.assertEquals(expectedTbls, tables);
  }

  @Test
  public void TestHs2GetTables() throws ImpalaException {
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.setSession(createSessionState("default", USER));
    req.opcode = TMetadataOpcode.GET_TABLES;
    req.get_tables_req = new TGetTablesReq();
    req.get_tables_req.setSchemaName("functional");
    // Get all tables
    req.get_tables_req.setTableName("%");
    TMetadataOpResponse resp = fe.execHiveServer2MetadataOp(req);
    assertEquals(2, resp.results.size());
    assertEquals("alltypes", resp.results.get(0).colVals.get(2).stringVal.toLowerCase());
    assertEquals(
        "alltypesagg", resp.results.get(1).colVals.get(2).stringVal.toLowerCase());
  }

  @Test
  public void TestHs2GetSchema() throws ImpalaException {
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.setSession(createSessionState("default", USER));
    req.opcode = TMetadataOpcode.GET_SCHEMAS;
    req.get_schemas_req = new TGetSchemasReq();
    // Get all schema (databases).
    req.get_schemas_req.setSchemaName("%");
    TMetadataOpResponse resp = fe.execHiveServer2MetadataOp(req);
    List<String> expectedDbs = Lists.newArrayList("functional",
        "functional_parquet", "functional_seq_snap", "tpcds", "tpch");
    assertEquals(expectedDbs.size(), resp.results.size());
    for (int i = 0; i < resp.results.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.results.get(i).colVals.get(0).stringVal.toLowerCase());
    }
  }

  @Test
  public void TestHs2GetColumns() throws ImpalaException {
    // It should return one column: alltypes.string_col.
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_COLUMNS;
    req.setSession(createSessionState("default", USER));
    req.get_columns_req = new TGetColumnsReq();
    req.get_columns_req.setSchemaName("functional");
    req.get_columns_req.setTableName("alltypes");
    req.get_columns_req.setColumnName("stri%");
    TMetadataOpResponse resp = fe.execHiveServer2MetadataOp(req);
    assertEquals(1, resp.results.size());

    // User does not have permission to access the table, no results should be returned.
    req.get_columns_req.setTableName("alltypesnopart");
    resp = fe.execHiveServer2MetadataOp(req);
    assertEquals(0, resp.results.size());

    // User does not have permission to access db or table, no results should be
    // returned.
    req.get_columns_req.setSchemaName("functional_seq_gzip");
    req.get_columns_req.setTableName("alltypes");
    resp = fe.execHiveServer2MetadataOp(req);
    assertEquals(0, resp.results.size());
  }

  @Test
  public void TestServerNameAuthorized() throws AnalysisException {
    // Authorization config that has a different server name from policy file.
    TestWithIncorrectConfig(
        new AuthorizationConfig("differentServerName", AUTHZ_POLICY_FILE));
 }

  @Test
  public void TestNoPermissionsWhenPolicyFileDoesNotExist() throws AnalysisException {
    // Validate a non-existent policy file.
    TestWithIncorrectConfig(
        new AuthorizationConfig("server1", AUTHZ_POLICY_FILE + "_does_not_exist"));
  }

  @Test
  public void TestConfigValidation() throws InternalException {
    // Empty / null server name.
    AuthorizationConfig config = new AuthorizationConfig("", AUTHZ_POLICY_FILE);
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(),
          "Authorization is enabled but the server name is null or empty. Set the " +
          "server name using the impalad --server_name flag.");
    }
    config = new AuthorizationConfig(null, AUTHZ_POLICY_FILE);
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(),
          "Authorization is enabled but the server name is null or empty. Set the " +
          "server name using the impalad --server_name flag.");
    }

    // Empty/null policy file.
    config = new AuthorizationConfig("server1", null);
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(),
          "Authorization is enabled but the policy file path was null or empty. " +
          "Set the policy file using the --authorization_policy_file impalad flag.");
    }

    config = new AuthorizationConfig("server1", "");
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(),
          "Authorization is enabled but the policy file path was null or empty. " +
          "Set the policy file using the --authorization_policy_file impalad flag.");
    }

    // Config validations skipped if authorization disabled
    config = new AuthorizationConfig("", "");
    Assert.assertFalse(config.isEnabled());
    config = new AuthorizationConfig(null, "");
    Assert.assertFalse(config.isEnabled());
    config = new AuthorizationConfig("", null);
    Assert.assertFalse(config.isEnabled());
    config = new AuthorizationConfig(null, null);
    Assert.assertFalse(config.isEnabled());
  }

  private void TestWithIncorrectConfig(AuthorizationConfig authzConfig)
      throws AnalysisException {
    AnalysisContext ac = new AnalysisContext(new Catalog(true, false, authzConfig),
        Catalog.DEFAULT_DB, USER);
    AuthzError(ac, "select * from functional.alltypesagg",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypesagg");
    AuthzError(ac, "ALTER TABLE functional_seq_snap.alltypes ADD COLUMNS (c1 int)",
        "User '%s' does not have privileges to execute 'ALTER' on: " +
        "functional_seq_snap.alltypes");
    AuthzError(ac, "drop table tpch.lineitem",
        "User '%s' does not have privileges to execute 'DROP' on: tpch.lineitem");
    AuthzError(ac, "show tables in functional",
        "User '%s' does not have privileges to access: functional.*");
  }

  private void AuthzOk(String stmt) throws AuthorizationException,
      AnalysisException {
    analysisContext.analyze(stmt);
  }

  /**
   * Verifies that a given statement fails authorization and the expected error
   * string matches.
   */
  private void AuthzError(String stmt, String expectedErrorString)
      throws AnalysisException {
    AuthzError(analysisContext, stmt, expectedErrorString);
  }


  private static void AuthzError(AnalysisContext analysisContext,
      String stmt, String expectedErrorString) throws AnalysisException {
    Preconditions.checkNotNull(expectedErrorString);
    try {
      analysisContext.analyze(stmt);
    } catch (AuthorizationException e) {
      // Insert the username into the error.
      expectedErrorString = String.format(expectedErrorString, USER.getName());
      String errorString = e.getMessage();
      Assert.assertTrue(
          "got error:\n" + errorString + "\nexpected:\n" + expectedErrorString,
          errorString.startsWith(expectedErrorString));
      return;
    }
    fail("Stmt didn't result in authorization error: " + stmt);
  }

  private static TSessionState createSessionState(String defaultDb, User user) {
    return new TSessionState(defaultDb, user.getName(), "", new TNetworkAddress("", 0));
  }
}
