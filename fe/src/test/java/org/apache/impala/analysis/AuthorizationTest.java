// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizeableTable;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.AuthorizationException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TMetadataOpcode;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.SentryPolicyService;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class AuthorizationTest extends FrontendTestBase {
  // Policy file has defined current user and 'test_user' have:
  //   ALL permission on 'tpch' database and 'newdb' database
  //   ALL permission on 'functional_seq_snap' database
  //   SELECT permissions on all tables in 'tpcds' database
  //   SELECT permissions on 'functional.alltypesagg' (no INSERT permissions)
  //   SELECT permissions on 'functional.complex_view' (no INSERT permissions)
  //   SELECT permissions on 'functional.view_view' (no INSERT permissions)
  //   SELECT permissions on columns ('id', 'int_col', and 'year') on
  //   'functional.alltypessmall' (no SELECT permissions on 'functional.alltypessmall')
  //   SELECT permissions on columns ('id', 'int_struct_col', 'struct_array_col',
  //   'int_map_col') on 'functional.allcomplextypes' (no SELECT permissions on
  //   'functional.allcomplextypes')
  //   ALL permissions on 'functional_parquet.allcomplextypes'
  //   SELECT permissions on all the columns of 'functional.alltypestiny' (no SELECT
  //   permissions on table 'functional.alltypestiny')
  //   INSERT permissions on 'functional.alltypes' (no SELECT permissions)
  //   INSERT permissions on all tables in 'functional_parquet' database
  //   No permissions on database 'functional_rc'
  //   Only column level permissions in 'functional_avro':
  //     SELECT permissions on columns ('id') on 'functional_avro.alltypessmall'
  public final static String AUTHZ_POLICY_FILE = "/test-warehouse/authz-policy.ini";
  public final static User USER = new User(System.getProperty("user.name"));

  // Tables in functional that the current user and 'test_user' have table- or
  // column-level SELECT or INSERT permission. I.e. that should be returned by
  // 'SHOW TABLES'.
  private static final List<String> FUNCTIONAL_VISIBLE_TABLES = Lists.newArrayList(
      "allcomplextypes", "alltypes", "alltypesagg", "alltypessmall", "alltypestiny",
      "complex_view", "view_view");

  /**
   * Test context whose instances are used to parameterize this test.
   */
  private static class TestContext {
    public final AuthorizationConfig authzConfig;
    public final ImpaladTestCatalog catalog;
    public TestContext(AuthorizationConfig authzConfig, ImpaladTestCatalog catalog) {
      this.authzConfig = authzConfig;
      this.catalog = catalog;
    }
  }

  private final TestContext ctx_;
  private final AnalysisContext analysisContext_;
  private final Frontend fe_;

  // Parameterize the test suite to run all tests using:
  // - a file based authorization policy
  // - a sentry service based authorization policy
  // These test contexts are statically initialized.
  private static final List<TestContext> testCtxs_;

  @Parameters
  public static Collection testVectors() { return testCtxs_; }

  /**
   * Create test contexts used for parameterizing this test. We create these statically
   * so that we do not have to re-create and initialize the auth policy and the catalog
   * for every test. Reloading the policy and the table metadata is very expensive
   * relative to the work done in the tests.
   */
  static {
    testCtxs_ = Lists.newArrayList();
    // Create and init file based auth config.
    AuthorizationConfig filePolicyAuthzConfig = createPolicyFileAuthzConfig();
    filePolicyAuthzConfig.validateConfig();
    ImpaladTestCatalog filePolicyCatalog = new ImpaladTestCatalog(filePolicyAuthzConfig);
    testCtxs_.add(new TestContext(filePolicyAuthzConfig, filePolicyCatalog));

    // Create and init sentry service based auth config.
    AuthorizationConfig sentryServiceAuthzConfig;
    try {
      sentryServiceAuthzConfig = createSentryServiceAuthzConfig();
    } catch (ImpalaException e) {
      // Convert the checked exception into an unchecked one because we have
      // no control of the initialization process, so there is no good place to
      // handle a checked exception thrown from a static block.
      throw new RuntimeException(e);
    }
    ImpaladTestCatalog sentryServiceCatalog =
        new ImpaladTestCatalog(sentryServiceAuthzConfig);
    testCtxs_.add(new TestContext(sentryServiceAuthzConfig, sentryServiceCatalog));
  }

  @BeforeClass
  public static void setUp() throws Exception {
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUp() {
    RuntimeEnv.INSTANCE.reset();
  }

  public AuthorizationTest(TestContext ctx) throws Exception {
    ctx_ = ctx;
    analysisContext_ = createAnalysisCtx(ctx_.authzConfig, USER.getName());
    fe_ = new Frontend(ctx_.authzConfig, ctx_.catalog);
  }

  public static AuthorizationConfig createPolicyFileAuthzConfig() {
    AuthorizationConfig result =
        AuthorizationConfig.createHadoopGroupAuthConfig("server1", AUTHZ_POLICY_FILE,
        System.getenv("IMPALA_HOME") + "/fe/src/test/resources/sentry-site.xml");
    result.validateConfig();
    return result;
  }

  public static AuthorizationConfig createSentryServiceAuthzConfig()
      throws ImpalaException {
    AuthorizationConfig result =
        AuthorizationConfig.createHadoopGroupAuthConfig("server1", null,
        System.getenv("IMPALA_HOME") + "/fe/src/test/resources/sentry-site.xml");
    setupSentryService(result);
    return result;
  }

  private static void setupSentryService(AuthorizationConfig authzConfig)
      throws ImpalaException {
    SentryPolicyService sentryService = new SentryPolicyService(
        authzConfig.getSentryConfig());
    // Server admin. Don't grant to any groups, that is done within
    // the test cases.
    String roleName = "admin";
    sentryService.createRole(USER, roleName, true);

    TPrivilege privilege = new TPrivilege("", TPrivilegeLevel.ALL,
        TPrivilegeScope.SERVER, false);
    privilege.setServer_name("server1");
    sentryService.grantRolePrivilege(USER, roleName, privilege);
    sentryService.revokeRoleFromGroup(USER, "admin", USER.getName());

    // insert functional alltypes
    roleName = "insert_functional_alltypes";
    roleName = roleName.toLowerCase();
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege("", TPrivilegeLevel.INSERT, TPrivilegeScope.TABLE,
        false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypes");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // insert_parquet
    roleName = "insert_parquet";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege("", TPrivilegeLevel.INSERT, TPrivilegeScope.TABLE,
        false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_parquet");
    privilege.setTable_name(AuthorizeableTable.ANY_TABLE_NAME);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // all newdb w/ all on URI
    roleName = "all_newdb";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege("", TPrivilegeLevel.ALL, TPrivilegeScope.DATABASE,
        false);
    privilege.setServer_name("server1");
    privilege.setDb_name("newdb");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    privilege = new TPrivilege("", TPrivilegeLevel.ALL, TPrivilegeScope.URI,
        false);
    privilege.setServer_name("server1");
    privilege.setUri("hdfs://localhost:20500/test-warehouse/new_table");
    privilege.setTable_name(AuthorizeableTable.ANY_TABLE_NAME);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    privilege = new TPrivilege("", TPrivilegeLevel.ALL, TPrivilegeScope.URI,
        false);
    privilege.setServer_name("server1");
    privilege.setUri("hdfs://localhost:20500/test-warehouse/UPPER_CASE");
    privilege.setTable_name(AuthorizeableTable.ANY_TABLE_NAME);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // all tpch
    roleName = "all_tpch";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());
    privilege = new TPrivilege("", TPrivilegeLevel.ALL, TPrivilegeScope.URI, false);
    privilege.setServer_name("server1");
    privilege.setUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    privilege = new TPrivilege("", TPrivilegeLevel.ALL, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("tpch");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select tpcds
    roleName = "select_tpcds";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege("", TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("tpcds");
    privilege.setTable_name(AuthorizeableTable.ANY_TABLE_NAME);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select_functional_alltypesagg
    roleName = "select_functional_alltypesagg";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege("", TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypesagg");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select_functional_complex_view
    roleName = "select_functional_complex_view";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege("", TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("complex_view");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select_functional_view_view
    roleName = "select_functional_view_view";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege("", TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("view_view");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // all_functional_seq_snap
    roleName = "all_functional_seq_snap";
    // Verify we are able to drop a role.
    sentryService.dropRole(USER, roleName, true);
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege("", TPrivilegeLevel.ALL, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_seq_snap");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select_column_level_functional
    roleName = "select_column_level_functional";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    // select (id, int_col, year) on functional.alltypessmall
    List<TPrivilege> privileges = Lists.newArrayList();
    for (String columnName: Arrays.asList("id", "int_col", "year")) {
      TPrivilege priv = new TPrivilege("", TPrivilegeLevel.SELECT,
          TPrivilegeScope.COLUMN, false);
      priv.setServer_name("server1");
      priv.setDb_name("functional");
      priv.setTable_name("alltypessmall");
      priv.setColumn_name(columnName);
      privileges.add(priv);
    }
    sentryService.grantRolePrivileges(USER, roleName, privileges);
    privileges.clear();

    // select (id, int_struct_col) on functional.allcomplextypes
    for (String columnName: Arrays.asList("id", "int_struct_col", "struct_array_col",
        "int_map_col")) {
      TPrivilege priv = new TPrivilege("", TPrivilegeLevel.SELECT,
          TPrivilegeScope.COLUMN, false);
      priv.setServer_name("server1");
      priv.setDb_name("functional");
      priv.setTable_name("allcomplextypes");
      priv.setColumn_name(columnName);
      privileges.add(priv);
    }
    sentryService.grantRolePrivileges(USER, roleName, privileges);
    privileges.clear();

    // table privileges on functional_parquet.allcomplextypes
    privilege = new TPrivilege("", TPrivilegeLevel.ALL, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_parquet");
    privilege.setTable_name("allcomplextypes");
    privileges.add(privilege);
    sentryService.grantRolePrivileges(USER, roleName, privileges);
    privileges.clear();

    // select (*) on functional.alltypestiny
    String[] columnNames = {"id", "bool_col", "tinyint_col", "smallint_col",
        "int_col", "bigint_col", "float_col", "double_col", "date_string_col",
        "timestamp_col", "string_col", "year", "month"};
    for (String columnName: Arrays.asList(columnNames)) {
      TPrivilege priv = new TPrivilege("", TPrivilegeLevel.SELECT,
          TPrivilegeScope.COLUMN, false);
      priv.setServer_name("server1");
      priv.setDb_name("functional");
      priv.setTable_name("alltypestiny");
      priv.setColumn_name(columnName);
      privileges.add(priv);
    }
    sentryService.grantRolePrivileges(USER, roleName, privileges);

    // select_column_level_functional_avro
    roleName = "select_column_level_functional_avro";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege("", TPrivilegeLevel.SELECT,
        TPrivilegeScope.COLUMN, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_avro");
    privilege.setTable_name("alltypessmall");
    privilege.setColumn_name("id");
    sentryService.grantRolePrivilege(USER, roleName, privilege);
  }

  @Test
  public void TestSentryService() throws ImpalaException {
    SentryPolicyService sentryService =
        new SentryPolicyService(ctx_.authzConfig.getSentryConfig());
    String roleName = "testRoleName";
    roleName = roleName.toLowerCase();

    sentryService.createRole(USER, roleName, true);
    String dbName = UUID.randomUUID().toString();
    TPrivilege privilege =
        new TPrivilege("", TPrivilegeLevel.ALL, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name(dbName);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    for (int i = 0; i < 2; ++i) {
      privilege = new TPrivilege("", TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE,
          false);
      privilege.setServer_name("server1");
      privilege.setDb_name(dbName);
      privilege.setTable_name("test_tbl_" + String.valueOf(i));
      sentryService.grantRolePrivilege(USER, roleName, privilege);
    }

    List<TPrivilege> privileges = Lists.newArrayList();
    for (int i = 0; i < 10; ++i) {
      TPrivilege priv = new TPrivilege("", TPrivilegeLevel.SELECT, TPrivilegeScope.COLUMN,
          false);
      priv.setServer_name("server1");
      priv.setDb_name(dbName);
      priv.setTable_name("test_tbl_1");
      priv.setColumn_name("col_" + String.valueOf(i));
      privileges.add(priv);
    }
    sentryService.grantRolePrivileges(USER, roleName, privileges);
  }

  @After
  public void TestTPCHCleanup() throws AuthorizationException, AnalysisException {
    // Failure to cleanup TPCH can cause:
    // TestDropDatabase(org.apache.impala.analysis.AuthorizationTest):
    // Cannot drop non-empty database: tpch
    if (ctx_.catalog.getDb("tpch").numFunctions() != 0) {
      fail("Failed to clean up functions in tpch.");
    }
  }

  @Test
  public void TestSelect() throws ImpalaException {
    // Can select from table that user has privileges on.
    AuthzOk("select * from functional.alltypesagg");

    AuthzOk("select * from functional_seq_snap.alltypes");

    // Can select from view that user has privileges on even though he/she doesn't
    // have privileges on underlying tables.
    AuthzOk("select * from functional.complex_view");

    // User has permission to select the view but not on the view (alltypes_view)
    // referenced in its view definition.
    AuthzOk("select * from functional.view_view");

    // User does not have SELECT privileges on this view.
    AuthzError("select * from functional.complex_view_sub",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.complex_view_sub");

    // User has SELECT privileges on the view and the join table.
    AuthzOk("select a.id from functional.view_view a "
        + "join functional.alltypesagg b ON (a.id = b.id)");

    // User has SELECT privileges on the view, but does not have privileges
    // to select join table.
    AuthzError("select a.id from functional.view_view a "
        + "join functional.alltypes b ON (a.id = b.id)",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // Tests authorization after a statement has been rewritten (IMPALA-3915).
    // User has SELECT privileges on the view which contains a subquery.
    AuthzOk("select * from functional_seq_snap.subquery_view");

    // User does not have SELECT privileges on the view which contains a subquery.
    AuthzError("select * from functional_rc.subquery_view",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional_rc.subquery_view");

    // Constant select.
    AuthzOk("select 1");

    // Unqualified table name.
    AuthzError("select * from alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: default.alltypes");

    // Select with no privileges on table.
    AuthzError("select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // Select with no privileges on view.
    AuthzError("select * from functional.complex_view_sub",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.complex_view_sub");

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

    // User has SELECT privileges on all the columns of 'alltypestiny'
    AuthzOk("select * from functional.alltypestiny");

    // No SELECT privileges on all the columns of 'alltypessmall'
    AuthzError("select * from functional.alltypessmall", "User '%s' does " +
        "not have privileges to execute 'SELECT' on: functional.alltypessmall");

    // No SELECT privileges on table 'alltypessmall'
    AuthzError("select count(*) from functional.alltypessmall", "User '%s' does " +
        "not have privileges to execute 'SELECT' on: functional.alltypessmall");
    AuthzError("select 1 from functional.alltypessmall", "User '%s' does not " +
        "have privileges to execute 'SELECT' on: functional.alltypessmall");
    AuthzOk("select 1, id from functional.alltypessmall");

    // No SELECT privileges on column 'month'
    AuthzError("select id, int_col, year, month from functional.alltypessmall",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypessmall");

    // User has column-level privileges on all referenced columns
    AuthzOk("select id, count(int_col) from functional.alltypessmall where " +
        "year = 2010 group by id");

    // No SELECT privileges on 'int_array_col'
    AuthzError("select a.id, b.item from functional.allcomplextypes a, " +
        "a.int_array_col b", "User '%s' does not have privileges to execute " +
        "'SELECT' on: functional.allcomplextypes");

    // Sufficient column-level privileges on both scalar and nested columns
    AuthzOk("select a.int_struct_col.f1 from functional.allcomplextypes a " +
        "where a.id = 1");
    AuthzOk("select pos, item.f1, f2 from functional.allcomplextypes t, " +
        "t.struct_array_col");
    AuthzOk("select * from functional.allcomplextypes.struct_array_col");
    AuthzOk("select key from functional.allcomplextypes.int_map_col");
    AuthzOk("select id, b.key from functional.allcomplextypes a, a.int_map_col b");

    // No SELECT privileges on 'alltypessmall'
    AuthzError("select a.* from functional.alltypesagg cross join " +
        "functional.alltypessmall b", "User '%s' does not have privileges to execute " +
        "'SELECT' on: functional.alltypessmall");

    // User has SELECT privileges on all columns referenced in the inline view
    AuthzOk("select * from (select id, int_col from functional.alltypessmall) v");
  }

  @Test
  public void TestUnion() throws ImpalaException {
    AuthzOk("select * from functional.alltypesagg union all " +
        "select * from functional.alltypesagg");

    AuthzError("select * from functional.alltypesagg union all " +
        "select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
         "functional.alltypes");

    AuthzOk("select id, int_col, year from functional.alltypessmall union all " +
        "select id, int_col, year from functional.alltypestiny");

    AuthzError("select * from functional.alltypessmall union all " +
        "select * from functional.alltypestiny", "User '%s' does not have privileges " +
        "to execute 'SELECT' on: functional.alltypessmall");
  }

  @Test
  public void TestInsert() throws ImpalaException {
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

    // User doesn't have INSERT permissions in the target view.
    // Inserting into a view is not allowed.
    AuthzError("insert into functional.alltypes_view select 1",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.alltypes_view");

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

    // User doesn't have INSERT permissions on the target table but has sufficient SELECT
    // permissions on all the referenced columns of the source table
    AuthzError("insert into functional.alltypestiny partition (month, year) " +
        "select * from functional.alltypestiny", "User '%s' does not have " +
        "privileges to execute 'INSERT' on: functional.alltypestiny");

    // User has INSERT permissions on target table but insufficient column-level
    // permissions on the source table
    AuthzError("insert into functional.alltypes partition (month, year) " +
        "select * from functional.alltypessmall", "User '%s' does not have " +
        "privileges to execute 'SELECT' on: functional.alltypessmall");

    // User has INSERT permissions on the target table and SELECT permissions on
    // all the columns of the source table
    AuthzOk("insert into functional.alltypes partition (month, year) " +
        "select * from functional.alltypestiny where id < 100");

    // Same as above with a column permutation
    AuthzOk("insert into table functional.alltypes" +
        "(id, bool_col, string_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, tinyint_col, timestamp_col) " +
        "partition (month, year) select id, bool_col, string_col, smallint_col, " +
        "int_col, bigint_col, float_col, double_col, date_string_col, tinyint_col, " +
        "timestamp_col, month, year from functional.alltypestiny");
  }

  @Test
  public void TestWithClause() throws ImpalaException {
    // User has SELECT privileges on table in WITH-clause view.
    AuthzOk("with t as (select * from functional.alltypesagg) select * from t");
    // User doesn't have SELECT privileges on table in WITH-clause view.
    AuthzError("with t as (select * from functional.alltypes) select * from t",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");
    // User has SELECT privileges on view in WITH-clause view.
    AuthzOk("with t as (select * from functional.complex_view) select * from t");

    // User has SELECT privileges on table in WITH-clause view in INSERT.
    AuthzOk("with t as (select * from functional_seq_snap.alltypes) " +
        "insert into functional_parquet.alltypes partition(month,year) select * from t");
    // User doesn't have SELECT privileges on table in WITH-clause view in INSERT.
    AuthzError("with t as (select * from functional_parquet.alltypes) " +
        "insert into functional_parquet.alltypes partition(month,year) select * from t",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
         "functional_parquet.alltypes");
    // User doesn't have SELECT privileges on view in WITH-clause view in INSERT.
    AuthzError("with t as (select * from functional.alltypes_view) " +
        "insert into functional_parquet.alltypes partition(month,year) select * from t",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
         "functional.alltypes_view");

    // User has SELECT privileges on columns referenced in the WITH-clause view.
    AuthzOk("with t as (select id, int_col from functional.alltypessmall) " +
        "select * from t");

    // User does not have SELECT privileges on 'month'
    AuthzError("with t as (select id, int_col from functional.alltypessmall " +
        "where month = 10) select count(*) from t", "User '%s' does not have " +
        "privileges to execute 'SELECT' on: functional.alltypessmall");
  }

  @Test
  public void TestExplain() throws ImpalaException {
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

    // Test explain on views. User has permissions on all the underlying tables.
    AuthzOk("explain select * from functional_seq_snap.alltypes_view");
    AuthzOk("explain insert into functional_parquet.alltypes " +
        "partition(month,year) select * from functional_seq_snap.alltypes_view");

    // Select on view without permissions on view.
    AuthzError("explain select * from functional.alltypes_view",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes_view");
    // Insert into view without permissions on view.
    AuthzError("explain insert into functional.alltypes_view " +
        "select * from functional_seq_snap.alltypes ",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.alltypes_view");

    // User has permission on view, but not on underlying tables.
    AuthzError("explain select * from functional.complex_view",
        "User '%s' does not have privileges to EXPLAIN this statement.");
    // User has permission on view in WITH clause, but not on underlying tables.
    AuthzError("explain with t as (select * from functional.complex_view) " +
        "select * from t",
        "User '%s' does not have privileges to EXPLAIN this statement.");

    // User has permission on view in WITH clause, but not on underlying tables.
    AuthzError("explain with t as (select * from functional.complex_view) " +
        "select * from t",
        "User '%s' does not have privileges to EXPLAIN this statement.");
    // User has permission on view and on view inside view,
    // but not on tables in view inside view.
    AuthzError("explain select * from functional.view_view",
        "User '%s' does not have privileges to EXPLAIN this statement.");
    // User doesn't have permission on tables in view inside view.
    AuthzError("explain insert into functional_seq_snap.alltypes " +
        "partition(month,year) select * from functional.view_view",
        "User '%s' does not have privileges to EXPLAIN this statement.");

    // User has SELECT privileges on the view, but does not have privileges
    // to select join table. Should get standard auth message back.
    AuthzError("explain select a.id from functional.view_view a "
        + "join functional.alltypes b ON (a.id = b.id)",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // User has privileges on tables inside the first view, but not the second
    // view. Should get masked auth error back.
    AuthzError("explain select a.id from functional.view_view a "
        + "join functional.complex_view b ON (a.id = b.id)",
        "User '%s' does not have privileges to EXPLAIN this statement.");

    // User has SELECT privileges on all referenced columns
    AuthzOk("explain select * from functional.alltypestiny");

    // User doesn't have SELECT privileges on all referenced columns
    AuthzError("explain select * from functional.alltypessmall", "User '%s' " +
        "does not have privileges to execute 'SELECT' on: functional.alltypessmall");
  }

  @Test
  public void TestUseDb() throws ImpalaException {
    // Positive cases (user has privileges on these tables).
    AuthzOk("use functional");
    AuthzOk("use functional_avro"); // Database with only column privileges.
    AuthzOk("use tpcds");
    AuthzOk("use tpch");

    // Should always be able to use default, even if privilege was not explicitly
    // granted.
    AuthzOk("use default");

    AuthzError("use functional_seq",
        "User '%s' does not have privileges to access: functional_seq.*");

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

    // All users should be able to use the system db.
    AuthzOk("use _impala_builtins");
  }

  @Test
  public void TestResetMetadata() throws ImpalaException {
    // Positive cases (user has privileges on these tables/views).
    AuthzOk("invalidate metadata functional.alltypesagg");
    AuthzOk("refresh functional.alltypesagg");
    AuthzOk("invalidate metadata functional.view_view");
    AuthzOk("refresh functional.view_view");
    // Positive cases for checking refresh partition
    AuthzOk("refresh functional.alltypesagg partition (year=2010, month=1, day=1)");
    AuthzOk("refresh functional.alltypes partition (year=2009, month=1)");
    AuthzOk("refresh functional_seq_snap.alltypes partition (year=2009, month=1)");

    AuthzError("invalidate metadata unknown_db.alltypessmall",
        "User '%s' does not have privileges to access: unknown_db.alltypessmall");
    AuthzError("invalidate metadata functional_seq.alltypessmall",
        "User '%s' does not have privileges to access: functional_seq.alltypessmall");
    AuthzError("invalidate metadata functional.alltypes_view",
        "User '%s' does not have privileges to access: functional.alltypes_view");
    AuthzError("invalidate metadata functional.unknown_table",
        "User '%s' does not have privileges to access: functional.unknown_table");
    AuthzError("invalidate metadata functional.alltypessmall",
        "User '%s' does not have privileges to access: functional.alltypessmall");
    AuthzError("refresh functional.alltypessmall",
        "User '%s' does not have privileges to access: functional.alltypessmall");
    AuthzError("refresh functional.alltypes_view",
        "User '%s' does not have privileges to access: functional.alltypes_view");
    // Only column-level privileges on the table
    AuthzError("invalidate metadata functional.alltypestiny", "User '%s' does not " +
        "have privileges to access: functional.alltypestiny");
    // Only column-level privileges on the table
    AuthzError("refresh functional.alltypestiny", "User '%s' does not have " +
        "privileges to access: functional.alltypestiny");

    AuthzError("invalidate metadata",
        "User '%s' does not have privileges to access: server");
    AuthzError(
        "refresh functional_rc.alltypesagg partition (year=2010, month=1, day=1)",
        "User '%s' does not have privileges to access: functional_rc.alltypesagg");
    AuthzError(
        "refresh functional_rc.alltypesagg partition (year=2010, month=1, day=9999)",
        "User '%s' does not have privileges to access: functional_rc.alltypesagg");

    // TODO: Add test support for dynamically changing privileges for
    // file-based policy.
    if (ctx_.authzConfig.isFileBasedPolicy()) return;
    SentryPolicyService sentryService = createSentryService();

    try {
      sentryService.grantRoleToGroup(USER, "admin", USER.getName());
      ((ImpaladTestCatalog) ctx_.catalog).reset();
      AuthzOk("invalidate metadata");
    } finally {
      sentryService.revokeRoleFromGroup(USER, "admin", USER.getName());
      ((ImpaladTestCatalog) ctx_.catalog).reset();
    }
  }

  @Test
  public void TestCreateTable() throws ImpalaException {
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

    // Create table AS SELECT positive and negative cases for SELECT privilege.
    AuthzOk("create table tpch.new_table as select * from functional.alltypesagg");
    AuthzError("create table tpch.new_table as select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // CTAS with a subquery.
    AuthzOk("create table tpch.new_table as select * from functional.alltypesagg " +
        "where id < (select max(year) from functional.alltypesagg)");

    AuthzError("create table functional.tbl tblproperties('a'='b')" +
        " as select 1",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "functional.tbl");

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

    // IMPALA-4000: Only users with ALL privileges on SERVER may create external Kudu
    // tables.
    AuthzError("create external table tpch.kudu_tbl stored as kudu " +
        "TBLPROPERTIES ('kudu.master_addresses'='127.0.0.1', 'kudu.table_name'='tbl')",
        "User '%s' does not have privileges to access: server1");
    AuthzError("create table tpch.kudu_tbl (i int, j int, primary key (i))" +
        " PARTITION BY HASH (i) PARTITIONS 9 stored as kudu TBLPROPERTIES " +
        "('kudu.master_addresses'='127.0.0.1')",
        "User '%s' does not have privileges to access: server1");

    // IMPALA-4000: ALL privileges on SERVER are not required to create managed tables.
    AuthzOk("create table tpch.kudu_tbl (i int, j int, primary key (i))" +
        " PARTITION BY HASH (i) PARTITIONS 9 stored as kudu");

    // User does not have permission to create table at the specified location..
    AuthzError("create table tpch.new_table (i int) location " +
        "'hdfs://localhost:20500/test-warehouse/alltypes'",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/alltypes");

    AuthzError("create table _impala_builtins.tbl(i int)",
        "Cannot modify system database.");

    // Check that create like file follows authorization rules for HDFS files
    AuthzError("create table tpch.table_DNE like parquet "
        + "'hdfs://localhost:20500/test-warehouse/alltypes'",
        "User '%s' does not have privileges to access: "
        + "hdfs://localhost:20500/test-warehouse/alltypes");

    // Sufficient column-level privileges in the source table
    AuthzOk("create table tpch.new_table as select id, int_col from " +
        "functional.alltypessmall where year = 2010");

    // Insufficient column-level privileges in the source table
    AuthzError("create table tpch.new as select * from functional.alltypessmall",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypessmall");

    // No permissions on target table but sufficient column-level privileges in the
    // source table
    AuthzError("create table tpch_rc.new_tbl as select * from functional.alltypestiny",
        "User '%s' does not have privileges to execute 'CREATE' on: tpch_rc.new_tbl");

    // Try creating an external table on a URI with upper case letters
    AuthzOk("create external table tpch.upper_case (a int) location " +
        "'hdfs://localhost:20500/test-warehouse/UPPER_CASE/test'");

    // Try creating table on the same URI in lower case. It should fail
    AuthzError("create external table tpch.upper_case (a int) location " +
        "'hdfs://localhost:20500/test-warehouse/upper_case/test'",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/upper_case/test");
  }

  @Test
  public void TestCreateView() throws ImpalaException {
    AuthzOk("create view tpch.new_view as select * from functional.alltypesagg");
    AuthzOk("create view tpch.new_view (a, b, c) as " +
        "select int_col, string_col, timestamp_col from functional.alltypesagg");
    // Create view IF NOT EXISTS, user has permission and table exists.
    AuthzOk("create view if not exists tpch.lineitem as " +
        "select * from functional.alltypesagg");
    // Create view IF NOT EXISTS, user has permission and table/view exists.
    try {
      AuthzOk("create view tpch.lineitem as select * from functional.alltypesagg");
      fail("Expected analysis error.");
    } catch (AnalysisException e) {
      Assert.assertEquals(e.getMessage(), "Table already exists: tpch.lineitem");
    }

    // Create view IF NOT EXISTS, user does not have permission and table/view exists.
    AuthzError("create view if not exists functional_seq.alltypes as " +
        "select * from functional.alltypesagg",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "functional_seq.alltypes");

    // No permissions on source table.
    AuthzError("create view tpch.new_view as select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes");

    // No permissions on target table.
    AuthzError("create view tpch_rc.new as select * from functional.alltypesagg",
        "User '%s' does not have privileges to execute 'CREATE' on: tpch_rc.new");

    // Unqualified view name.
    AuthzError("create view new_view as select * from functional.alltypesagg",
        "User '%s' does not have privileges to execute 'CREATE' on: default.new_view");

    // Table already exists (user does not have permission).
    AuthzError("create view functional.alltypes_view as " +
        "select * from functional.alltypesagg",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "functional.alltypes_view");

    // Database does not exist, user does not have access.
    AuthzError("create view nodb.alltypes as select * from functional.alltypesagg",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "nodb.alltypes");

    AuthzError("create view _impala_builtins.new_view as "
        + "select * from functional.alltypesagg",
        "Cannot modify system database.");

    // User has SELECT privileges on the referenced columns of the source table
    AuthzOk("create view tpch.new_view as select * from functional.alltypestiny");
    AuthzOk("create view tpch.new_view as select count(id) from " +
        "functional.alltypessmall");

    // No SELECT permissions on all the referenced columns
    AuthzError("create view tpch.new as select id, count(month) from " +
        "functional.alltypessmall where int_col = 1 group by id", "User '%s' does " +
        "not have privileges to execute 'SELECT' on: functional.alltypessmall");
  }

  @Test
  public void TestCreateDatabase() throws ImpalaException {
    // Database already exists (no permissions).
    AuthzError("create database functional",
        "User '%s' does not have privileges to execute 'CREATE' on: functional");

    // Non existent db (no permissions).
    AuthzError("create database nodb",
        "User '%s' does not have privileges to execute 'CREATE' on: nodb");

    // Non existent db (no permissions).
    AuthzError("create database if not exists _impala_builtins",
        "Cannot modify system database.");

    // TODO: Add test support for dynamically changing privileges for
    // file-based policy.
    if (ctx_.authzConfig.isFileBasedPolicy()) return;

    SentryPolicyService sentryService =
        new SentryPolicyService(ctx_.authzConfig.getSentryConfig());
    try {
      sentryService.grantRoleToGroup(USER, "admin", USER.getName());
      ctx_.catalog.reset();

      // User has permissions to create database.
      AuthzOk("create database newdb");

      // Create database with location specified explicitly (user has permission).
      // Since create database requires server-level privileges there should never
      // be a case where the user has privileges to create a database does not have
      // privileges on the URI.
      AuthzOk("create database newdb location " +
          "'hdfs://localhost:20500/test-warehouse/new_table'");
    } finally {
      sentryService.revokeRoleFromGroup(USER, "admin", USER.getName());
      ctx_.catalog.reset();
    }
  }

  @Test
  public void TestDropDatabase() throws ImpalaException {
    // User has permissions.
    AuthzOk("drop database tpch");
    AuthzOk("drop database tpch cascade");
    AuthzOk("drop database tpch restrict");
    // User has permissions, database does not exists and IF EXISTS specified.
    AuthzOk("drop database if exists newdb");
    AuthzOk("drop database if exists newdb cascade");
    AuthzOk("drop database if exists newdb restrict");
    // User has permission, database does not exists, IF EXISTS not specified.
    try {
      AuthzOk("drop database newdb");
      fail("Expected analysis error");
    } catch (AnalysisException e) {
      Assert.assertEquals(e.getMessage(), "Database does not exist: newdb");
    }
    try {
      AuthzOk("drop database newdb cascade");
      fail("Expected analysis error");
    } catch (AnalysisException e) {
      Assert.assertEquals(e.getMessage(), "Database does not exist: newdb");
    }
    try {
      AuthzOk("drop database newdb restrict");
      fail("Expected analysis error");
    } catch (AnalysisException e) {
      Assert.assertEquals(e.getMessage(), "Database does not exist: newdb");
    }

    // Database exists, user doesn't have permission to drop.
    AuthzError("drop database functional",
        "User '%s' does not have privileges to execute 'DROP' on: functional");
    AuthzError("drop database if exists functional",
        "User '%s' does not have privileges to execute 'DROP' on: functional");
    AuthzError("drop database if exists functional cascade",
        "User '%s' does not have privileges to execute 'DROP' on: functional");
    AuthzError("drop database if exists functional restrict",
        "User '%s' does not have privileges to execute 'DROP' on: functional");

    // Database does not exist, user doesn't have permission to drop.
    AuthzError("drop database nodb",
        "User '%s' does not have privileges to execute 'DROP' on: nodb");
    AuthzError("drop database if exists nodb",
        "User '%s' does not have privileges to execute 'DROP' on: nodb");
    AuthzError("drop database if exists nodb cascade",
        "User '%s' does not have privileges to execute 'DROP' on: nodb");
    AuthzError("drop database if exists nodb restrict",
        "User '%s' does not have privileges to execute 'DROP' on: nodb");

    AuthzError("drop database _impala_builtins",
        "Cannot modify system database.");
  }

  @Test
  public void TestDropTable() throws ImpalaException {
    // Drop table (user has permission).
    AuthzOk("drop table tpch.lineitem");
    AuthzOk("drop table if exists tpch.lineitem");

    // Drop table (user does not have permission).
    AuthzError("drop table functional.alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: functional.alltypes");
    AuthzError("drop table if exists functional.alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: functional.alltypes");
    // Drop table when user only has column-level privileges
    AuthzError("drop table if exists functional.alltypestiny", "User '%s' does not " +
        "have privileges to execute 'DROP' on: functional.alltypestiny");

    // Drop table with unqualified table name.
    AuthzError("drop table alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: default.alltypes");

    // Drop table with non-existent database.
    AuthzError("drop table nodb.alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: nodb.alltypes");

    // Drop table with non-existent table.
    AuthzError("drop table functional.notbl",
        "User '%s' does not have privileges to execute 'DROP' on: functional.notbl");

    // Using DROP TABLE on a view does not reveal privileged information.
    AuthzError("drop table functional.view_view",
        "User '%s' does not have privileges to execute 'DROP' on: functional.view_view");

    // Using DROP TABLE on a view does not reveal privileged information.
    AuthzError("drop table if exists _impala_builtins.tbl",
        "Cannot modify system database.");
  }

  @Test
  public void TestDropView() throws ImpalaException {
    // Drop view (user has permission).
    AuthzOk("drop view functional_seq_snap.alltypes_view");
    AuthzOk("drop view if exists functional_seq_snap.alltypes_view");

    // Drop view (user does not have permission).
    AuthzError("drop view functional.alltypes_view",
        "User '%s' does not have privileges to execute 'DROP' on: functional.alltypes");
    AuthzError("drop view if exists functional.alltypes_view",
        "User '%s' does not have privileges to execute 'DROP' on: functional.alltypes");

    // Drop view with unqualified table name.
    AuthzError("drop view alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: default.alltypes");

    // Drop view with non-existent database.
    AuthzError("drop view nodb.alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: nodb.alltypes");

    // Drop view with non-existent table.
    AuthzError("drop view functional.notbl",
        "User '%s' does not have privileges to execute 'DROP' on: functional.notbl");

    // Using DROP VIEW on a table does not reveal privileged information.
    AuthzError("drop view functional.alltypes",
        "User '%s' does not have privileges to execute 'DROP' on: functional.alltypes");

    // Using DROP VIEW on a table does not reveal privileged information.
    AuthzError("drop view _impala_builtins.my_view",
        "Cannot modify system database.");
  }

  @Test
  public void TestTruncate() throws ImpalaException {
    AuthzOk("truncate table functional_parquet.alltypes");

    // User doesn't have INSERT permissions in the target table.
    AuthzError("truncate table functional.alltypesagg",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.alltypesagg");

    // User doesn't have INSERT permissions in the target view.
    // Truncating a view is not allowed.
    AuthzError("truncate table functional.alltypes_view",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.alltypes_view");

    // User doesn't have INSERT permissions on the target table but has SELECT permissions
    // on all the columns.
    AuthzError("truncate table functional.alltypestiny", "User '%s' does not have " +
        "privileges to execute 'INSERT' on: functional.alltypestiny");
  }

  @Test
  public void TestAlterTable() throws ImpalaException {
    // User has permissions to modify tables.
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes ADD COLUMNS (c1 int)");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes REPLACE COLUMNS (c1 int)");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes CHANGE int_col c1 int");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes DROP int_col");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes RENAME TO functional_seq_snap.t1");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes SET FILEFORMAT PARQUET");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'/test-warehouse/new_table'");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes SET TBLPROPERTIES " +
        "('a'='b', 'c'='d')");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'hdfs://localhost:20500/test-warehouse/new_table'");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes PARTITION(year=2009, month=1) " +
        "SET LOCATION 'hdfs://localhost:20500/test-warehouse/new_table'");

    AuthzOk("ALTER TABLE functional_seq_snap.alltypes SET CACHED IN 'testPool'");
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes RECOVER PARTITIONS");

    // Alter table and set location to a path the user does not have access to.
    AuthzError("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'hdfs://localhost:20500/test-warehouse/no_access'",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/no_access");
    AuthzError("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'/test-warehouse/no_access'",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/no_access");
    AuthzError("ALTER TABLE functional_seq_snap.alltypes PARTITION(year=2009, month=1) " +
        "SET LOCATION '/test-warehouse/no_access'",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/no_access");

    // Add multiple partitions. User has access to location path.
    AuthzOk("ALTER TABLE functional_seq_snap.alltypes ADD " +
        "PARTITION(year=2011, month=1) " +
        "PARTITION(year=2011, month=2) " +
        "LOCATION 'hdfs://localhost:20500/test-warehouse/new_table'");
    // For one new partition location is set to a path the user does not have access to.
    AuthzError("ALTER TABLE functional_seq_snap.alltypes ADD " +
        "PARTITION(year=2011, month=3) " +
        "PARTITION(year=2011, month=4) LOCATION '/test-warehouse/no_access'",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20500/test-warehouse/no_access");

    // Different filesystem, user has permission to base path.
    AuthzError("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'hdfs://localhost:20510/test-warehouse/new_table'",
        "User '%s' does not have privileges to access: " +
        "hdfs://localhost:20510/test-warehouse/new_table");

    AuthzError("ALTER TABLE functional.alltypes SET FILEFORMAT PARQUET",
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
    AuthzError("ALTER TABLE functional.alltypes add partition (year=1, month=1)",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("ALTER TABLE functional.alltypes set cached in 'testPool'",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("ALTER TABLE functional.alltypes set uncached",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("ALTER TABLE functional.alltypes recover partitions",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");

    // Trying to ALTER TABLE a view does not reveal any privileged information.
    AuthzError("ALTER TABLE functional.view_view SET FILEFORMAT PARQUET",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.view_view");
    AuthzError("ALTER TABLE functional.view_view ADD COLUMNS (c1 int)",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.view_view");
    AuthzError("ALTER TABLE functional.view_view REPLACE COLUMNS (c1 int)",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.view_view");
    AuthzError("ALTER TABLE functional.view_view CHANGE int_col c1 int",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.view_view");
    AuthzError("ALTER TABLE functional.view_view DROP int_col",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.view_view");
    AuthzError("ALTER TABLE functional.view_views rename to functional_seq_snap.t1",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.view_view");

    // No privileges on target (existing table).
    AuthzError("ALTER TABLE functional_seq_snap.alltypes rename to functional.alltypes",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "functional.alltypes");

    // No privileges on target (existing view).
    AuthzError("ALTER TABLE functional_seq_snap.alltypes rename to " +
        "functional.alltypes_view",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "functional.alltypes");

    // ALTER TABLE on a view does not reveal privileged information.
    AuthzError("ALTER TABLE functional.alltypes_view rename to " +
        "functional_seq_snap.new_view",
        "User '%s' does not have privileges to execute 'ALTER' on: " +
        "functional.alltypes_view");

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

    AuthzError("ALTER TABLE alltypes SET TBLPROPERTIES ('a'='b', 'c'='d')",
        "User '%s' does not have privileges to execute 'ALTER' on: default.alltypes");

    // Alter table, user only has column-level privileges.
    AuthzError("ALTER TABLE functional.alltypestiny ADD COLUMNS (c1 int)", "User " +
        "'%s' does not have privileges to execute 'ALTER' on: functional.alltypestiny");

    // User has column-level privileges on the column being dropped but no table-level
    // privileges
    AuthzError("ALTER TABLE functional.alltypestinyt DROP id", "User '%s' does not " +
        "have privileges to execute 'ALTER' on: functional.alltypestiny");
  }

  @Test
  public void TestAlterView() throws ImpalaException {
    AuthzOk("ALTER VIEW functional_seq_snap.alltypes_view rename to " +
        "functional_seq_snap.v1");

    // No privileges on target (existing table).
    AuthzError("ALTER VIEW functional_seq_snap.alltypes_view rename to " +
        "functional.alltypes",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "functional.alltypes");

    // No privileges on target (existing view).
    AuthzError("ALTER VIEW functional_seq_snap.alltypes_view rename to " +
        "functional.alltypes_view",
        "User '%s' does not have privileges to execute 'CREATE' on: " +
        "functional.alltypes_view");

    // ALTER VIEW on a table does not reveal privileged information.
    AuthzError("ALTER VIEW functional.alltypes rename to " +
        "functional_seq_snap.new_view",
        "User '%s' does not have privileges to execute 'ALTER' on: " +
        "functional.alltypes");

    // Rename view that does not exist (no permissions).
    AuthzError("ALTER VIEW functional.notbl rename to functional_seq_snap.newtbl",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.notbl");

    // Rename view in db that does not exist (no permissions).
    AuthzError("ALTER VIEW nodb.alltypes rename to functional_seq_snap.newtbl",
        "User '%s' does not have privileges to execute 'ALTER' on: nodb.alltypes");

    // Alter view that does not exist (no permissions).
    AuthzError("ALTER VIEW functional.notbl rename to functional_seq_snap.new_view",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.notbl");

    // Alter view in db that does not exist (no permissions).
    AuthzError("ALTER VIEW nodb.alltypes rename to functional_seq_snap.new_view",
        "User '%s' does not have privileges to execute 'ALTER' on: nodb.alltypes");

    // Unqualified view name.
    AuthzError("ALTER VIEW alltypes rename to functional_seq_snap.new_view",
        "User '%s' does not have privileges to execute 'ALTER' on: default.alltypes");

    // No permissions on target view.
    AuthzError("alter view functional.alltypes_view as " +
        "select * from functional.alltypesagg",
        "User '%s' does not have privileges to execute 'ALTER' on: " +
        "functional.alltypes_view");

    // No permissions on source view.
    AuthzError("alter view functional_seq_snap.alltypes_view " +
        "as select * from functional.alltypes_view",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypes_view");
  }

  @Test
  public void TestComputeStatsTable() throws ImpalaException {
    AuthzOk("compute stats functional_seq_snap.alltypes");

    AuthzError("compute stats functional.alltypes",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("compute stats functional.alltypesagg",
        "User '%s' does not have privileges to execute 'ALTER' on: " +
        "functional.alltypesagg");
    // User has column-level privileges on the target table
    AuthzError("compute stats functional.alltypestiny", "User '%s' does not have " +
        "privileges to execute 'ALTER' on: functional.alltypestiny");
  }

  @Test
  public void TestDropStats() throws ImpalaException {
    AuthzOk("drop stats functional_seq_snap.alltypes");

    AuthzError("drop stats functional.alltypes",
        "User '%s' does not have privileges to execute 'ALTER' on: functional.alltypes");
    AuthzError("drop stats functional.alltypesagg",
        "User '%s' does not have privileges to execute 'ALTER' on: " +
        "functional.alltypesagg");
    // User has column-level privileges on the target table
    AuthzError("drop stats functional.alltypestiny", "User '%s' does not have " +
        "privileges to execute 'ALTER' on: functional.alltypestiny");
  }

  @Test
  public void TestDescribeDb() throws ImpalaException {
    AuthzOk("describe database functional_seq_snap");

    // Database doesn't exist.
    AuthzError("describe database nodb",
        "User '%s' does not have privileges to access: nodb");
    // Insufficient privileges on db.
    AuthzError("describe database functional_rc",
        "User '%s' does not have privileges to access: functional_rc");
  }

  @Test
  public void TestDescribe() throws ImpalaException {
    AuthzOk("describe functional.alltypesagg");
    AuthzOk("describe functional.alltypes");
    AuthzOk("describe functional.complex_view");

    // Unqualified table name.
    AuthzError("describe alltypes",
        "User '%s' does not have privileges to access: default.alltypes");
    // Database doesn't exist.
    AuthzError("describe nodb.alltypes",
        "User '%s' does not have privileges to access: nodb.alltypes");
    // User has column level privileges
    AuthzOk("describe functional.alltypestiny");
    // User has column level privileges on a subset of table columns
    AuthzOk("describe functional.alltypessmall");
    // User has column level privileges on described column.
    AuthzOk("describe functional.allcomplextypes.int_struct_col");
    // User has column level privileges on another column in table.
    AuthzOk("describe functional.allcomplextypes.complex_struct_col");
    // User has table level privileges without column level.
    AuthzOk("describe functional_parquet.allcomplextypes.complex_struct_col");
    // Insufficient privileges on table.
    AuthzError("describe formatted functional.alltypestiny",
        "User '%s' does not have privileges to access: functional.alltypestiny");
    // Insufficient privileges on table for nested column.
    AuthzError("describe functional.complextypestbl.nested_struct",
        "User '%s' does not have privileges to access: functional.complextypestbl");
    // Insufficient privileges on view.
    AuthzError("describe functional.alltypes_view",
        "User '%s' does not have privileges to access: functional.alltypes_view");
    // Insufficient privileges on db.
    AuthzError("describe functional_rc.alltypes",
        "User '%s' does not have privileges to access: functional_rc.alltypes");
  }

  @Test
  public void TestLoad() throws ImpalaException {
    // User has permission on table and URI.
    AuthzOk("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem'" +
        " into table functional.alltypes partition(month=10, year=2009)");

    // User does not have permission on table.
    AuthzError("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem'" +
        " into table functional.alltypesagg",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.alltypesagg");

    // User only has column-level privileges on the table
    AuthzError("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem'" +
        " into table functional.alltypestiny",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.alltypestiny");

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

    // Trying to LOAD a view does not reveal privileged information.
    AuthzError("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem'" +
        " into table functional.alltypes_view",
        "User '%s' does not have privileges to execute 'INSERT' on: " +
        "functional.alltypes_view");
  }

  @Test
  public void TestShowPermissions() throws ImpalaException {
    AuthzOk("show tables in functional");
    AuthzOk("show tables in functional_avro"); // Database with only column privileges.
    AuthzOk("show databases");
    AuthzOk("show tables in _impala_builtins");
    AuthzOk("show functions in _impala_builtins");

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

    // Show partitions and show table/column stats.
    String[] statsQuals = new String[] { "partitions", "table stats", "column stats" };
    for (String qual: statsQuals) {
      AuthzOk(String.format("show %s functional.alltypesagg", qual));
      AuthzOk(String.format("show %s functional.alltypes", qual));
      // User does not have access to db/table.
      AuthzError(String.format("show %s nodb.tbl", qual),
          "User '%s' does not have privileges to access: nodb.tbl");
      AuthzError(String.format("show %s functional.badtbl", qual),
          "User '%s' does not have privileges to access: functional.badtbl");
      AuthzError(String.format("show %s functional_rc.alltypes", qual),
          "User '%s' does not have privileges to access: functional_rc.alltypes");
      // User only has column-level privileges
      AuthzError(String.format("show %s functional.alltypestiny", qual),
          "User '%s' does not have privileges to access: functional.alltypestiny");
    }

    // Show files
    String[] partitions = new String[] { "", "partition(month=10, year=2010)" };
    for (String partition: partitions) {
      AuthzOk(String.format("show files in functional.alltypes %s", partition));
      // User does not have access to db/table.
      AuthzError(String.format("show files in nodb.tbl %s", partition),
          "User '%s' does not have privileges to access: nodb.tbl");
      AuthzError(String.format("show files in functional.badtbl %s", partition),
          "User '%s' does not have privileges to access: functional.badtbl");
      AuthzError(String.format("show files in functional_rc.alltypes %s", partition),
          "User '%s' does not have privileges to access: functional_rc.alltypes");
      // User only has column-level privileges.
      AuthzError(String.format("show files in functional.alltypestiny %s", partition),
          "User '%s' does not have privileges to access: functional.alltypestiny");
    }
  }

  @Test
  public void TestShowDbResultsFiltered() throws ImpalaException {
    // These are the only dbs that should show up because they are the only
    // dbs the user has any permissions on.
    List<String> expectedDbs = Lists.newArrayList("default", "functional",
        "functional_avro", "functional_parquet", "functional_seq_snap", "tpcds", "tpch");

    List<Db> dbs = fe_.getDbs(PatternMatcher.createHivePatternMatcher("*"), USER);
    assertEquals(expectedDbs, extractDbNames(dbs));

    dbs = fe_.getDbs(PatternMatcher.MATCHER_MATCH_ALL, USER);
    assertEquals(expectedDbs, extractDbNames(dbs));

    dbs = fe_.getDbs(PatternMatcher.createHivePatternMatcher(null), USER);
    assertEquals(expectedDbs, extractDbNames(dbs));

    dbs = fe_.getDbs(PatternMatcher.MATCHER_MATCH_NONE, USER);
    assertEquals(Collections.emptyList(), extractDbNames(dbs));

    dbs = fe_.getDbs(PatternMatcher.createHivePatternMatcher(""), USER);
    assertEquals(Collections.emptyList(), extractDbNames(dbs));

    dbs = fe_.getDbs(PatternMatcher.createHivePatternMatcher("functional_rc"), USER);
    assertEquals(Collections.emptyList(), extractDbNames(dbs));

    dbs = fe_.getDbs(PatternMatcher.createHivePatternMatcher("tp*|de*"), USER);
    expectedDbs = Lists.newArrayList("default", "tpcds", "tpch");
    assertEquals(expectedDbs, extractDbNames(dbs));
  }

  private List<String> extractDbNames(List<Db> dbs) {
    List<String> names = Lists.newArrayListWithCapacity(dbs.size());
    for (Db db: dbs) names.add(db.getName());
    return names;
  }

  @Test
  public void TestShowTableResultsFiltered() throws ImpalaException {
    List<String> tables = fe_.getTableNames("functional",
        PatternMatcher.createHivePatternMatcher("*"), USER);
    Assert.assertEquals(FUNCTIONAL_VISIBLE_TABLES, tables);

    tables = fe_.getTableNames("functional",
        PatternMatcher.MATCHER_MATCH_ALL, USER);
    Assert.assertEquals(FUNCTIONAL_VISIBLE_TABLES, tables);

    tables = fe_.getTableNames("functional",
        PatternMatcher.createHivePatternMatcher(null), USER);
    Assert.assertEquals(FUNCTIONAL_VISIBLE_TABLES, tables);

    tables = fe_.getTableNames("functional",
        PatternMatcher.MATCHER_MATCH_NONE, USER);
    Assert.assertEquals(Collections.emptyList(), tables);

    tables = fe_.getTableNames("functional",
        PatternMatcher.createHivePatternMatcher(""), USER);
    Assert.assertEquals(Collections.emptyList(), tables);

    tables = fe_.getTableNames("functional",
        PatternMatcher.createHivePatternMatcher("alltypes*|view_view"), USER);
    List<String> expectedTables = Lists.newArrayList(
        "alltypes", "alltypesagg", "alltypessmall", "alltypestiny", "view_view");
    Assert.assertEquals(expectedTables, tables);
  }

  @Test
  public void TestShowCreateTable() throws ImpalaException {
    AuthzOk("show create table functional.alltypesagg");
    AuthzOk("show create table functional.alltypes");
    // Have permissions on view and underlying table.
    AuthzOk("show create table functional_seq_snap.alltypes_view");

    // Unqualified table name.
    AuthzError("show create table alltypes",
        "User '%s' does not have privileges to access: default.alltypes");
    // Database doesn't exist.
    AuthzError("show create table nodb.alltypes",
        "User '%s' does not have privileges to access: nodb.alltypes");
    // Insufficient privileges on table.
    AuthzError("show create table functional.alltypestiny",
        "User '%s' does not have privileges to access: functional.alltypestiny");
    // Insufficient privileges on db.
    AuthzError("show create table functional_rc.alltypes",
        "User '%s' does not have privileges to access: functional_rc.alltypes");
    // User has column-level privileges on table
    AuthzError("show create table functional.alltypestiny",
        "User '%s' does not have privileges to access: functional.alltypestiny");

    // Cannot show SQL if user doesn't have permissions on underlying table.
    AuthzError("show create table functional.complex_view",
        "User '%s' does not have privileges to see the definition of view " +
        "'functional.complex_view'.");
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
    TResultSet resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(FUNCTIONAL_VISIBLE_TABLES.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(FUNCTIONAL_VISIBLE_TABLES.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    // Pattern "" and null is the same as "%" to match all
    req.get_tables_req.setTableName("");
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(FUNCTIONAL_VISIBLE_TABLES.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(FUNCTIONAL_VISIBLE_TABLES.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    req.get_tables_req.setTableName(null);
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(FUNCTIONAL_VISIBLE_TABLES.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(FUNCTIONAL_VISIBLE_TABLES.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    // Pattern ".*" matches all and "." is a wildcard that matches any single character
    req.get_tables_req.setTableName(".*");
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(FUNCTIONAL_VISIBLE_TABLES.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(FUNCTIONAL_VISIBLE_TABLES.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    req.get_tables_req.setTableName("alltypesag.");
    resp = fe_.execHiveServer2MetadataOp(req);
    List<String> expectedTblNames = Lists.newArrayList("alltypesagg");
    assertEquals(expectedTblNames.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedTblNames.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    // "_" is a wildcard for a single character
    req.get_tables_req.setTableName("alltypesag_");
    resp = fe_.execHiveServer2MetadataOp(req);
    expectedTblNames = Lists.newArrayList("alltypesagg");
    assertEquals(expectedTblNames.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedTblNames.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }

    // Get all tables of tpcds
    final int numTpcdsTables = 24;
    req.get_tables_req.setSchemaName("tpcds");
    req.get_tables_req.setTableName("%");
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(numTpcdsTables, resp.rows.size());
  }

  @Test
  public void TestHs2GetSchema() throws ImpalaException {
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.setSession(createSessionState("default", USER));
    req.opcode = TMetadataOpcode.GET_SCHEMAS;
    req.get_schemas_req = new TGetSchemasReq();
    // Get all schema (databases).
    req.get_schemas_req.setSchemaName("%");
    TResultSet resp = fe_.execHiveServer2MetadataOp(req);
    List<String> expectedDbs = Lists.newArrayList("default", "functional",
        "functional_avro", "functional_parquet", "functional_seq_snap", "tpcds", "tpch");
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    // Pattern "" and null is the same as "%" to match all
    req.get_schemas_req.setSchemaName("");
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    req.get_schemas_req.setSchemaName(null);
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    // Pattern ".*" matches all and "." is a wildcard that matches any single character
    req.get_schemas_req.setSchemaName(".*");
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    req.get_schemas_req.setSchemaName("defaul.");
    resp = fe_.execHiveServer2MetadataOp(req);
    expectedDbs = Lists.newArrayList("default");
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    // "_" is a wildcard that matches any single character
    req.get_schemas_req.setSchemaName("defaul_");
    resp = fe_.execHiveServer2MetadataOp(req);
    expectedDbs = Lists.newArrayList("default");
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
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
    TResultSet resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(1, resp.rows.size());

    // User does not have permission to access the table, no results should be returned.
    req.get_columns_req.setTableName("alltypesnopart");
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(0, resp.rows.size());

    // User does not have permission to access db or table, no results should be
    // returned.
    req.get_columns_req.setSchemaName("functional_seq_gzip");
    req.get_columns_req.setTableName("alltypes");
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(0, resp.rows.size());

    // User has SELECT privileges on all table columns but no table-level privileges.
    req.get_columns_req.setSchemaName("functional");
    req.get_columns_req.setTableName("alltypestiny");
    req.get_columns_req.setColumnName("%");
    resp = fe_.execHiveServer2MetadataOp(req);
    assertEquals(13, resp.rows.size());

    // User has SELECT privileges on some columns but no table-level privileges.
    req.get_columns_req.setSchemaName("functional");
    req.get_columns_req.setTableName("alltypessmall");
    req.get_columns_req.setColumnName("%");
    resp = fe_.execHiveServer2MetadataOp(req);
    String[] expectedColumnNames = {"id", "int_col", "year"};
    assertEquals(expectedColumnNames.length, resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedColumnNames[i],
          resp.rows.get(i).colVals.get(3).string_val.toLowerCase());
    }
  }

  @Test
  public void TestShortUsernameUsed() throws Exception {
    // Different long variations of the same username.
    List<User> users = Lists.newArrayList(
        new User(USER.getName() + "/abc.host.com@"),
        new User(USER.getName() + "/abc.host.com@REAL.COM"),
        new User(USER.getName() + "@REAL.COM"));
    for (User user: users) {
      AnalysisContext ctx = createAnalysisCtx(ctx_.authzConfig, user.getName());

      // Can select from table that user has privileges on.
      AuthzOk(ctx, "select * from functional.alltypesagg");

      // Unqualified table name.
      AuthzError(ctx, "select * from alltypes",
          "User '%s' does not have privileges to execute 'SELECT' on: default.alltypes");
    }
    // If the first character is '/', the short username should be the same as
    // the full username.
    assertEquals("/" + USER.getName(), new User("/" + USER.getName()).getShortName());
  }

  @Test
  public void TestShortUsernameWithAuthToLocal() throws ImpalaException {
    // We load the auth_to_local configs from core-site.xml in the test mode even if
    // kerberos is disabled. We use the following configuration for tests
    //   <property>
    //     <name>hadoop.security.auth_to_local</name>
    //     <value>RULE:[2:$1@$0](authtest@REALM.COM)s/(.*)@REALM.COM/auth_to_local_user/
    //       RULE:[1:$1]
    //       RULE:[2:$1]
    //       DEFAULT
    //     </value>
    //   </property>
    AuthorizationConfig authzConfig = new AuthorizationConfig("server1",
        AUTHZ_POLICY_FILE, "",
        LocalGroupResourceAuthorizationProvider.class.getName());
    ImpaladCatalog catalog = new ImpaladTestCatalog(authzConfig);

    // This test relies on the auth_to_local rule -
    // "RULE:[2:$1@$0](authtest@REALM.COM)s/(.*)@REALM.COM/auth_to_local_user/"
    // which converts any principal of type authtest/<hostname>@REALM.COM to user
    // auth_to_local_user. We have a sentry privilege in place that grants 'SELECT'
    // privilege on tpcds.* to user auth_to_local_user. To test the integration, we try
    // running the query with authtest/hostname@REALM.COM user which is converted to user
    // 'auth_to_local_user' and the authz should pass.
    User.setRulesForTesting(
        new Configuration().get(HADOOP_SECURITY_AUTH_TO_LOCAL, "DEFAULT"));
    User user = new User("authtest/hostname@REALM.COM");
    AnalysisContext ctx = createAnalysisCtx(authzConfig, user.getName());
    Frontend fe = new Frontend(authzConfig, catalog);

    // Can select from table that user has privileges on.
    AuthzOk(fe, ctx, "select * from tpcds.customer");
    // Does not have privileges to execute a query
    AuthzError(fe, ctx, "select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: functional.alltypes");

    // Unit tests for User#getShortName()
    // Different auth_to_local rules to apply on the username.
    List<String> rules = Lists.newArrayList(
        // Expects user@REALM and returns 'usera' (appends a in the end)
        "RULE:[1:$1@$0](.*@REALM.COM)s/(.*)@REALM.COM/$1a/g",
        // Same as above but expects user/host@REALM
        "RULE:[2:$1@$0](.*@REALM.COM)s/(.*)@REALM.COM/$1a/g");

    // Map from the user to the expected getShortName() output after applying
    // the corresponding rule from 'rules' list.
    List<Map.Entry<User, String>> users = Lists.newArrayList(
        Maps.immutableEntry(new User(USER.getName() + "@REALM.COM"), USER.getName()
            + "a"),
        Maps.immutableEntry(new User(USER.getName() + "/abc.host.com@REALM.COM"),
            USER.getName() + "a"));

    for (int idx = 0; idx < users.size(); ++idx) {
      Map.Entry<User, String> userObj = users.get(idx);
      assertEquals(userObj.getKey().getShortNameForTesting(rules.get(idx)),
          userObj.getValue());
    }

    // Test malformed rules. RuleParser throws an IllegalArgumentException.
    String malformedRule = "{((()";
    try {
      user.getShortNameForTesting(malformedRule);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid rule"));
      return;
    }
    Assert.fail("No exception caught while using getShortName() on a malformed rule");
  }

  @Test
  public void TestFunction() throws Exception {
    // First try with the less privileged user.
    AnalysisContext ctx = createAnalysisCtx(ctx_.authzConfig, USER.getName());
    AuthzError(ctx, "show functions",
        "User '%s' does not have privileges to access: default");
    AuthzOk(ctx, "show functions in tpch");

    AuthzError(ctx, "create function f() returns int location " +
        "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'",
        "User '%s' does not have privileges to CREATE/DROP functions.");

    AuthzError(ctx, "create function tpch.f() returns int location " +
        "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'",
        "User '%s' does not have privileges to CREATE/DROP functions.");

    AuthzError(ctx, "create function notdb.f() returns int location " +
        "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'",
        "User '%s' does not have privileges to CREATE/DROP functions.");

    AuthzError(ctx, "drop function if exists f()",
        "User '%s' does not have privileges to CREATE/DROP functions.");

    AuthzError(ctx, "drop function notdb.f()",
        "User '%s' does not have privileges to CREATE/DROP functions.");

    // TODO: Add test support for dynamically changing privileges for
    // file-based policy.
    if (ctx_.authzConfig.isFileBasedPolicy()) return;

    // Admin should be able to do everything
    SentryPolicyService sentryService =
        new SentryPolicyService(ctx_.authzConfig.getSentryConfig());
    try {
      sentryService.grantRoleToGroup(USER, "admin", USER.getName());
      ctx_.catalog.reset();

      AuthzOk("show functions");
      AuthzOk("show functions in tpch");

      AuthzOk("create function f() returns int location " +
          "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'");
      AuthzOk("create function tpch.f() returns int location " +
          "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'");
      AuthzOk("drop function if exists f()");

      // Can't add function to system db
      AuthzError("create function _impala_builtins.f() returns int location " +
          "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'",
          "Cannot modify system database.");
      AuthzError("drop function if exists pi()",
          "Cannot modify system database.");

      // Add default.f(), tpch.f()
      ctx_.catalog.addFunction(ScalarFunction.createForTesting("default", "f",
          new ArrayList<Type>(), Type.INT, "/dummy", "dummy.class", null,
          null, TFunctionBinaryType.NATIVE));
      ctx_.catalog.addFunction(ScalarFunction.createForTesting("tpch", "f",
          new ArrayList<Type>(), Type.INT, "/dummy", "dummy.class", null,
          null, TFunctionBinaryType.NATIVE));

      AuthzOk("drop function tpch.f()");
    } finally {
      sentryService.revokeRoleFromGroup(USER, "admin", USER.getName());
      ctx_.catalog.reset();

      AuthzError(ctx, "create function tpch.f() returns int location " +
          "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'",
          "User '%s' does not have privileges to CREATE/DROP functions.");

      // Couldn't create tpch.f() but can run it.
      AuthzOk("select tpch.f()");

      //Other tests don't expect tpch to contain functions
      //Specifically, if these functions are not cleaned up, TestDropDatabase() will fail
      ctx_.catalog.removeFunction(ScalarFunction.createForTesting("default", "f",
          new ArrayList<Type>(), Type.INT, "/dummy", "dummy.class", null,
          null, TFunctionBinaryType.NATIVE));
      ctx_.catalog.removeFunction(ScalarFunction.createForTesting("tpch", "f",
          new ArrayList<Type>(), Type.INT, "/dummy", "dummy.class", null,
          null, TFunctionBinaryType.NATIVE));
    }
  }

  @Test
  public void TestServerNameAuthorized() throws ImpalaException {
    if (ctx_.authzConfig.isFileBasedPolicy()) {
      // Authorization config that has a different server name from policy file.
      TestWithIncorrectConfig(AuthorizationConfig.createHadoopGroupAuthConfig(
          "differentServerName", AUTHZ_POLICY_FILE, ""),
          new User(System.getProperty("user.name")));
    } // TODO: Test using policy server.
  }

  @Test
  public void TestNoPermissionsWhenPolicyFileDoesNotExist() throws ImpalaException {
    // Test doesn't make sense except for file based policies.
    if (!ctx_.authzConfig.isFileBasedPolicy()) return;

    // Validate a non-existent policy file.
    // Use a HadoopGroupProvider in this case so the user -> group mappings can still be
    // resolved in the absence of the policy file.
    TestWithIncorrectConfig(AuthorizationConfig.createHadoopGroupAuthConfig("server1",
        AUTHZ_POLICY_FILE + "_does_not_exist", ""),
        new User(System.getProperty("user.name")));
  }

  @Test
  public void TestConfigValidation() throws InternalException {
    String sentryConfig = ctx_.authzConfig.getSentryConfig().getConfigFile();
    // Valid configs pass validation.
    AuthorizationConfig config = AuthorizationConfig.createHadoopGroupAuthConfig(
        "server1", AUTHZ_POLICY_FILE, sentryConfig);
    config.validateConfig();
    Assert.assertTrue(config.isEnabled());
    Assert.assertTrue(config.isFileBasedPolicy());

    config = AuthorizationConfig.createHadoopGroupAuthConfig("server1", null,
        sentryConfig);
    config.validateConfig();
    Assert.assertTrue(config.isEnabled());
    Assert.assertTrue(!config.isFileBasedPolicy());

    // Invalid configs
    // No sentry configuration file.
    config = AuthorizationConfig.createHadoopGroupAuthConfig(
        "server1", AUTHZ_POLICY_FILE, null);
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "A valid path to a sentry-site.xml config " +
          "file must be set using --sentry_config to enable authorization.");
    }

    // Empty / null server name.
    config = AuthorizationConfig.createHadoopGroupAuthConfig(
        "", AUTHZ_POLICY_FILE, sentryConfig);
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(),
          "Authorization is enabled but the server name is null or empty. Set the " +
          "server name using the impalad --server_name flag.");
    }
    config = AuthorizationConfig.createHadoopGroupAuthConfig(null, AUTHZ_POLICY_FILE,
        sentryConfig);
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(),
          "Authorization is enabled but the server name is null or empty. Set the " +
          "server name using the impalad --server_name flag.");
    }

    // Sentry config file does not exist.
    config = AuthorizationConfig.createHadoopGroupAuthConfig("server1", "",
        "/path/does/not/exist.xml");
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
      fail("Expected configuration to fail.");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(),
          "Sentry configuration file does not exist: \"/path/does/not/exist.xml\"");
    }

    // Invalid ResourcePolicyProvider class name.
    config = new AuthorizationConfig("server1", AUTHZ_POLICY_FILE, "",
        "ClassDoesNotExist");
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(),
          "The authorization policy provider class 'ClassDoesNotExist' was not found.");
    }

    // Valid class name, but class is not derived from ResourcePolicyProvider
    config = new AuthorizationConfig("server1", AUTHZ_POLICY_FILE, "",
        this.getClass().getName());
    Assert.assertTrue(config.isEnabled());
    try {
      config.validateConfig();
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(), String.format("The authorization policy " +
          "provider class '%s' must be a subclass of '%s'.", this.getClass().getName(),
          ResourceAuthorizationProvider.class.getName()));
    }

    // Config validations skipped if authorization disabled
    config = new AuthorizationConfig("", "", "", "");
    Assert.assertFalse(config.isEnabled());
    config = new AuthorizationConfig(null, "", "", null);
    Assert.assertFalse(config.isEnabled());
    config = new AuthorizationConfig("", null, "", "");
    Assert.assertFalse(config.isEnabled());
    config = new AuthorizationConfig(null, null, null, null);
    Assert.assertFalse(config.isEnabled());
  }

  @Test
  public void TestLocalGroupPolicyProvider() throws ImpalaException {
    if (!ctx_.authzConfig.isFileBasedPolicy()) return;
    // Use an authorization configuration that uses the
    // LocalGroupResourceAuthorizationProvider.
    AuthorizationConfig authzConfig = new AuthorizationConfig("server1",
        AUTHZ_POLICY_FILE, "",
        LocalGroupResourceAuthorizationProvider.class.getName());
    ImpaladCatalog catalog = new ImpaladTestCatalog(authzConfig);

    // Create an analysis context + FE with the test user (as defined in the policy file)
    User user = new User("test_user");
    AnalysisContext ctx = createAnalysisCtx(authzConfig, user.getName());
    Frontend fe = new Frontend(authzConfig, catalog);

    // Can select from table that user has privileges on.
    AuthzOk(fe, ctx, "select * from functional.alltypesagg");
    // Does not have privileges to execute a query
    AuthzError(fe, ctx, "select * from functional.alltypes",
        "User '%s' does not have privileges to execute 'SELECT' on: functional.alltypes");

    // Verify with the admin user
    user = new User("admin_user");
    ctx = createAnalysisCtx(authzConfig, user.getName());
    fe = new Frontend(authzConfig, catalog);

    // Admin user should have privileges to do anything
    AuthzOk(fe, ctx, "select * from functional.alltypesagg");
    AuthzOk(fe, ctx, "select * from functional.alltypes");
    AuthzOk(fe, ctx, "invalidate metadata");
    AuthzOk(fe, ctx, "create external table tpch.kudu_tbl stored as kudu " +
        "TBLPROPERTIES ('kudu.master_addresses'='127.0.0.1', 'kudu.table_name'='tbl')");
  }

  private void TestWithIncorrectConfig(AuthorizationConfig authzConfig, User user)
      throws ImpalaException {
    Frontend fe = new Frontend(authzConfig, ctx_.catalog);
    AnalysisContext ctx = createAnalysisCtx(authzConfig, user.getName());
    AuthzError(fe, ctx, "select * from functional.alltypesagg",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypesagg");
    AuthzError(fe, ctx, "ALTER TABLE functional_seq_snap.alltypes ADD COLUMNS (c1 int)",
        "User '%s' does not have privileges to execute 'ALTER' on: " +
        "functional_seq_snap.alltypes");
    AuthzError(fe, ctx, "drop table tpch.lineitem",
        "User '%s' does not have privileges to execute 'DROP' on: tpch.lineitem");
    AuthzError(fe, ctx, "show tables in functional",
        "User '%s' does not have privileges to access: functional.*");
  }

  private void AuthzOk(String stmt) throws ImpalaException {
    AuthzOk(analysisContext_, stmt);
  }

  private void AuthzOk(AnalysisContext context, String stmt) throws ImpalaException {
    AuthzOk(fe_, context, stmt);
  }

  private void AuthzOk(Frontend fe, AnalysisContext context, String stmt)
      throws ImpalaException {
    parseAndAnalyze(stmt, context, fe);
  }

  /**
   * Verifies that a given statement fails authorization and the expected error
   * string matches.
   */
  private void AuthzError(String stmt, String expectedErrorString)
      throws ImpalaException {
    AuthzError(analysisContext_, stmt, expectedErrorString);
  }

  private void AuthzError(AnalysisContext ctx, String stmt, String expectedErrorString)
      throws ImpalaException {
    AuthzError(fe_, ctx, stmt, expectedErrorString);
  }

  private void AuthzError(Frontend fe, AnalysisContext ctx,
      String stmt, String expectedErrorString)
      throws ImpalaException {
    Preconditions.checkNotNull(expectedErrorString);
    try {
      parseAndAnalyze(stmt, ctx, fe);
    } catch (AuthorizationException e) {
      // Insert the username into the error.
      expectedErrorString = String.format(expectedErrorString, ctx.getUser());
      String errorString = e.getMessage();
      Assert.assertTrue(
          "got error:\n" + errorString + "\nexpected:\n" + expectedErrorString,
          errorString.startsWith(expectedErrorString));
      return;
    }
    fail("Stmt didn't result in authorization error: " + stmt);
  }

  private static TSessionState createSessionState(String defaultDb, User user) {
    return new TSessionState(null, null,
        defaultDb, user.getName(), new TNetworkAddress("", 0));
  }

  private SentryPolicyService createSentryService() {
    return new SentryPolicyService(ctx_.authzConfig.getSentryConfig());
  }
}
