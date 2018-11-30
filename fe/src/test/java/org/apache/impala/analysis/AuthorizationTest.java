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
import org.apache.impala.authorization.User;
import org.apache.impala.authorization.sentry.SentryAuthorizationConfig;
import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.authorization.sentry.SentryPolicyService;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TMetadataOpcode;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.util.PatternMatcher;
import org.apache.sentry.core.model.db.AccessConstants;
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
  //   SELECT, REFRESH, DROP permissions on 'functional.alltypesagg'
  //   ALTER permissions on 'functional.alltypeserror'
  //   SELECT permissions on 'functional.complex_view'
  //   SELECT, REFRESH permissions on 'functional.view_view'
  //   ALTER, DROP permissions on 'functional.alltypes_view'
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
  //   REFRESH, INSERT, CREATE, ALTER, DROP permissions on 'functional_text_lzo' database
  public final static String AUTHZ_POLICY_FILE = "/test-warehouse/authz-policy.ini";
  public final static User USER = new User(System.getProperty("user.name"));

  // Tables in functional that the current user and 'test_user' have table- or
  // column-level SELECT or INSERT permission. I.e. that should be returned by
  // 'SHOW TABLES'.
  private static final List<String> FUNCTIONAL_VISIBLE_TABLES = Lists.newArrayList(
      "allcomplextypes", "alltypes", "alltypes_view", "alltypesagg", "alltypeserror",
      "alltypessmall", "alltypestiny", "complex_view", "view_view");

  /**
   * Test context whose instances are used to parameterize this test.
   */
  private static class TestContext {
    public final SentryAuthorizationConfig authzConfig;
    public final ImpaladTestCatalog catalog;

    public TestContext(SentryAuthorizationConfig authzConfig,
        ImpaladTestCatalog catalog) {
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
  public static Collection<TestContext> testVectors() { return testCtxs_; }

  /**
   * Create test contexts used for parameterizing this test. We create these statically
   * so that we do not have to re-create and initialize the auth policy and the catalog
   * for every test. Reloading the policy and the table metadata is very expensive
   * relative to the work done in the tests.
   */
  static {
    testCtxs_ = new ArrayList<>();
    // Create and init file based auth config.
    SentryAuthorizationConfig filePolicyAuthzConfig = createPolicyFileAuthzConfig();
    ImpaladTestCatalog filePolicyCatalog = new ImpaladTestCatalog(filePolicyAuthzConfig);
    testCtxs_.add(new TestContext(filePolicyAuthzConfig, filePolicyCatalog));

    // Create and init sentry service based auth config.
    SentryAuthorizationConfig sentryServiceAuthzConfig;
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

  public static SentryAuthorizationConfig createPolicyFileAuthzConfig() {
    SentryAuthorizationConfig result =
        SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1",
            AUTHZ_POLICY_FILE,
            System.getenv("IMPALA_HOME") + "/fe/src/test/resources/sentry-site.xml");
    return result;
  }

  public static SentryAuthorizationConfig createSentryServiceAuthzConfig()
      throws ImpalaException {
    SentryAuthorizationConfig result =
        SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1", null,
        System.getenv("IMPALA_HOME") + "/fe/src/test/resources/sentry-site.xml");
    setupSentryService(result);
    return result;
  }

  private static void setupSentryService(SentryAuthorizationConfig authzConfig)
      throws ImpalaException {
    SentryPolicyService sentryService = new SentryPolicyService(
        authzConfig.getSentryConfig());
    // Server admin. Don't grant to any groups, that is done within
    // the test cases.
    String roleName = "admin";
    sentryService.createRole(USER, roleName, true);

    TPrivilege privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.SERVER,
        false);
    privilege.setServer_name("server1");
    sentryService.grantRolePrivilege(USER, roleName, privilege);
    sentryService.revokeRoleFromGroup(USER, "admin", USER.getName());

    // insert functional alltypes
    roleName = "insert_functional_alltypes";
    roleName = roleName.toLowerCase();
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.INSERT, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypes");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // insert_parquet
    roleName = "insert_parquet";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.INSERT, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_parquet");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // refresh_functional_text_lzo
    roleName = "refresh_functional_text_lzo";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.REFRESH, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_text_lzo");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // refresh_functional_alltypesagg
    roleName = "refresh_functional_alltypesagg";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.REFRESH, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypesagg");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // drop_functional_alltypesagg
    roleName = "drop_functional_alltypesagg";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.DROP, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypesagg");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // refresh_functional_view_view
    roleName = "refresh_functional_view_view";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.REFRESH, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("view_view");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // drop_functional_alltypes_view
    roleName = "drop_functional_alltypes_view";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.DROP, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypes_view");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // insert_functional_text_lzo
    roleName = "insert_functional_text_lzo";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.INSERT, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_text_lzo");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // create_functional_text_lzo
    roleName = "create_functional_text_lzo";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.CREATE, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_text_lzo");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // alter_functional_text_lzo
    roleName = "alter_functional_text_lzo";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.ALTER, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_text_lzo");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // drop_functional_text_lzo
    roleName = "drop_functional_text_lzo";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.DROP, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_text_lzo");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // alter_functional_alltypeserror
    roleName = "alter_functional_alltypeserror";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.ALTER, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypeserror");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // alter_functional_alltypes_view
    roleName = "alter_functional_alltypes_view";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.ALTER, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypes_view");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // all newdb w/ all on URI
    roleName = "all_newdb";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("newdb");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.URI, false);
    privilege.setServer_name("server1");
    privilege.setUri("hdfs://localhost:20500/test-warehouse/new_table");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.URI, false);
    privilege.setServer_name("server1");
    privilege.setUri("hdfs://localhost:20500/test-warehouse/UPPER_CASE");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.URI, false);
    privilege.setServer_name("server1");
    privilege.setUri("hdfs://localhost:20500/test-warehouse/libTestUdfs.so");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // all tpch
    roleName = "all_tpch";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());
    privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.URI, false);
    privilege.setServer_name("server1");
    privilege.setUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("tpch");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select tpcds
    roleName = "select_tpcds";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("tpcds");
    privilege.setTable_name(AccessConstants.ALL);
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select_functional_alltypesagg
    roleName = "select_functional_alltypesagg";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypesagg");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select_functional_complex_view
    roleName = "select_functional_complex_view";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional");
    privilege.setTable_name("complex_view");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select_functional_view_view
    roleName = "select_functional_view_view";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    privilege = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
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

    privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_seq_snap");
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    // select_column_level_functional
    roleName = "select_column_level_functional";
    sentryService.createRole(USER, roleName, true);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());

    // select (id, int_col, year) on functional.alltypessmall
    List<TPrivilege> privileges = new ArrayList<>();
    for (String columnName: Arrays.asList("id", "int_col", "year")) {
      TPrivilege priv = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.COLUMN,
          false);
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
      TPrivilege priv = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.COLUMN,
          false);
      priv.setServer_name("server1");
      priv.setDb_name("functional");
      priv.setTable_name("allcomplextypes");
      priv.setColumn_name(columnName);
      privileges.add(priv);
    }
    sentryService.grantRolePrivileges(USER, roleName, privileges);
    privileges.clear();

    // table privileges on functional_parquet.allcomplextypes
    privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.TABLE, false);
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
      TPrivilege priv = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.COLUMN,
          false);
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

    privilege = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.COLUMN, false);
    privilege.setServer_name("server1");
    privilege.setDb_name("functional_avro");
    privilege.setTable_name("alltypessmall");
    privilege.setColumn_name("id");
    sentryService.grantRolePrivilege(USER, roleName, privilege);
  }

  @Test
  public void TestSentryService() throws ImpalaException {
    SentryPolicyService sentryService = new SentryPolicyService(
        ctx_.authzConfig.getSentryConfig());
    String roleName = "testRoleName";
    roleName = roleName.toLowerCase();

    sentryService.createRole(USER, roleName, true);
    String dbName = UUID.randomUUID().toString();
    TPrivilege privilege =
        new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.DATABASE, false);
    privilege.setServer_name("server1");
    privilege.setDb_name(dbName);
    sentryService.grantRoleToGroup(USER, roleName, USER.getName());
    sentryService.grantRolePrivilege(USER, roleName, privilege);

    for (int i = 0; i < 2; ++i) {
      privilege = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
      privilege.setServer_name("server1");
      privilege.setDb_name(dbName);
      privilege.setTable_name("test_tbl_" + String.valueOf(i));
      sentryService.grantRolePrivilege(USER, roleName, privilege);
    }

    List<TPrivilege> privileges = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      TPrivilege priv = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.COLUMN,
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
  public void TestShowDbResultsFiltered() throws ImpalaException {
    // These are the only dbs that should show up because they are the only
    // dbs the user has any permissions on.
    List<String> expectedDbs = Lists.newArrayList("default", "functional",
        "functional_avro", "functional_parquet", "functional_seq_snap",
        "functional_text_lzo", "tpcds", "tpch");

    List<? extends FeDb> dbs = fe_.getDbs(PatternMatcher.createHivePatternMatcher("*"), USER);
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

  private List<String> extractDbNames(List<? extends FeDb> dbs) {
    List<String> names = Lists.newArrayListWithCapacity(dbs.size());
    for (FeDb db: dbs) names.add(db.getName());
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
        "alltypes", "alltypes_view", "alltypesagg", "alltypeserror", "alltypessmall",
        "alltypestiny", "view_view");
    Assert.assertEquals(expectedTables, tables);
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
        "functional_avro", "functional_parquet", "functional_seq_snap",
        "functional_text_lzo", "tpcds", "tpch");
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
        // Historical note: Hadoop 2 accepts kerberos names missing a realm, but insists
        // on having a terminating '@' even when the default realm is intended. Hadoop 3
        // now has more normal name conventions, where to specify the default realm,
        // everything after and including the '@' character is omitted.
        new User(USER.getName() + "/abc.host.com"),
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
    SentryAuthorizationConfig authzConfig = new SentryAuthorizationConfig("server1",
        AUTHZ_POLICY_FILE, ctx_.authzConfig.getSentryConfig().getConfigFile(),
        LocalGroupResourceAuthorizationProvider.class.getName());
    try (ImpaladCatalog catalog = new ImpaladTestCatalog(authzConfig)) {
      // This test relies on the auth_to_local rule -
      // "RULE:[2:$1@$0](authtest@REALM.COM)s/(.*)@REALM.COM/auth_to_local_user/"
      // which converts any principal of type authtest/<hostname>@REALM.COM to user
      // auth_to_local_user. We have a sentry privilege in place that grants 'SELECT'
      // privilege on tpcds.* to user auth_to_local_user. To test the integration, we try
      // running the query with authtest/hostname@REALM.COM user which is converted to
      // user 'auth_to_local_user' and the authz should pass.
      User.setRulesForTesting(
          new Configuration().get(HADOOP_SECURITY_AUTH_TO_LOCAL, "DEFAULT"));
      User user = new User("authtest/hostname@REALM.COM");
      AnalysisContext ctx = createAnalysisCtx(authzConfig, user.getName());
      Frontend fe = new Frontend(authzConfig, catalog);

      // Can select from table that user has privileges on.
      AuthzOk(fe, ctx, "select * from tpcds.customer");
      // Does not have privileges to execute a query
      AuthzError(fe, ctx, "select * from functional.alltypes",
          "User '%s' does not have privileges to execute 'SELECT' on: " +
          "functional.alltypes");

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
  }

  @Test
  public void TestServerNameAuthorized() throws ImpalaException {
    if (ctx_.authzConfig.isFileBasedPolicy()) {
      // Authorization config that has a different server name from policy file.
      TestWithIncorrectConfig(SentryAuthorizationConfig.createHadoopGroupAuthConfig(
          "differentServerName", AUTHZ_POLICY_FILE,
          ctx_.authzConfig.getSentryConfig().getConfigFile()),
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
    TestWithIncorrectConfig(SentryAuthorizationConfig.createHadoopGroupAuthConfig(
        "server1", AUTHZ_POLICY_FILE + "_does_not_exist",
        ctx_.authzConfig.getSentryConfig().getConfigFile()),
        new User(System.getProperty("user.name")));
  }

  @Test
  public void TestConfigValidation() throws InternalException {
    String sentryConfig = ctx_.authzConfig.getSentryConfig().getConfigFile();
    // Valid configs pass validation.
    SentryAuthorizationConfig config =
        SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1",
            AUTHZ_POLICY_FILE, sentryConfig);
    Assert.assertTrue(config.isEnabled());
    Assert.assertTrue(config.isFileBasedPolicy());

    config = SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1", null,
        sentryConfig);
    Assert.assertTrue(config.isEnabled());
    Assert.assertTrue(!config.isFileBasedPolicy());

    // Invalid configs
    // No sentry configuration file.
    try {
      config = SentryAuthorizationConfig.createHadoopGroupAuthConfig(
          "server1", AUTHZ_POLICY_FILE, null);
      Assert.assertTrue(config.isEnabled());
    } catch (Exception e) {
      Assert.assertEquals("A valid path to a sentry-site.xml config " +
          "file must be set using --sentry_config to enable authorization.",
          e.getMessage());
    }

    // Empty / null server name.
    try {
      config = SentryAuthorizationConfig.createHadoopGroupAuthConfig(
          "", AUTHZ_POLICY_FILE, sentryConfig);
      Assert.assertTrue(config.isEnabled());
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(
          "Authorization is enabled but the server name is null or empty. Set the " +
          "server name using the impalad --server_name flag.",
          e.getMessage());
    }
    try {
      config = SentryAuthorizationConfig.createHadoopGroupAuthConfig(null,
          AUTHZ_POLICY_FILE, sentryConfig);
      Assert.assertTrue(config.isEnabled());
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(
          "Authorization is enabled but the server name is null or empty. Set the " +
          "server name using the impalad --server_name flag.",
          e.getMessage());
    }

    // Sentry config file does not exist.
    try {
      config = SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1", "",
          "/path/does/not/exist.xml");
      Assert.assertTrue(config.isEnabled());
      fail("Expected configuration to fail.");
    } catch (Exception e) {
      Assert.assertEquals(
          "Sentry configuration file does not exist: \"/path/does/not/exist.xml\"",
          e.getMessage());
    }

    // Invalid ResourcePolicyProvider class name.
    try {
      config = new SentryAuthorizationConfig("server1", AUTHZ_POLICY_FILE, sentryConfig,
          "ClassDoesNotExist");
      Assert.assertTrue(config.isEnabled());      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(
          "The authorization policy provider class 'ClassDoesNotExist' was not found.",
          e.getMessage());
    }

    // Valid class name, but class is not derived from ResourcePolicyProvider
    try {
      config = new SentryAuthorizationConfig("server1", AUTHZ_POLICY_FILE, sentryConfig,
          this.getClass().getName());
      Assert.assertTrue(config.isEnabled());      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format("The authorization policy " +
          "provider class '%s' must be a subclass of '%s'.", this.getClass().getName(),
          ResourceAuthorizationProvider.class.getName()),
          e.getMessage());
    }

    // Config validations skipped if authorization disabled
    config = new SentryAuthorizationConfig("", "", "", "");
    Assert.assertFalse(config.isEnabled());
    config = new SentryAuthorizationConfig(null, "", "", null);
    Assert.assertFalse(config.isEnabled());
    config = new SentryAuthorizationConfig("", null, "", "");
    Assert.assertFalse(config.isEnabled());
    config = new SentryAuthorizationConfig(null, null, null, null);
    Assert.assertFalse(config.isEnabled());
  }

  @Test
  public void TestLocalGroupPolicyProvider() throws ImpalaException {
    if (!ctx_.authzConfig.isFileBasedPolicy()) return;
    // Use an authorization configuration that uses the
    // LocalGroupResourceAuthorizationProvider.
    SentryAuthorizationConfig authzConfig = new SentryAuthorizationConfig("server1",
        AUTHZ_POLICY_FILE, ctx_.authzConfig.getSentryConfig().getConfigFile(),
        LocalGroupResourceAuthorizationProvider.class.getName());
    try (ImpaladCatalog catalog = new ImpaladTestCatalog(authzConfig)) {
      // Create an analysis context + FE with the test user
      // (as defined in the policy file)
      User user = new User("test_user");
      AnalysisContext ctx = createAnalysisCtx(authzConfig, user.getName());
      Frontend fe = new Frontend(authzConfig, catalog);

      // Can select from table that user has privileges on.
      AuthzOk(fe, ctx, "select * from functional.alltypesagg");
      // Does not have privileges to execute a query
      AuthzError(fe, ctx, "select * from functional.alltypes",
          "User '%s' does not have privileges to execute 'SELECT' on: " +
          "functional.alltypes");

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
}
