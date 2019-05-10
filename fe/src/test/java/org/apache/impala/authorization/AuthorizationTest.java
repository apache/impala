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

package org.apache.impala.authorization;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.authorization.sentry.SentryAuthorizationConfig;
import org.apache.impala.authorization.sentry.SentryAuthorizationFactory;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.Role;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.testutil.TestSentryGroupMapper;
import org.apache.impala.testutil.TestSentryResourceAuthorizationProvider;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TMetadataOpcode;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.util.PatternMatcher;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuthorizationTest extends FrontendTestBase {
  public static final User USER = new User(System.getProperty("user.name"));
  // Tables in functional that the current user and 'test_user' have table- or
  // column-level SELECT or INSERT permission. I.e. that should be returned by
  // 'SHOW TABLES'.
  private static final List<String> FUNCTIONAL_VISIBLE_TABLES = Lists.newArrayList(
      "alltypes", "alltypesagg", "alltypeserror", "alltypessmall", "alltypestiny");

  private static final SentryAuthorizationConfig AUTHZ_CONFIG =
      SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1",
          System.getenv("IMPALA_HOME") + "/fe/src/test/resources/sentry-site.xml");
  private static final ImpaladTestCatalog AUTHZ_CATALOG =
      new ImpaladTestCatalog(new SentryAuthorizationFactory(AUTHZ_CONFIG));
  private static final AuthorizationFactory AUTHZ_FACTORY =
      new SentryAuthorizationFactory(AUTHZ_CONFIG);

  private static final Frontend AUTHZ_FE;

  static {
    try {
      AUTHZ_FE = new Frontend(AUTHZ_FACTORY, AUTHZ_CATALOG);
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  private final AnalysisContext authzCtx;

  public AuthorizationTest() throws ImpalaException {
    authzCtx = createAnalysisCtx(AUTHZ_FACTORY, USER.getName());
    setupImpalaCatalog(AUTHZ_CATALOG);
  }

  @BeforeClass
  public static void setUp() throws ImpalaException {
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUp() throws ImpalaException {
    RuntimeEnv.INSTANCE.reset();
  }

  private static void setupImpalaCatalog(ImpaladTestCatalog catalog)
      throws ImpalaException {
    // Create admin user
    String role_ = "admin";
    TPrivilege privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.SERVER,
        false);
    Role role = catalog.addRole(role_);
    addRolePrivilege(catalog, privilege, role);
    catalog.addRoleGrantGroup(role_, TestSentryGroupMapper.SERVER_ADMIN);

    // Create test users
    Set<String> groups = new HashSet<>(Arrays.asList(
        USER.getName(),
        TestSentryGroupMapper.AUTH_TO_LOCAL,
        TestSentryGroupMapper.TEST_USER));
      role_ = "testRole";
      role = catalog.addRole(role_);


    // Insert on functional.alltypes
    privilege = new TPrivilege(TPrivilegeLevel.INSERT, TPrivilegeScope.TABLE, false);
    privilege.setDb_name("functional");
    privilege.setTable_name("alltypes");
    addRolePrivilege(catalog, privilege, role);

    // All on functional.[alltypesagg, alltypeserror, alltypestiny]
    List<String> tables = Arrays.asList(
        "alltypesagg",
        "alltypeserror",
        "alltypestiny");
    for (String table : tables) {
      privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.TABLE, false);
      privilege.setDb_name("functional");
      privilege.setTable_name(table);
      addRolePrivilege(catalog, privilege, role);
    }

    // Columns [id, int_col, year] on functional.alltypessmall
    List<String> columns = Arrays.asList("id", "int_col", "year");
    for (String column : columns) {
      privilege = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.COLUMN, false);
      privilege.setDb_name("functional");
      privilege.setTable_name("alltypessmall");
      privilege.setColumn_name(column);
      addRolePrivilege(catalog, privilege, role);
    }

    // Select on tpcds
    privilege = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.DATABASE, false);
    privilege.setDb_name("tpcds");
    addRolePrivilege(catalog, privilege, role);

    // Select on tpch
    privilege = new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.DATABASE, false);
    privilege.setDb_name("tpch");
    addRolePrivilege(catalog, privilege, role);

    for (String group: groups) {
      catalog.addRoleGrantGroup(role_, group);
    }
  }

  private static void addRolePrivilege(ImpaladTestCatalog catalog, TPrivilege privilege,
      Role role) throws CatalogException {
    privilege.setServer_name("server1");
    privilege.setPrincipal_type(TPrincipalType.ROLE);
    privilege.setPrincipal_id(role.getId());
    catalog.addRolePrivilege(role.getName(), privilege);
  }

  @After
  public void TestTPCHCleanup() throws AuthorizationException, AnalysisException {
    // Failure to cleanup TPCH can cause:
    // TestDropDatabase(org.apache.impala.analysis.AuthorizationTest):
    // Cannot drop non-empty database: tpch
    if (catalog_.getDb("tpch").numFunctions() != 0) {
      fail("Failed to clean up functions in tpch.");
    }
  }

  @Test
  public void TestShowDbResultsFiltered() throws ImpalaException {
    // These are the only dbs that should show up because they are the only
    // dbs the user has any permissions on.
    List<String> expectedDbs = Lists.newArrayList("default", "functional", "tpcds",
        "tpch");

    List<? extends FeDb> dbs = AUTHZ_FE.getDbs(
        PatternMatcher.createHivePatternMatcher("*"), USER);
    assertEquals(expectedDbs, extractDbNames(dbs));

    dbs = AUTHZ_FE.getDbs(PatternMatcher.MATCHER_MATCH_ALL, USER);
    assertEquals(expectedDbs, extractDbNames(dbs));

    dbs = AUTHZ_FE.getDbs(PatternMatcher.createHivePatternMatcher(null), USER);
    assertEquals(expectedDbs, extractDbNames(dbs));

    dbs = AUTHZ_FE.getDbs(PatternMatcher.MATCHER_MATCH_NONE, USER);
    assertEquals(Collections.emptyList(), extractDbNames(dbs));

    dbs = AUTHZ_FE.getDbs(PatternMatcher.createHivePatternMatcher(""), USER);
    assertEquals(Collections.emptyList(), extractDbNames(dbs));

    dbs = AUTHZ_FE.getDbs(PatternMatcher.createHivePatternMatcher("functional_rc"), USER);
    assertEquals(Collections.emptyList(), extractDbNames(dbs));

    dbs = AUTHZ_FE.getDbs(PatternMatcher.createHivePatternMatcher("tp*|de*"), USER);
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
    List<String> tables = AUTHZ_FE.getTableNames("functional",
        PatternMatcher.createHivePatternMatcher("*"), USER);
    Assert.assertEquals(FUNCTIONAL_VISIBLE_TABLES, tables);

    tables = AUTHZ_FE.getTableNames("functional",
        PatternMatcher.MATCHER_MATCH_ALL, USER);
    Assert.assertEquals(FUNCTIONAL_VISIBLE_TABLES, tables);

    tables = AUTHZ_FE.getTableNames("functional",
        PatternMatcher.createHivePatternMatcher(null), USER);
    Assert.assertEquals(FUNCTIONAL_VISIBLE_TABLES, tables);

    tables = AUTHZ_FE.getTableNames("functional",
        PatternMatcher.MATCHER_MATCH_NONE, USER);
    Assert.assertEquals(Collections.emptyList(), tables);

    tables = AUTHZ_FE.getTableNames("functional",
        PatternMatcher.createHivePatternMatcher(""), USER);
    Assert.assertEquals(Collections.emptyList(), tables);

    tables = AUTHZ_FE.getTableNames("functional",
        PatternMatcher.createHivePatternMatcher("alltypes*"), USER);
    List<String> expectedTables = Lists.newArrayList(
        "alltypes", "alltypesagg", "alltypeserror", "alltypessmall", "alltypestiny");
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
    TResultSet resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(FUNCTIONAL_VISIBLE_TABLES.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(FUNCTIONAL_VISIBLE_TABLES.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    // Pattern "" and null is the same as "%" to match all
    req.get_tables_req.setTableName("");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(FUNCTIONAL_VISIBLE_TABLES.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(FUNCTIONAL_VISIBLE_TABLES.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    req.get_tables_req.setTableName(null);
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(FUNCTIONAL_VISIBLE_TABLES.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(FUNCTIONAL_VISIBLE_TABLES.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    // Pattern ".*" matches all and "." is a wildcard that matches any single character
    req.get_tables_req.setTableName(".*");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(FUNCTIONAL_VISIBLE_TABLES.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(FUNCTIONAL_VISIBLE_TABLES.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    req.get_tables_req.setTableName("alltypesag.");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    List<String> expectedTblNames = Lists.newArrayList("alltypesagg");
    assertEquals(expectedTblNames.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedTblNames.get(i),
          resp.rows.get(i).colVals.get(2).string_val.toLowerCase());
    }
    // "_" is a wildcard for a single character
    req.get_tables_req.setTableName("alltypesag_");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
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
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
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
    TResultSet resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    List<String> expectedDbs = Lists.newArrayList("default", "functional", "tpcds",
        "tpch");
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    // Pattern "" and null is the same as "%" to match all
    req.get_schemas_req.setSchemaName("");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    req.get_schemas_req.setSchemaName(null);
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    // Pattern ".*" matches all and "." is a wildcard that matches any single character
    req.get_schemas_req.setSchemaName(".*");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    req.get_schemas_req.setSchemaName("defaul.");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    expectedDbs = Lists.newArrayList("default");
    assertEquals(expectedDbs.size(), resp.rows.size());
    for (int i = 0; i < resp.rows.size(); ++i) {
      assertEquals(expectedDbs.get(i),
          resp.rows.get(i).colVals.get(0).string_val.toLowerCase());
    }
    // "_" is a wildcard that matches any single character
    req.get_schemas_req.setSchemaName("defaul_");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
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
    TResultSet resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(1, resp.rows.size());

    // User does not have permission to access the table, no results should be returned.
    req.get_columns_req.setTableName("alltypesnopart");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(0, resp.rows.size());

    // User does not have permission to access db or table, no results should be
    // returned.
    req.get_columns_req.setSchemaName("functional_seq_gzip");
    req.get_columns_req.setTableName("alltypes");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(0, resp.rows.size());

    // User has SELECT privileges on all table columns but no table-level privileges.
    req.get_columns_req.setSchemaName("functional");
    req.get_columns_req.setTableName("alltypestiny");
    req.get_columns_req.setColumnName("%");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
    assertEquals(13, resp.rows.size());

    // User has SELECT privileges on some columns but no table-level privileges.
    req.get_columns_req.setSchemaName("functional");
    req.get_columns_req.setTableName("alltypessmall");
    req.get_columns_req.setColumnName("%");
    resp = AUTHZ_FE.execHiveServer2MetadataOp(req);
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
      AnalysisContext ctx = createAnalysisCtx(
          new SentryAuthorizationFactory(AUTHZ_CONFIG), user.getName());

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
       AuthorizationTest.AUTHZ_CONFIG.getSentryConfig().getConfigFile(),
        TestSentryResourceAuthorizationProvider.class.getName());
    SentryAuthorizationFactory authzFactory = new SentryAuthorizationFactory(
        authzConfig);
    try (ImpaladTestCatalog catalog = new ImpaladTestCatalog(authzFactory)) {
      setupImpalaCatalog(catalog);
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
      AnalysisContext ctx = createAnalysisCtx(authzFactory, user.getName());
      Frontend fe = new Frontend(authzFactory, catalog);

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
  public void TestConfigValidation() throws InternalException {
    String sentryConfig = AUTHZ_CONFIG.getSentryConfig().getConfigFile();
    // Valid configs pass validation.
    SentryAuthorizationConfig config =
        SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1", sentryConfig);
    Assert.assertTrue(config.isEnabled());

    config = SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1",
        sentryConfig);
    Assert.assertTrue(config.isEnabled());

    // Invalid configs
    // No sentry configuration file.
    try {
      config = SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1", null);
      Assert.assertTrue(config.isEnabled());
    } catch (Exception e) {
      Assert.assertEquals("A valid path to a sentry-site.xml config " +
          "file must be set using --sentry_config to enable authorization.",
          e.getMessage());
    }

    // Empty / null server name.
    try {
      config = SentryAuthorizationConfig.createHadoopGroupAuthConfig("", sentryConfig);
      Assert.assertTrue(config.isEnabled());
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(
          "Authorization is enabled but the server name is null or empty. Set the " +
          "server name using the impalad --server_name flag.",
          e.getMessage());
    }
    try {
      config = SentryAuthorizationConfig.createHadoopGroupAuthConfig(null, sentryConfig);
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
      config = SentryAuthorizationConfig.createHadoopGroupAuthConfig("server1",
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
      config = new SentryAuthorizationConfig("server1", sentryConfig,
          "ClassDoesNotExist");
      Assert.assertTrue(config.isEnabled());
      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(
          "The authorization policy provider class 'ClassDoesNotExist' was not found.",
          e.getMessage());
    }

    // Valid class name, but class is not derived from ResourcePolicyProvider
    try {
      config = new SentryAuthorizationConfig("server1", sentryConfig,
          this.getClass().getName());
      Assert.assertTrue(config.isEnabled());      fail("Expected configuration to fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format("The authorization policy " +
          "provider class '%s' must be a subclass of '%s'.", this.getClass().getName(),
          ResourceAuthorizationProvider.class.getName()),
          e.getMessage());
    }

    // Config validations skipped if authorization disabled
    config = new SentryAuthorizationConfig("", "", "");
    Assert.assertFalse(config.isEnabled());
    config = new SentryAuthorizationConfig(null, "", null);
    Assert.assertFalse(config.isEnabled());
    config = new SentryAuthorizationConfig(null, null, null);
    Assert.assertFalse(config.isEnabled());
  }

  @Test
  public void TestLocalGroupPolicyProvider() throws ImpalaException {
    // Use an authorization configuration that uses the
    // CustomClusterResourceAuthorizationProvider.
    SentryAuthorizationConfig authzConfig = new SentryAuthorizationConfig("server1",
        AuthorizationTest.AUTHZ_CONFIG.getSentryConfig().getConfigFile(),
        TestSentryResourceAuthorizationProvider.class.getName());
    SentryAuthorizationFactory authzFactory = new SentryAuthorizationFactory(authzConfig);
    try (ImpaladTestCatalog catalog = new ImpaladTestCatalog(authzFactory)) {
      setupImpalaCatalog(catalog);
      // Create an analysis context + FE with the test user
      // (as defined in the policy file)
      User user = new User("test_user");
      AnalysisContext ctx = createAnalysisCtx(authzFactory, user.getName());
      Frontend fe = new Frontend(authzFactory, catalog);

      // Can select from table that user has privileges on.
      AuthzOk(fe, ctx, "select * from functional.alltypesagg");
      // Does not have privileges to execute a query
      AuthzError(fe, ctx, "select * from functional.alltypes",
          "User '%s' does not have privileges to execute 'SELECT' on: " +
              "functional.alltypes");

      // Verify with the admin user
      user = new User("admin_user");
      ctx = createAnalysisCtx(authzFactory, user.getName());
      fe = new Frontend(authzFactory, catalog);

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
    SentryAuthorizationFactory authzFactory = new SentryAuthorizationFactory(authzConfig);
    Frontend fe = new Frontend(authzFactory, AUTHZ_CATALOG);
    AnalysisContext ctx = createAnalysisCtx(authzFactory, user.getName());
    AuthzError(fe, ctx, "select * from functional.alltypesagg",
        "User '%s' does not have privileges to execute 'SELECT' on: " +
        "functional.alltypesagg");
    AuthzError(fe, ctx, "drop table tpch.lineitem",
        "User '%s' does not have privileges to execute 'DROP' on: tpch.lineitem");
    AuthzError(fe, ctx, "show tables in functional",
        "User '%s' does not have privileges to access: functional.*");
  }

  private void AuthzOk(String stmt) throws ImpalaException {
    AuthzOk(authzCtx, stmt);
  }

  private void AuthzOk(AnalysisContext context, String stmt) throws ImpalaException {
    AuthzOk(AUTHZ_FE, context, stmt);
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
    AuthzError(authzCtx, stmt, expectedErrorString);
  }

  private void AuthzError(AnalysisContext ctx, String stmt, String expectedErrorString)
      throws ImpalaException {
    AuthzError(AUTHZ_FE, ctx, stmt, expectedErrorString);
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
