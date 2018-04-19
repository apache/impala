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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.AuthorizationException;
import org.apache.impala.catalog.Role;
import org.apache.impala.catalog.RolePrivilege;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.impala.util.SentryPolicyService;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthorizationTestV2 extends FrontendTestBase {
  private static final String SENTRY_SERVER = "server1";
  private final static User USER = new User(System.getProperty("user.name"));
  private final AnalysisContext analysisContext_;
  private final SentryPolicyService sentryService_;
  private final ImpaladTestCatalog authzCatalog_;
  private final Frontend authzFrontend_;

  public AuthorizationTestV2() {
    AuthorizationConfig authzConfig = AuthorizationConfig.createHadoopGroupAuthConfig(
        SENTRY_SERVER, null, System.getenv("IMPALA_HOME") +
        "/fe/src/test/resources/sentry-site.xml");
    authzConfig.validateConfig();
    analysisContext_ = createAnalysisCtx(authzConfig, USER.getName());
    authzCatalog_ = new ImpaladTestCatalog(authzConfig);
    authzFrontend_ = new Frontend(authzConfig, authzCatalog_);
    sentryService_ = new SentryPolicyService(authzConfig.getSentryConfig());
  }

  @BeforeClass
  public static void setUp() {
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUp() {
    RuntimeEnv.INSTANCE.reset();
  }

  @Before
  public void before() throws ImpalaException {
    // Remove existing roles in order to not interfere with these tests.
    for (TSentryRole role : sentryService_.listAllRoles(USER)) {
      authzCatalog_.removeRole(role.getRoleName());
    }
  }

  @Test
  public void testPrivilegeRequests() throws ImpalaException {
    // Select *
    Set<String> expectedPrivileges = Sets.newHashSet(
        "functional.alltypes",
        "functional.alltypes.id",
        "functional.alltypes.bool_col",
        "functional.alltypes.tinyint_col",
        "functional.alltypes.smallint_col",
        "functional.alltypes.int_col",
        "functional.alltypes.bigint_col",
        "functional.alltypes.float_col",
        "functional.alltypes.double_col",
        "functional.alltypes.date_string_col",
        "functional.alltypes.string_col",
        "functional.alltypes.timestamp_col",
        "functional.alltypes.year",
        "functional.alltypes.month"
    );
    verifyPrivilegeReqs("select * from functional.alltypes", expectedPrivileges);
    verifyPrivilegeReqs("select alltypes.* from functional.alltypes", expectedPrivileges);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "select * from alltypes",
        expectedPrivileges);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "select alltypes.* from alltypes", expectedPrivileges);
    verifyPrivilegeReqs("select a.* from functional.alltypes a", expectedPrivileges);

    // Select a specific column.
    expectedPrivileges = Sets.newHashSet(
        "functional.alltypes",
        "functional.alltypes.id"
    );
    verifyPrivilegeReqs("select id from functional.alltypes", expectedPrivileges);
    verifyPrivilegeReqs("select alltypes.id from functional.alltypes",
        expectedPrivileges);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "select alltypes.id from alltypes", expectedPrivileges);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "select id from alltypes",
        expectedPrivileges);
    verifyPrivilegeReqs("select alltypes.id from functional.alltypes",
        expectedPrivileges);
    verifyPrivilegeReqs("select a.id from functional.alltypes a", expectedPrivileges);
  }

  @Test
  public void testSelect() throws ImpalaException {
    // Select a specific column on a table.
    authorize("select id from functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "alltypes", "id", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional",
            "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onColumn("functional",
            "alltypes", "id", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Select a specific column on a view.
    // Column-level privileges on views are not currently supported.
    authorize("select id from functional.alltypes_view")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes_view"))
        .error(selectError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Constant select.
    authorize("select 1").ok();

    // Select on view and join table.
    authorize("select a.id from functional.view_view a " +
        "join functional.alltypesagg b ON (a.id = b.id)")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.ALL),
            onTable("functional", "alltypesagg", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.ALL),
            onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypesagg", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT))
        .error(selectError("functional.view_view"))
        .error(selectError("functional.view_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.view_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.view_view"), onTable("functional", "view_view",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)), onTable("functional",
            "alltypesagg", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Tests authorization after a statement has been rewritten (IMPALA-3915).
    authorize("select * from functional_seq_snap.subquery_view")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional_seq_snap", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional_seq_snap", TPrivilegeLevel.SELECT))
        .ok(onTable("functional_seq_snap", "subquery_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional_seq_snap", "subquery_view", TPrivilegeLevel.SELECT))
        .error(selectError("functional_seq_snap.subquery_view"))
        .error(selectError("functional_seq_snap.subquery_view"), onServer(
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional_seq_snap.subquery_view"),
            onDatabase("functional_seq_snap", allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional_seq_snap.subquery_view"),
            onTable("functional_seq_snap", "subquery_view", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Select without referencing a column.
    authorize("select 1 from functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Select from non-existent database.
    authorize("select 1 from nodb.alltypes")
        .error(selectError("nodb.alltypes"));

    // Select from non-existent table.
    authorize("select 1 from functional.notbl")
        .error(selectError("functional.notbl"));

    // Select with inline view.
    authorize("select a.* from (select * from functional.alltypes) a")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "alltypes", new String[]{"id", "bool_col",
            "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col",
            "double_col", "date_string_col", "string_col", "timestamp_col", "year",
            "month"}, TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onColumn("functional", "alltypes",
            new String[]{"id", "bool_col", "tinyint_col", "smallint_col", "int_col",
            "bigint_col", "float_col", "double_col", "date_string_col", "string_col",
            "timestamp_col", "year", "month"}, allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.SELECT)));

    // Select with columns referenced in function, where clause and group by.
    authorize("select count(id), int_col from functional.alltypes where id = 10 " +
        "group by id, int_col")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "alltypes", new String[]{"id", "int_col"},
            TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onColumn("functional", "alltypes",
            "id", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Select on tables with complex types.
    authorize("select a.int_struct_col.f1 from functional.allcomplextypes a " +
        "where a.id = 1")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "allcomplextypes",
            new String[]{"id", "int_struct_col"}, TPrivilegeLevel.SELECT))
        .error(selectError("functional.allcomplextypes"))
        .error(selectError("functional.allcomplextypes"), onServer(
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onTable("functional",
            "allcomplextypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onColumn("functional",
            "allcomplextypes", new String[]{"id", "int_struct_col"}, allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    authorize("select key, pos, item.f1, f2 from functional.allcomplextypes t, " +
        "t.struct_array_col, functional.allcomplextypes.int_map_col")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "allcomplextypes",
            new String[]{"struct_array_col", "int_map_col"}, TPrivilegeLevel.SELECT))
        .error(selectError("functional.allcomplextypes"))
        .error(selectError("functional.allcomplextypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onTable("functional",
            "allcomplextypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onColumn("functional",
            "allcomplextypes", new String[]{"struct_array_col", "int_map_col"},
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Select with cross join.
    authorize("select * from functional.alltypes a cross join " +
        "functional.alltypessmall b")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
            onTable("functional", "alltypessmall", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypessmall", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
            onTable("functional", "alltypessmall", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypessmall", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "alltypes", new String[]{"id", "bool_col",
            "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col",
            "double_col", "date_string_col", "string_col", "timestamp_col", "year",
            "month"}, TPrivilegeLevel.SELECT),
            onColumn("functional", "alltypessmall", new String[]{"id", "bool_col",
            "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col",
            "double_col", "date_string_col", "string_col", "timestamp_col", "year",
            "month"}, TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)), onTable("functional",
            "alltypessmall", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onColumn("functional", "alltypes",
            new String[]{"id", "bool_col", "tinyint_col", "smallint_col", "int_col",
            "bigint_col", "float_col", "double_col", "date_string_col", "string_col",
            "timestamp_col", "year", "month"}, allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.SELECT)), onColumn("functional", "alltypessmall",
            new String[]{"id", "bool_col", "tinyint_col", "smallint_col", "int_col",
            "bigint_col", "float_col", "double_col", "date_string_col", "string_col",
            "timestamp_col", "year", "month"}, allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.SELECT)));
  }

  private static String selectError(String object) {
    return "User '%s' does not have privileges to execute 'SELECT' on: " + object;
  }

  private static TPrivilegeLevel[] allExcept(TPrivilegeLevel... excludedPrivLevels) {
    HashSet<TPrivilegeLevel> excludedSet = Sets.newHashSet(excludedPrivLevels);
    List<TPrivilegeLevel> privLevels = new ArrayList<>();
    for (TPrivilegeLevel level : TPrivilegeLevel.values()) {
      if (!excludedSet.contains(level)) {
        privLevels.add(level);
      }
    }
    return privLevels.toArray(new TPrivilegeLevel[0]);
  }

  private class AuthzTest {
    private final AnalysisContext context_;
    private final String stmt_;
    private final String role_ = "authz_test_role";

    public AuthzTest(String stmt) {
      this(null, stmt);
    }

    public AuthzTest(AnalysisContext context, String stmt) {
      Preconditions.checkNotNull(stmt);
      context_ = context;
      stmt_ = stmt;
    }

    private void createRole(TPrivilege[]... privileges) throws ImpalaException {
      Role role = authzCatalog_.addRole(role_);
      authzCatalog_.addRoleGrantGroup(role_, USER.getName());
      for (TPrivilege[] privs : privileges) {
        for (TPrivilege privilege : privs) {
          privilege.setRole_id(role.getId());
          authzCatalog_.addRolePrivilege(role_, privilege);
        }
      }
    }

    private void dropRole() throws ImpalaException {
      authzCatalog_.removeRole(role_);
    }

    /**
     * This method runs with the specified privileges.
     *
     * A new temporary role will be created and assigned to the specified privileges
     * into the new role. The new role will be dropped once this method finishes.
     */
    public AuthzTest ok(TPrivilege[]... privileges) throws ImpalaException {
      try {
        createRole(privileges);
        if (context_ != null) {
          authzOk(context_, stmt_);
        } else {
          authzOk(stmt_);
        }
      } finally {
        dropRole();
      }
      return this;
    }

    /**
     * This method runs with the specified privileges.
     *
     * A new temporary role will be created and assigned to the specified privileges
     * into the new role. The new role will be dropped once this method finishes.
     */
    public AuthzTest error(String expectedError, TPrivilege[]... privileges)
        throws ImpalaException {
      try {
        createRole(privileges);
        if (context_ != null) {
          authzError(context_, stmt_, expectedError);
        } else {
          authzError(stmt_, expectedError);
        }
      } finally {
        dropRole();
      }
      return this;
    }
  }

  private AuthzTest authorize(String stmt) {
    return new AuthzTest(stmt);
  }

  private AuthzTest authorize(AnalysisContext ctx, String stmt) {
    return new AuthzTest(ctx, stmt);
  }

  private TPrivilege[] onServer(TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege("", levels[i], TPrivilegeScope.SERVER, false);
      privileges[i].setServer_name(SENTRY_SERVER);
      privileges[i].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
          privileges[i]));
    }
    return privileges;
  }

  private TPrivilege[] onDatabase(String db, TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege("", levels[i], TPrivilegeScope.DATABASE, false);
      privileges[i].setServer_name(SENTRY_SERVER);
      privileges[i].setDb_name(db);
      privileges[i].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
          privileges[i]));
    }
    return privileges;
  }

  private TPrivilege[] onTable(String db, String table, TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege("", levels[i], TPrivilegeScope.TABLE, false);
      privileges[i].setServer_name(SENTRY_SERVER);
      privileges[i].setDb_name(db);
      privileges[i].setTable_name(table);
      privileges[i].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
          privileges[i]));
    }
    return privileges;
  }

  private TPrivilege[] onColumn(String db, String table, String column,
      TPrivilegeLevel... levels) {
    return onColumn(db, table, new String[]{column}, levels);
  }

  private TPrivilege[] onColumn(String db, String table, String[] columns,
      TPrivilegeLevel... levels) {
    int size = columns.length * levels.length;
    TPrivilege[] privileges = new TPrivilege[size];
    int idx = 0;
    for (int i = 0; i < levels.length; i++) {
      for (String column : columns) {
        privileges[idx] = new TPrivilege("", levels[i], TPrivilegeScope.COLUMN, false);
        privileges[idx].setServer_name(SENTRY_SERVER);
        privileges[idx].setDb_name(db);
        privileges[idx].setTable_name(table);
        privileges[idx].setColumn_name(column);
        privileges[idx].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
            privileges[idx]));
        idx++;
      }
    }
    return privileges;
  }

  private TPrivilege[] onUri(String uri, TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege("", levels[i], TPrivilegeScope.URI, false);
      privileges[i].setServer_name(SENTRY_SERVER);
      privileges[i].setUri(uri);
      privileges[i].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
          privileges[i]));
    }
    return privileges;
  }

  private void authzOk(String stmt) throws ImpalaException {
    authzOk(analysisContext_, stmt);
  }

  private void authzOk(AnalysisContext context, String stmt) throws ImpalaException {
    authzOk(authzFrontend_, context, stmt);
  }

  private void authzOk(Frontend fe, AnalysisContext context, String stmt)
      throws ImpalaException {
    parseAndAnalyze(stmt, context, fe);
  }

  /**
   * Verifies that a given statement fails authorization and the expected error
   * string matches.
   */
  private void authzError(String stmt, String expectedError, Matcher matcher)
      throws ImpalaException {
    authzError(analysisContext_, stmt, expectedError, matcher);
  }

  private void authzError(String stmt, String expectedError)
      throws ImpalaException {
    authzError(analysisContext_, stmt, expectedError, startsWith());
  }

  private void authzError(AnalysisContext ctx, String stmt, String expectedError,
      Matcher matcher) throws ImpalaException {
    authzError(authzFrontend_, ctx, stmt, expectedError, matcher);
  }

  private void authzError(AnalysisContext ctx, String stmt, String expectedError)
      throws ImpalaException {
    authzError(authzFrontend_, ctx, stmt, expectedError, startsWith());
  }

  private interface Matcher {
    boolean match(String actual, String expected);
  }

  private static Matcher exact() {
    return new Matcher() {
      @Override
      public boolean match(String actual, String expected) {
        return actual.equals(expected);
      }
    };
  }

  private static Matcher startsWith() {
    return new Matcher() {
      @Override
      public boolean match(String actual, String expected) {
        return actual.startsWith(expected);
      }
    };
  }

  private void authzError(Frontend fe, AnalysisContext ctx,
      String stmt, String expectedErrorString, Matcher matcher)
      throws ImpalaException {
    Preconditions.checkNotNull(expectedErrorString);
    try {
      parseAndAnalyze(stmt, ctx, fe);
    } catch (AuthorizationException e) {
      // Insert the username into the error.
      expectedErrorString = String.format(expectedErrorString, ctx.getUser());
      String errorString = e.getMessage();
      assertTrue(
          "got error:\n" + errorString + "\nexpected:\n" + expectedErrorString,
          matcher.match(errorString, expectedErrorString));
      return;
    }
    fail("Stmt didn't result in authorization error: " + stmt);
  }


  private void verifyPrivilegeReqs(String stmt, Set<String> expectedPrivilegeNames)
      throws ImpalaException {
    verifyPrivilegeReqs(createAnalysisCtx(), stmt, expectedPrivilegeNames);
  }

  private void verifyPrivilegeReqs(AnalysisContext ctx, String stmt,
      Set<String> expectedPrivilegeNames) throws ImpalaException {
    AnalysisResult analysisResult = parseAndAnalyze(stmt, ctx, frontend_);
    ImmutableList<PrivilegeRequest> privilegeReqs =
        analysisResult.getAnalyzer().getPrivilegeReqs();

    assertEquals(expectedPrivilegeNames.size(), privilegeReqs.size());

    for (PrivilegeRequest privilegeReq : privilegeReqs) {
      assertTrue(String.format("Privilege on %s should not exist.",
          privilegeReq.getName()),
          expectedPrivilegeNames.contains(privilegeReq.getName()));
    }
  }
}
