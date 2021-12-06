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

import java.util.HashSet;

import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.ranger.RangerAuthorizationConfig;
import org.apache.impala.authorization.ranger.RangerAuthorizationFactory;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Role;
import org.apache.impala.catalog.User;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.util.EventSequence;
import org.junit.Test;

public class AnalyzeAuthStmtsTest extends FrontendTestBase {
  protected static final String SERVER_NAME = "server1";
  protected static final String RANGER_SERVICE_TYPE = "hive";
  protected static final String RANGER_APP_ID = "impala";

  // TODO: Change this to a @BeforeClass method. Then, clean up these
  // items in @AfterClass, else we've made a global change that may affect
  // other tests in random ways.
  public AnalyzeAuthStmtsTest() {
    catalog_.getAuthPolicy().addPrincipal(
        new Role("myRole", new HashSet<>()));
    catalog_.getAuthPolicy().addPrincipal(
        new User("myUser", new HashSet<>()));
  }

  // TODO: Switch to use a fixture with custom settings rather than the
  // current patchwork of base and derived class methods.
  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   */
  @Override
  public ParseNode AnalyzesOk(String stmt) {
    return AnalyzesOk(stmt, createAnalysisCtx(Catalog.DEFAULT_DB), null);
  }

  /**
   * Asserts if stmt passes analysis or the error string doesn't match and it
   * is non-null.
   */
  @Override
  public void AnalysisError(String stmt, String expectedErrorString) {
    AnalysisError(stmt, createAnalysisCtx(Catalog.DEFAULT_DB), expectedErrorString);
  }

  @Override
  protected AnalysisContext createAnalysisCtx(String defaultDb) {
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        defaultDb, System.getProperty("user.name"));
    EventSequence timeline = new EventSequence("Authorization Test");
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx,
        new RangerAuthorizationFactory(
             new RangerAuthorizationConfig(RANGER_SERVICE_TYPE, RANGER_APP_ID,
                 SERVER_NAME, null, null, null)),
        timeline);
    return analysisCtx;
  }

  private AnalysisContext createAuthDisabledAnalysisCtx() {
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        Catalog.DEFAULT_DB, System.getProperty("user.name"));
    EventSequence timeline = new EventSequence("Authorization Test");
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx,
        new NoopAuthorizationFactory(), timeline);
    return analysisCtx;
  }

  @Test
  public void AnalyzeShowRoles() {
    AnalyzesOk("SHOW ROLES");
    AnalyzesOk("SHOW ROLE GRANT GROUP myGroup");
    AnalyzesOk("SHOW CURRENT ROLES");

    AnalysisContext authDisabledCtx = createAuthDisabledAnalysisCtx();
    AnalysisError("SHOW ROLES", authDisabledCtx,
        "Authorization is not enabled.");
    AnalysisError("SHOW ROLE GRANT GROUP myGroup", authDisabledCtx,
        "Authorization is not enabled.");
    AnalysisError("SHOW CURRENT ROLES", authDisabledCtx,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeShowGrantPrincipal() {
    for (String type: new String[]{"ROLE myRole", "USER myUser", "GROUP myGroup"}) {
      AnalyzesOk(String.format("SHOW GRANT %s", type));
      AnalyzesOk(String.format("SHOW GRANT %s ON SERVER", type));
      AnalyzesOk(String.format("SHOW GRANT %s ON DATABASE functional", type));
      AnalyzesOk(String.format("SHOW GRANT %s ON TABLE functional.alltypes", type));
      AnalyzesOk(String.format("SHOW GRANT %s ON COLUMN functional.alltypes.year", type));
      AnalyzesOk(String.format("SHOW GRANT %s ON URI 'hdfs:////test-warehouse//foo'",
          type));

      AnalysisContext authDisabledCtx = createAuthDisabledAnalysisCtx();
      AnalysisError("SHOW GRANT ROLE myRole", authDisabledCtx,
          "Authorization is not enabled.");
      AnalysisError("SHOW GRANT ROLE myRole ON SERVER", authDisabledCtx,
          "Authorization is not enabled.");

      // Database, table, and column do not exist.
      AnalysisError(String.format("SHOW GRANT %s on DATABASE foo", type),
          "Error setting/showing privileges for database 'foo'. " +
              "Verify that the database exists and that you have permissions to issue " +
              "a GRANT/REVOKE/SHOW GRANT statement.");
      AnalysisError(String.format("SHOW GRANT %s on TABLE foo.bar", type),
          "Error setting/showing privileges for table 'foo.bar'. Verify that the " +
              "table exists and that you have permissions to issue a " +
              "GRANT/REVOKE/SHOW GRANT statement.");
      AnalysisError(String.format("SHOW GRANT %s on COLUMN foo.bar.baz", type),
          "Error setting/showing privileges for table 'foo.bar'. Verify that the " +
              "table exists and that you have permissions to issue a " +
              "GRANT/REVOKE/SHOW GRANT statement.");
    }

    // Determining if a user exists on the system is done in the AuthorizationPolicy and
    // these tests run with authorization disabled. The SHOW GRANT USER will be tested
    // in the custom cluster tests test_grant_revoke.
  }

  @Test
  public void AnalyzeCreateDropRole() throws AnalysisException {
    AnalyzesOk("DROP ROLE myRole");
    AnalyzesOk("CREATE ROLE doesNotExist");

    // Role names are case-insensitive
    AnalyzesOk("DROP ROLE MYrole");

    AnalysisContext authDisabledCtx = createAuthDisabledAnalysisCtx();
    AnalysisError("DROP ROLE myRole", authDisabledCtx,
        "Authorization is not enabled.");
    AnalysisError("CREATE ROLE doesNotExist", authDisabledCtx,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeGrantRevokeRole() throws AnalysisException {
    AnalyzesOk("GRANT ROLE myrole TO GROUP abc");
    AnalyzesOk("REVOKE ROLE myrole FROM GROUP abc");

    AnalysisContext authDisabledCtx = createAuthDisabledAnalysisCtx();
    AnalysisError("GRANT ROLE myrole TO GROUP abc", authDisabledCtx,
        "Authorization is not enabled.");
    AnalysisError("REVOKE ROLE myrole FROM GROUP abc", authDisabledCtx,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeGrantRevokePriv() throws AnalysisException {
    String[] idents = {"myRole", "ROLE myRole", "GROUP myGroup", "USER myUser"};
    boolean[] isGrantVals = {true, false};

    for (String ident : idents) {
      for (boolean isGrant : isGrantVals) {
        Object[] formatArgs = new String[]{"REVOKE", "FROM", ident};
        if (isGrant) formatArgs = new String[]{"GRANT", "TO", ident};
        // ALL privileges
        AnalyzesOk(String.format("%s ALL ON TABLE alltypes %s %s", formatArgs),
            createAnalysisCtx("functional"));
        AnalyzesOk(String.format("%s ALL ON TABLE functional.alltypes %s %s",
            formatArgs));
        AnalyzesOk(String.format("%s ALL ON TABLE functional_kudu.alltypes %s %s",
            formatArgs));
        AnalyzesOk(String.format("%s ALL ON DATABASE functional %s %s", formatArgs));
        AnalyzesOk(String.format("%s ALL ON SERVER %s %s", formatArgs));
        AnalyzesOk(String.format("%s ALL ON SERVER server1 %s %s", formatArgs));
        AnalyzesOk(String.format("%s ALL ON URI 'hdfs:////abc//123' %s %s",
            formatArgs));
        AnalysisError(String.format("%s ALL ON URI 'xxxx:////abc//123' %s %s",
            formatArgs), "No FileSystem for scheme: xxxx");
        AnalysisError(String.format(
            "%s ALL ON STORAGEHANDLER_URI 'kudu://localhost/tbl' %s %s", formatArgs),
            "Only 'RWSTORAGE' privilege may be applied at storage handler URI scope " +
                "in privilege spec.");
        AnalysisError(String.format("%s ALL ON DATABASE does_not_exist %s %s",
            formatArgs), "Error setting/showing privileges for " +
            "database 'does_not_exist'. Verify that the database exists and that you " +
            "have permissions to issue a GRANT/REVOKE/SHOW GRANT statement.");
        AnalysisError(String.format("%s ALL ON TABLE does_not_exist %s %s",
            formatArgs), "Error setting/showing privileges for table 'does_not_exist'. " +
            "Verify that the table exists and that you have permissions to issue " +
            "a GRANT/REVOKE/SHOW GRANT statement.");
        AnalysisError(String.format("%s ALL ON SERVER does_not_exist %s %s",
            formatArgs), "Specified server name 'does_not_exist' does not match the " +
            "configured server name 'server1'");

        // INSERT privilege
        AnalyzesOk(String.format("%s INSERT ON TABLE alltypesagg %s %s", formatArgs),
            createAnalysisCtx("functional"));
        AnalyzesOk(String.format(
            "%s INSERT ON TABLE functional_kudu.alltypessmall %s %s", formatArgs));
        AnalyzesOk(String.format("%s INSERT ON TABLE functional.alltypesagg %s %s",
            formatArgs));
        AnalyzesOk(String.format("%s INSERT ON DATABASE functional %s %s",
            formatArgs));
        AnalyzesOk(String.format("%s INSERT ON SERVER %s %s", formatArgs));
        AnalysisError(String.format("%s INSERT ON URI 'hdfs:////abc//123' %s %s",
            formatArgs), "Only 'ALL' privilege may be applied at URI scope in " +
            "privilege spec.");
        AnalysisError(String.format(
            "%s INSERT ON STORAGEHANDLER_URI 'kudu://localhost/tbl' %s %s", formatArgs),
            "Only 'RWSTORAGE' privilege may be applied at storage handler URI scope " +
                "in privilege spec.");

        // SELECT privilege
        AnalyzesOk(String.format("%s SELECT ON TABLE alltypessmall %s %s", formatArgs),
            createAnalysisCtx("functional"));
        AnalyzesOk(String.format("%s SELECT ON TABLE functional.alltypessmall %s %s",
            formatArgs));
        AnalyzesOk(String.format(
            "%s SELECT ON TABLE functional_kudu.alltypessmall %s %s", formatArgs));
        AnalyzesOk(String.format("%s SELECT ON DATABASE functional %s %s",
            formatArgs));
        AnalyzesOk(String.format("%s SELECT ON SERVER %s %s", formatArgs));
        AnalysisError(String.format("%s SELECT ON URI 'hdfs:////abc//123' %s %s",
            formatArgs), "Only 'ALL' privilege may be applied at URI scope in " +
            "privilege spec.");
        AnalysisError(String.format(
            "%s SELECT ON STORAGEHANDLER_URI 'kudu://localhost/tbl' %s %s", formatArgs),
            "Only 'RWSTORAGE' privilege may be applied at storage handler URI scope " +
                "in privilege spec.");

        // SELECT privileges on columns
        AnalyzesOk(String.format("%s SELECT (id, int_col) ON TABLE functional.alltypes " +
            "%s %s", formatArgs));
        AnalyzesOk(String.format("%s SELECT (id, id) ON TABLE functional.alltypes " +
            "%s %s", formatArgs));
        // SELECT privilege on both regular and partition columns
        AnalyzesOk(String.format("%s SELECT (id, int_col, year, month) ON TABLE " +
            "alltypes %s %s", formatArgs), createAnalysisCtx("functional"));
        AnalyzesOk(String.format("%s SELECT (id, bool_col) ON TABLE " +
            "functional_kudu.alltypessmall %s %s", formatArgs));
        // Column-level privileges on a VIEW
        AnalyzesOk(String.format("%s SELECT (id, bool_col) ON TABLE " +
            "functional.alltypes_hive_view %s %s", formatArgs));
        // Empty column list
        AnalysisError(String.format("%s SELECT () ON TABLE functional.alltypes " +
            "%s %s", formatArgs), "Empty column list in column privilege spec.");
        // INSERT/ALL privileges on columns
        AnalysisError(String.format("%s INSERT (id, tinyint_col) ON TABLE " +
            "functional.alltypes %s %s", formatArgs), "Only 'SELECT' privileges " +
            "are allowed in a column privilege spec.");
        AnalysisError(String.format("%s ALL (id, tinyint_col) ON TABLE " +
            "functional.alltypes %s %s", formatArgs), "Only 'SELECT' privileges " +
            "are allowed in a column privilege spec.");
        // Columns/table that don't exist
        AnalysisError(String.format("%s SELECT (invalid_col) ON TABLE " +
            "functional.alltypes %s %s", formatArgs), "Error setting/showing " +
            "column-level privileges for table 'functional.alltypes'. Verify that " +
            "both table and columns exist and that you have permissions to issue a " +
            "GRANT/REVOKE/SHOW GRANT statement.");
        AnalysisError(String.format("%s SELECT (id, int_col) ON TABLE " +
            "functional.does_not_exist %s %s", formatArgs), "Error setting/showing " +
            "privileges for table 'functional.does_not_exist'. Verify that the table " +
            "exists and that you have permissions to issue a " +
            "GRANT/REVOKE/SHOW GRANT statement.");

        // REFRESH privilege
        AnalyzesOk(String.format(
            "%s REFRESH ON TABLE functional.alltypes %s %s", formatArgs));
        AnalyzesOk(String.format(
            "%s REFRESH ON DATABASE functional %s %s", formatArgs));
        AnalyzesOk(String.format("%s REFRESH ON SERVER %s %s", formatArgs));
        AnalyzesOk(String.format("%s REFRESH ON SERVER server1 %s %s",
            formatArgs));
        AnalysisError(String.format(
            "%s REFRESH ON URI 'hdfs:////abc//123' %s %s", formatArgs),
            "Only 'ALL' privilege may be applied at URI scope in privilege spec.");
        AnalysisError(String.format(
            "%s REFRESH ON STORAGEHANDLER_URI 'kudu://localhost/tbl' %s %s", formatArgs),
            "Only 'RWSTORAGE' privilege may be applied at storage handler URI scope " +
                "in privilege spec.");

        // CREATE privilege
        AnalyzesOk(String.format("%s CREATE ON SERVER %s %s", formatArgs));
        AnalyzesOk(String.format("%s CREATE ON SERVER server1 %s %s", formatArgs));
        AnalyzesOk(String.format(
            "%s CREATE ON DATABASE functional %s %s", formatArgs));
        AnalysisError(String.format(
            "%s CREATE ON TABLE functional.alltypes %s %s", formatArgs),
            "Create-level privileges on tables are not supported.");
        AnalysisError(String.format(
            "%s CREATE ON URI 'hdfs:////abc//123' %s %s", formatArgs),
            "Only 'ALL' privilege may be applied at URI scope in privilege spec.");
        AnalysisError(String.format(
            "%s CREATE ON STORAGEHANDLER_URI 'kudu://localhost/tbl' %s %s", formatArgs),
            "Only 'RWSTORAGE' privilege may be applied at storage handler URI scope " +
                "in privilege spec.");

        // ALTER privilege
        AnalyzesOk(String.format("%s ALTER ON SERVER %s %s", formatArgs));
        AnalyzesOk(String.format("%s ALTER ON SERVER server1 %s %s", formatArgs));
        AnalyzesOk(String.format("%s ALTER ON DATABASE functional %s %s", formatArgs));
        AnalyzesOk(String.format(
            "%s ALTER ON TABLE functional.alltypes %s %s", formatArgs));
        AnalysisError(String.format(
            "%s ALTER ON URI 'hdfs:////abc/123' %s %s", formatArgs),
            "Only 'ALL' privilege may be applied at URI scope in privilege spec.");
        AnalysisError(String.format(
            "%s ALTER ON STORAGEHANDLER_URI 'kudu://localhost/tbl' %s %s", formatArgs),
            "Only 'RWSTORAGE' privilege may be applied at storage handler URI scope " +
                "in privilege spec.");

        // DROP privilege
        AnalyzesOk(String.format("%s DROP ON SERVER %s %s", formatArgs));
        AnalyzesOk(String.format("%s DROP ON SERVER server1 %s %s", formatArgs));
        AnalyzesOk(String.format("%s DROP ON DATABASE functional %s %s", formatArgs));
        AnalyzesOk(String.format(
            "%s DROP ON TABLE functional.alltypes %s myrole", formatArgs));
        AnalysisError(String.format(
            "%s DROP ON URI 'hdfs:////abc/123' %s %s", formatArgs),
            "Only 'ALL' privilege may be applied at URI scope in privilege spec.");
        AnalysisError(String.format(
            "%s DROP ON STORAGEHANDLER_URI 'kudu://localhost/tbl' %s %s", formatArgs),
            "Only 'RWSTORAGE' privilege may be applied at storage handler URI scope " +
            "in privilege spec.");

        // RWSTORAGE privilege
        AnalyzesOk(String.format(
            "%s RWSTORAGE ON STORAGEHANDLER_URI 'kudu://localhost/tbl' %s %s",
            formatArgs));
        AnalyzesOk(String.format(
            "%s RWSTORAGE ON STORAGEHANDLER_URI '*://*' %s %s", formatArgs));
        AnalyzesOk(String.format(
            "%s RWSTORAGE ON STORAGEHANDLER_URI 'kudu://*' %s %s", formatArgs));
        AnalyzesOk(String.format(
            "%s RWSTORAGE ON STORAGEHANDLER_URI 'kudu://localhost/*' %s %s", formatArgs));
        AnalysisError(String.format(
            "%s DROP ON STORAGEHANDLER_URI 'abc://localhost/tbl' %s %s", formatArgs),
            "The storage type \"abc\" is not supported. " +
                "A storage handler URI should be in the form of " +
                "<storage_type>://<hostname>[:<port>]/<path_to_resource>.");
        AnalysisError(String.format(
            "%s RWSTORAGE ON STORAGEHANDLER_URI 'kudu://*/*' %s %s", formatArgs),
            "A storage handler URI should be in the form of " +
            "<storage_type>://<hostname>[:<port>]/<path_to_resource>.");
      }

      AnalysisContext authDisabledCtx = createAuthDisabledAnalysisCtx();
      AnalysisError("GRANT ALL ON SERVER TO myRole", authDisabledCtx,
          "Authorization is not enabled.");
      AnalysisError("REVOKE ALL ON SERVER FROM myRole", authDisabledCtx,
          "Authorization is not enabled.");

      TQueryCtx noUserNameQueryCtx = TestUtils.createQueryContext(
          Catalog.DEFAULT_DB, "");
      EventSequence timeline = new EventSequence("Authorization Test");
      AnalysisContext noUserNameCtx = new AnalysisContext(noUserNameQueryCtx,
          new RangerAuthorizationFactory(
              new RangerAuthorizationConfig(RANGER_SERVICE_TYPE, RANGER_APP_ID,
                  SERVER_NAME, null, null, null)),
          timeline);
      AnalysisError("GRANT ALL ON SERVER TO myRole", noUserNameCtx,
          "Cannot execute authorization statement with an empty username.");
    }
  }
}
