// Copyright (c) 2014 Cloudera, Inc. All rights reserved.
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

import java.util.HashSet;

import org.junit.Test;

import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Role;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.thrift.TQueryCtx;

public class AnalyzeAuthStmtsTest extends AnalyzerTest {
  public AnalyzeAuthStmtsTest() throws AnalysisException {
    analyzer_ = createAnalyzer(Catalog.DEFAULT_DB);
    analyzer_.getCatalog().getAuthPolicy().addRole(
        new Role("myRole", new HashSet<String>()));
  }

  @Override
  protected Analyzer createAnalyzer(String defaultDb) {
    TQueryCtx queryCtx =
        TestUtils.createQueryContext(defaultDb, System.getProperty("user.name"));
    return new Analyzer(catalog_, queryCtx,
        AuthorizationConfig.createHadoopGroupAuthConfig("server1", null, null));
  }

  private Analyzer createAuthDisabledAnalyzer(String defaultDb) {
    TQueryCtx queryCtx =
        TestUtils.createQueryContext(defaultDb, System.getProperty("user.name"));
    return new Analyzer(catalog_, queryCtx,
        AuthorizationConfig.createAuthDisabledConfig());
  }

  @Test
  public void AnalyzeShowRoles() {
    AnalyzesOk("SHOW ROLES");
    AnalyzesOk("SHOW ROLE GRANT GROUP myGroup");
    AnalyzesOk("SHOW CURRENT ROLES");

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("SHOW ROLES", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("SHOW ROLE GRANT GROUP myGroup", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("SHOW CURRENT ROLES", authDisabledAnalyzer,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeShowGrantRole() {
    AnalyzesOk("SHOW GRANT ROLE myRole");
    AnalyzesOk("SHOW GRANT ROLE myRole ON SERVER");
    AnalyzesOk("SHOW GRANT ROLE myRole ON DATABASE functional");
    AnalyzesOk("SHOW GRANT ROLE myRole ON TABLE functional.alltypes");
    AnalyzesOk("SHOW GRANT ROLE myRole ON URI 'hdfs:////test-warehouse//foo'");
    AnalysisError("SHOW GRANT ROLE does_not_exist",
        "Role 'does_not_exist' does not exist.");
    AnalysisError("SHOW GRANT ROLE does_not_exist ON SERVER",
        "Role 'does_not_exist' does not exist.");

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("SHOW GRANT ROLE myRole", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("SHOW GRANT ROLE myRole ON SERVER", authDisabledAnalyzer,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeCreateDropRole() throws AnalysisException {
    AnalyzesOk("DROP ROLE myRole");
    AnalyzesOk("CREATE ROLE doesNotExist");

    AnalysisError("DROP ROLE doesNotExist", "Role 'doesNotExist' does not exist.");
    AnalysisError("CREATE ROLE myRole", "Role 'myRole' already exists.");

    // Role names are case-insensitive
    AnalyzesOk("DROP ROLE MYrole");
    AnalysisError("CREATE ROLE MYrole", "Role 'MYrole' already exists.");

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("DROP ROLE myRole", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("CREATE ROLE doesNotExist", authDisabledAnalyzer,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeGrantRevokeRole() throws AnalysisException {
    AnalyzesOk("GRANT ROLE myrole TO GROUP abc");
    AnalyzesOk("REVOKE ROLE myrole FROM GROUP abc");
    AnalysisError("GRANT ROLE doesNotExist TO GROUP abc",
        "Role 'doesNotExist' does not exist.");
    AnalysisError("REVOKE ROLE doesNotExist FROM GROUP abc",
        "Role 'doesNotExist' does not exist.");

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("GRANT ROLE myrole TO GROUP abc", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("REVOKE ROLE myrole FROM GROUP abc", authDisabledAnalyzer,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeGrantRevokePriv() throws AnalysisException {
    boolean[] isGrantVals = {true, false};
    for (boolean isGrant: isGrantVals) {
      Object[] formatArgs = new String[] {"REVOKE", "FROM"};
      if (isGrant) formatArgs = new String[] {"GRANT", "TO"};
      // ALL privileges
      AnalyzesOk(String.format("%s ALL ON TABLE alltypes %s myrole", formatArgs),
          createAnalyzer("functional"));
      AnalyzesOk(String.format("%s ALL ON TABLE functional.alltypes %s myrole",
          formatArgs));
      AnalyzesOk(String.format("%s ALL ON DATABASE functional %s myrole", formatArgs));
      AnalyzesOk(String.format("%s ALL ON SERVER %s myrole", formatArgs));
      AnalyzesOk(String.format("%s ALL ON URI 'hdfs:////abc//123' %s myrole",
          formatArgs));
      AnalysisError(String.format("%s ALL ON URI 'xxxx:////abc//123' %s myrole",
          formatArgs), "No FileSystem for scheme: xxxx");
      AnalysisError(String.format("%s ALL ON DATABASE does_not_exist %s myrole",
          formatArgs), "Error setting privileges for database 'does_not_exist'. " +
          "Verify that the database exists and that you have permissions to issue " +
          "a GRANT/REVOKE statement.");
      AnalysisError(String.format("%s ALL ON TABLE does_not_exist %s myrole",
          formatArgs), "Error setting privileges for table 'does_not_exist'. " +
          "Verify that the table exists and that you have permissions to issue " +
          "a GRANT/REVOKE statement.");

      // INSERT privilege
      AnalyzesOk(String.format("%s INSERT ON TABLE alltypesagg %s myrole", formatArgs),
          createAnalyzer("functional"));
      AnalyzesOk(String.format("%s INSERT ON TABLE functional.alltypesagg %s myrole",
          formatArgs));
      AnalyzesOk(String.format("%s INSERT ON DATABASE functional %s myrole",
          formatArgs));
      AnalysisError(String.format("%s INSERT ON SERVER %s myrole", formatArgs),
          "Only 'ALL' privilege may be applied at SERVER scope in privilege spec.");
      AnalysisError(String.format("%s INSERT ON URI 'hdfs:////abc//123' %s myrole",
          formatArgs), "Only 'ALL' privilege may be applied at URI scope in privilege " +
          "spec.");

      // SELECT privilege
      AnalyzesOk(String.format("%s SELECT ON TABLE alltypessmall %s myrole", formatArgs),
          createAnalyzer("functional"));
      AnalyzesOk(String.format("%s SELECT ON TABLE functional.alltypessmall %s myrole",
          formatArgs));
      AnalyzesOk(String.format("%s SELECT ON DATABASE functional %s myrole",
          formatArgs));
      AnalysisError(String.format("%s SELECT ON SERVER %s myrole", formatArgs),
          "Only 'ALL' privilege may be applied at SERVER scope in privilege spec.");
      AnalysisError(String.format("%s SELECT ON URI 'hdfs:////abc//123' %s myrole",
          formatArgs), "Only 'ALL' privilege may be applied at URI scope in privilege " +
          "spec.");

      // SELECT privileges on columns
      AnalyzesOk(String.format("%s SELECT (id, int_col) ON TABLE functional.alltypes " +
          "%s myrole", formatArgs));
      AnalyzesOk(String.format("%s SELECT (id, id) ON TABLE functional.alltypes " +
          "%s myrole", formatArgs));
      // SELECT privilege on both regular and partition columns
      AnalyzesOk(String.format("%s SELECT (id, int_col, year, month) ON TABLE " +
          "alltypes %s myrole", formatArgs), createAnalyzer("functional"));
      // Empty column list
      AnalysisError(String.format("%s SELECT () ON TABLE functional.alltypes " +
          "%s myrole", formatArgs), "Empty column list in column privilege spec.");
      // INSERT/ALL privileges on columns
      AnalysisError(String.format("%s INSERT (id, tinyint_col) ON TABLE " +
          "functional.alltypes %s myrole", formatArgs), "Only 'SELECT' privileges " +
          "are allowed in a column privilege spec.");
      AnalysisError(String.format("%s ALL (id, tinyint_col) ON TABLE " +
          "functional.alltypes %s myrole", formatArgs), "Only 'SELECT' privileges " +
          "are allowed in a column privilege spec.");
      // Column-level privileges on a VIEW
      AnalysisError(String.format("%s SELECT (id, bool_col) ON TABLE " +
          "functional.alltypes_hive_view %s myrole", formatArgs), "Column-level " +
          "privileges on views are not supported.");
      // Columns/table that don't exist
      AnalysisError(String.format("%s SELECT (invalid_col) ON TABLE " +
          "functional.alltypes %s myrole", formatArgs), "Error setting column-level " +
          "privileges for table 'functional.alltypes'. Verify that both table and " +
          "columns exist and that you have permissions to issue a GRANT/REVOKE " +
          "statement.");
      AnalysisError(String.format("%s SELECT (id, int_col) ON TABLE " +
          "functional.does_not_exist %s myrole", formatArgs), "Error setting " +
          "privileges for table 'functional.does_not_exist'. Verify that the table " +
          "exists and that you have permissions to issue a GRANT/REVOKE statement.");
    }

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("GRANT ALL ON SERVER TO myRole", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("REVOKE ALL ON SERVER FROM myRole", authDisabledAnalyzer,
        "Authorization is not enabled.");

    TQueryCtx queryCtxNoUsername = TestUtils.createQueryContext("default", "");
    Analyzer noUsernameAnalyzer = new Analyzer(catalog_, queryCtxNoUsername,
        AuthorizationConfig.createHadoopGroupAuthConfig("server1", null, null));
    AnalysisError("GRANT ALL ON SERVER TO myRole", noUsernameAnalyzer,
        "Cannot execute authorization statement with an empty username.");
  }
}
