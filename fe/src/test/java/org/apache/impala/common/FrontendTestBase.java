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

package org.apache.impala.common;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.analysis.ParseNode;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.service.FeCatalogManager;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Assert;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base class for most frontend tests. Contains common functions for unit testing
 * various components, e.g., ParsesOk(), ParserError(), AnalyzesOk(), AnalysisError(),
 * as well as helper functions for creating test-local tables/views and UDF/UDAs.
 *
 * Extend "typical" tests from this class. For deeper, or more specialized tests,
 * extend from {@link AbstractFrontendTest} and use the various fixtures directly.
 * This class is also used for "legacy" tests that used the many functions here
 * rather than the newer fixtures.
 */
public class FrontendTestBase extends AbstractFrontendTest {
  // Temporary shim until tests are updated to use the
  // frontend fixture.
  protected static Frontend frontend_ = feFixture_.frontend();
  protected static ImpaladTestCatalog catalog_ = feFixture_.catalog();
  protected final String[][] hintStyles_ = new String[][] {
      new String[] { "/* +", "*/" }, // traditional commented hint
      new String[] { "-- +", "\n" }, // eol commented hint
      new String[] { "[", "]" } // legacy style
  };

  // Adds a Udf: default.name(args) to the catalog.
  // TODO: we could consider having this be the sql to run instead but that requires
  // connecting to the BE.
  protected Function addTestFunction(String name,
      ArrayList<ScalarType> args, boolean varArgs) {
    return addTestFunction("default", name, args, varArgs);
  }

  protected Function addTestFunction(String name,
      ScalarType arg, boolean varArgs) {
    return addTestFunction("default", name, Lists.newArrayList(arg), varArgs);
  }

  protected Function addTestFunction(String db, String fnName,
      ArrayList<ScalarType> args, boolean varArgs) {
    return feFixture_.addTestFunction(db, fnName, args, varArgs);
  }

  protected void addTestUda(String name, Type retType, Type... argTypes) {
    feFixture_.addTestUda(name, retType, argTypes);
  }

  /**
   * Add a new dummy database with the given name to the catalog.
   * Returns the new dummy database.
   * The database is registered in testDbs_ and removed in the @After method.
   */
  protected Db addTestDb(String dbName, String comment) {
    return feFixture_.addTestDb(dbName, comment);
  }

  /**
   * Add a new dummy table to the catalog based on the given CREATE TABLE sql. The
   * returned table only has its metadata partially set, but is capable of being planned.
   * Only HDFS tables and external Kudu tables are supported.
   * Returns the new dummy table.
   * The test tables are registered in testTables_ and removed in the @After method.
   */
  protected Table addTestTable(String createTableSql) {
    return feFixture_.addTestTable(createTableSql);
  }

  /**
   * Adds a test-local view to the catalog based on the given CREATE VIEW sql.
   * The test views are registered in testTables_ and removed in the @After method.
   * Returns the new view.
   */
  protected Table addTestView(String createViewSql) {
    return feFixture_.addTestView(createViewSql);
  }

  /**
   * Adds a test-local view to the specified catalog based on the given CREATE VIEW sql.
   * The test views are registered in testTables_ and removed in the @After method.
   * Returns the new view.
   */
  protected Table addTestView(Catalog catalog, String createViewSql) {
    return feFixture_.addTestView(catalog, createViewSql);
  }

  protected Table addAllScalarTypesTestTable() {
    addTestDb("allscalartypesdb", "");
    return addTestTable("create table allscalartypes (" +
      "bool_col boolean, tinyint_col tinyint, smallint_col smallint, int_col int, " +
      "bigint_col bigint, float_col float, double_col double, dec1 decimal(9,0), " +
      "d2 decimal(10, 0), d3 decimal(20, 10), d4 decimal(38, 38), d5 decimal(10, 5), " +
      "timestamp_col timestamp, string_col string, varchar_col varchar(50), " +
      "char_col char (30), date_col date)");
  }

  /**
   * Inject the hint into the pattern using hint location.
   *
   * Example:
   *   pattern: insert %s into t %s select * from t
   *   hint: <token_hint_begin> hint_with_args(a) <token_hint_end>
   *   loc: Start(=oracle style) | End(=traditional style)
   */
  protected String InjectInsertHint(String pattern, String hint,
      InsertStmt.HintLocation loc) {
    final String oracleHint = (loc == InsertStmt.HintLocation.Start) ? hint : "";
    final String defaultHint  = (loc == InsertStmt.HintLocation.End) ? hint : "";
    return String.format(pattern, oracleHint, defaultHint);
  }

  /**
   * Parse 'stmt' and return the root StatementBase.
   */
  public StatementBase ParsesOk(String stmt) {
    return feFixture_.parseStmt(stmt);
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   */
  public ParseNode AnalyzesOk(String stmt) {
    return AnalyzesOk(stmt, createAnalysisCtx(), null);
  }

  public ParseNode AnalyzesOk(String stmt, AnalysisContext analysisCtx) {
    return AnalyzesOk(stmt, analysisCtx, null);
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   * If 'expectedWarning' is not null, asserts that a warning is produced.
   */
  public ParseNode AnalyzesOk(String stmt, String expectedWarning) {
    return AnalyzesOk(stmt, createAnalysisCtx(), expectedWarning);
  }

  protected AnalysisContext createAnalysisCtx() {
    return feFixture_.createAnalysisCtx();
  }

  protected AnalysisContext createAnalysisCtx(String defaultDb) {
    return feFixture_.createAnalysisCtx(defaultDb);
  }

  protected AnalysisContext createAnalysisCtx(TQueryOptions queryOptions) {
    return feFixture_.createAnalysisCtx(queryOptions);
  }

  protected AnalysisContext createAnalysisCtx(TQueryOptions queryOptions,
      AuthorizationFactory authzFactory) {
    return feFixture_.createAnalysisCtx(queryOptions, authzFactory);
  }

  protected AnalysisContext createAnalysisCtx(AuthorizationFactory authzFactory) {
    return feFixture_.createAnalysisCtx(authzFactory);
  }

  protected AnalysisContext createAnalysisCtx(AuthorizationFactory authzFactory,
      String user) {
    return feFixture_.createAnalysisCtx(authzFactory, user);
  }

  protected AnalysisContext createAnalysisCtxUsingHiveColLabels() {
    AnalysisContext analysisCtx = createAnalysisCtx();
    analysisCtx.setUseHiveColLabels(true);
    return analysisCtx;
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   * If 'expectedWarning' is not null, asserts that a warning is produced.
   */
  public ParseNode AnalyzesOk(String stmt, AnalysisContext ctx, String expectedWarning) {
    return feFixture_.analyzeStmt(stmt, ctx, expectedWarning);
  }

  /**
   * Analyzes the given statement without performing rewrites or authorization.
   */
  public StatementBase AnalyzesOkNoRewrite(StatementBase stmt) throws ImpalaException {
    AnalysisContext ctx = createAnalysisCtx();
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(frontend_, ctx.getQueryCtx().session.database, null);
    StmtTableCache loadedTables = mdLoader.loadTables(stmt);
    Analyzer analyzer = ctx.createAnalyzer(loadedTables);
    stmt.analyze(analyzer);
    return stmt;
  }

  /**
   * Asserts if stmt passes analysis.
   */
  public void AnalysisError(String stmt) {
    AnalysisError(stmt, null);
  }

  /**
   * Asserts if stmt passes analysis or the error string doesn't match and it
   * is non-null.
   */
  public void AnalysisError(String stmt, String expectedErrorString) {
    AnalysisError(stmt, createAnalysisCtx(), expectedErrorString);
  }

  /**
   * Asserts if stmt passes analysis or the error string doesn't match and it
   * is non-null.
   */
  public void AnalysisError(String stmt, AnalysisContext ctx, String expectedErrorString) {
    Preconditions.checkNotNull(expectedErrorString, "No expected error message given.");
    try {
      AnalysisResult analysisResult = parseAndAnalyze(stmt, ctx);
      Preconditions.checkNotNull(analysisResult.getStmt());
    } catch (Exception e) {
      String errorString = e.getMessage();
      Preconditions.checkNotNull(errorString, "Stack trace lost during exception.");
      String msg = "got error:\n" + errorString + "\nexpected:\n" + expectedErrorString;
      // TODO: This logic can be removed.
      // Different versions of Hive have slightly different error messages;
      // we normalize here as follows:
      // 'No FileSystem for Scheme "x"' -> 'No FileSystem for scheme: x'
      if (errorString.contains("No FileSystem for scheme ")) {
        errorString = errorString.replace("\"", "");
        errorString = errorString.replace("No FileSystem for scheme ",
            "No FileSystem for scheme: ");
      }
      Assert.assertTrue(msg, errorString.startsWith(expectedErrorString));
      return;
    }
    fail("Stmt didn't result in analysis error: " + stmt);
  }

  protected AnalysisResult parseAndAnalyze(String stmt, AnalysisContext ctx)
      throws ImpalaException {
    return parseAndAnalyze(stmt, ctx, frontend_);
  }

  protected AnalysisResult parseAndAnalyze(String stmt, AnalysisContext ctx, Frontend fe)
      throws ImpalaException {
    StatementBase parsedStmt = Parser.parse(stmt, ctx.getQueryOptions());
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(fe, ctx.getQueryCtx().session.database, null);
    StmtTableCache stmtTableCache = mdLoader.loadTables(parsedStmt);
    return ctx.analyzeAndAuthorize(parsedStmt, stmtTableCache, fe.getAuthzChecker());
  }

  /**
   * Creates a dummy {@link AuthorizationFactory} with authorization enabled, but does
   * not do the actual authorization.
   */
  protected AuthorizationFactory createAuthorizationFactory() {
    return createAuthorizationFactory(true);
  }

  /**
   * Creates a dummy {@link AuthorizationFactory} with authorization enabled, but does
   * not do the actual authorization.
   *
   * @param authorized the result of the authorization.
   */
  protected AuthorizationFactory createAuthorizationFactory(boolean authorized) {
    return new AuthorizationFactory() {
      @Override
      public AuthorizationConfig getAuthorizationConfig() {
        return new AuthorizationConfig() {
          @Override
          public boolean isEnabled() { return true; }
          @Override
          public String getProviderName() { return "noop"; }
          @Override
          public String getServerName() { return "server1"; }
        };
      }

      @Override
      public AuthorizationChecker newAuthorizationChecker(
          AuthorizationPolicy authzPolicy) {
        AuthorizationConfig authzConfig = getAuthorizationConfig();
        return new AuthorizationChecker(authzConfig) {
          @Override
          protected boolean authorize(User user, PrivilegeRequest request)
              throws InternalException {
            return authorized;
          }

          @Override
          public Set<String> getUserGroups(User user) throws InternalException {
            return Collections.emptySet();
          }

          @Override
          public void authorizeRowFilterAndColumnMask(User user,
              List<PrivilegeRequest> privilegeRequests)
              throws AuthorizationException, InternalException {
          }
        };
      }

      @Override
      public AuthorizationManager newAuthorizationManager(FeCatalogManager catalog,
          Supplier<? extends AuthorizationChecker> authzChecker) {
        return new NoopAuthorizationManager();
      }

      @Override
      public AuthorizationManager newAuthorizationManager(CatalogServiceCatalog catalog) {
        return new NoopAuthorizationManager();
      }
    };
  }
}
