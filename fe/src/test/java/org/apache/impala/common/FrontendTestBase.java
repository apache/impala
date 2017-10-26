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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.CreateViewStmt;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.InsertStmt;
import org.apache.impala.analysis.ParseNode;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.SqlParser;
import org.apache.impala.analysis.SqlScanner;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.View;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.EventSequence;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base class for most frontend tests. Contains common functions for unit testing
 * various components, e.g., ParsesOk(), ParserError(), AnalyzesOk(), AnalysisError(),
 * as well as helper functions for creating test-local tables/views and UDF/UDAs.
 */
public class FrontendTestBase {
  protected static ImpaladTestCatalog catalog_ = new ImpaladTestCatalog();
  protected static Frontend frontend_ = new Frontend(
      AuthorizationConfig.createAuthDisabledConfig(), catalog_);

  // Test-local list of test databases and tables. These are cleaned up in @After.
  protected final List<Db> testDbs_ = Lists.newArrayList();
  protected final List<Table> testTables_ = Lists.newArrayList();
  protected final String[][] hintStyles_ = new String[][] {
      new String[] { "/* +", "*/" }, // traditional commented hint
      new String[] { "\n-- +", "\n" }, // eol commented hint
      new String[] { "[", "]" } // legacy style
  };

  @BeforeClass
  public static void setUp() throws Exception {
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    RuntimeEnv.INSTANCE.setTestEnv(false);
  }

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
    ArrayList<Type> argTypes = Lists.newArrayList();
    argTypes.addAll(args);
    Function fn = ScalarFunction.createForTesting(
        db, fnName, argTypes, Type.INT, "/Foo", "Foo.class", null,
        null, TFunctionBinaryType.NATIVE);
    fn.setHasVarArgs(varArgs);
    catalog_.addFunction(fn);
    return fn;
  }

  protected void addTestUda(String name, Type retType, Type... argTypes) {
    FunctionName fnName = new FunctionName("default", name);
    catalog_.addFunction(
        AggregateFunction.createForTesting(
            fnName, Lists.newArrayList(argTypes), retType, retType,
            null, "init_fn_symbol", "update_fn_symbol", null, null,
            null, null, null, TFunctionBinaryType.NATIVE));
  }

  /**
   * Add a new dummy database with the given name to the catalog.
   * Returns the new dummy database.
   * The database is registered in testDbs_ and removed in the @After method.
   */
  protected Db addTestDb(String dbName, String comment) {
    Db db = catalog_.getDb(dbName);
    Preconditions.checkState(db == null, "Test db must not already exist.");
    db = new Db(dbName, new org.apache.hadoop.hive.metastore.api.Database(
        dbName, comment, "", Collections.<String, String>emptyMap()));
    catalog_.addDb(db);
    testDbs_.add(db);
    return db;
  }

  protected void clearTestDbs() {
    for (Db testDb: testDbs_) {
      catalog_.removeDb(testDb.getName());
    }
  }

  /**
   * Add a new dummy table to the catalog based on the given CREATE TABLE sql. The
   * returned table only has its metadata partially set, but is capable of being planned.
   * Only HDFS tables and external Kudu tables are supported.
   * Returns the new dummy table.
   * The test tables are registered in testTables_ and removed in the @After method.
   */
  protected Table addTestTable(String createTableSql) {
    CreateTableStmt createTableStmt = (CreateTableStmt) AnalyzesOk(createTableSql);
    Db db = catalog_.getDb(createTableStmt.getDb());
    Preconditions.checkNotNull(db, "Test tables must be created in an existing db.");
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        CatalogOpExecutor.createMetaStoreTable(createTableStmt.toThrift());
    Table dummyTable = Table.fromMetastoreTable(db, msTbl);
    if (dummyTable instanceof HdfsTable) {
      List<ColumnDef> columnDefs = Lists.newArrayList(
          createTableStmt.getPartitionColumnDefs());
      dummyTable.setNumClusteringCols(columnDefs.size());
      columnDefs.addAll(createTableStmt.getColumnDefs());
      for (int i = 0; i < columnDefs.size(); ++i) {
        ColumnDef colDef = columnDefs.get(i);
        dummyTable.addColumn(
            new Column(colDef.getColName(), colDef.getType(), colDef.getComment(), i));
      }
      try {
        HdfsTable hdfsTable = (HdfsTable) dummyTable;
        hdfsTable.addDefaultPartition(msTbl.getSd());
      } catch (CatalogException e) {
        e.printStackTrace();
        fail("Failed to add test table:\n" + createTableSql);
      }
    } else if (dummyTable instanceof KuduTable) {
      if (!Table.isExternalTable(msTbl)) {
        fail("Failed to add table, external kudu table expected:\n" + createTableSql);
      }
      try {
        KuduTable kuduTable = (KuduTable) dummyTable;
        kuduTable.loadSchemaFromKudu();
      } catch (ImpalaRuntimeException e) {
        e.printStackTrace();
        fail("Failed to add test table:\n" + createTableSql);
      }
    } else {
      fail("Test table type not supported:\n" + createTableSql);
    }
    db.addTable(dummyTable);
    testTables_.add(dummyTable);
    return dummyTable;
  }

  /**
   * Adds a test-local view to the catalog based on the given CREATE VIEW sql.
   * The test views are registered in testTables_ and removed in the @After method.
   * Returns the new view.
   */
  protected Table addTestView(String createViewSql) {
    CreateViewStmt createViewStmt = (CreateViewStmt) AnalyzesOk(createViewSql);
    Db db = catalog_.getDb(createViewStmt.getDb());
    Preconditions.checkNotNull(db, "Test views must be created in an existing db.");
    // Do not analyze the stmt to avoid applying rewrites that would alter the view
    // definition. We want to model real views as closely as possible.
    QueryStmt viewStmt = (QueryStmt) ParsesOk(createViewStmt.getInlineViewDef());
    View dummyView = View.createTestView(db, createViewStmt.getTbl(), viewStmt);
    db.addTable(dummyView);
    testTables_.add(dummyView);
    return dummyView;
  }

  protected Table addAllScalarTypesTestTable() {
    addTestDb("allscalartypesdb", "");
    return addTestTable("create table allscalartypes (" +
      "bool_col boolean, tinyint_col tinyint, smallint_col smallint, int_col int, " +
      "bigint_col bigint, float_col float, double_col double, dec1 decimal(9,0), " +
      "d2 decimal(10, 0), d3 decimal(20, 10), d4 decimal(38, 38), d5 decimal(10, 5), " +
      "timestamp_col timestamp, string_col string, varchar_col varchar(50), " +
      "char_col char (30))");
  }

  protected void clearTestTables() {
    for (Table testTable: testTables_) {
      testTable.getDb().removeTable(testTable.getName());
    }
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

  @After
  public void tearDown() {
    clearTestTables();
    clearTestDbs();
  }

  /**
   * Parse 'stmt' and return the root ParseNode.
   */
  public ParseNode ParsesOk(String stmt) {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    ParseNode node = null;
    try {
      node = (ParseNode) parser.parse().value;
    } catch (Exception e) {
      e.printStackTrace();
      fail("\nParser error:\n" + parser.getErrorMsg(stmt));
    }
    assertNotNull(node);
    return node;
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
    return createAnalysisCtx(Catalog.DEFAULT_DB);
  }

  protected AnalysisContext createAnalysisCtx(String defaultDb) {
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        defaultDb, System.getProperty("user.name"));
    EventSequence timeline = new EventSequence("Frontend Test Timeline");
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx,
        AuthorizationConfig.createAuthDisabledConfig(), timeline);
    return analysisCtx;
  }

  protected AnalysisContext createAnalysisCtx(TQueryOptions queryOptions) {
    TQueryCtx queryCtx = TestUtils.createQueryContext();
    queryCtx.client_request.query_options = queryOptions;
    EventSequence timeline = new EventSequence("Frontend Test Timeline");
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx,
        AuthorizationConfig.createAuthDisabledConfig(), timeline);
    return analysisCtx;
  }

  protected AnalysisContext createAnalysisCtx(AuthorizationConfig authzConfig) {
    return createAnalysisCtx(authzConfig, System.getProperty("user.name"));
  }

  protected AnalysisContext createAnalysisCtx(AuthorizationConfig authzConfig,
      String user) {
    TQueryCtx queryCtx = TestUtils.createQueryContext(Catalog.DEFAULT_DB, user);
    EventSequence timeline = new EventSequence("Frontend Test Timeline");
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx, authzConfig, timeline);
    return analysisCtx;
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
    try {
      AnalysisResult analysisResult = parseAndAnalyze(stmt, ctx);
      if (expectedWarning != null) {
        List<String> actualWarnings = analysisResult.getAnalyzer().getWarnings();
        boolean matchedWarning = false;
        for (String actualWarning: actualWarnings) {
          if (actualWarning.startsWith(expectedWarning)) {
            matchedWarning = true;
            break;
          }
        }
        if (!matchedWarning) {
          fail(String.format("Did not produce expected warning.\n" +
              "Expected warning:\n%s.\nActual warnings:\n%s",
              expectedWarning, Joiner.on("\n").join(actualWarnings)));
        }
      }
      Preconditions.checkNotNull(analysisResult.getStmt());
      return analysisResult.getStmt();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error:\n" + e.toString());
    }
    return null;
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
      Assert.assertTrue(
          "got error:\n" + errorString + "\nexpected:\n" + expectedErrorString,
          errorString.startsWith(expectedErrorString));
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
    StatementBase parsedStmt = fe.parse(stmt);
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(fe, ctx.getQueryCtx().session.database, null);
    StmtTableCache stmtTableCache = mdLoader.loadTables(parsedStmt);
    return ctx.analyzeAndAuthorize(parsedStmt, stmtTableCache, fe.getAuthzChecker());
  }
}
