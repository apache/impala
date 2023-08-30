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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.CreateViewStmt;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.ParseNode;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.User;
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
import org.apache.impala.util.NoOpEventSequence;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.impala.util.TSessionStateUtil;

/**
 * Test fixture for the front-end as a whole. Logically equivalent to a running
 * Impala and HMS cluster. Manages the test metadata catalog.
 * Use {@link SessionFixture} to represent a user session (with a user name,
 * session options, and so on), and a {@link QueryFixture} to represent a
 * single query.
 *
 * While this fixture provides methods to parse and analyze a query, these
 * actions are done with default options and handle the general case. Use
 * the above fixtures for greater control, and to get at multiple bits of a
 * query.
 *
 * {@link AbstractFrontendTest} manages a front-end fixture including setup
 * and teardown. Use it as the base class for new tests that wish to use the
 * test fixtures. {@link FrontendTestBase} extends AbstractFrontendTest and
 * wraps the fixture in a set of functions which act as shims for legacy tests.
 */

public class FrontendFixture {
  // Single instance used for all tests. Logically equivalent to a
  // single Impala cluster used by many clients.
  protected static final FrontendFixture instance_;

  static {
    try {
      instance_ = new FrontendFixture();
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  // The test catalog that can hold test-only tables.
  protected final ImpaladTestCatalog catalog_ = new ImpaladTestCatalog();

  // The actual Impala frontend that backs this fixture.
  protected final Frontend frontend_;

  // Test-local list of test databases and tables.
  protected final List<Db> testDbs_ = new ArrayList<>();
  protected final List<Table> testTables_ = new ArrayList<>();

  protected final AnalysisSessionFixture defaultSession_;

  public static FrontendFixture instance() {
    return instance_;
  }

  /**
   * Private constructor. Use {@link #instance()} to get access to
   * the front-end fixture.
   */
  private FrontendFixture() throws ImpalaException {
    defaultSession_ = new AnalysisSessionFixture();
    frontend_ = new Frontend(new NoopAuthorizationFactory(), catalog_);
  }

  /**
   * Call this from the test's @BeforeClass method.
   */
  public void setUp() throws Exception {
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  /**
   * Call this from the test's @AfterClass method.
   */
  public void cleanUp() throws Exception {
    RuntimeEnv.INSTANCE.reset();
    catalog_.close();
  }

  /**
   * Call this from the test's @After method.
   */
  public void tearDown() {
    clearTestTables();
    clearTestDbs();
  }

  public Frontend frontend() { return frontend_; }
  public ImpaladTestCatalog catalog() { return catalog_; }

  /**
   * Returns the default session with default options. Create your own
   * instance if your test needs to change any of the options. Any number
   * of sessions can be active at once.
   *
   * @return the default session with default options
   */
  public AnalysisSessionFixture session() { return defaultSession_; }

  /**
   * Add a new dummy database with the given name to the catalog.
   * Returns the new dummy database.
   * The database is registered in testDbs_ and removed in the @After method.
   */
  public Db addTestDb(String dbName, String comment) {
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
  public Table addTestTable(String createTableSql) {
    CreateTableStmt createTableStmt = (CreateTableStmt) analyzeStmt(createTableSql);
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
        hdfsTable.initializePartitionMetadata(msTbl);
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
        kuduTable.loadSchemaFromKudu(NoOpEventSequence.INSTANCE);
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

  protected void clearTestTables() {
    for (Table testTable: testTables_) {
      testTable.getDb().removeTable(testTable.getName());
    }
  }

  /**
   * Adds a test-local view to the catalog based on the given CREATE VIEW sql.
   * The test views are registered in testTables_ and removed in the @After method.
   * Returns the new view.
   */
  public Table addTestView(String createViewSql) {
    return addTestView(catalog_, createViewSql);
  }

  /**
   * Adds a test-local view to the specified catalog based on the given CREATE VIEW sql.
   * The test views are registered in testTables_ and removed in the @After method.
   * Returns the new view.
   */
  public Table addTestView(Catalog catalog, String createViewSql) {
    CreateViewStmt createViewStmt = (CreateViewStmt) analyzeStmt(createViewSql);
    Db db = catalog.getDb(createViewStmt.getDb());
    Preconditions.checkNotNull(db, "Test views must be created in an existing db.");
    // Do not analyze the stmt to avoid applying rewrites that would alter the view
    // definition. We want to model real views as closely as possible.
    QueryStmt viewStmt = (QueryStmt) parseStmt(createViewStmt.getInlineViewDef());
    View dummyView = View.createTestView(db, createViewStmt.getTbl(), viewStmt);
    db.addTable(dummyView);
    testTables_.add(dummyView);
    return dummyView;
  }

  // Adds a Udf: default.name(args) to the catalog.
  // TODO: we could consider having this be the sql to run instead but that requires
  // connecting to the BE.
  public Function addTestFunction(String name,
      ArrayList<ScalarType> args, boolean varArgs) {
    return addTestFunction("default", name, args, varArgs);
  }

  public Function addTestFunction(String name,
      ScalarType arg, boolean varArgs) {
    return addTestFunction("default", name, Lists.newArrayList(arg), varArgs);
  }

  public Function addTestFunction(String db, String fnName,
      ArrayList<ScalarType> args, boolean varArgs) {
    List<Type> argTypes = new ArrayList<>();
    argTypes.addAll(args);
    Function fn = ScalarFunction.createForTesting(
        db, fnName, argTypes, Type.INT, "/Foo", "Foo.class", null,
        null, TFunctionBinaryType.NATIVE);
    fn.setHasVarArgs(varArgs);
    catalog_.addFunction(fn);
    return fn;
  }

  public void addTestUda(String name, Type retType, Type... argTypes) {
    FunctionName fnName = new FunctionName("default", name);
    catalog_.addFunction(
        AggregateFunction.createForTesting(
            fnName, Lists.newArrayList(argTypes), retType, retType,
            null, "init_fn_symbol", "update_fn_symbol", null, null,
            null, null, null, TFunctionBinaryType.NATIVE));
  }

  public AnalysisContext createAnalysisCtx() {
    return createAnalysisCtx(Catalog.DEFAULT_DB);
  }

  public AnalysisContext createAnalysisCtx(String defaultDb) {
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        defaultDb, System.getProperty("user.name"));
    EventSequence timeline = new EventSequence("Frontend Test Timeline");
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx,
        new NoopAuthorizationFactory(), timeline);
    return analysisCtx;
  }

  public AnalysisContext createAnalysisCtx(TQueryOptions queryOptions) {
    return createAnalysisCtx(queryOptions, new NoopAuthorizationFactory());
  }

  public AnalysisContext createAnalysisCtx(TQueryOptions queryOptions,
      AuthorizationFactory authzFactory) {
    TQueryCtx queryCtx = TestUtils.createQueryContext();
    queryCtx.client_request.query_options = queryOptions;
    EventSequence timeline = new EventSequence("Frontend Test Timeline");
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx, authzFactory, timeline);
    return analysisCtx;
  }

  // This function is only called by createAnalysisCtx() in FrontendTestBase.java and
  // allows us to specify the requesting user when creating an analysis context
  // associated with an authorization request.
  public AnalysisContext createAnalysisCtx(TQueryOptions queryOptions,
      AuthorizationFactory authzFactory, String user) {
    TQueryCtx queryCtx = TestUtils.createQueryContext(Catalog.DEFAULT_DB, user);
    queryCtx.client_request.query_options = queryOptions;
    EventSequence timeline = new EventSequence("Frontend Test Timeline");
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx, authzFactory, timeline);
    return analysisCtx;
  }

  public AnalysisContext createAnalysisCtx(AuthorizationFactory authzFactory) {
    return createAnalysisCtx(authzFactory, System.getProperty("user.name"));
  }

  public AnalysisContext createAnalysisCtx(AuthorizationFactory authzFactory,
      String user) {
    TQueryCtx queryCtx = TestUtils.createQueryContext(Catalog.DEFAULT_DB, user);
    EventSequence timeline = new EventSequence("Frontend Test Timeline");
    AnalysisContext analysisCtx = new AnalysisContext(queryCtx, authzFactory, timeline);
    return analysisCtx;
  }

  /**
   * Parse 'stmt' and return the root StatementBase.
   */
  public StatementBase parseStmt(String stmt) {
    try {
      StatementBase node = Parser.parse(stmt);
      assertNotNull(node);
      return node;
    } catch (AnalysisException e) {
      fail("Parser error:\n" + e.getMessage());
      throw new IllegalStateException(); // Keep compiler happy
    }
  }

  public AnalysisResult parseAndAnalyze(String stmt, AnalysisContext ctx)
      throws ImpalaException {
    StatementBase parsedStmt = Parser.parse(stmt, ctx.getQueryOptions());
    User user = new User(TSessionStateUtil.getEffectiveUser(ctx.getQueryCtx().session));
    StmtMetadataLoader mdLoader = new StmtMetadataLoader(
        frontend_, ctx.getQueryCtx().session.database, null, user, null);
    StmtTableCache stmtTableCache = mdLoader.loadTables(parsedStmt);
    return ctx.analyzeAndAuthorize(parsedStmt, stmtTableCache,
        frontend_.getAuthzChecker());
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   * If 'expectedWarning' is not null, asserts that a warning is produced.
   * Otherwise, asserts no warnings if 'assertNoWarnings' is true.
   */
  public ParseNode analyzeStmt(String stmt, AnalysisContext ctx,
      String expectedWarning, boolean assertNoWarnings) {
    try {
      AnalysisResult analysisResult = parseAndAnalyze(stmt, ctx);
      List<String> actualWarnings = analysisResult.getAnalyzer().getWarnings();
      if (expectedWarning != null) {
        boolean matchedWarning = false;
        for (String actualWarning: actualWarnings) {
          if (actualWarning.startsWith(expectedWarning)) {
            matchedWarning = true;
            break;
          }
        }
        if (!matchedWarning) {
          fail(String.format("Did not produce expected warning.\n"
                  + "Expected warning:\n%s.\nActual warnings:\n%s\nsql:\n%s",
              expectedWarning, Joiner.on("\n").join(actualWarnings), stmt));
        }
      } else if (assertNoWarnings && !actualWarnings.isEmpty()) {
        fail(String.format("Should not produce any warnings. Got:\n%s\nsql:\n%s",
            Joiner.on("\n").join(actualWarnings), stmt));
      }
      Preconditions.checkNotNull(analysisResult.getStmt());
      return analysisResult.getStmt();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Error during analysis:\n" + e.toString() + "\nsql:\n" + stmt);
      throw new IllegalStateException(); // Keep compiler happy
    }
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   * Uses default options; use {@link QueryFixture} for greater control.
   */
  public ParseNode analyzeStmt(String stmt) {
    return analyzeStmt(stmt, createAnalysisCtx(), null, false);
  }
}
