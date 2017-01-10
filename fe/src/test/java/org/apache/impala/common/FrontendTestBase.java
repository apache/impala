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
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.CreateViewStmt;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.ParseNode;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.SqlParser;
import org.apache.impala.analysis.SqlScanner;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.View;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.After;
import org.junit.Assert;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base class for most frontend tests. Contains common functions for unit testing
 * various components, e.g., ParsesOk(), ParserError(), AnalyzesOk(), AnalysisError(),
 * as well as helper functions for creating test-local tables/views and UDF/UDAs.
 */
public class FrontendTestBase {
  protected static ImpaladCatalog catalog_ = new ImpaladTestCatalog();
  protected static Frontend frontend_ = new Frontend(
      AuthorizationConfig.createAuthDisabledConfig(), catalog_);

  // Test-local list of test databases and tables. These are cleaned up in @After.
  protected final List<Db> testDbs_ = Lists.newArrayList();
  protected final List<Table> testTables_ = Lists.newArrayList();

  protected Analyzer createAnalyzer(String defaultDb) {
    TQueryCtx queryCtx =
        TestUtils.createQueryContext(defaultDb, System.getProperty("user.name"));
    return new Analyzer(catalog_, queryCtx,
        AuthorizationConfig.createAuthDisabledConfig());
  }

  protected Analyzer createAnalyzer(TQueryOptions queryOptions) {
    TQueryCtx queryCtx = TestUtils.createQueryContext();
    queryCtx.client_request.query_options = queryOptions;
    return new Analyzer(catalog_, queryCtx,
        AuthorizationConfig.createAuthDisabledConfig());
  }

  protected Analyzer createAnalyzerUsingHiveColLabels() {
    Analyzer analyzer = createAnalyzer(Catalog.DEFAULT_DB);
    analyzer.setUseHiveColLabels(true);
    return analyzer;
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
    db = new Db(dbName, catalog_, new org.apache.hadoop.hive.metastore.api.Database(
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
   * Add a new dummy table to the catalog based on the given CREATE TABLE sql.
   * The dummy table only has the column definitions and no other metadata.
   * Returns the new dummy table.
   * The test tables are registered in testTables_ and removed in the @After method.
   */
  protected Table addTestTable(String createTableSql) {
    CreateTableStmt createTableStmt = (CreateTableStmt) AnalyzesOk(createTableSql);
    // Currently does not support partitioned tables.
    Preconditions.checkState(createTableStmt.getPartitionColumnDefs().isEmpty());
    Db db = catalog_.getDb(createTableStmt.getDb());
    Preconditions.checkNotNull(db, "Test tables must be created in an existing db.");
    HdfsTable dummyTable = new HdfsTable(null, db,
        createTableStmt.getTbl(), createTableStmt.getOwner());
    List<ColumnDef> columnDefs = createTableStmt.getColumnDefs();
    for (int i = 0; i < columnDefs.size(); ++i) {
      ColumnDef colDef = columnDefs.get(i);
      dummyTable.addColumn(new Column(colDef.getColName(), colDef.getType(), i));
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

  protected void clearTestTables() {
    for (Table testTable: testTables_) {
      testTable.getDb().removeTable(testTable.getName());
    }
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
    return AnalyzesOk(stmt, createAnalyzer(Catalog.DEFAULT_DB), null);
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   * If 'expectedWarning' is not null, asserts that a warning is produced.
   */
  public ParseNode AnalyzesOk(String stmt, String expectedWarning) {
    return AnalyzesOk(stmt, createAnalyzer(Catalog.DEFAULT_DB), expectedWarning);
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   * If 'expectedWarning' is not null, asserts that a warning is produced.
   */
  public ParseNode AnalyzesOk(String stmt, Analyzer analyzer, String expectedWarning) {
    try {
      AnalysisContext analysisCtx = new AnalysisContext(catalog_,
          TestUtils.createQueryContext(Catalog.DEFAULT_DB,
            System.getProperty("user.name")),
          AuthorizationConfig.createAuthDisabledConfig());
      analysisCtx.analyze(stmt, analyzer);
      AnalysisContext.AnalysisResult analysisResult = analysisCtx.getAnalysisResult();
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
   * Asserts if stmt passes analysis.
   */
  public void AnalysisError(String stmt) {
    AnalysisError(stmt, null);
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   */
  public ParseNode AnalyzesOk(String stmt, Analyzer analyzer) {
    return AnalyzesOk(stmt, analyzer, null);
  }

  /**
   * Asserts if stmt passes analysis or the error string doesn't match and it
   * is non-null.
   */
  public void AnalysisError(String stmt, String expectedErrorString) {
    AnalysisError(stmt, createAnalyzer(Catalog.DEFAULT_DB), expectedErrorString);
  }

  /**
   * Asserts if stmt passes analysis or the error string doesn't match and it
   * is non-null.
   */
  public void AnalysisError(String stmt, Analyzer analyzer, String expectedErrorString) {
    Preconditions.checkNotNull(expectedErrorString, "No expected error message given.");
    try {
      AnalysisContext analysisCtx = new AnalysisContext(catalog_,
          TestUtils.createQueryContext(Catalog.DEFAULT_DB,
              System.getProperty("user.name")),
              AuthorizationConfig.createAuthDisabledConfig());
      analysisCtx.analyze(stmt, analyzer);
      AnalysisContext.AnalysisResult analysisResult = analysisCtx.getAnalysisResult();
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
}
