// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.catalog.AggregateFunction;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.ImpaladCatalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.testutil.ImpaladTestCatalog;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.thrift.TQueryOptions;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class AnalyzerTest {
  protected final static Logger LOG = LoggerFactory.getLogger(AnalyzerTest.class);
  protected static ImpaladCatalog catalog_ = new ImpaladTestCatalog();

  // Test-local list of test databases and tables. These are cleaned up in @After.
  protected final List<Db> testDbs_ = Lists.newArrayList();
  protected final List<Table> testTables_ = Lists.newArrayList();

  protected Analyzer analyzer_;

  // maps from type to string that will result in literal of that type
  protected static Map<ScalarType, String> typeToLiteralValue_ =
      new HashMap<ScalarType, String>();
  static {
    typeToLiteralValue_.put(Type.BOOLEAN, "true");
    typeToLiteralValue_.put(Type.TINYINT, "1");
    typeToLiteralValue_.put(Type.SMALLINT, (Byte.MAX_VALUE + 1) + "");
    typeToLiteralValue_.put(Type.INT, (Short.MAX_VALUE + 1) + "");
    typeToLiteralValue_.put(Type.BIGINT,
        ((long) Integer.MAX_VALUE + 1) + "");
    typeToLiteralValue_.put(Type.FLOAT, "cast(1.0 as float)");
    typeToLiteralValue_.put(Type.DOUBLE,
        "cast(" + (Float.MAX_VALUE + 1) + " as double)");
    typeToLiteralValue_.put(Type.TIMESTAMP,
        "cast('2012-12-21 00:00:00.000' as timestamp)");
    typeToLiteralValue_.put(Type.STRING, "'Hello, World!'");
    typeToLiteralValue_.put(Type.NULL, "NULL");
  }

  protected Analyzer createAnalyzer(String defaultDb) {
    TQueryCtx queryCtx =
        TestUtils.createQueryContext(defaultDb, System.getProperty("user.name"));
    return new Analyzer(catalog_, queryCtx,
        AuthorizationConfig.createAuthDisabledConfig());
  }

  protected Analyzer createAnalyzer(TQueryOptions queryOptions) {
    TQueryCtx queryCtx = TestUtils.createQueryContext();
    queryCtx.request.query_options = queryOptions;
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
    Function fn = new ScalarFunction(
        new FunctionName(db, fnName), argTypes, Type.INT, null, null, null, null);
    fn.setHasVarArgs(varArgs);
    catalog_.addFunction(fn);
    return fn;
  }

  protected void addTestUda(String name, Type retType, Type... argTypes) {
    FunctionName fnName = new FunctionName("default", name);
    catalog_.addFunction(
        new AggregateFunction(fnName, Lists.newArrayList(argTypes), retType, false));
  }

  /**
   * Add a new dummy database with the given name to the catalog.
   * Returns the new dummy database.
   * The database is registered in testDbs_ and removed in the @After method.
   */
  protected Db addTestDb(String dbName) {
    Db db = catalog_.getDb(dbName);
    Preconditions.checkState(db == null, "Test db must not already exist.");
    db = new Db(dbName, catalog_);
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
    HdfsTable dummyTable = new HdfsTable(null, null, db, createTableStmt.getTbl(),
        createTableStmt.getOwner());
    List<ColumnDef> columnDefs = createTableStmt.getColumnDefs();
    for (int i = 0; i < columnDefs.size(); ++i) {
      ColumnDef colDef = columnDefs.get(i);
      dummyTable.addColumn(new Column(colDef.getColName(), colDef.getType(), i));
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

  @After
  public void tearDown() {
    clearTestTables();
    clearTestDbs();
  }

  /**
   * Check whether SelectStmt components can be converted to thrift.
   */
  protected void checkSelectToThrift(SelectStmt node) {
    // convert select list exprs and where clause to thrift
    List<Expr> selectListExprs = node.getResultExprs();
    List<TExpr> thriftExprs = Expr.treesToThrift(selectListExprs);
    LOG.info("select list:\n");
    for (TExpr expr: thriftExprs) {
      LOG.info(expr.toString() + "\n");
    }
    for (Expr expr: selectListExprs) {
      checkBinaryExprs(expr);
    }
    if (node.getWhereClause() != null) {
      TExpr thriftWhere = node.getWhereClause().treeToThrift();
      LOG.info("WHERE pred: " + thriftWhere.toString() + "\n");
      checkBinaryExprs(node.getWhereClause());
    }
    AggregateInfo aggInfo = node.getAggInfo();
    if (aggInfo != null) {
      if (aggInfo.getGroupingExprs() != null) {
        LOG.info("grouping exprs:\n");
        for (Expr expr: aggInfo.getGroupingExprs()) {
          LOG.info(expr.treeToThrift().toString() + "\n");
          checkBinaryExprs(expr);
        }
      }
      LOG.info("aggregate exprs:\n");
      for (Expr expr: aggInfo.getAggregateExprs()) {
        LOG.info(expr.treeToThrift().toString() + "\n");
        checkBinaryExprs(expr);
      }
      if (node.getHavingPred() != null) {
        TExpr thriftHaving = node.getHavingPred().treeToThrift();
        LOG.info("HAVING pred: " + thriftHaving.toString() + "\n");
        checkBinaryExprs(node.getHavingPred());
      }
    }
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
      analyzer_ = analyzer;
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
    LOG.info("processing " + stmt);
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

  /**
   * Generates and analyzes two variants of the given query by replacing all occurrences
   * of "$TBL" in the query string with the unqualified and fully-qualified version of
   * the given table name. The unqualified variant is analyzed using an analyzer that has
   * tbl's db set as the default database.
   * Example:
   * query = "select id from $TBL, $TBL"
   * tbl = "functional.alltypes"
   * Variants generated and analyzed:
   * select id from alltypes, alltypes (default db is "functional")
   * select id from functional.alltypes, functional.alltypes (default db is "default")
   */
  protected void TblsAnalyzeOk(String query, TableName tbl) {
    Preconditions.checkState(tbl.isFullyQualified());
    Preconditions.checkState(query.contains("$TBL"));
    String uqQuery = query.replace("$TBL", tbl.getTbl());
    AnalyzesOk(uqQuery, createAnalyzer(tbl.getDb()));
    String fqQuery = query.replace("$TBL", tbl.toString());
    AnalyzesOk(fqQuery);
  }

  /**
   * Same as TblsAnalyzeOk(), except that analysis of all variants is expected
   * to fail with the given error message.
   */
  protected void TblsAnalysisError(String query, TableName tbl,
      String expectedError) {
    Preconditions.checkState(tbl.isFullyQualified());
    Preconditions.checkState(query.contains("$TBL"));
    String uqQuery = query.replace("$TBL", tbl.getTbl());
    AnalysisError(uqQuery, createAnalyzer(tbl.getDb()), expectedError);
    String fqQuery = query.replace("$TBL", tbl.toString());
    AnalysisError(fqQuery, expectedError);
  }

  /**
   * Makes sure that operands to binary exprs having same type.
   */
  private void checkBinaryExprs(Expr expr) {
    if (expr instanceof BinaryPredicate
        || (expr instanceof ArithmeticExpr
        && ((ArithmeticExpr) expr).getOp() != ArithmeticExpr.Operator.BITNOT)) {
      Assert.assertEquals(expr.getChildren().size(), 2);
      // The types must be equal or one of them is NULL_TYPE.
      Assert.assertTrue(expr.getChild(0).getType() == expr.getChild(1).getType()
          || expr.getChild(0).getType().isNull() || expr.getChild(1).getType().isNull());
    }
    for (Expr child: expr.getChildren()) {
      checkBinaryExprs(child);
    }
  }

  @Test
  public void TestCompressedText() throws AnalysisException {
    AnalyzesOk("SELECT count(*) FROM functional_text_lzo.tinyinttable");
    // TODO: Disabling the text/{gzip,bzip,snap} analysis test until the corresponding
    //       databases are loaded.
    // AnalyzesOk("SELECT count(*) FROM functional_text_gzip.tinyinttable");
    // AnalyzesOk("SELECT count(*) FROM functional_text_snap.tinyinttable");
    // AnalyzesOk("SELECT count(*) FROM functional_text_bzip.tinyinttable");
  }

  @Test
  public void TestMemLayout() throws AnalysisException {
    testSelectStar();
    testNonNullable();
    testMixedNullable();
    testNonMaterializedSlots();
  }

  private void testSelectStar() throws AnalysisException {
    AnalyzesOk("select * from functional.AllTypes");
    DescriptorTable descTbl = analyzer_.getDescTbl();
    TupleDescriptor tupleD = descTbl.getTupleDesc(new TupleId(0));
    for (SlotDescriptor slotD: tupleD.getSlots()) {
      slotD.setIsMaterialized(true);
    }
    descTbl.computeMemLayout();
    Assert.assertEquals(97.0f, tupleD.getAvgSerializedSize(), 0.0);
    checkLayoutParams("functional.alltypes.bool_col", 1, 2, 0, 0);
    checkLayoutParams("functional.alltypes.tinyint_col", 1, 3, 0, 1);
    checkLayoutParams("functional.alltypes.smallint_col", 2, 4, 0, 2);
    checkLayoutParams("functional.alltypes.id", 4, 8, 0, 3);
    checkLayoutParams("functional.alltypes.int_col", 4, 12, 0, 4);
    checkLayoutParams("functional.alltypes.float_col", 4, 16, 0, 5);
    checkLayoutParams("functional.alltypes.year", 4, 20, 0, 6);
    checkLayoutParams("functional.alltypes.month", 4, 24, 0, 7);
    checkLayoutParams("functional.alltypes.bigint_col", 8, 32, 1, 0);
    checkLayoutParams("functional.alltypes.double_col", 8, 40, 1, 1);
    int strSlotSize = PrimitiveType.STRING.getSlotSize();
    checkLayoutParams("functional.alltypes.date_string_col", strSlotSize, 48, 1, 2);
    checkLayoutParams("functional.alltypes.string_col",
        strSlotSize, 48 + strSlotSize, 1, 3);
  }

  private void testNonNullable() throws AnalysisException {
    // both slots are non-nullable bigints. The layout should look like:
    // (byte range : data)
    // 0 - 7: count(int_col)
    // 8 - 15: count(*)
    AnalyzesOk("select count(int_col), count(*) from functional.AllTypes");
    DescriptorTable descTbl = analyzer_.getDescTbl();
    TupleDescriptor aggDesc = descTbl.getTupleDesc(new TupleId(1));
    for (SlotDescriptor slotD: aggDesc.getSlots()) {
      slotD.setIsMaterialized(true);
    }
    descTbl.computeMemLayout();
    Assert.assertEquals(16.0f, aggDesc.getAvgSerializedSize(), 0.0);
    Assert.assertEquals(16, aggDesc.getByteSize());
    checkLayoutParams(aggDesc.getSlots().get(0), 8, 0, 0, -1);
    checkLayoutParams(aggDesc.getSlots().get(1), 8, 8, 0, -1);
  }

  private void testMixedNullable() throws AnalysisException {
    // one slot is nullable, one is not. The layout should look like:
    // (byte range : data)
    // 0 : 1 nullable-byte (only 1 bit used)
    // 1 - 7: padded bytes
    // 8 - 15: sum(int_col)
    // 16 - 23: count(*)
    AnalyzesOk("select sum(int_col), count(*) from functional.AllTypes");
    DescriptorTable descTbl = analyzer_.getDescTbl();
    TupleDescriptor aggDesc = descTbl.getTupleDesc(new TupleId(1));
    for (SlotDescriptor slotD: aggDesc.getSlots()) {
      slotD.setIsMaterialized(true);
    }
    descTbl.computeMemLayout();
    Assert.assertEquals(16.0f, aggDesc.getAvgSerializedSize(), 0.0);
    Assert.assertEquals(24, aggDesc.getByteSize());
    checkLayoutParams(aggDesc.getSlots().get(0), 8, 8, 0, 0);
    checkLayoutParams(aggDesc.getSlots().get(1), 8, 16, 0, -1);
  }

  /**
   * Tests that computeMemLayout() ignores non-materialized slots.
   */
  private void testNonMaterializedSlots() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes");
    DescriptorTable descTbl = analyzer_.getDescTbl();
    TupleDescriptor tupleD = descTbl.getTupleDesc(new TupleId(0));
    ArrayList<SlotDescriptor> slots = tupleD.getSlots();
    for (SlotDescriptor slotD: slots) {
      slotD.setIsMaterialized(true);
    }
    // Mark slots 0 (id), 7 (double_col), 9 (string_col) as non-materialized.
    slots.get(0).setIsMaterialized(false);
    slots.get(7).setIsMaterialized(false);
    slots.get(9).setIsMaterialized(false);

    descTbl.computeMemLayout();
    Assert.assertEquals(68.0f, tupleD.getAvgSerializedSize(), 0.0);
    // Check non-materialized slots.
    checkLayoutParams("functional.alltypes.id", 0, -1, 0, 0);
    checkLayoutParams("functional.alltypes.double_col", 0, -1, 0, 0);
    checkLayoutParams("functional.alltypes.string_col", 0, -1, 0, 0);
    // Check materialized slots.
    checkLayoutParams("functional.alltypes.bool_col", 1, 2, 0, 0);
    checkLayoutParams("functional.alltypes.tinyint_col", 1, 3, 0, 1);
    checkLayoutParams("functional.alltypes.smallint_col", 2, 4, 0, 2);
    checkLayoutParams("functional.alltypes.int_col", 4, 8, 0, 3);
    checkLayoutParams("functional.alltypes.float_col", 4, 12, 0, 4);
    checkLayoutParams("functional.alltypes.year", 4, 16, 0, 5);
    checkLayoutParams("functional.alltypes.month", 4, 20, 0, 6);
    checkLayoutParams("functional.alltypes.bigint_col", 8, 24, 0, 7);
    int strSlotSize = PrimitiveType.STRING.getSlotSize();
    checkLayoutParams("functional.alltypes.date_string_col", strSlotSize, 32, 1, 0);
  }

  private void checkLayoutParams(SlotDescriptor d, int byteSize, int byteOffset,
      int nullIndicatorByte, int nullIndicatorBit) {
    Assert.assertEquals(byteSize, d.getByteSize());
    Assert.assertEquals(byteOffset, d.getByteOffset());
    Assert.assertEquals(nullIndicatorByte, d.getNullIndicatorByte());
    Assert.assertEquals(nullIndicatorBit, d.getNullIndicatorBit());
  }

  private void checkLayoutParams(String colAlias, int byteSize, int byteOffset,
      int nullIndicatorByte, int nullIndicatorBit) {
    SlotDescriptor d = analyzer_.getSlotDescriptor(colAlias);
    checkLayoutParams(d, byteSize, byteOffset, nullIndicatorByte, nullIndicatorBit);
  }

  // Analyzes query and asserts that the first result expr returns the given type.
  // Requires query to parse to a SelectStmt.
  protected void checkExprType(String query, Type type) {
    SelectStmt select = (SelectStmt) AnalyzesOk(query);
    Assert.assertEquals(select.getResultExprs().get(0).getType(), type);
  }

  /**
   * We distinguish between three classes of unsupported types:
   * 1. Complex types, e.g., map
   *    For tables with such types we prevent loading the table metadata.
   * 2. Primitive types
   *    For tables with unsupported primitive types (e.g., binary)
   *    we can run queries as long as the unsupported columns are not referenced.
   *    We fail analysis if a query references an unsupported primitive column.
   * 3. Partition-column types
   *    We do not support table partitioning on timestamp columns
   */
  @Test
  public void TestUnsupportedTypes() {
    // Select supported types from a table with mixed supported/unsupported types.
    AnalyzesOk("select int_col, str_col, bigint_col from functional.unsupported_types");

    // Select supported types from a table with mixed supported/unsupported types.
    AnalyzesOk("select int_col, str_col, bigint_col from functional.unsupported_types");

    // Unsupported type binary.
    AnalysisError("select bin_col from functional.unsupported_types",
        "Unsupported type 'BINARY' in 'bin_col'.");
    // Mixed supported/unsupported types.
    AnalysisError("select int_col, str_col, bin_col " +
        "from functional.unsupported_types",
        "Unsupported type 'BINARY' in 'bin_col'.");
    // Unsupported partition-column type.
    AnalysisError("select * from functional.unsupported_partition_types",
        "Failed to load metadata for table: 'functional.unsupported_partition_types'");

    // Try with hbase
    AnalyzesOk("describe functional_hbase.allcomplextypes");
  }

  @Test
  public void TestBinaryHBaseTable() {
    AnalyzesOk("select * from functional_hbase.alltypessmallbinary");
  }

  @Test
  public void TestUnsupportedSerde() {
    AnalysisError("select * from functional.bad_serde",
        "Failed to load metadata for table: 'functional.bad_serde'");
  }

  @Test
  public void TestResetMetadata() {
    AnalyzesOk("invalidate metadata");
    AnalyzesOk("invalidate metadata functional.alltypessmall");
    AnalyzesOk("invalidate metadata functional.alltypes_view");
    AnalyzesOk("invalidate metadata functional.bad_serde");
    AnalyzesOk("refresh functional.alltypessmall");
    AnalyzesOk("refresh functional.alltypes_view");
    AnalyzesOk("refresh functional.bad_serde");

    // invalidate metadata <table name> checks the Hive Metastore for table existence
    // and should not throw an AnalysisError if the table or db does not exist.
    AnalyzesOk("invalidate metadata functional.unknown_table");
    AnalyzesOk("invalidate metadata unknown_db.unknown_table");

    AnalysisError("refresh functional.unknown_table",
        "Table does not exist: functional.unknown_table");
    AnalysisError("refresh unknown_db.unknown_table",
        "Database does not exist: unknown_db");
  }

  @Test
  public void TestExplain() {
    // Analysis error from explain insert: too many partitioning columns.
    AnalysisError("explain insert into table functional.alltypessmall " +
        "partition (year=2009, month=4, year=10)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Duplicate column 'year' in partition clause");

    // Analysis error from explain query
    AnalysisError("explain " +
        "select id from (select id+2 from functional_hbase.alltypessmall) a",
        "Could not resolve column/field reference: 'id'");

    // Positive test for explain query
    AnalyzesOk("explain select * from functional.AllTypes");

    // Positive test for explain insert
    AnalyzesOk("explain insert into table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, int_col, " +
        "float_col, float_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");
  }

  @Test
  public void TestLimitAndOffset() {
    // Arithmetic expressions that result in a positive, integral value are OK
    AnalyzesOk("select * from functional.AllTypes limit 10 * 10 + 10 - 10 % 10");
    AnalyzesOk("select * from functional.AllTypes limit 1 ^ 0 | 3 & 3");
    // Test offset, requires order by and limit
    AnalyzesOk("select * from functional.AllTypes order by id limit 10 offset 1+2*3%4");
    // Test offset within an inline view and with-clause view
    AnalyzesOk("select t5.id from (select id from functional.AllTypes order by id " +
        "limit 10 offset 2) t5");
    AnalyzesOk("with t5 as (select id from functional.AllTypes order by id limit 10 " +
        "offset 2) select * from t5");

    // Casting to int is fine
    AnalyzesOk("select id, bool_col from functional.AllTypes limit CAST(10.0 AS INT)");
    AnalyzesOk("select id, bool_col from functional.AllTypes limit " +
        "CAST(NOT FALSE AS INT)");
    AnalyzesOk("select * from functional.AllTypes order by id limit 10 " +
        "offset CAST(1.0 AS INT)");

    // Analysis error from negative values
    AnalysisError("select * from functional.AllTypes limit 10 - 20",
        "LIMIT must be a non-negative integer: 10 - 20 = -10");
    AnalysisError("select * from functional.AllTypes order by id limit 10 " +
        "offset 10 - 20",
        "OFFSET must be a non-negative integer: 10 - 20 = -10");

    // Analysis error from non-integral values
    AnalysisError("select * from functional.AllTypes limit 10.0",
        "LIMIT expression must be an integer type but is 'DECIMAL(3,1)': 10.0");
    AnalysisError("select * from functional.AllTypes limit NOT FALSE",
        "LIMIT expression must be an integer type but is 'BOOLEAN': NOT FALSE");
    AnalysisError("select * from functional.AllTypes limit CAST(\"asdf\" AS INT)",
        "LIMIT expression evaluates to NULL: CAST('asdf' AS INT)");
    AnalysisError("select * from functional.AllTypes order by id limit 10 " +
        "OFFSET 10.0",
        "OFFSET expression must be an integer type but is 'DECIMAL(3,1)': 10.0");
    AnalysisError("select * from functional.AllTypes order by id limit 10 " +
        "offset CAST('asdf' AS INT)",
        "OFFSET expression evaluates to NULL: CAST('asdf' AS INT)");

    // Analysis error from non-constant expressions
    AnalysisError("select id, bool_col from functional.AllTypes limit id < 10",
        "LIMIT expression must be a constant expression: id < 10");
    AnalysisError("select id, bool_col from functional.AllTypes order by id limit 10 " +
        "offset id < 10",
        "OFFSET expression must be a constant expression: id < 10");

    // Offset is only valid with an order by
    AnalysisError("SELECT a FROM test LIMIT 10 OFFSET 5",
        "OFFSET requires an ORDER BY clause: LIMIT 10 OFFSET 5");
    AnalysisError("SELECT x.id FROM (SELECT id FROM alltypesagg LIMIT 5 OFFSET 5) x " +
        "ORDER BY x.id LIMIT 100 OFFSET 4",
        "OFFSET requires an ORDER BY clause: LIMIT 5 OFFSET 5");
    AnalysisError("SELECT a FROM test OFFSET 5",
        "OFFSET requires an ORDER BY clause: OFFSET 5");
    AnalyzesOk("SELECT id FROM functional.Alltypes ORDER BY bool_col OFFSET 5");
  }

  @Test
  public void TestAnalyzeShowCreateTable() {
    AnalyzesOk("show create table functional.AllTypes");
    AnalysisError("show create table functional.alltypes_view",
        "SHOW CREATE TABLE not supported on VIEW: functional.alltypes_view");
    AnalysisError("show create table functional.not_a_table",
        "Table does not exist: functional.not_a_table");
    AnalysisError("show create table doesnt_exist",
        "Table does not exist: default.doesnt_exist");
  }

  private Function createFunction(boolean hasVarArgs, Type... args) {
    return new Function(new FunctionName("test"), args, Type.INVALID, hasVarArgs);
  }

  @Test
  // Test matching function signatures.
  public void TestFunctionMatching() {
    Function[] fns = new Function[14];
    // test()
    fns[0] = createFunction(false);

    // test(int)
    fns[1] = createFunction(false, Type.INT);

    // test(int...)
    fns[2] = createFunction(true, Type.INT);

    // test(tinyint)
    fns[3] = createFunction(false, Type.TINYINT);

    // test(tinyint...)
    fns[4] = createFunction(true, Type.TINYINT);

    // test(double)
    fns[5] = createFunction(false, Type.DOUBLE);

    // test(double...)
    fns[6] = createFunction(true, Type.DOUBLE);

    // test(double, double)
    fns[7] = createFunction(false, Type.DOUBLE, Type.DOUBLE);

    // test(double, double...)
    fns[8] = createFunction(true, Type.DOUBLE, Type.DOUBLE);

    // test(smallint, tinyint)
    fns[9] = createFunction(false, Type.SMALLINT, Type.TINYINT);

    // test(int, double, double, double)
    fns[10] = createFunction(false, Type.INT, Type.DOUBLE, Type.DOUBLE, Type.DOUBLE);

    // test(int, string, int...)
    fns[11] = createFunction(true, Type.INT, Type.STRING, Type.INT);

    // test(tinying, string, tinyint, int, tinyint)
    fns[12] = createFunction(false, Type.TINYINT, Type.STRING, Type.TINYINT, Type.INT,
        Type.TINYINT);

    // test(tinying, string, bigint, int, tinyint)
    fns[13] = createFunction(false, Type.TINYINT, Type.STRING, Type.BIGINT, Type.INT,
        Type.TINYINT);

    Assert.assertFalse(fns[1].compare(fns[0], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[1].compare(fns[2], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[1].compare(fns[3], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[1].compare(fns[4], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[1].compare(fns[5], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[1].compare(fns[6], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[1].compare(fns[7], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[1].compare(fns[8], Function.CompareMode.IS_SUPERTYPE_OF));

    Assert.assertTrue(fns[1].compare(fns[2], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[3].compare(fns[4], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[5].compare(fns[6], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertFalse(fns[5].compare(fns[7], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertFalse(fns[5].compare(fns[8], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[6].compare(fns[7], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[6].compare(fns[8], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[7].compare(fns[8], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertFalse(fns[1].compare(fns[3], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertFalse(fns[1].compare(fns[4], Function.CompareMode.IS_INDISTINGUISHABLE));

    Assert.assertFalse(fns[9].compare(fns[4], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[2].compare(fns[9], Function.CompareMode.IS_SUPERTYPE_OF));

    Assert.assertTrue(fns[8].compare(fns[10], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[10].compare(fns[8], Function.CompareMode.IS_SUPERTYPE_OF));

    Assert.assertTrue(fns[11].compare(fns[12], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[11].compare(fns[13], Function.CompareMode.IS_SUPERTYPE_OF));

    for (int i = 0; i < fns.length; ++i) {
      for (int j = 0; j < fns.length; ++j) {
        if (i == j) {
          Assert.assertTrue(
              fns[i].compare(fns[i], Function.CompareMode.IS_IDENTICAL));
          Assert.assertTrue(
              fns[i].compare(fns[i], Function.CompareMode.IS_INDISTINGUISHABLE));
          Assert.assertTrue(
              fns[i].compare(fns[i], Function.CompareMode.IS_SUPERTYPE_OF));
        } else {
          Assert.assertFalse(fns[i].compare(fns[j], Function.CompareMode.IS_IDENTICAL));
          if (fns[i].compare(fns[j], Function.CompareMode.IS_INDISTINGUISHABLE)) {
            // If it's a indistinguishable, at least one of them must be a super type
            // of the other
            Assert.assertTrue(
                fns[i].compare(fns[j], Function.CompareMode.IS_SUPERTYPE_OF) ||
                fns[j].compare(fns[i], Function.CompareMode.IS_SUPERTYPE_OF));
          } else if (fns[i].compare(fns[j], Function.CompareMode.IS_INDISTINGUISHABLE)) {
            // This is reflexive
            Assert.assertTrue(
                fns[j].compare(fns[i], Function.CompareMode.IS_INDISTINGUISHABLE));
          }
        }
      }
    }
  }
}
