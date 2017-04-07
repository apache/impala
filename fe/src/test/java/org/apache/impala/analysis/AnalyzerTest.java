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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.thrift.TExpr;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class AnalyzerTest extends FrontendTestBase {
  protected final static Logger LOG = LoggerFactory.getLogger(AnalyzerTest.class);

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
    AnalyzesOk(uqQuery, createAnalysisCtx(tbl.getDb()));
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
    AnalysisError(uqQuery, createAnalysisCtx(tbl.getDb()), expectedError);
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
    SelectStmt stmt = (SelectStmt) AnalyzesOk("select * from functional.AllTypes");
    Analyzer analyzer = stmt.getAnalyzer();
    DescriptorTable descTbl = analyzer.getDescTbl();
    TupleDescriptor tupleDesc = descTbl.getTupleDesc(new TupleId(0));
    tupleDesc.materializeSlots();
    descTbl.computeMemLayout();

    Assert.assertEquals(97.0f, tupleDesc.getAvgSerializedSize(), 0.0);
    checkLayoutParams("functional.alltypes.date_string_col", 16, 0, 88, 0, analyzer);
    checkLayoutParams("functional.alltypes.string_col", 16, 16, 88, 1, analyzer);
    checkLayoutParams("functional.alltypes.timestamp_col", 16, 32, 88, 2, analyzer);
    checkLayoutParams("functional.alltypes.bigint_col", 8, 48, 88, 3, analyzer);
    checkLayoutParams("functional.alltypes.double_col", 8, 56, 88, 4, analyzer);
    checkLayoutParams("functional.alltypes.id", 4, 64, 88, 5, analyzer);
    checkLayoutParams("functional.alltypes.int_col", 4, 68, 88, 6, analyzer);
    checkLayoutParams("functional.alltypes.float_col", 4, 72, 88, 7, analyzer);
    checkLayoutParams("functional.alltypes.year", 4, 76, 89, 0, analyzer);
    checkLayoutParams("functional.alltypes.month", 4, 80, 89, 1, analyzer);
    checkLayoutParams("functional.alltypes.smallint_col", 2, 84, 89, 2, analyzer);
    checkLayoutParams("functional.alltypes.bool_col", 1, 86, 89, 3, analyzer);
    checkLayoutParams("functional.alltypes.tinyint_col", 1, 87, 89, 4, analyzer);
  }

  private void testNonNullable() throws AnalysisException {
    // both slots are non-nullable bigints. The layout should look like:
    // (byte range : data)
    // 0 - 7: count(int_col)
    // 8 - 15: count(*)
    SelectStmt stmt = (SelectStmt) AnalyzesOk(
        "select count(int_col), count(*) from functional.AllTypes");
    DescriptorTable descTbl = stmt.getAnalyzer().getDescTbl();
    TupleDescriptor aggDesc = descTbl.getTupleDesc(new TupleId(1));
    aggDesc.materializeSlots();
    descTbl.computeMemLayout();
    Assert.assertEquals(16.0f, aggDesc.getAvgSerializedSize(), 0.0);
    Assert.assertEquals(16, aggDesc.getByteSize());
    checkLayoutParams(aggDesc.getSlots().get(0), 8, 0, 0, -1);
    checkLayoutParams(aggDesc.getSlots().get(1), 8, 8, 0, -1);
  }

  private void testMixedNullable() throws AnalysisException {
    // one slot is nullable, one is not. The layout should look like:
    // (byte range : data)
    // 0 - 7: sum(int_col)
    // 8 - 15: count(*)
    // 16 - 17: nullable-byte (only 1 bit used)
    SelectStmt stmt = (SelectStmt) AnalyzesOk(
        "select sum(int_col), count(*) from functional.AllTypes");
    DescriptorTable descTbl = stmt.getAnalyzer().getDescTbl();
    TupleDescriptor aggDesc = descTbl.getTupleDesc(new TupleId(1));
    aggDesc.materializeSlots();
    descTbl.computeMemLayout();
    Assert.assertEquals(16.0f, aggDesc.getAvgSerializedSize(), 0.0);
    Assert.assertEquals(17, aggDesc.getByteSize());
    checkLayoutParams(aggDesc.getSlots().get(0), 8, 0, 16, 0);
    checkLayoutParams(aggDesc.getSlots().get(1), 8, 8, 0, -1);
  }

  /**
   * Tests that computeMemLayout() ignores non-materialized slots.
   */
  private void testNonMaterializedSlots() throws AnalysisException {
    SelectStmt stmt = (SelectStmt) AnalyzesOk("select * from functional.alltypes");
    Analyzer analyzer = stmt.getAnalyzer();
    DescriptorTable descTbl = analyzer.getDescTbl();
    TupleDescriptor tupleDesc = descTbl.getTupleDesc(new TupleId(0));
    tupleDesc.materializeSlots();
    // Mark slots 0 (id), 7 (double_col), 9 (string_col) as non-materialized.
    ArrayList<SlotDescriptor> slots = tupleDesc.getSlots();
    slots.get(0).setIsMaterialized(false);
    slots.get(7).setIsMaterialized(false);
    slots.get(9).setIsMaterialized(false);
    descTbl.computeMemLayout();

    Assert.assertEquals(68.0f, tupleDesc.getAvgSerializedSize(), 0.0);
    // Check non-materialized slots.
    checkLayoutParams("functional.alltypes.id", 0, -1, 0, 0, analyzer);
    checkLayoutParams("functional.alltypes.double_col", 0, -1, 0, 0, analyzer);
    checkLayoutParams("functional.alltypes.string_col", 0, -1, 0, 0, analyzer);
    // Check materialized slots.
    checkLayoutParams("functional.alltypes.date_string_col", 16, 0, 60, 0, analyzer);
    checkLayoutParams("functional.alltypes.timestamp_col", 16, 16, 60, 1, analyzer);
    checkLayoutParams("functional.alltypes.bigint_col", 8, 32, 60, 2, analyzer);
    checkLayoutParams("functional.alltypes.int_col", 4, 40, 60, 3, analyzer);
    checkLayoutParams("functional.alltypes.float_col", 4, 44, 60, 4, analyzer);
    checkLayoutParams("functional.alltypes.year", 4, 48, 60, 5, analyzer);
    checkLayoutParams("functional.alltypes.month", 4, 52, 60, 6, analyzer);
    checkLayoutParams("functional.alltypes.smallint_col", 2, 56, 60, 7, analyzer);
    checkLayoutParams("functional.alltypes.bool_col", 1, 58, 61, 0, analyzer);
    checkLayoutParams("functional.alltypes.tinyint_col", 1, 59, 61, 1, analyzer);
  }

  private void checkLayoutParams(SlotDescriptor d, int byteSize, int byteOffset,
      int nullIndicatorByte, int nullIndicatorBit) {
    Assert.assertEquals(byteSize, d.getByteSize());
    Assert.assertEquals(byteOffset, d.getByteOffset());
    Assert.assertEquals(nullIndicatorByte, d.getNullIndicatorByte());
    Assert.assertEquals(nullIndicatorBit, d.getNullIndicatorBit());
  }

  private void checkLayoutParams(String colAlias, int byteSize, int byteOffset,
      int nullIndicatorByte, int nullIndicatorBit, Analyzer analyzer) {
    SlotDescriptor d = analyzer.getSlotDescriptor(colAlias);
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
    // Unsupported type date in a star expansion.
    AnalysisError("select * from functional.unsupported_types",
        "Unsupported type 'DATE' in 'functional.unsupported_types.date_col'.");
    // Mixed supported/unsupported types.
    AnalysisError("select int_col, str_col, bin_col " +
        "from functional.unsupported_types",
        "Unsupported type 'BINARY' in 'bin_col'.");
    AnalysisError("create table tmp as select * from functional.unsupported_types",
        "Unsupported type 'DATE' in 'functional.unsupported_types.date_col'.");
    // Unsupported type in the target insert table.
    AnalysisError("insert into functional.unsupported_types " +
        "values(null, null, null, null, null, null)",
        "Unable to INSERT into target table (functional.unsupported_types) because " +
        "the column 'date_col' has an unsupported type 'DATE'");
    // Unsupported partition-column type.
    AnalysisError("select * from functional.unsupported_partition_types",
        "Failed to load metadata for table: 'functional.unsupported_partition_types'");

    // Try with hbase
    AnalyzesOk("describe functional_hbase.allcomplextypes");

    for (ScalarType t: Type.getUnsupportedTypes()) {
      // Create/Alter table.
      AnalysisError(String.format("create table new_table (new_col %s)", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format(
          "create table new_table (new_col int) PARTITIONED BY (p_col %s)", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format(
          "alter table functional.alltypes add columns (new_col %s)", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format(
          "alter table functional.alltypes change column int_col new_col %s", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));

      // UDFs.
      final String udfSuffix = " LOCATION '/test-warehouse/libTestUdfs.so' " +
          "SYMBOL='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10BooleanValE'";
      AnalysisError(String.format(
          "create function foo(VARCHAR(5)) RETURNS %s" + udfSuffix, t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format(
          "create function foo(%s) RETURNS int" + udfSuffix, t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));

      // UDAs.
      final String udaSuffix = " LOCATION '/test-warehouse/libTestUdas.so' " +
          "UPDATE_FN='AggUpdate'";
      AnalysisError(String.format("create aggregate function foo(string, double) " +
          "RETURNS %s" + udaSuffix, t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format("create aggregate function foo(%s, double) " +
          "RETURNS int" + udaSuffix, t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));

      // Cast,
      AnalysisError(String.format("select cast('abc' as %s)", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));

    }
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
    AnalyzesOk("refresh functional.alltypessmall partition (year=2009, month=1)");
    AnalyzesOk("refresh functional.alltypessmall partition (year=2009, month=NULL)");

    // invalidate metadata <table name> checks the Hive Metastore for table existence
    // and should not throw an AnalysisError if the table or db does not exist.
    AnalyzesOk("invalidate metadata functional.unknown_table");
    AnalyzesOk("invalidate metadata unknown_db.unknown_table");

    AnalysisError("refresh functional.unknown_table",
        "Table does not exist: functional.unknown_table");
    AnalysisError("refresh unknown_db.unknown_table",
        "Database does not exist: unknown_db");
    AnalysisError("refresh functional.alltypessmall partition (year=2009, int_col=10)",
        "Column 'int_col' is not a partition column in table: functional.alltypessmall");
    AnalysisError("refresh functional.alltypessmall partition (year=2009)",
        "Items in partition spec must exactly match the partition columns in "
            + "the table definition: functional.alltypessmall (1 vs 2)");
    AnalysisError("refresh functional.alltypessmall partition (year=2009, year=2009)",
        "Duplicate partition key name: year");
    AnalysisError(
        "refresh functional.alltypessmall partition (year=2009, month='foo')",
        "Value of partition spec (column=month) has incompatible type: 'STRING'. "
            + "Expected type: 'INT'");
    AnalysisError("refresh functional.zipcode_incomes partition (year=2009, month=1)",
        "Table is not partitioned: functional.zipcode_incomes");
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

    // Analysis error from explain upsert
    AnalysisError("explain upsert into table functional.alltypes select * from " +
        "functional.alltypes", "UPSERT is only supported for Kudu tables");

    // Positive test for explain query
    AnalyzesOk("explain select * from functional.AllTypes");

    // Positive test for explain insert
    AnalyzesOk("explain insert into table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, int_col, " +
        "float_col, float_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");

    // Positive test for explain upsert
    AnalyzesOk("explain upsert into table functional_kudu.testtbl select * from " +
        "functional_kudu.testtbl");
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
    AnalysisError("select id, bool_col from functional.AllTypes limit count(*)",
        "LIMIT expression must be a constant expression: count(*)");
    AnalysisError("select id, bool_col from functional.AllTypes order by id limit 10 " +
        "offset count(*)",
        "OFFSET expression must be a constant expression: count(*)");

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
    AnalyzesOk("show create table functional.alltypes_view");
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
