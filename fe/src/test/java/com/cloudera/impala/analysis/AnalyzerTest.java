// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExpr;

public class AnalyzerTest {
  protected final static Logger LOG = LoggerFactory.getLogger(AnalyzerTest.class);
  protected static Catalog catalog;

  protected Analyzer analyzer;

  // maps from type to string that will result in literal of that type
  protected static Map<PrimitiveType, String> typeToLiteralValue =
      new HashMap<PrimitiveType, String>();
  static {
    typeToLiteralValue.put(PrimitiveType.BOOLEAN, "true");
    typeToLiteralValue.put(PrimitiveType.TINYINT, "1");
    typeToLiteralValue.put(PrimitiveType.SMALLINT, (Byte.MAX_VALUE + 1) + "");
    typeToLiteralValue.put(PrimitiveType.INT, (Short.MAX_VALUE + 1) + "");
    typeToLiteralValue.put(PrimitiveType.BIGINT, ((long) Integer.MAX_VALUE + 1) + "");
    typeToLiteralValue.put(PrimitiveType.FLOAT, "1.0");
    typeToLiteralValue.put(PrimitiveType.DOUBLE, (Float.MAX_VALUE + 1) + "");
    typeToLiteralValue.put(PrimitiveType.DATE, "'2012-12-21'");
    typeToLiteralValue.put(PrimitiveType.DATETIME, "'2012-12-21 00:00:00'");
    typeToLiteralValue.put(PrimitiveType.TIMESTAMP, "'2012-12-21 00:00:00.000'");
  }

  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new Catalog();
  }

  @AfterClass
  public static void cleanUp() {
    catalog.close();
  }

  /**
   * Check whether SelectStmt components can be converted to thrift.
   */
  protected void CheckSelectToThrift(SelectStmt node) {
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
      System.err.println(e.toString());
      fail("\nParser error:\n" + parser.getErrorMsg(stmt));
    }
    assertNotNull(node);
    return node;
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   */
  public ParseNode AnalyzesOk(String stmt) {
    LOG.info("analyzing " + stmt);
    ParseNode node = ParsesOk(stmt);
    assertNotNull(node);
    analyzer = new Analyzer(catalog);
    try {
      node.analyze(analyzer);
    } catch (AnalysisException e) {
      fail("Analysis error:\n" + e.toString());
    } catch (AuthorizationException e) {
      fail("Authorization error:\n" + e.toString());
    }
    if (node instanceof SelectStmt) {
      CheckSelectToThrift((SelectStmt) node);
    } else if (node instanceof InsertStmt) {
      InsertStmt insertStmt = (InsertStmt) node;
      if (insertStmt.getQueryStmt() instanceof SelectStmt) {
        CheckSelectToThrift((SelectStmt) insertStmt.getQueryStmt());
      }
    }
    return node;
  }

  /**
   * Asserts if stmt passes analysis or the error string doesn't match and it
   * is non-null.
   */
  public void AnalysisError(String stmt, String expectedErrorString) {
    LOG.info("analyzing " + stmt);
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    ParseNode node = null;
    try {
      node = (ParseNode) parser.parse().value;
    } catch (Exception e) {
      System.err.println(e.toString());
      fail("\nParser error:\n" + parser.getErrorMsg(stmt));
    }
    assertNotNull(node);
    analyzer = new Analyzer(catalog);
    try {
      node.analyze(analyzer);
    } catch (AnalysisException e) {
      if (expectedErrorString != null) {
        String errorString = e.getMessage();
        Assert.assertTrue(
            "got error:\n" + errorString + "\nexpected:\n" + expectedErrorString,
            errorString.startsWith(expectedErrorString));
      }
      return;
    } catch (AuthorizationException e) {
      fail("Authorization error: " + e.getMessage());
    }
    fail("Stmt didn't result in analysis error: " + stmt);
  }

  /**
   * Asserts if stmt passes analysis.
   */
  public void AnalysisError(String stmt) {
    AnalysisError(stmt, null);
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
  public void TestMemLayout() throws AnalysisException, AuthorizationException {
    testSelectStar();
    testNonNullable();
    TestMixedNullable();
  }

  private void testSelectStar() throws AnalysisException, AuthorizationException {
    AnalyzesOk("select * from functional.AllTypes");
    DescriptorTable descTbl = analyzer.getDescTbl();
    for (SlotDescriptor slotD : descTbl.getTupleDesc(new TupleId(0)).getSlots()) {
      slotD.setIsMaterialized(true);
    }
    descTbl.computeMemLayout();
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
    checkLayoutParams("functional.alltypes.string_col", strSlotSize, 48 + strSlotSize, 1, 3);
  }

  private void testNonNullable() throws AnalysisException {
    // both slots are non-nullable bigints. The layout should look like:
    // (byte range : data)
    // 0 - 7: count(int_col)
    // 8 - 15: count(*)
    AnalyzesOk("select count(int_col), count(*) from functional.AllTypes");
    DescriptorTable descTbl = analyzer.getDescTbl();
    com.cloudera.impala.analysis.TupleDescriptor aggDesc =
        descTbl.getTupleDesc(new TupleId(1));
    for (SlotDescriptor slotD: aggDesc.getSlots()) {
      slotD.setIsMaterialized(true);
    }
    descTbl.computeMemLayout();
    Assert.assertEquals(aggDesc.getByteSize(), 16);
    checkLayoutParams(aggDesc.getSlots().get(0), 8, 0, 0, -1);
    checkLayoutParams(aggDesc.getSlots().get(1), 8, 8, 0, -1);
  }

  private void TestMixedNullable() throws AnalysisException {
    // one slot is nullable, one is not. The layout should look like:
    // (byte range : data)
    // 0 : 1 nullable-byte (only 1 bit used)
    // 1 - 7: padded bytes
    // 8 - 15: sum(int_col)
    // 16 - 23: count(*)
    AnalyzesOk("select sum(int_col), count(*) from functional.AllTypes");
    DescriptorTable descTbl = analyzer.getDescTbl();
    com.cloudera.impala.analysis.TupleDescriptor aggDesc =
        descTbl.getTupleDesc(new TupleId(1));
    for (SlotDescriptor slotD: aggDesc.getSlots()) {
      slotD.setIsMaterialized(true);
    }
    descTbl.computeMemLayout();
    Assert.assertEquals(aggDesc.getByteSize(), 24);
    checkLayoutParams(aggDesc.getSlots().get(0), 8, 8, 0, 0);
    checkLayoutParams(aggDesc.getSlots().get(1), 8, 16, 0, -1);
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
    SlotDescriptor d = analyzer.getSlotDescriptor(colAlias);
    checkLayoutParams(d, byteSize, byteOffset, nullIndicatorByte, nullIndicatorBit);
  }

  /**
   * We distinguish between three classes of unsupported types:
   * 1. Complex types, e.g., map
   *    For tables with such types we prevent loading the table metadata.
   * 2. Primitive types
   *    For tables with unsupported primitive types (e.g., decimal)
   *    we can run queries as long as the unsupported columns are not referenced.
   *    We fail analysis if a query references an unsupported primitive column.
   * 3. Partition-column types
   *    We do not support table partitioning on timestamp columns
   */
  @Test
  public void TestUnsupportedTypes() throws AuthorizationException {
    // The table metadata should not have been loaded.
    AnalysisError("select * from functional.map_table",
        "Failed to load metadata for table: functional.map_table");

    // Select supported types from a table with mixed supported/unsupported types.
    AnalyzesOk("select int_col, str_col, bigint_col from functional.unsupported_types");
    // Unsupported type decimal.
    AnalysisError("select dec_col from functional.unsupported_types",
        "Unsupported type 'DECIMAL' in 'dec_col'.");
    // Unsupported type binary.
    AnalysisError("select bin_col from functional.unsupported_types",
        "Unsupported type 'BINARY' in 'bin_col'.");
    // Mixed supported/unsupported types.
    AnalysisError("select int_col, dec_col, str_col, bin_col " +
        "from functional.unsupported_types",
        "Unsupported type 'DECIMAL' in 'dec_col'.");
    // Unsupported partition-column type.
    AnalysisError("select * from functional.unsupported_partition_types",
        "Failed to load metadata for table: functional.unsupported_partition_types");
  }

  @Test
  public void TestUnsupportedSerde() {
    AnalysisError("select * from functional.bad_serde",
                  "Failed to load metadata for table: functional.bad_serde");
  }

  @Test
  public void TestExplain() {
    // Analysis error from explain insert: too many partitioning columns.
    AnalysisError("explain insert into table functional.alltypessmall " +
        "partition (year=2009, month=4, year=10)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Superfluous columns in PARTITION clause: year.");

    // Analysis error from explain query
    AnalysisError("explain " +
    		"select id from (select id+2 from functional.hbasealltypessmall) a",
        "couldn't resolve column reference: 'id'");

    // Positive test for explain query
    AnalyzesOk("explain select * from functional.AllTypes");

    // Positive test for explain insert
    AnalyzesOk("explain insert into table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, int_col, " +
        "float_col, float_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");
  }
}
