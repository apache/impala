// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TExpr;
import com.google.common.base.Preconditions;

public class AnalyzerTest {
  private final static Logger LOG = LoggerFactory.getLogger(AnalyzerTest.class);
  private static Catalog catalog;

  private Analyzer analyzer;

  // maps from type to string that will result in literal of that type
  private static Map<PrimitiveType, String> typeToLiteralValue =
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

  /**
   * Check whether SelectStmt components can be converted to thrift.
   *
   * @param node
   * @return
   */
  private void CheckSelectToThrift(SelectStmt node) {
    // convert select list exprs and where clause to thrift
    List<Expr> selectListExprs = node.getSelectListExprs();
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
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   *
   * @param stmt
   * @return
   * @throws AnalysisException
   */
  public ParseNode AnalyzesOk(String stmt) throws AnalysisException {
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
      fail("Analysis error:\n" + e.toString());
    } catch (InternalException e) {
      fail("Internal exception:\n" + e.toString());
    }
    if(node instanceof SelectStmt) {
      CheckSelectToThrift((SelectStmt)node);
    } else if (node instanceof InsertStmt) {
      InsertStmt insertStmt = (InsertStmt) node;
      CheckSelectToThrift(insertStmt.getSelectStmt());
    }
    return node;
  }

  /**
   * Asserts if stmt passes analysis or the error string doesn't match and it
   * is non-null.
   *
   * @param stmt
   * @param expectedErrorString
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
    } catch (InternalException e) {
      fail("Internal exception:\n" + e.toString());
    }

    fail("Stmt didn't result in analysis error: " + stmt);
  }

  /**
   * Asserts if stmt passes analysis.
   *
   * @param stmt
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
      Assert.assertEquals(expr.getChild(0).getType(), expr.getChild(1).getType());
    }
    for (Expr child: expr.getChildren()) {
      checkBinaryExprs(child);
    }
  }

  @Test
  public void TestMemLayout() throws AnalysisException {
    TestSelectStar();
    TestNonNullable();
    TestMixedNullable();
  }

  private void TestSelectStar() throws AnalysisException {
    AnalyzesOk("select * from AllTypes");
    DescriptorTable descTbl = analyzer.getDescTbl();
    for (SlotDescriptor slotD: descTbl.getTupleDesc(new TupleId(0)).getSlots()) {
      slotD.setIsMaterialized(true);
    }
    descTbl.computeMemLayout();
    checkLayoutParams("alltypes.bool_col", 1, 2, 0, 0);
    checkLayoutParams("alltypes.tinyint_col", 1, 3, 0, 1);
    checkLayoutParams("alltypes.smallint_col", 2, 4, 0, 2);
    checkLayoutParams("alltypes.year", 4, 8, 0, 3);
    checkLayoutParams("alltypes.month", 4, 12, 0, 4);
    checkLayoutParams("alltypes.id", 4, 16, 0, 5);
    checkLayoutParams("alltypes.int_col", 4, 20, 0, 6);
    checkLayoutParams("alltypes.float_col", 4, 24, 0, 7);
    checkLayoutParams("alltypes.bigint_col", 8, 32, 1, 0);
    checkLayoutParams("alltypes.double_col", 8, 40, 1, 1);
    int strSlotSize = PrimitiveType.STRING.getSlotSize();
    checkLayoutParams("alltypes.date_string_col", strSlotSize, 48, 1, 2);
    checkLayoutParams("alltypes.string_col", strSlotSize, 48 + strSlotSize, 1, 3);
  }

  private void TestNonNullable() throws AnalysisException {
    // both slots are non-nullable bigints.  The layout should look like:
    // (byte range : data)
    // 0 - 7:  count(int_col)
    // 8 - 15: count(*)
    AnalyzesOk("select count(int_col), count(*) from AllTypes");
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
    // one slot is nullable, one is not.  The layout should look like:
    // (byte range : data)
    // 0 : 1 nullable-byte (only 1 bit used)
    // 1 - 7: padded bytes
    // 8 - 15: sum(int_col)
    // 16 - 23: count(*)
    AnalyzesOk("select sum(int_col), count(*) from AllTypes");
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

  @Test
  public void TestSubquery() throws AnalysisException {
    AnalyzesOk("select y x from (select id y from hbasealltypessmall) a");
    AnalyzesOk("select id from (select id from hbasealltypessmall) a");
    AnalyzesOk("select * from (select id+2 from hbasealltypessmall) a");
    AnalyzesOk("select t1 c from " +
        "(select c t1 from (select id c from hbasealltypessmall) t1) a");
    AnalysisError("select id from (select id+2 from hbasealltypessmall) a",
        "couldn't resolve column reference: 'id'");
    AnalyzesOk("select a.* from (select id+2 from hbasealltypessmall) a");

    // join test
    AnalyzesOk("select * from (select id+2 id from hbasealltypessmall) a " +
        "join (select * from AllTypes where true) b");
    AnalyzesOk("select a.x from (select count(id) x from AllTypes) a");
    AnalyzesOk("select a.* from (select count(id) from AllTypes) a");
    AnalysisError("select a.id from (select id y from hbasealltypessmall) a",
        "unknown column 'id' (table alias 'a')");
    AnalyzesOk("select * from (select * from AllTypes) a where year = 2009");
    AnalyzesOk("select * from (select * from alltypesagg) a right outer join " +
        "                    (select * from alltypessmall) b using (id, int_col) " +
        "       where a.day >= 6 and b.month > 2 and a.tinyint_col = 15 and " +
        "             b.string_col = '15' and a.tinyint_col + b.tinyint_col < 15");
    AnalyzesOk("select * from (select a.smallint_col+b.smallint_col  c1" +
        "                      from alltypesagg a join alltypessmall b " +
        "                           using (id, int_col)) x " +
        "       where x.c1 > 100");
    AnalyzesOk("select a.* from" +
        " (select * from (select id+2 from hbasealltypessmall) b) a");
    AnalysisError("select * from " +
        "(select * from alltypes a join alltypes b on (a.int_col = b.int_col)) x",
        "duplicated inline view column alias: 'year' in inline view 'x'");

    // subquery on the rhs of the join
    AnalyzesOk("select x.float_col "+
        "       from alltypessmall c join " +
        "            (select a.smallint_col smallint_col, a.tinyint_col tinyint_col, " +
        "                   a.int_col int_col, b.float_col float_col" +
        "             from (select * from alltypesagg a where month=1) a join " +
        "                  alltypessmall b on (a.smallint_col = b.id)) x " +
        "            on (x.tinyint_col = c.id)");

    // aggregate test
    AnalyzesOk("select count(*) from (select count(id) from AllTypes group by id) a");
    AnalyzesOk("select count(a.x) from (select id+2 x from hbasealltypessmall) a");
    AnalyzesOk("select * from (select id, zip from (select * from testtbl) x " +
        "       group by zip, id having count(*) > 0) x");

    AnalysisError("select zip + count(*) from testtbl",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");

    // negative aggregate test
    AnalysisError("select * from " +
        "(select id, zip from testtbl group by id having count(*) > 0) x",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");
    AnalysisError("select * from " +
        "(select id from testtbl group by id having zip + count(*) > 0) x",
        "HAVING clause not produced by aggregation output " +
        "(missing from GROUP BY clause?)");
    AnalysisError("select * from " +
        "(select zip, count(*) from testtbl group by 3) x",
        "GROUP BY: ordinal exceeds number of items in select list");
    AnalysisError("select * from " +
        "(select * from alltypes group by 1) x",
        "cannot combine '*' in select list with GROUP BY");
    AnalysisError("select * from " +
        "(select zip, count(*) from testtbl group by count(*)) x",
        "GROUP BY expression must not contain aggregate functions");
    AnalysisError("select * from " +
        "(select zip, count(*) from testtbl group by count(*) + min(zip)) x",
        "GROUP BY expression must not contain aggregate functions");
    AnalysisError("select * from " +
        "(select zip, count(*) from testtbl group by 2) x",
        "GROUP BY expression must not contain aggregate functions");

    // multiple grouping cols
    AnalyzesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
        "group by string_col, int_col, bigint_col");
    AnalyzesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
        "group by 2, 1, 3");
    AnalysisError("select int_col, string_col, bigint_col, count(*) from alltypes " +
        "group by 2, 1, 4", "GROUP BY expression must not contain aggregate functions");
    // can't group by floating-point exprs
    AnalysisError("select float_col, count(*) from alltypes group by 1",
        "GROUP BY expression must have a discrete (non-floating point) type");
    AnalysisError("select int_col + 0.5, count(*) from alltypes group by 1",
        "GROUP BY expression must have a discrete (non-floating point) type");

    // order by, top-n
    AnalyzesOk("select * from (select zip, count(*) from (select * from testtbl) x " +
        "       group by 1 order by count(*) + min(zip) limit 5) x");
    AnalyzesOk("select c1, c2 from (select zip c1 , count(*) c2 " +
        "                           from (select * from testtbl) x group by 1) x "+
        "        order by 2, 1 limit 5");
  }

  @Test
  public void TestStar() throws AnalysisException {
    AnalyzesOk("select * from AllTypes");
    AnalyzesOk("select alltypes.* from AllTypes");
    // different db
    AnalyzesOk("select testdb1.alltypes.* from testdb1.alltypes");
    // two tables w/ identical names from different dbs
    AnalyzesOk("select alltypes.*, testdb1.alltypes.* from alltypes, testdb1.alltypes");
    AnalyzesOk("select * from alltypes, testdb1.alltypes");
  }

  @Test
  public void TestTimestampValueExprs() throws AnalysisException {
   AnalyzesOk("select cast (0 as timestamp)");
   AnalyzesOk("select cast (0.1 as timestamp)");
   AnalyzesOk("select cast ('1970-10-10 10:00:00.123' as timestamp)");
  }

  @Test
  public void TestBooleanValueExprs() throws AnalysisException {
    // Test predicates in where clause.
    AnalyzesOk("select * from AllTypes where true");
    AnalyzesOk("select * from AllTypes where false");
    AnalyzesOk("select * from AllTypes where bool_col = true");
    AnalyzesOk("select * from AllTypes where bool_col = false");
    AnalyzesOk("select * from AllTypes where true or false");
    AnalyzesOk("select * from AllTypes where true and false");
    AnalyzesOk("select * from AllTypes where true or false and bool_col = false");
    AnalyzesOk("select * from AllTypes where true and false or bool_col = false");
    // Test predicates in select list.
    AnalyzesOk("select bool_col = true from AllTypes");
    AnalyzesOk("select bool_col = false from AllTypes");
    AnalyzesOk("select true or false and bool_col = false from AllTypes");
    AnalyzesOk("select true and false or bool_col = false from AllTypes");
  }

  @Test
  public void TestOrdinals() throws AnalysisException {
    // can't group or order on *
    AnalysisError("select * from alltypes group by 1",
        "cannot combine '*' in select list with GROUP BY");
    AnalysisError("select * from alltypes order by 1",
        "ORDER BY: ordinal refers to '*' in select list");
  }

  @Test
  public void TestFromClause() throws AnalysisException {
    AnalyzesOk("select int_col from alltypes");
    AnalysisError("select int_col from badtbl", "unknown table");
    // case-insensitive
    AnalyzesOk("SELECT INT_COL FROM ALLTYPES");
    AnalyzesOk("select AllTypes.Int_Col from alltypes");
    // aliases work
    AnalyzesOk("select a.int_col from alltypes a");
    // implicit aliases
    AnalyzesOk("select int_col, zip from alltypes, testtbl");
    // duplicate alias
    AnalysisError("select a.int_col, a.id from alltypes a, testtbl a",
        "duplicate table alias");
    // duplicate implicit alias
    AnalysisError("select int_col from alltypes, alltypes", "duplicate table alias");

    // resolves dbs correctly
    AnalyzesOk("select zip from testtbl");
    AnalysisError("select zip from testdb1.testtbl", "couldn't resolve column reference");
  }

  @Test
  public void TestNoFromClause() throws AnalysisException {
    AnalyzesOk("select 'test'");
    AnalyzesOk("select 1 + 1, -128, 'two', 1.28");
    AnalyzesOk("select -1, 1 - 1, 10 - -1, 1 - - - 1");
    AnalyzesOk("select -1.0, 1.0 - 1.0, 10.0 - -1.0, 1.0 - - - 1.0");
    AnalysisError("select a + 1");
    // Test predicates in select list.
    AnalyzesOk("select true");
    AnalyzesOk("select false");
    AnalyzesOk("select true or false");
    AnalyzesOk("select true and false");
    // Test NULL's in select list.
    AnalyzesOk("select null");
    AnalyzesOk("select null and null");
    AnalyzesOk("select null or null");
  }

  @Test public void TestOnClause() throws AnalysisException {
    AnalyzesOk(
        "select a.int_col from alltypes a join alltypes b on (a.int_col = b.int_col)");
    AnalyzesOk(
        "select a.int_col " +
        "from alltypes a join alltypes b on " +
        "(a.int_col = b.int_col and a.string_col = b.string_col)");
    // ON or USING clause not required for inner join
    AnalyzesOk("select a.int_col from alltypes a join alltypes b");
    // unknown column
    AnalysisError(
        "select a.int_col from alltypes a join alltypes b on (a.int_col = b.badcol)",
        "unknown column 'badcol'");
    // ambiguous col ref
    AnalysisError(
        "select a.int_col from alltypes a join alltypes b on (int_col = int_col)",
        "Unqualified column reference 'int_col' is ambiguous");
    // unknown alias
    AnalysisError(
        "select a.int_col from alltypes a join alltypes b on " +
        "(a.int_col = badalias.int_col)",
        "unknown table alias: 'badalias'");
    // incompatible comparison
    AnalysisError(
        "select a.int_col from alltypes a join alltypes b on (a.bool_col = b.string_col)",
        "operands are not comparable: a.bool_col = b.string_col");
    AnalyzesOk(
        "select a.int_col, b.int_col, c.int_col " +
        "from alltypes a join alltypes b on " +
        "(a.int_col = b.int_col and a.string_col = b.string_col)" +
        "join alltypes c on " +
        "(b.int_col = c.int_col and b.string_col = c.string_col " +
        "and b.bool_col = c.bool_col)");
    // can't reference an alias that gets declared afterwards
    AnalysisError(
        "select a.int_col, b.int_col, c.int_col " +
        "from alltypes a join alltypes b on " +
        "(c.int_col = b.int_col and a.string_col = b.string_col)" +
        "join alltypes c on " +
        "(b.int_col = c.int_col and b.string_col = c.string_col " +
        "and b.bool_col = c.bool_col)",
        "unknown table alias: 'c'");

    // outer joins require ON/USING clause
    AnalyzesOk("select * from alltypes a left outer join alltypes b on (a.id = b.id)");
    AnalyzesOk("select * from alltypes a left outer join alltypes b using (id)");
    AnalysisError("select * from alltypes a left outer join alltypes b",
        "LEFT OUTER JOIN requires an ON or USING clause");
    AnalyzesOk("select * from alltypes a right outer join alltypes b on (a.id = b.id)");
    AnalyzesOk("select * from alltypes a right outer join alltypes b using (id)");
    AnalysisError("select * from alltypes a right outer join alltypes b",
        "RIGHT OUTER JOIN requires an ON or USING clause");
    AnalyzesOk("select * from alltypes a full outer join alltypes b on (a.id = b.id)");
    AnalyzesOk("select * from alltypes a full outer join alltypes b using (id)");
    AnalysisError("select * from alltypes a full outer join alltypes b",
        "FULL OUTER JOIN requires an ON or USING clause");

    // semi join requires ON/USING clause
    AnalyzesOk("select a.id from alltypes a left semi join alltypes b on (a.id = b.id)");
    AnalyzesOk("select a.id from alltypes a left semi join alltypes b using (id)");
    AnalysisError("select a.id from alltypes a left semi join alltypes b",
        "LEFT SEMI JOIN requires an ON or USING clause");
    // TODO: enable when implemented
    // must not reference semi-joined alias outside of join clause
    //AnalysisError(
        //"select a.id, b.id from alltypes a left semi join alltypes b on (a.id = b.id)",
        //"x");
  }

  @Test public void TestUsingClause() throws AnalysisException {
    AnalyzesOk("select a.int_col, b.int_col from alltypes a join alltypes b using (int_col)");
    AnalyzesOk("select a.int_col, b.int_col from alltypes a join alltypes b " +
        "using (int_col, string_col)");
    AnalyzesOk(
        "select a.int_col, b.int_col, c.int_col " +
        "from alltypes a join alltypes b using (int_col, string_col) " +
        "join alltypes c using (int_col, string_col, bool_col)");
    // unknown column
    AnalysisError("select a.int_col from alltypes a join alltypes b using (badcol)",
        "unknown column badcol for alias a");
    AnalysisError(
        "select a.int_col from alltypes a join alltypes b using (int_col, badcol)",
        "unknown column badcol for alias a ");
  }

  @Test
  public void TestWhereClause() throws AnalysisException {
    AnalyzesOk("select zip, name from testtbl where id > 15");
    AnalysisError("select zip, name from testtbl where badcol > 15",
        "couldn't resolve column reference");
    AnalyzesOk("select * from testtbl where true");
    AnalysisError("select * from testtbl where count(*) > 0",
        "aggregation function not allowed in WHERE clause");
    // NULL literal in binary predicate.
    for (BinaryPredicate.Operator op : BinaryPredicate.Operator.values()) {
      AnalyzesOk("select id from testtbl where id " +  op.toString() + " NULL");
    }
    // bool literal in binary predicate.
    for (BinaryPredicate.Operator op : BinaryPredicate.Operator.values()) {
      AnalyzesOk("select id from testtbl where id " +  op.toString() + " true");
      AnalyzesOk("select id from testtbl where id " +  op.toString() + " false");
    }
    // NULL literal predicate.
    AnalyzesOk("select id from testtbl where NULL OR NULL");
    AnalyzesOk("select id from testtbl where NULL AND NULL");
    AnalyzesOk("select id from testtbl where NOT NULL");
    // bool literal predicate
    AnalyzesOk("select id from testtbl where true");
    AnalyzesOk("select id from testtbl where false");
    AnalyzesOk("select id from testtbl where true OR true");
    AnalyzesOk("select id from testtbl where true OR false");
    AnalyzesOk("select id from testtbl where false OR false");
    AnalyzesOk("select id from testtbl where false OR true");
    AnalyzesOk("select id from testtbl where true AND true");
    AnalyzesOk("select id from testtbl where true AND false");
    AnalyzesOk("select id from testtbl where false AND false");
    AnalyzesOk("select id from testtbl where false AND true");
  }

  @Test
  public void TestAggregates() throws AnalysisException {
    AnalyzesOk("select count(*), min(id), max(id), sum(id), avg(id) from testtbl");
    AnalyzesOk("select count(id, zip) from testtbl");
    AnalysisError("select id, zip from testtbl where count(*) > 0",
        "aggregation function not allowed in WHERE clause");

    // only count() allows '*'
    AnalysisError("select avg(*) from testtbl",
        "'*' can only be used in conjunction with COUNT");
    AnalysisError("select min(*) from testtbl",
        "'*' can only be used in conjunction with COUNT");
    AnalysisError("select max(*) from testtbl",
        "'*' can only be used in conjunction with COUNT");
    AnalysisError("select sum(*) from testtbl",
        "'*' can only be used in conjunction with COUNT");

    // multiple args
    AnalyzesOk("select count(id, zip) from testtbl");
    AnalysisError("select min(id, zip) from testtbl",
        "MIN requires exactly one parameter");
    AnalysisError("select max(id, zip) from testtbl",
        "MAX requires exactly one parameter");
    AnalysisError("select sum(id, zip) from testtbl",
        "SUM requires exactly one parameter");
    AnalysisError("select avg(id, zip) from testtbl",
        "AVG requires exactly one parameter");

    // nested aggregates
    AnalysisError("select sum(count(*)) from testtbl",
        "aggregate function cannot contain aggregate parameters");

    // wrong type
    AnalysisError("select sum(timestamp_col) from alltypes",
        "SUM requires a numeric parameter: SUM(timestamp_col)");
    AnalysisError("select sum(string_col) from alltypes",
        "SUM requires a numeric parameter: SUM(string_col)");
    AnalysisError("select avg(string_col) from alltypes",
        "AVG requires a numeric or timestamp parameter: AVG(string_col)");
  }

  @Test
  public void TestDistinct() throws AnalysisException {
    // DISTINCT
    AnalyzesOk("select distinct id, zip from testtbl");
    AnalyzesOk("select distinct * from testtbl");
    AnalysisError("select distinct count(*) from testtbl",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip from testtbl group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip, count(*) from testtbl group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalyzesOk("select count(distinct id, zip) from testtbl");
    AnalysisError("select count(distinct id, zip), count(distinct zip) from testtbl",
        "all DISTINCT aggregate functions need to have the same set of parameters");
    AnalyzesOk("select tinyint_col, count(distinct int_col, bigint_col) "
        + "from alltypesagg group by 1");
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "sum(distinct int_col) from alltypesagg group by 1");
    AnalysisError("select tinyint_col, count(distinct int_col),"
        + "sum(distinct bigint_col) from alltypesagg group by 1",
        "all DISTINCT aggregate functions need to have the same set of parameters");
    // min and max are ignored in terms of DISTINCT
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "min(distinct smallint_col), max(distinct string_col) "
        + "from alltypesagg group by 1");
  }

  @Test
  public void TestDistinctInlineView() throws AnalysisException {
    // DISTINCT
    AnalyzesOk("select distinct id from " +
        "(select distinct id, zip from (select * from testtbl) x) y");
    AnalyzesOk("select distinct * from " +
        "(select distinct * from (Select * from testtbl) x) y");
    AnalyzesOk("select distinct * from (select count(*) from testtbl) x");
    AnalyzesOk("select count(distinct id, zip) from (select * from testtbl) x");
    AnalyzesOk("select * from (select tinyint_col, count(distinct int_col, bigint_col) "
        + "from (select * from alltypesagg) x group by 1) y");
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "sum(distinct int_col) from (select * from alltypesagg) x group by 1");

    // Error case when distinct is inside an inline view
    AnalysisError("select * from " + "(select distinct count(*) from testtbl) x",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select * from " +
        "(select distinct id, zip from testtbl group by 1, 2) x",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select * from " +
        "(select distinct id, zip, count(*) from testtbl group by 1, 2) x",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select * from " +
        "(select count(distinct id, zip), count(distinct zip) from testtbl) x",
        "all DISTINCT aggregate functions need to have the same set of parameters");
    AnalysisError("select * from " + "(select tinyint_col, count(distinct int_col),"
        + "sum(distinct bigint_col) from alltypesagg group by 1) x",
        "all DISTINCT aggregate functions need to have the same set of parameters");

    // Error case when inline view is in the from clause
    AnalysisError("select distinct count(*) from (select * from testtbl) x",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip from (select * from testtbl) x group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip, count(*) from " +
        "(select * from testtbl) x group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalyzesOk("select count(distinct id, zip) from (select * from testtbl) x");
    AnalysisError("select count(distinct id, zip), count(distinct zip) " +
        " from (select * from testtbl) x",
        "all DISTINCT aggregate functions need to have the same set of parameters");
    AnalyzesOk("select tinyint_col, count(distinct int_col, bigint_col) "
        + "from (select * from alltypesagg) x group by 1");
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "sum(distinct int_col) from (select * from alltypesagg) x group by 1");
    AnalysisError("select tinyint_col, count(distinct int_col),"
        + "sum(distinct bigint_col) from (select * from alltypesagg) x group by 1",
        "all DISTINCT aggregate functions need to have the same set of parameters");
  }

  @Test
  public void TestGroupBy() throws AnalysisException {
    AnalyzesOk("select zip, count(*) from testtbl group by zip");
    AnalyzesOk("select zip + count(*) from testtbl group by zip");
    // doesn't group by all non-agg select list items
    AnalysisError("select zip, count(*) from testtbl",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");
    AnalysisError("select zip + count(*) from testtbl",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");

    AnalyzesOk("select id, zip from testtbl group by zip, id having count(*) > 0");
    AnalysisError("select id, zip from testtbl group by id having count(*) > 0",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");
    AnalysisError("select id from testtbl group by id having zip + count(*) > 0",
        "HAVING clause not produced by aggregation output " +
        "(missing from GROUP BY clause?)");
    // resolves ordinals
    AnalyzesOk("select zip, count(*) from testtbl group by 1");
    AnalyzesOk("select count(*), zip from testtbl group by 2");
    AnalysisError("select zip, count(*) from testtbl group by 3",
        "GROUP BY: ordinal exceeds number of items in select list");
    AnalysisError("select * from alltypes group by 1",
        "cannot combine '*' in select list with GROUP BY");
    // picks up select item alias
    AnalyzesOk("select zip z, count(*) from testtbl group by z");

    // can't group by aggregate
    AnalysisError("select zip, count(*) from testtbl group by count(*)",
        "GROUP BY expression must not contain aggregate functions");
    AnalysisError("select zip, count(*) from testtbl group by count(*) + min(zip)",
        "GROUP BY expression must not contain aggregate functions");
    AnalysisError("select zip, count(*) from testtbl group by 2",
        "GROUP BY expression must not contain aggregate functions");

    // multiple grouping cols
    AnalyzesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
        "group by string_col, int_col, bigint_col");
    AnalyzesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
        "group by 2, 1, 3");
    AnalysisError("select int_col, string_col, bigint_col, count(*) from alltypes " +
        "group by 2, 1, 4", "GROUP BY expression must not contain aggregate functions");
    // can't group by floating-point exprs
    AnalysisError("select float_col, count(*) from alltypes group by 1",
        "GROUP BY expression must have a discrete (non-floating point) type");
    AnalysisError("select int_col + 0.5, count(*) from alltypes group by 1",
        "GROUP BY expression must have a discrete (non-floating point) type");
  }

  @Test public void TestAvgSubstitution() throws AnalysisException {
    SelectStmt select = (SelectStmt) AnalyzesOk(
        "select avg(id) from testtbl having count(id) > 0 order by avg(zip)");
    ArrayList<Expr> selectListExprs = select.getSelectListExprs();
    assertNotNull(selectListExprs);
    assertEquals(selectListExprs.size(), 1);
    // all agg exprs are replaced with refs to agg output slots
    Expr havingPred = select.getHavingPred();
    assertEquals("<slot 2> / <slot 3>",
        selectListExprs.get(0).toSql());
    assertNotNull(havingPred);
    // we only have one 'count(id)' slot (slot 2)
    assertEquals(havingPred.toSql(), "<slot 3> > 0");
    Expr orderingExpr = select.getSortInfo().getOrderingExprs().get(0);
    assertNotNull(orderingExpr);
    assertEquals("<slot 4> / <slot 5>", orderingExpr.toSql());
  }

  @Test public void TestOrderBy() throws AnalysisException {
    AnalyzesOk("select zip, id from testtbl order by zip");
    AnalyzesOk("select zip, id from testtbl order by zip asc");
    AnalyzesOk("select zip, id from testtbl order by zip desc");

    // resolves ordinals
    AnalyzesOk("select zip, id from testtbl order by 1");
    AnalyzesOk("select zip, id from testtbl order by 2 desc, 1 asc");
    // ordinal out of range
    AnalysisError("select zip, id from testtbl order by 0",
        "ORDER BY: ordinal must be >= 1");
    AnalysisError("select zip, id from testtbl order by 3",
        "ORDER BY: ordinal exceeds number of items in select list");
    // can't order by '*'
    AnalysisError("select * from alltypes order by 1",
        "ORDER BY: ordinal refers to '*' in select list");
    // picks up select item alias
    AnalyzesOk("select zip z, id c from testtbl order by z, c");

    // can introduce additional aggregates in order by clause
    AnalyzesOk("select zip, count(*) from testtbl group by 1 order by count(*)");
    AnalyzesOk("select zip, count(*) from testtbl group by 1 order by count(*) " +
        "+ min(zip)");
    AnalysisError("select zip, count(*) from testtbl group by 1 order by id",
        "ORDER BY expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");

    // multiple ordering exprs
    AnalyzesOk("select int_col, string_col, bigint_col from alltypes " +
               "order by string_col, 15.7 * float_col, int_col + bigint_col");
    AnalyzesOk("select int_col, string_col, bigint_col from alltypes " +
               "order by 2, 1, 3");

    // ordering by floating-point exprs is okay
    AnalyzesOk("select float_col, int_col + 0.5 from alltypes order by 1, 2");
    AnalyzesOk("select float_col, int_col + 0.5 from alltypes order by 2, 1");
  }

  @Test
  public void TestBinaryPredicates() throws AnalysisException {
    // AnalyzesOk("select * from alltypes where bool_col != true");
    AnalyzesOk("select * from alltypes where tinyint_col <> 1");
    AnalyzesOk("select * from alltypes where smallint_col <= 23");
    AnalyzesOk("select * from alltypes where int_col > 15");
    AnalyzesOk("select * from alltypes where bigint_col >= 17");
    AnalyzesOk("select * from alltypes where float_col < 15.0");
    AnalyzesOk("select * from alltypes where double_col > 7.7");
    // automatic type cast if compatible
    AnalyzesOk("select * from alltypes where 1 = 0");
    AnalyzesOk("select * from alltypes where int_col = smallint_col");
    AnalyzesOk("select * from alltypes where bigint_col = float_col");
    AnalyzesOk("select * from alltypes where bool_col = 0");
    AnalyzesOk("select * from alltypes where int_col = '0'");
    AnalyzesOk("select * from alltypes where string_col = 15");
    // invalid casts
    AnalysisError("select * from alltypes where bool_col = '15'",
        "operands are not comparable: bool_col = '15'");
    //AnalysisError("select * from alltypes where date_col = 15",
        //"operands are not comparable: date_col = 15");
    //AnalysisError("select * from alltypes where datetime_col = 1.0",
        //"operands are not comparable: datetime_col = 1.0");
  }

  @Test
  public void TestStringCasts() throws AnalysisException {
    AnalyzesOk("select * from alltypes where tinyint_col = '0.5'");
    AnalyzesOk("select * from alltypes where tinyint_col = '0.5'");
    AnalyzesOk("select * from alltypes where smallint_col = '0.5'");
    AnalyzesOk("select * from alltypes where int_col = '0.5'");
    AnalyzesOk("select * from alltypes where bigint_col = '0.5'");
    AnalyzesOk("select 1.0 = '" + Double.toString(Double.MIN_VALUE) + "'");
    AnalyzesOk("select 1.0 = '-" + Double.toString(Double.MIN_VALUE) + "'");
    AnalyzesOk("select 1.0 = '" + Double.toString(Double.MAX_VALUE) + "'");
    AnalyzesOk("select 1.0 = '-" + Double.toString(Double.MAX_VALUE) + "'");
    // Test chains of minus. Note that "--" is the a comment symbol.
    AnalyzesOk("select * from alltypes where tinyint_col = '-1'");
    AnalyzesOk("select * from alltypes where tinyint_col = '- -1'");
    AnalyzesOk("select * from alltypes where tinyint_col = '- - -1'");
    AnalyzesOk("select * from alltypes where tinyint_col = '- - - -1'");
    // Test correct casting to compatible type on bitwise ops.
    AnalyzesOk("select 1 | '" + Byte.toString(Byte.MIN_VALUE) + "'");
    AnalyzesOk("select 1 | '" + Byte.toString(Byte.MAX_VALUE) + "'");
    AnalyzesOk("select 1 | '" + Short.toString(Short.MIN_VALUE) + "'");
    AnalyzesOk("select 1 | '" + Short.toString(Short.MAX_VALUE) + "'");
    AnalyzesOk("select 1 | '" + Integer.toString(Integer.MIN_VALUE) + "'");
    AnalyzesOk("select 1 | '" + Integer.toString(Integer.MAX_VALUE) + "'");
    // We need to add 1 to MIN_VALUE because there are no negative integer literals.
    // The reason is that whether a minus belongs to an
    // arithmetic expr or a literal must be decided by the parser, not the lexer.
    AnalyzesOk("select 1 | '" + Long.toString(Long.MIN_VALUE+1)  + "'");
    AnalyzesOk("select 1 | '" + Long.toString(Long.MAX_VALUE) + "'");
    // Numeric overflow
    AnalysisError("select * from alltypes where tinyint_col = " +
                "'" + Long.toString(Long.MIN_VALUE) + "1'");
    AnalysisError("select * from alltypes where tinyint_col = " +
        "'" + Long.toString(Long.MAX_VALUE) + "1'");
    AnalysisError("select * from alltypes where tinyint_col = " +
        "'" + Double.toString(Double.MAX_VALUE) + "1'");
    // Java converts a float underflow to 0.0.
    // Since there is no easy, reliable way to detect underflow,
    // we don't consider it an error.
    AnalyzesOk("select * from alltypes where tinyint_col = " +
        "'" + Double.toString(Double.MIN_VALUE) + "1'");
    // Failure to convert a comment.
    AnalysisError("select * from alltypes where tinyint_col = '--1'");
  }

  @Test
  public void TestLikePredicates() throws AnalysisException {
    AnalyzesOk("select * from alltypes where string_col like 'test%'");
    AnalyzesOk("select * from alltypes where string_col like string_col");
    AnalyzesOk("select * from alltypes where 'test' like string_col");
    AnalyzesOk("select * from alltypes where string_col rlike 'test%'");
    AnalyzesOk("select * from alltypes where string_col regexp 'test.*'");
    AnalysisError("select * from alltypes where string_col like 5",
        "right operand of LIKE must be of type STRING");
    AnalysisError("select * from alltypes where 'test' like 5",
        "right operand of LIKE must be of type STRING");
    AnalysisError("select * from alltypes where int_col like 'test%'",
        "left operand of LIKE must be of type STRING");
    AnalysisError("select * from alltypes where string_col regexp 'test]['",
        "invalid regular expression in 'string_col REGEXP 'test][''");
  }

  @Test
  public void TestCompoundPredicates() throws AnalysisException {
    AnalyzesOk("select * from alltypes where string_col = '5' and int_col = 5");
    AnalyzesOk("select * from alltypes where string_col = '5' or int_col = 5");
    AnalyzesOk("select * from alltypes where (string_col = '5' " +
               "or int_col = 5) and string_col > '1'");
    AnalyzesOk("select * from alltypes where not string_col = '5'");
    AnalyzesOk("select * from alltypes where int_col = '5'");
  }

  @Test
  public void TestIsNullPredicates() throws AnalysisException {
    AnalyzesOk("select * from alltypes where int_col is null");
    AnalyzesOk("select * from alltypes where string_col is not null");
    // TODO: add null literals (i think this would require a null type, which is
    // compatible with anything else)
    // AnalyzesOk("select * from alltypes where null is not null");
  }

  /**
   * Test of all arithmetic type casts following mysql's casting policy.
   * @throws AnalysisException
   */
  @Test
  public void TestArithmeticTypeCasts() throws AnalysisException {
    List<PrimitiveType> numericPlusString =
        (List<PrimitiveType>) PrimitiveType.getNumericTypes().clone();
    numericPlusString.add(PrimitiveType.STRING);

    for (PrimitiveType type1 : PrimitiveType.getNumericTypes()) {
      for (PrimitiveType type2 : numericPlusString) {
        PrimitiveType compatibleType =
            PrimitiveType.getAssignmentCompatibleType(type1, type2);
        PrimitiveType promotedType = compatibleType.getMaxResolutionType();

        // +, -, *
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.ADD, null,
                      promotedType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.ADD, null,
                      promotedType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.SUBTRACT, null,
                      promotedType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.SUBTRACT, null,
                      promotedType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.MULTIPLY, null,
                      promotedType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.MULTIPLY, null,
                      promotedType);

        // /
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.DIVIDE, null,
                      PrimitiveType.DOUBLE);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.DIVIDE, null,
                      PrimitiveType.DOUBLE);

        // % div, &, |, ^ only for fixed-point types
        if (!type1.isFixedPointType() || !type2.isFixedPointType()) {
          continue;
        }
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.MOD, null,
            compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.MOD, null,
            compatibleType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.INT_DIVIDE, null,
                      compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.INT_DIVIDE, null,
                      compatibleType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.BITAND, null,
                      compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.BITAND, null,
                      compatibleType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.BITOR, null,
                      compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.BITOR, null,
                      compatibleType);
        typeCastTest(type1, type2, false, ArithmeticExpr.Operator.BITXOR, null,
                      compatibleType);
        typeCastTest(type1, type2, true, ArithmeticExpr.Operator.BITXOR, null,
                      compatibleType);
      }
    }

    for (PrimitiveType type : PrimitiveType.getFixedPointTypes()) {
      typeCastTest(null, type, false, ArithmeticExpr.Operator.BITNOT, null, type);
    }
  }

  /**
   * Test of all type casts in comparisons following mysql's casting policy.
   * @throws AnalysisException
   */
  @Test
  public void TestComparisonTypeCasts() throws AnalysisException {
    // test on all comparison ops
    for (BinaryPredicate.Operator cmpOp : BinaryPredicate.Operator.values()) {
      // test all numeric
      for (PrimitiveType type1 : PrimitiveType.getNumericTypes()) {
        for (PrimitiveType type2 : PrimitiveType.getNumericTypes()) {
          PrimitiveType compatibleType =
              PrimitiveType.getAssignmentCompatibleType(type1, type2);
          typeCastTest(type1, type2, false, null, cmpOp, compatibleType);
          typeCastTest(type1, type2, true, null, cmpOp, compatibleType);
        }
      }
    }
  }

  /**
   * Generate an expr of the form "<type1> <arithmeticOp | cmpOp> <type2>"
   * and make sure that the expr has the correct type (opType for arithmetic
   * ops or bool for comparisons) and that both operands are of type 'opType'.
   * @throws AnalysisException
   */
  private void typeCastTest(PrimitiveType type1, PrimitiveType type2,
      boolean op1IsLiteral, ArithmeticExpr.Operator arithmeticOp,
      BinaryPredicate.Operator cmpOp, PrimitiveType opType) throws AnalysisException {
    Preconditions.checkState((arithmeticOp == null) != (cmpOp == null));
    boolean arithmeticMode = arithmeticOp != null;
    String op1 = "";
    if (type1 != null) {
      if (op1IsLiteral) {
        op1 = typeToLiteralValue.get(type1);
      } else {
        op1 = TestSchemaUtils.getAllTypesColumn(type1);
      }
    }
    String op2 = TestSchemaUtils.getAllTypesColumn(type2);
    String queryStr = null;
    if (arithmeticMode) {
      queryStr = "select " + op1 + " " + arithmeticOp.toString() + " " + op2 +
            " AS a from alltypes";
    } else {
      queryStr = "select int_col from alltypes " +
            "where " + op1 + " " + cmpOp.toString() + " " + op2;
    }
    System.err.println(queryStr);
    SelectStmt select = (SelectStmt) AnalyzesOk(queryStr);
    Expr expr = null;
    if (arithmeticMode) {
      ArrayList<Expr> selectListExprs = select.getSelectListExprs();
      assertNotNull(selectListExprs);
      assertEquals(selectListExprs.size(), 1);
      // check the first expr in select list
      expr = selectListExprs.get(0);
      assertEquals(opType, expr.getType());
    } else {
      // check the where clause
      expr = select.getWhereClause();
      assertEquals(PrimitiveType.BOOLEAN, expr.getType());
    }

    checkCasts(expr);
    assertEquals(opType, expr.getChild(0).getType());
    if (type1 != null) {
      assertEquals(opType, expr.getChild(1).getType());
    }
  }

  // TODO: re-enable tests as soon as we have date-related types
  // @Test
  public void DoNotTestStringLiteralToDateCasts() throws AnalysisException {
    // positive tests are included in TestComparisonTypeCasts
    AnalysisError("select int_col from alltypes where date_col = 'ABCD'",
        "Unable to parse string 'ABCD' to date");
    AnalysisError("select int_col from alltypes where date_col = 'ABCD-EF-GH'",
        "Unable to parse string 'ABCD-EF-GH' to date");
    AnalysisError("select int_col from alltypes where date_col = '2006'",
        "Unable to parse string '2006' to date");
    AnalysisError("select int_col from alltypes where date_col = '0.5'",
        "Unable to parse string '0.5' to date");
    AnalysisError("select int_col from alltypes where date_col = '2006-10-10 ABCD'",
        "Unable to parse string '2006-10-10 ABCD' to date");
    AnalysisError("select int_col from alltypes where date_col = '2006-10-10 12:11:05.ABC'",
        "Unable to parse string '2006-10-10 12:11:05.ABC' to date");
  }

  // TODO: generate all possible error combinations of types and operands
  @Test
  public void TestFixedPointArithmeticOps() throws AnalysisException {
    // negative tests, no floating point types allowed
    AnalysisError("select ~float_col from alltypes",
        "Bitwise operations only allowed on fixed-point types");
    AnalysisError("select float_col ^ int_col from alltypes",
        "Invalid floating point argument to operation ^");
    AnalysisError("select float_col & int_col from alltypes",
        "Invalid floating point argument to operation &");
    AnalysisError("select double_col | bigint_col from alltypes",
        "Invalid floating point argument to operation |");
    AnalysisError("select int_col from alltypes where float_col & bool_col > 5",
        "Arithmetic operation requires numeric or string operands");
  }

  @Test
  public void TestNestedFunctions() throws AnalysisException {
    AnalyzesOk("select sin(pi())");
    AnalyzesOk("select sin(cos(pi()))");
    AnalyzesOk("select sin(cos(tan(e())))");
  }

  @Test
  public void TestVarArgFunctions() throws AnalysisException {
    AnalyzesOk("select concat('a')");
    AnalyzesOk("select concat('a', 'b')");
    AnalyzesOk("select concat('a', 'b', 'c')");
    AnalyzesOk("select concat('a', 'b', 'c', 'd')");
    AnalyzesOk("select concat('a', 'b', 'c', 'd', 'e')");
    // Test different vararg type signatures for same function name.
    AnalyzesOk("select coalesce(true)");
    AnalyzesOk("select coalesce(true, false, true)");
    AnalyzesOk("select coalesce(5)");
    AnalyzesOk("select coalesce(5, 6, 7)");
    AnalyzesOk("select coalesce('a')");
    AnalyzesOk("select coalesce('a', 'b', 'c')");
    // Need at least one argument.
    AnalysisError("select concat()");
    AnalysisError("select coalesce()");
  }

  @Test
  public void TestCaseExpr() throws AnalysisException {
    // No case expr.
    AnalyzesOk("select case when 20 > 10 then 20 else 15 end");
    // No else.
    AnalyzesOk("select case when 20 > 10 then 20 end");
    // First when condition is a boolean slotref.
    AnalyzesOk("select case when bool_col then 20 else 15 end from alltypes");
    // Requires casting then exprs.
    AnalyzesOk("select case when 20 > 10 then 20 when 1 > 2 then 1.0 else 15 end");
    // Requires casting then exprs.
    AnalyzesOk("select case when 20 > 10 then 20 when 1 > 2 then 1.0 " +
    		"when 4 < 5 then 2 else 15 end");
    // First when expr doesn't return boolean.
    AnalysisError("select case when 20 then 20 when 1 > 2 then timestamp_col " +
        "when 4 < 5 then 2 else 15 end from alltypes",
        "When expr '20' is not of type boolean and not castable to type boolean.");
    // Then exprs return incompatible types.
    AnalysisError("select case when 20 > 10 then 20 when 1 > 2 then timestamp_col " +
        "when 4 < 5 then 2 else 15 end from alltypes",
        "Incompatible return types 'TINYINT' and 'TIMESTAMP' " +
        "of exprs '20' and 'timestamp_col'.");

    // With case expr.
    AnalyzesOk("select case int_col when 20 then 30 else 15 end " +
    		"from alltypes");
    // No else.
    AnalyzesOk("select case int_col when 20 then 30 end " +
        "from alltypes");
    // Requires casting case expr.
    AnalyzesOk("select case int_col when bigint_col then 30 else 15 end " +
        "from alltypes");
    // Requires casting when expr.
    AnalyzesOk("select case bigint_col when int_col then 30 else 15 end " +
        "from alltypes");
    // Requires multiple casts.
    AnalyzesOk("select case bigint_col when int_col then 30 " +
    		"when double_col then 1.0 else 15 end from alltypes");
    // Type of case expr is incompatible with first when expr.
    AnalysisError("select case bigint_col when timestamp_col then 30 " +
        "when double_col then 1.0 else 15 end from alltypes",
        "Incompatible return types 'BIGINT' and 'TIMESTAMP' " +
        "of exprs 'bigint_col' and 'timestamp_col'.");
    // Then exprs return incompatible types.
    AnalysisError("select case bigint_col when int_col then 30 " +
        "when double_col then timestamp_col else 15 end from alltypes",
        "Incompatible return types 'TINYINT' and 'TIMESTAMP' " +
        "of exprs '30' and 'timestamp_col'.");

    // Test different type classes (all types are tested in BE tests).
    AnalyzesOk("select case when true then 1 end");
    AnalyzesOk("select case when true then 1.0 end");
    AnalyzesOk("select case when true then 'abc' end");
    AnalyzesOk("select case when true then cast('2011-01-01 09:01:01' as timestamp) end");
  }

  @Test
  public void TestInsert() throws AnalysisException {
    testInsertStatic(true);
    testInsertStatic(false);
    testInsertDynamic(true);
    testInsertDynamic(false);
  }

  /**
   * Run tests for dynamic partitions for INSERT INTO/OVERWRITE:
   *
   * @param overwrite
   *          If true, tests INSERT OVERWRITE, else tests INSERT INTO.
   * @throws AnalysisException
   */
  private void testInsertDynamic(boolean overwrite) throws AnalysisException {
    String qualifier = null;
    if (overwrite) {
      qualifier = "overwrite";
    } else {
      qualifier = "into";
    }
    // Fully dynamic partitions.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year, " +
        "month from alltypes");
    // Fully dynamic partitions with NULL literals as partitioning columns.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, NULL, NULL " +
        "from alltypes");
    // Fully dynamic partitions. Order of corresponding select list items doesn't matter, as long as
    // they appear at the very end of the select list.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, month, " +
        "year from alltypes");
    // Partially dynamic partitions.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, month from " +
        "alltypes");
    // Partially dynamic partitions.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year from " +
        "alltypes");
    // Partially dynamic partitions with NULL literal as column.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, NULL from " +
        "alltypes");
    // Partially dynamic partitions with NULL literal as column.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, NULL from " +
        "alltypes");
    // Partially dynamic partitions with NULL literal in partition clause.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month=NULL)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes");
    // Partially dynamic partitions with NULL literal in partition clause.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year=NULL, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes");
    // No corresponding select list items of fully dynamic partitions.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "No matching select list item found for dynamic partition 'year'.\n" +
        "The select list items corresponding to dynamic partition keys " +
        "must be at the end of the select list.");
    // No corresponding select list items of partially dynamic partitions.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "No matching select list item found for dynamic partition 'month'.\n" +
        "The select list items corresponding to dynamic partition keys " +
        "must be at the end of the select list.");
    // No corresponding select list items of partially dynamic partitions.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
        "partition (year, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "No matching select list item found for dynamic partition 'year'.\n" +
        "The select list items corresponding to dynamic partition keys " +
        "must be at the end of the select list.");
    // Select '*' includes partitioning columns, and hence, is not union compatible.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
    		"partition (year=2009, month=4)" +
        "select * from alltypes",
        "Target table 'alltypessmall' and result of select statement are not union " +
        "compatible.\n" +
        "Target table expects 11 columns but the select statement returns 13.");
    // Select '*' includes partitioning columns
    // but they don't appear at the end of the select list.
    AnalysisError("insert " + qualifier + " table alltypessmall partition (year, month)" +
        "select * from alltypes",
        "Target table 'alltypessmall' and result of select statement are not union " +
        "compatible.\n" +
        "Incompatible types 'INT' and 'TIMESTAMP' in column 'alltypes.timestamp_col'.");
  }

  /**
   * Run general tests and tests using static partitions for INSERT INTO/OVERWRITE:
   *
   * @param overwrite
   *          If true, tests INSERT OVERWRITE, else tests INSERT INTO.
   * @throws AnalysisException
   */
  private void testInsertStatic(boolean overwrite) throws AnalysisException {
    String qualifier = null;
    if (overwrite) {
      qualifier = "overwrite";
    } else {
      qualifier = "into";
    }
    // Unpartitioned table without partition clause.
    AnalyzesOk("insert " + qualifier + " table alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col from alltypes");
    // Static partition.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes");
    // Static partition with NULL partition keys.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year=NULL, month=NULL)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes");
    // Static partition with partial NULL partition keys.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month=NULL)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes");
    // Static partition with partial NULL partition keys.
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year=NULL, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes");
    // Union compatibility requires cast of select list expr in column 5 (int_col -> bigint).
    AnalyzesOk("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, int_col, " +
        "float_col, float_col, date_string_col, string_col, timestamp_col from alltypes");
    // No partition clause given for partitioned table.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "No PARTITION clause given for insertion into partitioned table 'alltypessmall'.");
    // Not union compatible, unequal number of columns.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, timestamp_col from alltypes",
        "Target table 'alltypessmall' and result of select statement are not union " +
        "compatible.\n" +
        "Target table expects 11 columns but the select statement returns 10.");
    // Not union compatible, incompatible type in last column (bool_col -> string).
    AnalysisError("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, bool_col, timestamp_col from alltypes",
        "Target table 'alltypessmall' and result of select statement are not union " +
        "compatible.\n" +
        "Incompatible types 'STRING' and 'BOOLEAN' in column 'bool_col'.");
    // Too many partitioning columns.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, month=4, year=10)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "Superfluous columns in PARTITION clause: year.");
    // Too few partitioning columns.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "Missing partition column 'month' from PARTITION clause.");
    // Non-partitioning column in partition clause.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
        "partition (year=2009, bigint_col=10)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "Missing partition column 'month' from PARTITION clause.");
    // Partition clause given for insertion into HBase table.
    AnalysisError("insert " + qualifier + " table hbasealltypessmall " +
        "partition (year=2009, bigint_col=10)" +
        "select bool_col, double_col, float_col, bigint_col, int_col, " +
        "smallint_col, tinyint_col, date_string_col, string_col, timestamp_col from " +
        "alltypes",
        "PARTITION clause is not allowed for HBase tables.");
    // Loss of precision when casting in column 6 (double_col -> float).
    AnalysisError("insert " + qualifier + " table alltypessmall " +
    		"partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "double_col, double_col, date_string_col, string_col, timestamp_col from alltypes",
        "Inserting into target table 'alltypessmall' may result in loss of precision.\n" +
        "Would need to cast 'double_col' to 'FLOAT'.");
    // Select '*' includes partitioning columns, and hence, is not union compatible.
    AnalysisError("insert " + qualifier + " table alltypessmall " +
    		"partition (year=2009, month=4)" +
        "select * from alltypes",
        "Target table 'alltypessmall' and result of select statement are not union " +
        "compatible.\n" +
        "Target table expects 11 columns but the select statement returns 13.");
  }

  /**
   * Check that:
   * - we don't cast literals (we should have simply converted the literal
   *   to the target type)
   * - we don't do redundant casts (ie, we don't cast a bigint expr to a bigint)
   */
  private void checkCasts(Expr expr) {
    if (expr instanceof CastExpr) {
      Assert.assertFalse(expr.getType() == expr.getChild(0).getType());
      Assert.assertFalse(expr.getChild(0) instanceof LiteralExpr);
    }
    for (Expr child: expr.getChildren()) {
      checkCasts(child);
    }
  }

}
