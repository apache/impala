// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.common.AnalysisException;

public class AnalyzerTest {
  private static Catalog catalog;
  private final static Logger log = LoggerFactory.getLogger(AnalyzerTest.class);

  // maps from type to string that will result in literal of that type
  private static Map<PrimitiveType, String> typeToLiteralValue =
      new HashMap<PrimitiveType, String>();
  static {
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
    HiveMetaStoreClient client = TestSchemaUtils.createSchemaAndClient();
    catalog = new Catalog(client);
  }

  /**
   * Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
   *
   * @param stmt
   * @return
   */
  public ParseNode AnalyzesOk(String stmt) {
    log.info("analyzing " + stmt);
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
    Analyzer analyzer = new Analyzer(catalog);
    try {
      node.analyze(analyzer);
    } catch (AnalysisException e) {
      fail("Analysis error:\n" + e.toString());
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
    log.info("analyzing " + stmt);
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
    Analyzer analyzer = new Analyzer(catalog);
    try {
      node.analyze(analyzer);
    } catch (AnalysisException e) {
      if (expectedErrorString != null) {
        String errorString = e.getMessage();
        assertEquals(expectedErrorString, errorString);
      }
      return;
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

  @Test
  public void TestStar() {
    AnalyzesOk("select * from AllTypes");
    AnalyzesOk("select alltypes.* from AllTypes");
    // different db
    AnalyzesOk("select testdb1.alltypes.* from testdb1.alltypes");
    // two tables w/ identical names from different dbs
    AnalyzesOk("select alltypes.*, testdb1.alltypes.* from alltypes, testdb1.alltypes");
    AnalyzesOk("select * from alltypes, testdb1.alltypes");
  }

  @Test
  public void TestOrdinals() {
    // can't group or order on *
    AnalysisError("select * from alltypes group by 1");
    AnalysisError("select * from alltypes order by 1");
  }

  @Test
  public void TestFromClause() {
    AnalyzesOk("select int_col from alltypes");
    AnalysisError("select int_col from badtbl");
    // case-insensitive
    AnalyzesOk("SELECT INT_COL FROM ALLTYPES");
    AnalyzesOk("select AllTypes.Int_Col from alltypes");
    // aliases work
    AnalyzesOk("select a.int_col from alltypes a");
    // implicit aliases
    AnalyzesOk("select int_col, id from alltypes, testtbl");
    // duplicate alias
    AnalysisError("select a.int_col, a.id from alltypes a, testtbl a");
    // duplicate implicit alias
    AnalysisError("select int_col from alltypes, alltypes");

    // resolves dbs correctly
    AnalyzesOk("select zip from testtbl");
    AnalysisError("select zip from testdb1.testtbl");
  }

  @Test public void TestOnClause() {
    AnalyzesOk("select a.int_col from alltypes a join alltypes b on (a.int_col = b.int_col)");
    AnalyzesOk(
        "select a.int_col " +
        "from alltypes a join alltypes b on " +
        "(a.int_col = b.int_col and a.string_col = b.string_col)");
    // unknown column
    AnalysisError("select a.int_col from alltypes a join alltypes b on (a.int_col = b.badcol)");
    // ambiguous col ref
    AnalysisError("select a.int_col from alltypes a join alltypes b on (int_col = int_col)");
    // unknown alias
    AnalysisError(
        "select a.int_col from alltypes a join alltypes b on (a.int_col = badalias.int_col)");
    // incompatible comparison
    AnalysisError("select a.int_col from alltypes a join alltypes b on (a.int_col = b.date_col)");
    AnalyzesOk(
        "select a.int_col, b.int_col, c.int_col " +
        "from alltypes a join alltypes b on " +
        "(a.int_col = b.int_col and a.string_col = b.string_col)" +
        "join alltypes c on " +
        "(b.int_col = c.int_col and b.string_col = c.string_col and b.bool_col = c.bool_col)");
    // can't reference an alias that gets declared afterwards
    AnalysisError(
        "select a.int_col, b.int_col, c.int_col " +
        "from alltypes a join alltypes b on " +
        "(c.int_col = b.int_col and a.string_col = b.string_col)" +
        "join alltypes c on " +
        "(b.int_col = c.int_col and b.string_col = c.string_col and b.bool_col = c.bool_col)");
  }

  @Test public void TestUsingClause() {
    AnalyzesOk("select a.int_col, b.int_col from alltypes a join alltypes b using (int_col)");
    AnalyzesOk("select a.int_col, b.int_col from alltypes a join alltypes b using (int_col, string_col)");
    AnalyzesOk(
        "select a.int_col, b.int_col, c.int_col " +
        "from alltypes a join alltypes b using (int_col, string_col) " +
        "join alltypes c using (int_col, string_col, bool_col)");
    // unknown column
    AnalysisError("select a.int_col from alltypes a join alltypes b using (badcol)");
    AnalysisError("select a.int_col from alltypes a join alltypes b using (int_col, badcol)");
  }

  @Test
  public void TestWhereClause() {
    AnalyzesOk("select zip, name from testtbl where id > 15");
    AnalysisError("select zip, name from testtbl where badcol > 15");
    AnalyzesOk("select * from testtbl where true");
    AnalysisError("select * from testtbl where count(*) > 0");
  }

  @Test
  public void TestAggregates() {
    AnalyzesOk("select count(*), min(id), max(id), sum(id), avg(id) from testtbl");
    AnalyzesOk("select count(id, zip) from testtbl");
    AnalysisError("select id, zip from testtbl where count(*) > 0");

    // only count() allows '*'
    AnalysisError("select avg(*) from testtbl");
    AnalysisError("select min(*) from testtbl");
    AnalysisError("select max(*) from testtbl");
    AnalysisError("select sum(*) from testtbl");

    // multiple args
    AnalyzesOk("select count(id, zip) from testtbl");
    AnalysisError("select min(id, zip) from testtbl");
    AnalysisError("select max(id, zip) from testtbl");
    AnalysisError("select sum(id, zip) from testtbl");
    AnalysisError("select avg(id, zip) from testtbl");

    // nested aggregates
    AnalysisError("select sum(count(*)) from testtbl");

    // wrong type
    AnalysisError("select sum(date_col) from alltypes");
    AnalysisError("select avg(date_col) from alltypes");
    AnalysisError("select sum(datetime_col) from alltypes");
    AnalysisError("select avg(datetime_col) from alltypes");
    AnalysisError("select sum(timestamp_col) from alltypes");
    AnalysisError("select avg(timestamp_col) from alltypes");
    AnalysisError("select sum(string_col) from alltypes");
    AnalysisError("select avg(string_col) from alltypes");
  }

  @Test
  public void TestGroupBy() {
    AnalyzesOk("select zip, count(*) from testtbl group by zip");
    AnalyzesOk("select zip + count(*) from testtbl group by zip");
    // doesn't group by all non-agg select list items
    AnalysisError("select zip, count(*) from testtbl");
    AnalysisError("select zip + count(*) from testtbl");

    AnalyzesOk("select id, zip from testtbl group by zip, id having count(*) > 0");
    AnalysisError("select id, zip from testtbl group by id having count(*) > 0");
    AnalysisError("select id from testtbl group by id having zip + count(*) > 0");
    // resolves ordinals
    AnalyzesOk("select zip, count(*) from testtbl group by 1");
    AnalyzesOk("select count(*), zip from testtbl group by 2");
    AnalysisError("select zip, count(*) from testtbl group by 3");
    AnalysisError("select * from alltypes group by 1");
    // picks up select item alias
    AnalyzesOk("select zip z, count(*) from testtbl group by z");

    // can't group by aggregate
    AnalysisError("select zip, count(*) from testtbl group by count(*)");
    AnalysisError("select zip, count(*) from testtbl group by count(*) + min(zip)");
    AnalysisError("select zip, count(*) from testtbl group by 2");

    // multiple grouping cols
    AnalyzesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
               "group by string_col, int_col, bigint_col");
    AnalyzesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
               "group by 2, 1, 3");
    AnalysisError("select int_col, string_col, bigint_col, count(*) from alltypes " +
                   "group by 2, 1, 4");
    // can't group by floating-point exprs
    AnalysisError("select float_col, count(*) from alltypes group by 1");
    AnalysisError("select int_col + 0.5, count(*) from alltypes group by 1");
  }

  @Test public void TestAvgSubstitution() {
    SelectStmt select = (SelectStmt) AnalyzesOk(
        "select avg(id) from testtbl having count(id) > 0 order by avg(zip)");
    ArrayList<Expr> selectListExprs = select.getSelectListExprs();
    assertNotNull(selectListExprs);
    assertEquals(selectListExprs.size(), 1);
    // all agg exprs are replaced with refs to agg output slots
    Expr havingPred = select.getHavingPred();
    assertEquals("CAST(<slot 2> AS DOUBLE) / CAST(<slot 3> AS DOUBLE)",
        selectListExprs.get(0).toSql());
    assertNotNull(havingPred);
    // we only have one 'count(id)' slot (slot 2)
    assertEquals(havingPred.toSql(), "<slot 3> > 0");
    Expr orderingExpr = select.getOrderingExprs().get(0);
    assertNotNull(orderingExpr);
    assertEquals("CAST(<slot 4> AS DOUBLE) / CAST(<slot 5> AS DOUBLE)", orderingExpr.toSql());
  }

  @Test public void TestOrderBy() {
    AnalyzesOk("select zip, id from testtbl order by zip");
    AnalyzesOk("select zip, id from testtbl order by zip asc");
    AnalyzesOk("select zip, id from testtbl order by zip desc");

    // resolves ordinals
    AnalyzesOk("select zip, id from testtbl order by 1");
    AnalyzesOk("select zip, id from testtbl order by 2 desc, 1 asc");
    // ordinal out of range
    AnalysisError("select zip, id from testtbl order by 0");
    AnalysisError("select zip, id from testtbl order by 3");
    // can't order by '*'
    AnalysisError("select * from alltypes order by 1");
    // picks up select item alias
    AnalyzesOk("select zip z, id c from testtbl order by z, c");

    // can introduce additional aggregates in order by clause
    AnalyzesOk("select zip, count(*) from testtbl group by 1 order by count(*)");
    AnalyzesOk("select zip, count(*) from testtbl group by 1 order by count(*) + min(zip)");
    AnalysisError("select zip, count(*) from testtbl group by 1 order by id");

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
  public void TestBinaryPredicates() {
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
    AnalysisError("select * from alltypes where date_col = 15");
    AnalysisError("select * from alltypes where datetime_col = 1.0");
  }

  @Test
  public void TestLikePredicates() {
    AnalyzesOk("select * from alltypes where string_col like 'test%'");
    AnalyzesOk("select * from alltypes where string_col like string_col");
    AnalyzesOk("select * from alltypes where 'test' like string_col");
    AnalyzesOk("select * from alltypes where string_col rlike 'test%'");
    AnalyzesOk("select * from alltypes where string_col regexp 'test.*'");
    AnalysisError("select * from alltypes where string_col like 5");
    AnalysisError("select * from alltypes where 'test' like 5");
    AnalysisError("select * from alltypes where int_col like 'test%'");
  }

  @Test
  public void TestCompoundPredicates() {
    AnalyzesOk("select * from alltypes where string_col = '5' and int_col = 5");
    AnalyzesOk("select * from alltypes where string_col = '5' or int_col = 5");
    AnalyzesOk("select * from alltypes where (string_col = '5' or int_col = 5) and string_col > '1'");
    AnalyzesOk("select * from alltypes where not string_col = '5'");
    AnalyzesOk("select * from alltypes where int_col = '5'");
  }

  @Test
  public void TestLiteralPredicates() {
    AnalyzesOk("select * from alltypes where true");
    AnalyzesOk("select * from alltypes where false");
    AnalyzesOk("select * from alltypes where true or false");
  }

  @Test
  public void TestIsNullPredicates() {
    AnalyzesOk("select * from alltypes where int_col is null");
    AnalyzesOk("select * from alltypes where string_col is not null");
    // TODO: add null literals (i think this would require a null type, which is
    // compatible with anything else)
    // AnalyzesOk("select * from alltypes where null is not null");
  }

  /**
   * Test of all arithmetic type casts
   * following Hive's casting policy
   *
   */
  @Test
  public void TestArithmeticTypeCasts() {
    // default comparison op - ignored in this test
    BinaryPredicate.Operator cmpOp = BinaryPredicate.Operator.EQ;
    // test on all possible arithmetic ops
    for (ArithmeticExpr.Operator arithOp : ArithmeticExpr.Operator.values()) {
      // we have a special test cases for these later on
      if (arithOp.isBitwiseOperation() ||
          arithOp == ArithmeticExpr.Operator.INT_DIVIDE ||
          arithOp == ArithmeticExpr.Operator.DIVIDE) {
        continue;
      }
      // test all numeric types
      for (PrimitiveType type : PrimitiveType.values()) {
        if (!type.isNumericType()) {
          continue;
        }
        // non-literals
        typeCastTest(type, true, false, arithOp, cmpOp);
        // literals
        typeCastTest(type, true, true, arithOp, cmpOp);
      }
    }
  }

  /**
   * Test of all type casts in comparisons
   * following Hive's casting policy
   *
   */
  @Test
  public void TestComparisonTypeCasts() {
    // default arithmetic op - ignored in this test
    ArithmeticExpr.Operator arithOp = ArithmeticExpr.Operator.PLUS;
    // test on all comparison ops
    for (BinaryPredicate.Operator cmpOp : BinaryPredicate.Operator.values()) {
      // test all numeric and date types
      for (PrimitiveType type : PrimitiveType.values()) {
        if (!type.isNumericType() && !type.isDateType()) {
          continue;
        }
        // non-literals
        typeCastTest(PrimitiveType.TINYINT, false, false, arithOp, cmpOp);
        // literals
        typeCastTest(PrimitiveType.TINYINT, false, true, arithOp, cmpOp);
      }
    }
  }

  /**
   * If literalMode is true there is exactly one literal (and one non-literal)
   * otherwise we assume two non-literals
   *
   * @param type
   *          type to test
   * @param arithmeticMode
   *          if true test casts in an arithmetic expression,
   *          otherwise tests casts in a binary predicate
   * @param literalMode
   *          whether to test a literal or non-literal
   * @param arithOp
   *          arithmetic operator to use (ignored if not arithmeticMode)
   * @param cmpOp
   *          comparison operator to use (ignored if arithmeticMode)
   *
   */
  private void typeCastTest(PrimitiveType type, boolean arithmeticMode,
      boolean literalMode, ArithmeticExpr.Operator arithOp,
      BinaryPredicate.Operator cmpOp) {
    for (PrimitiveType t : PrimitiveType.values()) {
      if (!type.isValid() || t.ordinal() < type.ordinal()) {
        continue;
      }
      PrimitiveType resultType =
          PrimitiveType.getAssignmentCompatibleType(type, t);
      if (!resultType.isValid()) {
        continue;
      }
      String lval = null;
      if (literalMode) {
        lval = typeToLiteralValue.get(type);
      } else {
        lval = TestSchemaUtils.getAllTypesColumn(type);
      }
      String rval = TestSchemaUtils.getAllTypesColumn(t);
      String queryStr = null;
      if (arithmeticMode) {
        queryStr = "select " + lval + " " + arithOp.toString() + " " + rval +
            " AS a from alltypes";
      } else {
        queryStr = "select int_col from alltypes " +
            "where " + lval + " " + cmpOp.toString() + " " + rval;
      }
      System.err.println(queryStr);
      SelectStmt select = (SelectStmt) AnalyzesOk(queryStr);
      Expr exprToCheck = null;
      if (arithmeticMode) {
        ArrayList<Expr> selectListExprs = select.getSelectListExprs();
        assertNotNull(selectListExprs);
        assertEquals(selectListExprs.size(), 1);
        // check the first expr in select list
        exprToCheck = selectListExprs.get(0);
        assertEquals(resultType, exprToCheck.getType());
      } else {
        // check the where clause
        exprToCheck = select.getWhereClause();
        assertEquals(PrimitiveType.BOOLEAN, exprToCheck.getType());
      }
      if (t == PrimitiveType.STRING) {
        if (type.isDateType() && literalMode) {
          // comparing two strings, no cast
          assertNoCastNode(exprToCheck, 0, PrimitiveType.STRING);
          assertNoCastNode(exprToCheck, 1, PrimitiveType.STRING);
        } else {
          // always cast the string
          assertNoCastNode(exprToCheck, 0, resultType);
          assertCastNode(exprToCheck, 1, resultType);
        }
      } else if (t.ordinal() == type.ordinal()) {
        // types identical, no casts
        assertNoCastNode(exprToCheck, 0, resultType);
        assertNoCastNode(exprToCheck, 1, resultType);
      } else {
        if (literalMode) {
          assertNoCastNode(exprToCheck, 0, resultType);
        } else {
          assertCastNode(exprToCheck, 0, resultType);
        }
        if (t == resultType) {
          assertNoCastNode(exprToCheck, 1, resultType);
        } else {
          assertCastNode(exprToCheck, 1, resultType);
        }
      }
    }
  }

  @Test
  public void TestStringLiteralToDateCasts() {
    // positive tests are included in TestComparisonTypeCasts
    AnalysisError("select int_col from alltypes " +
    		"where date_col = 'ABCD'");
    AnalysisError("select int_col from alltypes " +
        "where date_col = 'ABCD-EF-GH'");
    AnalysisError("select int_col from alltypes " +
        "where date_col = '2006'");
    AnalysisError("select int_col from alltypes " +
        "where date_col = '0.5'");
    AnalysisError("select int_col from alltypes " +
        "where date_col = '2006-10-10 ABCD'");
    AnalysisError("select int_col from alltypes " +
        "where date_col = '2006-10-10 12:11:05.ABC'");
  }

  @Test
  public void TestDivisionArithmeticOps() {
    // test all numeric types
    for (PrimitiveType ltype : PrimitiveType.values()) {
      if (!ltype.isNumericType()) {
        continue;
      }
      // test all numeric and string types
      for (PrimitiveType rtype : PrimitiveType.values()) {
        if (!rtype.isNumericType() && !rtype.isStringType()) {
          continue;
        }
        String lval = TestSchemaUtils.getAllTypesColumn(ltype);
        String rval = TestSchemaUtils.getAllTypesColumn(rtype);
        String queryStr = "select " +
            lval + " / " + " " + rval + " from alltypes";
        SelectStmt select = (SelectStmt) AnalyzesOk(queryStr);
        ArrayList<Expr> selectListExprs = select.getSelectListExprs();
        assertEquals(selectListExprs.size(), 1);
        Expr exprToCheck = selectListExprs.get(0);
        // division always yields double
        assertEquals(PrimitiveType.DOUBLE, exprToCheck.getType());
        // check for casts
        if(ltype != PrimitiveType.DOUBLE) {
          assertCastNode(exprToCheck, 0, PrimitiveType.DOUBLE);
        } else {
          assertNoCastNode(exprToCheck, 0, PrimitiveType.DOUBLE);
        }
        if(rtype != PrimitiveType.DOUBLE) {
          assertCastNode(exprToCheck, 1, PrimitiveType.DOUBLE);
        } else {
          assertNoCastNode(exprToCheck, 1, PrimitiveType.DOUBLE);
        }
      }
    }
  }

  @Test
  public void TestFixedPointArithmeticOps() {
    // positive tests
    testFixedPointArithmeticOpsPositive();

    // negative tests, no floating point types allowed
    AnalysisError("select ~float_col from alltypes");
    AnalysisError("select float_col ^ int_col from alltypes");
    AnalysisError("select float_col & int_col from alltypes");
    AnalysisError("select double_col | bigint_col from alltypes");
    AnalysisError("select int_col from alltypes where float_col & bool_col > 5");
  }

  private void testFixedPointArithmeticOpsPositive() {
    // test all bitwise ops
    for(ArithmeticExpr.Operator arithOp : ArithmeticExpr.Operator.values()) {
      if (!arithOp.isBitwiseOperation() &&
          arithOp != ArithmeticExpr.Operator.INT_DIVIDE) {
        continue;
      }
      // test all combinations of fixed-point types
      for (PrimitiveType ltype : PrimitiveType.values()) {
        if (!ltype.isFixedPointType()) {
          continue;
        }
        for (PrimitiveType rtype : PrimitiveType.values()) {
          if (!rtype.isFixedPointType()) {
            continue;
          }
          if(rtype.ordinal() < ltype.ordinal()) {
            continue;
          }
          String lval = TestSchemaUtils.getAllTypesColumn(ltype);
          String rval = TestSchemaUtils.getAllTypesColumn(rtype);
          String queryStr = null;
          if(arithOp == ArithmeticExpr.Operator.BITNOT) {
            queryStr = "select " + arithOp + "" + lval + " from alltypes";
          } else {
            queryStr = "select " + lval + " " +
                arithOp.toString() + " " + rval + " from alltypes";
          }
          SelectStmt select = (SelectStmt) AnalyzesOk(queryStr);
          ArrayList<Expr> selectListExprs = select.getSelectListExprs();
          assertEquals(selectListExprs.size(), 1);
          Expr exprToCheck = selectListExprs.get(0);
          // special casting logic for bitwise ops
          // the result is always an integer
          PrimitiveType expectedType =
            PrimitiveType.getAssignmentCompatibleType(ltype, rtype);
          if (arithOp == ArithmeticExpr.Operator.BITNOT) {
            expectedType = ltype;
          }
          assertEquals(expectedType, exprToCheck.getType());
        }
      }
    }
  }

  /**
   * Asserts that the childIndex child
   * of the given expression
   * is a cast node of castType
   *
   * @param select
   *          Select Statement
   * @param childIndex
   *          child of select's first expression
   * @param castType
   *          expected target type of cast node
   */
  private void assertCastNode(Expr expr,
      int childIndex,
      PrimitiveType castType) {
    Expr child = expr.getChild(childIndex);
    Assert.assertTrue(child instanceof CastExpr);
    CastExpr cast = (CastExpr) child;
    assertEquals(castType, cast.getType());
  }

  /**
   * Asserts that the childIndex child
   * of the first expression in the select
   * is not a cast node, and returns a given type
   *
   * @param select
   *          Select Statement
   * @param childIndex
   *          child of select's first expression
   * @param type
   *          expected type
   */
  private void assertNoCastNode(Expr expr,
      int childIndex, PrimitiveType type) {
    Expr child = expr.getChild(childIndex);
    Assert.assertFalse(child instanceof CastExpr);
    assertEquals(type, child.getType());
  }
}
