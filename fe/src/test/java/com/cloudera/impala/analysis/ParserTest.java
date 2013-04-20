// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;

import org.junit.Test;

import com.cloudera.impala.analysis.TimestampArithmeticExpr.TimeUnit;
import com.cloudera.impala.catalog.FileFormat;

public class ParserTest {

  // Representative operands for testing.
  private final String[] operands =
      new String[] {"i", "5", "true", "NULL", "'a'", "(1.5 * 8)" };

  /**
   * Asserts in case of parser error.
   * @param stmt
   * @return parse result
   */
  public Object ParsesOk(String stmt) {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    Object result = null;
    try {
      result = parser.parse().value;
    } catch (Exception e) {
      System.err.println(parser.getErrorMsg(stmt));
      fail("\n" + parser.getErrorMsg(stmt));
    }
    assertNotNull(result);
    return result;
  }

  /**
   * Asserts if stmt parses fine or the error string doesn't match and it is non-null.
   * @param stmt
   * @param expectedErrorString
   */
  public void ParserError(String stmt, String expectedErrorString) {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    try {
      parser.parse();
    } catch (java.lang.Exception e) {
      if (expectedErrorString != null) {
        String errorString = parser.getErrorMsg(stmt);
        assertEquals(expectedErrorString, errorString);
      }
      return;
    }
    fail("Stmt didn't result in parsing error: " + stmt);
  }

  /**
   * Asserts if stmt scans fine.
   * @param stmt
   */
  public void ScannerError(String stmt) {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    try {
      parser.parse();
    } catch (java.lang.Exception e) {
      fail("Stmt didn't result in scanning error but a parsing error instead: " + stmt);
    } catch (java.lang.Error e) {
      // Exception message is always 'failure occurred on input'.
      return;
    }
    fail("Stmt didn't result in scanning or parsing error: " + stmt);
  }

  /**
   * Asserts if stmt parses fine.
   * @param stmt
   */
  public void ParserError(String stmt) {
    ParserError(stmt, null);
  }

  @Test
  public void TestNoFromClause() {
    ParsesOk("select 1 + 1, 'two', f(3), a + b");
    ParserError("select 1 + 1 'two' f(3) a + b");
    ParserError("select a, 2 where a > 2");
    ParserError("select a, 2 group by");
    ParserError("select a, 2 order by 1");
    ParserError("select a, 2 limit 1");
    ParserError("select a, 2 order by 1 limit 1");
  }

  @Test
  public void TestSelect() {
    ParsesOk("select a from tbl");
    ParsesOk("select a, b, c, d from tbl");
    ParsesOk("select true, false, NULL from tbl");
    ParsesOk("select all a, b, c from tbl");
    ParserError("a from tbl");
    ParserError("select a b c from tbl");
    ParserError("select all from tbl");
  }

  @Test
  public void TestAlias() {
    ParsesOk("select a b from tbl");
    ParsesOk("select a b, c from tbl");
    ParsesOk("select a as a, b as b, c as c, d as d from tbl");
    ParserError("a from tbl");
    ParserError("select a as a, b c d from tbl");

    ParsesOk("select a from tbl b");
    ParsesOk("select a from tbl as b");
    ParsesOk("select a from (select * from tbl) b");
    ParsesOk("select a from (select * from tbl) as b");
    ParsesOk("select a from (select * from tbl b) as b");
  }

  @Test
  public void TestStar() {
    ParsesOk("select * from tbl");
    ParsesOk("select tbl.* from tbl");
    ParsesOk("select db.tbl.* from tbl");
    ParserError("select bla.db.tbl.* from tbl");
    ParserError("select * + 5 from tbl");
    ParserError("select (*) from tbl");
    ParserError("select *.id from tbl");
    ParserError("select * from tbl.*");
    ParserError("select * from tbl where * = 5");
    ParserError("select * from tbl where f(*) = 5");
    ParserError("select * from tbl where tbl.* = 5");
    ParserError("select * from tbl where f(tbl.*) = 5");
  }

  @Test
  public void TestJoinHints() {
    ParsesOk("select * from functional.alltypes a join [broadcast] " +
        "functional.alltypes b using (int_col)");
    ParsesOk("select * from functional.alltypes a join [bla,bla] " +
        "functional.alltypes b using (int_col)");
    ParserError("select * from functional.alltypes a join [bla bla] " +
        "functional.alltypes b using (int_col)");
    ParserError("select * from functional.alltypes a join [1 + 2] " +
        "functional.alltypes b using (int_col)");
  }

  @Test
  public void TestFromClause() {
    ParsesOk("select * from src src1 " +
        "left outer join src src2 on " +
        "  src1.key = src2.key and src1.key < 10 and src2.key > 10 " +
        "right outer join src src3 on " +
        "  src2.key = src3.key and src3.key < 10 " +
        "full outer join src src3 on " +
        "  src2.key = src3.key and src3.key < 10 " +
        "left semi join src src3 on " +
        "  src2.key = src3.key and src3.key < 10 " +
        "join src src3 on " +
        "  src2.key = src3.key and src3.key < 10 " +
        "inner join src src3 on " +
        "  src2.key = src3.key and src3.key < 10 " +
        "where src2.bla = src3.bla " +
        "order by src1.key, src1.value, src2.key, src2.value, src3.key, src3.value");
    ParsesOk("select * from src src1 " +
        "left outer join src src2 on " +
        "  (src1.key = src2.key and src1.key < 10 and src2.key > 10) " +
        "right outer join src src3 on " +
        "  (src2.key = src3.key and src3.key < 10) " +
        "full outer join src src3 on " +
        "  (src2.key = src3.key and src3.key < 10) " +
        "left semi join src src3 on " +
        "  (src2.key = src3.key and src3.key < 10) " +
        "join src src3 on " +
        "  (src2.key = src3.key and src3.key < 10) " +
        "inner join src src3 on " +
        "  (src2.key = src3.key and src3.key < 10) " +
        "where src2.bla = src3.bla " +
        "order by src1.key, src1.value, src2.key, src2.value, src3.key, src3.value");
    ParsesOk("select * from src src1 " +
        "left outer join src src2 using (a, b, c) " +
        "right outer join src src3 using (d, e, f) " +
        "full outer join src src3 using (d, e, f) " +
        "left semi join src src3 using (d, e, f) " +
        "join src src3 using (d, e, f) " +
        "inner join src src3 using (d, e, f) " +
        "where src2.bla = src3.bla " +
        "order by src1.key, src1.value, src2.key, src2.value, src3.key, src3.value");
    // Test NULLs in on clause.
    ParsesOk("select * from src src1 " +
        "left outer join src src2 on NULL " +
        "right outer join src src3 on (NULL) " +
        "full outer join src src3 on NULL " +
        "left semi join src src3 on (NULL) " +
        "join src src3 on NULL " +
        "inner join src src3 on (NULL) " +
        "where src2.bla = src3.bla " +
        "order by src1.key, src1.value, src2.key, src2.value, src3.key, src3.value");
    // Arbitrary exprs in on clause parse ok.
    ParsesOk("select * from src src1 join src src2 on ('a')");
    ParsesOk("select * from src src1 join src src2 on (f(a, b))");
    ParserError("select * from src src1 join src src2 using (1)");
    ParserError("select * from src src1 " +
        "left outer join src src2 on (src1.key = src2.key and)");
  }

  @Test
  public void TestWhereClause() {
    ParsesOk("select a, b from t where a > 15");
    ParsesOk("select a, b from t where true");
    ParsesOk("select a, b from t where NULL");
    // Non-predicate exprs that return boolean.
    ParsesOk("select a, b from t where case a when b then true else false end");
    ParsesOk("select a, b from t where if (a > b, true, false)");
    ParsesOk("select a, b from t where bool_col");
    // Arbitrary non-predicate exprs parse ok but are semantically incorrect.
    ParsesOk("select a, b from t where 10.5");
    ParsesOk("select a, b from t where trim('abc')");
    ParsesOk("select a, b from t where s + 20");
    ParserError("select a, b from t where a > 15 from test");
    ParserError("select a, b where a > 15");
    ParserError("select where a, b from t");
  }

  @Test
  public void TestGroupBy() {
    ParsesOk("select a, b, count(c) from test group by 1, 2");
    ParsesOk("select a, b, count(c) from test group by a, b");
    ParsesOk("select a, b, count(c) from test group by true, false, NULL");
    // semantically wrong but parses fine
    ParsesOk("select a, b, count(c) from test group by 1, b");
    ParserError("select a, b, count(c) from test group 1, 2");
    ParserError("select a, b, count(c) from test group by order by a");
  }

  @Test
  public void TestOrderBy() {
    ParsesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
             "order by string_col, 15.7 * float_col, int_col + bigint_col");
    ParsesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
             "order by string_col asc, 15.7 * float_col desc, int_col + bigint_col asc");
    ParsesOk("select int_col from alltypes order by true, false, NULL");
    ParserError("select int_col, string_col, bigint_col, count(*) from alltypes " +
                "order by by string_col asc desc");
  }

  @Test
  public void TestHaving() {
    ParsesOk("select a, b, count(c) from test group by a, b having count(*) > 5");
    ParsesOk("select a, b, count(c) from test group by a, b having NULL");
    ParsesOk("select a, b, count(c) from test group by a, b having true");
    ParsesOk("select a, b, count(c) from test group by a, b having false");
    // Non-predicate exprs that return boolean.
    ParsesOk("select count(c) from test group by a having if (a > b, true, false)");
    ParsesOk("select count(c) from test group by a " +
        "having case a when b then true else false end");
    // Arbitrary non-predicate exprs parse ok but are semantically incorrect.
    ParsesOk("select a, b, count(c) from test group by a, b having 5");
    ParserError("select a, b, count(c) from test group by a, b having order by 5");
    ParserError("select a, b, count(c) from test having count(*) > 5 group by a, b");
  }

  @Test
  public void TestLimit() {
    ParsesOk("select a, b, c from test inner join test2 using(a) limit 10");
    ParserError("select a, b, c from test inner join test2 using(a) limit 'a'");
    ParserError("select a, b, c from test inner join test2 using(a) limit a");
    ParserError("select a, b, c from test inner join test2 using(a) limit 10 + 10");
    ParserError("select a, b, c from test inner join test2 using(a) limit 10 " +
        "where a > 10");
    ParserError("select a, b, c from test inner join test2 using(a) limit true");
    ParserError("select a, b, c from test inner join test2 using(a) limit false");
    ParserError("select a, b, c from test inner join test2 using(a) limit NULL");
  }

  @Test
  public void TestUnion() {
    // Single union test.
    ParsesOk("select a from test union select a from test");
    ParsesOk("select a from test union all select a from test");
    ParsesOk("select a from test union distinct select a from test");
    // Chained union test.
    ParsesOk("select a from test union select a from test " +
        "union select a from test union select a from test");
    ParsesOk("select a from test union all select a from test " +
        "union all select a from test union all select a from test");
    ParsesOk("select a from test union distinct select a from test " +
        "union distinct select a from test union distinct select a from test ");
    // Mixed union with all and distinct.
    ParsesOk("select a from test union select a from test " +
        "union all select a from test union distinct select a from test");
    // No from clause.
    ParsesOk("select sin() union select cos()");
    ParsesOk("select sin() union all select cos()");
    ParsesOk("select sin() union distinct select cos()");

    // All select blocks in parenthesis.
    ParsesOk("(select a from test) union (select a from test) " +
        "union (select a from test) union (select a from test)");
    // Union with order by,
    ParsesOk("(select a from test) union (select a from test) " +
        "union (select a from test) union (select a from test) order by a");
    // Union with limit.
    ParsesOk("(select a from test) union (select a from test) " +
        "union (select a from test) union (select a from test) limit 10");
    // Union with order by and limit.
    ParsesOk("(select a from test) union (select a from test) " +
        "union (select a from test) union (select a from test) order by a limit 10");
    // Union with some select blocks in parenthesis, and others not.
    ParsesOk("(select a from test) union select a from test " +
        "union (select a from test) union select a from test");
    ParsesOk("select a from test union (select a from test) " +
        "union select a from test union (select a from test)");
    // Union with order by and limit binding to last select.
    ParsesOk("(select a from test) union (select a from test) " +
        "union select a from test union select a from test order by a limit 10");
    // Union with order by and limit.
    // Last select with order by and limit is in parenthesis.
    ParsesOk("select a from test union (select a from test) " +
        "union select a from test union (select a from test order by a limit 10) " +
        "order by a limit 1");
    // Union with order by and limit.
    // Last select with order by and limit is not in parenthesis.
    ParsesOk("select a from test union select a from test " +
        "union select a from test union select a from test order by a limit 10 " +
        "order by a limit 1");

    // Nested unions with order by and limit.
    ParsesOk("select a union " +
        "((select b) union (select c) order by 1 limit 1)");
    ParsesOk("select a union " +
    		"((select b) union " +
    		"  ((select c) union (select d) " +
    		"   order by 1 limit 1) " +
    		" order by 1 limit 1)");

    // Union in insert query.
    ParsesOk("insert into table t select a from test union select a from test");
    ParsesOk("insert into table t select a from test union select a from test " +
        "union select a from test union select a from test");
    ParsesOk("insert overwrite table t select a from test union select a from test");
    ParsesOk("insert overwrite table t select a from test union select a from test " +
        "union select a from test union select a from test");

    // No complete select statement on lhs.
    ParserError("a from test union select a from test");
    // No complete select statement on rhs.
    ParserError("select a from test union a from test");
    // Union cannot be a column or table since it's a keyword.
    ParserError("select union from test");
    ParserError("select a from union");
  }

  @Test
  public void TestOverflow() {
    ParsesOk("select " + Long.toString(Long.MAX_VALUE) + " from test");
    // We need to add 1 to MIN_VALUE because there are no negative integer literals.
    // The reason is that whether a minus belongs to an
    // arithmetic expr or a literal must be decided by the parser, not the lexer.
    ParsesOk("select " + Long.toString(Long.MIN_VALUE+1) + " from test");
    ParsesOk("select " + Double.toString(Double.MAX_VALUE) + " from test");
    ParsesOk("select " + Double.toString(Double.MIN_VALUE) + " from test");
    ParsesOk("select 0.0 from test");
    ParserError("select " + Long.toString(Long.MAX_VALUE) + "1 from test");
    ParserError("select " + Long.toString(Long.MIN_VALUE) + "1 from test");
    // java converts a float overflow to infinity, we consider it an error
    ParserError("select " + Double.toString(Double.MAX_VALUE) + "1 from test");
    // Java converts a float underflow to 0.0.
    // Since there is no easy, reliable way to detect underflow,
    // we don't consider it an error.
    ParsesOk("select " + Double.toString(Double.MIN_VALUE) + "1 from test");
  }

  @Test
  public void TestIdentQuoting() {
    ParsesOk("select a from `t`");
    ParsesOk("select a from `default`.`t`");
    ParsesOk("select a from `default`.t");
    ParsesOk("select a from default.`t`");
    ParsesOk("select 01a from default.`01_t`");

    ParsesOk("select `a` from default.t");
    ParsesOk("select `tbl`.`a` from default.t");
    ParsesOk("select `db`.`tbl`.`a` from default.t");
    ParsesOk("select `12db`.`tbl`.`12_a` from default.t");

    // Mixed quoting
    ParsesOk("select `db`.tbl.`a` from default.t");
    ParserError("select `db.table.a` from default.t");

    // Invalid identifiers in table names
    ParserError("select a from `all types`");
    ParserError("select a from `default`.`all types`");

    // Wrong quotes
    ParserError("select a from 'default'.'t'");

    // Lots of quoting
    ParsesOk(
        "select `db`.`tbl`.`a` from `default`.`t` `alias` where `alias`.`col` = 'string'"
        + " group by `alias`.`col`");

    // Quoting keywords should fail
    ParserError("select `col` `from` tbl");
  }

  @Test
  public void TestLiteralExprs() {
    // negative integer literal
    ParsesOk("select -1 from t where -1");
    ParsesOk("select - 1 from t where - 1");
    ParsesOk("select a - - 1 from t where a - - 1");
    ParsesOk("select a - - - 1 from t where a - - - 1");

    // positive integer literal
    ParsesOk("select +1 from t where +1");
    ParsesOk("select + 1 from t where + 1");
    ParsesOk("select a + + 1 from t where a + + 1");
    ParsesOk("select a + + + 1 from t where a + + + 1");

    // Float literals
    ParsesOk("select +1.0 from t where +1.0");
    ParsesOk("select +-1.0 from t where +-1.0");
    ParsesOk("select +1.-0 from t where +1.-0");

    // mixed signs
    ParsesOk("select -+-1 from t where -+-1");
    ParsesOk("select - +- 1 from t where - +- 1");
    ParsesOk("select 1 + -+ 1 from t where 1 + -+ 1");

    // Boolean literals
    ParsesOk("select true from t where true");
    ParsesOk("select false from t where false");

    // Null literal
    ParsesOk("select NULL from t where NULL");

    // -- is parsed as a comment starter
    ParserError("select --1");

    // Postfix operators must be binary
    ParserError("select 1- from t");
    ParserError("select 1 + from t");

    // Only - and + can be unary operators
    ParserError("select /1 from t");
    ParserError("select *1 from t");
    ParserError("select &1 from t");
    ParserError("select =1 from t");

    // test string literals with and without quotes in the literal
    ParsesOk("select 5, 'five', 5.0, i + 5 from t");
    ParsesOk("select \"\\\"five\\\"\" from t\n");
    ParsesOk("select \"\'five\'\" from t\n");
    ParsesOk("select \"\'five\" from t\n");

    // missing quotes
    ParserError("select \'5 from t");
    ParserError("select \"5 from t");
    ParserError("select '5 from t");
    ParserError("select `5 from t");
    ParserError("select \"\"five\"\" from t\n");
    ParserError("select 5.0.5 from t");

    // we implement MySQL-style escape sequences just like Hive:
    // http://dev.mysql.com/doc/refman/5.0/en/string-literals.html
    // test escape sequences with single and double quoted strings
    testStringLiteral("\\0");
    testStringLiteral("\\\\");
    testStringLiteral("\\b");
    testStringLiteral("\\n");
    testStringLiteral("\\r");
    testStringLiteral("\\t");
    testStringLiteral("\\Z");
    // MySQL deals with escapes and "%" and "_" specially for pattern matching
    testStringLiteral("\\%");
    testStringLiteral("\\\\%");
    testStringLiteral("\\_");
    testStringLiteral("\\\\_");
    // all escape sequences back-to-back
    testStringLiteral("\\0\\\\\\b\\n\\r\\t\\Z\\%\\\\%\\_\\\\_");
    // mixed regular chars and escape sequences
    testStringLiteral("a\\0b\\\\c\\bd\\ne\\rf\\tg\\Zh\\%i\\\\%j\\_k\\\\_l");
    // escaping non-escape chars should scan ok and result in the character itself
    testStringLiteral("\\a\\b\\c\\d\\1\\2\\3\\$\\&\\*");
    // Single backslash is a scanner error.
    ScannerError("select \"\\\" from t");
  }

  // test string literal s with single and double quotes
  private void testStringLiteral(String s) {
    String singleQuoteQuery = "select " + "'" + s + "'" + " from t";
    String doubleQuoteQuery = "select " + "\"" + s + "\"" + " from t";
    ParsesOk(singleQuoteQuery);
    ParsesOk(doubleQuoteQuery);
  }

  @Test
  public void TestFunctionCallExprs() {
    ParsesOk("select f1(5), f2('five'), f3(5.0, i + 5) from t");
    ParsesOk("select f1(true), f2(true and false), f3(null) from t");
    ParserError("select f( from t");
    ParserError("select f(5.0 5.0) from t");
  }

  @Test
  public void TestArithmeticExprs() {
    for (String lop: operands) {
      for (String rop: operands) {
        for (ArithmeticExpr.Operator op : ArithmeticExpr.Operator.values()) {
          // Test BITNOT separately.
          if (op == ArithmeticExpr.Operator.BITNOT) {
            continue;
          }
          String expr = String.format("%s %s %s", lop, op.toString(), rop);
          ParsesOk(String.format("select %s from t where %s", expr, expr));
        }
      }
      // Test BITNOT.
      String bitNotExpr = String.format("%s %s",
          ArithmeticExpr.Operator.BITNOT.toString(), lop);
      ParsesOk(String.format("select %s from t where %s", bitNotExpr, bitNotExpr));
    }
    ParserError("select (i + 5)(1 - i) from t");
    ParserError("select %a from t");
    ParserError("select *a from t");
    ParserError("select /a from t");
    ParserError("select &a from t");
    ParserError("select |a from t");
    ParserError("select ^a from t");
    ParserError("select a ~ a from t");
  }

  /**
   * We have three variants of timestamp arithmetic exprs, as in MySQL:
   * http://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html
   * (section #function_date-add)
   * 1. Non-function-call like version, e.g., 'a + interval b timeunit'
   * 2. Beginning with an interval (only for '+'), e.g., 'interval b timeunit + a'
   * 3. Function-call like version, e.g., date_add(a, interval b timeunit)
   */
  @Test
  public void TestTimestampArithmeticExprs() {
    // Tests all valid time units.
    for (TimeUnit timeUnit : TimeUnit.values()) {
      // Non-function call like versions.
      ParsesOk("select a + interval b " + timeUnit.toString());
      ParsesOk("select a - interval b " + timeUnit.toString());
      ParsesOk("select NULL + interval NULL " + timeUnit.toString());
      ParsesOk("select NULL - interval NULL " + timeUnit.toString());
      // Reversed interval and timestamp is ok for addition.
      ParsesOk("select interval b " + timeUnit.toString() + " + a");
      ParsesOk("select interval NULL " + timeUnit.toString() + " + NULL");
      // Reversed interval and timestamp is an error for subtraction.
      ParserError("select interval b " + timeUnit.toString() + " - a");
      // Function-call like versions.
      ParsesOk("select date_add(a, interval b " + timeUnit.toString() + ")");
      ParsesOk("select date_sub(a, interval b " + timeUnit.toString() + ")");
      ParsesOk("select date_add(NULL, interval NULL " + timeUnit.toString() + ")");
      ParsesOk("select date_sub(NULL, interval NULL " + timeUnit.toString() + ")");
      // Invalid function name for timestamp arithmetic expr should parse ok.
      ParsesOk("select error(a, interval b " + timeUnit.toString() + ")");
      // Invalid time unit parses ok.
      ParsesOk("select error(a, interval b error)");
      // Missing 'interval' keyword. Note that the non-function-call like version will
      // pass without 'interval' because the time unit is recognized as an alias.
      ParserError("select date_add(a, b " + timeUnit.toString() + ")");
      ParserError("select date_sub(a, b " + timeUnit.toString() + ")");
    }

    // Test chained timestamp arithmetic exprs.
    ParsesOk("select a + interval b years + interval c months + interval d days");
    ParsesOk("select a - interval b years - interval c months - interval d days");
    ParsesOk("select a + interval b years - interval c months + interval d days");
    // Starting with interval.
    ParsesOk("select interval b years + a + interval c months + interval d days");
    ParsesOk("select interval b years + a - interval c months - interval d days");
    ParsesOk("select interval b years + a - interval c months + interval d days");

    // To many arguments.
    ParserError("select date_sub(a, c, interval b year)");
    ParserError("select date_sub(a, interval b year, c)");
  }

  @Test
  public void TestCaseExprs() {
    // Test regular exps.
    ParsesOk("select case a when '5' then x when '6' then y else z end from t");
    ParsesOk("select case when 'a' then x when false then y else z end from t");
    // Test predicates in case, when, then, and else exprs.
    ParsesOk("select case when a > 2 then x when false then false else true end from t");
    ParsesOk("select case false when a > 2 then x when '6' then false else true end " +
        "from t");
    // Test NULLs;
    ParsesOk("select case NULL when NULL then NULL when NULL then NULL else NULL end " +
        "from t");
    ParsesOk("select case when NULL then NULL when NULL then NULL else NULL end from t");
    // Missing end.
    ParserError("select case a when true then x when false then y else z from t");
    // Missing else after first when.
    ParserError("select case a when true when false then y else z end from t");
    // Incorrectly placed comma.
    ParserError("select case a when true, false then y else z end from t");
  }

  @Test
  public void TestCastExprs() {
    ParsesOk("select cast(a + 5.0 as string) from t");
    ParsesOk("select cast(NULL as string) from t");
    ParserError("select cast(a + 5.0 as badtype) from t");
    ParserError("select cast(a + 5.0, string) from t");
  }

  @Test
  public void TestConditionalExprs() {
    ParsesOk("select if(TRUE, TRUE, FALSE) from t");
    ParsesOk("select if(NULL, NULL, NULL) from t");
    ParsesOk("select c1, c2, if(TRUE, TRUE, FALSE) from t");
    ParsesOk("select if(1 = 2, c1, c2) from t");
    ParsesOk("select if(1 = 2, c1, c2)");
    ParserError("select if()");
  }

  @Test
  public void TestAggregateExprs() {
    ParsesOk("select count(*), count(a), count(distinct a, b) from t");
    ParsesOk("select count(NULL), count(TRUE), count(FALSE), " +
             "count(distinct TRUE, FALSE, NULL) from t");
    ParserError("select count() from t");
    ParsesOk("select count(all *) from t");
    ParsesOk("select count(all 1) from t");
    ParsesOk("select min(a), min(distinct a) from t");
    ParserError("select min() from t");
    ParsesOk("select max(a), max(distinct a) from t");
    ParserError("select max() from t");
    ParsesOk("select sum(a), sum(distinct a) from t");
    ParserError("select sum() from t");
    ParsesOk("select avg(a), avg(distinct a) from t");
    ParserError("select avg() from t");
    ParsesOk("select distinct a, b, c from t");
    ParsesOk("select distinctpc(a), distinctpc(distinct a) from t");
    ParserError("select distinctpc() from t");
    ParsesOk("select distinctpcsa(a), distinctpcsa(distinct a) from t");
    ParserError("select distinctpcsa() from t");
  }

  @Test
  public void TestPredicates() {
    ArrayList<String> operations = new ArrayList<String>();
    for (BinaryPredicate.Operator op : BinaryPredicate.Operator.values()) {
      operations.add(op.toString());
    }
    operations.add("like");
    operations.add("rlike");
    operations.add("regexp");

    for (String lop: operands) {
      for (String rop: operands) {
        for (String op : operations) {
          String expr = String.format("%s %s %s", lop, op.toString(), rop);
          ParsesOk(String.format("select %s from t where %s", expr, expr));
        }
      }
      String isNullExr = String.format("%s is null", lop);
      String isNotNullExr = String.format("%s is not null", lop);
      ParsesOk(String.format("select %s from t where %s", isNullExr, isNullExr));
      ParsesOk(String.format("select %s from t where %s", isNotNullExr, isNotNullExr));
    }
  }

  private void testCompoundPredicates(String andStr, String orStr, String notStr) {
    // select a, b, c from t where a = 5 and f(b)
    ParsesOk("select a, b, c from t where a = 5 " + andStr + " f(b)");
    // select a, b, c from t where a = 5 or f(b)
    ParsesOk("select a, b, c from t where a = 5 " + orStr + " f(b)");
    // select a, b, c from t where (a = 5 or f(b)) and c = 7
    ParsesOk("select a, b, c from t where (a = 5 " + orStr + " f(b)) " +
        andStr + " c = 7");
    // select a, b, c from t where not a = 5
    ParsesOk("select a, b, c from t where " + notStr + "a = 5");
    // select a, b, c from t where not f(a)
    ParsesOk("select a, b, c from t where " + notStr + "f(a)");
    // select a, b, c from t where (not a = 5 or not f(b)) and not c = 7
    ParsesOk("select a, b, c from t where (" + notStr + "a = 5 " + orStr + " " +
        notStr + "f(b)) " + andStr + " " + notStr + "c = 7");
    // select a, b, c from t where (!(!a = 5))
    ParsesOk("select a, b, c from t where (" + notStr + "(" + notStr + "a = 5))");
    // select a, b, c from t where (!(!f(a)))
    ParsesOk("select a, b, c from t where (" + notStr + "(" + notStr + "f(a)))");
    // semantically incorrect negation, but parses ok
    ParsesOk("select a, b, c from t where a = " + notStr + "5");
    // unbalanced parentheses
    ParserError("select a, b, c from t where (a = 5 " + orStr + " b = 6) " + andStr + " c = 7)");
    ParserError("select a, b, c from t where ((a = 5 " + orStr + " b = 6) " + andStr + " c = 7");
    // incorrectly positioned negation (!)
    ParserError("select a, b, c from t where a = 5 " + orStr + " " + notStr);
    ParserError("select a, b, c from t where " + notStr + "(a = 5) " + orStr + " " + notStr);
  }

  private void testLiteralTruthValues(String andStr, String orStr, String notStr) {
    String[] truthValues = {"true", "false", "null"};
    for (String l: truthValues) {
      for (String r: truthValues) {
        String andExpr = String.format("%s %s %s", l, andStr, r);
        String orExpr = String.format("%s %s %s", l, orStr, r);
        ParsesOk(String.format("select %s from t where %s", andExpr, andExpr));
        ParsesOk(String.format("select %s from t where %s", orExpr, orExpr));
      }
      String notExpr = String.format("%s %s", notStr, l);
      ParsesOk(String.format("select %s from t where %s", notExpr, notExpr));
    }
  }

  @Test
  public void TestCompoundPredicates() {
    String[] andStrs = { "and", "&&" };
    String[] orStrs = { "or", "||" };
    // Note the trailing space in "not ". We want to test "!" without a space.
    String[] notStrs = { "!", "not " };
    // Test all combinations of representations for 'or', 'and', and 'not'.
    for (String andStr : andStrs) {
      for (String orStr : orStrs) {
        for (String notStr : notStrs) {
          testCompoundPredicates(andStr, orStr, notStr);
          testLiteralTruthValues(andStr, orStr, notStr);
        }
      }
    }
  }

  @Test
  public void TestBetweenPredicate() {
    ParsesOk("select a, b, c from t where i between x and y");
    ParsesOk("select a, b, c from t where i not between x and y");
    ParsesOk("select a, b, c from t where true not between false and NULL");
    ParsesOk("select a, b, c from t where 'abc' between 'a' like 'a' and 'b' like 'b'");
    // Additional conditions before and after between predicate.
    ParsesOk("select a, b, c from t where true and false and i between x and y");
    ParsesOk("select a, b, c from t where i between x and y and true and false");
    ParsesOk("select a, b, c from t where i between x and (y and true) and false");
    ParsesOk("select a, b, c from t where i between x and (y and (true and false))");
    // Chaining/nesting of between predicates.
    ParsesOk("select a, b, c from t " +
        "where true between false and true and 'b' between 'a' and 'c'");
    // true between ('b' between 'a' and 'b') and ('bb' between 'aa' and 'cc)
    ParsesOk("select a, b, c from t " +
        "where true between 'b' between 'a' and 'c' and 'bb' between 'aa' and 'cc'");
    // Missing condition expr.
    ParserError("select a, b, c from t where between 5 and 10");
    // Missing lower bound.
    ParserError("select a, b, c from t where i between and 10");
    // Missing upper bound.
    ParserError("select a, b, c from t where i between 5 and");
    // Missing exprs after between.
    ParserError("select a, b, c from t where i between");
    // AND has a higher precedence than OR.
    ParserError("select a, b, c from t where true between 5 or 10 and 20");
  }

  @Test
  public void TestInPredicate() {
    ParsesOk("select a, b, c from t where i in (x, y, z)");
    ParsesOk("select a, b, c from t where i not in (x, y, z)");
    // Test NULL and boolean literals.
    ParsesOk("select a, b, c from t where NULL in (NULL, NULL, NULL)");
    ParsesOk("select a, b, c from t where true in (true, false, true)");
    ParsesOk("select a, b, c from t where NULL not in (NULL, NULL, NULL)");
    ParsesOk("select a, b, c from t where true not in (true, false, true)");
    // Missing condition expr.
    ParserError("select a, b, c from t where in (x, y, z)");
    // Missing parentheses around in list.
    ParserError("select a, b, c from t where i in x, y, z");
    ParserError("select a, b, c from t where i in (x, y, z");
    ParserError("select a, b, c from t where i in x, y, z)");
    // Missing in list.
    ParserError("select a, b, c from t where i in");
    ParserError("select a, b, c from t where i in ( )");
  }

  @Test
  public void TestSlotRef() {
    ParsesOk("select a from t where b > 5");
    ParsesOk("select a.b from a where b > 5");
    ParsesOk("select a.b.c from a.b where b > 5");
    ParserError("select a.b.c.d from a.b where b > 5");
  }

  /**
   * Run positive tests for INSERT INTO/OVERWRITE:
   *
   * @param overwrite
   *          If true, tests INSERT OVERWRITE, else tests INSERT INTO.
   */
  private void testInsert(boolean overwrite, boolean use_kw_table) {
    String qualifier = overwrite ? "overwrite" : "into";
    qualifier = use_kw_table ? qualifier + " table" : qualifier;

    // Entire unpartitioned table.
    ParsesOk("insert " + qualifier + " t select a from src where b > 5");
    // Static partition with one partitioning key.
    ParsesOk(
        "insert " + qualifier + " t partition (pk1=10) select a from src where b > 5");
    // Dynamic partition with one partitioning key.
    ParsesOk("insert " + qualifier + " t partition (pk1) select a from src where b > 5");
    // Static partition with two partitioning keys.
    ParsesOk("insert " + qualifier + " t partition (pk1=10, pk2=20) " +
        "select a from src where b > 5");
    // Fully dynamic partition with two partitioning keys.
    ParsesOk("insert " + qualifier + " t partition (pk1, pk2) " +
        "select a from src where b > 5");
    // Partially dynamic partition with two partitioning keys.
    ParsesOk("insert " + qualifier + " t partition (pk1=10, pk2) " +
        "select a from src where b > 5");
    // Partially dynamic partition with two partitioning keys.
    ParsesOk("insert " + qualifier + " t partition (pk1, pk2=20) " +
        "select a from src where b > 5");
    // Static partition with two NULL partitioning keys.
    ParsesOk("insert " + qualifier + " t partition (pk1=NULL, pk2=NULL) " +
        "select a from src where b > 5");
    // Static partition with boolean partitioning keys.
    ParsesOk("insert " + qualifier + " t partition (pk1=false, pk2=true) " +
        "select a from src where b > 5");
  }

  @Test
  public void TestInsert() {
    // Positive tests.
    testInsert(true, false);
    testInsert(true, true);
    testInsert(false, false);
    testInsert(false, true);
    // Missing 'overwrite/insert'.
    ParserError("insert table t select a from src where b > 5");
    // Missing target table identifier.
    ParserError("insert overwrite table select a from src where b > 5");
    // Missing target table identifier.
    ParserError("insert into table select a from src where b > 5");
    // Missing select statement.
    ParserError("insert overwrite table t");
    // Missing select statement.
    ParserError("insert into table t");
    // Missing parentheses around 'partition'.
    ParserError("insert overwrite table t partition pk1=10 " +
    		"select a from src where b > 5");
    // Missing parentheses around 'partition'.
    ParserError("insert into table t partition pk1=10 " +
        "select a from src where b > 5");
    // Missing comma in partition list.
    ParserError("insert overwrite table t partition (pk1=10 pk2=20) " +
    		"select a from src where b > 5");
    // Missing comma in partition list.
    ParserError("insert into table t partition (pk1=10 pk2=20) " +
        "select a from src where b > 5");
  }

  @Test
  public void TestUse() {
    ParserError("USE");
    ParserError("USE db1 db2");
    ParsesOk("USE db1");
  }

  @Test
  public void TestShow() {
    // Missing arguments
    ParserError("SHOW");
    // Short form ok
    ParsesOk("SHOW TABLES");
    // Malformed pattern (no quotes)
    ParserError("SHOW TABLES tablename");
    // Well-formed pattern
    ParsesOk("SHOW TABLES 'tablename|othername'");
    // Empty pattern ok
    ParsesOk("SHOW TABLES ''");
    // Databases
    ParsesOk("SHOW DATABASES");
    ParsesOk("SHOW SCHEMAS");
    ParsesOk("SHOW DATABASES LIKE 'pattern'");
    ParsesOk("SHOW SCHEMAS LIKE 'p*ttern'");
  }

  @Test
  public void TestDescribe() {
    // Missing argument
    ParserError("DESCRIBE");
    // Unqualified table ok
    ParsesOk("DESCRIBE tablename");
    // Fully-qualified table ok
    ParsesOk("DESCRIBE databasename.tablename");
  }

  @Test
  public void TestCreateDatabase() {
    // Both CREATE DATABASE and CREATE SCHEMA are valid (and equivalent)
    String [] dbKeywords = {"DATABASE", "SCHEMA"};
    for (String kw: dbKeywords) {
      ParsesOk(String.format("CREATE %s Foo", kw));
      ParsesOk(String.format("CREATE %s IF NOT EXISTS Foo", kw));

      ParsesOk(String.format("CREATE %s Foo COMMENT 'Some comment'", kw));
      ParsesOk(String.format("CREATE %s Foo LOCATION '/hdfs_location'", kw));
      ParsesOk(String.format("CREATE %s Foo LOCATION '/hdfs_location'", kw));
      ParsesOk(String.format(
          "CREATE %s Foo COMMENT 'comment' LOCATION '/hdfs_location'", kw));

      // Only string literals are supported
      ParserError(String.format("CREATE %s Foo COMMENT mytable", kw));
      ParserError(String.format("CREATE %s Foo LOCATION /hdfs_location", kw));

      // COMMENT needs to be *before* LOCATION
      ParserError(String.format(
          "CREATE %s Foo LOCATION '/hdfs/location' COMMENT 'comment'", kw));

      ParserError(String.format("CREATE %s Foo COMMENT LOCATION '/hdfs_location'", kw));
      ParserError(String.format("CREATE %s Foo LOCATION", kw));
      ParserError(String.format("CREATE %s Foo LOCATION 'dfsd' 'dafdsf'", kw));

      ParserError(String.format("CREATE Foo", kw));
      ParserError(String.format("CREATE %s 'Foo'", kw));
      ParserError(String.format("CREATE %s", kw));
      ParserError(String.format("CREATE %s IF EXISTS Foo", kw));

      ParserError(String.format("CREATE %sS Foo", kw));
    }
  }

  @Test
  public void TestAlterTableAddReplaceColumns() {
    String[] addReplaceKw = {"ADD", "REPLACE"};
    for (String addReplace: addReplaceKw) {
      ParsesOk(String.format(
          "ALTER TABLE Foo %s COLUMNS (i int, s string)", addReplace));
      ParsesOk(String.format(
          "ALTER TABLE TestDb.Foo %s COLUMNS (i int, s string)", addReplace));
      ParsesOk(String.format(
          "ALTER TABLE TestDb.Foo %s COLUMNS (i int)", addReplace));
      ParsesOk(String.format(
          "ALTER TABLE TestDb.Foo %s COLUMNS (i int comment 'hi')", addReplace));

      // Negative syntax tests
      ParserError(String.format("ALTER TABLE TestDb.Foo %s COLUMNS i int", addReplace));
      ParserError(String.format(
          "ALTER TABLE TestDb.Foo %s COLUMNS (int i)", addReplace));
      ParserError(String.format(
          "ALTER TABLE TestDb.Foo %s COLUMNS (i int COMMENT)", addReplace));
      ParserError(String.format("ALTER TestDb.Foo %s COLUMNS (i int)", addReplace));
      ParserError(String.format("ALTER TestDb.Foo %s COLUMNS", addReplace));
      ParserError(String.format("ALTER TestDb.Foo %s COLUMNS ()", addReplace));
      ParserError(String.format("ALTER Foo %s COLUMNS (i int, s string)", addReplace));
      ParserError(String.format("ALTER TABLE %s COLUMNS (i int, s string)", addReplace));
      // Don't yet support ALTER TABLE ADD COLUMN syntax
      ParserError(String.format("ALTER TABLE Foo %s COLUMN i int", addReplace));
      ParserError(String.format("ALTER TABLE Foo %s COLUMN (i int)", addReplace));
    }
  }

  @Test
  public void TestAlterTableAddPartition() {
    ParsesOk("ALTER TABLE Foo ADD PARTITION (i=1)");
    ParsesOk("ALTER TABLE TestDb.Foo ADD IF NOT EXISTS PARTITION (i=1, s='Hello')");
    ParsesOk("ALTER TABLE TestDb.Foo ADD PARTITION (i=1, s='Hello') LOCATION '/a/b'");
    ParsesOk("ALTER TABLE Foo ADD PARTITION (i=NULL)");
    ParsesOk("ALTER TABLE Foo ADD PARTITION (i=NULL, j=2, k=NULL)");

    // Cannot use dynamic partition syntax
    ParserError("ALTER TABLE TestDb.Foo ADD PARTITION (partcol)");
    ParserError("ALTER TABLE TestDb.Foo ADD PARTITION (i=1, partcol)");
    // Location needs to be a string literal
    ParserError("ALTER TABLE TestDb.Foo ADD PARTITION (i=1, s='Hello') LOCATION a/b");

    ParserError("ALTER TABLE Foo ADD IF EXISTS PARTITION (i=1, s='Hello')");
    ParserError("ALTER TABLE TestDb.Foo ADD (i=1, s='Hello')");
    ParserError("ALTER TABLE TestDb.Foo ADD (i=1)");
    ParserError("ALTER TABLE Foo (i=1)");
    ParserError("ALTER TABLE TestDb.Foo PARTITION (i=1)");
    ParserError("ALTER TABLE Foo ADD PARTITION");
    ParserError("ALTER TABLE TestDb.Foo ADD PARTITION ()");
    ParserError("ALTER Foo ADD PARTITION (i=1)");
    ParserError("ALTER TABLE ADD PARTITION (i=1)");
  }

  @Test
  public void TestAlterTableDropColumn() {
    // KW_COLUMN is optional
    String[] columnKw = {"COLUMN", ""};
    for (String kw: columnKw) {
      ParsesOk(String.format("ALTER TABLE Foo DROP %s col1", kw));
      ParsesOk(String.format("ALTER TABLE TestDb.Foo DROP %s col1", kw));

      // Negative syntax tests
      ParserError(String.format("ALTER TABLE TestDb.Foo DROP %s col1, col2", kw));
      ParserError(String.format("ALTER TABLE TestDb.Foo DROP %s", kw));
      ParserError(String.format("ALTER TABLE Foo DROP %s 'col1'", kw));
      ParserError(String.format("ALTER Foo DROP %s col1", kw));
      ParserError(String.format("ALTER TABLE DROP %s col1", kw));
      ParserError(String.format("ALTER TABLE DROP %s", kw));
    }
  }

  @Test
  public void TestAlterTableDropPartition() {
    ParsesOk("ALTER TABLE Foo DROP PARTITION (i=1)");
    ParsesOk("ALTER TABLE TestDb.Foo DROP IF EXISTS PARTITION (i=1, s='Hello')");
    ParsesOk("ALTER TABLE Foo DROP PARTITION (i=NULL)");
    ParsesOk("ALTER TABLE Foo DROP PARTITION (i=NULL, j=2, k=NULL)");

    // Cannot use dynamic partition syntax
    ParserError("ALTER TABLE Foo DROP PARTITION (partcol)");
    ParserError("ALTER TABLE Foo DROP PARTITION (i=1, j)");

    ParserError("ALTER TABLE Foo DROP IF NOT EXISTS PARTITION (i=1, s='Hello')");
    ParserError("ALTER TABLE TestDb.Foo DROP (i=1, s='Hello')");
    ParserError("ALTER TABLE TestDb.Foo DROP (i=1)");
    ParserError("ALTER TABLE Foo (i=1)");
    ParserError("ALTER TABLE TestDb.Foo PARTITION (i=1)");
    ParserError("ALTER TABLE Foo DROP PARTITION");
    ParserError("ALTER TABLE TestDb.Foo DROP PARTITION ()");
    ParserError("ALTER Foo DROP PARTITION (i=1)");
    ParserError("ALTER TABLE DROP PARTITION (i=1)");
  }

  @Test
  public void TestAlterTableChangeColumn() {
    // KW_COLUMN is optional
    String[] columnKw = {"COLUMN", ""};
    for (String kw: columnKw) {
      ParsesOk(String.format("ALTER TABLE Foo.Bar CHANGE %s c1 c2 int", kw));
      ParsesOk(String.format("ALTER TABLE Foo CHANGE %s c1 c2 int comment 'hi'", kw));

      // Negative syntax tests
      ParserError(String.format("ALTER TABLE Foo CHANGE %s c1 int c2", kw));
      ParserError(String.format("ALTER TABLE Foo CHANGE %s col1 int", kw));
      ParserError(String.format("ALTER TABLE Foo CHANGE %s col1", kw));
      ParserError(String.format("ALTER TABLE Foo CHANGE %s", kw));
      ParserError(String.format("ALTER TABLE CHANGE %s c1 c2 int", kw));
    }
  }

  @Test
  public void TestAlterTableSet() {
    // Supported file formats
    for (FileFormat format: FileFormat.values()) {
      ParsesOk("ALTER TABLE Foo SET FILEFORMAT " + format);
      ParsesOk("ALTER TABLE TestDb.Foo SET FILEFORMAT " + format);
      ParsesOk("ALTER TABLE TestDb.Foo PARTITION (a=1) SET FILEFORMAT " + format);
      ParsesOk("ALTER TABLE Foo PARTITION (s='str') SET FILEFORMAT " + format);
      ParserError("ALTER TABLE TestDb.Foo PARTITION (i=5) SET " + format);
      ParserError("ALTER TABLE TestDb.Foo SET " + format);
      ParserError("ALTER TABLE TestDb.Foo " + format);
    }
    ParserError("ALTER TABLE TestDb.Foo SET FILEFORMAT");

    ParsesOk("ALTER TABLE Foo SET LOCATION '/a/b/c'");
    ParsesOk("ALTER TABLE TestDb.Foo SET LOCATION '/a/b/c'");

    ParsesOk("ALTER TABLE Foo PARTITION (i=1,s='str') SET LOCATION '/a/i=1/s=str'");
    ParsesOk("ALTER TABLE Foo PARTITION (s='str') SET LOCATION '/a/i=1/s=str'");

    ParserError("ALTER TABLE Foo PARTITION (s) SET LOCATION '/a'");
    ParserError("ALTER TABLE Foo PARTITION () SET LOCATION '/a'");
    ParserError("ALTER TABLE Foo PARTITION ('str') SET FILEFORMAT TEXTFILE");
    ParserError("ALTER TABLE Foo PARTITION (a=1, 5) SET FILEFORMAT TEXTFILE");
    ParserError("ALTER TABLE Foo PARTITION () SET FILEFORMAT PARQUETFILE");
    ParserError("ALTER TABLE Foo PARTITION (,) SET FILEFORMAT PARQUETFILE");
    ParserError("ALTER TABLE Foo PARTITION (a=1) SET FILEFORMAT");
    ParserError("ALTER TABLE Foo PARTITION (a=1) SET LOCATION");
    ParserError("ALTER TABLE TestDb.Foo SET LOCATION abc");
    ParserError("ALTER TABLE TestDb.Foo SET LOCATION");
    ParserError("ALTER TABLE TestDb.Foo SET");
  }

  @Test
  public void TestAlterTableRename() {
    ParsesOk("ALTER TABLE TestDb.Foo RENAME TO TestDb.Foo2");
    ParsesOk("ALTER TABLE Foo RENAME TO TestDb.Foo2");
    ParsesOk("ALTER TABLE TestDb.Foo RENAME TO Foo2");
    ParsesOk("ALTER TABLE Foo RENAME TO Foo2");
    ParserError("ALTER TABLE Foo RENAME TO 'Foo2'");
    ParserError("ALTER TABLE Foo RENAME Foo2");
    ParserError("ALTER TABLE Foo RENAME TO");
    ParserError("ALTER TABLE Foo TO Foo2");
    ParserError("ALTER TABLE Foo TO Foo2");
  }

  @Test
  public void TestCreateTable() {
    // Support unqualified and fully-qualified table names
    ParsesOk("CREATE TABLE Foo (i int)");
    ParsesOk("CREATE TABLE Foo.Bar (i int)");
    ParsesOk("CREATE TABLE IF NOT EXISTS Foo.Bar (i int)");
    ParsesOk("CREATE TABLE Foo.Bar2 LIKE Foo.Bar1");
    ParsesOk("CREATE TABLE IF NOT EXISTS Bar2 LIKE Bar1");
    ParsesOk("CREATE EXTERNAL TABLE IF NOT EXISTS Bar2 LIKE Bar1");
    ParsesOk("CREATE EXTERNAL TABLE IF NOT EXISTS Bar2 LIKE Bar1 LOCATION '/a/b'");
    ParsesOk("CREATE TABLE Foo2 LIKE Foo COMMENT 'sdafsdf'");
    ParsesOk("CREATE TABLE Foo2 LIKE Foo COMMENT ''");
    ParsesOk("CREATE TABLE Foo2 LIKE Foo STORED AS PARQUETFILE");
    ParsesOk("CREATE TABLE Foo2 LIKE Foo COMMENT 'tbl' " +
        "STORED AS PARQUETFILE LOCATION '/a/b'");
    ParsesOk("CREATE TABLE Foo2 LIKE Foo STORED AS TEXTFILE LOCATION '/a/b'");

    // Table and column names starting with digits.
    ParsesOk("CREATE TABLE 01_Foo (01_i int, 02_j string)");

    ParsesOk("CREATE TABLE Foo (i int, s string)");
    ParsesOk("CREATE EXTERNAL TABLE Foo (i int, s string)");
    ParsesOk("CREATE EXTERNAL TABLE Foo (i int, s string) LOCATION '/test-warehouse/'");
    ParsesOk("CREATE TABLE Foo (i int, s string) COMMENT 'hello' LOCATION '/a/b/'");

    // Partitioned tables
    ParsesOk("CREATE TABLE Foo (i int) PARTITIONED BY (j string)");
    ParsesOk("CREATE TABLE Foo (i int) PARTITIONED BY (s string, d double)");
    ParsesOk("CREATE TABLE Foo (i int, s string) PARTITIONED BY (s string, d double)" +
        " COMMENT 'hello' LOCATION '/a/b/'");
    ParserError("CREATE TABLE Foo (i int) PARTITIONED BY (int)");
    ParserError("CREATE TABLE Foo (i int) PARTITIONED BY ()");
    ParserError("CREATE TABLE Foo (i int) PARTITIONED BY");

    // Column comments
    ParsesOk("CREATE TABLE Foo (i int COMMENT 'hello', s string)");
    ParsesOk("CREATE TABLE Foo (i int COMMENT 'hello', s string COMMENT 'hi')");
    ParsesOk("CREATE TABLE T (i int COMMENT 'hi') PARTITIONED BY (j int COMMENT 'bye')");

    // Supported file formats
    for (FileFormat format: FileFormat.values()) {
      ParsesOk("CREATE TABLE Foo (i int, s string) STORED AS " + format);
      ParsesOk("CREATE EXTERNAL TABLE Foo (i int, s string) STORED AS " + format);
      ParsesOk(String.format(
          "CREATE TABLE Foo (i int, s string) STORED AS %s LOCATION '/b'", format));
      ParsesOk(String.format(
          "CREATE EXTERNAL TABLE Foo (f float) COMMENT 'c' STORED AS %s LOCATION '/b'",
          format));
    }
    ParserError("CREATE TABLE Foo (i int, s string) STORED AS SEQFILE");
    ParserError("CREATE TABLE Foo (i int, s string) STORED TEXTFILE");
    ParserError("CREATE TABLE Foo LIKE Bar STORED AS TEXT");
    ParserError("CREATE TABLE Foo LIKE Bar COMMENT");
    ParserError("CREATE TABLE Foo LIKE Bar STORED TEXTFILE");
    ParserError("CREATE TABLE Foo LIKE Bar STORED AS");
    ParserError("CREATE TABLE Foo LIKE Bar LOCATION");

    // Row format syntax
    ParsesOk("CREATE TABLE T (i int) ROW FORMAT DELIMITED");
    ParsesOk("CREATE TABLE T (i int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'");
    ParsesOk("CREATE TABLE T (i int) ROW FORMAT DELIMITED LINES TERMINATED BY '|'");
    ParsesOk("CREATE TABLE T (i int) ROW FORMAT DELIMITED ESCAPED BY '\'");
    ParsesOk("CREATE TABLE T (i int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\0'" +
        " ESCAPED BY '\3' LINES TERMINATED BY '\1'");
    ParsesOk("CREATE TABLE T (i int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\0'" +
        " LINES TERMINATED BY '\1' STORED AS TEXTFILE");
    ParsesOk("CREATE TABLE T (i int) COMMENT 'hi' ROW FORMAT DELIMITED STORED AS RCFILE");
    ParsesOk("CREATE TABLE T (i int) COMMENT 'hello' ROW FORMAT DELIMITED FIELDS " +
        "TERMINATED BY '\0' LINES TERMINATED BY '\1' STORED AS TEXTFILE LOCATION '/a'");

    // Negative row format syntax
    ParserError("CREATE TABLE T (i int) ROW FORMAT DELIMITED TERMINATED BY '\0'");
    ParserError("CREATE TABLE T (i int) ROW FORMAT DELIMITED FIELDS TERMINATED BY");
    ParserError("CREATE TABLE T (i int) ROW FORMAT DELIMITED LINES TERMINATED BY");
    ParserError("CREATE TABLE T (i int) ROW FORMAT DELIMITED ESCAPED BY");
    ParserError("CREATE TABLE T (i int) ROW FORMAT DELIMITED FIELDS TERMINATED '|'");
    ParserError("CREATE TABLE T (i int) ROW FORMAT DELIMITED FIELDS TERMINATED BY |");
    ParserError("CREATE TABLE T (i int) ROW FORMAT DELIMITED FIELDS BY '\0'");
    ParserError("CREATE TABLE T (i int) ROW FORMAT DELIMITED LINES BY '\n'");
    ParserError("CREATE TABLE T (i int) FIELDS TERMINATED BY '\0'");
    ParserError("CREATE TABLE T (i int) ROWS TERMINATED BY '\0'");
    ParserError("CREATE TABLE T (i int) ESCAPED BY '\0'");

    // Order should be: [comment] [partition by cols] [row format] [stored as FILEFORMAT]
    // [location]
    ParserError("CREATE TABLE Foo (d double) COMMENT 'c' PARTITIONED BY (i int)");
    ParserError("CREATE TABLE Foo (d double) STORED AS TEXTFILE COMMENT 'c'");
    ParserError("CREATE TABLE Foo (d double) STORED AS TEXTFILE ROW FORMAT DELIMITED");
    ParserError("CREATE TABLE Foo (d double) ROW FORMAT DELIMITED COMMENT 'c'");
    ParserError("CREATE TABLE Foo (d double) LOCATION 'a' COMMENT 'c'");
    ParserError("CREATE TABLE Foo (d double) LOCATION 'a' COMMENT 'c' STORED AS RCFILE");
    ParserError("CREATE TABLE Foo (d double) LOCATION 'a' STORED AS RCFILE");

    // Location and comment need to be string literals, file format is not
    ParserError("CREATE TABLE Foo (d double) LOCATION a");
    ParserError("CREATE TABLE Foo (d double) COMMENT c");
    ParserError("CREATE TABLE Foo (d double COMMENT c)");
    ParserError("CREATE TABLE Foo (d double COMMENT 'c') PARTITIONED BY (j COMMENT hi)");
    ParserError("CREATE TABLE Foo (d double) STORED AS 'TEXTFILE'");

    // Invalid syntax
    ParserError("CREATE TABLE IF EXISTS Foo.Bar (i int)");
    ParserError("CREATE TABLE Bar LIKE Bar2 (i int)");
    ParserError("CREATE IF NOT EXISTS TABLE Foo.Bar (i int)");
    ParserError("CREATE TABLE Foo (d double) STORED TEXTFILE");
    ParserError("CREATE TABLE Foo (d double) AS TEXTFILE");
    ParserError("CREATE TABLE Foo i int");
    ParserError("CREATE TABLE Foo (i intt)");
    ParserError("CREATE TABLE Foo (int i)");
    ParserError("CREATE TABLE Foo ()");
    ParserError("CREATE TABLE Foo.Bar");
    ParserError("CREATE TABLE Foo");
    ParserError("CREATE TABLE");
    ParserError("CREATE EXTERNAL");
    ParserError("CREATE EXTERNAL TABLE Foo");
    ParserError("CREATE");
  }

  @Test
  public void TestDrop() {
    ParsesOk("DROP TABLE Foo");
    ParsesOk("DROP TABLE Foo.Bar");
    ParsesOk("DROP TABLE IF EXISTS Foo.Bar");
    ParsesOk("DROP DATABASE Foo");
    ParsesOk("DROP SCHEMA Foo");
    ParsesOk("DROP DATABASE IF EXISTS Foo");
    ParsesOk("DROP SCHEMA IF EXISTS Foo");

    ParserError("DROP");
    ParserError("DROP Foo");
    ParserError("DROP DATABASE Foo.Bar");
    ParserError("DROP SCHEMA Foo.Bar");
    ParserError("DROP DATABASE Foo Bar");
    ParserError("DROP SCHEMA Foo Bar");
    ParserError("DROP TABLE IF Foo");
    ParserError("DROP TABLE EXISTS Foo");
    ParserError("DROP IF EXISTS TABLE Foo");
    ParserError("DROP TBL Foo");
  }

  @Test
  public void TestTypeSynonyms() {
    ParsesOk("CREATE TABLE bar (i INTEGER)");
    ParsesOk("CREATE TABLE bar (r REAL)");
    ParsesOk("SELECT CAST(a as INTEGER) from tbl");
    ParsesOk("SELECT CAST(a as REAL) from tbl");
  }

  @Test public void TestGetErrorMsg() {

    // missing select
    ParserError("c, b, c from t",
        "Syntax error at:\n" +
        "c, b, c from t\n" +
        "^\n" +
        "Encountered: IDENTIFIER\n" +
        "Expected: ALTER, CREATE, DESCRIBE, DROP, SELECT, SHOW, USE, INSERT\n");

    // missing select list
    ParserError("select from t",
        "Syntax error at:\n" +
        "select from t\n" +
        "       ^\n" +
        "Encountered: FROM\n" +
        "Expected: ALL, AVG, CASE, CAST, COUNT, DISTINCT, DISTINCTPC, " +
        "DISTINCTPCSA, FALSE, IF, MIN, MAX, NOT, NULL, SUM, TRUE, INTERVAL, " +
        "IDENTIFIER\n");

    // missing from
    ParserError("select c, b, c where a = 5",
        "Syntax error at:\n" +
        "select c, b, c where a = 5\n" +
        "               ^\n" +
        "Encountered: WHERE\n" +
        "Expected: AND, AS, BETWEEN, DIV, FROM, IS, IN, LIKE, LIMIT, NOT, OR, " +
        "ORDER, REGEXP, RLIKE, UNION, COMMA, IDENTIFIER\n");

    // missing table list
    ParserError("select c, b, c from where a = 5",
        "Syntax error at:\n" +
        "select c, b, c from where a = 5\n" +
        "                    ^\n" +
        "Encountered: WHERE\n" +
        "Expected: IDENTIFIER\n");

    // missing predicate in where clause (no group by)
    ParserError("select c, b, c from t where",
        "Syntax error at:\n" +
        "select c, b, c from t where\n" +
        "                           ^\n" +
        "Encountered: EOF\n" +
        "Expected: AVG, CASE, CAST, COUNT, DISTINCTPC, DISTINCTPCSA, " +
        "FALSE, IF, MIN, MAX, NOT, NULL, SUM, TRUE, INTERVAL, IDENTIFIER\n");

    // missing predicate in where clause (group by)
    ParserError("select c, b, c from t where group by a, b",
        "Syntax error at:\n" +
        "select c, b, c from t where group by a, b\n" +
        "                            ^\n" +
        "Encountered: GROUP\n" +
        "Expected: AVG, CASE, CAST, COUNT, DISTINCTPC, DISTINCTPCSA, " +
        "FALSE, IF, MIN, MAX, NOT, NULL, SUM, TRUE, INTERVAL, IDENTIFIER\n");

    // unmatched string literal starting with "
    ParserError("select c, \"b, c from t",
        "Unmatched string literal at:\n" +
        "select c, \"b, c from t\n" +
        "           ^\n");

    // unmatched string literal starting with '
    ParserError("select c, 'b, c from t",
        "Unmatched string literal at:\n" +
        "select c, 'b, c from t\n" +
        "           ^\n");

    // numeric overflow for Long literal
    ParserError("select " + Long.toString(Long.MAX_VALUE) + "1 from t",
        "Numeric overflow at:\n" +
        "select 92233720368547758071 from t\n" +
        "       ^\n");

    // test placement of error indicator ^ on queries with multiple lines
    ParserError("select (i + 5)(1 - i) from t",
        "Syntax error at:\n" +
        "select (i + 5)(1 - i) from t\n" +
        "              ^\n" +
        "Encountered: (\n" +
        "Expected: AND, AS, ASC, BETWEEN, DESC, DIV, ELSE, END, FROM, FULL, " +
        "GROUP, HAVING, IS, IN, INNER, JOIN, LEFT, LIKE, LIMIT, NOT, OR, ORDER, " +
        "REGEXP, RLIKE, RIGHT, UNION, WHEN, WHERE, THEN, COMMA, " +
        "IDENTIFIER\n");

    ParserError("select (i + 5)\n(1 - i) from t",
        "Syntax error at:\n" +
        "select (i + 5)\n" +
        "(1 - i) from t\n" +
        "^\n" +
        "Encountered: (\n" +
        "Expected: AND, AS, ASC, BETWEEN, DESC, DIV, ELSE, END, FROM, FULL, " +
        "GROUP, HAVING, IS, IN, INNER, JOIN, LEFT, LIKE, LIMIT, NOT, OR, ORDER, " +
        "REGEXP, RLIKE, RIGHT, UNION, WHEN, WHERE, THEN, COMMA, " +
        "IDENTIFIER\n");

    ParserError("select (i + 5)\n(1 - i)\nfrom t",
        "Syntax error at:\n" +
        "select (i + 5)\n" +
        "(1 - i)\n" +
        "^\n" +
        "from t\n" +
        "Encountered: (\n" +
        "Expected: AND, AS, ASC, BETWEEN, DESC, DIV, ELSE, END, FROM, FULL, " +
        "GROUP, HAVING, IS, IN, INNER, JOIN, LEFT, LIKE, LIMIT, NOT, OR, ORDER, " +
        "REGEXP, RLIKE, RIGHT, UNION, WHEN, WHERE, THEN, COMMA, " +
        "IDENTIFIER\n");
  }
}
