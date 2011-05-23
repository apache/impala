// (c) Copyright 2011 Cloudera, Inc.

package com.cloudera.impala.parser;

import beaver.Parser;

import java.io.IOException;
import java.io.StringReader;
import java.lang.Double;
import java.lang.Long;
import java.lang.String;
import org.junit.*;
import static org.junit.Assert.*;

public class ParserTest {
  // Asserts in case of parser error.
  public void ParsesOk(String stmt) {
    // yyreset() instead?
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    Object result = null;
    try {
      result = parser.parse().value;
    } catch (java.lang.Exception e) {
      System.err.println(e.toString());
      fail("\n" + parser.getErrorMsg(stmt));
    }
    assertNotNull(result);
  }

  // Asserts if stmt parses fine or the error string doesn't match and it is non-null.
  public void ParserError(String stmt, String expectedErrorString) {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    try {
      parser.parse();
    } catch (java.lang.Exception e) {
      //System.err.println(parser.getErrorMsg(stmt));
      if (expectedErrorString != null) {
        String errorString = parser.getErrorMsg(stmt);
        assertEquals(errorString, expectedErrorString);
      }
      return;
    }
    fail("Stmt didn't result in parsing error: " + stmt);
  }

  // Asserts if stmt parses fine.
  public void ParserError(String stmt) {
    ParserError(stmt, null);
  }

  @Test public void TestStar() {
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

  @Test public void TestFromClause() {
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
    ParserError("select * from src src1 join src src2 using (1)");
    ParserError("select * from src src1 " +
        "left outer join src src2 on (src1.key = src2.key and)");
  }

  @Test public void TestWhereClause() {
    ParsesOk("select a, b, count(c) from test where a > 15");
    ParsesOk("select a, b, count(c) from test where true");
    ParserError("select a, b, count(c) where a > 15 from test");
    ParserError("select a, b, count(c) from test where 15");
  }

  @Test public void TestGroupBy() {
    ParsesOk("select a, b, count(c) from test group by 1, 2");
    ParsesOk("select a, b, count(c) from test group by a, b");
    // semantically wrong but parses fine
    ParsesOk("select a, b, count(c) from test group by 1, b");
    ParserError("select a, b, count(c) from test group 1, 2");
    ParserError("select a, b, count(c) from test group by order by a");
  }

  @Test public void TestHaving() {
    ParsesOk("select a, b, count(c) from test group by a, b having count(*) > 5");
    ParserError("select a, b, count(c) from test group by a, b having 5");
    ParserError("select a, b, count(c) from test group by a, b having order by 5");
    ParserError("select a, b, count(c) from test having count(*) > 5 group by a, b");
  }

  @Test public void TestLimit() {
    ParsesOk("select a, b, c from test inner join test2 using(a) limit 10");
    ParserError("select a, b, c from test inner join test2 using(a) limit 'a'");
    ParserError("select a, b, c from test inner join test2 using(a) limit a");
    ParserError("select a, b, c from test inner join test2 using(a) limit 10 " +
        "where a > 10");
  }

  @Test public void TestOverflow() {
    ParsesOk("select " + Long.toString(Long.MAX_VALUE) + " from test");
    ParsesOk("select " + Long.toString(Long.MIN_VALUE) + " from test");
    ParsesOk("select " + Double.toString(Double.MAX_VALUE) + " from test");
    ParsesOk("select " + Double.toString(Double.MIN_VALUE) + " from test");
  }

  @Test public void TestLiteralExprs() {
    ParsesOk("select 5, 'five', 5.0, i + 5 from t");
    // TODO: introduce UNMATCHED_STRING token and error production
    //ParserError("select '5 from t");
    ParserError("select 5.0.5 from t");
  }

  @Test public void TestFunctionCallExprs() {
    ParsesOk("select f1(5), f2('five'), f3(5.0, i + 5) from t");
    ParserError("select f( from t");
    ParserError("select f(5.0 5.0) from t");
  }

  @Test public void TestArithmeticExprs() {
    ParsesOk("select (i + 5) * (i - -5) / (a % 10) from t");
    ParsesOk("select a & b, a | b, a ^ b, ~a from t");
    ParserError("select (i + 5)(1 - i) from t");
    ParserError("select +a from t");
    ParserError("select %a from t");
    ParserError("select *a from t");
    ParserError("select /a from t");
    ParserError("select &a from t");
    ParserError("select |a from t");
    ParserError("select ^a from t");
    ParserError("select a ~ a from t");
  }

  @Test public void TestCaseExprs() {
    ParsesOk("select case a when '5' then x when '6' then y else z end from t");
    ParsesOk("select case when '5' then x when '6' then y else z end from t");
    ParserError("select case a when '5' then x when '6' then y else z from t");
    ParserError("select case a when '5' when '6' then y else z end from t");
    ParserError("select case a when '5', '6' then y else z end from t");
  }

  @Test public void TestCastExprs() {
    ParsesOk("select cast(a + 5.0 as string) from t");
    ParserError("select cast(a + 5.0 as badtype) from t");
    ParserError("select cast(a + 5.0, string) from t");
  }

  @Test public void TestAggregateExprs() {
    ParsesOk("select count(*), count(a), count(distinct a, b) from t");
    ParserError("select count() from t");
    ParsesOk("select min(a), min(distinct a) from t");
    ParserError("select min() from t");
    ParsesOk("select max(a), max(distinct a) from t");
    ParserError("select max() from t");
    ParsesOk("select sum(a), sum(distinct a) from t");
    ParserError("select sum() from t");
    ParsesOk("select avg(a), avg(distinct a) from t");
    ParserError("select avg() from t");
  }

  @Test public void TestPredicates() {
    ParsesOk("select a, b, c from t where i = 5");
    ParsesOk("select a, b, c from t where i != 5");
    ParsesOk("select a, b, c from t where i <> 5");
    ParsesOk("select a, b, c from t where i > 5");
    ParsesOk("select a, b, c from t where i >= 5");
    ParsesOk("select a, b, c from t where i < 5");
    ParsesOk("select a, b, c from t where i <= 5");
    ParsesOk("select a, b, c from t where i like 'abc%'");
    ParsesOk("select a, b, c from t where i rlike 'abc.*'");
    ParsesOk("select a, b, c from t where i regexp 'abc.*'");
    ParsesOk("select a, b, c from t where a = 5 and b = 6");
    ParsesOk("select a, b, c from t where a = 5 or b = 6");
    ParsesOk("select a, b, c from t where (a = 5 or b = 6) and c = 7");
    ParsesOk("select a, b, c from t where not a = 5");
    ParsesOk("select a, b, c from t where (not a = 5 or not b = 6) and not c = 7");
    ParsesOk("select a, b, c from t where i is null");
    ParsesOk("select a, b, c from t where i is not null");
    ParsesOk("select a, b, c from t where i + 5 is not null");
    ParsesOk("select a, b, c from t where true");
    ParsesOk("select a, b, c from t where false");
    ParsesOk("select a, b, c from t where false and true");
  }

  @Test public void TestGetErrorMsg() {
    ParserError("select (i + 5)(1 - i) from t",
        "Syntax error at:\n" +
        "select (i + 5)(1 - i) from t\n" +
        "              ^\n");
    ParserError("select (i + 5)\n(1 - i) from t",
        "Syntax error at:\n" +
        "select (i + 5)\n" +
        "(1 - i) from t\n" +
        "^\n");
    ParserError("select (i + 5)\n(1 - i)\nfrom t",
        "Syntax error at:\n" +
        "select (i + 5)\n" +
        "(1 - i)\n" +
        "^\n" +
        "from t\n");
  }

}
