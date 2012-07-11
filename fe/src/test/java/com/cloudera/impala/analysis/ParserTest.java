// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.StringReader;

import org.junit.Test;

public class ParserTest {
  /**
   * Asserts in case of parser error.
   * @param stmt
   */
  public void ParsesOk(String stmt) {
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
   * Asserts if stmt parses fine.
   * @param stmt
   */
  public void ParserError(String stmt) {
    ParserError(stmt, null);
  }

  @Test public void TestNoFromClause() {
    ParsesOk("select 1 + 1, 'two', f(3), a + b");
    ParserError("select 1 + 1 'two' f(3) a + b");
    ParserError("select a, 2 where a > 2");
  }

  @Test public void TestSelect() {
    ParsesOk("select a from tbl");
    ParsesOk("select a, b, c, d from tbl");
    ParserError("a from tbl");
    ParserError("select a b c from tbl");
  }

  @Test public void TestAlias() {
    ParsesOk("select a b from tbl");
    ParsesOk("select a b, c from tbl");
    ParsesOk("select a as a, b as b, c as c, d as d from tbl");
    ParserError("a from tbl");
    ParserError("select a as a, b c d from tbl");
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
    ParserError("select * from src src1 join src src2 on ('a')");
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

  @Test public void TestOrderBy() {
    ParsesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
             "order by string_col, 15.7 * float_col, int_col + bigint_col");
    ParsesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
             "order by string_col asc, 15.7 * float_col desc, int_col + bigint_col asc");
    ParserError("select int_col, string_col, bigint_col, count(*) from alltypes " +
                "order by by string_col asc desc");
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
    ParserError("select a, b, c from test inner join test2 using(a) limit 10 + 10");
    ParserError("select a, b, c from test inner join test2 using(a) limit 10 " +
        "where a > 10");
  }

  @Test public void TestUnion() {
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

  @Test public void TestOverflow() {
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

  @Test public void TestLiteralPredicates() {
    // NULL literal predicate.
    ParsesOk("select a from t where NULL OR NULL");
    ParsesOk("select a from t where NULL AND NULL");
    // NULL in select list currently becomes a literal predicate.
    ParsesOk("select NULL from t");
    // bool literal predicate
    ParsesOk("select a from t where true");
    ParsesOk("select a from t where false");
    ParsesOk("select a from t where true OR true");
    ParsesOk("select a from t where true OR false");
    ParsesOk("select a from t where false OR false");
    ParsesOk("select a from t where false OR true");
    ParsesOk("select a from t where true AND true");
    ParsesOk("select a from t where true AND false");
    ParsesOk("select a from t where false AND false");
    ParsesOk("select a from t where false AND true");
  }

  @Test public void TestLiteralExprs() {
    // negative integer literal
    ParsesOk("select -1 from t");
    ParsesOk("select - 1 from t");
    ParsesOk("select a - - 1 from t");
    ParsesOk("select a - - - 1 from t");
    // NULL literal in binary predicate.
    for (BinaryPredicate.Operator op : BinaryPredicate.Operator.values()) {
      ParsesOk("select a from t where a " +  op.toString() + " NULL");
    }
    // bool literal in binary predicate.
    for (BinaryPredicate.Operator op : BinaryPredicate.Operator.values()) {
      ParsesOk("select a from t where a " +  op.toString() + " true");
      ParsesOk("select a from t where a " +  op.toString() + " false");
    }
    // test string literals with and without quotes in the literal
    ParsesOk("select 5, 'five', 5.0, i + 5 from t");
    ParsesOk("select \"\\\"five\\\"\" from t\n");
    ParsesOk("select \"\'five\'\" from t\n");
    ParsesOk("select \"\'five\" from t\n");
    // missing quotes
    ParserError("select \'5 from t");
    ParserError("select \"5 from t");
    ParserError("select '5 from t");
    ParserError("select \"\"five\"\" from t\n");
    ParserError("select 5.0.5 from t");
    // NULL literal in arithmetic expr
    for (ArithmeticExpr.Operator op : ArithmeticExpr.Operator.values()) {
      ParserError("select a from t where a " +  op.toString() + " NULL");
    }
    // bool literal in arithmetic expr
    for (ArithmeticExpr.Operator op : ArithmeticExpr.Operator.values()) {
      ParserError("select a from t where a " +  op.toString() + " true");
      ParserError("select a from t where a " +  op.toString() + " false");
    }
  }

  @Test public void TestFunctionCallExprs() {
    ParsesOk("select f1(5), f2('five'), f3(5.0, i + 5) from t");
    ParsesOk("select f1(true), f2(true and false), f3(null) from t");
    ParserError("select f( from t");
    ParserError("select f(5.0 5.0) from t");
  }

  @Test public void TestArithmeticExprs() {
    ParsesOk("select (i + 5) * (i - -5) / (a % 10) from t");
    ParsesOk("select a & b, a | b, a ^ b, ~a from t");
    ParsesOk("select 15.7 * f from t");
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
    // Test regular exps.
    ParsesOk("select case a when '5' then x when '6' then y else z end from t");
    ParsesOk("select case when 'a' then x when false then y else z end from t");
    // Test predicates in case, when, then, and else exprs.
    ParsesOk("select case when a > 2 then x when false then false else true end from t");
    ParsesOk("select case false when a > 2 then x when '6' then false else true end " +
        "from t");
    // Missing end.
    ParserError("select case a when true then x when false then y else z from t");
    // Missing else after first when.
    ParserError("select case a when true when false then y else z end from t");
    // Incorrectly placed comma.
    ParserError("select case a when true, false then y else z end from t");
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
    ParsesOk("select distinct a, b, c from t");
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
    ParsesOk("select a, b, c from t where i is null");
    ParsesOk("select a, b, c from t where i is not null");
    ParsesOk("select a, b, c from t where i + 5 is not null");
    ParsesOk("select a, b, c from t where true");
    ParsesOk("select a, b, c from t where false");
    ParsesOk("select a, b, c from t where false and true");
  }

  private void testCompoundPredicates(String andStr, String orStr, String notStr) {
    // select a, b, c from t where a = 5 and b = 6
    ParsesOk("select a, b, c from t where a = 5 " + andStr + " b = 6");
    // select a, b, c from t where a = 5 or b = 6
    ParsesOk("select a, b, c from t where a = 5 " + orStr + " b = 6");
    // select a, b, c from t where (a = 5 or b = 6) and c = 7
    ParsesOk("select a, b, c from t where (a = 5 " + orStr + " b = 6) " +
        andStr + " c = 7");
    // select a, b, c from t where not a = 5
    ParsesOk("select a, b, c from t where " + notStr + "a = 5");
    // select a, b, c from t where (not a = 5 or not b = 6) and not c = 7
    ParsesOk("select a, b, c from t where (" + notStr + "a = 5 " + orStr + " " +
        notStr + "b = 6) " + andStr + " " + notStr + "c = 7");
    // select a, b, c from t where (!(!a = 5))
    ParsesOk("select a, b, c from t where (" + notStr + "(" + notStr + "a = 5))");
    // unbalanced parentheses
    ParserError("select a, b, c from t where (a = 5 " + orStr + " b = 6) " + andStr + " c = 7)");
    ParserError("select a, b, c from t where ((a = 5 " + orStr + " b = 6) " + andStr + " c = 7");
    // incorrectly positioned negation (!)
    ParserError("select a, b, c from t where a = " + notStr + "5");
    ParserError("select a, b, c from t where a = 5 " + orStr + " " + notStr);
    ParserError("select a, b, c from t where " + notStr + "(a = 5) " + orStr + " " + notStr);
  }

  @Test public void TestCompoundPredicates() {
    String[] andStrs = { "and", "&&" };
    String[] orStrs = { "or", "||" };
    // Note the trailing space in "not ". We want to test "!" without a space.
    String[] notStrs = { "!", "not " };
    // Test all combinations of representations for 'or', 'and', and 'not'.
    for (String andStr : andStrs) {
      for (String orStr : orStrs) {
        for (String notStr : notStrs) {
          testCompoundPredicates(andStr, orStr, notStr);
        }
      }
    }
  }

  @Test public void TestBetweenPredicate() {
    ParsesOk("select a, b, c from t where i between x and y");
    ParsesOk("select a, b, c from t where i not between x and y");
    // Missing condition expr.
    ParserError("select a, b, c from t where between 5 and 10");
    // Missing lower bound.
    ParserError("select a, b, c from t where i between and 10");
    // Missing upper bound.
    ParserError("select a, b, c from t where i between 5 and");
    // Missing exprs after between.
    ParserError("select a, b, c from t where i between");
  }

  @Test public void TestInPredicate() {
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

  @Test public void TestSlotRef() {
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
  private void testInsert(boolean overwrite) {
    String qualifier = null;
    if (overwrite) {
      qualifier = "overwrite";
    } else {
      qualifier = "into";
    }
    // Entire unpartitioned table.
    ParsesOk("insert " + qualifier + " table t select a from src where b > 5");
    // Static partition with one partitioning key.
    ParsesOk("insert " + qualifier + " table t partition (pk1=10) select a from src where b > 5");
    // Dynamic partition with one partitioning key.
    ParsesOk("insert " + qualifier + " table t partition (pk1) select a from src where b > 5");
    // Static partition with two partitioning keys.
    ParsesOk("insert " + qualifier + " table t partition (pk1=10, pk2=20) " +
        "select a from src where b > 5");
    // Fully dynamic partition with two partitioning keys.
    ParsesOk("insert " + qualifier + " table t partition (pk1, pk2) " +
        "select a from src where b > 5");
    // Partially dynamic partition with two partitioning keys.
    ParsesOk("insert " + qualifier + " table t partition (pk1=10, pk2) " +
        "select a from src where b > 5");
    // Partially dynamic partition with two partitioning keys.
    ParsesOk("insert " + qualifier + " table t partition (pk1, pk2=20) " +
        "select a from src where b > 5");
    // Static partition with two NULL partitioning keys.
    ParsesOk("insert " + qualifier + " table t partition (pk1=NULL, pk2=NULL) " +
        "select a from src where b > 5");
  }

  @Test public void TestInsert() {
    // Positive tests.
    testInsert(true);
    testInsert(false);
    // Missing 'overwrite/insert'.
    ParserError("insert table t select a from src where b > 5");
    // Missing 'table'.
    ParserError("insert overwrite t select a from src where b > 5");
    // Missing 'table'.
    ParserError("insert into t select a from src where b > 5");
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

  @Test public void TestGetErrorMsg() {

    // missing select
    ParserError("c, b, c from t",
        "Syntax error at:\n" +
        "c, b, c from t\n" +
        "^\n" +
        "Encountered: IDENTIFIER\n" +
        "Expected: SELECT, INSERT\n");

    // missing select list
    ParserError("select from t",
        "Syntax error at:\n" +
        "select from t\n" +
        "       ^\n" +
        "Encountered: FROM\n" +
        "Expected: AVG, CASE, CAST, COUNT, DISTINCT, FALSE, " +
        "MIN, MAX, NOT, NULL, SUM, TRUE, IDENTIFIER\n");

    // missing from
    ParserError("select c, b, c where a = 5",
        "Syntax error at:\n" +
        "select c, b, c where a = 5\n" +
        "               ^\n" +
        "Encountered: WHERE\n" +
        "Expected: AS, BETWEEN, DIV, FROM, IS, IN, LIKE, LIMIT, NOT, ORDER, " +
        "REGEXP, RLIKE, UNION, COMMA, IDENTIFIER\n");

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
        "Expected: AVG, CASE, CAST, COUNT, FALSE, MIN, MAX, NOT, NULL, SUM, " +
        "TRUE, IDENTIFIER\n");

    // missing predicate in where clause (group by)
    ParserError("select c, b, c from t where group by a, b",
        "Syntax error at:\n" +
        "select c, b, c from t where group by a, b\n" +
        "                            ^\n" +
        "Encountered: GROUP\n" +
        "Expected: AVG, CASE, CAST, COUNT, FALSE, MIN, MAX, NOT, NULL, SUM, " +
        "TRUE, IDENTIFIER\n");

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
