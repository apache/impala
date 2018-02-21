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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.TimestampArithmeticExpr.TimeUnit;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.compat.MetastoreShim;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ParserTest extends FrontendTestBase {

  // Representative operands for testing.
  private final static String[] operands_ =
      new String[] {"i", "5", "true", "NULL", "'a'", "(1.5 * 8)" };

  /**
   * Attempts to parse the given select statement, and asserts in case of parser error.
   * Also asserts that the first select-list expression is of given class.
   */
  public <C extends Expr> Object ParsesOk(String selectStmtSql, Class<C> cl) {
    Object parseNode = ParsesOk(selectStmtSql);
    if (!(parseNode instanceof SelectStmt)) {
      fail(String.format("Statement parsed ok but it is not a select stmt: %s",
          selectStmtSql));
    }
    SelectStmt selectStmt = (SelectStmt) parseNode;
    Expr firstExpr = selectStmt.getSelectList().getItems().get(0).getExpr();
    // Check the class of the first select-list expression.
    assertTrue(String.format(
        "Expression is of class '%s'. Expected class '%s'",
          firstExpr.getClass().getSimpleName(), cl.getSimpleName()),
        firstExpr.getClass().equals(cl));
    return parseNode;
  }

  /**
   * Asserts if stmt parses fine or the error string doesn't match and it is non-null.
   */
  public void ParserError(String stmt, String expectedErrorString) {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    Object result = null; // Save this object to make debugging easier
    try {
      result = parser.parse().value;
    } catch (Exception e) {
      if (expectedErrorString != null) {
        String errorString = parser.getErrorMsg(stmt);
        StringBuilder message = new StringBuilder();
        message.append("Got: ");
        message.append(errorString).append("\nExpected: ").append(expectedErrorString);
        assertTrue(message.toString(), errorString.startsWith(expectedErrorString));
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

  /**
   * Tests table and column aliases which can specified as
   * identifiers (quoted or unquoted) or string literals (single or double quoted).
   */
  @Test
  public void TestAlias() {
    char[] quotes = {'\'', '"', '`', ' '};
    for (int i = 0; i < quotes.length; ++i) {
      char quote = quotes[i];
      // Column aliases.
      ParsesOk("select a 'b' from tbl".replace('\'', quote));
      ParsesOk("select a as 'b' from tbl".replace('\'', quote));
      ParsesOk("select a 'x', b as 'y', c 'z' from tbl".replace('\'', quote));
      ParsesOk(
          "select a 'x', b as 'y', sum(x) over () 'z' from tbl".replace('\'', quote));
      ParsesOk("select a.b 'x' from tbl".replace('\'', quote));
      ParsesOk("select a.b as 'x' from tbl".replace('\'', quote));
      ParsesOk("select a.b.c.d 'x' from tbl".replace('\'', quote));
      ParsesOk("select a.b.c.d as 'x' from tbl".replace('\'', quote));
      // Table aliases.
      ParsesOk("select a from tbl 'b'".replace('\'', quote));
      ParsesOk("select a from tbl as 'b'".replace('\'', quote));
      ParsesOk("select a from db.tbl 'b'".replace('\'', quote));
      ParsesOk("select a from db.tbl as 'b'".replace('\'', quote));
      ParsesOk("select a from db.tbl.col 'b'".replace('\'', quote));
      ParsesOk("select a from db.tbl.col as 'b'".replace('\'', quote));
      ParsesOk("select a from (select * from tbl) 'b'".replace('\'', quote));
      ParsesOk("select a from (select * from tbl) as 'b'".replace('\'', quote));
      ParsesOk("select a from (select * from tbl b) as 'b'".replace('\'', quote));
      // With-clause view aliases.
      ParsesOk("with 't' as (select 1) select * from t".replace('\'', quote));
    }
    ParserError("a from tbl");
    ParserError("select a as a, b c d from tbl");
  }

  @Test
  public void TestStar() {
    ParsesOk("select * from tbl");
    ParsesOk("select tbl.* from tbl");
    ParsesOk("select db.tbl.* from tbl");
    ParsesOk("select db.tbl.struct_col.* from tbl");
    ParserError("select * + 5 from tbl");
    ParserError("select (*) from tbl");
    ParserError("select *.id from tbl");
    ParserError("select * from tbl.*");
    ParserError("select * from tbl where * = 5");
    ParsesOk("select * from tbl where f(*) = 5");
    ParserError("select * from tbl where tbl.* = 5");
    ParserError("select * from tbl where f(tbl.*) = 5");
  }

  /**
   * Various test cases for (multiline) C-style comments.
   */
  @Test
  public void TestMultilineComment() {
    ParserError("/**/");
    ParserError("/*****/");
    ParserError("/* select 1 */");
    ParserError("/*/ select 1");
    ParserError("select 1 /*/");
    ParsesOk("/**/select 1");
    ParsesOk("select/* */1");
    ParsesOk("/** */ select 1");
    ParsesOk("select 1/* **/");
    ParsesOk("/*/*/select 1");
    ParsesOk("/*//*/select 1");
    ParsesOk("select 1/***/");
    ParsesOk("/*****/select 1");
    ParsesOk("/**//**/select 1");
    ParserError("/**/**/select 1");
    ParsesOk("\nselect 1/**/");
    ParsesOk("/*\n*/select 1");
    ParsesOk("/*\r*/select 1");
    ParsesOk("/*\r\n*/select 1");
    ParsesOk("/**\n* Doc style\n*/select 1");
    ParsesOk("/************\n*\n* Header style\n*\n***********/select 1");
    ParsesOk("/* 1 */ select 1 /* 2 */");
    ParsesOk("select\n/**/\n1");
    ParserError("/**// select 1");
    ParserError("/**/*/ select 1");
    ParserError("/ **/ select 1");
    ParserError("/** / select 1");
    ParserError("/\n**/ select 1");
    ParserError("/**\n/ select 1");
    ParsesOk("/*--*/ select 1");
    ParsesOk("/* --foo */ select 1");
    ParsesOk("/*\n--foo */ select 1");
    ParsesOk("/*\n--foo\n*/ select 1");
    ParserError("select 1 /* --bar");
    ParserError("select 1 /*--");
    ParsesOk("/* select 1; */ select 1");
    ParsesOk("/** select 1; */ select 1");
    ParsesOk("/* select */ select 1 /* 1 */");
    // Test hint with arguments
    ParsesOk("select 1 /* hint_with_args(() */");
    // Empty argument list in hint_with_args hint is not allowed
    ParserError("select 1 /*+ hint_with_args() */");
    // Mismatching parentheses
    ParserError("select 1 /*+ hint_with_args(() */");
    ParserError("select 1 /*+ hint_with_args(a) \n");
    ParserError("select 1 --+ hint_with_args(a) */\n from t");
  }

  /**
   * Various test cases for one line comment style (starts with --).
   */
  @Test
  public void TestSinglelineComment() {
    ParserError("--");
    ParserError("--select 1");
    ParsesOk("select 1--");
    ParsesOk("select 1 --foo");
    ParsesOk("select 1 --\ncol_name");
    ParsesOk("--foo's \nselect 1 --bar");
    ParsesOk("--foo\nselect 1 --bar");
    ParsesOk("--foo\r\nselect 1 --bar");
    ParsesOk("--/* foo */\n select 1");
    ParsesOk("select 1 --/**/");
    ParsesOk("-- foo /*\nselect 1");
    ParserError("-- baz /*\nselect 1*/");
    ParsesOk("select -- blah\n 1");
    ParsesOk("select -- select 1\n 1");
    // Test hint with arguments
    ParsesOk("select 1 -- hint_with_args(()");
    // Mismatching parentheses
    ParserError("select 1 -- +hint_with_args(()\n");
  }

  /**
   * Parses stmt and checks that the all table refs in stmt have the expected join hints.
   * The expectedHints contains the hints of all table refs from left to right (starting
   * with the second tableRef because the first one cannot have hints).
   */
  private void TestJoinHints(String stmt, String... expectedHints) {
    SelectStmt selectStmt = (SelectStmt) ParsesOk(stmt);
    Preconditions.checkState(selectStmt.getTableRefs().size() > 1);
    List<String> actualHints = Lists.newArrayList();
    assertTrue(selectStmt.getTableRefs().get(0).getJoinHints().isEmpty());
    for (int i = 1; i < selectStmt.getTableRefs().size(); ++i) {
      List<PlanHint> hints = selectStmt.getTableRefs().get(i).getJoinHints();
      for (PlanHint hint: hints) actualHints.add(hint.toString());
    }
    if (actualHints.isEmpty()) actualHints = Lists.newArrayList((String) null);
    assertEquals(Lists.newArrayList(expectedHints), actualHints);
  }

  private void TestTableHints(String stmt, String... expectedHints) {
    SelectStmt selectStmt = (SelectStmt) ParsesOk(stmt);
    Preconditions.checkState(selectStmt.getTableRefs().size() > 0);
    List<String> actualHints = Lists.newArrayList();
    for (int i = 0; i < selectStmt.getTableRefs().size(); ++i) {
      List<PlanHint> hints = selectStmt.getTableRefs().get(i).getTableHints();
      for (PlanHint hint: hints) actualHints.add(hint.toString());
    }
    if (actualHints.isEmpty()) actualHints = Lists.newArrayList((String) null);
    assertEquals(Lists.newArrayList(expectedHints), actualHints);
  }

  private void TestTableAndJoinHints(String stmt, String... expectedHints) {
    SelectStmt selectStmt = (SelectStmt) ParsesOk(stmt);
    Preconditions.checkState(selectStmt.getTableRefs().size() > 0);
    List<String> actualHints = Lists.newArrayList();
    for (int i = 0; i < selectStmt.getTableRefs().size(); ++i) {
      List<PlanHint> joinHints = selectStmt.getTableRefs().get(i).getJoinHints();
      for (PlanHint hint: joinHints) actualHints.add(hint.toString());
      List<PlanHint> tableHints = selectStmt.getTableRefs().get(i).getTableHints();
      for (PlanHint hint: tableHints) actualHints.add(hint.toString());
    }
    if (actualHints.isEmpty()) actualHints = Lists.newArrayList((String) null);
    assertEquals(Lists.newArrayList(expectedHints), actualHints);
  }

  /**
   * Parses stmt and checks that the select-list plan hints in stmt are the
   * expected hints.
   */
  private void TestSelectListHints(String stmt, String... expectedHints) {
    SelectStmt selectStmt = (SelectStmt) ParsesOk(stmt);
    List<String> actualHints = Lists.newArrayList();
    List<PlanHint> hints = selectStmt.getSelectList().getPlanHints();
    for (PlanHint hint: hints) actualHints.add(hint.toString());
    if (actualHints.isEmpty()) actualHints = Lists.newArrayList((String) null);
    assertEquals(Lists.newArrayList(expectedHints), actualHints);
  }

  /**
   * Creates an insert into, an insert overwrite, and a CTAS statement with
   * the given hints and checks that the parsed hints are the same as the expected hints.
   */
  private void TestInsertAndCtasHints(String insertPart, String ctasPart,
        String[] hintStyle, String hints, String... expectedHints) {
    String hintsPart = hintStyle[0] + hints + hintStyle[1];
    TestInsertStmtHints(String.format("insert %%s into %s %%s select * from t",
        insertPart), hintsPart, expectedHints);
    TestInsertStmtHints(String.format("insert %%s overwrite %s %%s select * from t",
        insertPart), hintsPart, expectedHints);
    TestCtasHints(String.format("create %s table %s as select * from t",
        hintsPart, ctasPart), expectedHints);
  }

  /**
   * Injects hints into pattern and checks that the injected hints match the expected
   * hints. This function covers both insert and upsert statements.
   */
  private void TestInsertStmtHints(String pattern, String hint, String... expectedHints) {
    for (InsertStmt.HintLocation loc: InsertStmt.HintLocation.values()) {
      InsertStmt insertStmt = (InsertStmt) ParsesOk(InjectInsertHint(pattern, hint, loc));
      assertEquals(expectedHints, HintsToStrings(insertStmt.getPlanHints()));
    }
  }

  /**
   * Injects hints into pattern and expect parser error on the injected hints.
   * It covers both insert and upsert statements.
   */
  private void ParserErrorOnInsertStmtHints(String pattern, String hint) {
    for (InsertStmt.HintLocation loc: InsertStmt.HintLocation.values()) {
      ParserError(InjectInsertHint(pattern, hint, loc));
    }
  }

  /**
   * Parses stmt and checks that the CTAS hints stmt are the expected hints.
   */
  private void TestCtasHints(String stmt, String... expectedHints) {
    CreateTableAsSelectStmt ctasStmt = (CreateTableAsSelectStmt) ParsesOk(stmt);
    assertEquals(expectedHints, HintsToStrings(ctasStmt.getInsertStmt().getPlanHints()));
  }

  static private String[] HintsToStrings(List<PlanHint> hints) {
    if (hints.isEmpty()) return new String[] { null };
    String[] hintAsStrings = new String[hints.size()];
    for (int i = 0; i < hints.size(); ++i) hintAsStrings[i] = hints.get(i).toString();
    return hintAsStrings;
  }

  @Test
  public void TestPlanHints() {
    String[][] commentStyles = new String[][] {
        new String[] { "/*", "*/" }, // traditional comment
        new String[] { "--", "\n" } // eol comment
    };
    for (String[] hintStyle: hintStyles_) {
      String prefix = hintStyle[0];
      String suffix = hintStyle[1];
      // Test join hints.
      TestJoinHints(String.format(
          "select * from functional.alltypes a join %sbroadcast%s " +
              "functional.alltypes b", prefix, suffix), "broadcast");
      TestJoinHints(String.format(
          "select * from functional.alltypes a join %sbroadcast%s " +
              "functional.alltypes b using(id)", prefix, suffix), "broadcast");
      TestJoinHints(String.format(
          "select * from functional.alltypes a join %sbroadcast%s " +
              "functional.alltypes b on(a.id = b.id)", prefix, suffix), "broadcast");
      TestJoinHints(String.format(
          "select * from functional.alltypes a cross join %sbroadcast%s " +
              "functional.alltypes b", prefix, suffix), "broadcast");
      // Multiple comma-separated hints.
      TestJoinHints(String.format(
          "select * from functional.alltypes a join " +
              "%sbroadcast,shuffle,foo,bar%s " +
              "functional.alltypes b using(id)", prefix, suffix),
              "broadcast", "shuffle", "foo", "bar");
      // Test hints in a multi-way join.
      TestJoinHints(String.format(
          "select * from functional.alltypes a " +
              "join %sbroadcast%s functional.alltypes b using(id) " +
              "join %sshuffle%s functional.alltypes c using(int_col) " +
              "join %sbroadcast%s functional.alltypes d using(int_col) " +
              "join %sshuffle%s functional.alltypes e using(string_col)",
              prefix, suffix, prefix, suffix, prefix, suffix, prefix, suffix),
              "broadcast", "shuffle", "broadcast", "shuffle");
      // Test hints in a multi-way join (flipped prefix/suffix -> bad hint start/ends).
      ParserError(String.format(
          "select * from functional.alltypes a " +
              "join %sbroadcast%s functional.alltypes b using(id) " +
              "join %sshuffle%s functional.alltypes c using(int_col) " +
              "join %sbroadcast%s functional.alltypes d using(int_col) " +
              "join %sshuffle%s functional.alltypes e using(string_col)",
              prefix, suffix, suffix, prefix, prefix, suffix, suffix, prefix));
      // Test hints in a multi-way join (missing prefixes/suffixes).
      ParserError(String.format(
          "select * from functional.alltypes a " +
              "join %sbroadcast%s functional.alltypes b using(id) " +
              "join %sshuffle%s functional.alltypes c using(int_col) " +
              "join %sbroadcast%s functional.alltypes d using(int_col) " +
              "join %sshuffle%s functional.alltypes e using(string_col)",
              suffix, suffix, suffix, suffix, prefix, "", "", ""));

      // Test insert/CTAS hints.
      TestInsertAndCtasHints("t", "t", hintStyle, "noshuffle", "noshuffle");
      TestInsertAndCtasHints("t partition(x, y)", "t partitioned by(x, y)",
          hintStyle, "noshuffle", "noshuffle");
      TestInsertAndCtasHints("t(a, b) partition(x, y)", "t partitioned by(x, y)",
          hintStyle, "shuffle", "shuffle");
      TestInsertAndCtasHints("t(a, b) partition(x, y)", "t partitioned by(x, y)",
          hintStyle, "foo,bar,baz", "foo", "bar", "baz");

      // Test upsert hints.
      TestInsertStmtHints("upsert %s into t %s select * from t",
           String.format("%sshuffle%s", prefix, suffix), "shuffle");
      TestInsertStmtHints("upsert %s into t (x, y) %s select * from t",
           String.format("%sshuffle%s", prefix, suffix), "shuffle");

      // Test TableRef hints.
      TestTableHints(String.format(
          "select * from functional.alltypes %sschedule_disk_local%s", prefix, suffix),
          "schedule_disk_local");
      TestTableHints(String.format(
          "select * from functional.alltypes %sschedule_cache_local," +
          "schedule_random_replica%s", prefix, suffix), "schedule_cache_local",
          "schedule_random_replica");
      TestTableHints(String.format(
          "select * from functional.alltypes a %sschedule_cache_local," +
          "schedule_random_replica%s", prefix, suffix), "schedule_cache_local",
          "schedule_random_replica");
      TestTableHints(String.format(
          "select * from functional.alltypes a %sschedule_cache_local," +
          "schedule_random_replica%s" + ", functional.alltypes b %sschedule_remote%s",
          prefix, suffix, prefix, suffix), "schedule_cache_local",
          "schedule_random_replica", "schedule_remote");

      // Test both TableRef and join hints.
      TestTableAndJoinHints(String.format(
          "select * from functional.alltypes a %sschedule_cache_local," +
          "schedule_random_replica%s join %sbroadcast%s functional.alltypes b " +
          "%sschedule_remote%s using(id)", prefix, suffix, prefix, suffix, prefix,
          suffix), "schedule_cache_local", "schedule_random_replica", "broadcast",
          "schedule_remote");

      TestSelectListHints(String.format(
          "select %sfoo,bar,baz%s * from functional.alltypes a", prefix, suffix),
          "foo", "bar", "baz");

      // Test select-list hints (e.g., straight_join). The legacy-style hint has no
      // prefix and suffix.
      {
        String localPrefix = prefix;
        String localSuffix = suffix;
        if (prefix == "[") {
          localPrefix = "";
          localSuffix = "";
        }
        TestSelectListHints(String.format(
            "select %sstraight_join%s * from functional.alltypes a", localPrefix,
            localSuffix), "straight_join");
      }

      // Below are tests for hints that are not supported by the legacy syntax.
      if (prefix == "[") continue;

      // Test mixing commented hints and comments.
      for (String[] commentStyle: commentStyles) {
        String commentPrefix = commentStyle[0];
        String commentSuffix = commentStyle[1];
        String queryTemplate =
            "$1comment$2 select $1comment$2 $3straight_join$4 $1comment$2 * " +
            "from $1comment$2 functional.alltypes a join $1comment$2 $3shuffle$4 " +
            "$1comment$2 functional.alltypes b $1comment$2 on $1comment$2 " +
            "(a.id = b.id)";
        String query = queryTemplate.replaceAll("\\$1", commentPrefix)
            .replaceAll("\\$2", commentSuffix).replaceAll("\\$3", prefix)
            .replaceAll("\\$4", suffix);
        TestSelectListHints(query, "straight_join");
        TestJoinHints(query, "shuffle");
      }

      // Tests for hints with arguments.
      TestInsertStmtHints("insert %s into t %s select * from t",
           String.format("%shint_with_args(a)%s", prefix, suffix), "hint_with_args(a)");
      TestInsertStmtHints("insert %s into t %s select * from t",
           String.format("%sclustered,shuffle,hint_with_args(a)%s", prefix, suffix),
           "clustered", "shuffle", "hint_with_args(a)");
      TestInsertStmtHints("insert %s into t %s select * from t",
           String.format("%shint_with_args(a,b)%s", prefix, suffix),
           "hint_with_args(a,b)");
      TestInsertStmtHints("insert %s into t %s select * from t",
           String.format("%shint_with_args(a  , b)%s", prefix, suffix),
           "hint_with_args(a,b)");
      TestInsertStmtHints("insert %s into t %s select * from t",
           String.format("%shint_with_args(  a  , b , c  , d, e, f    )%s", prefix,
             suffix), "hint_with_args(a,b,c,d,e,f)");
      ParserErrorOnInsertStmtHints("insert %s into t %s select * from t",
           String.format("%shint_with_args(  a  ,  , ,,, b  )%s", prefix, suffix));

      TestInsertAndCtasHints("t", "t",
          hintStyle, "hint_with_args(a)", "hint_with_args(a)");
      TestInsertAndCtasHints("t", "t",
          hintStyle, "clustered,shuffle,hint_with_args(a)",
          "clustered", "shuffle", "hint_with_args(a)");
      TestInsertAndCtasHints("t", "t",
          hintStyle, "hint_with_args(a,b)", "hint_with_args(a,b)");
      TestInsertAndCtasHints("t", "t",
          hintStyle, "hint_with_args(a  , b)", "hint_with_args(a,b)");

      ParserErrorOnInsertStmtHints("insert %s into t %s select * from t",
          String.format("%shint_with_args(  a  ,  , ,,, b  )%s", prefix, suffix));
      ParserError(String.format(
         "create table t %shint_with_args(  a  ,  , ,,, b  )%s as select * from t",
         prefix, suffix));

      // Negative tests for hints cannot be specified at the both available locations.
      ParserError(String.format("insert %s into t %s select * from t",
           String.format("%sshuffle%s", prefix, suffix),
           String.format("%sclustered%s", prefix, suffix)));
      ParserError(String.format("upsert %s into t %s select * from t",
           String.format("%sshuffle%s", prefix, suffix),
           String.format("%sclustered%s", prefix, suffix)));
    }
    // No "+" at the beginning so the comment is not recognized as a hint.
    TestJoinHints("select * from functional.alltypes a join /* comment */" +
        "functional.alltypes b using (int_col)", (String) null);
    TestSelectListHints("select /* comment */ * from functional.alltypes",
        (String) null);
    TestInsertStmtHints("insert %s into t(a, b) partition(x, y) %s select 1",
        "/* comment */", (String) null);
    TestCtasHints("create /* comment */ table t partitioned by (x, y) as select 1",
        (String) null);
    TestSelectListHints("select /* -- +straight_join */ * from functional.alltypes",
        (String) null);
    TestSelectListHints("select /* abcdef +straight_join */ * from functional.alltypes",
        (String) null);
    TestSelectListHints("select \n-- abcdef +straight_join\n * from functional.alltypes",
        (String) null);
    TestSelectListHints("select \n-- /*+straight_join\n * from functional.alltypes",
        (String) null);

    // Commented hints cannot span lines (recognized as comments instead).
    TestSelectListHints("select /*\n +straight_join */ * from functional.alltypes",
        (String) null);
    TestSelectListHints("select /* +straight_join \n*/ * from functional.alltypes",
        (String) null);
    TestSelectListHints("select /* +straight_\njoin */ * from functional.alltypes",
        (String) null);
    ParserError("select -- +straight_join * from functional.alltypes");
    ParserError("select \n-- +straight_join * from functional.alltypes");

    // Missing "/*" or "/*"
    ParserError("select * from functional.alltypes a join + */" +
        "functional.alltypes b using (int_col)");
    ParserError("select * from functional.alltypes a join /* + " +
        "functional.alltypes b using (int_col)");

    // Test empty hint tokens.
    TestSelectListHints("select /* +straight_join, ,, */ * from functional.alltypes",
        "straight_join");
    // Traditional commented hints are not parsed inside a comment.
    ParserError("select /* /* +straight_join */ */ * from functional.alltypes");
  }

  @Test
  public void TestFromClause() {
    String tblRefs[] = new String[] { "tbl", "db.tbl", "db.tbl.col", "db.tbl.col.fld" };
    for (String tbl: tblRefs) {
      ParsesOk(
          ("select * from $TBL src1 " +
           "left outer join $TBL src2 on " +
           "  src1.key = src2.key and src1.key < 10 and src2.key > 10 " +
           "right outer join $TBL src3 on " +
           "  src2.key = src3.key and src3.key < 10 " +
           "full outer join $TBL src3 on " +
           "  src2.key = src3.key and src3.key < 10 " +
           "left semi join $TBL src3 on " +
           "  src2.key = src3.key and src3.key < 10 " +
           "left anti join $TBL src3 on " +
           "  src2.key = src3.key and src3.key < 10 " +
           "right semi join $TBL src3 on " +
           "  src2.key = src3.key and src3.key < 10 " +
           "right anti join $TBL src3 on " +
           "  src2.key = src3.key and src3.key < 10 " +
           "join $TBL src3 on " +
           "  src2.key = src3.key and src3.key < 10 " +
           "inner join $TBL src3 on " +
           "  src2.key = src3.key and src3.key < 10 ").replace("$TBL", tbl));
      ParsesOk(
          ("select * from $TBL src1 " +
           "left outer join $TBL src2 using (a, b, c) " +
           "right outer join $TBL src3 using (d, e, f) " +
           "full outer join $TBL src4 using (d, e, f) " +
           "left semi join $TBL src5 using (d, e, f) " +
           "left anti join $TBL src6 using (d, e, f) " +
           "right semi join $TBL src7 using (d, e, f) " +
           "right anti join $TBL src8 using (d, e, f) " +
           "join $TBL src9 using (d, e, f) " +
           "inner join $TBL src10 using (d, e, f) ").replace("$TBL", tbl));

      // Test cross joins
      ParsesOk("select * from $TBL cross join $TBL".replace("$TBL", tbl));
    }

    // Test NULLs in on clause.
    ParsesOk("select * from src src1 " +
        "left outer join src src2 on NULL " +
        "right outer join src src3 on (NULL) " +
        "full outer join src src3 on NULL " +
        "left semi join src src3 on (NULL) " +
        "left anti join src src3 on (NULL) " +
        "right semi join src src3 on (NULL) " +
        "right anti join src src3 on (NULL) " +
        "join src src3 on NULL " +
        "inner join src src3 on (NULL) " +
        "where src2.bla = src3.bla " +
        "order by src1.key, src1.value, src2.key, src2.value, src3.key, src3.value");
    // Arbitrary exprs in on clause parse ok.
    ParsesOk("select * from src src1 join src src2 on ('a')");
    ParsesOk("select * from src src1 join src src2 on (f(a, b))");
    ParserError("select * from src src1 " +
        "left outer join src src2 on (src1.key = src2.key and)");

    // Using clause requires SlotRef.
    ParserError("select * from src src1 join src src2 using (1)");
    ParserError("select * from src src1 join src src2 using (f(id))");
    // Using clause required parenthesis.
    ParserError("select * from src src1 join src src2 using id");

    // Cross joins do not accept on/using
    ParserError("select * from a cross join b on (a.id = b.id)");
    ParserError("select * from a cross join b using (id)");
  }

  @Test
  public void TestTableSampleClause() {
    String tblRefs[] = new String[] { "tbl", "db.tbl", "db.tbl.col", "db.tbl.col.fld" };
    String tblAliases[] = new String[] { "", "t" };
    String tblSampleClauses[] = new String[] {
        "", "tablesample system(10)", "tablesample system(100) repeatable(20)" };
    String tblHints[] = new String[] {
        "", "/* +schedule_remote */", "[schedule_random_replica]"
    };
    for (String tbl: tblRefs) {
      for (String alias: tblAliases) {
        for (String smp: tblSampleClauses) {
          for (String hint: tblHints) {
            // Single table.
            ParsesOk(String.format("select * from %s %s %s %s", tbl, alias, smp, hint));
            // Multi table.
            ParsesOk(String.format(
                "select a.* from %s %s %s %s join %s %s %s %s using (id)",
                tbl, alias, smp, hint, tbl, alias, smp, hint));
            ParsesOk(String.format(
                "select a.* from %s %s %s %s, %s %s %s %s",
                tbl, alias, smp, hint, tbl, alias, smp, hint));
            // Inline view.
            ParsesOk(String.format("select * from (select 1 from %s %s) v %s %s",
                tbl, alias, smp, hint));

          }
        }
      }
    }

    // Table alias most come before TABLESAMPLE.
    ParserError("select * from t tablesample (10) a");
    // Hints must come after TABLESAMPLE.
    ParserError("select * from t [schedule_remote] tablesample (10)");
    ParserError("select * from t /* +schedule_remote */ tablesample (10)");
    // Missing SYSTEM.
    ParserError("select * from t tablesample (10)");
    // Missing parenthesis.
    ParserError("select * from t tablesample system 10");
    // Percent must be int literal.
    ParserError("select * from t tablesample system (10 + 10");
    // Missing random seed.
    ParserError("select * from t tablesample system (10) repeatable");
    // Random seed must be an int literal.
    ParserError("select * from t tablesample system (10) repeatable (10 + 10)");
    // Negative percent.
    ParserError("select * from t tablesample system (-10)");
    // Negative random seed.
    ParserError("select * from t tablesample system (10) repeatable(-10)");
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
    ParsesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
             "order by string_col asc, float_col desc, int_col + bigint_col " +
             "asc nulls first");
    ParsesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
             "order by string_col asc, float_col desc, int_col + bigint_col " +
             "desc nulls last");
    ParsesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
             "order by string_col asc, float_col desc, int_col + bigint_col " +
             "nulls first");
    ParsesOk("select int_col, string_col, bigint_col, count(*) from alltypes " +
             "order by string_col asc, float_col desc nulls last, int_col + bigint_col " +
             "nulls first");
    ParsesOk("select int_col from alltypes order by true, false, NULL");
    ParserError("select int_col, string_col, bigint_col, count(*) from alltypes " +
                "order by by string_col asc desc");
    ParserError("select int_col, string_col, bigint_col, count(*) from alltypes " +
                "nulls first");
    ParserError("select int_col, string_col, bigint_col, count(*) from alltypes " +
                "order by string_col nulls");
    ParserError("select int_col, string_col, bigint_col, count(*) from alltypes " +
                "order by string_col nulls first asc");
    ParserError("select int_col, string_col, bigint_col, count(*) from alltypes " +
                "order by string_col nulls first last");
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
    ParsesOk("select a, b, c from test inner join test2 using(a) limit 10 + 10");
    // The following will parse because limit takes an expr, though they will fail in
    // analysis
    ParsesOk("select a, b, c from test inner join test2 using(a) limit 'a'");
    ParsesOk("select a, b, c from test inner join test2 using(a) limit a");
    ParsesOk("select a, b, c from test inner join test2 using(a) limit true");
    ParsesOk("select a, b, c from test inner join test2 using(a) limit false");
    ParsesOk("select a, b, c from test inner join test2 using(a) limit NULL");
    // Not an expr, will not parse
    ParserError("select a, b, c from test inner join test2 using(a) limit 10 " +
        "where a > 10");
  }

  @Test
  public void TestOffset() {
    ParsesOk("select a from test order by a limit 10 offset 5");
    ParsesOk("select a from test order by a limit 10 offset 0");
    ParsesOk("select a from test order by a limit 10 offset 0 + 5 / 2");
    ParsesOk("select a from test order by a asc limit 10 offset 5");
    ParsesOk("select a from test order by a offset 5");
    ParsesOk("select a from test limit 10 offset 5"); // Parses OK, doesn't analyze
    ParsesOk("select a from test offset 5"); // Parses OK, doesn't analyze
    ParsesOk("select a from (select a from test offset 5) A"); // Doesn't analyze
    ParsesOk("select a from (select a from test order by a offset 5) A");
    ParserError("select a from test order by a limit offset");
    ParserError("select a from test order by a limit offset 5");
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
    ParsesOk("(select a from test) union (select a from test) " +
        "union (select a from test) union (select a from test) order by a nulls first");
    // Union with limit.
    ParsesOk("(select a from test) union (select a from test) " +
        "union (select a from test) union (select a from test) limit 10");
    // Union with order by, offset and limit.
    ParsesOk("(select a from test) union (select a from test) " +
        "union (select a from test) union (select a from test) order by a limit 10");
    ParsesOk("(select a from test) union (select a from test) " +
        "union (select a from test) union (select a from test) order by a " +
        "nulls first limit 10");
    ParsesOk("(select a from test) union (select a from test) " +
        "union (select a from test) union (select a from test) order by a " +
        "nulls first offset 10");
    ParserError("select a from test union (select a from test) " +
        "union (select a from test) union (select a from test) offset 10");
    // Union with some select blocks in parenthesis, and others not.
    ParsesOk("(select a from test) union select a from test " +
        "union (select a from test) union select a from test");
    ParsesOk("select a from test union (select a from test) " +
        "union select a from test union (select a from test)");
    // Union with order by, offset and limit binding to last select.
    ParsesOk("(select a from test) union (select a from test) " +
        "union select a from test union select a from test order by a limit 10");
    ParsesOk("(select a from test) union (select a from test) " +
        "union select a from test union select a from test order by a offset 10");
    ParsesOk("(select a from test) union (select a from test) " +
        "union select a from test union select a from test order by a");
    // Union with order by and limit.
    // Last select with order by and limit is in parenthesis.
    ParsesOk("select a from test union (select a from test) " +
        "union select a from test union (select a from test order by a limit 10) " +
        "order by a limit 1");
    ParsesOk("select a from test union (select a from test) " +
        "union select a from test union (select a from test order by a offset 10) " +
        "order by a limit 1");
    ParsesOk("select a from test union (select a from test) " +
        "union select a from test union (select a from test order by a) " +
        "order by a limit 1");
    // Union with order by, offset in first operand.
    ParsesOk("select a from test order by a union select a from test");
    ParsesOk("select a from test order by a offset 5 union select a from test");
    ParsesOk("select a from test offset 5 union select a from test");
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

    // Union in upsert query.
    ParsesOk("upsert into table t select a from test union select a from test");
    ParsesOk("upsert into table t select a from test union select a from test " +
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
  public void TestValuesStmt() throws AnalysisException {
    // Values stmt with a single row.
    ParsesOk("values(1, 'a', abc, 1.0, *)");
    ParsesOk("select * from (values(1, 'a', abc, 1.0, *)) as t");
    ParsesOk("values(1, 'a', abc, 1.0, *) union all values(1, 'a', abc, 1.0, *)");
    ParsesOk("insert into t values(1, 'a', abc, 1.0, *)");
    ParsesOk("upsert into t values(1, 'a', abc, 1.0, *)");
    // Values stmt with multiple rows.
    ParsesOk("values(1, abc), ('x', cde), (2), (efg, fgh, ghi)");
    ParsesOk("select * from (values(1, abc), ('x', cde), (2), (efg, fgh, ghi)) as t");
    ParsesOk("values(1, abc), ('x', cde), (2), (efg, fgh, ghi) " +
        "union all values(1, abc), ('x', cde), (2), (efg, fgh, ghi)");
    ParsesOk("insert into t values(1, abc), ('x', cde), (2), (efg, fgh, ghi)");
    ParsesOk("upsert into t values(1, abc), ('x', cde), (2), (efg, fgh, ghi)");
    // Test additional parenthesis.
    ParsesOk("(values(1, abc), ('x', cde), (2), (efg, fgh, ghi))");
    ParsesOk("values((1, abc), ('x', cde), (2), (efg, fgh, ghi))");
    ParsesOk("(values((1, abc), ('x', cde), (2), (efg, fgh, ghi)))");
    // Test alias inside select list to assign column names.
    ParsesOk("values(1 as x, 2 as y, 3 as z)");
    // Test order by and limit.
    ParsesOk("values(1, 'a') limit 10");
    ParsesOk("values(1, 'a') order by 1");
    ParsesOk("values(1, 'a') order by 1 limit 10");
    ParsesOk("values(1, 'a') order by 1 offset 10");
    ParsesOk("values(1, 'a') offset 10");
    ParsesOk("values(1, 'a'), (2, 'b') order by 1 limit 10");
    ParsesOk("values((1, 'a'), (2, 'b')) order by 1 limit 10");

    ParserError("values()");
    ParserError("values 1, 'a', abc, 1.0");
    ParserError("values(1, 'a') values(1, 'a')");
    ParserError("select values(1, 'a')");
    ParserError("select * from values(1, 'a', abc, 1.0) as t");
    ParserError("values((1, 2, 3), values(1, 2, 3))");
    ParserError("values((1, 'a'), (1, 'a') order by 1)");
    ParserError("values((1, 'a'), (1, 'a') limit 10)");
  }

  @Test
  public void TestWithClause() throws AnalysisException {
    ParsesOk("with t as (select 1 as a) select a from t");
    ParsesOk("with t(x) as (select 1 as a) select x from t");
    ParsesOk("with t as (select c from tab) select * from t");
    ParsesOk("with t(x, y) as (select * from tab) select * from t");
    ParsesOk("with t as (values(1, 2, 3), (4, 5, 6)) select * from t");
    ParsesOk("with t(x, y, z) as (values(1, 2, 3), (4, 5, 6)) select * from t");
    ParsesOk("with t1 as (select 1 as a), t2 as (select 2 as a) select a from t1");
    ParsesOk("with t1 as (select c from tab), t2 as (select c from tab)" +
        "select c from t2");
    ParsesOk("with t1(x) as (select c from tab), t2(x) as (select c from tab)" +
        "select x from t2");
    // With clause and union statement.
    ParsesOk("with t1 as (select 1 as a), t2 as (select 2 as a)" +
        "select a from t1 union all select a from t2");
    // With clause and join.
    ParsesOk("with t1 as (select 1 as a), t2 as (select 2 as a)" +
        "select a from t1 inner join t2 on t1.a = t2.a");
    // With clause in inline view.
    ParsesOk("select * from (with t as (select 1 as a) select * from t) as a");
    ParsesOk("select * from (with t(x) as (select 1 as a) select * from t) as a");
    // With clause in query statement of insert statement.
    ParsesOk("insert into x with t as (select * from tab) select * from t");
    ParsesOk("insert into x with t(x, y) as (select * from tab) select * from t");
    ParsesOk("insert into x with t as (values(1, 2, 3)) select * from t");
    ParsesOk("insert into x with t(x, y) as (values(1, 2, 3)) select * from t");
    // With clause before insert statement.
    ParsesOk("with t as (select 1) insert into x select * from t");
    ParsesOk("with t(x) as (select 1) insert into x select * from t");
    // With clause in query statement of upsert statement.
    ParsesOk("upsert into x with t as (select * from tab) select * from t");
    ParsesOk("upsert into x with t(x, y) as (select * from tab) select * from t");
    ParsesOk("upsert into x with t as (values(1, 2, 3)) select * from t");
    ParsesOk("upsert into x with t(x, y) as (values(1, 2, 3)) select * from t");
    // With clause before upsert statement.
    ParsesOk("with t as (select 1) upsert into x select * from t");
    ParsesOk("with t(x) as (select 1) upsert into x select * from t");

    // Test quoted identifier or string literal as table alias.
    ParsesOk("with `t1` as (select 1 a), 't2' as (select 2 a), \"t3\" as (select 3 a)" +
        "select a from t1 union all select a from t2 union all select a from t3");
    // Multiple with clauses. Operands must be in parenthesis to
    // have their own with clause.
    ParsesOk("with t as (select 1) " +
        "(with t as (select 2) select * from t) union all " +
        "(with t as (select 3) select * from t)");
    ParsesOk("with t as (select 1) " +
        "(with t as (select 2) select * from t) union all " +
        "(with t as (select 3) select * from t) order by 1 limit 1");
    // Multiple with clauses. One before the insert and one inside the query statement.
    ParsesOk("with t as (select 1) insert into x with t as (select 2) select * from t");
    ParsesOk("with t(c1) as (select 1) " +
        "insert into x with t(c2) as (select 2) select * from t");
    // Multiple with clauses. One before the upsert and one inside the query statement.
    ParsesOk("with t as (select 1) upsert into x with t as (select 2) select * from t");
    ParsesOk("with t(c1) as (select 1) " +
        "upsert into x with t(c2) as (select 2) select * from t");

    // Empty with clause.
    ParserError("with t as () select 1");
    ParserError("with t(x) as () select 1");
    // No labels inside parenthesis.
    ParserError("with t() as (select 1 as a) select a from t");
    // Missing select, union or insert statement after with clause.
    ParserError("select * from (with t as (select 1 as a)) as a");
    ParserError("with t as (select 1)");
    // Missing parenthesis around with query statement.
    ParserError("with t as select 1 as a select a from t");
    ParserError("with t as select 1 as a union all select a from t");
    ParserError("with t1 as (select 1 as a), t2 as select 2 as a select a from t");
    ParserError("with t as select 1 as a select a from t");
    // Missing parenthesis around column labels.
    ParserError("with t c1 as (select 1 as a) select c1 from t");
    // Insert in with clause is not valid.
    ParserError("with t as (insert into x select * from tab) select * from t");
    ParserError("with t(c1) as (insert into x select * from tab) select * from t");
    // Upsert in with clause is not valid.
    ParserError("with t as (upsert into x select * from tab) select * from t");
    ParserError("with t(c1) as (upsert into x select * from tab) select * from t");
    // Union operands need to be parenthesized to have their own with clause.
    ParserError("select * from t union all with t as (select 2) select * from t");
  }

  @Test
  public void TestNumericLiteralMinMaxValues() {
    // Test integer types.
    ParsesOk(String.format("select %s", Byte.toString(Byte.MIN_VALUE)));
    ParsesOk(String.format("select %s", Byte.toString(Byte.MAX_VALUE)));
    ParsesOk(String.format("select %s", Short.toString(Short.MIN_VALUE)));
    ParsesOk(String.format("select %s", Short.toString(Short.MAX_VALUE)));
    ParsesOk(String.format("select %s", Integer.toString(Integer.MIN_VALUE)));
    ParsesOk(String.format("select %s", Integer.toString(Integer.MAX_VALUE)));
    ParsesOk(String.format("select %s", Long.toString(Long.MIN_VALUE)));
    ParsesOk(String.format("select %s", Long.toString(Long.MAX_VALUE)));

    // Overflowing long integers parse ok. Analysis will handle it.
    ParsesOk(String.format("select %s1", Long.toString(Long.MIN_VALUE)));
    ParsesOk(String.format("select %s1", Long.toString(Long.MAX_VALUE)));
    // Test min int64-1.
    BigInteger minMinusOne = BigInteger.valueOf(Long.MAX_VALUE);
    minMinusOne = minMinusOne.add(BigInteger.ONE);
    ParsesOk(String.format("select %s", minMinusOne.toString()));
    // Test max int64+1.
    BigInteger maxPlusOne = BigInteger.valueOf(Long.MAX_VALUE);
    maxPlusOne = maxPlusOne.add(BigInteger.ONE);
    ParsesOk(String.format("select %s", maxPlusOne.toString()));

    // Test floating-point types.
    ParsesOk(String.format("select %s", Float.toString(Float.MIN_VALUE)));
    ParsesOk(String.format("select %s", Float.toString(Float.MAX_VALUE)));
    ParsesOk(String.format("select -%s", Float.toString(Float.MIN_VALUE)));
    ParsesOk(String.format("select -%s", Float.toString(Float.MAX_VALUE)));
    ParsesOk(String.format("select %s", Double.toString(Double.MIN_VALUE)));
    ParsesOk(String.format("select %s", Double.toString(Double.MAX_VALUE)));
    ParsesOk(String.format("select -%s", Double.toString(Double.MIN_VALUE)));
    ParsesOk(String.format("select -%s", Double.toString(Double.MAX_VALUE)));

    // Test overflow and underflow
    ParsesOk(String.format("select %s1", Double.toString(Double.MIN_VALUE)));
    ParsesOk(String.format("select %s1", Double.toString(Double.MAX_VALUE)));
  }

  @Test
  public void TestIdentQuoting() {
    ParsesOk("select a from `t`");
    ParsesOk("select a from default.`t`");
    ParsesOk("select a from default.t");
    ParsesOk("select a from default.`t`");
    ParsesOk("select 01a from default.`01_t`");

    ParsesOk("select `a` from default.t");
    ParsesOk("select `tbl`.`a` from default.t");
    ParsesOk("select `db`.`tbl`.`a` from default.t");
    ParsesOk("select `12db`.`tbl`.`12_a` from default.t");

    // Make sure quoted float literals are identifiers.
    ParsesOk("select `8e6`", SlotRef.class);
    ParsesOk("select `4.5e2`", SlotRef.class);
    ParsesOk("select `.7e9`", SlotRef.class);

    // Mixed quoting
    ParsesOk("select `db`.tbl.`a` from default.t");
    ParsesOk("select `db.table.a` from default.t");

    // Identifiers consisting of only whitespace not allowed.
    ParserError("select a from ` `");
    ParserError("select a from `    `");
    // Empty quoted identifier doesn't parse.
    ParserError("select a from ``");

    // Whitespace can be interspersed with other characters.
    // Whitespace is trimmed from the beginning and end of an identifier.
    ParsesOk("select a from `a a a    `");
    ParsesOk("select a from `    a a a`");
    ParsesOk("select a from `    a a a    `");

    // Quoted identifiers can contain any characters except "`".
    ParsesOk("select a from `all types`");
    ParsesOk("select a from default.`all types`");
    ParsesOk("select a from `~!@#$%^&*()-_=+|;:'\",<.>/?`");
    // Quoted identifiers do not unescape escape sequences.
    ParsesOk("select a from `ab\rabc`");
    ParsesOk("select a from `ab\tabc`");
    ParsesOk("select a from `ab\fabc`");
    ParsesOk("select a from `ab\babc`");
    ParsesOk("select a from `ab\nabc`");
    // Test non-printable control characters inside quoted identifiers.
    ParsesOk("select a from `abc\u0000abc`");
    ParsesOk("select a from `abc\u0019abc`");
    ParsesOk("select a from `abc\u007fabc`");

    // Quoted identifiers can contain keywords.
    ParsesOk("select `select`, `insert`, `upsert` from `table` where `where` = 10");

    // Quoted identifiers cannot contain "`"
    ParserError("select a from `abcde`abcde`");
    ParserError("select a from `abc\u0060abc`");

    // Wrong quotes
    ParserError("select a from 'default'.'t'");

    // Lots of quoting
    ParsesOk(
        "select `db`.`tbl`.`a` from `default`.`t` `alias` where `alias`.`col` = 'string'"
        + " group by `alias`.`col`");
  }

  @Test
  public void TestLiteralExprs() {
    // negative integer literal
    ParsesOk("select -1 from t where -1", NumericLiteral.class);
    ParsesOk("select - 1 from t where - 1", NumericLiteral.class);
    ParsesOk("select a - - 1 from t where a - - 1", ArithmeticExpr.class);
    ParsesOk("select a - - - 1 from t where a - - - 1", ArithmeticExpr.class);

    // positive integer literal
    ParsesOk("select +1 from t where +1", NumericLiteral.class);
    ParsesOk("select + 1 from t where + 1", NumericLiteral.class);
    ParsesOk("select a + + 1 from t where a + + 1", ArithmeticExpr.class);
    ParsesOk("select a + + + 1 from t where a + + + 1", ArithmeticExpr.class);

    // float literals
    ParsesOk("select +1.0 from t where +1.0", NumericLiteral.class);
    ParsesOk("select +-1.0 from t where +-1.0", NumericLiteral.class);
    ParsesOk("select +1.-0 from t where +1.-0", ArithmeticExpr.class);

    // test scientific notation
    ParsesOk("select 8e6 from t where 8e6", NumericLiteral.class);
    ParsesOk("select +8e6 from t where +8e6", NumericLiteral.class);
    ParsesOk("select 8e+6 from t where 8e+6", NumericLiteral.class);
    ParsesOk("select -8e6 from t where -8e6", NumericLiteral.class);
    ParsesOk("select 8e-6 from t where 8e-6", NumericLiteral.class);
    ParsesOk("select -8e-6 from t where -8e-6", NumericLiteral.class);
    // with a decimal point
    ParsesOk("select 4.5e2 from t where 4.5e2", NumericLiteral.class);
    ParsesOk("select +4.5e2 from t where +4.5e2", NumericLiteral.class);
    ParsesOk("select 4.5e+2 from t where 4.5e+2", NumericLiteral.class);
    ParsesOk("select -4.5e2 from t where -4.5e2", NumericLiteral.class);
    ParsesOk("select 4.5e-2 from t where 4.5e-2", NumericLiteral.class);
    ParsesOk("select -4.5e-2 from t where -4.5e-2", NumericLiteral.class);
    // with a decimal point but without a number before the decimal
    ParsesOk("select .7e9 from t where .7e9", NumericLiteral.class);
    ParsesOk("select +.7e9 from t where +.7e9", NumericLiteral.class);
    ParsesOk("select .7e+9 from t where .7e+9", NumericLiteral.class);
    ParsesOk("select -.7e9 from t where -.7e9", NumericLiteral.class);
    ParsesOk("select .7e-9 from t where .7e-9", NumericLiteral.class);
    ParsesOk("select -.7e-9 from t where -.7e-9", NumericLiteral.class);

    // mixed signs
    ParsesOk("select -+-1 from t where -+-1", NumericLiteral.class);
    ParsesOk("select - +- 1 from t where - +- 1", NumericLiteral.class);
    ParsesOk("select 1 + -+ 1 from t where 1 + -+ 1", ArithmeticExpr.class);

    // Boolean literals
    ParsesOk("select true from t where true", BoolLiteral.class);
    ParsesOk("select false from t where false", BoolLiteral.class);

    // Null literal
    ParsesOk("select NULL from t where NULL", NullLiteral.class);

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
    ParsesOk("select 'five', 5, 5.0, i + 5 from t", StringLiteral.class);
    ParsesOk("select \"\\\"five\\\"\" from t\n", StringLiteral.class);
    ParsesOk("select \"\'five\'\" from t\n", StringLiteral.class);
    ParsesOk("select \"\'five\" from t\n", StringLiteral.class);

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
    ParserError("select \"\\\" from t",
        "Syntax error in line 1:\n" +
            "select \"\\\" from t\n" +
            "        ^\n" +
            "Encountered: Unexpected character");
    // Unsupported character
    ParserError("@",
        "Syntax error in line 1:\n" +
        "@\n" +
        "^\n" +
        "Encountered: Unexpected character");
    ParsesOk("SELECT '@'");
  }

  // test string literal s with single and double quotes
  private void testStringLiteral(String s) {
    String singleQuoteQuery = "select " + "'" + s + "'" + " from t";
    String doubleQuoteQuery = "select " + "\"" + s + "\"" + " from t";
    ParsesOk(singleQuoteQuery, StringLiteral.class);
    ParsesOk(doubleQuoteQuery, StringLiteral.class);
  }

  @Test
  public void TestFunctionCallExprs() {
    ParsesOk("select f1(5), f2('five'), f3(5.0, i + 5) from t");
    ParsesOk("select f1(true), f2(true and false), f3(null) from t");
    ParsesOk("select f1(*)");
    ParsesOk("select f1(distinct col)");
    ParsesOk("select f1(distinct col, col2)");
    ParsesOk("select decode(col, col2, col3)");
    // nullif should rewrite to if
    assertEquals("SELECT if(col IS DISTINCT FROM col2, col, NULL) FROM t",
        ParsesOk("select nullif(col, col2) from t").toSql());
    assertEquals("SELECT if(col IS DISTINCT FROM col2, col, NULL) FROM t",
        ParsesOk("select _impala_builtins.nullif(col, col2) from t").toSql());
    ParserError("select f( from t");
    ParserError("select f(5.0 5.0) from t");
  }

  @Test
  public void TestArithmeticExprs() {
    for (String lop: operands_) {
      for (String rop: operands_) {
        for (ArithmeticExpr.Operator op : ArithmeticExpr.Operator.values()) {
          String expr = null;
          switch (op.getPos()) {
            case BINARY_INFIX:
              expr = String.format("%s %s %s", lop, op.toString(), rop);
              break;
            case UNARY_POSTFIX:
              expr = String.format("%s %s", lop, op.toString());
              break;
            case UNARY_PREFIX:
              expr = String.format("%s %s", op.toString(), lop);
              break;
            default:
              fail("Unknown operator kind: " + op.getPos());
          }
          ParsesOk(String.format("select %s from t where %s", expr, expr));
        }
      }
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

  @Test
  public void TestFactorialPrecedenceAssociativity() {
    // Test factorial precedence relative to other arithmetic operators.
    // Should be parsed as 3 + (3!)
    // TODO: disabled b/c IMPALA-2149 - low precedence of prefix ! prevents this from
    // parsing in the expected way
    SelectStmt stmt = (SelectStmt) ParsesOk("SELECT 3 + 3!");
    Expr e = stmt.getSelectList().getItems().get(0).getExpr();
    assertTrue(e instanceof ArithmeticExpr);
    // ArithmeticExpr ae = (ArithmeticExpr) e;
    // assertEquals("Expected ! to bind more tightly than +",
    //              ArithmeticExpr.Operator.ADD, ae.getOp());
    // assertEquals(2, ae.getChildren().size());
    // assertTrue(ae.getChild(1) instanceof ArithmeticExpr);
    // assertEquals(ArithmeticExpr.Operator.FACTORIAL,
    //              ((ArithmeticExpr)ae.getChild(1)).getOp());

    // Test factorial associativity.
    stmt = (SelectStmt) ParsesOk("SELECT 3! = 4");
    // Should be parsed as (3!) = (4)
    e = stmt.getSelectList().getItems().get(0).getExpr();
    assertTrue(e instanceof BinaryPredicate);
    BinaryPredicate bp = (BinaryPredicate) e;
    assertEquals(BinaryPredicate.Operator.EQ, bp.getOp());
    assertEquals(2, bp.getChildren().size());

    // Test != not broken
    stmt = (SelectStmt) ParsesOk("SELECT 3 != 4");
    // Should be parsed as (3) != (4)
    e = stmt.getSelectList().getItems().get(0).getExpr();
    assertTrue(e instanceof BinaryPredicate);
    bp = (BinaryPredicate) e;
    assertEquals(BinaryPredicate.Operator.NE, bp.getOp());
    assertEquals(2, bp.getChildren().size());
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

      ParserError("select date_sub(distinct NULL, interval NULL " +
          timeUnit.toString() + ")");
      ParserError("select date_sub(*, interval NULL " + timeUnit.toString() + ")");
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
    ParsesOk("select count(all *) from t");
    ParsesOk("select count(all 1) from t");
    ParsesOk("select min(a), min(distinct a) from t");
    ParsesOk("select max(a), max(distinct a) from t");
    ParsesOk("select sum(a), sum(distinct a) from t");
    ParsesOk("select avg(a), avg(distinct a) from t");
    ParsesOk("select distinct a, b, c from t");
    ParsesOk("select distinctpc(a), distinctpc(distinct a) from t");
    ParsesOk("select distinctpcsa(a), distinctpcsa(distinct a) from t");
    ParsesOk("select ndv(a), ndv(distinct a) from t");
    ParsesOk("select group_concat(a) from t");
    ParsesOk("select group_concat(a, ', ') from t");
    ParsesOk("select group_concat(a, ', ', c) from t");
  }

  @Test
  public void TestAnalyticExprs() {
    ParsesOk("select sum(v) over (partition by a, 2*b order by 3*c rows between "
        + "2+2 preceding and 2-2 following) from t");
    ParsesOk("select sum(v) over (order by 3*c rows between "
        + "unbounded preceding and unbounded following) from t");
    ParsesOk("select sum(v) over (partition by a, 2*b) from t");
    ParsesOk("select sum(v) over (partition by a, 2*b order by 3*c range between "
        + "unbounded preceding and unbounded following) from t");
    ParsesOk("select sum(v) over (order by 3*c range between "
        + "2 following and 4 following) from t");
    ParsesOk("select sum(v) over (partition by a, 2*b) from t");
    ParsesOk("select 2 * x, sum(v) over (partition by a, 2*b order by 3*c rows between "
        + "2+2 preceding and 2-2 following), rank() over (), y from t");
    // not a function call
    ParserError("select v over (partition by a, 2*b order by 3*c rows between 2 "
        + "preceding and 2 following) from t");
    // something missing
    ParserError("select sum(v) over (partition a, 2*b order by 3*c rows between "
        + "unbounded preceding and current row) from t");
    ParserError("select sum(v) over (partition by a, 2*b order 3*c rows between 2 "
        + "preceding and 2 following) from t");
    ParserError("select sum(v) over (partition by a, 2*b order by 3*c rows 2 "
        + "preceding and 2 following) from t");
    ParsesOk("select sum(v) over (partition by a, 2*b) from t");
    // Special case for DECODE, which results in a parse error when used in
    // an analytic context. Note that "ecode() over ()" would parse fine since
    // that is handled by the standard function call lookup.
    ParserError("select decode(1, 2, 3) over () from t");
  }

  @Test
  public void TestPredicates() {
    ArrayList<String> operations = new ArrayList<String>();
    for (BinaryPredicate.Operator op : BinaryPredicate.Operator.values()) {
      operations.add(op.toString());
    }
    operations.add("like");
    operations.add("ilike");
    operations.add("rlike");
    operations.add("regexp");
    operations.add("iregexp");

    ArrayList<String> boolTestVals = new ArrayList<String>();
    boolTestVals.add("null");
    boolTestVals.add("unknown");
    boolTestVals.add("true");
    boolTestVals.add("false");

    for (String lop : operands_) {
      for (String rop : operands_) {
        for (String op : operations) {
          String expr = String.format("%s %s %s", lop, op.toString(), rop);
          ParsesOk(String.format("select %s from t where %s", expr, expr));
        }
      }
      for (String val : boolTestVals) {
        String isExpr = String.format("%s is %s", lop, val);
        String isNotExpr = String.format("%s is not %s", lop, val);
        ParsesOk(String.format("select %s from t where %s", isExpr, isExpr));
        ParsesOk(String.format("select %s from t where %s", isNotExpr, isNotExpr));
      }
      ParserError(String.format("select %s is nonsense", lop));
      ParserError(String.format("select %s is not nonsense", lop));
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
    ParserError("select a, b, c from t where " +
        "(a = 5 " + orStr + " b = 6) " + andStr + " c = 7)");
    ParserError("select a, b, c from t where " +
        "((a = 5 " + orStr + " b = 6) " + andStr + " c = 7");
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

    // Test right associativity of NOT.
    for (String notStr : notStrs) {
      SelectStmt stmt =
          (SelectStmt) ParsesOk(String.format("select %s a != b", notStr));
      // The NOT should be applied on the result of a != b, and not on a only.
      Expr e = stmt.getSelectList().getItems().get(0).getExpr();
      assertTrue(e instanceof CompoundPredicate);
      CompoundPredicate cp = (CompoundPredicate) e;
      assertEquals(CompoundPredicate.Operator.NOT, cp.getOp());
      assertEquals(1, cp.getChildren().size());
      assertTrue(cp.getChild(0) instanceof BinaryPredicate);
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
    ParsesOk("select a.b.c.d from a.b where b > 5");
  }

  /**
   * Run positive tests for INSERT INTO/OVERWRITE.
   */
  private void testInsert() {
    for (String qualifier: new String[] {"overwrite", "into"}) {
      for (String optTbl: new String[] {"", "table"}) {
        // Entire unpartitioned table.
        ParsesOk(String.format("insert %s %s t select a from src where b > 5",
            qualifier, optTbl));
        // Static partition with one partitioning key.
        ParsesOk(String.format(
            "insert %s %s t partition (pk1=10) select a from src where b > 5",
            qualifier, optTbl));
        // Dynamic partition with one partitioning key.
        ParsesOk(String.format(
            "insert %s %s t partition (pk1) select a from src where b > 5",
            qualifier, optTbl));
        // Static partition with two partitioning keys.
        ParsesOk(String.format("insert %s %s t partition (pk1=10, pk2=20) " +
            "select a from src where b > 5",
            qualifier, optTbl));
        // Fully dynamic partition with two partitioning keys.
        ParsesOk(String.format(
            "insert %s %s t partition (pk1, pk2) select a from src where b > 5",
            qualifier, optTbl));
        // Partially dynamic partition with two partitioning keys.
        ParsesOk(String.format(
            "insert %s %s t partition (pk1=10, pk2) select a from src where b > 5",
            qualifier, optTbl));
        // Partially dynamic partition with two partitioning keys.
        ParsesOk(String.format(
            "insert %s %s t partition (pk1, pk2=20) select a from src where b > 5",
            qualifier, optTbl));
        // Static partition with two NULL partitioning keys.
        ParsesOk(String.format("insert %s %s t partition (pk1=NULL, pk2=NULL) " +
            "select a from src where b > 5",
            qualifier, optTbl));
        // Static partition with boolean partitioning keys.
        ParsesOk(String.format("insert %s %s t partition (pk1=false, pk2=true) " +
            "select a from src where b > 5",
            qualifier, optTbl));
        // Static partition with arbitrary exprs as partitioning keys.
        ParsesOk(String.format("insert %s %s t partition (pk1=abc, pk2=(5*8+10)) " +
            "select a from src where b > 5",
            qualifier, optTbl));
        ParsesOk(String.format(
            "insert %s %s t partition (pk1=f(a), pk2=!true and false) " +
                "select a from src where b > 5",
                qualifier, optTbl));
        // Permutation
        ParsesOk(String.format("insert %s %s t(a,b,c) values(1,2,3)",
            qualifier, optTbl));
        // Permutation with mismatched select list (should parse fine)
        ParsesOk(String.format("insert %s %s t(a,b,c) values(1,2,3,4,5,6)",
            qualifier, optTbl));
        // Permutation and partition
        ParsesOk(String.format("insert %s %s t(a,b,c) partition(d) values(1,2,3,4)",
            qualifier, optTbl));
        // Empty permutation list
        ParsesOk(String.format("insert %s %s t() select 1 from a",
            qualifier, optTbl));
        // Permutation with optional query statement
        ParsesOk(String.format("insert %s %s t() partition(d) ",
            qualifier, optTbl));
        ParsesOk(String.format("insert %s %s t() ",
            qualifier, optTbl));
        // No comma in permutation list
        ParserError(String.format("insert %s %s t(a b c) select 1 from a",
            qualifier, optTbl));
        // Can't use strings as identifiers in permutation list
        ParserError(String.format("insert %s %s t('a') select 1 from a",
            qualifier, optTbl));
        // Expressions not allowed in permutation list
        ParserError(String.format("insert %s %s t(a=1, b) select 1 from a",
            qualifier, optTbl));
      }
    }
  }

  @Test
  public void TestInsert() {
    // Positive tests.
    testInsert();
    // Missing query statement
    ParserError("insert into table t");
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
    // Misplaced plan hints.
    ParserError("insert [shuffle] into table t partition (pk1=10 pk2=20) " +
        "select a from src where b > 5");
    ParserError("insert into [shuffle] table t partition (pk1=10 pk2=20) " +
        "select a from src where b > 5");
    ParserError("insert into table t [shuffle] partition (pk1=10 pk2=20) " +
        "select a from src where b > 5");
    ParserError("insert into table t partition [shuffle] (pk1=10 pk2=20) " +
        "select a from src where b > 5");
  }

  @Test
  public void TestUpsert() {
    for (String optTbl: new String[] {"", "table"}) {
      // SELECT clause
      ParsesOk(String.format("upsert into %s t select a from src", optTbl));
      // VALUES clause
      ParsesOk(String.format("upsert into %s t values (1, 2, 3)", optTbl));
      // Permutation
      ParsesOk(String.format("upsert into %s t (a,b,c) values(1,2,3)", optTbl));
      // Permutation with mismatched select list (should parse fine)
      ParsesOk(String.format("upsert into %s t (a,b,c) values(1,2,3,4,5,6)", optTbl));
      // Empty permutation list
      ParsesOk(String.format("upsert into %s t () select 1 from a", optTbl));
      // Permutation with optional query statement
      ParsesOk(String.format("upsert into %s t () ", optTbl));
      // WITH clause
      ParsesOk(String.format("with x as (select a from src where b > 5) upsert into %s " +
          "t select * from x", optTbl));

      // Missing query statement
      ParserError(String.format("upsert into %s t", optTbl));
      // Missing 'into'.
      ParserError(String.format("upsert %s t select a from src where b > 5", optTbl));
      // Missing target table identifier.
      ParserError(String.format("upsert into %s select a from src where b > 5", optTbl));
      // No comma in permutation list
      ParserError(String.format("upsert %s t(a b c) select 1 from a", optTbl));
      // Can't use strings as identifiers in permutation list
      ParserError(String.format("upsert %s t('a') select 1 from a", optTbl));
      // Expressions not allowed in permutation list
      ParserError(String.format("upsert %s t(a=1, b) select 1 from a", optTbl));
      // Upsert doesn't support ignore.
      ParserError(String.format("upsert ignore into %s t select a from src", optTbl));
      // Upsert doesn't support partition clauses.
      ParserError(String.format(
          "upsert into %s t partition (pk1=10) select a from src", optTbl));
      // Upsert doesn't support overwrite.
      ParserError(String.format("upsert overwrite %s t select 1 from src", optTbl));
    }
  }

  @Test
  public void TestUpdate() {
    ParsesOk("update t set x = 3 where a < b");
    ParsesOk("update t set x = (3 + length(\"hallo\")) where a < 'adasas'");
    ParsesOk("update t set x = 3");
    ParsesOk("update t set x=3, x=4 from a.b t where b = 10");
    ParserError("update t");
    ParserError("update t set x < 3");
    ParserError("update t set x");
    ParserError("update t set 4 = x");
    ParserError("update from t set x = 3");
    ParserError("update t where x = 4");
    ParserError("update t a set a = 10  where x = 4");
    ParserError("update t a from t b where set a = 10 x = 4");
  }

  @Test
  public void TestKuduUpdate() {
    //TestUtils.assumeKuduIsSupported();
    ParserError("update (select * from functional_kudu.testtbl) a set name = '10'");
  }

  @Test
  public void TestDelete() {
    ParsesOk("delete from t");
    ParsesOk("delete a from t a");
    ParsesOk("delete a from t a join b on a.id = b.id where true");
    ParsesOk("delete a from t a join b where true");
    ParsesOk("delete t from t");
    ParsesOk("delete t from t where a < b");
    ParsesOk("delete a from t a where a < b");
    ParsesOk("delete FROM t where a < b");
    ParsesOk("delete t where a < b");
    ParsesOk("delete t");
    ParserError("delete t join f on t.id = f.id");
  }

  @Test
  public void TestUse() {
    ParserError("USE");
    ParserError("USE db1 db2");
    ParsesOk("USE db1");
  }

  @Test
  public void TestShow() {
    // Short form ok
    ParsesOk("SHOW TABLES");
    // Well-formed pattern
    ParsesOk("SHOW TABLES 'tablename|othername'");
    // Empty pattern ok
    ParsesOk("SHOW TABLES ''");
    // Databases
    ParsesOk("SHOW DATABASES");
    ParsesOk("SHOW SCHEMAS");
    ParsesOk("SHOW DATABASES LIKE 'pattern'");
    ParsesOk("SHOW SCHEMAS LIKE 'p*ttern'");
    // Data sources
    ParsesOk("SHOW DATA SOURCES");
    ParsesOk("SHOW DATA SOURCES 'pattern'");
    ParsesOk("SHOW DATA SOURCES LIKE 'pattern'");
    ParsesOk("SHOW DATA SOURCES LIKE 'p*ttern'");

    // Functions
    for (String fnType: new String[] { "", "AGGREGATE", "ANALYTIC"}) {
      ParsesOk(String.format("SHOW %s FUNCTIONS", fnType));
      ParsesOk(String.format("SHOW %s FUNCTIONS LIKE 'pattern'", fnType));
      ParsesOk(String.format("SHOW %s FUNCTIONS LIKE 'p*ttern'", fnType));
      ParsesOk(String.format("SHOW %s FUNCTIONS", fnType));
      ParsesOk(String.format("SHOW %s FUNCTIONS in DB LIKE 'pattern'", fnType));
      ParsesOk(String.format("SHOW %s FUNCTIONS in DB", fnType));
    }

    // Show table/column stats.
    ParsesOk("SHOW TABLE STATS tbl");
    ParsesOk("SHOW TABLE STATS db.tbl");
    ParsesOk("SHOW TABLE STATS `db`.`tbl`");
    ParsesOk("SHOW COLUMN STATS tbl");
    ParsesOk("SHOW COLUMN STATS db.tbl");
    ParsesOk("SHOW COLUMN STATS `db`.`tbl`");

    // Show partitions
    ParsesOk("SHOW PARTITIONS tbl");
    ParsesOk("SHOW PARTITIONS db.tbl");
    ParsesOk("SHOW PARTITIONS `db`.`tbl`");

    // Show range partitions
    ParsesOk("SHOW RANGE PARTITIONS tbl");
    ParsesOk("SHOW RANGE PARTITIONS db.tbl");
    ParsesOk("SHOW RANGE PARTITIONS `db`.`tbl`");

    // Show files of table
    ParsesOk("SHOW FILES IN tbl");
    ParsesOk("SHOW FILES IN db.tbl");
    ParsesOk("SHOW FILES IN `db`.`tbl`");
    ParsesOk("SHOW FILES IN db.tbl PARTITION(x='a',y='b')");

    // Missing arguments
    ParserError("SHOW");
    // Malformed pattern (no quotes)
    ParserError("SHOW TABLES tablename");
    // Invalid SHOW DATA SOURCE statements
    ParserError("SHOW DATA");
    ParserError("SHOW SOURCE");
    ParserError("SHOW DATA SOURCE LIKE NotStrLiteral");
    ParserError("SHOW UNKNOWN FUNCTIONS");
    // Missing table/column qualifier.
    ParserError("SHOW STATS tbl");
    // Missing table.
    ParserError("SHOW TABLE STATS");
    ParserError("SHOW COLUMN STATS");
    // String literal not accepted.
    ParserError("SHOW TABLE STATS 'strlit'");
    // Missing table.
    ParserError("SHOW FILES IN");

    ParsesOk("SHOW FILES IN db.tbl PARTITION(p)");
  }

  @Test
  public void TestShowCreateTable() {
    ParsesOk("SHOW CREATE TABLE x");
    ParsesOk("SHOW CREATE TABLE db.x");
    ParserError("SHOW CREATE TABLE");
    ParserError("SHOW CREATE TABLE x y z");
  }

  @Test
  public void TestDescribeDb() {
    // Missing argument
    ParserError("DESCRIBE DATABASE");
    ParserError("DESCRIBE DATABASE FORMATTED");
    ParserError("DESCRIBE DATABASE EXTENDED");

    // database ok
    ParsesOk("DESCRIBE DATABASE databasename");
    ParsesOk("DESCRIBE DATABASE FORMATTED databasename");
    ParsesOk("DESCRIBE DATABASE EXTENDED databasename");
  }

  @Test
  public void TestDescribe() {
    // Missing argument
    ParserError("DESCRIBE");
    ParserError("DESCRIBE FORMATTED");
    ParserError("DESCRIBE EXTENDED");

    // Unqualified table ok
    ParsesOk("DESCRIBE tablename");
    ParsesOk("DESCRIBE FORMATTED tablename");
    ParsesOk("DESCRIBE EXTENDED tablename");

    // Fully-qualified table ok
    ParsesOk("DESCRIBE databasename.tablename");
    ParsesOk("DESCRIBE FORMATTED databasename.tablename");
    ParsesOk("DESCRIBE EXTENDED databasename.tablename");

    // Paths within table ok.
    ParsesOk("DESCRIBE databasename.tablename.field1");
    ParsesOk("DESCRIBE databasename.tablename.field1.field2");
    ParsesOk("DESCRIBE FORMATTED databasename.tablename.field1");
    ParsesOk("DESCRIBE FORMATTED databasename.tablename.field1.field2");
    ParsesOk("DESCRIBE EXTENDED databasename.tablename.field1");
    ParsesOk("DESCRIBE EXTENDED databasename.tablename.field1.field2");
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
  public void TestCreateFunction() {
    ParsesOk("CREATE FUNCTION Foo() RETURNS INT LOCATION 'f.jar' SYMBOL='class.Udf'");
    ParsesOk("CREATE FUNCTION Foo(INT, INT) RETURNS STRING LOCATION " +
        "'f.jar' SYMBOL='class.Udf'");
    ParsesOk("CREATE FUNCTION Foo(INT, DOUBLE) RETURNS STRING LOCATION " +
        "'f.jar' SYMBOL='class.Udf'");
    ParsesOk("CREATE FUNCTION Foo() RETURNS STRING LOCATION " +
        "'f.jar' SYMBOL='class.Udf' COMMENT='hi'");
    ParsesOk("CREATE FUNCTION IF NOT EXISTS Foo() RETURNS INT LOCATION 'foo.jar' " +
        "SYMBOL='class.Udf'");
    ParsesOk("CREATE FUNCTION foo LOCATION 'f.jar' SYMBOL='class.Udf'");
    ParsesOk("CREATE FUNCTION db.foo LOCATION 'f.jar' SYMBOL='class.Udf'");
    ParsesOk("CREATE FUNCTION IF NOT EXISTS foo LOCATION 'f.jar' SYMBOL='class.Udf'");
    ParsesOk("CREATE FUNCTION IF NOT EXISTS db.foo LOCATION 'f.jar' SYMBOL='class.Udf'");

    // Try more interesting function names
    ParsesOk("CREATE FUNCTION User.Foo() RETURNS INT LOCATION 'a'");
    ParsesOk("CREATE FUNCTION `Foo`() RETURNS INT LOCATION 'a'");
    ParsesOk("CREATE FUNCTION `Foo.Bar`() RETURNS INT LOCATION 'a'");
    ParsesOk("CREATE FUNCTION `Foo`.Bar() RETURNS INT LOCATION 'a'");

    // Bad function name
    ParserError("CREATE FUNCTION User.() RETURNS INT LOCATION 'a'");
    ParserError("CREATE FUNCTION User.Foo.() RETURNS INT LOCATION 'a'");
    ParserError("CREATE FUNCTION User..Foo() RETURNS INT LOCATION 'a'");
    ParserError("CREATE FUNCTION Foo() LOCATION 'a.jar' SYMBOL='class.Udf");
    // Bad function name that parses but won't analyze.
    ParsesOk("CREATE FUNCTION A.B.C.D.Foo() RETURNS INT LOCATION 'a'");

    // Missing location
    ParserError("CREATE FUNCTION FOO() RETURNS INT");

    // Missing return type
    ParserError("CREATE FUNCTION FOO() LOCATION 'foo.jar'");

    // Bad opt args
    ParserError("CREATE FUNCTION Foo() RETURNS INT SYMBOL='1' LOCATION 'a'");
    ParserError("CREATE FUNCTION Foo() RETURNS INT LOCATION 'a' SYMBOL");
    ParserError("CREATE FUNCTION Foo() RETURNS INT LOCATION 'a' SYMBOL='1' SYMBOL='2'");
    ParserError("CREATE FUNCTION IF NOT EXISTS db.foo LOCATION 'f.jar' SYMBOL='1'" +
         " SYMBOL='2'");

    // Missing arguments
    ParserError("CREATE FUNCTION Foo RETURNS INT LOCATION 'f.jar'");
    ParserError("CREATE FUNCTION Foo(INT,) RETURNS INT LOCATION 'f.jar'");
    ParserError("CREATE FUNCTION FOO RETURNS INT LOCATION 'foo.jar'");

    // NULL return type or argument type.
    ParserError("CREATE FUNCTION Foo(NULL) RETURNS INT LOCATION 'f.jar'");
    ParserError("CREATE FUNCTION Foo(NULL, INT) RETURNS INT LOCATION 'f.jar'");
    ParserError("CREATE FUNCTION Foo(INT, NULL) RETURNS INT LOCATION 'f.jar'");
    ParserError("CREATE FUNCTION Foo() RETURNS NULL LOCATION 'f.jar'");
  }

  @Test
  public void TestVariadicCreateFunctions() {
    String fnCreates[] = {"CREATE FUNCTION ", "CREATE AGGREGATE FUNCTION " };
    for (String fnCreate: fnCreates) {
      ParsesOk(fnCreate + "Foo(int...) RETURNS INT LOCATION 'f.jar'");
      ParsesOk(fnCreate + "Foo(int ...) RETURNS INT LOCATION 'f.jar'");
      ParsesOk(fnCreate + "Foo(int, double ...) RETURNS INT LOCATION 'f.jar'");

      ParserError(fnCreate + "Foo(...) RETURNS INT LOCATION 'f.jar'");
      ParserError(fnCreate + "Foo(int..., double) RETURNS INT LOCATION 'f.jar'");
      ParserError(fnCreate + "Foo(int) RETURNS INT... LOCATION 'f.jar'");
      ParserError(fnCreate + "Foo(int. . .) RETURNS INT... LOCATION 'f.jar'");
      ParserError(fnCreate + "Foo(int, ...) RETURNS INT... LOCATION 'f.jar'");
    }
  }

  @Test
  public void TestCreateAggregate() {
    String loc = " LOCATION 'f.so' UPDATE_FN='class' ";
    String c = "CREATE AGGREGATE FUNCTION Foo() RETURNS INT ";

    ParsesOk(c + loc);
    ParsesOk(c + "INTERMEDIATE STRING" + loc + "comment='c'");
    ParsesOk(c + loc + "comment='abcd'");
    ParsesOk(c + loc + "init_fn='InitFnSymbol'");
    ParsesOk(c + loc + "init_fn='I' merge_fn='M'");
    ParsesOk(c + loc + "merge_fn='M' init_fn='I'");
    ParsesOk(c + loc + "merge_fn='M' Init_fn='I' serialize_fn='S' Finalize_fn='F'");
    ParsesOk(c + loc + "Init_fn='M' Finalize_fn='I' merge_fn='S' serialize_fn='F'");
    ParsesOk(c + loc + "merge_fn='M'");
    ParsesOk(c + "INTERMEDIATE CHAR(10)" + loc);

    ParserError("CREATE UNKNOWN FUNCTION " + "Foo() RETURNS INT" + loc);
    ParserError(c + loc + "init_fn='1' init_fn='1'");
    ParserError(c + loc + "unknown='1'");

    // CHAR must specify size
    ParserError(c + "INTERMEDIATE CHAR()" + loc);
    ParserError(c + "INTERMEDIATE CHAR(ab)" + loc);
    ParserError(c + "INTERMEDIATE CHAR('')" + loc);
    ParserError(c + "INTERMEDIATE CHAR('10')" + loc);
    ParserError(c + "INTERMEDIATE CHAR(-10)" + loc);
    // Parses okay, will fail in analysis
    ParsesOk(c + "INTERMEDIATE CHAR(0)" + loc);

    // Optional args must be at the end
    ParserError("CREATE UNKNOWN FUNCTION " + "Foo() RETURNS INT" + loc);
    ParserError("CREATE AGGREGATE FUNCTION Foo() init_fn='1' RETURNS INT" + loc);
    ParserError(c + "init_fn='1'" + loc);

    // Variadic args
    ParsesOk("CREATE AGGREGATE FUNCTION Foo(INT...) RETURNS INT LOCATION 'f.jar'");
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
      // Kudu column options
      ParsesOk(String.format("ALTER TABLE Foo %s COLUMNS (i int PRIMARY KEY NOT NULL " +
          "ENCODING RLE COMPRESSION SNAPPY BLOCK_SIZE 1024 DEFAULT 10, " +
          "j string NULL ENCODING PLAIN_ENCODING COMPRESSION LZ4 BLOCK_SIZE 10 " +
          "DEFAULT 'test')", addReplace));

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
    ParsesOk("ALTER TABLE Foo ADD PARTITION (i=abc, j=(5*8+10), k=!true and false)");

    // Multiple partition specs
    ParsesOk("ALTER TABLE Foo ADD PARTITION (i=1, s='one') " +
        "PARTITION (i=2, s='two') PARTITION (i=3, s='three')");
    ParsesOk("ALTER TABLE TestDb.Foo ADD PARTITION (i=1, s='one') LOCATION 'a/b' " +
        "PARTITION (i=2, s='two') LOCATION 'c/d' " +
        "PARTITION (i=3, s='three') " +
        "PARTITION (i=4, s='four') LOCATION 'e/f'");
    ParsesOk("ALTER TABLE TestDb.Foo ADD IF NOT EXISTS " +
        "PARTITION (i=1, s='one') " +
        "PARTITION (i=2, s='two') LOCATION 'c/d'");
    ParserError("ALTER TABLE TestDb.Foo ADD " +
        "PARTITION (i=1, s='one') " +
        "IF NOT EXISTS PARTITION (i=2, s='two') LOCATION 'c/d'");

    // Location needs to be a string literal
    ParserError("ALTER TABLE TestDb.Foo ADD PARTITION (i=1, s='Hello') LOCATION a/b");

    // Caching ops
    ParsesOk("ALTER TABLE Foo ADD PARTITION (j=2) CACHED IN 'pool'");
    ParserError("ALTER TABLE Foo ADD PARTITION (j=2) CACHED 'pool'");
    ParserError("ALTER TABLE Foo ADD PARTITION (j=2) CACHED IN");
    ParserError("ALTER TABLE Foo ADD PARTITION (j=2) CACHED");
    ParsesOk("ALTER TABLE Foo ADD PARTITION (j=2) CACHED IN 'pool' WITH replication = 3");
    ParserError("ALTER TABLE Foo ADD PARTITION (j=2) CACHED IN 'pool' " +
        "with replication = -1");
    ParsesOk("ALTER TABLE Foo ADD PARTITION (j=2) UNCACHED");
    ParsesOk("ALTER TABLE Foo ADD PARTITION (j=2) LOCATION 'a/b' UNCACHED");
    ParsesOk("ALTER TABLE Foo ADD PARTITION (j=2) LOCATION 'a/b' CACHED IN 'pool'");
    ParsesOk("ALTER TABLE Foo ADD PARTITION (j=2) LOCATION 'a/b' CACHED IN 'pool' " +
        "with replication = 3");
    ParserError("ALTER TABLE Foo ADD PARTITION (j=2) CACHED IN 'pool' LOCATION 'a/b'");
    ParserError("ALTER TABLE Foo ADD PARTITION (j=2) UNCACHED LOCATION 'a/b'");

    // Multiple partition specs with caching ops
    ParsesOk("ALTER TABLE Foo ADD PARTITION (j=2) CACHED IN 'pool' " +
        "PARTITION (j=3) UNCACHED " +
        "PARTITION (j=4) CACHED IN 'pool' WITH replication = 3 " +
        "PARTITION (j=5) LOCATION 'a/b' CACHED IN 'pool' " +
        "PARTITION (j=5) LOCATION 'c/d' CACHED IN 'pool' with replication = 3");

    ParserError("ALTER TABLE Foo ADD IF EXISTS PARTITION (i=1, s='Hello')");
    ParserError("ALTER TABLE TestDb.Foo ADD (i=1, s='Hello')");
    ParserError("ALTER TABLE TestDb.Foo ADD (i=1)");
    ParserError("ALTER TABLE Foo (i=1)");
    ParserError("ALTER TABLE TestDb.Foo PARTITION (i=1)");
    ParserError("ALTER TABLE Foo ADD PARTITION");
    ParserError("ALTER TABLE TestDb.Foo ADD PARTITION ()");
    ParserError("ALTER Foo ADD PARTITION (i=1)");
    ParserError("ALTER TABLE ADD PARTITION (i=1)");
    ParserError("ALTER TABLE ADD");
    ParserError("ALTER TABLE DROP");

    // Kudu range partitions
    String[] ifNotExistsOption = {"IF NOT EXISTS", ""};
    for (String option: ifNotExistsOption) {
      ParsesOk(String.format("ALTER TABLE Foo ADD %s RANGE PARTITION 10 < VALUES < 20",
          option));
      ParsesOk(String.format("ALTER TABLE Foo ADD %s RANGE PARTITION VALUE = 100",
          option));
      ParserError(String.format("ALTER TABLE Foo ADD %s RANGE PARTITION 10 < VALUES " +
          "<= 20, PARTITION 20 < VALUES <= 30", option));
      ParserError(String.format("ALTER TABLE Foo ADD %s (RANGE PARTITION 10 < VALUES " +
          "<= 20)", option));
    }
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
    // PURGE is optional
    String[] purgeKw = {"PURGE", ""};
    for (String kw: purgeKw) {
      ParsesOk(String.format("ALTER TABLE Foo DROP PARTITION (i=1) %s", kw));
      ParsesOk(String.format("ALTER TABLE TestDb.Foo DROP IF EXISTS "
        + "PARTITION (i=1, s='Hello') %s", kw));
      ParsesOk(String.format("ALTER TABLE Foo DROP PARTITION (i=NULL) %s", kw));
      ParsesOk(String.format("ALTER TABLE Foo DROP PARTITION (i=NULL, "
        + "j=2, k=NULL) %s", kw));
      ParsesOk(String.format("ALTER TABLE Foo DROP PARTITION (i=abc, "
        + "j=(5*8+10), k=!true and false) %s", kw));

      ParserError(String.format("ALTER TABLE Foo DROP IF NOT EXISTS "
        + "PARTITION (i=1, s='Hello') %s", kw));
      ParserError(String.format("ALTER TABLE TestDb.Foo DROP (i=1, s='Hello') %s", kw));
      ParserError(String.format("ALTER TABLE TestDb.Foo DROP (i=1) %s", kw));
      ParserError(String.format("ALTER TABLE Foo (i=1) %s", kw));
      ParserError(String.format("ALTER TABLE TestDb.Foo PARTITION (i=1) %s", kw));
      ParserError(String.format("ALTER TABLE Foo DROP PARTITION %s", kw));
      ParserError(String.format("ALTER TABLE TestDb.Foo DROP PARTITION () %s", kw));
      ParserError(String.format("ALTER Foo DROP PARTITION (i=1) %s", kw));
      ParserError(String.format("ALTER TABLE DROP PARTITION (i=1) %s", kw));
    }

    // Kudu range partitions
    String[] ifExistsOption = {"IF EXISTS", ""};
    for (String option: ifExistsOption) {
      ParsesOk(String.format("ALTER TABLE Foo DROP %s RANGE PARTITION 10 < VALUES < 20",
          option));
      ParsesOk(String.format("ALTER TABLE Foo DROP %s RANGE PARTITION VALUE = 100",
          option));
      ParserError(String.format("ALTER TABLE Foo DROP %s RANGE PARTITION 10 < VALUES " +
          "<= 20, PARTITION 20 < VALUES <= 30", option));
      ParserError(String.format("ALTER TABLE Foo DROP %s (RANGE PARTITION 10 < VALUES " +
          "<= 20)", option));
      ParserError(String.format("ALTER TABLE Foo DROP %s RANGE PARTITION VALUE = 100 " +
          "PURGE", option));
    }
  }

  @Test
  public void TestAlterTableChangeColumn() {
    // KW_COLUMN is optional
    String[] columnKw = {"COLUMN", ""};
    for (String kw: columnKw) {
      ParsesOk(String.format("ALTER TABLE Foo.Bar CHANGE %s c1 c2 int", kw));
      ParsesOk(String.format("ALTER TABLE Foo CHANGE %s c1 c2 int comment 'hi'", kw));
      // Kudu column options
      ParsesOk(String.format("ALTER TABLE Foo CHANGE %s c1 c2 int comment 'hi' " +
          "NULL ENCODING PLAIN_ENCODING COMPRESSION LZ4 DEFAULT 10 BLOCK_SIZE 1024", kw));

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
    String [] supportedFileFormats =
        {"TEXTFILE", "SEQUENCEFILE", "PARQUET", "PARQUETFILE", "RCFILE", "AVRO"};
    for (String format: supportedFileFormats) {
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

    ParserError("ALTER TABLE Foo PARTITION () SET LOCATION '/a'");
    ParserError("ALTER TABLE Foo PARTITION () SET FILEFORMAT PARQUETFILE");
    ParserError("ALTER TABLE Foo PARTITION (,) SET FILEFORMAT PARQUET");
    ParserError("ALTER TABLE Foo PARTITION (a=1) SET FILEFORMAT");
    ParserError("ALTER TABLE Foo PARTITION (a=1) SET LOCATION");
    ParserError("ALTER TABLE TestDb.Foo SET LOCATION abc");
    ParserError("ALTER TABLE TestDb.Foo SET LOCATION");
    ParserError("ALTER TABLE TestDb.Foo SET");

    ParsesOk("ALTER TABLE Foo SET ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    ParsesOk("ALTER TABLE Foo SET ROW FORMAT DELIMITED LINES TERMINATED BY '\n'");
    ParsesOk("ALTER TABLE Foo SET ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
        "LINES TERMINATED BY '\n'");
    ParsesOk("ALTER TABLE Foo SET ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
        "ESCAPED BY '\' LINES TERMINATED BY '\n'");
    ParsesOk("ALTER TABLE Foo PARTITION (i=1) SET ROW FORMAT DELIMITED " +
        "FIELDS TERMINATED BY ','");
    ParsesOk("ALTER TABLE Foo PARTITION (i=1) SET ROW FORMAT DELIMITED " +
        "LINES TERMINATED BY '\n'");
    ParsesOk("ALTER TABLE Foo PARTITION (i=1) SET ROW FORMAT DELIMITED " +
        "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'");
    ParsesOk("ALTER TABLE Foo PARTITION (i=1) SET ROW FORMAT DELIMITED " +
        "FIELDS TERMINATED BY ',' ESCAPED BY '\' LINES TERMINATED BY '\n'");
    ParserError("ALTER TABLE Foo SET ROW FORMAT");
    ParserError("ALTER TABLE Foo SET ROW FORMAT DELIMITED FIELDS");
    ParserError("ALTER TABLE Foo PARTITION () SET ROW FORMAT DELIMITED " +
        "FIELDS TERMINATED BY ','");
    ParserError("ALTER TABLE Foo PARTITION (i=1) SET ROW FORMAT");

    String[] tblPropTypes = {"TBLPROPERTIES", "SERDEPROPERTIES"};
    String[] partClauses = {"", "PARTITION(k1=10, k2=20)"};
    for (String propType: tblPropTypes) {
      for (String part: partClauses) {
        ParsesOk(String.format("ALTER TABLE Foo %s SET %s ('a'='b')", part, propType));
        ParsesOk(String.format("ALTER TABLE Foo %s SET %s ('abc'='123')",
            part, propType));
        ParsesOk(String.format("ALTER TABLE Foo %s SET %s ('abc'='123', 'a'='1')",
            part, propType));
        ParsesOk(String.format("ALTER TABLE Foo %s SET %s ('a'='1', 'b'='2', 'c'='3')",
            part, propType));
        ParserError(String.format("ALTER TABLE Foo %s SET %s ( )", part, propType));
        ParserError(String.format("ALTER TABLE Foo %s SET %s ('a', 'b')",
            part, propType));
        ParserError(String.format("ALTER TABLE Foo %s SET %s ('a'='b',)",
            part, propType));
        ParserError(String.format("ALTER TABLE Foo %s SET %s ('a'=b)", part, propType));
        ParserError(String.format("ALTER TABLE Foo %s SET %s (a='b')", part, propType));
        ParserError(String.format("ALTER TABLE Foo %s SET %s (a=b)", part, propType));
      }
    }

    // Test SET COLUMN STATS.
    ParsesOk("ALTER TABLE Foo SET COLUMN STATS col ('numDVs'='10')");
    ParsesOk("ALTER TABLE Foo SET COLUMN STATS col ('numDVs'='10','maxSize'='20')");
    ParsesOk("ALTER TABLE TestDb.Foo SET COLUMN STATS col ('avgSize'='20')");
    ParserError("ALTER TABLE SET COLUMN STATS col ('numDVs'='10'");
    ParserError("ALTER TABLE Foo SET COLUMN STATS ('numDVs'='10'");
    ParserError("ALTER TABLE Foo SET COLUMN STATS col");
    ParserError("ALTER TABLE Foo SET COLUMN STATS col ()");
    ParserError("ALTER TABLE Foo SET COLUMN STATS col (numDVs='10')");
    ParserError("ALTER TABLE Foo SET COLUMN STATS col ('numDVs'=10)");
    ParserError("ALTER TABLE Foo PARTITION (p=1) SET COLUMN STATS col ('avgSize'='20')");

    for (String cacheClause: Lists.newArrayList("UNCACHED", "CACHED in 'pool'",
        "CACHED in 'pool' with replication = 4")) {
      ParsesOk("ALTER TABLE Foo SET " + cacheClause);
      ParsesOk("ALTER TABLE Foo PARTITION(j=0) SET " + cacheClause);
      ParserError("ALTER TABLE Foo PARTITION(j=0) " + cacheClause);
    }
  }

  @Test
  public void TestAlterTableSortBy() {
    ParsesOk("ALTER TABLE TEST SORT BY (int_col, id)");
    ParsesOk("ALTER TABLE TEST SORT BY ()");
    ParserError("ALTER TABLE TEST PARTITION (year=2009, month=4) SORT BY (int_col, id)");
  }

  @Test
  public void TestAlterTableOrViewRename() {
    for (String entity: Lists.newArrayList("TABLE", "VIEW")) {
      ParsesOk(String.format("ALTER %s TestDb.Foo RENAME TO TestDb.Foo2", entity));
      ParsesOk(String.format("ALTER %s Foo RENAME TO TestDb.Foo2", entity));
      ParsesOk(String.format("ALTER %s TestDb.Foo RENAME TO Foo2", entity));
      ParsesOk(String.format("ALTER %s Foo RENAME TO Foo2", entity));
      ParserError(String.format("ALTER %s Foo RENAME TO 'Foo2'", entity));
      ParserError(String.format("ALTER %s Foo RENAME Foo2", entity));
      ParserError(String.format("ALTER %s Foo RENAME TO", entity));
      ParserError(String.format("ALTER %s Foo TO Foo2", entity));
      ParserError(String.format("ALTER %s Foo TO Foo2", entity));
    }
  }

  @Test
  public void TestAlterTableRecoverPartitions() {
    ParsesOk("ALTER TABLE TestDb.Foo RECOVER PARTITIONS");
    ParsesOk("ALTER TABLE Foo RECOVER PARTITIONS");

    ParserError("ALTER TABLE Foo RECOVER PARTITIONS ()");
    ParserError("ALTER TABLE Foo RECOVER PARTITIONS (i=1)");
    ParserError("ALTER TABLE RECOVER");
    ParserError("ALTER TABLE RECOVER PARTITIONS");
    ParserError("ALTER TABLE Foo RECOVER");
    ParserError("ALTER TABLE Foo RECOVER PARTITION");
  }

  @Test
  public void TestAlterTableAlterColumn() {
    for (String column : Lists.newArrayList("", "COLUMN")) {
      ParsesOk(String.format("ALTER TABLE foo ALTER %s bar SET default 0", column));
      ParsesOk(String.format(
          "ALTER TABLE foo ALTER %s bar SET default 0 block_size 0", column));
      ParsesOk(String.format("ALTER TABLE foo ALTER %s bar DROP default", column));

      ParserError(String.format("ALTER TABLE foo ALTER %s bar", column));
      ParserError(String.format("ALTER TABLE foo ALTER %s bar SET default", column));
      ParserError(String.format("ALTER TABLE foo ALTER %s bar SET error 0", column));
      ParserError(
          String.format("ALTER TABLE foo ALTER %s bar SET default 0 error 0", column));
      ParserError(String.format("ALTER TABLE foo ALTER %s bar DROP comment", column));
    }
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
    ParsesOk("CREATE TABLE Foo LIKE PARQUET '/user/foo'");

    // Table and column names starting with digits.
    ParsesOk("CREATE TABLE 01_Foo (01_i int, 02_j string)");

    // Unpartitioned tables
    ParsesOk("CREATE TABLE Foo (i int, s string)");
    ParsesOk("CREATE EXTERNAL TABLE Foo (i int, s string)");
    ParsesOk("CREATE EXTERNAL TABLE Foo (i int, s string) LOCATION '/test-warehouse/'");
    ParsesOk("CREATE TABLE Foo (i int, s string) COMMENT 'hello' LOCATION '/a/b/'");
    ParsesOk("CREATE TABLE Foo (i int, s string) COMMENT 'hello' LOCATION '/a/b/' " +
        "TBLPROPERTIES ('123'='1234')");
    // No column definitions.
    ParsesOk("CREATE TABLE Foo COMMENT 'hello' LOCATION '/a/b/' " +
        "TBLPROPERTIES ('123'='1234')");

    // Partitioned tables
    ParsesOk("CREATE TABLE Foo (i int) PARTITIONED BY (j string)");
    ParsesOk("CREATE TABLE Foo (i int) PARTITIONED BY (s string, d double)");
    ParsesOk("CREATE TABLE Foo (i int, s string) PARTITIONED BY (s string, d double)" +
        " COMMENT 'hello' LOCATION '/a/b/'");
    // No column definitions.
    ParsesOk("CREATE TABLE Foo PARTITIONED BY (s string, d double)" +
        " COMMENT 'hello' LOCATION '/a/b/'");
    ParserError("CREATE TABLE Foo (i int) PARTITIONED BY (int)");
    ParserError("CREATE TABLE Foo (i int) PARTITIONED BY ()");
    ParserError("CREATE TABLE Foo (i int) PARTITIONED BY");

    // Sort by clause
    ParsesOk("CREATE TABLE Foo (i int, j int) SORT BY ()");
    ParsesOk("CREATE TABLE Foo (i int) SORT BY (i)");
    ParsesOk("CREATE TABLE Foo (i int) SORT BY (j)");
    ParsesOk("CREATE TABLE Foo (i int, j int) SORT BY (i,j)");
    ParsesOk("CREATE EXTERNAL TABLE Foo (i int, s string) SORT BY (s) " +
        "LOCATION '/test-warehouse/'");
    ParsesOk("CREATE TABLE Foo (i int, s string) SORT BY (s) COMMENT 'hello' " +
        "LOCATION '/a/b/' TBLPROPERTIES ('123'='1234')");

    // SORT BY must be the first table option
    ParserError("CREATE TABLE Foo (i int, s string) COMMENT 'hello' SORT BY (s) " +
        "LOCATION '/a/b/' TBLPROPERTIES ('123'='1234')");
    ParserError("CREATE TABLE Foo (i int, s string) COMMENT 'hello' LOCATION '/a/b/' " +
        "SORT BY (s) TBLPROPERTIES ('123'='1234')");
    ParserError("CREATE TABLE Foo (i int, s string) COMMENT 'hello' LOCATION '/a/b/' " +
        "TBLPROPERTIES ('123'='1234') SORT BY (s)");

    // Malformed SORT BY clauses
    ParserError("CREATE TABLE Foo (i int, j int) SORT BY");
    ParserError("CREATE TABLE Foo (i int, j int) SORT BY (i,)");
    ParserError("CREATE TABLE Foo (i int, j int) SORT BY (int)");

    // Create table like other table with sort columns
    ParsesOk("CREATE TABLE Foo SORT BY(bar) LIKE Baz STORED AS TEXTFILE LOCATION '/a/b'");
    ParserError("CREATE TABLE SORT BY(bar) Foo LIKE Baz STORED AS TEXTFILE " +
        "LOCATION '/a/b'");
    // SORT BY must be the first table option
    ParserError("CREATE TABLE Foo LIKE Baz STORED AS TEXTFILE LOCATION '/a/b' " +
        "SORT BY(bar)");

    // CTAS with sort columns
    ParsesOk("CREATE TABLE Foo SORT BY(bar) AS SELECT * FROM BAR");
    ParserError("CREATE TABLE Foo AS SELECT * FROM BAR SORT BY(bar)");

    // Create table like file with sort columns
    ParsesOk("CREATE TABLE Foo LIKE PARQUET '/user/foo' SORT BY (id)");
    ParserError("CREATE TABLE Foo SORT BY (id) LIKE PARQUET '/user/foo'");

    // Column comments
    ParsesOk("CREATE TABLE Foo (i int COMMENT 'hello', s string)");
    ParsesOk("CREATE TABLE Foo (i int COMMENT 'hello', s string COMMENT 'hi')");
    ParsesOk("CREATE TABLE T (i int COMMENT 'hi') PARTITIONED BY (j int COMMENT 'bye')");

    // Supported file formats
    String [] supportedFileFormats =
        {"TEXTFILE", "SEQUENCEFILE", "PARQUET", "PARQUETFILE", "RCFILE", "AVRO"};
    for (String format: supportedFileFormats) {
      ParsesOk("CREATE TABLE Foo (i int, s string) STORED AS " + format);
      ParsesOk("CREATE EXTERNAL TABLE Foo (i int, s string) STORED AS " + format);
      ParsesOk(String.format(
          "CREATE TABLE Foo (i int, s string) STORED AS %s LOCATION '/b'", format));
      ParsesOk(String.format(
          "CREATE EXTERNAL TABLE Foo (f float) COMMENT 'c' STORED AS %s LOCATION '/b'",
          format));
      // No column definitions.
      ParsesOk(String.format(
          "CREATE EXTERNAL TABLE Foo COMMENT 'c' STORED AS %s LOCATION '/b'", format));
      ParserError(String.format("CREATE EXTERNAL TABLE t PRIMARY KEYS (i) STORED AS " +
          "%s", format));
    }

    ParsesOk("CREATE TABLE foo (i INT) STORED AS KUDU");
    ParsesOk("CREATE TABLE foo (i INT PRIMARY KEY) STORED AS KUDU");
    ParsesOk("CREATE TABLE foo (i INT, j INT, PRIMARY KEY (i, j)) STORED AS KUDU");
    ParsesOk("CREATE TABLE foo (i INT, j INT, PRIMARY KEY (j, i)) STORED AS KUDU");
    ParsesOk("CREATE TABLE foo (i INT PRIMARY KEY, PRIMARY KEY(i)) STORED AS KUDU");
    ParsesOk("CREATE TABLE foo (i INT PRIMARY KEY, j INT PRIMARY KEY) STORED AS KUDU");
    ParserError("CREATE TABLE foo (i INT) PRIMARY KEY (i) STORED AS KUDU");
    ParserError("CREATE TABLE foo (i INT, PRIMARY KEY) STORED AS KUDU");
    ParserError("CREATE TABLE foo (PRIMARY KEY(a), a INT) STORED AS KUDU");
    ParserError("CREATE TABLE foo (i INT) PRIMARY KEY (i) STORED AS KUDU");

    // Table Properties
    String[] tblPropTypes = {"TBLPROPERTIES", "WITH SERDEPROPERTIES"};
    for (String propType: tblPropTypes) {
      ParsesOk(String.format(
          "CREATE TABLE Foo (i int) %s ('a'='b', 'c'='d', 'e'='f')", propType));
      ParserError(String.format("CREATE TABLE Foo (i int) %s", propType));
      ParserError(String.format("CREATE TABLE Foo (i int) %s ()", propType));
      ParserError(String.format("CREATE TABLE Foo (i int) %s ('a')", propType));
      ParserError(String.format("CREATE TABLE Foo (i int) %s ('a'=)", propType));
      ParserError(String.format("CREATE TABLE Foo (i int) %s ('a'=c)", propType));
      ParserError(String.format("CREATE TABLE Foo (i int) %s (a='c')", propType));
    }
    ParsesOk("CREATE TABLE Foo (i int) WITH SERDEPROPERTIES ('a'='b') " +
        "TBLPROPERTIES ('c'='d', 'e'='f')");
    // TBLPROPERTIES must go after SERDEPROPERTIES
    ParserError("CREATE TABLE Foo (i int) TBLPROPERTIES ('c'='d', 'e'='f') " +
        "WITH SERDEPROPERTIES ('a'='b')");

    ParserError("CREATE TABLE Foo (i int) SERDEPROPERTIES ('a'='b')");

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

    // Order should be: [comment] [partition by cols] [row format] [serdeproperties (..)]
    // [stored as FILEFORMAT] [location] [cache spec] [tblproperties (...)]
    ParserError("CREATE TABLE Foo (d double) COMMENT 'c' PARTITIONED BY (i int)");
    ParserError("CREATE TABLE Foo (d double) STORED AS TEXTFILE COMMENT 'c'");
    ParserError("CREATE TABLE Foo (d double) STORED AS TEXTFILE ROW FORMAT DELIMITED");
    ParserError("CREATE TABLE Foo (d double) ROW FORMAT DELIMITED COMMENT 'c'");
    ParserError("CREATE TABLE Foo (d double) LOCATION 'a' COMMENT 'c'");
    ParserError("CREATE TABLE Foo (d double) UNCACHED LOCATION '/a/b'");
    ParserError("CREATE TABLE Foo (d double) CACHED IN 'pool' LOCATION '/a/b'");
    ParserError("CREATE TABLE Foo (d double) CACHED IN 'pool' REPLICATION = 8 " +
        "LOCATION '/a/b'");
    ParserError("CREATE TABLE Foo (d double) LOCATION 'a' COMMENT 'c' STORED AS RCFILE");
    ParserError("CREATE TABLE Foo (d double) LOCATION 'a' STORED AS RCFILE");
    ParserError("CREATE TABLE Foo (d double) TBLPROPERTIES('a'='b') LOCATION 'a'");
    ParserError("CREATE TABLE Foo (i int) LOCATION 'a' WITH SERDEPROPERTIES('a'='b')");

    // Location and comment need to be string literals, file format is not
    ParserError("CREATE TABLE Foo (d double) LOCATION a");
    ParserError("CREATE TABLE Foo (d double) COMMENT c");
    ParserError("CREATE TABLE Foo (d double COMMENT c)");
    ParserError("CREATE TABLE Foo (d double COMMENT 'c') PARTITIONED BY (j COMMENT hi)");
    ParserError("CREATE TABLE Foo (d double) STORED AS 'TEXTFILE'");

    // Caching
    ParsesOk("CREATE TABLE Foo (i int) CACHED IN 'myPool'");
    ParsesOk("CREATE TABLE Foo (i int) CACHED IN 'myPool' WITH REPLICATION = 4");
    ParsesOk("CREATE TABLE Foo (i int) PARTITIONED BY(j int) CACHED IN 'myPool'");
    ParsesOk("CREATE TABLE Foo (i int) PARTITIONED BY(j int) CACHED IN 'myPool'" +
        " WITH REPLICATION = 4");
    ParsesOk("CREATE TABLE Foo (i int) PARTITIONED BY(j int) CACHED IN 'myPool'");
    ParsesOk("CREATE TABLE Foo (i int) PARTITIONED BY(j int) LOCATION '/a' " +
        "CACHED IN 'myPool'");
    ParserError("CREATE TABLE Foo (i int) CACHED IN myPool");
    ParserError("CREATE TABLE Foo (i int) PARTITIONED BY(j int) CACHED IN");
    ParserError("CREATE TABLE Foo (i int) CACHED 'myPool'");
    ParserError("CREATE TABLE Foo (i int) IN 'myPool'");
    ParserError("CREATE TABLE Foo (i int) PARTITIONED BY(j int) CACHED IN 'myPool' " +
        "LOCATION '/a'");
    ParserError("CREATE TABLE Foo (i int) CACHED IN 'myPool' WITH REPLICATION = -1");
    ParserError("CREATE TABLE Foo (i int) CACHED IN 'myPool' WITH REPLICATION = 1.0");
    ParserError("CREATE TABLE Foo (i int) CACHED IN 'myPool' " +
        "WITH REPLICATION = cast(1 as double)");

    // Invalid syntax
    ParserError("CREATE TABLE IF EXISTS Foo.Bar (i int)");
    ParserError("CREATE TABLE Bar LIKE Bar2 (i int)");
    ParserError("CREATE IF NOT EXISTS TABLE Foo.Bar (i int)");
    ParserError("CREATE TABLE Foo (d double) STORED TEXTFILE");
    ParserError("CREATE TABLE Foo (d double) AS TEXTFILE");
    ParserError("CREATE TABLE Foo i int");
    ParserError("CREATE TABLE Foo (i intt)");
    ParserError("CREATE TABLE Foo (int i)");
    ParserError("CREATE TABLE Foo (i int,)");
    ParserError("CREATE TABLE Foo ()");
    ParserError("CREATE TABLE");
    ParserError("CREATE EXTERNAL");
    ParserError("CREATE");

    // Valid syntax for tables PRODUCED BY DATA SOURCE
    ParsesOk("CREATE TABLE Foo (i int, s string) PRODUCED BY DATA SOURCE Bar");
    ParsesOk("CREATE TABLE Foo (i int, s string) PRODUCED BY DATA SOURCE Bar(\"\")");
    ParsesOk("CREATE TABLE Foo (i int) PRODUCED BY DATA SOURCE " +
        "Bar(\"Foo \\!@#$%^&*()\")");
    ParsesOk("CREATE TABLE IF NOT EXISTS Foo (i int) PRODUCED BY DATA SOURCE Bar(\"\")");

    // Invalid syntax for tables PRODUCED BY DATA SOURCE
    ParserError("CREATE TABLE Foo (i int) PRODUCED BY DATA Foo");
    ParserError("CREATE TABLE Foo (i int) PRODUCED BY DATA SRC Foo");
    ParserError("CREATE TABLE Foo (i int) PRODUCED BY DATA SOURCE Foo.Bar");
    ParserError("CREATE TABLE Foo (i int) PRODUCED BY DATA SOURCE Foo()");
    ParserError("CREATE EXTERNAL TABLE Foo (i int) PRODUCED BY DATA SOURCE Foo(\"\")");
    ParserError("CREATE TABLE Foo (i int) PRODUCED BY DATA SOURCE Foo(\"\") " +
        "LOCATION 'x'");
    ParserError("CREATE TABLE Foo (i int) PRODUCED BY DATA SOURCE Foo(\"\") " +
        "ROW FORMAT DELIMITED");
    ParserError("CREATE TABLE Foo (i int) PARTITIONED BY (j string) PRODUCED BY DATA " +
        "SOURCE Foo(\"\")");


    // Flexible partitioning
    ParsesOk("CREATE TABLE Foo (i int) PARTITION BY HASH(i) PARTITIONS 4");
    ParsesOk("CREATE TABLE Foo (i int) PARTITION BY HASH(i) PARTITIONS 4, " +
        "HASH(a) PARTITIONS 2");
    ParsesOk("CREATE TABLE Foo (i int) PARTITION BY HASH PARTITIONS 4");
    ParsesOk("CREATE TABLE Foo (i int, k int) PARTITION BY HASH PARTITIONS 4," +
        " HASH(k) PARTITIONS 4");
    ParserError("CREATE TABLE Foo (i int) PARTITION BY HASH(i)");
    ParserError("CREATE EXTERNAL TABLE Foo PARTITION BY HASH PARTITIONS 4");

    // Range partitioning
    ParsesOk("CREATE TABLE Foo (i int) PARTITION BY RANGE (PARTITION VALUE = 10)");
    ParsesOk("CREATE TABLE Foo (i int) PARTITION BY RANGE(i) " +
        "(PARTITION 1 <= VALUES < 10, PARTITION 10 <= VALUES < 20, " +
        "PARTITION 21 < VALUES <= 30, PARTITION VALUE = 50)");
    ParsesOk("CREATE TABLE Foo (a int) PARTITION BY RANGE(a) " +
        "(PARTITION 10 <= VALUES)");
    ParsesOk("CREATE TABLE Foo (a int) PARTITION BY RANGE(a) " +
        "(PARTITION VALUES < 10)");
    ParsesOk("CREATE TABLE Foo (a int) PARTITION BY RANGE (a) " +
        "(PARTITION VALUE = 10, PARTITION VALUE = 20)");
    ParsesOk("CREATE TABLE Foo (a int) PARTITION BY RANGE(a) " +
        "(PARTITION VALUES <= 10, PARTITION VALUE = 20)");
    ParsesOk("CREATE TABLE Foo (a int, b int) PARTITION BY RANGE(a, b) " +
        "(PARTITION VALUE = (2001, 1), PARTITION VALUE = (2001, 2), " +
        "PARTITION VALUE = (2002, 1))");
    ParsesOk("CREATE TABLE Foo (a int, b string) PARTITION BY " +
        "HASH (a) PARTITIONS 3, RANGE (a, b) (PARTITION VALUE = (1, 'abc'), " +
        "PARTITION VALUE = (2, 'def'))");
    ParsesOk("CREATE TABLE Foo (a int) PARTITION BY RANGE (a) " +
        "(PARTITION VALUE = 1 + 1) STORED AS KUDU");
    ParsesOk("CREATE TABLE Foo (a int) PARTITION BY RANGE (a) " +
        "(PARTITION 1 + 1 < VALUES) STORED AS KUDU");
    ParsesOk("CREATE TABLE Foo (a int, b int) PARTITION BY RANGE (a) " +
        "(PARTITION b < VALUES <= a) STORED AS KUDU");
    ParsesOk("CREATE TABLE Foo (a int) PARTITION BY RANGE (a) " +
        "(PARTITION now() <= VALUES, PARTITION VALUE = add_months(now(), 2)) " +
        "STORED AS KUDU");

    ParserError("CREATE TABLE Foo (a int) PARTITION BY RANGE (a) ()");
    ParserError("CREATE TABLE Foo (a int) PARTITION BY HASH (a) PARTITIONS 4, " +
        "RANGE (a) (PARTITION VALUE = 10), RANGE (a) (PARTITION VALUES < 10)");
    ParserError("CREATE TABLE Foo (a int) PARTITION BY RANGE (a) " +
        "(PARTITION VALUE = 10), HASH (a) PARTITIONS 3");
    ParserError("CREATE TABLE Foo (a int) PARTITION BY RANGE (a) " +
        "(PARTITION VALUES = 10) STORED AS KUDU");
    ParserError("CREATE TABLE Foo (a int) PARTITION BY RANGE (a) " +
        "(PARTITION 10 < VALUE < 20) STORED AS KUDU");

    // Column options for Kudu tables
    String[] encodings = {"encoding auto_encoding", "encoding plain_encoding",
        "encoding prefix_encoding", "encoding group_varint", "encoding rle",
        "encoding dict_encoding", "encoding bit_shuffle", "encoding unknown", ""};
    String[] compression = {"compression default_compression",
        "compression no_compression", "compression snappy", "compression lz4",
        "compression zlib", "compression unknown", ""};

    String[] nullability = {"not null", "null", ""};
    String[] defaultVal = {"default 10", ""};
    String[] blockSize = {"block_size 4096", ""};
    for (String enc: encodings) {
      for (String comp: compression) {
        for (String nul: nullability) {
          for (String def: defaultVal) {
            for (String block: blockSize) {
              ParsesOk(String.format("CREATE TABLE Foo (i int PRIMARY KEY " +
                  "%s %s %s %s %s) STORED AS KUDU", nul, enc, comp, def, block));
              ParsesOk(String.format("CREATE TABLE Foo (i int PRIMARY KEY " +
                  "%s %s %s %s %s) STORED AS KUDU", block, nul, enc, comp, def));
              ParsesOk(String.format("CREATE TABLE Foo (i int PRIMARY KEY " +
                  "%s %s %s %s %s) STORED AS KUDU", def, block, nul, enc, comp));
              ParsesOk(String.format("CREATE TABLE Foo (i int PRIMARY KEY " +
                  "%s %s %s %s %s) STORED AS KUDU", comp, def, block, nul, enc));
              ParsesOk(String.format("CREATE TABLE Foo (i int PRIMARY KEY " +
                  "%s %s %s %s %s) STORED AS KUDU", enc, comp, def, block, nul));
              ParsesOk(String.format("CREATE TABLE Foo (i int PRIMARY KEY " +
                  "%s %s %s %s %s) STORED AS KUDU", enc, comp, block, def, nul));
            }
          }
        }
      }
    }
    // Column option is specified multiple times for the same column
    ParserError("CREATE TABLE Foo(a int PRIMARY KEY ENCODING RLE ENCODING PLAIN) " +
        "STORED AS KUDU");
    // Constant expr used in DEFAULT
    ParsesOk("CREATE TABLE Foo(a int PRIMARY KEY, b int DEFAULT 1+1) STORED AS KUDU");
    ParsesOk("CREATE TABLE Foo(a int PRIMARY KEY, b float DEFAULT cast(1.1 as float)) " +
        "STORED AS KUDU");
    // Non-literal value used in BLOCK_SIZE
    ParserError("CREATE TABLE Foo(a int PRIMARY KEY, b int BLOCK_SIZE 1+1) " +
        "STORED AS KUDU");
    ParserError("CREATE TABLE Foo(a int PRIMARY KEY BLOCK_SIZE -1) STORED AS KUDU");
  }

  @Test
  public void TestCreateDataSource() {
    ParsesOk("CREATE DATA SOURCE foo LOCATION '/foo.jar' CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");
    ParsesOk("CREATE DATA SOURCE foo LOCATION \"/foo.jar\" CLASS \"com.bar.Foo\" " +
        "API_VERSION \"V1\"");
    ParsesOk("CREATE DATA SOURCE foo LOCATION '/x/foo@hi_^!#.jar' CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");

    ParserError("CREATE DATA foo LOCATION '/foo.jar' CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");
    ParserError("CREATE DATA SRC foo.bar LOCATION '/foo.jar' CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo.bar LOCATION '/x/foo.jar' CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo LOCATION /x/foo.jar CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo LOCATION '/x/foo.jar' CLASS com.bar.Foo " +
        "API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo LOCATION '/x/foo.jar' CLASS 'com.bar.Foo' " +
        "API_VERSION V1");
    ParserError("CREATE DATA SOURCE LOCATION '/x/foo.jar' CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo LOCATION CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo LOCATION '/foo.jar' API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo LOCATION '/foo.jar' CLASS API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo LOCATION '/foo.jar' CLASS 'com.bar.Foo'");
    ParserError("CREATE DATA SOURCE foo LOCATION '/foo.jar' CLASS 'Foo' API_VERSION");
    ParserError("CREATE DATA SOURCE foo CLASS 'com.bar.Foo' LOCATION '/x/foo.jar' " +
        "API_VERSION 'V1'");
    ParserError("CREATE DATA SOURCE foo CLASS 'com.bar.Foo' API_VERSION 'V1' " +
        "LOCATION '/x/foo.jar' ");
    ParserError("CREATE DATA SOURCE foo API_VERSION 'V1' LOCATION '/x/foo.jar' " +
        "CLASS 'com.bar.Foo'");
  }

  @Test
  public void TestDropDataSource() {
    ParsesOk("DROP DATA SOURCE foo");

    ParserError("DROP DATA foo");
    ParserError("DROP DATA SRC foo");
    ParserError("DROP DATA SOURCE foo.bar");
    ParserError("DROP DATA SOURCE");
  }

  @Test
  public void TestCreateView() {
    ParsesOk("CREATE VIEW Bar AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW Bar COMMENT 'test' AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW Bar (x, y, z) AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW Bar (x, y COMMENT 'foo', z) AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW Bar (x, y, z) COMMENT 'test' AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW IF NOT EXISTS Bar AS SELECT a, b, c from t");

    ParsesOk("CREATE VIEW Foo.Bar AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW Foo.Bar COMMENT 'test' AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW Foo.Bar (x, y, z) AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW Foo.Bar (x, y, z COMMENT 'foo') AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW Foo.Bar (x, y, z) COMMENT 'test' AS SELECT a, b, c from t");
    ParsesOk("CREATE VIEW IF NOT EXISTS Foo.Bar AS SELECT a, b, c from t");

    // Test all valid query statements as view definitions.
    ParsesOk("CREATE VIEW Bar AS SELECT 1, 2, 3");
    ParsesOk("CREATE VIEW Bar AS VALUES(1, 2, 3)");
    ParsesOk("CREATE VIEW Bar AS SELECT 1, 2, 3 UNION ALL select 4, 5, 6");
    ParsesOk("CREATE VIEW Bar AS WITH t AS (SELECT 1, 2, 3) SELECT * FROM t");

    // Mismatched number of columns in column definition and view definition parses ok.
    ParsesOk("CREATE VIEW Bar (x, y) AS SELECT 1, 2, 3");

    // No view name.
    ParserError("CREATE VIEW AS SELECT c FROM t");
    // Missing AS keyword
    ParserError("CREATE VIEW Bar SELECT c FROM t");
    // Empty column definition not allowed.
    ParserError("CREATE VIEW Foo.Bar () AS SELECT c FROM t");
    // Column definitions cannot include types.
    ParserError("CREATE VIEW Foo.Bar (x int) AS SELECT c FROM t");
    ParserError("CREATE VIEW Foo.Bar (x int COMMENT 'x') AS SELECT c FROM t");
    // A type does not parse as an identifier.
    ParserError("CREATE VIEW Foo.Bar (int COMMENT 'x') AS SELECT c FROM t");
    // Missing view definition.
    ParserError("CREATE VIEW Foo.Bar (x) AS");
    // Invalid view definitions. A view definition must be a query statement.
    ParserError("CREATE VIEW Foo.Bar (x) AS INSERT INTO t select * from t");
    ParserError("CREATE VIEW Foo.Bar (x) AS UPSERT INTO t select * from t");
    ParserError("CREATE VIEW Foo.Bar (x) AS CREATE TABLE Wrong (i int)");
    ParserError("CREATE VIEW Foo.Bar (x) AS ALTER TABLE Foo COLUMNS (i int, s string)");
    ParserError("CREATE VIEW Foo.Bar (x) AS CREATE VIEW Foo.Bar AS SELECT 1");
    ParserError("CREATE VIEW Foo.Bar (x) AS ALTER VIEW Foo.Bar AS SELECT 1");
  }

  @Test
  public void TestAlterView() {
    ParsesOk("ALTER VIEW Bar AS SELECT 1, 2, 3");
    ParsesOk("ALTER VIEW Bar AS SELECT a, b, c FROM t");
    ParsesOk("ALTER VIEW Bar AS VALUES(1, 2, 3)");
    ParsesOk("ALTER VIEW Bar AS SELECT 1, 2, 3 UNION ALL select 4, 5, 6");

    ParsesOk("ALTER VIEW Foo.Bar AS SELECT 1, 2, 3");
    ParsesOk("ALTER VIEW Foo.Bar AS SELECT a, b, c FROM t");
    ParsesOk("ALTER VIEW Foo.Bar AS VALUES(1, 2, 3)");
    ParsesOk("ALTER VIEW Foo.Bar AS SELECT 1, 2, 3 UNION ALL select 4, 5, 6");
    ParsesOk("ALTER VIEW Foo.Bar AS WITH t AS (SELECT 1, 2, 3) SELECT * FROM t");

    // Must be ALTER VIEW not ALTER TABLE.
    ParserError("ALTER TABLE Foo.Bar AS SELECT 1, 2, 3");
    // Missing view name.
    ParserError("ALTER VIEW AS SELECT 1, 2, 3");
    // Missing AS name.
    ParserError("ALTER VIEW Foo.Bar SELECT 1, 2, 3");
    // Missing view definition.
    ParserError("ALTER VIEW Foo.Bar AS");
    // Invalid view definitions. A view definition must be a query statement.
    ParserError("ALTER VIEW Foo.Bar AS INSERT INTO t select * from t");
    ParserError("ALTER VIEW Foo.Bar AS UPSERT INTO t select * from t");
    ParserError("ALTER VIEW Foo.Bar AS CREATE TABLE Wrong (i int)");
    ParserError("ALTER VIEW Foo.Bar AS ALTER TABLE Foo COLUMNS (i int, s string)");
    ParserError("ALTER VIEW Foo.Bar AS CREATE VIEW Foo.Bar AS SELECT 1, 2, 3");
    ParserError("ALTER VIEW Foo.Bar AS ALTER VIEW Foo.Bar AS SELECT 1, 2, 3");
  }

  @Test
  public void TestCreateTableAsSelect() {
    ParsesOk("CREATE TABLE Foo AS SELECT 1, 2, 3");
    ParsesOk("CREATE TABLE Foo AS SELECT * from foo.bar");
    ParsesOk("CREATE TABLE Foo.Bar AS SELECT int_col, bool_col from tbl limit 10");
    ParsesOk("CREATE TABLE Foo.Bar LOCATION '/a/b' AS SELECT * from foo");
    ParsesOk("CREATE TABLE IF NOT EXISTS Foo.Bar LOCATION '/a/b' AS SELECT * from foo");
    ParsesOk("CREATE TABLE Foo STORED AS PARQUET AS SELECT 1");
    ParsesOk("CREATE TABLE Foo ROW FORMAT DELIMITED STORED AS PARQUETFILE AS SELECT 1");
    ParsesOk("CREATE TABLE Foo TBLPROPERTIES ('a'='b', 'c'='d') AS SELECT * from bar");
    ParsesOk("CREATE TABLE Foo PRIMARY KEY (a, b) AS SELECT * from bar");
    ParsesOk("CREATE TABLE Foo PRIMARY KEY (a, b) PARTITION BY HASH PARTITIONS 2 " +
        "AS SELECT * from bar");
    ParsesOk("CREATE TABLE Foo PRIMARY KEY (a, b) PARTITION BY HASH (b) PARTITIONS 2 " +
        "AS SELECT * from bar");

    // With clause works
    ParsesOk("CREATE TABLE Foo AS with t1 as (select 1) select * from t1");

    // Incomplete AS SELECT statement
    ParserError("CREATE TABLE Foo ROW FORMAT DELIMITED STORED AS PARQUET AS SELECT");
    ParserError("CREATE TABLE Foo ROW FORMAT DELIMITED STORED AS PARQUET AS WITH");
    ParserError("CREATE TABLE Foo ROW FORMAT DELIMITED STORED AS PARQUET AS");

    // INSERT/UPSERT statements are not allowed
    ParserError("CREATE TABLE Foo AS INSERT INTO Foo SELECT 1");
    ParserError("CREATE TABLE Foo AS UPSERT INTO Foo SELECT 1");

    // Column and partition definitions not allowed
    ParserError("CREATE TABLE Foo(i int) AS SELECT 1");
    ParserError("CREATE TABLE Foo PARTITIONED BY(i int) AS SELECT 1");

    // Partitioned by syntax following insert into syntax
    ParsesOk("CREATE TABLE Foo PARTITIONED BY (a) AS SELECT 1");
    ParsesOk("CREATE TABLE Foo PARTITIONED BY (a) ROW FORMAT DELIMITED STORED AS " +
        "PARQUETFILE AS SELECT 1");
    ParsesOk("CREATE TABLE Foo PARTITIONED BY (a) AS SELECT 1, 2");
    ParsesOk("CREATE TABLE Foo PARTITIONED BY (a) AS SELECT * from Bar");
    ParsesOk("CREATE TABLE Foo PARTITIONED BY (a, b) AS SELECT * from Bar");
    ParserError("CREATE TABLE Foo PARTITIONED BY (a=2, b) AS SELECT * from Bar");
    ParserError("CREATE TABLE Foo PARTITIONED BY (a, b=2) AS SELECT * from Bar");

    // Flexible partitioning
    ParsesOk("CREATE TABLE Foo PRIMARY KEY (i) PARTITION BY HASH(i) PARTITIONS 4 AS " +
        "SELECT 1");
    ParserError("CREATE TABLE Foo PARTITION BY HASH(i) PARTITIONS 4 AS SELECT 1");
    ParsesOk("CREATE TABLE Foo PRIMARY KEY (a) PARTITION BY HASH(a) PARTITIONS 4 " +
        "TBLPROPERTIES ('a'='b', 'c'='d') AS SELECT * from bar");
    ParsesOk("CREATE TABLE Foo PRIMARY KEY (a) PARTITION BY RANGE(a) " +
        "(PARTITION 1 < VALUES < 10, PARTITION 10 <= VALUES < 20, PARTITION VALUE = 30) " +
        "STORED AS KUDU AS SELECT * FROM Bar");
  }

  @Test
  public void TestDrop() {
    // PURGE is optional
    String[] purgeKw = {"PURGE", ""};
    for (String kw: purgeKw) {
      ParsesOk(String.format("DROP TABLE Foo %s", kw));
      ParsesOk(String.format("DROP TABLE Foo.Bar %s", kw));
      ParsesOk(String.format("DROP TABLE IF EXISTS Foo %s", kw));
      ParsesOk(String.format("DROP TABLE IF EXISTS Foo.Bar %s", kw));
    }

    ParsesOk("DROP VIEW Foo");
    ParsesOk("DROP VIEW Foo.Bar");
    ParsesOk("DROP VIEW IF EXISTS Foo.Bar");
    ParsesOk("DROP DATABASE Foo");
    ParsesOk("DROP DATABASE Foo CASCADE");
    ParsesOk("DROP DATABASE Foo RESTRICT");
    ParsesOk("DROP SCHEMA Foo");
    ParsesOk("DROP DATABASE IF EXISTS Foo");
    ParsesOk("DROP DATABASE IF EXISTS Foo CASCADE");
    ParsesOk("DROP DATABASE IF EXISTS Foo RESTRICT");
    ParsesOk("DROP SCHEMA IF EXISTS Foo");
    ParsesOk("DROP FUNCTION Foo()");
    ParsesOk("DROP AGGREGATE FUNCTION Foo(INT)");
    ParsesOk("DROP FUNCTION Foo.Foo(INT)");
    ParsesOk("DROP AGGREGATE FUNCTION IF EXISTS Foo()");
    ParsesOk("DROP FUNCTION IF EXISTS Foo(INT)");
    ParsesOk("DROP FUNCTION IF EXISTS Foo(INT...)");
    ParsesOk("DROP FUNCTION Foo");
    ParsesOk("DROP FUNCTION IF EXISTS Foo");

    ParserError("DROP");
    ParserError("DROP Foo");
    ParserError("DROP DATABASE Foo.Bar");
    ParserError("DROP SCHEMA Foo.Bar");
    ParserError("DROP SCHEMA Foo PURGE");
    ParserError("DROP DATABASE Foo Bar");
    ParserError("DROP DATABASE Foo PURGE");
    ParserError("DROP DATABASE CASCADE Foo");
    ParserError("DROP DATABASE CASCADE RESTRICT Foo");
    ParserError("DROP DATABASE RESTRICT CASCADE Foo");
    ParserError("DROP CASCADE DATABASE IF EXISTS Foo");
    ParserError("DROP RESTRICT DATABASE IF EXISTS Foo");
    ParserError("DROP SCHEMA Foo Bar");
    ParserError("DROP TABLE IF Foo");
    ParserError("DROP TABLE EXISTS Foo");
    ParserError("DROP IF EXISTS TABLE Foo");
    ParserError("DROP TBL Foo");
    ParserError("DROP VIEW IF Foo");
    ParserError("DROP VIEW EXISTS Foo");
    ParserError("DROP IF EXISTS VIEW Foo");
    ParserError("DROP VIW Foo");
    ParserError("DROP VIEW Foo purge");
    ParserError("DROP FUNCTION Foo)");
    ParserError("DROP FUNCTION Foo(");
    ParserError("DROP FUNCTION Foo PURGE");
    ParserError("DROP FUNCTION");
    ParserError("DROP BLAH FUNCTION");
    ParserError("DROP IF EXISTS FUNCTION Foo()");
    ParserError("DROP FUNCTION Foo RETURNS INT");
    ParserError("DROP FUNCTION Foo(INT) RETURNS INT");
    ParserError("DROP FUNCTION Foo.(INT) RETURNS INT");
    ParserError("DROP FUNCTION Foo..(INT) RETURNS INT");
    ParserError("DROP FUNCTION Foo(NULL) RETURNS INT");
    ParserError("DROP FUNCTION Foo(INT) RETURNS NULL");
    ParserError("DROP BLAH FUNCTION IF EXISTS Foo.A.Foo(INT)");
    ParserError("DROP FUNCTION IF EXISTS Foo(...)");
  }

  @Test
  public void TestTruncateTable() {
    ParsesOk("TRUNCATE TABLE Foo");
    ParsesOk("TRUNCATE TABLE Foo.Bar");
    ParsesOk("TRUNCATE Foo");
    ParsesOk("TRUNCATE Foo.Bar");

    ParserError("TRUNCATE");
    ParserError("TRUNCATE TABLE");
    ParserError("TRUNCATE TBL Foo");
    ParserError("TRUNCATE VIEW Foo");
    ParserError("TRUNCATE DATABASE Foo");
  }

  @Test
  public void TestLoadData() {
    ParsesOk("LOAD DATA INPATH '/a/b' INTO TABLE Foo");
    ParsesOk("LOAD DATA INPATH '/a/b' INTO TABLE Foo.Bar");
    ParsesOk("LOAD DATA INPATH '/a/b' OVERWRITE INTO TABLE Foo.Bar");
    ParsesOk("LOAD DATA INPATH '/a/b' INTO TABLE Foo PARTITION(a=1, b='asdf')");
    ParsesOk("LOAD DATA INPATH '/a/b' INTO TABLE Foo PARTITION(a=1)");

    ParserError("LOAD DATA INPATH '/a/b' INTO Foo PARTITION(a=1)");
    ParserError("LOAD DATA INPATH '/a/b' INTO Foo PARTITION(a)");
    ParserError("LOAD DATA INPATH '/a/b' INTO Foo PARTITION");
    ParserError("LOAD DATA INPATH /a/b/c INTO Foo");
    ParserError("LOAD DATA INPATH /a/b/c INTO Foo");

    // Loading data from a 'LOCAL' path is not supported.
    ParserError("LOAD DATA LOCAL INPATH '/a/b' INTO TABLE Foo");
  }

  /**
   * Wraps the given typeDefs in a CREATE TABLE and CAST and runs ParsesOk().
   * Also tests that the type is parsed correctly in ARRAY, MAP, and STRUCT types.
   */
  private void TypeDefsParseOk(String... typeDefs) {
    for (String typeDefStr: typeDefs) {
      ParsesOk(String.format("CREATE TABLE t (i %s)", typeDefStr));
      ParsesOk(String.format("SELECT CAST (i AS %s)", typeDefStr));
      // Test typeDefStr in complex types.
      ParsesOk(String.format("CREATE TABLE t (i MAP<%s, %s>)", typeDefStr, typeDefStr));
      ParsesOk(String.format("CREATE TABLE t (i ARRAY<%s>)", typeDefStr));
      ParsesOk(String.format("CREATE TABLE t (i STRUCT<f:%s>)", typeDefStr));
    }
  }

  /**
   * Asserts that the given typeDefs fail to parse.
   */
  private void TypeDefsError(String... typeDefs) {
    for (String typeDefStr: typeDefs) {
      ParserError(String.format("CREATE TABLE t (i %s)", typeDefStr));
      ParserError(String.format("SELECT CAST (i AS %s)", typeDefStr));
    }
  }

  @Test
  public void TestTypes() {
    // Test primitive types.
    TypeDefsParseOk("BOOLEAN");
    TypeDefsParseOk("TINYINT");
    TypeDefsParseOk("SMALLINT");
    TypeDefsParseOk("INT", "INTEGER");
    TypeDefsParseOk("BIGINT");
    TypeDefsParseOk("FLOAT");
    TypeDefsParseOk("DOUBLE", "REAL");
    TypeDefsParseOk("STRING");
    TypeDefsParseOk("CHAR(1)", "CHAR(20)");
    TypeDefsParseOk("VARCHAR(1)", "VARCHAR(20)");
    TypeDefsParseOk("BINARY");
    TypeDefsParseOk("DECIMAL");
    TypeDefsParseOk("TIMESTAMP");

    // Test decimal.
    TypeDefsParseOk("DECIMAL");
    TypeDefsParseOk("DECIMAL(1)");
    TypeDefsParseOk("DECIMAL(1, 2)");
    TypeDefsParseOk("DECIMAL(2, 1)");
    TypeDefsParseOk("DECIMAL(6, 6)");
    TypeDefsParseOk("DECIMAL(100, 0)");
    TypeDefsParseOk("DECIMAL(0, 0)");

    TypeDefsError("DECIMAL()");
    TypeDefsError("DECIMAL(a)");
    TypeDefsError("DECIMAL(1, a)");
    TypeDefsError("DECIMAL(1, 2, 3)");
    TypeDefsError("DECIMAL(-1)");

    // Test complex types.
    TypeDefsParseOk("ARRAY<BIGINT>");
    TypeDefsParseOk("MAP<TINYINT, DOUBLE>");
    TypeDefsParseOk("STRUCT<f:TINYINT>");
    TypeDefsParseOk("STRUCT<a:TINYINT, b:BIGINT, c:DOUBLE>");
    TypeDefsParseOk("STRUCT<a:TINYINT COMMENT 'x', b:BIGINT, c:DOUBLE COMMENT 'y'>");

    // Test that struct-field names can be identifiers or keywords even if unquoted.
    // This behavior is needed to parse type strings from the Hive Metastore which
    // may have unquoted identifiers corresponding to keywords.
    for (String keyword: SqlScanner.keywordMap.keySet()) {
      // Skip keywords that are not valid field/column names in the Metastore.
      if (!MetastoreShim.validateName(keyword)) continue;
      String structType = "STRUCT<" + keyword + ":INT>";
      TypeDefsParseOk(structType);
    }

    TypeDefsError("CHAR()");
    TypeDefsError("CHAR(1, 1)");
    TypeDefsError("ARRAY<>");
    TypeDefsError("ARRAY BIGINT");
    TypeDefsError("MAP<>");
    TypeDefsError("MAP<TINYINT>");
    TypeDefsError("MAP<TINYINT, BIGINT, DOUBLE>");
    TypeDefsError("STRUCT<>");
    TypeDefsError("STRUCT<TINYINT>");
    TypeDefsError("STRUCT<a TINYINT>");
    TypeDefsError("STRUCT<'a':TINYINT>");
  }

  @Test
  public void TestResetMetadata() {
    ParsesOk("invalidate metadata");
    ParsesOk("invalidate metadata Foo");
    ParsesOk("invalidate metadata Foo.S");
    ParsesOk("refresh Foo");
    ParsesOk("refresh Foo.S");
    ParsesOk("refresh Foo partition (col=2)");
    ParsesOk("refresh Foo.S partition (col=2)");
    ParsesOk("refresh Foo.S partition (col1 = 2, col2 = 3)");
    ParsesOk("refresh functions Foo");

    ParserError("invalidate");
    ParserError("invalidate metadata Foo.S.S");
    ParserError("invalidate metadata partition (col=2)");
    ParserError("invalidate metadata Foo.S partition (col=2)");
    ParserError("REFRESH Foo.S.S");
    ParserError("refresh");
    ParserError("refresh Foo.S partition (col1 = 2, col2)");
    ParserError("refresh Foo.S partition ()");
    ParserError("refresh functions Foo.S");
  }

  @Test
  public void TestComputeDropStats() {
    String[] prefixes = {"compute", "drop"};
    String[] okSuffixes = {"stats bar", "stats `bar`", "stats foo.bar",
        "stats `foo`.`bar`"};
    String[] okComputeSuffixes = {"(ab)", "(ab, bc)", "()"};
    String[] errorSuffixes = {
     // Missing table name.
     "stats",
     // Missing 'stats' keyword.
     "`bar`",
     // Cannot use string literal as table name.
     "stats 'foo'",
     // Cannot analyze multiple tables in one stmt.
     "stats foo bar"
    };

    for (String prefix: prefixes) {
      for (String suffix: okSuffixes) {
        ParsesOk(prefix + " " + suffix);
      }
      for (String suffix: errorSuffixes) {
        ParserError(prefix + " " + suffix);
      }
    }
    for (String suffix: okSuffixes) {
      for (String computeSuffix: okComputeSuffixes) {
        ParsesOk("compute" + " " + suffix + " " + computeSuffix);
      }
    }
  }

  @Test
  public void TestGetErrorMsg() {

    // missing select
    ParserError("c, b, c from t",
        "Syntax error in line 1:\n" +
        "c, b, c from t\n" +
        "^\n" +
        "Encountered: IDENTIFIER\n" +
        "Expected: ALTER, COMPUTE, CREATE, DELETE, DESCRIBE, DROP, EXPLAIN, GRANT, " +
        "INSERT, INVALIDATE, LOAD, REFRESH, REVOKE, SELECT, SET, SHOW, TRUNCATE, " +
        "UPDATE, UPSERT, USE, VALUES, WITH\n");

    // missing select list
    ParserError("select from t",
        "Syntax error in line 1:\n" +
        "select from t\n" +
        "       ^\n" +
        "Encountered: FROM\n" +
        "Expected: ALL, CASE, CAST, DEFAULT, DISTINCT, EXISTS, FALSE, IF, INTERVAL, " +
        "LEFT, NOT, NULL, REPLACE, RIGHT, STRAIGHT_JOIN, TRUNCATE, TRUE, IDENTIFIER");

    // missing from
    ParserError("select c, b, c where a = 5",
        "Syntax error in line 1:\n" +
        "select c, b, c where a = 5\n" +
        "               ^\n" +
        "Encountered: WHERE\n" +
        "Expected: AND, AS, BETWEEN, DEFAULT, DIV, FROM, ILIKE, IN, IREGEXP, IS, LIKE, " +
        "LIMIT, NOT, OR, ORDER, REGEXP, RLIKE, UNION, COMMA, IDENTIFIER\n");

    // missing table list
    ParserError("select c, b, c from where a = 5",
        "Syntax error in line 1:\n" +
        "select c, b, c from where a = 5\n" +
        "                    ^\n" +
        "Encountered: WHERE\n" +
        "Expected: DEFAULT, IDENTIFIER\n");

    // missing predicate in where clause (no group by)
    ParserError("select c, b, c from t where",
        "Syntax error in line 1:\n" +
        "select c, b, c from t where\n" +
        "                           ^\n" +
        "Encountered: EOF\n" +
        "Expected: CASE, CAST, DEFAULT, EXISTS, FALSE, IF, INTERVAL, LEFT, NOT, NULL, " +
        "REPLACE, RIGHT, TRUNCATE, TRUE, IDENTIFIER");

    // missing predicate in where clause (group by)
    ParserError("select c, b, c from t where group by a, b",
        "Syntax error in line 1:\n" +
        "select c, b, c from t where group by a, b\n" +
        "                            ^\n" +
        "Encountered: GROUP\n" +
        "Expected: CASE, CAST, DEFAULT, EXISTS, FALSE, IF, INTERVAL, LEFT, NOT, NULL, " +
        "REPLACE, RIGHT, TRUNCATE, TRUE, IDENTIFIER");

    // unmatched string literal starting with "
    ParserError("select c, \"b, c from t",
        "Unmatched string literal in line 1:\n" +
        "select c, \"b, c from t\n" +
        "           ^\n");

    // unmatched string literal starting with '
    ParserError("select c, 'b, c from t",
        "Unmatched string literal in line 1:\n" +
        "select c, 'b, c from t\n" +
        "           ^\n");

    // test placement of error indicator ^ on queries with multiple lines
    ParserError("select (i + 5)(1 - i) from t",
        "Syntax error in line 1:\n" +
        "select (i + 5)(1 - i) from t\n" +
        "              ^\n" +
        "Encountered: (\n" +
        "Expected:");

    ParserError("select (i + 5)\n(1 - i) from t",
        "Syntax error in line 2:\n" +
        "(1 - i) from t\n" +
        "^\n" +
        "Encountered: (\n" +
        "Expected");

    ParserError("select (i + 5)\n(1 - i)\nfrom t",
        "Syntax error in line 2:\n" +
        "(1 - i)\n" +
        "^\n" +
        "Encountered: (\n" +
        "Expected");

    // Long line: error in the middle
    ParserError("select c, b, c,c,c,c,c,c,c,c,c,a a a,c,c,c,c,c,c,c,cd,c,d,d,,c, from t",
        "Syntax error in line 1:\n" +
        "... b, c,c,c,c,c,c,c,c,c,a a a,c,c,c,c,c,c,c,cd,c,d,d,,c,...\n" +
        "                             ^\n" +
        "Encountered: IDENTIFIER\n" +
        "Expected: CROSS, FROM, FULL, GROUP, HAVING, INNER, JOIN, LEFT, LIMIT, OFFSET, " +
        "ON, ORDER, RIGHT, STRAIGHT_JOIN, TABLESAMPLE, UNION, USING, WHERE, COMMA\n");

    // Long line: error close to the start
    ParserError("select a a a, b, c,c,c,c,c,c,c,c,c,c,c,c,c,c,c,c,cd,c,d,d,,c, from t",
        "Syntax error in line 1:\n" +
        "select a a a, b, c,c,c,c,c,c,c,c,c,c,c,...\n" +
        "           ^\n" +
        "Encountered: IDENTIFIER\n" +
        "Expected: CROSS, FROM, FULL, GROUP, HAVING, INNER, JOIN, LEFT, LIMIT, OFFSET, " +
        "ON, ORDER, RIGHT, STRAIGHT_JOIN, TABLESAMPLE, UNION, USING, WHERE, COMMA\n");

    // Long line: error close to the end
    ParserError("select a, b, c,c,c,c,c,c,c,c,c,c,c,c,c,c,c,c,cd,c,d,d, ,c, from t",
        "Syntax error in line 1:\n" +
        "...c,c,c,c,c,c,c,c,cd,c,d,d, ,c, from t\n" +
        "                             ^\n" +
        "Encountered: COMMA\n" +
        "Expected: CASE, CAST, DEFAULT, EXISTS, FALSE, IF, INTERVAL, LEFT, NOT, NULL, " +
        "REPLACE, RIGHT, TRUNCATE, TRUE, IDENTIFIER");

    // Parsing identifiers that have different names printed as EXPECTED
    ParserError("DROP DATA SRC foo",
        "Syntax error in line 1:\n" +
        "DROP DATA SRC foo\n" +
        "          ^\n" +
        "Encountered: IDENTIFIER\n" +
        "Expected: SOURCE\n");
    ParserError("SHOW DATA SRCS",
        "Syntax error in line 1:\n" +
        "SHOW DATA SRCS\n" +
        "          ^\n" +
        "Encountered: IDENTIFIER\n" +
        "Expected: SOURCES\n");
    ParserError("USE ` `",
        "Syntax error in line 1:\n" +
        "USE ` `\n" +
        "    ^\n" +
        "Encountered: EMPTY IDENTIFIER\n" +
        "Expected: DEFAULT, IDENTIFIER\n");

    // Expecting = token
    ParserError("SET foo",
         "Syntax error in line 1:\n" +
         "SET foo\n" +
         "       ^\n" +
         "Encountered: EOF\n" +
         "Expected: =\n");
  }

  @Test
  public void TestExplain() {
    ParsesOk("explain select a from tbl");
    ParsesOk("explain insert into tbl select a, b, c, d from tbl");
    ParsesOk("explain upsert into tbl select a, b, c, d from tbl");
    ParserError("explain");
    // cannot EXPLAIN an explain stmt
    ParserError("explain explain select a from tbl");
    // cannot EXPLAIN DDL stmt
    ParserError("explain CREATE TABLE Foo (i int)");
  }

  @Test
  public void TestSubqueries() {
    // Binary nested predicates
    String subquery = "(SELECT count(*) FROM bar)";
    String[] operators = {"=", "!=", "<>", ">", ">=", "<", "<=", "<=>",
      "IS DISTINCT FROM", "IS NOT DISTINCT FROM"};
    for (String op: operators) {
      ParsesOk(String.format("SELECT * FROM foo WHERE a %s %s", op, subquery));
      ParsesOk(String.format("SELECT * FROM foo WHERE %s %s a", subquery, op));
    }
    // Binary predicate with an arithmetic expr
    ParsesOk("SELECT * FROM foo WHERE a+1 > (SELECT count(a) FROM bar)");
    ParsesOk("SELECT * FROM foo WHERE (SELECT count(a) FROM bar) < a+1");

    // [NOT] IN nested predicates
    ParsesOk("SELECT * FROM foo WHERE a IN (SELECT a FROM bar)");
    ParsesOk("SELECT * FROM foo WHERE a NOT IN (SELECT a FROM bar)");

    // [NOT] EXISTS predicates
    ParsesOk("SELECT * FROM foo WHERE EXISTS (SELECT a FROM bar WHERE b > 0)");
    ParsesOk("SELECT * FROM foo WHERE NOT EXISTS (SELECT a FROM bar WHERE b > 0)");
    ParsesOk("SELECT * FROM foo WHERE NOT (EXISTS (SELECT a FROM bar))");

    // Compound nested predicates
    ParsesOk("SELECT * FROM foo WHERE a = (SELECT count(a) FROM bar) AND " +
             "b != (SELECT count(b) FROM baz) and c IN (SELECT c FROM qux)");
    ParsesOk("SELECT * FROM foo WHERE EXISTS (SELECT a FROM bar WHERE b < 0) AND " +
             "NOT EXISTS (SELECT a FROM baz WHERE b > 0)");
    ParsesOk("SELECT * FROM foo WHERE EXISTS (SELECT a from bar) AND " +
             "NOT EXISTS (SELECT a FROM baz) AND b IN (SELECT b FROM bar) AND " +
             "c NOT IN (SELECT c FROM qux) AND d = (SELECT max(d) FROM quux)");

    // Nested parentheses
    ParsesOk("SELECT * FROM foo WHERE EXISTS ((SELECT * FROM bar))");
    ParsesOk("SELECT * FROM foo WHERE EXISTS (((SELECT * FROM bar)))");
    ParsesOk("SELECT * FROM foo WHERE a IN ((SELECT a FROM bar))");
    ParsesOk("SELECT * FROM foo WHERE a = ((SELECT max(a) FROM bar))");

    // More than one nesting level
    ParsesOk("SELECT * FROM foo WHERE a IN (SELECT a FROM bar WHERE b IN " +
             "(SELECT b FROM baz))");
    ParsesOk("SELECT * FROM foo WHERE EXISTS (SELECT a FROM bar WHERE b NOT IN " +
             "(SELECT b FROM baz WHERE c < 10 AND d = (SELECT max(d) FROM qux)))");

    // Binary predicate between subqueries
    for (String op: operators) {
      ParsesOk(String.format("SELECT * FROM foo WHERE %s %s %s", subquery, op,
          subquery));
    }

    // Malformed nested subqueries
    // Missing or misplaced parenthesis around a subquery
    ParserError("SELECT * FROM foo WHERE a IN SELECT a FROM bar");
    ParserError("SELECT * FROM foo WHERE a = SELECT count(*) FROM bar");
    ParserError("SELECT * FROM foo WHERE EXISTS SELECT * FROM bar");
    ParserError("SELECT * FROM foo WHERE a IN (SELECT a FROM bar");
    ParserError("SELECT * FROM foo WHERE a IN SELECT a FROM bar)");
    ParserError("SELECT * FROM foo WHERE a IN (SELECT) a FROM bar");

    // Invalid syntax for [NOT] EXISTS
    ParserError("SELECT * FROM foo WHERE a EXISTS (SELECT * FROM bar)");
    ParserError("SELECT * FROM foo WHERE a NOT EXISTS (SELECT * FROM bar)");

    // Set operations between subqueries
    ParserError("SELECT * FROM foo WHERE EXISTS ((SELECT a FROM bar) UNION " +
                "(SELECT a FROM baz))");

    // Nested predicate in the HAVING clause
    ParsesOk("SELECT a, count(*) FROM foo GROUP BY a HAVING count(*) > " +
             "(SELECT count(*) FROM bar)");
    ParsesOk("SELECT a, count(*) FROM foo GROUP BY a HAVING 10 > " +
             "(SELECT count(*) FROM bar)");

    // Subquery in the SELECT clause
    ParsesOk("SELECT a, b, (SELECT c FROM foo) FROM foo");
    ParsesOk("SELECT (SELECT a FROM foo), b, c FROM bar");
    ParsesOk("SELECT (SELECT (SELECT a FROM foo) FROM bar) FROM baz");
    ParsesOk("SELECT (SELECT a FROM foo)");

    // Malformed subquery in the SELECT clause
    ParserError("SELECT SELECT a FROM foo FROM bar");
    ParserError("SELECT (SELECT a FROM foo FROM bar");
    ParserError("SELECT SELECT a FROM foo) FROM bar");
    ParserError("SELECT (SELECT) a FROM foo");

    // Subquery in the GROUP BY clause
    ParsesOk("SELECT a, count(*) FROM foo GROUP BY (SELECT a FROM bar)");
    ParsesOk("SELECT a, count(*) FROM foo GROUP BY a, (SELECT b FROM bar)");

    // Malformed subquery in the GROUP BY clause
    ParserError("SELECT a, count(*) FROM foo GROUP BY SELECT a FROM bar");
    ParserError("SELECT a, count(*) FROM foo GROUP BY (SELECT) a FROM bar");
    ParserError("SELECT a, count(*) FROM foo GROUP BY (SELECT a FROM bar");

    // Subquery in the ORDER BY clause
    ParsesOk("SELECT a, b FROM foo ORDER BY (SELECT a FROM bar)");
    ParsesOk("SELECT a, b FROM foo ORDER BY (SELECT a FROM bar) DESC");
    ParsesOk("SELECT a, b FROM foo ORDER BY a ASC, (SELECT a FROM bar) DESC");

    // Malformed subquery in the ORDER BY clause
    ParserError("SELECT a, count(*) FROM foo ORDER BY SELECT a FROM bar");
    ParserError("SELECT a, count(*) FROM foo ORDER BY (SELECT) a FROM bar DESC");
    ParserError("SELECT a, count(*) FROM foo ORDER BY (SELECT a FROM bar ASC");
  }

  @Test
  public void TestSet() {
    ParsesOk("SET foo='bar'");
    ParsesOk("SET foo=\"bar\"");
    ParsesOk("SET foo=bar");
    ParsesOk("SET foo = bar");
    ParsesOk("SET foo=1");
    ParsesOk("SET foo=true");
    ParsesOk("SET foo=false");
    ParsesOk("SET foo=1.2");
    ParsesOk("SET foo=null");
    ParsesOk("SET foo=10g");
    ParsesOk("SET `foo`=0");
    ParsesOk("SET foo=''");
    ParsesOk("SET");

    ParserError("SET foo");
    ParserError("SET foo=");
    ParserError("SET foo=1+2");
    ParserError("SET foo = '10");
  }

  @Test
  public void TestCreateDropRole() {
    ParsesOk("CREATE ROLE foo");
    ParsesOk("DROP ROLE foo");
    ParsesOk("DROP ROLE foo");
    ParsesOk("CREATE ROLE `role`");
    ParsesOk("DROP ROLE  `role`");
    ParserError("CREATE ROLE");
    ParserError("DROP ROLE");
    ParserError("CREATE ROLE 'foo'");
    ParserError("DROP ROLE 'foo'");
  }

  @Test
  public void TestGrantRevokeRole() {
    ParsesOk("GRANT ROLE foo TO GROUP bar");
    ParsesOk("REVOKE ROLE foo FROM GROUP bar");
    ParsesOk("GRANT ROLE `foo` TO GROUP `bar`");

    ParserError("GRANT ROLE foo TO GROUP");
    ParserError("GRANT ROLE foo FROM GROUP bar");

    ParserError("REVOKE ROLE foo FROM GROUP");
    ParserError("REVOKE ROLE foo TO GROUP bar");
  }

  @Test
  public void TestGrantRevokePrivilege() {
    Object[][] grantRevFormatStrs = {{"GRANT", "TO"}, {"REVOKE", "FROM"}};
    for (Object[] formatStr: grantRevFormatStrs) {
      ParsesOk(String.format("%s ALL ON TABLE foo %s myRole", formatStr));

      // KW_ROLE is optional (Hive requires KW_ROLE, but Impala does not).
      ParsesOk(String.format("%s ALL ON TABLE foo %s ROLE myRole", formatStr));

      ParsesOk(String.format("%s ALL ON DATABASE foo %s myRole", formatStr));
      ParsesOk(String.format("%s ALL ON URI 'foo' %s  myRole", formatStr));

      ParsesOk(String.format("%s INSERT ON TABLE foo %s myRole", formatStr));
      ParsesOk(String.format("%s INSERT ON DATABASE foo %s myRole", formatStr));
      ParsesOk(String.format("%s INSERT ON URI 'foo' %s  myRole", formatStr));

      ParsesOk(String.format("%s SELECT ON TABLE foo %s myRole", formatStr));
      ParserError(String.format("%s SELECT ON TABLE %s myRole", formatStr));
      ParsesOk(String.format("%s SELECT ON DATABASE foo %s myRole", formatStr));
      ParserError(String.format("%s SELECT ON DATABASE %s myRole", formatStr));
      ParsesOk(String.format("%s SELECT ON URI 'foo' %s myRole", formatStr));
      ParserError(String.format("%s SELECT ON URI %s myRole", formatStr));

      // Column-level authorization on TABLE scope
      ParsesOk(String.format("%s SELECT (a, b) ON TABLE foo %s myRole", formatStr));
      ParsesOk(String.format("%s SELECT () ON TABLE foo %s myRole", formatStr));
      ParsesOk(String.format("%s INSERT (a, b) ON TABLE foo %s myRole", formatStr));
      ParsesOk(String.format("%s ALL (a, b) ON TABLE foo %s myRole", formatStr));
      ParserError(String.format("%s SELECT (*) ON TABLE foo %s myRole", formatStr));

      ParserError(String.format("%s SELECT (a,) ON TABLE foo %s myRole", formatStr));
      ParserError(String.format("%s SELECT a, b ON TABLE foo %s myRole", formatStr));
      ParserError(String.format("%s SELECT (a), b ON TABLE foo %s myRole", formatStr));
      ParserError(String.format("%s SELECT ON TABLE (a, b) foo %s myRole", formatStr));
      ParserError(String.format("%s SELECT ((a)) ON TABLE foo %s myRole", formatStr));
      ParserError(String.format("%s SELECT (a, b) ON DATABASE foo %s myRole",
          formatStr));
      ParserError(String.format("%s SELECT (a, b) ON URI 'foo' %s myRole", formatStr));

      // Server scope does not accept a name.
      ParsesOk(String.format("%s ALL ON SERVER %s myRole", formatStr));
      ParsesOk(String.format("%s INSERT ON SERVER %s myRole", formatStr));
      ParsesOk(String.format("%s SELECT ON SERVER %s myRole", formatStr));

      // URIs are string literals
      ParserError(String.format("%s ALL ON URI foo %s myRole", formatStr));
      ParserError(String.format("%s ALL ON DATABASE 'foo' %s myRole", formatStr));
      ParserError(String.format("%s ALL ON TABLE 'foo' %s myRole", formatStr));

      // No object name (only works for SERVER scope)
      ParserError(String.format("GRANT ALL ON TABLE FROM myrole", formatStr));
      ParserError(String.format("GRANT ALL ON DATABASE FROM myrole", formatStr));
      ParserError(String.format("GRANT ALL ON URI FROM myrole", formatStr));

      // No role specified
      ParserError(String.format("%s ALL ON TABLE foo %s", formatStr));
      // Invalid privilege
      ParserError(String.format("%s FAKE ON TABLE foo %s myRole", formatStr));
    }
    ParsesOk("GRANT ALL ON TABLE foo TO myRole WITH GRANT OPTION");
    ParsesOk("GRANT ALL ON DATABASE foo TO myRole WITH GRANT OPTION");
    ParsesOk("GRANT ALL ON SERVER TO myRole WITH GRANT OPTION");
    ParsesOk("GRANT ALL ON URI '/abc/' TO myRole WITH GRANT OPTION");
    ParserError("GRANT ALL ON TABLE foo TO myRole WITH GRANT");
    ParserError("GRANT ALL ON TABLE foo TO myRole WITH");
    ParserError("GRANT ALL ON TABLE foo TO ROLE");
    ParserError("REVOKE ALL ON TABLE foo TO ROLE");

    ParsesOk("REVOKE GRANT OPTION FOR ALL ON TABLE foo FROM myRole");
    ParsesOk("REVOKE GRANT OPTION FOR ALL ON DATABASE foo FROM myRole");
    ParsesOk("REVOKE GRANT OPTION FOR ALL ON SERVER FROM myRole");
    ParsesOk("REVOKE GRANT OPTION FOR ALL ON URI '/abc/' FROM myRole");
    ParserError("REVOKE GRANT OPTION ALL ON URI '/abc/' FROM myRole");
    ParserError("REVOKE GRANT ALL ON URI '/abc/' FROM myRole");

    ParserError("ALL ON TABLE foo TO myrole");
    ParserError("ALL ON TABLE foo FROM myrole");

    ParserError("GRANT ALL ON TABLE foo FROM myrole");
    ParserError("REVOKE ALL ON TABLE foo TO myrole");

    ParserError("GRANT UPDATE ON TABLE foo TO myRole");
    ParserError("GRANT DELETE ON TABLE foo TO myRole");
    ParserError("GRANT UPSERT ON TABLE foo TO myRole");
  }

  @Test
  public void TestShowRoles() {
    ParsesOk("SHOW ROLES");
    ParsesOk("SHOW CURRENT ROLES");
    ParsesOk("SHOW ROLE GRANT GROUP myGroup");
    ParserError("SHOW ROLES blah");
    ParserError("SHOW ROLE GRANT GROUP");
    ParserError("SHOW CURRENT");
    ParserError("SHOW ROLE");
    ParserError("SHOW");
  }

  @Test
  public void TestShowGrantRole() {
    // Show all grants on a role
    ParsesOk("SHOW GRANT ROLE foo");

    // Show grants on a specific object
    ParsesOk("SHOW GRANT ROLE foo ON SERVER");
    ParsesOk("SHOW GRANT ROLE foo ON DATABASE foo");
    ParsesOk("SHOW GRANT ROLE foo ON TABLE foo");
    ParsesOk("SHOW GRANT ROLE foo ON TABLE foo.bar");
    ParsesOk("SHOW GRANT ROLE foo ON URI '/abc/123'");

    ParserError("SHOW GRANT ROLE");
    ParserError("SHOW GRANT ROLE foo ON SERVER foo");
    ParserError("SHOW GRANT ROLE foo ON DATABASE");
    ParserError("SHOW GRANT ROLE foo ON TABLE");
    ParserError("SHOW GRANT ROLE foo ON URI abc");
  }

  @Test
  public void TestShowCreateFunction() {
    ParsesOk("SHOW CREATE FUNCTION foo");
    ParsesOk("SHOW CREATE FUNCTION foo.bar");
    ParsesOk("SHOW CREATE AGGREGATE FUNCTION foo");
    ParsesOk("SHOW CREATE AGGREGATE FUNCTION foo.bar");

    ParserError("SHOW CREATE FUNCTION");
    ParserError("SHOW CREATE AGGREGATE FUNCTION");
    ParserError("SHOW CREATE ANALYTIC FUNCTION foo");
    ParserError("SHOW CREATE AGGREGATE ANALYTIC FUNCTION foo");
  }

  @Test
  public void TestComputeStats() {
    ParsesOk("COMPUTE STATS alltypes");
    ParsesOk("COMPUTE STATS functional.alltypes");
    ParsesOk("COMPUTE STATS alltypes TABLESAMPLE SYSTEM(10)");
    ParsesOk("COMPUTE STATS alltypes TABLESAMPLE SYSTEM(10) REPEATABLE(10)");
    ParsesOk("COMPUTE STATS functional.alltypes TABLESAMPLE SYSTEM(10) REPEATABLE(10)");
    ParserError("COMPUTE functional.alltypes");
    ParserError("COMPUTE STATS ON functional.alltypes");
    ParserError("COMPUTE STATS");
  }

  @Test
  public void TestComputeIncrementalStats() {
    ParsesOk("COMPUTE INCREMENTAL STATS functional.alltypes");
    ParsesOk(
        "COMPUTE INCREMENTAL STATS functional.alltypes PARTITION(month=10, year=2010)");
    ParsesOk(
        "DROP INCREMENTAL STATS functional.alltypes PARTITION(month=10, year=2010)");
    ParserError("COMPUTE INCREMENTAL STATS");
    ParserError("COMPUTE INCREMENTAL functional.alltypes");
    ParserError("DROP INCREMENTAL STATS functional.alltypes");
    ParserError("COMPUTE INCREMENTAL STATS functional.alltypes TABLESAMPLE SYSTEM(10)");
  }

  @Test
  public void TestSemiColon() {
    ParserError(";", "Syntax error");
    ParsesOk("SELECT 1;");
    ParsesOk(" SELECT 1 ; ");
    ParsesOk("  SELECT  1  ;  ");
    ParserError("SELECT 1; SELECT 2;",
        "Syntax error in line 1:\n" +
        "SELECT 1; SELECT 2;\n" +
        "          ^\n" +
        "Encountered: SELECT\n" +
        "Expected");
    ParsesOk("SELECT 1;;;");
    ParsesOk("SELECT 1 FROM functional.alltypestiny WHERE 1 = (SELECT 1);");
    ParserError("SELECT 1 FROM functional.alltypestiny WHERE 1 = (SELECT 1;)",
        "Syntax error");
    ParserError("SELECT 1 FROM functional.alltypestiny WHERE 1 = (SELECT 1;);",
        "Syntax error");
    ParsesOk("CREATE TABLE functional.test_table (col INT);");
    ParsesOk("DESCRIBE functional.alltypes;");
    ParsesOk("SET num_nodes=1;");
  }
}
