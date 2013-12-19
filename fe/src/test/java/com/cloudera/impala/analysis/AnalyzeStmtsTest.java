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

import static org.junit.Assert.fail;

import java.lang.reflect.Field;

import org.junit.Test;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Uda;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class AnalyzeStmtsTest extends AnalyzerTest {

  @Test
  public void TestFromClause() throws AnalysisException {
    AnalyzesOk("select int_col from functional.alltypes");
    AnalysisError("select int_col from badtbl", "Table does not exist: default.badtbl");

    // case-insensitive
    AnalyzesOk("SELECT INT_COL FROM FUNCTIONAL.ALLTYPES");
    AnalyzesOk("SELECT INT_COL FROM functional.alltypes");
    AnalyzesOk("SELECT INT_COL FROM functional.aLLTYPES");
    AnalyzesOk("SELECT INT_COL FROM Functional.ALLTYPES");
    AnalyzesOk("SELECT INT_COL FROM FUNCTIONAL.ALLtypes");
    AnalyzesOk("SELECT INT_COL FROM FUNCTIONAL.alltypes");
    AnalyzesOk("select functional.AllTypes.Int_Col from functional.alltypes");

    // explicit aliases work
    AnalyzesOk("select a.int_col from functional.alltypes a");
    // columns without table alias can be resolved if they are not ambiguous
    AnalyzesOk("select int_col, zip from functional.alltypes, functional.testtbl");
    // implicit fully-qualified table name as alias works
    AnalyzesOk("select functional.alltypes.int_col from functional.alltypes");
    // implicit non-fully-qualified table name as alias works
    AnalyzesOk("select alltypes.int_col from functional.alltypes");

    // implicit fully-qualified table name on non-fully qualified table reference
    AnalyzesOk("select functional.alltypes.int_col from alltypes",
        createAnalyzer("functional"));
    // implicit non-fully-qualified table name on non-fully qualified table reference
    AnalyzesOk("select alltypes.int_col from alltypes",
        createAnalyzer("functional"));

    // duplicate alias
    AnalysisError("select a.int_col, a.id " +
        "          from functional.alltypes a, functional.testtbl a",
        "Duplicate table alias");
    // duplicate implicit alias
    AnalysisError("select int_col from functional.alltypes, " +
        "functional.alltypes", "Duplicate table alias");

    // resolves dbs correctly
    AnalyzesOk("select zip from functional.testtbl");
    AnalysisError("select int_col from functional.testtbl",
        "couldn't resolve column reference");
  }

  @Test
  public void TestNoFromClause() throws AnalysisException {
    AnalyzesOk("select 'test'");
    AnalyzesOk("select 1 + 1, -128, 'two', 1.28");
    AnalyzesOk("select -1, 1 - 1, 10 - -1, 1 - - - 1");
    AnalyzesOk("select -1.0, 1.0 - 1.0, 10.0 - -1.0, 1.0 - - - 1.0");
    AnalysisError("select a + 1", "couldn't resolve column reference: 'a'");
    // Test predicates in select list.
    AnalyzesOk("select true");
    AnalyzesOk("select false");
    AnalyzesOk("select true or false");
    AnalyzesOk("select true and false");
    // Test NULL's in select list.
    AnalyzesOk("select null");
    AnalyzesOk("select null and null");
    AnalyzesOk("select null or null");
    AnalyzesOk("select null is null");
    AnalyzesOk("select null is not null");
    AnalyzesOk("select int_col is not null from functional.alltypes");
  }

  @Test
  public void TestStar() throws AnalysisException {
    AnalyzesOk("select * from functional.AllTypes");
    AnalyzesOk("select functional.alltypes.* from functional.AllTypes");
    // different db
    AnalyzesOk("select functional_seq.alltypes.* from functional_seq.alltypes");
    // two tables w/ identical names from different dbs
    AnalyzesOk("select functional.alltypes.*, functional_seq.alltypes.* " +
        "from functional.alltypes, functional_seq.alltypes");
    AnalyzesOk("select * from functional.alltypes, functional_seq.alltypes");
    // '*' without from clause has no meaning.
    AnalysisError("select *", "'*' expression in select list requires FROM clause.");
    AnalysisError("select 1, *, 2+4",
        "'*' expression in select list requires FROM clause.");
    AnalysisError("select a.*", "unknown table: a");
  }

  @Test
  public void TestOrdinals() throws AnalysisException {
    // can't group or order on *
    AnalysisError("select * from functional.alltypes group by 1",
        "cannot combine '*' in select list with GROUP BY");
    AnalysisError("select * from functional.alltypes order by 1",
        "ORDER BY: ordinal refers to '*' in select list");
  }

  @Test
  public void TestSubquery() throws AnalysisException {
    AnalyzesOk("select y x from (select id y from functional_hbase.alltypessmall) a");
    AnalyzesOk("select id from (select id from functional_hbase.alltypessmall) a");
    AnalyzesOk("select * from (select id+2 from functional_hbase.alltypessmall) a");
    AnalyzesOk("select t1 c from " +
        "(select c t1 from (select id c from functional_hbase.alltypessmall) t1) a");
    AnalysisError("select id from (select id+2 from functional_hbase.alltypessmall) a",
        "couldn't resolve column reference: 'id'");
    AnalyzesOk("select a.* from (select id+2 from functional_hbase.alltypessmall) a");

    // join test
    AnalyzesOk("select * from (select id+2 id from functional_hbase.alltypessmall) a " +
        "join (select * from functional.AllTypes where true) b");
    AnalyzesOk("select a.x from (select count(id) x from functional.AllTypes) a");
    AnalyzesOk("select a.* from (select count(id) from functional.AllTypes) a");
    AnalysisError("select a.id from (select id y from functional_hbase.alltypessmall) a",
        "unknown column 'id' (table alias 'a')");
    AnalyzesOk("select * from (select * from functional.AllTypes) a where year = 2009");
    AnalyzesOk("select * from (select * from functional.alltypesagg) a right outer join" +
        "             (select * from functional.alltypessmall) b using (id, int_col) " +
        "       where a.day >= 6 and b.month > 2 and a.tinyint_col = 15 and " +
        "             b.string_col = '15' and a.tinyint_col + b.tinyint_col < 15");
    AnalyzesOk("select * from (select a.smallint_col+b.smallint_col  c1" +
        "         from functional.alltypesagg a join functional.alltypessmall b " +
        "         using (id, int_col)) x " +
        "         where x.c1 > 100");
    AnalyzesOk("select a.* from" +
        " (select * from (select id+2 from functional_hbase.alltypessmall) b) a");
    AnalysisError("select * from " +
        "(select * from functional.alltypes a join " +
        "functional.alltypes b on (a.int_col = b.int_col)) x",
        "duplicated inline view column alias: 'id' in inline view 'x'");

    // subquery on the rhs of the join
    AnalyzesOk("select x.float_col " +
        "       from functional.alltypessmall c join " +
        "          (select a.smallint_col smallint_col, a.tinyint_col tinyint_col, " +
        "                   a.int_col int_col, b.float_col float_col" +
        "          from (select * from functional.alltypesagg a where month=1) a join " +
        "                  functional.alltypessmall b on (a.smallint_col = b.id)) x " +
        "            on (x.tinyint_col = c.id)");

    // aggregate test
    AnalyzesOk("select count(*) from (select count(id) from " +
               "functional.AllTypes group by id) a");
    AnalyzesOk("select count(a.x) from (select id+2 x " +
               "from functional_hbase.alltypessmall) a");
    AnalyzesOk("select * from (select id, zip " +
        "       from (select * from functional.testtbl) x " +
        "       group by zip, id having count(*) > 0) x");

    AnalysisError("select zip + count(*) from functional.testtbl",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");

    // union test
    AnalyzesOk("select a.* from " +
        "(select int_col from functional.alltypes " +
        " union all " +
        " select tinyint_col from functional.alltypessmall) a");
    AnalyzesOk("select a.* from " +
        "(select int_col from functional.alltypes " +
        " union all " +
        " select tinyint_col from functional.alltypessmall) a " +
        "union all " +
        "select smallint_col from functional.alltypes");
    AnalyzesOk("select a.* from " +
        "(select int_col from functional.alltypes " +
        " union all " +
        " select b.smallint_col from " +
        "  (select smallint_col from functional.alltypessmall" +
        "   union all" +
        "   select tinyint_col from functional.alltypes) b) a");
    // negative union test, column labels are inherited from first select block
    AnalysisError("select tinyint_col from " +
        "(select int_col from functional.alltypes " +
        " union all " +
        " select tinyint_col from functional.alltypessmall) a",
        "couldn't resolve column reference: 'tinyint_col'");

    // negative aggregate test
    AnalysisError("select * from " +
        "(select id, zip from functional.testtbl group by id having count(*) > 0) x",
        "select list expression not produced by aggregation output " +
            "(missing from GROUP BY clause?)");
    AnalysisError("select * from " +
        "(select id from functional.testtbl group by id having zip + count(*) > 0) x",
        "HAVING clause not produced by aggregation output " +
            "(missing from GROUP BY clause?)");
    AnalysisError("select * from " +
        "(select zip, count(*) from functional.testtbl group by 3) x",
        "GROUP BY: ordinal exceeds number of items in select list");
    AnalysisError("select * from " +
        "(select * from functional.alltypes group by 1) x",
        "cannot combine '*' in select list with GROUP BY");
    AnalysisError("select * from " +
        "(select zip, count(*) from functional.testtbl group by count(*)) x",
        "GROUP BY expression must not contain aggregate functions");
    AnalysisError("select * from " +
        "(select zip, count(*) from functional.testtbl group by count(*) + min(zip)) x",
        "GROUP BY expression must not contain aggregate functions");
    AnalysisError("select * from " +
        "(select zip, count(*) from functional.testtbl group by 2) x",
        "GROUP BY expression must not contain aggregate functions");

    // order by, top-n
    AnalyzesOk("select * from (select zip, count(*) " +
        "       from (select * from functional.testtbl) x " +
        "       group by 1 order by count(*) + min(zip) limit 5) x");
    AnalyzesOk("select c1, c2 from (select zip c1 , count(*) c2 " +
        "                     from (select * from functional.testtbl) x group by 1) x " +
        "        order by 2, 1 limit 5");

    // test NULLs
    AnalyzesOk("select * from (select NULL) a");

    // test that auto-generated columns are not used by default
    AnalyzesOk("select `int_col * 1`, a, int_col, `NOT bool_col` from " +
        "(select int_col * 1, int_col as a, int_col, !bool_col, concat(string_col) " +
        "from functional.alltypes) t");
    // test auto-generated column labels by enforcing their use in inline views
    AnalyzesOk("select _c0, a, int_col, _c3 from " +
        "(select int_col * 1, int_col as a, int_col, !bool_col, concat(string_col) " +
        "from functional.alltypes) t", createAnalyzerUsingHiveColLabels());
    // test auto-generated column labels in group by and order by
    AnalyzesOk("select _c0, count(a), count(int_col), _c3 from " +
        "(select int_col * 1, int_col as a, int_col, !bool_col, concat(string_col) " +
        "from functional.alltypes) t group by _c0, _c3 order by _c0 limit 10",
        createAnalyzerUsingHiveColLabels());
    // test auto-generated column labels in multiple scopes
    AnalyzesOk("select x.front, x._c1, x._c2 from " +
        "(select y.back as front, y._c0 * 10, y._c2 + 2 from " +
        "(select int_col * 10, int_col as back, int_col + 2 from " +
        "functional.alltypestiny) y) x",
        createAnalyzerUsingHiveColLabels());

    // ambiguous reference to an auto-generated column
    AnalysisError("select _c0 from " +
        "(select int_col * 2, id from functional.alltypes) a inner join " +
        "(select int_col + 6, id from functional.alltypes) b " +
        "on (a.id = b.id)",
        createAnalyzerUsingHiveColLabels(),
        "Unqualified column reference '_c0' is ambiguous");
    // auto-generated column doesn't exist
    AnalysisError("select _c0, a, _c2, _c3 from " +
        "(select int_col * 1, int_col as a, int_col, !bool_col, concat(string_col) " +
        "from functional.alltypes) t",
        createAnalyzerUsingHiveColLabels(),
        "couldn't resolve column reference: '_c2'");
  }

  @Test
  public void TestOnClause() throws AnalysisException {
    AnalyzesOk(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (a.int_col = b.int_col)");
    AnalyzesOk(
        "select a.int_col " +
        "from functional.alltypes a join functional.alltypes b on " +
        "(a.int_col = b.int_col and a.string_col = b.string_col)");
    AnalyzesOk(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (a.bool_col)");
    AnalyzesOk(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (NULL)");
    // ON or USING clause not required for inner join
    AnalyzesOk("select a.int_col from functional.alltypes a join functional.alltypes b");
    // arbitrary expr not returning bool
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on trim(a.string_col)",
        "ON clause 'trim(a.string_col)' requires return type 'BOOLEAN'. " +
        "Actual type is 'STRING'.");
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on a.int_col * b.float_col",
        "ON clause 'a.int_col * b.float_col' requires return type 'BOOLEAN'. " +
        "Actual type is 'DOUBLE'.");
    // unknown column
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (a.int_col = b.badcol)",
        "unknown column 'badcol'");
    // ambiguous col ref
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (int_col = int_col)",
        "Unqualified column reference 'int_col' is ambiguous");
    // unknown alias
    AnalysisError(
        "select a.int_col from functional.alltypes a join functional.alltypes b on " +
        "(a.int_col = badalias.int_col)",
        "unknown table alias: 'badalias'");
    // incompatible comparison
    AnalysisError(
        "select a.int_col from functional.alltypes a join " +
        "functional.alltypes b on a.bool_col = b.string_col",
        "operands are not comparable: a.bool_col = b.string_col");
    AnalyzesOk(
    "select a.int_col, b.int_col, c.int_col " +
        "from functional.alltypes a join functional.alltypes b on " +
        "(a.int_col = b.int_col and a.string_col = b.string_col)" +
        "join functional.alltypes c on " +
        "(b.int_col = c.int_col and b.string_col = c.string_col " +
        "and b.bool_col = c.bool_col)");
    // can't reference an alias that gets declared afterwards
    AnalysisError(
        "select a.int_col, b.int_col, c.int_col " +
        "from functional.alltypes a join functional.alltypes b on " +
        "(c.int_col = b.int_col and a.string_col = b.string_col)" +
        "join functional.alltypes c on " +
        "(b.int_col = c.int_col and b.string_col = c.string_col " +
        "and b.bool_col = c.bool_col)",
        "unknown table alias: 'c'");

    // outer joins require ON/USING clause
    AnalyzesOk("select * from functional.alltypes a left outer join " +
        "functional.alltypes b on (a.id = b.id)");
    AnalyzesOk("select * from functional.alltypes a left outer join " +
        "functional.alltypes b using (id)");
    AnalysisError("select * from functional.alltypes a " +
        "left outer join functional.alltypes b",
        "LEFT OUTER JOIN requires an ON or USING clause");
    AnalyzesOk("select * from functional.alltypes a right outer join " +
        "functional.alltypes b on (a.id = b.id)");
    AnalyzesOk("select * from functional.alltypes a right outer join " +
        "functional.alltypes b using (id)");
    AnalysisError("select * from functional.alltypes a " +
        "right outer join functional.alltypes b",
        "RIGHT OUTER JOIN requires an ON or USING clause");
    AnalyzesOk("select * from functional.alltypes a full outer join " +
        "functional.alltypes b on (a.id = b.id)");
    AnalyzesOk("select * from functional.alltypes a full outer join " +
        "functional.alltypes b using (id)");
    AnalysisError("select * from functional.alltypes a full outer join " +
        "functional.alltypes b",
        "FULL OUTER JOIN requires an ON or USING clause");

    // semi join requires ON/USING clause
    AnalyzesOk("select a.id from functional.alltypes a left semi join " +
        "functional.alltypes b on (a.id = b.id)");
    AnalyzesOk("select a.id from functional.alltypes a left semi join " +
        "functional.alltypes b using (id)");
    AnalysisError("select a.id from functional.alltypes a " +
        "left semi join functional.alltypes b",
        "LEFT SEMI JOIN requires an ON or USING clause");
    // TODO: enable when implemented
    // must not reference semi-joined alias outside of join clause
    // AnalysisError(
    // "select a.id, b.id from alltypes a left semi join alltypes b on (a.id = b.id)",
    // "x");
  }

  @Test
  public void TestUsingClause() throws AnalysisException {
    AnalyzesOk("select a.int_col, b.int_col from functional.alltypes a join " +
        "functional.alltypes b using (int_col)");
    AnalyzesOk("select a.int_col, b.int_col from " +
        "functional.alltypes a join functional.alltypes b " +
        "using (int_col, string_col)");
    AnalyzesOk(
        "select a.int_col, b.int_col, c.int_col " +
        "from functional.alltypes a " +
        "join functional.alltypes b using (int_col, string_col) " +
        "join functional.alltypes c using (int_col, string_col, bool_col)");
    // unknown column
    AnalysisError("select a.int_col from functional.alltypes a " +
        "join functional.alltypes b using (badcol)",
        "unknown column badcol for alias a");
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
         "join functional.alltypes b using (int_col, badcol)",
        "unknown column badcol for alias a ");
  }

  @Test
  public void TestJoinHints() throws AnalysisException {
    AnalyzesOk("select * from functional.alltypes a join [broadcast] " +
        "functional.alltypes b using (int_col)");
    AnalyzesOk("select * from functional.alltypes a join [shuffle] " +
        "functional.alltypes b using (int_col)");
    AnalyzesOk("select * from functional.alltypes a cross join [broadcast] " +
        "functional.alltypes b");
    AnalysisError(
        "select * from functional.alltypes a join [broadcast,shuffle] " +
         "functional.alltypes b using (int_col)",
        "Conflicting JOIN hint: shuffle");
    AnalysisError(
        "select * from functional.alltypes a join [bla] " +
         "functional.alltypes b using (int_col)",
        "JOIN hint not recognized: bla");
    AnalysisError("select * from functional.alltypes a cross join [shuffle] " +
        "functional.alltypes b",
        "CROSS JOIN does not support SHUFFLE.");
    AnalysisError("select * from functional.alltypes a right outer join [broadcast] " +
        "functional.alltypes b using (int_col)",
        "RIGHT OUTER JOIN does not support BROADCAST.");
    AnalysisError("select * from functional.alltypes a full outer join [broadcast] " +
        "functional.alltypes b using (int_col)",
        "FULL OUTER JOIN does not support BROADCAST.");
  }

  @Test
  public void TestWhereClause() throws AnalysisException {
    AnalyzesOk("select zip, name from functional.testtbl where id > 15");
    AnalysisError("select zip, name from functional.testtbl where badcol > 15",
        "couldn't resolve column reference");
    AnalyzesOk("select * from functional.testtbl where true");
    AnalysisError("select * from functional.testtbl where count(*) > 0",
        "aggregate function not allowed in WHERE clause");
    // NULL and bool literal in binary predicate.
    for (BinaryPredicate.Operator op : BinaryPredicate.Operator.values()) {
      AnalyzesOk("select id from functional.testtbl where id " +
          op.toString() + " true");
      AnalyzesOk("select id from functional.testtbl where id " +
          op.toString() + " false");
      AnalyzesOk("select id from functional.testtbl where id " +
          op.toString() + " NULL");
    }
    // Where clause is a SlotRef of type bool.
    AnalyzesOk("select id from functional.alltypes where bool_col");
    // Arbitrary exprs that do not return bool.
    AnalysisError("select id from functional.alltypes where int_col",
        "WHERE clause requires return type 'BOOLEAN'. Actual type is 'INT'.");
    AnalysisError("select id from functional.alltypes where trim('abc')",
        "WHERE clause requires return type 'BOOLEAN'. Actual type is 'STRING'.");
    AnalysisError("select id from functional.alltypes where (int_col + float_col) * 10",
        "WHERE clause requires return type 'BOOLEAN'. Actual type is 'DOUBLE'.");
  }

  @Test
  public void TestFunctions() throws AnalysisException {
    // Test with partition columns and substitution
    AnalyzesOk("select year(timestamp_col), count(*) " +
        "from functional.alltypes group by 1");
    AnalyzesOk("select year(timestamp_col), count(*) " +
        "from functional.alltypes group by year(timestamp_col)");
  }

  void addTestUda(String name, PrimitiveType retType, PrimitiveType... argTypes) {
    FunctionName fnName = new FunctionName("default", name);
    catalog_.addFunction(new Uda(fnName, new FunctionArgs(Lists.newArrayList(argTypes),
        false), retType));
  }

  @Test
  public void TestAggregates() throws AnalysisException {
    // Add udas:
    //   bigint AggFn(int)
    //   bigint AggFn(bigint)
    //   bigint AggFn(double)
    //   string AggFn(string, string)
    // TODO: if we could persist these in the catalog, we'd just use those
    // TODO: add cases where the intermediate type is not the return type when
    // the planner supports that.
    addTestUda("AggFn", PrimitiveType.BIGINT, PrimitiveType.INT);
    addTestUda("AggFn", PrimitiveType.BIGINT, PrimitiveType.BIGINT);
    addTestUda("AggFn", PrimitiveType.BIGINT, PrimitiveType.DOUBLE);
    addTestUda("AggFn", PrimitiveType.STRING,
        PrimitiveType.STRING, PrimitiveType.STRING);

    AnalyzesOk("select aggfn(int_col) from functional.alltypesagg");
    AnalysisError("select default.AggFn(1)",
        "aggregation without a FROM clause is not allowed");
    AnalyzesOk("select default.aggfn(int_col) from functional.alltypes");
    AnalyzesOk("select count(*) from functional.testtbl");
    AnalyzesOk("select min(id), max(id), sum(id) from functional.testtbl");
    AnalyzesOk("select avg(id) from functional.testtbl");

    AnalyzesOk("select count(*), min(id), max(id), sum(id), avg(id), aggfn(id) " +
        "from functional.testtbl");
    AnalyzesOk("select AggFn(tinyint_col), AggFn(int_col), AggFn(bigint_col), " +
        "AggFn(double_col) from functional.alltypes");
    AnalysisError("select AggFn(string_col) from functional.alltypes",
        "No matching function with signature: default.aggfn(STRING)");
    AnalyzesOk("select AggFn(string_col, string_col) from functional.alltypes");

    AnalyzesOk("select count(NULL), min(NULL), max(NULL), sum(NULL), avg(NULL), " +
        "group_concat(NULL), group_concat(name, NULL) from functional.testtbl");
    AnalysisError("select id, zip from functional.testtbl where count(*) > 0",
        "aggregate function not allowed in WHERE clause");
    AnalysisError("select 1 from functional.alltypes where aggfn(1)",
        "aggregate function not allowed in WHERE clause");

    // only count() allows '*'
    AnalysisError("select avg(*) from functional.testtbl",
        "'*' can only be used in conjunction with COUNT");
    AnalysisError("select min(*) from functional.testtbl",
        "'*' can only be used in conjunction with COUNT");
    AnalysisError("select max(*) from functional.testtbl",
        "'*' can only be used in conjunction with COUNT");
    AnalysisError("select group_concat(*) from functional.testtbl",
        "'*' can only be used in conjunction with COUNT");

    // multiple args
    AnalysisError("select count(id, zip) from functional.testtbl",
        "COUNT must have DISTINCT for multiple arguments: COUNT(id, zip)");
    AnalysisError("select min(id, zip) from functional.testtbl",
        "MIN requires exactly one parameter");
    AnalysisError("select max(id, zip) from functional.testtbl",
        "MAX requires exactly one parameter");
    AnalysisError("select sum(id, zip) from functional.testtbl",
        "SUM requires exactly one parameter");
    AnalysisError("select avg(id, zip) from functional.testtbl",
        "AVG requires exactly one parameter");
    AnalysisError("select group_concat(name, '-', ',') from functional.testtbl",
        "GROUP_CONCAT requires one or two parameters");

    // nested aggregates
    AnalysisError("select sum(count(*)) from functional.testtbl",
        "aggregate function cannot contain aggregate parameters");
    AnalysisError("select min(aggfn(int_col)) from functional.alltypes",
        "aggregate function cannot contain aggregate parameters: " +
        "MIN(default.aggfn(int_col))");

    // wrong type
    AnalysisError("select sum(timestamp_col) from functional.alltypes",
        "SUM requires a numeric parameter: SUM(timestamp_col)");
    AnalysisError("select sum(string_col) from functional.alltypes",
        "SUM requires a numeric parameter: SUM(string_col)");
    AnalysisError("select avg(string_col) from functional.alltypes",
        "AVG requires a numeric or timestamp parameter: AVG(string_col)");

    // aggregate requires table in the FROM clause
    AnalysisError("select count(*)", "aggregation without a FROM clause is not allowed");
    AnalysisError("select min(1)", "aggregation without a FROM clause is not allowed");
    AnalysisError("select group_concat('')",
        "aggregation without a FROM clause is not allowed");

    // test group_concat
    AnalyzesOk("select group_concat(string_col) from functional.alltypes");
    AnalyzesOk("select group_concat(string_col, '-') from functional.alltypes");
    AnalyzesOk("select group_concat(string_col, string_col) from functional.alltypes");
    // test all types as arguments
    for (PrimitiveType type: typeToLiteralValue_.keySet()) {
      String literal = typeToLiteralValue_.get(type);
      String query1 = String.format(
          "select group_concat(%s) from functional.alltypes", literal);
      String query2 = String.format(
          "select group_concat(string_col, %s) from functional.alltypes", literal);
      if (type == PrimitiveType.STRING || type == PrimitiveType.NULL_TYPE) {
        AnalyzesOk(query1);
        AnalyzesOk(query2);
      } else {
        AnalysisError(query1,
            "GROUP_CONCAT requires first parameter to be of type STRING");
        AnalysisError(query2,
            "GROUP_CONCAT requires second parameter to be of type STRING");
      }
    }

    // Test distinct estimate
    for (PrimitiveType type: typeToLiteralValue_.keySet()) {
      AnalyzesOk(String.format(
          "select distinctpc(%s) from functional.alltypes",
          typeToLiteralValue_.get(type)));
      AnalyzesOk(String.format(
          "select ndv(%s) from functional.alltypes",
          typeToLiteralValue_.get(type)));
    }
  }

  @Test
  public void TestDistinct() throws AnalysisException {
    AnalyzesOk("select count(distinct id) as sum_id from functional.testtbl");
    // DISTINCT
    AnalyzesOk("select count(distinct id) as sum_id from " +
        "functional.testtbl order by sum_id");
    AnalyzesOk("select count(distinct id) as sum_id from " +
        "functional.testtbl order by max(id)");
    AnalyzesOk("select distinct id, zip from functional.testtbl");
    AnalyzesOk("select distinct * from functional.testtbl");
    AnalysisError("select distinct count(*) from functional.testtbl",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip from functional.testtbl group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip, count(*) from " +
        "functional.testtbl group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalyzesOk("select count(distinct id, zip) from functional.testtbl");
    AnalysisError("select count(distinct id, zip), count(distinct zip) " +
        "from functional.testtbl",
        "all DISTINCT aggregate functions need to have the same set of parameters");
    AnalyzesOk("select tinyint_col, count(distinct int_col, bigint_col) "
        + "from functional.alltypesagg group by 1");
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "sum(distinct int_col) from functional.alltypesagg group by 1");
    AnalysisError("select tinyint_col, count(distinct int_col),"
        + "sum(distinct bigint_col) from functional.alltypesagg group by 1",
        "all DISTINCT aggregate functions need to have the same set of parameters");
    // min and max are ignored in terms of DISTINCT
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "min(distinct smallint_col), max(distinct string_col) "
        + "from functional.alltypesagg group by 1");
    AnalysisError("select group_concat(distinct name) from functional.testtbl",
            "GROUP_CONCAT does not support DISTINCT");
  }

  @Test
  public void TestDistinctInlineView() throws AnalysisException {
    // DISTINCT
    AnalyzesOk("select distinct id from " +
        "(select distinct id, zip from (select * from functional.testtbl) x) y");
    AnalyzesOk("select distinct * from " +
        "(select distinct * from (Select * from functional.testtbl) x) y");
    AnalyzesOk("select distinct * from (select count(*) from functional.testtbl) x");
    AnalyzesOk("select count(distinct id, zip) " +
        "from (select * from functional.testtbl) x");
    AnalyzesOk("select * from (select tinyint_col, count(distinct int_col, bigint_col) "
        + "from (select * from functional.alltypesagg) x group by 1) y");
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "sum(distinct int_col) from " +
        "(select * from functional.alltypesagg) x group by 1");

    // Error case when distinct is inside an inline view
    AnalysisError("select * from " +
        "(select distinct count(*) from functional.testtbl) x",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select * from " +
        "(select distinct id, zip from functional.testtbl group by 1, 2) x",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select * from " +
        "(select distinct id, zip, count(*) from functional.testtbl group by 1, 2) x",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select * from " +
        "(select count(distinct id, zip), count(distinct zip) " +
        "from functional.testtbl) x",
        "all DISTINCT aggregate functions need to have the same set of parameters");
    AnalysisError("select * from " + "(select tinyint_col, count(distinct int_col),"
        + "sum(distinct bigint_col) from functional.alltypesagg group by 1) x",
        "all DISTINCT aggregate functions need to have the same set of parameters");

    // Error case when inline view is in the from clause
    AnalysisError("select distinct count(*) from (select * from functional.testtbl) x",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip from " +
        "(select * from functional.testtbl) x group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip, count(*) from " +
        "(select * from functional.testtbl) x group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalyzesOk("select count(distinct id, zip) " +
        "from (select * from functional.testtbl) x");
    AnalysisError("select count(distinct id, zip), count(distinct zip) " +
        " from (select * from functional.testtbl) x",
        "all DISTINCT aggregate functions need to have the same set of parameters");
    AnalyzesOk("select tinyint_col, count(distinct int_col, bigint_col) "
        + "from (select * from functional.alltypesagg) x group by 1");
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "sum(distinct int_col) from " +
        "(select * from functional.alltypesagg) x group by 1");
    AnalysisError("select tinyint_col, count(distinct int_col),"
        + "sum(distinct bigint_col) from " +
        "(select * from functional.alltypesagg) x group by 1",
        "all DISTINCT aggregate functions need to have the same set of parameters");
  }

  @Test
  public void TestGroupBy() throws AnalysisException {
    AnalyzesOk("select zip, count(*) from functional.testtbl group by zip");
    AnalyzesOk("select zip + count(*) from functional.testtbl group by zip");
    // grouping on constants is ok and doesn't require them to be in select list
    AnalyzesOk("select count(*) from functional.testtbl group by 2*3+4");
    AnalyzesOk("select count(*) from functional.testtbl " +
        "group by true, false, NULL");
    // ok for constants in select list not to be in group by list
    AnalyzesOk("select true, NULL, 1*2+5 as a, zip, count(*) from functional.testtbl " +
        "group by zip");

    // doesn't group by all non-agg select list items
    AnalysisError("select zip, count(*) from functional.testtbl",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");
    AnalysisError("select zip + count(*) from functional.testtbl",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");

    // test having clause
    AnalyzesOk("select id, zip from functional.testtbl " +
        "group by zip, id having count(*) > 0");
    AnalyzesOk("select count(*) from functional.alltypes " +
        "group by bool_col having bool_col");
    // arbitrary exprs not returning boolean
    AnalysisError("select count(*) from functional.alltypes " +
        "group by bool_col having 5 + 10 * 5.6",
        "HAVING clause '5.0 + 10.0 * 5.6' requires return type 'BOOLEAN'. " +
        "Actual type is 'DOUBLE'.");
    AnalysisError("select count(*) from functional.alltypes " +
        "group by bool_col having int_col",
        "HAVING clause 'int_col' requires return type 'BOOLEAN'. Actual type is 'INT'.");
    AnalysisError("select id, zip from functional.testtbl " +
        "group by id having count(*) > 0",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");
    AnalysisError("select id from functional.testtbl " +
        "group by id having zip + count(*) > 0",
        "HAVING clause not produced by aggregation output " +
        "(missing from GROUP BY clause?)");
    // resolves ordinals
    AnalyzesOk("select zip, count(*) from functional.testtbl group by 1");
    AnalyzesOk("select count(*), zip from functional.testtbl group by 2");
    AnalysisError("select zip, count(*) from functional.testtbl group by 3",
        "GROUP BY: ordinal exceeds number of items in select list");
    AnalysisError("select * from functional.alltypes group by 1",
        "cannot combine '*' in select list with GROUP BY");
    // picks up select item alias
    AnalyzesOk("select zip z, id iD1, id ID2, count(*) " +
        "from functional.testtbl group by z, ID1, id2");
    // ambiguous alias
    AnalysisError("select zip a, id a, count(*) from functional.testtbl group by a",
        "Column a in group by clause is ambiguous");
    AnalysisError("select zip id, id, count(*) from functional.testtbl group by id",
        "Column id in group by clause is ambiguous");
    AnalysisError("select zip id, zip ID, count(*) from functional.testtbl group by id",
        "Column id in group by clause is ambiguous");


    // can't group by aggregate
    AnalysisError("select zip, count(*) from functional.testtbl group by count(*)",
        "GROUP BY expression must not contain aggregate functions");
    AnalysisError("select zip, count(*) " +
        "from functional.testtbl group by count(*) + min(zip)",
        "GROUP BY expression must not contain aggregate functions");
    AnalysisError("select zip, count(*) from functional.testtbl group by 2",
        "GROUP BY expression must not contain aggregate functions");

    // multiple grouping cols
    AnalyzesOk("select int_col, string_col, bigint_col, count(*) " +
        "from functional.alltypes group by string_col, int_col, bigint_col");
    AnalyzesOk("select int_col, string_col, bigint_col, count(*) " +
        "from functional.alltypes group by 2, 1, 3");
    AnalysisError("select int_col, string_col, bigint_col, count(*) " +
        "from functional.alltypes group by 2, 1, 4",
        "GROUP BY expression must not contain aggregate functions");

    // group by floating-point column
    AnalyzesOk("select float_col, double_col, count(*) " +
        "from functional.alltypes group by 1, 2");
    // group by floating-point exprs
    AnalyzesOk("select int_col + 0.5, count(*) from functional.alltypes group by 1");
    AnalyzesOk("select cast(int_col as double), count(*)" +
        "from functional.alltypes group by 1");
  }

  @Test
  public void TestOrderBy() throws AnalysisException {
    AnalyzesOk("select zip, id from functional.testtbl order by zip");
    AnalyzesOk("select zip, id from functional.testtbl order by zip asc");
    AnalyzesOk("select zip, id from functional.testtbl order by zip desc");
    AnalyzesOk("select zip, id from functional.testtbl " +
        "order by true asc, false desc, NULL asc");

    // resolves ordinals
    AnalyzesOk("select zip, id from functional.testtbl order by 1");
    AnalyzesOk("select zip, id from functional.testtbl order by 2 desc, 1 asc");
    // ordinal out of range
    AnalysisError("select zip, id from functional.testtbl order by 0",
        "ORDER BY: ordinal must be >= 1");
    AnalysisError("select zip, id from functional.testtbl order by 3",
        "ORDER BY: ordinal exceeds number of items in select list");
    // can't order by '*'
    AnalysisError("select * from functional.alltypes order by 1",
        "ORDER BY: ordinal refers to '*' in select list");
    // picks up select item alias
    AnalyzesOk("select zip z, id C, id D from functional.testtbl order by z, C, d");

    // can introduce additional aggregates in order by clause
    AnalyzesOk("select zip, count(*) from functional.testtbl group by 1 order by count(*)");
    AnalyzesOk("select zip, count(*) from functional.testtbl " +
        "group by 1 order by count(*) + min(zip)");
    AnalysisError("select zip, count(*) from functional.testtbl group by 1 order by id",
        "ORDER BY expression not produced by aggregation output " +
        "(missing from GROUP BY clause?)");

    // multiple ordering exprs
    AnalyzesOk("select int_col, string_col, bigint_col from functional.alltypes " +
               "order by string_col, 15.7 * float_col, int_col + bigint_col");
    AnalyzesOk("select int_col, string_col, bigint_col from functional.alltypes " +
               "order by 2, 1, 3");

    // ordering by floating-point exprs is okay
    AnalyzesOk("select float_col, int_col + 0.5 from functional.alltypes order by 1, 2");
    AnalyzesOk("select float_col, int_col + 0.5 from functional.alltypes order by 2, 1");

    // select-list item takes precedence
    AnalyzesOk("select t1.int_col from functional.alltypes t1, " +
        "functional.alltypes t2 where t1.id = t2.id order by int_col");

    // Ambiguous alias cause error
    AnalysisError("select string_col a, int_col a from " +
        "functional.alltypessmall order by a limit 1",
        "Column a in order clause is ambiguous");
    AnalysisError("select string_col a, int_col A from " +
        "functional.alltypessmall order by a limit 1",
        "Column a in order clause is ambiguous");
  }

  @Test
  public void TestUnion() {
    // Selects on different tables.
    AnalyzesOk("select int_col from functional.alltypes union " +
        "select int_col from functional.alltypessmall");
    // Selects on same table without aliases.
    AnalyzesOk("select int_col from functional.alltypes union " +
        "select int_col from functional.alltypes");
    // Longer union chain.
    AnalyzesOk("select int_col from functional.alltypes union " +
        "select int_col from functional.alltypes " +
        "union select int_col from functional.alltypes union " +
        "select int_col from functional.alltypes");
    // All columns, perfectly compatible.
    AnalyzesOk("select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year," +
        "month from functional.alltypes union " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year," +
        "month from functional.alltypes");
    // Make sure table aliases aren't visible across union operands.
    AnalyzesOk("select a.smallint_col from functional.alltypes a " +
        "union select a.int_col from functional.alltypessmall a");
    // All columns compatible with NULL.
    AnalyzesOk("select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year," +
        "month from functional.alltypes union " +
        "select NULL, NULL, NULL, NULL, NULL, NULL, " +
        "NULL, NULL, NULL, NULL, NULL, NULL," +
        "NULL from functional.alltypes");

    // No from clause. Has literals and NULLs. Requires implicit casts.
    AnalyzesOk("select 1, 2, 3 " +
        "union select NULL, NULL, NULL " +
        "union select 1.0, NULL, 3 " +
        "union select NULL, 10, NULL");
    // Implicit casts on integer types.
    AnalyzesOk("select tinyint_col from functional.alltypes " +
        "union select smallint_col from functional.alltypes " +
        "union select int_col from functional.alltypes " +
        "union select bigint_col from functional.alltypes");
    // Implicit casts on float types.
    AnalyzesOk("select float_col from functional.alltypes union " +
        "select double_col from functional.alltypes");
    // Implicit casts on all numeric types with two columns from each select.
    AnalyzesOk("select tinyint_col, double_col from functional.alltypes " +
        "union select smallint_col, float_col from functional.alltypes " +
        "union select int_col, bigint_col from functional.alltypes " +
        "union select bigint_col, int_col from functional.alltypes " +
        "union select float_col, smallint_col from functional.alltypes " +
        "union select double_col, tinyint_col from functional.alltypes");

    // With order by and limit.
    AnalyzesOk("(select int_col from functional.alltypes) " +
        "union (select tinyint_col from functional.alltypessmall) " +
        "order by int_col limit 1");
    // Bigger order by.
    AnalyzesOk("(select tinyint_col, double_col from functional.alltypes) " +
        "union (select smallint_col, float_col from functional.alltypes) " +
        "union (select int_col, bigint_col from functional.alltypes) " +
        "union (select bigint_col, int_col from functional.alltypes) " +
        "order by double_col, tinyint_col");
    // Bigger order by with ordinals.
    AnalyzesOk("(select tinyint_col, double_col from functional.alltypes) " +
        "union (select smallint_col, float_col from functional.alltypes) " +
        "union (select int_col, bigint_col from functional.alltypes) " +
        "union (select bigint_col, int_col from functional.alltypes) " +
        "order by 2, 1");

    // Unequal number of columns.
    AnalysisError("select int_col from functional.alltypes " +
        "union select int_col, float_col from functional.alltypes",
        "Operands have unequal number of columns:\n" +
        "'SELECT int_col FROM functional.alltypes' has 1 column(s)\n" +
        "'SELECT int_col, float_col FROM functional.alltypes' has 2 column(s)");
    // Unequal number of columns, longer union chain.
    AnalysisError("select int_col from functional.alltypes " +
        "union select tinyint_col from functional.alltypes " +
        "union select smallint_col from functional.alltypes " +
        "union select smallint_col, bigint_col from functional.alltypes",
        "Operands have unequal number of columns:\n" +
        "'SELECT int_col FROM functional.alltypes' has 1 column(s)\n" +
        "'SELECT smallint_col, bigint_col FROM functional.alltypes' has 2 column(s)");
    // Incompatible types.
    AnalysisError("select bool_col from functional.alltypes " +
        "union select string_col from functional.alltypes",
        "Incompatible return types 'BOOLEAN' and 'STRING' " +
            "of exprs 'bool_col' and 'string_col'.");
    // Incompatible types, longer union chain.
    AnalysisError("select int_col, string_col from functional.alltypes " +
        "union select tinyint_col, bool_col from functional.alltypes " +
        "union select smallint_col, int_col from functional.alltypes " +
        "union select smallint_col, bool_col from functional.alltypes",
        "Incompatible return types 'STRING' and 'BOOLEAN' of " +
            "exprs 'string_col' and 'bool_col'.");
    // Invalid ordinal in order by.
    AnalysisError("(select int_col from functional.alltypes) " +
        "union (select int_col from functional.alltypessmall) order by 2",
        "ORDER BY: ordinal exceeds number of items in select list: 2");
    // Ambiguous order by.
    AnalysisError("(select int_col a, string_col a from functional.alltypes) " +
        "union (select int_col a, string_col a " +
        "from functional.alltypessmall) order by a",
        "Column a in order clause is ambiguous");
    // Ambiguous alias in the second union operand should work.
    AnalyzesOk("(select int_col a, string_col b from functional.alltypes) " +
        "union (select int_col a, string_col a " +
        "from functional.alltypessmall) order by a");

    // Column labels are inherited from first select block.
    // Order by references an invalid column
    AnalysisError("(select smallint_col from functional.alltypes) " +
        "union (select int_col from functional.alltypessmall) order by int_col",
        "couldn't resolve column reference: 'int_col'");
    // Make sure table aliases aren't visible across union operands.
    AnalysisError("select a.smallint_col from functional.alltypes a " +
        "union select a.int_col from functional.alltypessmall",
        "unknown table alias: 'a'");
  }

  @Test
  public void TestValuesStmt() throws AnalysisException {
    // Values stmt with a single row.
    AnalyzesOk("values(1, 2, 3)");
    AnalyzesOk("select * from (values('a', NULL, 'c')) as t");
    AnalyzesOk("values(1.0, 2, NULL) union all values(1, 2.0, 3)");
    AnalyzesOk("insert overwrite table functional.alltypes " +
        "partition (year=2009, month=10)" +
        "values(1, true, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a', cast(0 as timestamp))");
    AnalyzesOk("insert overwrite table functional.alltypes " +
        "partition (year, month) " +
        "values(1, true, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a', cast(0 as timestamp)," +
        "2009, 10)");
    // Values stmt with multiple rows.
    AnalyzesOk("values((1, 2, 3), (4, 5, 6))");
    AnalyzesOk("select * from (values('a', 'b', 'c')) as t");
    AnalyzesOk("select * from (values(('a', 'b', 'c'), ('d', 'e', 'f'))) as t");
    AnalyzesOk("values((1.0, 2, NULL), (2.0, 3, 4)) union all values(1, 2.0, 3)");
    AnalyzesOk("insert overwrite table functional.alltypes " +
        "partition (year=2009, month=10) " +
        "values(" +
        "(1, true, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a', cast(0 as timestamp))," +
        "(2, false, 2, 2, NULL, 2, 2.0, 2.0, 'b', 'b', cast(0 as timestamp))," +
        "(3, true, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c', cast(0 as timestamp)))");
    AnalyzesOk("insert overwrite table functional.alltypes " +
        "partition (year, month) " +
        "values(" +
        "(1, true, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a', cast(0 as timestamp), 2009, 10)," +
        "(2, false, 2, 2, NULL, 2, 2.0, 2.0, 'b', 'b', cast(0 as timestamp), 2009, 2)," +
        "(3, true, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c', cast(0 as timestamp), 2009, 3))");
    // Test multiple aliases. Values() is like union, the column labels are 'x' and 'y'.
    AnalyzesOk("values((1 as x, 'a' as y), (2 as k, 'b' as j))");
    // Test order by and limit.
    AnalyzesOk("values(1 as x, 'a') order by 2 limit 10");
    AnalyzesOk("values(1 as x, 'a' as y), (2, 'b') order by y limit 10");
    AnalyzesOk("values((1, 'a'), (2, 'b')) order by 1 limit 10");

    AnalysisError("values(1, 'a', 1.0, *)",
        "'*' expression in select list requires FROM clause.");
    AnalysisError("values(sum(1), 'a', 1.0)",
        "aggregation without a FROM clause is not allowed");
    AnalysisError("values(1, id, 2)",
        "couldn't resolve column reference: 'id'");
    AnalysisError("values((1 as x, 'a' as y), (2, 'b')) order by c limit 1",
        "couldn't resolve column reference: 'c'");
    AnalysisError("values((1, 2), (3, 4, 5))",
        "Operands have unequal number of columns:\n" +
        "'(1, 2)' has 2 column(s)\n" +
        "'(3, 4, 5)' has 3 column(s)");
    AnalysisError("values((1, 'a'), (3, 4))",
        "Incompatible return types 'STRING' and 'TINYINT' of exprs ''a'' and '4'");
    AnalysisError("insert overwrite table functional.alltypes " +
        "partition (year, month) " +
        "values(1, true, 'a', 1, 1, 1, 1.0, 1.0, 'a', 'a', cast(0 as timestamp)," +
        "2009, 10)",
        "Target table 'functional.alltypes' is incompatible with SELECT / PARTITION " +
        "expressions.\n" +
        "Expression ''a'' (type: STRING) is not compatible with column " +
        "'tinyint_col' (type: TINYINT)");
  }

  @Test
  public void TestWithClause() throws AnalysisException {
    // Single view in WITH clause.
    AnalyzesOk("with t as (select int_col x, bigint_col y from functional.alltypes)" +
        "select x, y from t");
    // Multiple views in WITH clause. Only one view is used.
    AnalyzesOk("with t1 as (select int_col x, bigint_col y from functional.alltypes)," +
        "t2 as (select 1 x , 10 y), t3 as (values(2 x , 20 y), (3, 30)), " +
        "t4 as (select 4 x, 40 y union all select 5, 50), " +
        "t5 as (select * from (values(6 x, 60 y)) as a) " +
        "select x, y from t3");
    // Multiple views in WITH clause. All views used in a union.
    AnalyzesOk("with t1 as (select int_col x, bigint_col y from functional.alltypes)," +
        "t2 as (select 1 x , 10 y), t3 as (values(2 x , 20 y), (3, 30)), " +
        "t4 as (select 4 x, 40 y union all select 5, 50), " +
        "t5 as (select * from (values(6 x, 60 y)) as a) " +
        "select * from t1 union all select * from t2 union all select * from t3 " +
        "union all select * from t4 union all select * from t5");
    // Multiple views in WITH clause. All views used in a join.
    AnalyzesOk("with t1 as (select int_col x, bigint_col y from functional.alltypes)," +
        "t2 as (select 1 x , 10 y), t3 as (values(2 x , 20 y), (3, 30)), " +
        "t4 as (select 4 x, 40 y union all select 5, 50), " +
        "t5 as (select * from (values(6 x, 60 y)) as a) " +
        "select t1.y, t2.y, t3.y, t4.y, t5.y from t1, t2, t3, t4, t5 " +
        "where t1.y = t2.y and t2.y = t3.y and t3.y = t4.y and t4.y = t5.y");
    // WITH clause in insert statement.
    AnalyzesOk("with t1 as (select * from functional.alltypestiny)" +
        "insert into functional.alltypes partition(year, month) select * from t1");
    // WITH-clause view used in inline view.
    AnalyzesOk("with t1 as (select 'a') select * from (select * from t1) as t2");
    AnalyzesOk("with t1 as (select 'a') " +
        "select * from (select * from (select * from t1) as t2) as t3");
    // WITH-clause inside inline view.
    AnalyzesOk("select * from (with t1 as (values(1 x, 10 y)) select * from t1) as t2");

    // Test case-insensitive matching of WITH-clause views to base table refs.
    AnalyzesOk("with T1 as (select int_col x, bigint_col y from functional.alltypes)," +
        "t2 as (select 1 x , 10 y), T3 as (values(2 x , 20 y), (3, 30)), " +
        "t4 as (select 4 x, 40 y union all select 5, 50), " +
        "T5 as (select * from (values(6 x, 60 y)) as a) " +
        "select * from t1 union all select * from T2 union all select * from t3 " +
        "union all select * from T4 union all select * from t5");

    // Multiple WITH clauses. One for the UnionStmt and one for each union operand.
    AnalyzesOk("with t1 as (values('a', 'b')) " +
        "(with t2 as (values('c', 'd')) select * from t2) union all" +
        "(with t3 as (values('e', 'f')) select * from t3) order by 1 limit 1");
    // Multiple WITH clauses. One before the insert and one inside the query statement.
    AnalyzesOk("with t1 as (select * from functional.alltypestiny) " +
        "insert into functional.alltypes partition(year, month) " +
        "with t2 as (select * from functional.alltypessmall) select * from t1");

    // Table aliases do not conflict because they are in different scopes.
    // Aliases are resolved from inner-most to the outer-most scope.
    AnalyzesOk("with t1 as (select 'a') " +
        "select t2.* from (with t1 as (select 'b') select * from t1) as t2");
    // Table aliases do not conflict because t1 from the inline view is never used.
    AnalyzesOk("with t1 as (select 1), t2 as (select 2)" +
        "select * from functional.alltypes as t1");
    AnalyzesOk("with t1 as (select 1), t2 as (select 2) select * from t2 as t1");
    AnalyzesOk("with t1 as (select 1) select * from (select 2) as t1");
    // Fully-qualified table does not conflict with WITH-clause table.
    AnalyzesOk("with alltypes as (select * from functional.alltypes) " +
        "select * from functional.alltypes union all select * from alltypes");

    // Use a custom analyzer to change the default db to functional.
    // Recursion is prevented because 'alltypes' in t1 refers to the table
    // functional.alltypes, and 'alltypes' in the final query refers to the
    // view 'alltypes'.
    AnalyzesOk("with t1 as (select int_col x, bigint_col y from alltypes), " +
        "alltypes as (select x a, y b from t1)" +
        "select a, b from alltypes",
        createAnalyzer("functional"));
    // Recursion is prevented because of scoping rules. The inner 'complex_view'
    // refers to a view in the catalog.
    AnalyzesOk("with t1 as (select abc x, xyz y from complex_view), " +
        "complex_view as (select x a, y b from t1)" +
        "select a, b from complex_view",
        createAnalyzer("functional"));
    // Nested WITH clauses. Scoping prevents recursion.
    AnalyzesOk("with t1 as (with t1 as (select int_col x, bigint_col y from alltypes) " +
        "select x, y from t1), " +
        "alltypes as (select x a, y b from t1) " +
        "select a, b from alltypes",
        createAnalyzer("functional"));
    // Nested WITH clause inside a subquery.
    AnalyzesOk("with t1 as " +
        "(select * from (with t2 as (select * from functional.alltypes) " +
        "select * from t2) t3) " +
        "select * from t1");
    // Nested WITH clause inside a union stmt.
    AnalyzesOk("with t1 as " +
        "(with t2 as (values('a', 'b')) select * from t2 union all select * from t2) " +
        "select * from t1");
    // Nested WITH clause inside a union stmt's operand.
    AnalyzesOk("with t1 as " +
        "(select 'x', 'y' union all (with t2 as (values('a', 'b')) select * from t2)) " +
        "select * from t1");

    // Single WITH clause. Multiple references to same view.
    AnalyzesOk("with t as (select 1 x)" +
        "select x from t union all select x from t");
    // Multiple references in same select statement require aliases.
    AnalyzesOk("with t as (select 'a' x)" +
        "select t1.x, t2.x, t.x from t as t1, t as t2, t " +
        "where t1.x = t2.x and t2.x = t.x");

    // Test column labels in WITH-clause view for non-SlotRef exprs.
    AnalyzesOk("with t as (select int_col + 2, !bool_col from functional.alltypes) " +
        "select `int_col + 2`, `NOT bool_col` from t");

    // Conflicting table aliases in WITH clause.
    AnalysisError("with t1 as (select 1), t1 as (select 2) select * from t1",
        "Duplicate table alias: 't1'");
    AnalysisError("with t1 as (select * from functional.alltypestiny) " +
        "insert into functional.alltypes partition(year, month) " +
        "with t1 as (select * from functional.alltypessmall) select * from t1",
        "Duplicate table alias: 't1");
    // Aliases conflict because t1 from the inline view is used.
    AnalysisError("with t1 as (select 1 x), t2 as (select 2 y)" +
        "select * from functional.alltypes as t1 inner join t1",
        "Duplicate table alias: 't1'");
    AnalysisError("with t1 as (select 1), t2 as (select 2) " +
        "select * from t2 as t1 inner join t1",
        "Duplicate table alias: 't1'");
    AnalysisError("with t1 as (select 1) select * from (select 2) as t1 inner join t1",
        "Duplicate table alias: 't1'");
    // Multiple references in same select statement require aliases.
    AnalysisError("with t1 as (select 'a' x) select * from t1 inner join t1",
        "Duplicate table alias: 't1'");
    // If one was given, we must use the explicit alias for column references.
    AnalysisError("with t1 as (select 'a' x) select t1.x from t1 as t2",
        "unknown table alias: 't1'");
    // WITH-clause tables cannot be inserted into.
    AnalysisError("with t1 as (select 'a' x) insert into t1 values('b' x)",
        "Table does not exist: default.t1");

    // Recursive table references are not allowed.
    AnalysisError("with t as (select int_col x, bigint_col y from t) " +
        "select x, y from t",
        "Unsupported recursive reference to table 't' in WITH clause.");
    AnalysisError("with t as (select 1 as x, 2 as y union all select * from t) " +
        "select x, y from t",
        "Unsupported recursive reference to table 't' in WITH clause.");
    AnalysisError("with t as (select a.* from (select * from t) as a) " +
        "select x, y from t",
        "Unsupported recursive reference to table 't' in WITH clause.");
    // Recursive table reference is still recognized as such if a name from
    // a view in the catalog is used.
    AnalysisError("with alltypes_view as (select int_col x from alltypes_view) " +
        "select x from alltypes_view",
        createAnalyzer("functional"),
        "Unsupported recursive reference to table 'alltypes_view' in WITH clause.");
    // Recursion in nested WITH clause.
    AnalysisError("with t1 as (with t2 as (select * from t1) select * from t2) " +
        "select * from t1 ",
        "Unsupported recursive reference to table 't1' in WITH clause.");
    // Recursion in nested WITH clause inside a subquery.
    AnalysisError("with t1 as " +
        "(select * from (with t2 as (select * from t1) select * from t2) t3) " +
        "select * from t1",
        "Unsupported recursive reference to table 't1' in WITH clause.");
    // Recursion with a union's WITH clause.
    AnalysisError("with t1 as " +
        "(with t2 as (select * from t1) select * from t2 union all select * from t2)" +
        "select * from t1",
        "Unsupported recursive reference to table 't1' in WITH clause.");
    // Recursion with a union operand's WITH clause.
    AnalysisError("with t1 as " +
        "(select 'x', 'y' union all (with t2 as (select * from t1) select * from t2))" +
        "select * from t1",
        "Unsupported recursive reference to table 't1' in WITH clause.");
    // Recursion is prevented because a view definition may only reference
    // views to its left.
    AnalysisError("with t1 as (select int_col x, bigint_col y from t2), " +
        "t2 as (select int_col x, bigint_col y from t1) select x, y from t1",
        "Table does not exist: default.t2");
  }

  @Test
  public void TestViews() throws AnalysisException {
    // Simple selects on our pre-defined views.
    AnalyzesOk("select * from functional.alltypes_view");
    AnalyzesOk("select x, y, z from functional.alltypes_view_sub");
    AnalyzesOk("select abc, xyz from functional.complex_view");
    // Test a view on a view.
    AnalyzesOk("select * from functional.view_view");
    // Aliases of views.
    AnalyzesOk("select t.x, t.y, t.z from functional.alltypes_view_sub t");

    // Views in a union.
    AnalyzesOk("select * from functional.alltypes_view_sub union all " +
        "select * from functional.alltypes_view_sub");
    // View in a subquery.
    AnalyzesOk("select t.* from (select * from functional.alltypes_view_sub) t");
    // View in a WITH-clause view.
    AnalyzesOk("with t as (select * from functional.complex_view) " +
        "select abc, xyz from t");

    // Complex query on a complex view with a join and an aggregate.
    AnalyzesOk("select sum(t1.abc), t2.xyz from functional.complex_view t1 " +
        "inner join functional.complex_view t2 on (t1.abc = t2.abc) " +
        "group by t2.xyz");

    // Cannot insert into a view.
    AnalysisError("insert into functional.alltypes_view partition(year, month) " +
        "select * from functional.alltypes",
        "Impala does not support inserting into views: functional.alltypes_view");
    // Cannot load into a view.
    AnalysisError("load data inpath '/test-warehouse/tpch.lineitem/lineitem.tbl' " +
        "into table functional.alltypes_view",
        "LOAD DATA only supported for HDFS tables: functional.alltypes_view");
    // Need to give view-references an explicit alias.
    AnalysisError("select * from functional.alltypes_view_sub " +
        "inner join functional.alltypes_view_sub",
        "Duplicate table alias: 'functional.alltypes_view_sub'");
    // Column names were redefined in view.
    AnalysisError("select int_col from functional.alltypes_view_sub",
        "couldn't resolve column reference: 'int_col'");
  }

  @Test
  public void TestLoadData() throws AnalysisException {
    for (String overwrite: Lists.newArrayList("", "overwrite")) {
      // Load specific data file.
      AnalyzesOk(String.format("load data inpath '%s' %s into table tpch.lineitem",
          "/test-warehouse/tpch.lineitem/lineitem.tbl", overwrite));

      // Load files from a data directory.
      AnalyzesOk(String.format("load data inpath '%s' %s into table tpch.lineitem",
          "/test-warehouse/tpch.lineitem/", overwrite));

      // Load files from a data directory into a partition.
      AnalyzesOk(String.format("load data inpath '%s' %s into table " +
          "functional.alltypes partition(year=2009, month=12)",
          "/test-warehouse/tpch.lineitem/", overwrite));

      // Source directory cannot contain subdirs.
      AnalysisError(String.format("load data inpath '%s' %s into table tpch.lineitem",
          "/test-warehouse/", overwrite),
          "INPATH location 'hdfs://localhost:20500/test-warehouse' cannot " +
          "contain subdirectories.");

      // Source directory cannot be empty.
      AnalysisError(String.format("load data inpath '%s' %s into table tpch.lineitem",
          "/test-warehouse/emptytable", overwrite),
          "INPATH location 'hdfs://localhost:20500/test-warehouse/emptytable' " +
          "contains no visible files.");

      // Cannot load a hidden files.
      AnalysisError(String.format("load data inpath '%s' %s into table tpch.lineitem",
          "/test-warehouse/alltypessmall/year=2009/month=1/.hidden", overwrite),
          "INPATH location 'hdfs://localhost:20500/test-warehouse/alltypessmall/" +
          "year=2009/month=1/.hidden' points to a hidden file.");
      AnalysisError(String.format("load data inpath '%s' %s into table tpch.lineitem",
          "/test-warehouse/alltypessmall/year=2009/month=1/_hidden", overwrite),
          "INPATH location 'hdfs://localhost:20500/test-warehouse/alltypessmall/" +
          "year=2009/month=1/_hidden' points to a hidden file.");

      // Source directory does not exist.
      AnalysisError(String.format("load data inpath '%s' %s into table tpch.lineitem",
          "/test-warehouse/does_not_exist", overwrite),
          "INPATH location 'hdfs://localhost:20500/test-warehouse/does_not_exist' " +
          "does not exist.");
      // Empty source directory string
      AnalysisError(String.format("load data inpath '%s' %s into table tpch.lineitem",
          "", overwrite), "URI path cannot be empty.");

      // Partition spec does not exist in table.
      AnalysisError(String.format("load data inpath '%s' %s into table " +
          "functional.alltypes partition(year=123, month=10)",
          "/test-warehouse/tpch.lineitem/", overwrite),
          "Partition spec does not exist: (year=123, month=10)");

      // Cannot load into non-HDFS tables.
      AnalysisError(String.format("load data inpath '%s' %s into table " +
          "functional_hbase.alltypessmall",
          "/test-warehouse/tpch.lineitem/", overwrite),
          "LOAD DATA only supported for HDFS tables: functional_hbase.alltypessmall");

      // Load into partitioned table without specifying a partition spec.
      AnalysisError(String.format("load data inpath '%s' %s into table " +
          "functional.alltypes",
          "/test-warehouse/tpch.lineitem/", overwrite),
          "Table is partitioned but no partition spec was specified: " +
          "functional.alltypes");

      // Database/table do not exist.
      AnalysisError(String.format("load data inpath '%s' %s into table " +
          "nodb.alltypes",
          "/test-warehouse/tpch.lineitem/", overwrite),
          "Database does not exist: nodb");
      AnalysisError(String.format("load data inpath '%s' %s into table " +
          "functional.notbl",
          "/test-warehouse/tpch.lineitem/", overwrite),
          "Table does not exist: functional.notbl");

      AnalysisError(String.format("load data inpath '%s' %s into table " +
          "tpch.lineitem", "file:///test-warehouse/test.out", overwrite),
          "URI location 'file:/test-warehouse/test.out' must point to an " +
          "HDFS file system");

      // File type / table type mismatch.
      AnalysisError(String.format("load data inpath '%s' %s into table " +
          "tpch.lineitem",
          "/test-warehouse/alltypes_text_lzo/year=2009/month=4", overwrite),
          "Compressed file not supported without compression input format: " +
          "hdfs://localhost:20500/test-warehouse/alltypes_text_lzo/" +
          "year=2009/month=4/000021_0.lzo");
      // When table type matches, analysis passes for partitioned and unpartitioned
      // tables.
      AnalyzesOk(String.format("load data inpath '%s' %s into table " +
          "functional_text_lzo.alltypes partition(year=2009, month=4)",
          "/test-warehouse/alltypes_text_lzo/year=2009/month=4", overwrite));
      AnalyzesOk(String.format("load data inpath '%s' %s into table " +
          "functional_text_lzo.jointbl",
          "/test-warehouse/alltypes_text_lzo/year=2009/month=4", overwrite));

      // Verify with a read-only table
      AnalysisError(String.format("load data inpath '%s' into table " +
          "functional_seq.alltypes partition(year=2009, month=3)",
          "/test-warehouse/alltypes_seq/year=2009/month=5", overwrite),
          "Unable to LOAD DATA into target table (functional_seq.alltypes) because " +
          "Impala does not have WRITE access to HDFS location: " +
          "hdfs://localhost:20500/test-warehouse/alltypes_seq/year=2009/month=3");
    }
  }

  @Test
  public void TestInsert() throws AnalysisException {
    for (String qualifier: ImmutableList.of("INTO", "OVERWRITE")) {
      testInsertStatic(qualifier);
      testInsertDynamic(qualifier);
      testInsertUnpartitioned(qualifier);
      testInsertWithPermutation(qualifier);
    }

    // Test INSERT into a table that Impala does not have WRITE access to.
    AnalysisError("insert into functional_seq.alltypes partition(year, month)" +
        "select * from functional.alltypes",
        "Unable to INSERT into target table (functional_seq.alltypes) because Impala " +
        "does not have WRITE access to at least one HDFS path: " +
        "hdfs://localhost:20500/test-warehouse/alltypes_seq/year=2009/month=1");

    // Test plan hints for partitioned Hdfs tables.
    AnalyzesOk("insert into functional.alltypessmall " +
        "partition (year, month) [shuffle] select * from functional.alltypes");
    AnalyzesOk("insert into table functional.alltypessmall " +
        "partition (year, month) [noshuffle] select * from functional.alltypes");
    // Multiple non-conflicting hints and case insensitivity of hints.
    AnalyzesOk("insert into table functional.alltypessmall " +
        "partition (year, month) [shuffle, ShUfFlE] select * from functional.alltypes");
    // Unknown plan hint,
    AnalysisError("insert into functional.alltypessmall " +
        "partition (year, month) [badhint] select * from functional.alltypes",
        "INSERT hint not recognized: badhint");
    // Conflicting plan hints.
    AnalysisError("insert into table functional.alltypessmall " +
        "partition (year, month) [shuffle, noshuffle] select * from functional.alltypes",
        "Conflicting INSERT hint: noshuffle");
    // Plan hints require a partition clause.
    AnalysisError("insert into table functional.alltypesnopart [shuffle] " +
        "select * from functional.alltypesnopart",
        "INSERT hints are only supported for inserting into partitioned Hdfs tables.");
    // Plan hints do not make sense for inserting into HBase tables.
    AnalysisError("insert into table functional_hbase.alltypes [shuffle] " +
        "select * from functional_hbase.alltypes",
        "INSERT hints are only supported for inserting into partitioned Hdfs tables.");
  }

  /**
   * Run tests for dynamic partitions for INSERT INTO/OVERWRITE.
   */
  private void testInsertDynamic(String qualifier) throws AnalysisException {
    // Fully dynamic partitions.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year, " +
        "month from functional.alltypes");
    // Fully dynamic partitions with NULL literals as partitioning columns.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, " +
        "string_col, timestamp_col, NULL, NULL from functional.alltypes");
    // Fully dynamic partitions with NULL partition keys and column values.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year, month)" +
        "select NULL, NULL, NULL, NULL, NULL, NULL, " +
        "NULL, NULL, NULL, NULL, NULL, NULL, " +
        "NULL from functional.alltypes");
    // Fully dynamic partitions. Order of corresponding select list items doesn't matter,
    // as long as they appear at the very end of the select list.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, month, " +
        "year from functional.alltypes");
    // Partially dynamic partitions.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, month " +
        "from functional.alltypes");
    // Partially dynamic partitions with NULL static partition key value.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=NULL, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year from " +
        "functional.alltypes");
    // Partially dynamic partitions.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year from " +
        "functional.alltypes");
    // Partially dynamic partitions with NULL static partition key value.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year, month=NULL)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year from " +
        "functional.alltypes");
    // Partially dynamic partitions with NULL literal as column.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, NULL from " +
        "functional.alltypes");
    // Partially dynamic partitions with NULL literal as column.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, NULL from " +
        "functional.alltypes");
    // Partially dynamic partitions with NULL literal in partition clause.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "Partition (year=2009, month=NULL)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");
    // Partially dynamic partitions with NULL literal in partition clause.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=NULL, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");

    // Select '*' includes partitioning columns at the end.
    AnalyzesOk("insert " + qualifier +
        " table functional.alltypessmall partition (year, month)" +
        "select * from functional.alltypes");
    // No corresponding select list items of fully dynamic partitions.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Target table 'functional.alltypessmall' has more columns (13) than the " +
        "SELECT / VALUES clause and PARTITION clause return (11)");
    // No corresponding select list items of partially dynamic partitions.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Target table 'functional.alltypessmall' has more columns (13) than the " +
        "SELECT / VALUES clause and PARTITION clause return (12)");

    // No corresponding select list items of partially dynamic partitions.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Target table 'functional.alltypessmall' has more columns (13) than the " +
        "SELECT / VALUES clause and PARTITION clause return (12)");
    // Select '*' includes partitioning columns, and hence, is not union compatible.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select * from functional.alltypes",
        "Target table 'functional.alltypessmall' has fewer columns (13) than the " +
        "SELECT / VALUES clause and PARTITION clause return (15)");
  }

  /**
   * Tests for inserting into unpartitioned tables
   */
  private void testInsertUnpartitioned(String qualifier) throws AnalysisException {
    // Wrong number of columns.
    AnalysisError(
        "insert " + qualifier + " table functional.alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col from functional.alltypes",
        "Target table 'functional.alltypesnopart' has more columns (11) than the SELECT" +
        " / VALUES clause returns (10)");

    // Wrong number of columns.
    if (!qualifier.contains("OVERWRITE")) {
      AnalysisError("INSERT " + qualifier + " TABLE functional_hbase.alltypes " +
          "SELECT * FROM functional.alltypesagg",
          "Target table 'functional_hbase.alltypes' has fewer columns (13) than the " +
          "SELECT / VALUES clause returns (14)");
    }
    // Unpartitioned table without partition clause.
    AnalyzesOk("insert " + qualifier + " table functional.alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col from " +
        "functional.alltypes");
    // All NULL column values.
    AnalyzesOk("insert " + qualifier + " table functional.alltypesnopart " +
        "select NULL, NULL, NULL, NULL, NULL, NULL, " +
        "NULL, NULL, NULL, NULL, NULL " +
        "from functional.alltypes");

    String hbaseQuery =  "INSERT " + qualifier + " TABLE " +
        "functional_hbase.insertalltypesagg select id, bigint_col, bool_col, " +
        "date_string_col, day, double_col, float_col, int_col, month, smallint_col, " +
        "string_col, timestamp_col, tinyint_col, year from functional.alltypesagg";

    // HBase doesn't support OVERWRITE so error out if the query is
    // trying to do that.
    if (!qualifier.contains("OVERWRITE")) {
      AnalyzesOk(hbaseQuery);
    } else {
      AnalysisError(hbaseQuery, "HBase doesn't have a way to perform INSERT OVERWRITE");
    }

    // Unpartitioned table with partition clause
    AnalysisError("INSERT " + qualifier +
        " TABLE functional.alltypesnopart PARTITION(year=2009) " +
        "SELECT * FROM functional.alltypes", "PARTITION clause is only valid for INSERT" +
        " into partitioned table. 'functional.alltypesnopart' is not partitioned");

    // Unknown target DB
    AnalysisError("INSERT " + qualifier + " table UNKNOWNDB.alltypesnopart SELECT * " +
        "from functional.alltypesnopart", "Database does not exist: UNKNOWNDB");
  }

  /**
   * Run general tests and tests using static partitions for INSERT INTO/OVERWRITE:
   */
  private void testInsertStatic(String qualifier) throws AnalysisException {
    // Static partition.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");

    // Static partition with NULL partition keys
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=NULL, month=NULL)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");
    // Static partition with NULL column values
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=NULL, month=NULL)" +
        "select NULL, NULL, NULL, NULL, NULL, NULL, " +
        "NULL, NULL, NULL, NULL, NULL " +
        "from functional.alltypes");
    // Static partition with NULL partition keys.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=NULL, month=NULL)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");
    // Static partition with partial NULL partition keys.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month=NULL)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");
    // Static partition with partial NULL partition keys.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=NULL, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");
    // Arbitrary exprs as partition key values. Constant exprs are ok.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=-1, month=cast(100*20+10 as INT))" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");

    // Union compatibility requires cast of select list expr in column 5
    // (int_col -> bigint).
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, int_col, " +
        "float_col, float_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");
    // No partition clause given for partitioned table.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Not enough partition columns mentioned in query. Missing columns are: year, " +
        "month");
    // Not union compatible, unequal number of columns.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, timestamp_col from functional.alltypes",
        "Target table 'functional.alltypessmall' has more columns (13) than the " +
        "SELECT / VALUES clause and PARTITION clause return (12)");
    // Not union compatible, incompatible type in last column (bool_col -> string).
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, bool_col, timestamp_col " +
        "from functional.alltypes",
        "Target table 'functional.alltypessmall' is incompatible with SELECT / " +
        "PARTITION expressions.\nExpression 'bool_col' (type: BOOLEAN) is not " +
        "compatible with column 'string_col' (type: STRING)");
    // Duplicate partition columns
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month=4, year=10)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Duplicate column 'year' in partition clause");
    // Too few partitioning columns.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Not enough partition columns mentioned in query. Missing columns are: month");
    // Non-partitioning column in partition clause.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, bigint_col=10)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Column 'bigint_col' is not a partition column");
    // Loss of precision when casting in column 6 (double_col -> float).
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "double_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Possible loss of precision for target table 'functional.alltypessmall'.\n" +
        "Expression 'double_col' (type: DOUBLE) would need to be cast to FLOAT for " +
        "column 'float_col'");
    // Select '*' includes partitioning columns, and hence, is not union compatible.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select * from functional.alltypes",
        "Target table 'functional.alltypessmall' has fewer columns (13) than the " +
        "SELECT / VALUES clause and PARTITION clause return (15)");
    // Partition columns should be type-checked
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=\"should be an int\", month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Target table 'functional.alltypessmall' is incompatible with " +
        "SELECT / PARTITION expressions.");
    // Arbitrary exprs as partition key values. Non-constant exprs should fail.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=-1, month=int_col)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Non-constant expressions are not supported as static partition-key values " +
        "in 'month=int_col'.");

    if (qualifier.contains("OVERWRITE")) {
      AnalysisError("insert " + qualifier + " table functional_hbase.alltypessmall " +
          "partition(year, month) select * from functional.alltypessmall",
          "PARTITION clause is not valid for INSERT into HBase tables. " +
          "'functional_hbase.alltypessmall' is an HBase table");
    }
  }

  private void testInsertWithPermutation(String qualifier) throws AnalysisException {
    // Duplicate column in permutation
    AnalysisError("insert " + qualifier + " table functional.tinytable(a, a, b)" +
        "values(1, 2, 3)", "Duplicate column 'a' in column permutation");

    // Unknown column in permutation
    AnalysisError("insert " + qualifier + " table functional.tinytable" +
        "(a, c) values(1, 2)", "Unknown column 'c' in column permutation");

    // Too few columns in permutation - fill with NULL values
    AnalyzesOk("insert " + qualifier + " table functional.tinytable(a) values('hello')");

    // Too many columns in select list
    AnalysisError("insert " + qualifier + " table functional.tinytable(a, b)" +
        " select 'a', 'b', 'c' from functional.alltypes",
        "Column permutation mentions fewer columns (2) than the SELECT / VALUES clause" +
        " returns (3)");

    // Too few columns in select list
    AnalysisError("insert " + qualifier + " table functional.tinytable(a, b)" +
        " select 'a' from functional.alltypes",
        "Column permutation mentions more columns (2) than the SELECT / VALUES clause" +
        " returns (1)");

    // Type error in select clause brought on by permutation. tinyint_col and string_col
    // are swapped in the permutation clause
    AnalysisError("insert " + qualifier + " table functional.alltypesnopart" +
        "(id, bool_col, string_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, tinyint_col, timestamp_col)" +
        " select * from functional.alltypesnopart",
        "Target table 'functional.alltypesnopart' is incompatible with SELECT / " +
        "PARTITION expressions.\nExpression 'functional.alltypesnopart.tinyint_col' " +
        "(type: TINYINT) is not compatible with column 'string_col' (type: STRING)");

    // Above query should work fine if select list also permuted
    AnalyzesOk("insert " + qualifier + " table functional.alltypesnopart" +
        "(id, bool_col, string_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, tinyint_col, timestamp_col)" +
        " select id, bool_col, string_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, tinyint_col, timestamp_col" +
        " from functional.alltypesnopart");

    // Mentioning partition keys (year, month) in permutation
    AnalyzesOk("insert " + qualifier + " table functional.alltypes" +
        "(id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, " +
        "year, month) select * from functional.alltypes");

    // Duplicate mention of partition column
    AnalysisError("insert " + qualifier + " table functional.alltypes" +
        "(id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, " +
        "year, month) PARTITION(year) select * from functional.alltypes",
        "Duplicate column 'year' in partition clause");

    // Split partition columns between permutation and PARTITION clause.  Also confirm
    // that dynamic columns in PARTITION clause are looked for at the end of the select
    // list.
    AnalyzesOk("insert " + qualifier + " table functional.alltypes" +
        "(id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, " +
        "year) PARTITION(month) select * from functional.alltypes");

    // Split partition columns, one dynamic in permutation clause, one static in PARTITION
    // clause
    AnalyzesOk("insert " + qualifier + " table functional.alltypes(id, year)" +
        "PARTITION(month=2009) select 1, 2 from functional.alltypes");

    // Omit most columns, should default to NULL
    AnalyzesOk("insert " + qualifier + " table functional.alltypesnopart" +
        "(id, bool_col) select id, bool_col from functional.alltypesnopart");

    // Can't omit partition keys, they have to be mentioned somewhere
    AnalysisError("insert " + qualifier + " table functional.alltypes(id)" +
        " select id from functional.alltypes",
        "Not enough partition columns mentioned in query. " +
        "Missing columns are: year, month");

    // Duplicate partition columns, one with partition key
    AnalysisError("insert " + qualifier + " table functional.alltypes(year)" +
        " partition(year=2012, month=3) select 1 from functional.alltypes",
        "Duplicate column 'year' in partition clause");

    // Type error between dynamic partition column mentioned in PARTITION column and
    // select list (confirm that dynamic partition columns are mapped to the last select
    // list expressions)
    AnalysisError("insert " + qualifier + " table functional.alltypes" +
        "(id, bool_col, string_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, tinyint_col, timestamp_col) " +
        "PARTITION (year, month)" +
        " select id, bool_col, month, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, tinyint_col, timestamp_col, " +
        "year, string_col from functional.alltypes",
        "Target table 'functional.alltypes' is incompatible with SELECT / PARTITION " +
        "expressions.\n" +
        "Expression 'month' (type: INT) is not compatible with column 'string_col' " +
        "(type: STRING)");

    // Empty permutation and no query statement
    AnalyzesOk("insert " + qualifier + " table functional.alltypesnopart()");
    // Empty permutation can't receive any select list exprs
    AnalysisError("insert " + qualifier + " table functional.alltypesnopart() select 1",
        "Column permutation mentions fewer columns (0) than the SELECT / VALUES clause " +
        "returns (1)");
    // Empty permutation with static partition columns can omit query statement
    AnalyzesOk("insert " + qualifier + " table functional.alltypes() " +
        "partition(year=2012, month=1)");
    // No mentioned columns to receive select-list exprs
    AnalysisError("insert " + qualifier + " table functional.alltypes() " +
        "partition(year=2012, month=1) select 1",
        "Column permutation and PARTITION clause mention fewer columns (0) than the " +
        "SELECT / VALUES clause and PARTITION clause return (1)");
    // Can't have dynamic partition columns with no query statement
    AnalysisError("insert " + qualifier + " table functional.alltypes() " +
       "partition(year, month)",
       "Column permutation and PARTITION clause mention more columns (2) than the " +
       "SELECT / VALUES clause and PARTITION clause return (0)");
    // If there are select-list exprs for dynamic partition columns, empty permutation is
    // ok
    AnalyzesOk("insert " + qualifier + " table functional.alltypes() " +
        "partition(year, month) select 1,2 from functional.alltypes");

    if (!qualifier.contains("OVERWRITE")) {
      // Simple permutation
      AnalyzesOk("insert " + qualifier + " table functional_hbase.alltypesagg" +
          "(id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
          "float_col, double_col, date_string_col, string_col, timestamp_col) " +
          "select * from functional.alltypesnopart");
      // Too few columns in permutation
      AnalysisError("insert " + qualifier + " table functional_hbase.alltypesagg" +
          "(id, tinyint_col, smallint_col, int_col, bigint_col, " +
          "float_col, double_col, date_string_col, string_col) " +
          "select * from functional.alltypesnopart",
          "Column permutation mentions fewer columns (9) than the SELECT /" +
          " VALUES clause returns (11)");
      // Omitting the row-key column is an error
      AnalysisError("insert " + qualifier + " table functional_hbase.alltypesagg" +
          "(bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
          "float_col, double_col, date_string_col, string_col, timestamp_col) " +
          "select bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
          "float_col, double_col, date_string_col, string_col, timestamp_col from " +
          "functional.alltypesnopart",
          "Row-key column 'id' must be explicitly mentioned in column permutation.");
    }
  }

  /**
   * Simple test that checks the number of members of statements and table refs
   * against a fixed expected value. The intention is alarm developers to
   * properly change the clone() method when adding members to statements.
   * Once the clone() method has been appropriately changed, the expected
   * number of members should be updated to make the test pass.
   */
  @Test
  public void cloneTest() {
    testNumberOfMembers(QueryStmt.class, 10);
    testNumberOfMembers(UnionStmt.class, 7);
    testNumberOfMembers(ValuesStmt.class, 0);

    // Also check TableRefs.
    testNumberOfMembers(TableRef.class, 11);
    testNumberOfMembers(BaseTableRef.class, 2);
    testNumberOfMembers(InlineViewRef.class, 6);
    testNumberOfMembers(ViewRef.class, 2);
  }

  @SuppressWarnings("rawtypes")
  private void testNumberOfMembers(Class cl, int expectedNumMembers) {
    int actualNumMembers = 0;
    for (Field f: cl.getDeclaredFields()) {
      // Exclude synthetic fields such as enum jump tables that may be added at runtime.
      if (!f.isSynthetic()) ++actualNumMembers;
    }
    if (actualNumMembers != expectedNumMembers) {
      fail(String.format("The number of members in %s have changed.\n" +
          "Expected %s but found %s. Please modify clone() accordingly and " +
          "change the expected number of members in this test.",
          cl.getSimpleName(), expectedNumMembers, actualNumMembers));
    }
  }
}
