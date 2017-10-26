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

import static org.junit.Assert.fail;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.testutil.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Preconditions;

// TODO: Expand this test, in particular, because view creation relies
// on producing correct SQL.
public class ToSqlTest extends FrontendTestBase {

  // Helpers for templated join tests.
  private static final String[] joinConditions_ =
      new String[] {"USING (id)", "ON (a.id = b.id)"};

  // All left semi join types.
  private static final String[] leftSemiJoinTypes_ =
      new String[] {"LEFT SEMI JOIN", "LEFT ANTI JOIN"};

  // All right semi join types.
  private static final String[] rightSemiJoinTypes_ =
      new String[] {"RIGHT SEMI JOIN", "RIGHT ANTI JOIN"};

  // All join types that take an ON or USING clause, i.e., all joins except CROSS JOIN.
  private static final String[] joinTypes_;

  // Same as joinTypes_, but excluding semi joins.
  private static final String[] nonSemiJoinTypes_;

  static {
    // Exclude the NULL AWARE LEFT ANTI JOIN operator because it cannot
    // be directly expressed via SQL.
    joinTypes_ = new String[JoinOperator.values().length - 2];
    int numNonSemiJoinTypes = JoinOperator.values().length - 2 -
        leftSemiJoinTypes_.length - rightSemiJoinTypes_.length;
    nonSemiJoinTypes_ = new String[numNonSemiJoinTypes];
    int i = 0;
    int j = 0;
    for (JoinOperator op: JoinOperator.values()) {
      if (op.isCrossJoin() || op.isNullAwareLeftAntiJoin()) continue;
      joinTypes_[i++] = op.toString();
      if (op.isSemiJoin()) continue;
      nonSemiJoinTypes_[j++] = op.toString();
    }
  }

  private void testToSql(String query, String expected) {
    testToSql(query, System.getProperty("user.name"), expected);
  }

  private void testToSql(String query, String defaultDb, String expected) {
    testToSql(query, defaultDb, expected, false);
  }

  private void testToSql(String query, String defaultDb, String expected,
      boolean ignoreWhitespace) {
    String actual = null;
    try {
      ParseNode node = AnalyzesOk(query, createAnalysisCtx(defaultDb));
      actual = node.toSql();
      if (ignoreWhitespace) {
        // Transform whitespace to single space.
        actual = actual.replace('\n', ' ').replaceAll(" +", " ").trim();
      }
      if (!actual.equals(expected)) {
        String msg = "\n<<< Expected(length:" + expected.length() + "): [" + expected
          + "]\n>>> Actual(length:" + actual.length() + "): [" + actual + "]\n";
        System.err.println(msg);
        fail(msg);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed to analyze query: " + query + "\n" + e.getMessage());
    }
    // Parse and analyze the resulting SQL to ensure its validity.
    AnalyzesOk(actual, createAnalysisCtx(defaultDb));
  }

  private void runTestTemplate(String sql, String expectedSql, String[]... testDims) {
    Object[] testVector = new Object[testDims.length];
    runTestTemplate(sql, expectedSql, 0, testVector, testDims);
  }

  private void runTestTemplate(String sql, String expectedSql, int dim,
      Object[] testVector, String[]... testDims) {
    if (dim >= testDims.length) {
      testToSql(String.format(sql, testVector), String.format(expectedSql, testVector));
      return;
    }
    for (String s: testDims[dim]) {
      testVector[dim] = s;
      runTestTemplate(sql, expectedSql, dim + 1, testVector, testDims);
    }
  }

  /**
   * Generates and runs testToSql() on two variants of the given query by replacing all
   * occurrences of "$TBL" in the query string with the unqualified and fully-qualified
   * version of the given table name. The unqualified variant is analyzed using an
   * analyzer that has tbl's db set as the default database.
   * The SQL is expected to the same for both variants because toSql() should fully
   * qualify unqualified table names.
   * Example:
   * query = "select id from $TBL, $TBL"
   * tbl = "functional.alltypes"
   * Variants generated and analyzed:
   * select id from alltypes, alltypes (default db is "functional")
   * select id from functional.alltypes, functional.alltypes (default db is "default")
   */
  private void TblsTestToSql(String query, TableName tbl, String expectedSql) {
    Preconditions.checkState(tbl.isFullyQualified());
    Preconditions.checkState(query.contains("$TBL"));
    String uqQuery = query.replace("$TBL", tbl.getTbl());
    testToSql(uqQuery, tbl.getDb(), expectedSql);
    AnalyzesOk(uqQuery, createAnalysisCtx(tbl.getDb()));
    String fqQuery = query.replace("$TBL", tbl.toString());
    testToSql(fqQuery, expectedSql);
  }

  @Test
  public void selectListTest() {
    testToSql("select 1234, 1234.0, 1234.0 + 1, 1234.0 + 1.0, 1 + 1, \"abc\" " +
        "from functional.alltypes",
        "SELECT 1234, 1234.0, 1234.0 + 1, 1234.0 + 1.0, 1 + 1, 'abc' " +
        "FROM functional.alltypes");
    // Test aliases.
    testToSql("select 1234 i, 1234.0 as j, (1234.0 + 1) k, (1234.0 + 1.0) as l " +
        "from functional.alltypes",
        "SELECT 1234 i, 1234.0 j, (1234.0 + 1) k, (1234.0 + 1.0) l " +
        "FROM functional.alltypes");
    // Test select without from.
    testToSql("select 1234 i, 1234.0 as j, (1234.0 + 1) k, (1234.0 + 1.0) as l",
        "SELECT 1234 i, 1234.0 j, (1234.0 + 1) k, (1234.0 + 1.0) l");
    // Test select without from.
    testToSql("select null, 1234 < 5678, 1234.0 < 5678.0, 1234 < null " +
        "from functional.alltypes",
        "SELECT NULL, 1234 < 5678, 1234.0 < 5678.0, 1234 < NULL " +
        "FROM functional.alltypes");
  }

  private boolean isCollectionTableRef(String tableName) {
    return tableName.split("\\.").length > 0;
  }

  /**
   * Test all table/column combinations in the select list of a query
   * using implicit and explicit table aliases.
   */
  private void testAllTableAliases(String[] tables, String[] columns)
      throws AnalysisException {
    for (String tbl: tables) {
      TableName tblName = new TableName("functional", tbl);
      String uqAlias = tbl.substring(tbl.lastIndexOf(".") + 1);
      String fqAlias = "functional." + tbl;
      boolean isCollectionTblRef = isCollectionTableRef(tbl);
      for (String col: columns) {
        // Test implicit table aliases with unqualified and fully qualified
        // table/view names. Unqualified table/view names should be fully
        // qualified in the generated SQL (IMPALA-962).
        TblsTestToSql(String.format("select %s from $TBL", col), tblName,
            String.format("SELECT %s FROM %s", col, fqAlias));
        TblsTestToSql(String.format("select %s.%s from $TBL", uqAlias, col), tblName,
            String.format("SELECT %s.%s FROM %s", uqAlias, col, fqAlias));
        // Only references to base tables/views have a fully-qualified implicit alias.
        if (!isCollectionTblRef) {
          TblsTestToSql(String.format("select %s.%s from $TBL", fqAlias, col), tblName,
              String.format("SELECT %s.%s FROM %s", fqAlias, col, fqAlias));
        }

        // Explicit table alias.
        TblsTestToSql(String.format("select %s from $TBL a", col), tblName,
            String.format("SELECT %s FROM %s a", col, fqAlias));
        TblsTestToSql(String.format("select a.%s from $TBL a", col), tblName,
            String.format("SELECT a.%s FROM %s a", col, fqAlias));
      }
    }

    // Multiple implicit fully-qualified aliases work.
    for (String t1: tables) {
      for (String t2: tables) {
        if (t1 == t2) continue;
        // Collection tables do not have a fully-qualified implicit alias.
        if (isCollectionTableRef(t1) && isCollectionTableRef(t2)) continue;
        for (String col: columns) {
          testToSql(String.format(
              "select functional.%s.%s, functional.%s.%s " +
                  "from functional.%s, functional.%s", t1, col, t2, col, t1, t2),
              String.format("SELECT functional.%s.%s, functional.%s.%s " +
                  "FROM functional.%s, functional.%s", t1, col, t2, col, t1, t2));
        }
      }
    }
  }

  /**
   * Tests the toSql() of the given child table and column assumed to be in
   * functional.allcomplextypes, including different combinations of
   * implicit/explicit aliases of the parent and child table.
   */
  private void testChildTableRefs(String childTable, String childColumn) {
    TableName tbl = new TableName("functional", "allcomplextypes");

    // Child table uses unqualified implicit alias of parent table.
    TblsTestToSql(
        String.format("select %s from $TBL, allcomplextypes.%s",
            childColumn, childTable), tbl,
        String.format("SELECT %s FROM %s, functional.allcomplextypes.%s",
            childColumn, tbl.toSql(), childTable));
    // Child table uses fully qualified implicit alias of parent table.
    TblsTestToSql(
        String.format("select %s from $TBL, functional.allcomplextypes.%s",
            childColumn, childTable), tbl,
        String.format("SELECT %s FROM %s, functional.allcomplextypes.%s",
            childColumn, tbl.toSql(), childTable));
    // Child table uses explicit alias of parent table.
    TblsTestToSql(
        String.format("select %s from $TBL a, a.%s",
            childColumn, childTable), tbl,
        String.format("SELECT %s FROM %s a, a.%s",
            childColumn, tbl.toSql(), childTable));

    // Parent/child/child join.
    TblsTestToSql(
        String.format("select b.%s from $TBL a, a.%s b, a.int_map_col c",
            childColumn, childTable), tbl,
        String.format("SELECT b.%s FROM %s a, a.%s b, a.int_map_col c",
            childColumn, tbl.toSql(), childTable));
    TblsTestToSql(
        String.format("select c.%s from $TBL a, a.int_array_col b, a.%s c",
            childColumn, childTable), tbl,
        String.format("SELECT c.%s FROM %s a, a.int_array_col b, a.%s c",
            childColumn, tbl.toSql(), childTable));

    // Test join types. Parent/child joins do not require an ON or USING clause.
    for (String joinType: joinTypes_) {
      TblsTestToSql(String.format("select 1 from $TBL %s allcomplextypes.%s",
          joinType, childTable), tbl,
          String.format("SELECT 1 FROM %s %s functional.allcomplextypes.%s",
          tbl.toSql(), joinType, childTable));
      TblsTestToSql(String.format("select 1 from $TBL a %s a.%s",
          joinType, childTable), tbl,
          String.format("SELECT 1 FROM %s a %s a.%s",
          tbl.toSql(), joinType, childTable));
    }

    // Legal, but not a parent/child join.
    TblsTestToSql(
        String.format("select %s from $TBL a, functional.allcomplextypes.%s",
            childColumn, childTable), tbl,
        String.format("SELECT %s FROM %s a, functional.allcomplextypes.%s",
            childColumn, tbl.toSql(), childTable));
    TblsTestToSql(
        String.format("select %s from $TBL.%s, functional.allcomplextypes",
            childColumn, childTable), tbl,
        String.format("SELECT %s FROM %s.%s, functional.allcomplextypes",
            childColumn, tbl.toSql(), childTable));
  }

  @Test
  public void TestCreateTable() throws AnalysisException {
    testToSql("create table p (a int) partitioned by (day string) sort by (a) " +
        "comment 'This is a test'",
        "default",
        "CREATE TABLE default.p ( a INT ) PARTITIONED BY ( day STRING ) " +
        "SORT BY ( a ) COMMENT 'This is a test' STORED AS TEXTFILE" , true);
    // Table with SORT BY clause.
    testToSql("create table p (a int, b int) partitioned by (day string) sort by (a ,b) ",
        "default",
        "CREATE TABLE default.p ( a INT, b INT ) PARTITIONED BY ( day STRING ) " +
        "SORT BY ( a, b ) STORED AS TEXTFILE" , true);
    // Kudu table with a TIMESTAMP column default value
    testToSql("create table p (a bigint primary key, b timestamp default '1987-05-19') " +
        "partition by hash(a) partitions 3 stored as kudu " +
        "tblproperties ('kudu.master_addresses'='foo')",
        "default",
        "CREATE TABLE default.p ( a BIGINT PRIMARY KEY, b TIMESTAMP " +
        "DEFAULT '1987-05-19' ) PARTITION BY HASH (a) PARTITIONS 3 " +
        "STORED AS KUDU TBLPROPERTIES ('kudu.master_addresses'='foo', " +
        "'storage_handler'='com.cloudera.kudu.hive.KuduStorageHandler')", true);
  }

  @Test
  public void TestCreateTableAsSelect() throws AnalysisException {
    // Partitioned table.
    testToSql("create table p partitioned by (int_col) as " +
        "select double_col, int_col from functional.alltypes", "default",
        "CREATE TABLE default.p PARTITIONED BY ( int_col ) STORED AS " +
        "TEXTFILE AS SELECT double_col, int_col FROM functional.alltypes",
        true);
    // Table with a comment.
    testToSql("create table p partitioned by (int_col) comment 'This is a test' as " +
        "select double_col, int_col from functional.alltypes", "default",
        "CREATE TABLE default.p PARTITIONED BY ( int_col ) COMMENT 'This is a test' " +
        "STORED AS TEXTFILE AS SELECT double_col, int_col FROM functional.alltypes",
        true);
    // Table with SORT BY clause.
    testToSql("create table p partitioned by (int_col) sort by (string_col) as " +
        "select double_col, string_col, int_col from functional.alltypes", "default",
        "CREATE TABLE default.p PARTITIONED BY ( int_col ) SORT BY ( string_col ) " +
        "STORED AS TEXTFILE AS SELECT double_col, string_col, int_col FROM " +
        "functional.alltypes", true);
    // Kudu table with multiple partition params
    testToSql("create table p primary key (a,b) partition by hash(a) partitions 3, " +
        "range (b) (partition value = 1) stored as kudu " +
        "tblproperties ('kudu.master_addresses'='foo') as select int_col a, bigint_col " +
        "b from functional.alltypes",
        "default",
        "CREATE TABLE default.p PRIMARY KEY (a, b) PARTITION BY HASH (a) PARTITIONS 3, " +
        "RANGE (b) (PARTITION VALUE = 1) STORED AS KUDU TBLPROPERTIES " +
        "('kudu.master_addresses'='foo', " +
        "'storage_handler'='com.cloudera.kudu.hive.KuduStorageHandler') AS " +
        "SELECT int_col a, bigint_col b FROM functional.alltypes", true);
  }

  @Test
  public void TestCreateTableLike() throws AnalysisException {
    testToSql("create table p like functional.alltypes", "default",
        "CREATE TABLE p LIKE functional.alltypes");
    // Table with sort columns.
    testToSql("create table p sort by (id) like functional.alltypes", "default",
        "CREATE TABLE p SORT BY (id) LIKE functional.alltypes");
  }

  @Test
  public void TestCreateTableLikeFile() throws AnalysisException {
    testToSql("create table if not exists p like parquet " +
        "'/test-warehouse/schemas/alltypestiny.parquet'", "default",
        "CREATE TABLE IF NOT EXISTS default.p LIKE PARQUET " +
        "'hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet' " +
        "STORED AS TEXTFILE", true);
    // Table with sort columns.
    testToSql("create table if not exists p like parquet " +
        "'/test-warehouse/schemas/alltypestiny.parquet' sort by (int_col, id)", "default",
        "CREATE TABLE IF NOT EXISTS default.p LIKE PARQUET " +
        "'hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet' " +
        "SORT BY ( int_col, id ) STORED AS TEXTFILE", true);
  }

  @Test
  public void TestTableAliases() throws AnalysisException {
    String[] tables = new String[] { "alltypes", "alltypes_view" };
    String[] columns = new String[] { "int_col", "*" };
    testAllTableAliases(tables, columns);

    // Unqualified '*' is not ambiguous.
    testToSql("select * from functional.alltypes " +
            "cross join functional_parquet.alltypes",
        "SELECT * FROM functional.alltypes CROSS JOIN functional_parquet.alltypes");
  }

  @Test
  public void TestStructFields() throws AnalysisException {
    String[] tables = new String[] { "allcomplextypes", };
    String[] columns = new String[] { "id", "int_struct_col.f1",
        "nested_struct_col.f2.f12.f21" };
    testAllTableAliases(tables, columns);
  }

  @Test
  public void TestCollectionTableRefs() throws AnalysisException {
    // Test ARRAY type referenced as a table.
    testAllTableAliases(new String[] {
        "allcomplextypes.int_array_col"},
        new String[] {Path.ARRAY_ITEM_FIELD_NAME, "*"});
    testAllTableAliases(new String[]{
            "allcomplextypes.struct_array_col"},
        new String[]{"f1", "f2", "*"});

    // Test MAP type referenced as a table.
    testAllTableAliases(new String[] {
        "allcomplextypes.int_map_col"},
        new String[] {
            Path.MAP_KEY_FIELD_NAME,
            Path.MAP_VALUE_FIELD_NAME,
            "*"});
    testAllTableAliases(new String[]{
            "allcomplextypes.struct_map_col"},
        new String[]{Path.MAP_KEY_FIELD_NAME, "f1", "f2", "*"});

    // Test complex table ref path with structs and multiple collections.
    testAllTableAliases(new String[]{
            "allcomplextypes.complex_nested_struct_col.f2.f12"},
        new String[]{Path.MAP_KEY_FIELD_NAME, "f21", "*"});

    // Test toSql() of child table refs.
    testChildTableRefs("int_array_col", Path.ARRAY_ITEM_FIELD_NAME);
    testChildTableRefs("int_map_col", Path.MAP_KEY_FIELD_NAME);
    testChildTableRefs("complex_nested_struct_col.f2.f12", "f21");
  }

  /**
   * Tests quoting of identifiers for view compatibility with Hive,
   * and for proper quoting of Impala keywords in view-definition stmts.
   */
  @Test
  public void TestIdentifierQuoting() {
    // The quotes of quoted identifiers will be removed if they are unnecessary.
    testToSql("select 1 as `abc`, 2.0 as 'xyz'", "SELECT 1 abc, 2.0 xyz");

    // These identifiers are lexable by Impala but not Hive. For view compatibility
    // we enclose the idents in quotes.
    testToSql("select 1 as _c0, 2.0 as $abc", "SELECT 1 `_c0`, 2.0 `$abc`");

    // Quoted identifiers that require quoting in both Impala and Hive.
    testToSql("select 1 as `???`, 2.0 as '^^^'", "SELECT 1 `???`, 2.0 `^^^`");

    // Test quoting of identifiers that are Impala keywords.
    testToSql("select `end`.`alter`, `end`.`table` from " +
        "(select 1 as `alter`, 2 as `table`) `end`",
        "SELECT `end`.`alter`, `end`.`table` FROM " +
        "(SELECT 1 `alter`, 2 `table`) `end`");

    // Test quoting of inline view aliases.
    testToSql("select a from (select 1 as a) as _t",
        "SELECT a FROM (SELECT 1 a) `_t`");

    // Test quoting of WITH-clause views.
    testToSql("with _t as (select 1 as a) select * from _t",
        "WITH `_t` AS (SELECT 1 a) SELECT * FROM `_t`");

    // Test quoting of non-SlotRef exprs in inline views.
    testToSql("select `1 + 10`, `trim('abc')` from (select 1 + 10, trim('abc')) as t",
        "SELECT `1 + 10`, `trim('abc')` FROM (SELECT 1 + 10, trim('abc')) t");
  }

  @Test
  public void normalizeStringLiteralTest() {
    testToSql("select \"'\"", "SELECT '\\''");
    testToSql("select \"\\'\"", "SELECT '\\''");
    testToSql("select \"\\\\'\"", "SELECT '\\\\\\''");
    testToSql("select '\"'", "SELECT '\"'");
    testToSql("select '\\\"'", "SELECT '\"'");
    testToSql("select '\\''", "SELECT '\\''");
    testToSql("select '\\\\\\''", "SELECT '\\\\\\''");
    testToSql("select regexp_replace(string_col, \"\\\\'\", \"'\") from " +
        "functional.alltypes", "SELECT regexp_replace(string_col, '\\\\\\'', '\\'') " +
        "FROM functional.alltypes");
    testToSql("select * from functional.alltypes where '123' = \"123\"",
        "SELECT * FROM functional.alltypes WHERE '123' = '123'");
  }

  // Test the toSql() output of the where clause.
  @Test
  public void whereTest() {
    testToSql("select id from functional.alltypes " +
        "where tinyint_col < 40 OR int_col = 4 AND float_col > 1.4",
        "SELECT id FROM functional.alltypes " +
        "WHERE tinyint_col < 40 OR int_col = 4 AND float_col > 1.4");
    testToSql("select id from functional.alltypes where string_col = \"abc\"",
        "SELECT id FROM functional.alltypes WHERE string_col = 'abc'");
    testToSql("select id from functional.alltypes where string_col = 'abc'",
        "SELECT id FROM functional.alltypes WHERE string_col = 'abc'");
    testToSql("select id from functional.alltypes " +
        "where 5 between smallint_col and int_col",
        "SELECT id FROM functional.alltypes WHERE 5 BETWEEN smallint_col AND int_col");
    testToSql("select id from functional.alltypes " +
        "where 5 not between smallint_col and int_col",
        "SELECT id FROM functional.alltypes " +
        "WHERE 5 NOT BETWEEN smallint_col AND int_col");
    testToSql("select id from functional.alltypes where 5 in (smallint_col, int_col)",
        "SELECT id FROM functional.alltypes WHERE 5 IN (smallint_col, int_col)");
    testToSql("select id from functional.alltypes " +
            "where 5 not in (smallint_col, int_col)",
        "SELECT id FROM functional.alltypes WHERE 5 NOT IN (smallint_col, int_col)");
  }

  // Test the toSql() output of joins in a standalone select block.
  @Test
  public void joinTest() {
    testToSql("select * from functional.alltypes a, functional.alltypes b " +
        "where a.id = b.id",
        "SELECT * FROM functional.alltypes a, functional.alltypes b WHERE a.id = b.id");
    testToSql("select * from functional.alltypes a cross join functional.alltypes b",
        "SELECT * FROM functional.alltypes a CROSS JOIN functional.alltypes b");
    runTestTemplate("select * from functional.alltypes a %s functional.alltypes b %s",
        "SELECT * FROM functional.alltypes a %s functional.alltypes b %s",
        joinTypes_, joinConditions_);
  }

  private void planHintsTestForInsertAndUpsert(String prefix, String suffix) {
    for (InsertStmt.HintLocation loc: InsertStmt.HintLocation.values()) {
      // Insert hint.
      testToSql(InjectInsertHint(
            "insert%s into functional.alltypes(int_col, bool_col) " +
            "partition(year, month)%s" +
            "select int_col, bool_col, year, month from functional.alltypes",
          String.format(" %snoshuffle%s", prefix, suffix), loc),
          InjectInsertHint("INSERT%s INTO TABLE functional.alltypes(int_col, " +
            "bool_col) PARTITION (year, month)%s " +
            "SELECT int_col, bool_col, year, month FROM functional.alltypes",
            " \n-- +noshuffle\n", loc));
      testToSql(InjectInsertHint(
            "insert%s into functional.alltypes(int_col, bool_col) " +
            "partition(year, month)%s" +
            "select int_col, bool_col, year, month from functional.alltypes",
          String.format(" %sshuffle,clustered%s", prefix, suffix), loc),
          InjectInsertHint("INSERT%s INTO TABLE functional.alltypes(int_col, " +
            "bool_col) PARTITION (year, month)%s " +
            "SELECT int_col, bool_col, year, month FROM functional.alltypes",
            " \n-- +shuffle,clustered\n", loc));

      // Upsert hint.
      testToSql(InjectInsertHint(
            "upsert%s into functional_kudu.alltypes(id, int_col)%s" +
            "select id, int_col from functional_kudu.alltypes",
          String.format(" %snoshuffle%s", prefix, suffix), loc),
          InjectInsertHint("UPSERT%s INTO TABLE functional_kudu.alltypes(id, int_col)" +
            "%s SELECT id, int_col FROM functional_kudu.alltypes",
            " \n-- +noshuffle\n", loc));
      testToSql(InjectInsertHint(
            "upsert%s into functional_kudu.alltypes(id, int_col)%s" +
            "select id, int_col from functional_kudu.alltypes",
          String.format(" %sshuffle,clustered%s", prefix, suffix), loc),
          InjectInsertHint("UPSERT%s INTO TABLE functional_kudu.alltypes(id, int_col)" +
            "%s SELECT id, int_col FROM functional_kudu.alltypes",
            " \n-- +shuffle,clustered\n", loc));
    }
  }

  /**
   * Tests that the toSql() of plan hints use the end-of-line commented hint style
   * (for view compatibility with Hive) regardless of what style was used in the
   * original query.
   */
  @Test
  public void planHintsTest() {
    for (String[] hintStyle: hintStyles_) {
      String prefix = hintStyle[0];
      String suffix = hintStyle[1];

      // Hint in Insert/Upsert.
      planHintsTestForInsertAndUpsert(prefix, suffix);

      // Join hint.
      testToSql(String.format(
          "select * from functional.alltypes a join %sbroadcast%s " +
          "functional.alltypes b on a.id = b.id", prefix, suffix),
          "SELECT * FROM functional.alltypes a INNER JOIN \n-- +broadcast\n " +
          "functional.alltypes b ON a.id = b.id");

      // Table hint
      testToSql(String.format(
          "select * from functional.alltypes atp %sschedule_random_replica%s", prefix,
          suffix),
          "SELECT * FROM functional.alltypes atp\n-- +schedule_random_replica\n");
      testToSql(String.format(
          "select * from functional.alltypes %sschedule_random_replica%s", prefix,
          suffix),
          "SELECT * FROM functional.alltypes\n-- +schedule_random_replica\n");
      testToSql(String.format(
          "select * from functional.alltypes %sschedule_random_replica," +
          "schedule_disk_local%s", prefix, suffix),
          "SELECT * FROM functional.alltypes\n-- +schedule_random_replica," +
          "schedule_disk_local\n");
      testToSql(String.format(
          "select c1 from (select atp.tinyint_col as c1 from functional.alltypes atp " +
          "%sschedule_random_replica%s) s1", prefix, suffix),
          "SELECT c1 FROM (SELECT atp.tinyint_col c1 FROM functional.alltypes atp\n-- +" +
          "schedule_random_replica\n) s1");

      // Select-list hint. The legacy-style hint has no prefix and suffix.
      if (prefix.contains("[")) {
        prefix = "";
        suffix = "";
      }
      // Comment-style select-list plan hint.
      testToSql(String.format(
          "select %sstraight_join%s * from functional.alltypes", prefix, suffix),
          "SELECT \n-- +straight_join\n * FROM functional.alltypes");
      testToSql(
          String.format("select distinct %sstraight_join%s * from functional.alltypes",
          prefix, suffix),
          "SELECT DISTINCT \n-- +straight_join\n * FROM functional.alltypes");
    }
  }

  // Test the toSql() output of aggregate and group by expressions.
  @Test
  public void aggregationTest() {
    testToSql("select COUNT(*), count(id), COUNT(id), SUM(id), AVG(id) " +
        "from functional.alltypes group by tinyint_col",
        "SELECT count(*), count(id), count(id), sum(id), avg(id) " +
        "FROM functional.alltypes GROUP BY tinyint_col");
    testToSql("select avg(float_col / id) from functional.alltypes group by tinyint_col",
        "SELECT avg(float_col / id) " +
        "FROM functional.alltypes GROUP BY tinyint_col");
    testToSql("select avg(double_col) from functional.alltypes " +
        "group by int_col, tinyint_col, bigint_col",
        "SELECT avg(double_col) FROM functional.alltypes " +
        "GROUP BY int_col, tinyint_col, bigint_col");
    // Group by with having clause
    testToSql("select avg(id) from functional.alltypes " +
        "group by tinyint_col having count(tinyint_col) > 10",
        "SELECT avg(id) FROM functional.alltypes " +
        "GROUP BY tinyint_col HAVING count(tinyint_col) > 10");
    testToSql("select sum(id) from functional.alltypes group by tinyint_col " +
        "having avg(tinyint_col) > 10 AND count(tinyint_col) > 5",
        "SELECT sum(id) FROM functional.alltypes GROUP BY tinyint_col " +
        "HAVING avg(tinyint_col) > 10 AND count(tinyint_col) > 5");
  }

  // Test the toSql() output of the order by clause.
  @Test
  public void orderByTest() {
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col ASC, float_col DESC, int_col ASC",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col ASC, float_col DESC, int_col ASC");
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col DESC, float_col ASC, int_col DESC",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col DESC, float_col ASC, int_col DESC");
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC");
    // Test limit/offset
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC LIMIT 10 OFFSET 5",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC LIMIT 10 OFFSET 5");
    // Offset shouldn't be printed if it's not necessary
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC LIMIT 10 OFFSET 0",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col ASC NULLS FIRST, float_col DESC NULLS LAST, " +
        "int_col DESC LIMIT 10");

    // Check we do not print NULLS FIRST/LAST unless necessary
    testToSql("select id, string_col from functional.alltypes " +
        "order by string_col DESC NULLS FIRST, float_col ASC NULLS LAST, " +
        "int_col DESC",
        "SELECT id, string_col FROM functional.alltypes " +
        "ORDER BY string_col DESC, float_col ASC, " +
        "int_col DESC");
  }

  // Test the toSql() output of queries with all clauses.
  @Test
  public void allTest() {
    testToSql("select bigint_col, avg(double_col), sum(tinyint_col) " +
        "from functional.alltypes " +
        "where double_col > 2.5 AND string_col != \"abc\"" +
        "group by bigint_col, int_col " +
        "having count(int_col) > 10 OR sum(bigint_col) > 20 " +
        "order by 2 DESC NULLS LAST, 3 ASC",
        "SELECT bigint_col, avg(double_col), sum(tinyint_col) " +
        "FROM functional.alltypes " +
        "WHERE double_col > 2.5 AND string_col != 'abc' " +
        "GROUP BY bigint_col, int_col " +
        "HAVING count(int_col) > 10 OR sum(bigint_col) > 20 " +
        "ORDER BY 2 DESC NULLS LAST, 3 ASC");
  }

  @Test
  public void unionTest() {
    testToSql("select bool_col, rank() over(order by id) from functional.alltypes " +
        "union select bool_col, int_col from functional.alltypessmall " +
        "union select bool_col, bigint_col from functional.alltypes",
        "SELECT bool_col, rank() OVER (ORDER BY id ASC) FROM functional.alltypes " +
        "UNION SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION SELECT bool_col, bigint_col FROM functional.alltypes");
    testToSql("select bool_col, int_col from functional.alltypes " +
        "union all select bool_col, int_col from functional.alltypessmall " +
        "union all select bool_col, int_col from functional.alltypessmall " +
        "union all select bool_col, int_col from functional.alltypessmall " +
        "union all select bool_col, bigint_col from functional.alltypes",
        "SELECT bool_col, int_col FROM functional.alltypes " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL SELECT bool_col, bigint_col FROM functional.alltypes");
    // With 'order by' and 'limit' on union, and also on last select.
    testToSql("(select bool_col, int_col from functional.alltypes) " +
        "union all (select bool_col, int_col from functional.alltypessmall) " +
        "union all (select bool_col, bigint_col " +
        "from functional.alltypes order by 1 nulls first limit 1) " +
        "order by int_col nulls first, bool_col limit 5 + 5",
        "SELECT bool_col, int_col FROM functional.alltypes " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL SELECT bool_col, bigint_col " +
        "FROM functional.alltypes ORDER BY 1 ASC NULLS FIRST LIMIT 1 " +
        "ORDER BY int_col ASC NULLS FIRST, bool_col ASC LIMIT 5 + 5");
    // With 'order by' and 'limit' on union but not on last select.
    testToSql("select bool_col, int_col from functional.alltypes " +
        "union all select bool_col, int_col from functional.alltypessmall " +
        "union all (select bool_col, bigint_col from functional.alltypes) " +
        "order by int_col nulls first, bool_col limit 10",
        "SELECT bool_col, int_col FROM functional.alltypes " +
        "UNION ALL SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION ALL (SELECT bool_col, bigint_col FROM functional.alltypes) " +
        "ORDER BY int_col ASC NULLS FIRST, bool_col ASC LIMIT 10");
    // Nested unions require parenthesis.
    testToSql("select bool_col, int_col from functional.alltypes " +
        "union all (select bool_col, int_col from functional.alltypessmall " +
        "union distinct (select bool_col, bigint_col from functional.alltypes)) " +
        "order by int_col, bool_col limit 10",
        "SELECT bool_col, int_col FROM functional.alltypes UNION ALL " +
        "(SELECT bool_col, int_col FROM functional.alltypessmall " +
        "UNION SELECT bool_col, bigint_col FROM functional.alltypes) " +
        "ORDER BY int_col ASC, bool_col ASC LIMIT 10");
  }

  @Test
  public void valuesTest() {
    testToSql("values(1, 'a', 1.0)", "VALUES(1, 'a', 1.0)");
    testToSql("values(1 as x, 'a' y, 1.0 as z)", "VALUES(1 x, 'a' y, 1.0 z)");
    testToSql("values(1, 'a'), (2, 'b'), (3, 'c')",
        "VALUES((1, 'a'), (2, 'b'), (3, 'c'))");
    testToSql("values(1 x, 'a' as y), (2 as y, 'b'), (3, 'c' x)",
        "VALUES((1 x, 'a' y), (2 y, 'b'), (3, 'c' x))");
    testToSql("select * from (values(1, 'a'), (2, 'b')) as t",
        "SELECT * FROM (VALUES((1, 'a'), (2, 'b'))) t");
    testToSql("values(1, 'a'), (2, 'b') union all values(3, 'c')",
        "VALUES((1, 'a'), (2, 'b')) UNION ALL (VALUES(3, 'c'))");
    testToSql("insert into table functional.alltypessmall " +
        "partition (year=2009, month=4) " +
        "values(1, true, 1, 1, 10, 10, 10.0, 10.0, 'a', 'a', cast (0 as timestamp))",
        "INSERT INTO TABLE functional.alltypessmall PARTITION (year=2009, month=4) " +
        "VALUES(1, TRUE, 1, 1, 10, 10, 10.0, 10.0, 'a', 'a', CAST(0 AS TIMESTAMP))");
    testToSql("upsert into table functional_kudu.testtbl values(1, 'a', 1)",
        "UPSERT INTO TABLE functional_kudu.testtbl VALUES(1, 'a', 1)");
  }

  /**
   * Tests that toSql() properly handles inline views and their expression substitutions.
   */
  @Test
  public void inlineViewTest() {
    // Test joins in an inline view.
    testToSql("select t.* from " +
        "(select a.* from functional.alltypes a, functional.alltypes b " +
        "where a.id = b.id) t",
        "SELECT t.* FROM " +
        "(SELECT a.* FROM functional.alltypes a, functional.alltypes b " +
        "WHERE a.id = b.id) t");
    testToSql("select t.* from (select a.* from functional.alltypes a " +
        "cross join functional.alltypes b) t",
        "SELECT t.* FROM (SELECT a.* FROM functional.alltypes a " +
        "CROSS JOIN functional.alltypes b) t");
    runTestTemplate("select t.* from (select a.* from functional.alltypes a %s " +
            "functional.alltypes b %s) t",
        "SELECT t.* FROM (SELECT a.* FROM functional.alltypes a %s " +
            "functional.alltypes b %s) t", nonSemiJoinTypes_, joinConditions_);
    runTestTemplate("select t.* from (select a.* from functional.alltypes a %s " +
            "functional.alltypes b %s) t",
        "SELECT t.* FROM (SELECT a.* FROM functional.alltypes a %s " +
            "functional.alltypes b %s) t", leftSemiJoinTypes_, joinConditions_);
    runTestTemplate("select t.* from (select b.* from functional.alltypes a %s " +
            "functional.alltypes b %s) t",
        "SELECT t.* FROM (SELECT b.* FROM functional.alltypes a %s " +
            "functional.alltypes b %s) t", rightSemiJoinTypes_, joinConditions_);

    // Test undoing expr substitution in select-list exprs and on clause.
    testToSql("select t1.int_col, t2.int_col from " +
        "(select int_col, rank() over (order by int_col) from functional.alltypes) " +
        "t1 inner join " +
        "(select int_col from functional.alltypes) t2 on (t1.int_col = t2.int_col)",
        "SELECT t1.int_col, t2.int_col FROM " +
        "(SELECT int_col, rank() OVER (ORDER BY int_col ASC) " +
        "FROM functional.alltypes) t1 INNER JOIN " +
        "(SELECT int_col FROM functional.alltypes) t2 ON (t1.int_col = t2.int_col)");
    // Test undoing expr substitution in aggregates and group by and having clause.
    testToSql("select count(t1.string_col), sum(t2.float_col) from " +
        "(select id, string_col from functional.alltypes) t1 inner join " +
        "(select id, float_col from functional.alltypes) t2 on (t1.id = t2.id) " +
        "group by t1.id, t2.id having count(t2.float_col) > 2",
        "SELECT count(t1.string_col), sum(t2.float_col) FROM " +
        "(SELECT id, string_col FROM functional.alltypes) t1 INNER JOIN " +
        "(SELECT id, float_col FROM functional.alltypes) t2 ON (t1.id = t2.id) " +
        "GROUP BY t1.id, t2.id HAVING count(t2.float_col) > 2");
    // Test undoing expr substitution in order by clause.
    testToSql("select t1.id, t2.id from " +
        "(select id, string_col from functional.alltypes) t1 inner join " +
        "(select id, float_col from functional.alltypes) t2 on (t1.id = t2.id) " +
        "order by t1.id, t2.id nulls first",
        "SELECT t1.id, t2.id FROM " +
        "(SELECT id, string_col FROM functional.alltypes) t1 INNER JOIN " +
        "(SELECT id, float_col FROM functional.alltypes) t2 ON (t1.id = t2.id) " +
        "ORDER BY t1.id ASC, t2.id ASC NULLS FIRST");
    // Test undoing expr substitution in where-clause conjuncts.
    testToSql("select t1.id, t2.id from " +
        "(select id, string_col from functional.alltypes) t1, " +
        "(select id, float_col from functional.alltypes) t2 " +
        "where t1.id = t2.id and t1.string_col = 'abc' and t2.float_col < 10",
        "SELECT t1.id, t2.id FROM " +
        "(SELECT id, string_col FROM functional.alltypes) t1, " +
        "(SELECT id, float_col FROM functional.alltypes) t2 " +
        "WHERE t1.id = t2.id AND t1.string_col = 'abc' AND t2.float_col < 10");

    // Test inline views with correlated table refs. Implicit alias only.
    testToSql(
        "select cnt from functional.allcomplextypes t, " +
        "(select count(*) cnt from t.int_array_col) v",
        "SELECT cnt FROM functional.allcomplextypes t, " +
        "(SELECT count(*) cnt FROM t.int_array_col) v");
    // Multiple correlated table refs. Explicit aliases.
    testToSql(
        "select avg from functional.allcomplextypes t, " +
        "(select avg(a1.item) avg from t.int_array_col a1, t.int_array_col a2) v",
        "SELECT avg FROM functional.allcomplextypes t, " +
        "(SELECT avg(a1.item) avg FROM t.int_array_col a1, t.int_array_col a2) v");
    // Correlated table ref has child ref itself. Mix of explicit and implicit aliases.
    testToSql(
        "select key, item from functional.allcomplextypes t, " +
        "(select a1.key, value.item from t.array_map_col a1, a1.value) v",
        "SELECT key, item FROM functional.allcomplextypes t, " +
        "(SELECT a1.key, value.item FROM t.array_map_col a1, a1.value) v");
    // Correlated table refs in a union.
    testToSql(
        "select item from functional.allcomplextypes t, " +
        "(select * from t.int_array_col union all select * from t.int_array_col) v",
        "SELECT item FROM functional.allcomplextypes t, " +
        "(SELECT * FROM t.int_array_col UNION ALL SELECT * FROM t.int_array_col) v");
    // Correlated inline view in WITH-clause.
    testToSql(
        "with w as (select c from functional.allcomplextypes t, " +
        "(select count(a1.key) c from t.array_map_col a1) v1) " +
        "select * from w",
        "WITH w AS (SELECT c FROM functional.allcomplextypes t, " +
        "(SELECT count(a1.key) c FROM t.array_map_col a1) v1) " +
        "SELECT * FROM w");
  }

  @Test
  public void TestUpdate() {
    TestUtils.assumeKuduIsSupported();
    testToSql("update functional_kudu.dimtbl set name = '10' where name < '11'",
        "UPDATE functional_kudu.dimtbl SET name = '10' FROM functional_kudu.dimtbl " +
            "WHERE name < '11'");

    testToSql(
        "update functional_kudu.dimtbl set name = '10', zip=cast(99 as int) where name " +
            "< '11'",
        "UPDATE functional_kudu.dimtbl SET name = '10', zip = CAST(99 AS INT) FROM " +
            "functional_kudu.dimtbl WHERE name < '11'");

    testToSql("update a set name = '10' FROM functional_kudu.dimtbl a",
        "UPDATE a SET name = '10' FROM functional_kudu.dimtbl a");

    testToSql(
        "update a set a.name = 'oskar' from functional_kudu.dimtbl a join functional" +
            ".alltypes b on a.id = b.id where zip > 94549",
        "UPDATE a SET a.name = 'oskar' FROM functional_kudu.dimtbl a INNER JOIN " +
            "functional.alltypes b ON a.id = b.id WHERE zip > 94549");
  }

  @Test
  public void TestDelete() {
    TestUtils.assumeKuduIsSupported();
    testToSql("delete functional_kudu.testtbl where zip = 10",
        "DELETE FROM functional_kudu.testtbl WHERE zip = 10");
    testToSql("delete from functional_kudu.testtbl where zip = 10",
        "DELETE FROM functional_kudu.testtbl WHERE zip = 10");
    testToSql("delete a from functional_kudu.testtbl a where zip = 10",
        "DELETE a FROM functional_kudu.testtbl a WHERE zip = 10");
  }

  /**
   * Tests that toSql() properly handles subqueries in the where clause.
   */
  // TODO Fix testToSql to print the stmt after the first analysis phase and not
  // after the rewrite.
  @Ignore("Prints the rewritten statement")
  @Test
  public void subqueryTest() {
    // Nested predicates
    testToSql("select * from functional.alltypes where id in " +
        "(select id from functional.alltypestiny)",
        "SELECT * FROM functional.alltypes WHERE id IN " +
        "(SELECT id FROM functional.alltypestiny)");
    testToSql("select * from functional.alltypes where id not in " +
            "(select id from functional.alltypestiny)",
        "SELECT * FROM functional.alltypes WHERE id NOT IN " +
            "(SELECT id FROM functional.alltypestiny)");
    testToSql("select * from functional.alltypes where bigint_col = " +
            "(select count(*) from functional.alltypestiny)",
        "SELECT * FROM functional.alltypes WHERE bigint_col = " +
            "(SELECT count(*) FROM functional.alltypestiny)");
    testToSql("select * from functional.alltypes where exists " +
            "(select * from functional.alltypestiny)",
        "SELECT * FROM functional.alltypes WHERE EXISTS " +
            "(SELECT * FROM functional.alltypestiny)");
    testToSql("select * from functional.alltypes where not exists " +
            "(select * from functional.alltypestiny)",
        "SELECT * FROM functional.alltypes WHERE NOT EXISTS " +
            "(SELECT * FROM functional.alltypestiny)");
    // Multiple nested predicates in the WHERE clause
    testToSql("select * from functional.alltypes where not (id < 10 and " +
            "(int_col in (select int_col from functional.alltypestiny)) and " +
            "(string_col = (select max(string_col) from functional.alltypestiny)))",
        "SELECT * FROM functional.alltypes WHERE NOT (id < 10 AND " +
            "(int_col IN (SELECT int_col FROM functional.alltypestiny)) AND " +
        "(string_col = (SELECT max(string_col) FROM functional.alltypestiny)))");
    // Multiple nesting levels
    testToSql("select * from functional.alltypes where id in " +
        "(select id from functional.alltypestiny where int_col = " +
        "(select avg(int_col) from functional.alltypesagg))",
        "SELECT * FROM functional.alltypes WHERE id IN " +
        "(SELECT id FROM functional.alltypestiny WHERE int_col = " +
        "(SELECT avg(int_col) FROM functional.alltypesagg))");
    // Inline view with a subquery
    testToSql("select * from (select id from functional.alltypes where " +
        "int_col in (select int_col from functional.alltypestiny)) t where " +
        "t.id < 10",
        "SELECT * FROM (SELECT id FROM functional.alltypes WHERE " +
        "int_col IN (SELECT int_col FROM functional.alltypestiny)) t WHERE " +
        "t.id < 10");
    // Subquery in a WITH clause
    testToSql("with t as (select * from functional.alltypes where id in " +
        "(select id from functional.alltypestiny)) select * from t",
        "WITH t AS (SELECT * FROM functional.alltypes WHERE id IN " +
        "(SELECT id FROM functional.alltypestiny)) SELECT * FROM t");
    testToSql("with t as (select * from functional.alltypes s where id in " +
        "(select id from functional.alltypestiny t where s.id = t.id)) " +
        "select * from t t1, t t2 where t1.id = t2.id",
        "WITH t AS (SELECT * FROM functional.alltypes s WHERE id IN " +
        "(SELECT id FROM functional.alltypestiny t WHERE s.id = t.id)) " +
        "SELECT * FROM t t1, t t2 WHERE t1.id = t2.id");
  }

  @Test
  public void withClauseTest() {
    // WITH clause in select stmt.
    testToSql("with t as (select * from functional.alltypes) select * from t",
        "WITH t AS (SELECT * FROM functional.alltypes) SELECT * FROM t");
    testToSql("with t(c1) as (select * from functional.alltypes) select * from t",
        "WITH t(c1) AS (SELECT * FROM functional.alltypes) SELECT * FROM t");
    testToSql("with t(`table`, col, `create`) as (select * from functional.alltypes) " +
        "select * from t",
        "WITH t(`table`, col, `create`) AS (SELECT * FROM functional.alltypes) " +
        "SELECT * FROM t");
    testToSql("with t(c1, c2) as (select * from functional.alltypes) select * from t",
        "WITH t(c1, c2) AS (SELECT * FROM functional.alltypes) SELECT * FROM t");
    testToSql("with t as (select sum(int_col) over(partition by tinyint_col, " +
        "bool_col order by float_col rows between unbounded preceding and " +
        "current row) as x from functional.alltypes) " +
        "select t1.x, t2.x from t t1 join t t2 on (t1.x = t2.x)",
        "WITH t AS (SELECT sum(int_col) OVER (PARTITION BY tinyint_col, bool_col " +
        "ORDER BY float_col ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
        "x FROM functional.alltypes) SELECT t1.x, t2.x FROM t t1 INNER JOIN t t2 ON " +
        "(t1.x = t2.x)");
    // WITH clause in select stmt with a join and an ON clause.
    testToSql("with t as (select * from functional.alltypes) " +
        "select * from t a inner join t b on (a.int_col = b.int_col)",
        "WITH t AS (SELECT * FROM functional.alltypes) " +
        "SELECT * FROM t a INNER JOIN t b ON (a.int_col = b.int_col)");
    testToSql("with t(c1, c2) as (select * from functional.alltypes) " +
        "select a.c1, a.c2 from t a inner join t b on (a.c1 = b.c2)",
        "WITH t(c1, c2) AS (SELECT * FROM functional.alltypes) " +
        "SELECT a.c1, a.c2 FROM t a INNER JOIN t b ON (a.c1 = b.c2)");
    // WITH clause in select stmt with a join and a USING clause.
    testToSql("with t as (select * from functional.alltypes) " +
        "select * from t a inner join t b using(int_col)",
        "WITH t AS (SELECT * FROM functional.alltypes) " +
        "SELECT * FROM t a INNER JOIN t b USING (int_col)");
    // WITH clause in a union stmt.
    testToSql("with t1 as (select * from functional.alltypes)" +
        "select * from t1 union all select * from t1",
        "WITH t1 AS (SELECT * FROM functional.alltypes) " +
        "SELECT * FROM t1 UNION ALL SELECT * FROM t1");
    // WITH clause in values stmt.
    testToSql("with t1 as (select * from functional.alltypes) values(1, 2), (3, 4)",
        "WITH t1 AS (SELECT * FROM functional.alltypes) VALUES((1, 2), (3, 4))");
    // WITH clause in insert stmt.
    testToSql("with t1 as (select * from functional.alltypes) " +
            "insert into functional.alltypes partition(year, month) select * from t1",
        "WITH t1 AS (SELECT * FROM functional.alltypes) " +
        "INSERT INTO TABLE functional.alltypes PARTITION (year, month) " +
            "SELECT * FROM t1");
    // WITH clause in upsert stmt.
    testToSql("with t1 as (select * from functional.alltypes) upsert into " +
        "functional_kudu.testtbl select bigint_col, string_col, int_col from t1",
        "WITH t1 AS (SELECT * FROM functional.alltypes) UPSERT INTO TABLE " +
        "functional_kudu.testtbl SELECT bigint_col, string_col, int_col FROM t1");
    // Test joins in WITH-clause view.
    testToSql("with t as (select a.* from functional.alltypes a, " +
            "functional.alltypes b where a.id = b.id) select * from t",
        "WITH t AS (SELECT a.* FROM functional.alltypes a, " +
            "functional.alltypes b WHERE a.id = b.id) SELECT * FROM t");
    testToSql("with t as (select a.* from functional.alltypes a " +
            "cross join functional.alltypes b) select * from t",
        "WITH t AS (SELECT a.* FROM functional.alltypes a " +
        "CROSS JOIN functional.alltypes b) SELECT * FROM t");
    runTestTemplate("with t as (select a.* from functional.alltypes a %s " +
        "functional.alltypes b %s) select * from t",
        "WITH t AS (SELECT a.* FROM functional.alltypes a %s " +
        "functional.alltypes b %s) SELECT * FROM t", nonSemiJoinTypes_, joinConditions_);
    runTestTemplate("with t as (select a.* from functional.alltypes a %s " +
        "functional.alltypes b %s) select * from t",
        "WITH t AS (SELECT a.* FROM functional.alltypes a %s " +
        "functional.alltypes b %s) SELECT * FROM t",
        leftSemiJoinTypes_, joinConditions_);
    runTestTemplate("with t as (select b.* from functional.alltypes a %s " +
        "functional.alltypes b %s) select * from t",
        "WITH t AS (SELECT b.* FROM functional.alltypes a %s " +
        "functional.alltypes b %s) SELECT * FROM t",
        rightSemiJoinTypes_, joinConditions_);
    // WITH clause in complex query with joins and and order by + limit.
    testToSql("with t as (select int_col x, bigint_col y from functional.alltypestiny " +
        "order by id nulls first limit 2) " +
        "select * from t t1 left outer join t t2 on t1.y = t2.x " +
        "full outer join t t3 on t2.y = t3.x order by t1.x nulls first limit 5 * 2",
        "WITH t AS (SELECT int_col x, bigint_col y FROM functional.alltypestiny " +
        "ORDER BY id ASC NULLS FIRST LIMIT 2) " +
        "SELECT * FROM t t1 LEFT OUTER JOIN t t2 ON t1.y = t2.x " +
        "FULL OUTER JOIN t t3 ON t2.y = t3.x ORDER BY t1.x ASC NULLS FIRST LIMIT 5 * 2");
  }

  // Test the toSql() output of insert queries.
  @Test
  public void insertTest() {
    // Insert into unpartitioned table without partition clause.
    testToSql("insert into table functional.alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "INSERT INTO TABLE functional.alltypesnopart " +
        "SELECT id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col, timestamp_col FROM functional.alltypes");
    // Insert into overwrite unpartitioned table without partition clause.
    testToSql("insert overwrite table functional.alltypesnopart " +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "INSERT OVERWRITE TABLE functional.alltypesnopart " +
        "SELECT id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col, timestamp_col FROM functional.alltypes");
    // Static partition.
    testToSql("insert into table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "INSERT INTO TABLE functional.alltypessmall " +
        "PARTITION (year=2009, month=4) SELECT id, " +
        "bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, " +
        "double_col, date_string_col, string_col, timestamp_col " +
        "FROM functional.alltypes");
    // Fully dynamic partitions.
    testToSql("insert into table functional.alltypessmall " +
        "partition (year, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year, " +
        "month from functional.alltypes",
        "INSERT INTO TABLE functional.alltypessmall " +
        "PARTITION (year, month) SELECT id, bool_col, " +
        "tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, " +
        "date_string_col, string_col, timestamp_col, year, month " +
        "FROM functional.alltypes");
    // Partially dynamic partitions.
    testToSql("insert into table functional.alltypessmall " +
        "partition (year=2009, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, month " +
        "from functional.alltypes",
        "INSERT INTO TABLE functional.alltypessmall " +
        "PARTITION (year=2009, month) SELECT id, " +
        "bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, " +
        "double_col, date_string_col, string_col, timestamp_col, month " +
        "FROM functional.alltypes");

    // Permutations
    testToSql("insert into table functional.alltypesnopart(id, bool_col, tinyint_col) " +
        " values(1, true, 0)",
        "INSERT INTO TABLE functional.alltypesnopart(id, bool_col, tinyint_col) " +
        "VALUES(1, TRUE, 0)");

    // Permutations that mention partition column
    testToSql("insert into table functional.alltypes(id, year, month) " +
        " values(1, 1990, 12)",
        "INSERT INTO TABLE functional.alltypes(id, year, month) " +
        "VALUES(1, 1990, 12)");

    // Empty permutation with no select statement
    testToSql("insert into table functional.alltypesnopart()",
              "INSERT INTO TABLE functional.alltypesnopart()");

    // Permutation and explicit partition clause
    testToSql("insert into table functional.alltypes(id) " +
        " partition (year=2009, month) values(1, 12)",
        "INSERT INTO TABLE functional.alltypes(id) " +
        "PARTITION (year=2009, month) VALUES(1, 12)");
  }

  @Test
  public void upsertTest() {
    // VALUES clause
    testToSql("upsert into functional_kudu.testtbl values (1, 'a', 1)",
        "UPSERT INTO TABLE functional_kudu.testtbl VALUES(1, 'a', 1)");

    // SELECT clause
    testToSql("upsert into functional_kudu.testtbl select bigint_col, string_col, " +
        "int_col from functional.alltypes", "UPSERT INTO TABLE functional_kudu.testtbl " +
        "SELECT bigint_col, string_col, int_col FROM functional.alltypes");

    // WITH clause
    testToSql("with x as (select bigint_col, string_col, int_col from " +
        "functional.alltypes) upsert into table functional_kudu.testtbl select * from x",
        "WITH x AS (SELECT bigint_col, string_col, int_col FROM functional.alltypes) " +
        "UPSERT INTO TABLE functional_kudu.testtbl SELECT * FROM x");

    // Permutation
    testToSql("upsert into table functional_kudu.testtbl (zip, id, name) values " +
        "(1, 1, 'a')", "UPSERT INTO TABLE functional_kudu.testtbl(zip, id, name) " +
        "VALUES(1, 1, 'a')");
  }

  @Test
  public void alterTableAddPartitionTest() {
    // Add partition
    testToSql(
        "alter table functional.alltypes add partition (year=2050, month=1)",
        "ALTER TABLE functional.alltypes ADD PARTITION (year=2050, month=1)");
    // Add multiple partitions
    testToSql(
        "alter table functional.alltypes add partition (year=2050, month=1) " +
        "partition (year=2050, month=2)",
        "ALTER TABLE functional.alltypes ADD PARTITION (year=2050, month=1) " +
        "PARTITION (year=2050, month=2)");
    // with IF NOT EXISTS
    testToSql(
        "alter table functional.alltypes add if not exists " +
        "partition (year=2050, month=1) " +
        "partition (year=2050, month=2)",
        "ALTER TABLE functional.alltypes ADD IF NOT EXISTS " +
        "PARTITION (year=2050, month=1) " +
        "PARTITION (year=2050, month=2)");
    // with location
    testToSql(
        "alter table functional.alltypes add if not exists " +
        "partition (year=2050, month=1) location 'hdfs://localhost:20500/y2050m1' " +
        "partition (year=2050, month=2) location '/y2050m2'",
        "ALTER TABLE functional.alltypes ADD IF NOT EXISTS "+
        "PARTITION (year=2050, month=1) LOCATION 'hdfs://localhost:20500/y2050m1' " +
        "PARTITION (year=2050, month=2) LOCATION 'hdfs://localhost:20500/y2050m2'");
    // and caching
    testToSql(
        "alter table functional.alltypes add if not exists " +
        "partition (year=2050, month=1) location 'hdfs://localhost:20500/y2050m1' " +
        "cached in 'testPool' with replication=3 " +
        "partition (year=2050, month=2) location '/y2050m2' " +
        "uncached",
        "ALTER TABLE functional.alltypes ADD IF NOT EXISTS "+
        "PARTITION (year=2050, month=1) LOCATION 'hdfs://localhost:20500/y2050m1' " +
        "CACHED IN 'testPool' WITH REPLICATION = 3 " +
        "PARTITION (year=2050, month=2) LOCATION 'hdfs://localhost:20500/y2050m2' " +
        "UNCACHED");
  }

  @Test
  public void testAnalyticExprs() {
    testToSql(
        "select sum(int_col) over (partition by id order by tinyint_col "
          + "rows between unbounded preceding and current row) from functional.alltypes",
        "SELECT sum(int_col) OVER (PARTITION BY id ORDER BY tinyint_col ASC ROWS "
          + "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM functional.alltypes");
    testToSql(
        "select last_value(tinyint_col ignore nulls) over (order by tinyint_col) "
          + "from functional.alltypesagg",
        "SELECT last_value(tinyint_col IGNORE NULLS) OVER (ORDER BY tinyint_col ASC) "
          + "FROM functional.alltypesagg");
  }

  /**
   * Tests all expressions including whether their toSql() is properly
   * enclosed in parentheses.
   */
  @Test
  public void testExprs() {
    // AggregateExpr.
    testToSql("select count(*), (count(*)), avg(int_col), (avg(int_col)), " +
        "sum(int_col), (sum(int_col)), min(int_col), (min(int_col)), " +
        "max(int_col), (max(int_col)) from functional.alltypes",
        "SELECT count(*), (count(*)), avg(int_col), (avg(int_col)), " +
        "sum(int_col), (sum(int_col)), min(int_col), (min(int_col)), " +
        "max(int_col), (max(int_col)) FROM functional.alltypes");
    // ArithmeticExpr.
    testToSql("select 1 * 1, (1 * 1), 2 / 2, (2 / 2), 3 % 3, (3 % 3), " +
        "4 DIV 4, (4 DIV 4), 5 + 5, (5 + 5), 6 - 6, (6 - 6), 7 & 7, (7 & 7), " +
        "8 | 8, (8 | 8), 9 ^ 9, (9 ^ 9), ~10, (~10)",
        "SELECT 1 * 1, (1 * 1), 2 / 2, (2 / 2), 3 % 3, (3 % 3), " +
         "4 DIV 4, (4 DIV 4), 5 + 5, (5 + 5), 6 - 6, (6 - 6), 7 & 7, (7 & 7), " +
        "8 | 8, (8 | 8), 9 ^ 9, (9 ^ 9), ~10, (~10)");
    testToSql("select (((1 + 2) * (3 - 4) + 6) / 7)",
        "SELECT (((1 + 2) * (3 - 4) + 6) / 7)");

    // CaseExpr.
    // Single case without else clause. No case expr.
    testToSql("select case when true then 1 end, " +
        "(case when true then 1 end)",
        "SELECT CASE WHEN TRUE THEN 1 END, " +
        "(CASE WHEN TRUE THEN 1 END)");
    // Multiple cases with else clause. No case expr.
    testToSql("select case when true then 1 when false then 2 else 3 end, " +
        "(case when true then 1 when false then 2 else 3 end)",
        "SELECT CASE WHEN TRUE THEN 1 WHEN FALSE THEN 2 ELSE 3 END, " +
        "(CASE WHEN TRUE THEN 1 WHEN FALSE THEN 2 ELSE 3 END)");
    // Multiple cases with else clause with case expr.
    testToSql("select case true when true then 1 when false then 2 else 3 end, " +
        "(case true when true then 1 when false then 2 else 3 end)",
        "SELECT CASE TRUE WHEN TRUE THEN 1 WHEN FALSE THEN 2 ELSE 3 END, " +
        "(CASE TRUE WHEN TRUE THEN 1 WHEN FALSE THEN 2 ELSE 3 END)");
    // DECODE version of CaseExpr.
    testToSql("select decode(1, 2, 3), (decode(4, 5, 6))",
        "SELECT decode(1, 2, 3), (decode(4, 5, 6))");
    testToSql("select decode(1, 2, 3, 4, 5, 6), (decode(1, 2, 3, 4, 5, 6))",
        "SELECT decode(1, 2, 3, 4, 5, 6), (decode(1, 2, 3, 4, 5, 6))");

    // CastExpr.
    testToSql("select cast(NULL as INT), (cast(NULL as INT))",
        "SELECT CAST(NULL AS INT), (CAST(NULL AS INT))");
    // FunctionCallExpr.
    testToSql("select pi(), (pi()), trim('a'), (trim('a'))",
        "SELECT pi(), (pi()), trim('a'), (trim('a'))");
    // LiteralExpr.
    testToSql("select 10, (10), 20.0, (20.0), NULL, (NULL), 'abc', ('abc')",
        "SELECT 10, (10), 20.0, (20.0), NULL, (NULL), 'abc', ('abc')");
    // BetweenPredicate.
    testToSql("select 5 between 10 and 20, (5 between 10 and 20)",
        "SELECT 5 BETWEEN 10 AND 20, (5 BETWEEN 10 AND 20)");
    testToSql("select 5 not between 10 and 20, (5 not between 10 and 20)",
        "SELECT 5 NOT BETWEEN 10 AND 20, (5 NOT BETWEEN 10 AND 20)");
    // BinaryPredicate.
    testToSql("select 'a' = 'b', ('a' = 'b'), 'a' != 'b', ('a' != 'b'), " +
        "1 < 2, (1 < 2), 1 <= 2, (1 <= 2), 1 > 2, (1 > 2), 1 >= 2, (1 >= 2)",
        "SELECT 'a' = 'b', ('a' = 'b'), 'a' != 'b', ('a' != 'b'), " +
        "1 < 2, (1 < 2), 1 <= 2, (1 <= 2), 1 > 2, (1 > 2), 1 >= 2, (1 >= 2)");
    // CompoundPredicate.
    testToSql("select true and false, (true and false), " +
        "false or true, (false or true), " +
        "!true, (!true), not false, (not false)",
        "SELECT TRUE AND FALSE, (TRUE AND FALSE), " +
         "FALSE OR TRUE, (FALSE OR TRUE), " +
        "NOT TRUE, (NOT TRUE), NOT FALSE, (NOT FALSE)");
    testToSql("select ((true and (false or false) or true) and (false or true))",
        "SELECT ((TRUE AND (FALSE OR FALSE) OR TRUE) AND (FALSE OR TRUE))");
    // InPredicate.
    testToSql("select 5 in (4, 6, 7, 5), (5 in (4, 6, 7, 5))," +
        "5 not in (4, 6, 7, 5), (5 not In (4, 6, 7, 5))",
        "SELECT 5 IN (4, 6, 7, 5), (5 IN (4, 6, 7, 5)), " +
        "5 NOT IN (4, 6, 7, 5), (5 NOT IN (4, 6, 7, 5))");
    // IsNullPredicate.
    testToSql("select 5 is null, (5 is null), 10 is not null, (10 is not null)",
        "SELECT 5 IS NULL, (5 IS NULL), 10 IS NOT NULL, (10 IS NOT NULL)");
    // Boolean test expression (expanded to istrue/false).
    testToSql("select (true is true)", "SELECT (istrue(TRUE))");
    testToSql("select (true is not true)", "SELECT (isnottrue(TRUE))");
    testToSql("select (true is false)", "SELECT (isfalse(TRUE))");
    testToSql("select (true is unknown)", "SELECT (TRUE IS NULL)");
    testToSql("select (true is not unknown)", "SELECT (TRUE IS NOT NULL)");
    testToSql("select not(true is true)", "SELECT NOT (istrue(TRUE))");
    testToSql("select (false is false)", "SELECT (isfalse(FALSE))");
    testToSql("select (null is unknown)", "SELECT (NULL IS NULL)");
    testToSql("select (1 > 1 is true is unknown)", "SELECT (istrue(1 > 1) IS NULL)");
    // LikePredicate.
    testToSql("select 'a' LIKE '%b.', ('a' LIKE '%b.'), " +
        "'a' ILIKE '%b.', ('a' ILIKE '%b.'), " +
        "'b' RLIKE '.c%', ('b' RLIKE '.c%')," +
        "'d' IREGEXP '.e%', ('d' IREGEXP '.e%')," +
        "'d' REGEXP '.e%', ('d' REGEXP '.e%')",
        "SELECT 'a' LIKE '%b.', ('a' LIKE '%b.'), " +
        "'a' ILIKE '%b.', ('a' ILIKE '%b.'), " +
        "'b' RLIKE '.c%', ('b' RLIKE '.c%'), " +
        "'d' IREGEXP '.e%', ('d' IREGEXP '.e%'), " +
        "'d' REGEXP '.e%', ('d' REGEXP '.e%')" );
    // SlotRef.
    testToSql("select bool_col, (bool_col), int_col, (int_col) " +
        "string_col, (string_col), timestamp_col, (timestamp_col) " +
        "from functional.alltypes",
        "SELECT bool_col, (bool_col), int_col, (int_col) " +
         "string_col, (string_col), timestamp_col, (timestamp_col) " +
        "FROM functional.alltypes");

    // TimestampArithmeticExpr.
    // Non-function-call like version.
    testToSql("select timestamp_col + interval 10 years, " +
        "(timestamp_col + interval 10 years) from functional.alltypes",
        "SELECT timestamp_col + INTERVAL 10 years, " +
        "(timestamp_col + INTERVAL 10 years) FROM functional.alltypes");
    testToSql("select timestamp_col - interval 20 months, " +
        "(timestamp_col - interval 20 months) from functional.alltypes",
        "SELECT timestamp_col - INTERVAL 20 months, " +
        "(timestamp_col - INTERVAL 20 months) FROM functional.alltypes");
    // Reversed interval and timestamp using addition.
    testToSql("select interval 30 weeks + timestamp_col, " +
        "(interval 30 weeks + timestamp_col) from functional.alltypes",
        "SELECT INTERVAL 30 weeks + timestamp_col, " +
        "(INTERVAL 30 weeks + timestamp_col) FROM functional.alltypes");
    // Function-call like version.
    testToSql("select date_add(timestamp_col, interval 40 days), " +
        "(date_add(timestamp_col, interval 40 days)) from functional.alltypes",
        "SELECT DATE_ADD(timestamp_col, INTERVAL 40 days), " +
        "(DATE_ADD(timestamp_col, INTERVAL 40 days)) FROM functional.alltypes");
    testToSql("select date_sub(timestamp_col, interval 40 hours), " +
        "(date_sub(timestamp_col, interval 40 hours)) from functional.alltypes",
        "SELECT DATE_SUB(timestamp_col, INTERVAL 40 hours), " +
        "(DATE_SUB(timestamp_col, INTERVAL 40 hours)) FROM functional.alltypes");
  }

  /**
   * Tests decimals are output correctly.
   */
  @Test
  public void testDecimal() {
    testToSql("select cast(1 as decimal)", "SELECT CAST(1 AS DECIMAL(9,0))");
  }

  /**
   * Tests set query option statements are output correctly.
   */
  @Test
  public void testSet() {
    testToSql("set a = 1", "SET a='1'");
    testToSql("set `a b` = \"x y\"", "SET `a b`='x y'");
    testToSql("set", "SET");
  }

  @Test
  public void testTableSample() {
    testToSql("select * from functional.alltypes tablesample system(10)",
        "SELECT * FROM functional.alltypes TABLESAMPLE SYSTEM(10)");
    testToSql(
        "select * from functional.alltypes tablesample system(10) repeatable(20)",
        "SELECT * FROM functional.alltypes TABLESAMPLE SYSTEM(10) REPEATABLE(20)");
    testToSql(
        "select * from functional.alltypes a " +
        "tablesample system(10) /* +schedule_random */",
        "SELECT * FROM functional.alltypes a " +
        "TABLESAMPLE SYSTEM(10)\n-- +schedule_random\n");
    testToSql(
        "with t as (select * from functional.alltypes tablesample system(5)) " +
        "select * from t",
        "WITH t AS (SELECT * FROM functional.alltypes TABLESAMPLE SYSTEM(5)) " +
        "SELECT * FROM t");
  }

  /**
   * Tests invalidate statements are output correctly.
   */
  @Test
  public void testInvalidate() {
    testToSql("INVALIDATE METADATA", "INVALIDATE METADATA");
    testToSql("INVALIDATE METADATA functional.alltypes",
        "INVALIDATE METADATA functional.alltypes");
  }

  /**
   * Tests refresh statements are output correctly.
   */
  @Test
  public void testRefresh() {
    testToSql("REFRESH functional.alltypes", "REFRESH functional.alltypes");
    testToSql("REFRESH functional.alltypes PARTITION (year=2009, month=1)",
        "REFRESH functional.alltypes PARTITION (year=2009, month=1)");
    testToSql("REFRESH FUNCTIONS functional", "REFRESH FUNCTIONS functional");
  }
}
