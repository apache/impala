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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TFunctionCategory;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AnalyzeStmtsTest extends AnalyzerTest {

  static {
    FeSupport.loadLibrary();
  }

  /**
   * Tests analyzing the given collection table reference and field assumed to be in
   * functional.allcomplextypes, including different combinations of
   * implicit/explicit aliases of the parent and collection table.
   */
  private void testCollectionTableRefs(String collectionTable, String collectionField) {
    TableName tbl = new TableName("functional", "allcomplextypes");

    // Collection table uses unqualified implicit alias of parent table.
    TblsAnalyzeOk(String.format("select %s from $TBL, allcomplextypes.%s",
        collectionField, collectionTable), tbl);
    // Collection table uses fully qualified implicit alias of parent table.
    TblsAnalyzeOk(String.format("select %s from $TBL, functional.allcomplextypes.%s",
        collectionField, collectionTable), tbl);
    // Collection table uses explicit alias of parent table.
    TblsAnalyzeOk(String.format("select %s from $TBL a, a.%s",
        collectionField, collectionTable), tbl);

    // Parent/collection/collection join.
    TblsAnalyzeOk(String.format("select b.%s from $TBL a, a.%s b, a.int_map_col c",
        collectionField, collectionTable), tbl);
    TblsAnalyzeOk(String.format("select c.%s from $TBL a, a.int_array_col b, a.%s c",
        collectionField, collectionTable), tbl);
    TblsAnalyzeOk(String.format(
        "select 1 from $TBL, allcomplextypes.%s, functional.allcomplextypes.%s",
        collectionTable, collectionTable), tbl);

    // Test join types. Parent/collection joins do not require an ON or USING clause.
    for (JoinOperator joinOp: JoinOperator.values()) {
      if (joinOp.isNullAwareLeftAntiJoin() || joinOp.isIcebergDeleteJoin()) continue;
      TblsAnalyzeOk(String.format("select 1 from $TBL %s allcomplextypes.%s",
          joinOp, collectionTable), tbl);
      TblsAnalyzeOk(String.format("select 1 from $TBL a %s a.%s",
          joinOp, collectionTable), tbl);
    }

    // Legal, but not a parent/collection join.
    TblsAnalyzeOk(String.format("select %s from $TBL a, functional.allcomplextypes.%s",
        collectionField, collectionTable), tbl);
    TblsAnalyzeOk(String.format("select %s from $TBL.%s, functional.allcomplextypes",
        collectionField, collectionTable), tbl);
    TblsAnalyzeOk(String.format("select %s from functional.allcomplextypes a, $TBL.%s",
        collectionField, collectionTable), tbl);
    TblsAnalyzeOk(String.format("select %s from functional.allcomplextypes.%s, $TBL",
        collectionField, collectionTable), tbl);
    // Non parent/collection outer or semi  joins require an ON or USING clause.
    for (JoinOperator joinOp: JoinOperator.values()) {
      if (joinOp.isNullAwareLeftAntiJoin() || joinOp.isCrossJoin() || joinOp.isInnerJoin()
          || joinOp.isIcebergDeleteJoin()) {
        continue;
      }
      AnalysisError(String.format(
          "select 1 from functional.allcomplextypes.%s %s functional.allcomplextypes",
          collectionTable, joinOp),
          String.format("%s requires an ON or USING clause", joinOp));
    }

    // Duplicate explicit alias.
    TblsAnalysisError(String.format("select %s from $TBL a, a.%s a",
        collectionField, collectionTable), tbl,
        "Duplicate table alias: 'a'");
    TblsAnalysisError(String.format("select %s from $TBL a, a.%s b, a.%s b",
        collectionField, collectionTable, collectionTable), tbl,
        "Duplicate table alias: 'b'");
    // Duplicate implicit alias.
    TblsAnalysisError(String.format("select %s from $TBL a, a.%s, a.%s",
        collectionField, collectionTable, collectionTable), tbl,
        String.format("Duplicate table alias: '%s'", "a." + collectionTable));
    // Duplicate implicit/explicit alias.
    TblsAnalysisError(String.format(
        "select %s from $TBL, functional.allcomplextypes.%s allcomplextypes",
        collectionField, collectionTable), tbl,
        "Duplicate table alias: 'allcomplextypes'");

    // Parent/collection join requires the child to use an alias of the parent.
    AnalysisError(String.format(
        "select %s from allcomplextypes, %s", collectionField, collectionTable),
        createAnalysisCtx("functional"),
        String.format("Could not resolve table reference: '%s'", collectionTable));
    AnalysisError(String.format(
        "select %s from functional.allcomplextypes, %s",
        collectionField, collectionTable),
        String.format("Could not resolve table reference: '%s'", collectionTable));

    // Ambiguous collection table ref.
    AnalysisError(String.format(
        "select %s from functional.allcomplextypes, " +
        "functional_parquet.allcomplextypes, allcomplextypes.%s",
        collectionField, collectionTable),
        "Unqualified table alias is ambiguous: 'allcomplextypes'");
  }

  private boolean isCollectionTableRef(String tableName) {
    return tableName.split("\\.").length > 0;
  }

  /**
   * Test accessing all table/column combinations in the select list of a query
   * using implicit and explicit table aliases. The given tables are expected to
   * be unqualified and present in the 'functional' database.
   */
  private void testAllTableAliases(String[] tables, String[] columns)
      throws AnalysisException {
    for (String tbl: tables) {
      TableName tblName = new TableName("functional", tbl);
      String uqAlias = tbl.substring(tbl.lastIndexOf(".") + 1);
      String fqAlias = "functional." + tbl;
      // True if 'tbl' refers to a collection, false otherwise. A value of false implies
      // the table must be a base table or view.
      boolean isCollectionTblRef = isCollectionTableRef(tbl);
      for (String col: columns) {
        // Test implicit table aliases with unqualified and fully-qualified table names.
        TblsAnalyzeOk(String.format("select %s from $TBL", col), tblName);
        TblsAnalyzeOk(String.format("select %s.%s from $TBL", uqAlias, col), tblName);
        // Only references to base tables/views have a fully-qualified implicit alias.
        if (!isCollectionTblRef) {
          TblsAnalyzeOk(String.format("select %s.%s from $TBL", fqAlias, col), tblName);
        }

        // Explicit table alias.
        TblsAnalyzeOk(String.format("select %s from $TBL a", col), tblName);
        TblsAnalyzeOk(String.format("select a.%s from $TBL a", col), tblName);

        String errRefStr = "column/field reference";
        if (col.endsWith("*")) errRefStr = "star expression";
        // Explicit table alias must be used.
        TblsAnalysisError(String.format("select %s.%s from $TBL a",
            uqAlias, col, tbl), tblName,
            String.format("Could not resolve %s: '%s.%s'",
            errRefStr, uqAlias, col));
        TblsAnalysisError(String.format("select %s.%s from $TBL a",
            fqAlias, col, tbl), tblName,
            String.format("Could not resolve %s: '%s.%s'",
            errRefStr, fqAlias, col));
      }
    }

    // Test that multiple implicit fully-qualified aliases work.
    for (String t1: tables) {
      for (String t2: tables) {
        if (t1.equals(t2)) continue;
        // Collection tables do not have a fully-qualified implicit alias.
        if (isCollectionTableRef(t1) && isCollectionTableRef(t2)) continue;
        for (String col: columns) {
          AnalyzesOk(String.format(
              "select functional.%s.%s, functional.%s.%s " +
                  "from functional.%s, functional.%s", t1, col, t2, col, t1, t2));
        }
      }
    }

    String col = columns[0];
    for (String tbl: tables) {
      TableName tblName = new TableName("functional", tbl);
      // Make sure a column reference requires an existing table alias.
      TblsAnalysisError("select alltypessmall.int_col from $TBL", tblName,
          "Could not resolve column/field reference: 'alltypessmall.int_col'");
      // Duplicate explicit alias.
      TblsAnalysisError(
          String.format("select a.%s from $TBL a, functional.testtbl a", col),
          tblName, "Duplicate table alias");
      // Duplicate implicit alias.
      TblsAnalysisError(String.format("select %s from $TBL, $TBL", col), tblName,
          "Duplicate table alias");
      // Duplicate implicit/explicit alias.
      String uqAlias = tbl.substring(tbl.lastIndexOf(".") + 1);
      TblsAnalysisError(String.format(
          "select %s.%s from $TBL, functional.testtbl %s", tbl, col, uqAlias), tblName,
          "Duplicate table alias");
    }
  }

  /**
   * Test default hints applied during analysis.
   */
  public void testDefaultHintApplied(AnalysisContext insertCtx) {
    String defaultHints =
        insertCtx.getQueryOptions().getDefault_hints_insert_statement();
    List<PlanHint> planHints =
        insertCtx.getAnalysisResult().getInsertStmt().getPlanHints();
    String[] defaultHintsArray = defaultHints.trim().split(":");
    Assert.assertEquals(defaultHintsArray.length, planHints.size());
    for (String hint: defaultHintsArray) {
      Assert.assertTrue(planHints.contains(new PlanHint(hint.trim())));
    }
  }

  /**
   * Test default hints ignored when query has plan hints.
   */
  public void testDefaultHintIgnored(String query, String defaultHints) {
    // Analyze query with out default hints.
    AnalysisContext insertCtx = createAnalysisCtx();
    AnalyzesOk(query, insertCtx);
    List<PlanHint> planHints =
        insertCtx.getAnalysisResult().getInsertStmt().getPlanHints();

    // Analyze query with default hints.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement(defaultHints);
    AnalyzesOk(query, insertCtx);
    List<PlanHint> planHintsWithDefaultHints =
        insertCtx.getAnalysisResult().getInsertStmt().getPlanHints();

    // Default hint should be ignored when plan hints exist.
    Assert.assertEquals(planHints, planHintsWithDefaultHints);
  }

  @Test
  public void TestCollectionTableRefs() throws AnalysisException {
    // Test ARRAY type referenced as a table.
    testAllTableAliases(new String[] {
        "allcomplextypes.int_array_col"},
        new String[] {Path.ARRAY_POS_FIELD_NAME, Path.ARRAY_ITEM_FIELD_NAME, "*"});
    testAllTableAliases(new String[] {
        "allcomplextypes.struct_array_col"},
        new String[] {"f1", "f2", "*"});

    // Test MAP type referenced as a table.
    testAllTableAliases(new String[] {
        "allcomplextypes.int_map_col"},
        new String[] {Path.MAP_KEY_FIELD_NAME, Path.MAP_VALUE_FIELD_NAME, "*"});
    testAllTableAliases(new String[] {
        "allcomplextypes.struct_map_col"},
        new String[] {Path.MAP_KEY_FIELD_NAME, "f1", "f2", "*"});

    // Test complex table ref path with structs and multiple collections.
    testAllTableAliases(new String[] {
        "allcomplextypes.complex_nested_struct_col.f2.f12"},
        new String[] {Path.MAP_KEY_FIELD_NAME, "f21", "*"});

    // Test resolution of collection table refs.
    testCollectionTableRefs("int_array_col", Path.ARRAY_POS_FIELD_NAME);
    testCollectionTableRefs("int_array_col", Path.ARRAY_ITEM_FIELD_NAME);
    testCollectionTableRefs("int_map_col", Path.MAP_KEY_FIELD_NAME);
    testCollectionTableRefs("complex_nested_struct_col.f2.f12", "f21");

    // Path resolution error is reported before duplicate alias.
    AnalysisError("select 1 from functional.allcomplextypes a, a",
        "Illegal table reference to non-collection type: 'a'");

    // Invalid reference to non-collection type.
    AnalysisError("select 1 from functional.allcomplextypes.int_struct_col",
        "Illegal table reference to non-collection type: " +
         "'functional.allcomplextypes.int_struct_col'\n" +
        "Path resolved to type: STRUCT<f1:INT,f2:INT>");
    AnalysisError("select 1 from functional.allcomplextypes a, a.int_struct_col",
        "Illegal table reference to non-collection type: 'a.int_struct_col'\n" +
        "Path resolved to type: STRUCT<f1:INT,f2:INT>");
    AnalysisError("select 1 from functional.allcomplextypes.int_array_col.item",
        "Illegal table reference to non-collection type: " +
        "'functional.allcomplextypes.int_array_col.item'\n" +
        "Path resolved to type: INT");
    AnalysisError("select 1 from functional.allcomplextypes.int_array_col a, a.pos",
        "Illegal table reference to non-collection type: 'a.pos'\n" +
        "Path resolved to type: BIGINT");
    AnalysisError("select 1 from functional.allcomplextypes.int_map_col.key",
        "Illegal table reference to non-collection type: " +
        "'functional.allcomplextypes.int_map_col.key'\n" +
        "Path resolved to type: STRING");
    AnalysisError("select 1 from functional.allcomplextypes.int_map_col a, a.key",
        "Illegal table reference to non-collection type: 'a.key'\n" +
        "Path resolved to type: STRING");

    // Test that parent/collection joins without an ON clause analyze ok.
    for (JoinOperator joinOp: JoinOperator.values()) {
      if (joinOp.isNullAwareLeftAntiJoin() || joinOp.isIcebergDeleteJoin()) continue;
      AnalyzesOk(String.format(
          "select 1 from functional.allcomplextypes a %s a.int_array_col b", joinOp));
      AnalyzesOk(String.format(
          "select 1 from functional.allcomplextypes a %s a.struct_array_col b", joinOp));
      AnalyzesOk(String.format(
          "select 1 from functional.allcomplextypes a %s a.int_map_col b", joinOp));
      AnalyzesOk(String.format(
          "select 1 from functional.allcomplextypes a %s a.struct_map_col", joinOp));
    }

    AnalysisError("select pos from " +
        "(select int_array_col from functional.allcomplextypes) v",
        "Could not resolve column/field reference: 'pos'");
    AnalyzesOk("select pos from " +
        "(select int_array_col from functional.allcomplextypes) v, v.int_array_col");
  }

  @Test
  public void TestCatalogTableRefs() throws AnalysisException {
    String[] tables = new String[] { "alltypes", "alltypes_view" };
    String[] columns = new String[] { "int_col", "*" };
    testAllTableAliases(tables, columns);

    // Unqualified '*' is not ambiguous.
    AnalyzesOk("select * from functional.alltypes " +
        "cross join functional_parquet.alltypes");

    // Ambiguous unqualified column reference.
    AnalysisError("select int_col from functional.alltypes " +
        "cross join functional_parquet.alltypes",
        "Column/field reference is ambiguous: 'int_col'");
    // Ambiguous implicit unqualified table alias.
    AnalysisError("select alltypes.int_col from functional.alltypes " +
        "cross join functional_parquet.alltypes",
        "Unqualified table alias is ambiguous: 'alltypes'");
    AnalysisError("select alltypes.* from functional.alltypes " +
        "cross join functional_parquet.alltypes",
        "Unqualified table alias is ambiguous: 'alltypes'");

    // Mixing unqualified and fully-qualified table refs without explicit aliases is an
    // error because we'd expect a consistent result if we created a view of this stmt
    // (table names are fully qualified during view creation).
    AnalysisError("select alltypes.smallint_col, functional.alltypes.int_col " +
        "from alltypes inner join functional.alltypes " +
        "on (alltypes.id = functional.alltypes.id)",
        createAnalysisCtx("functional"),
        "Duplicate table alias: 'functional.alltypes'");
  }

  @Test
  public void TestTableSampleClause() {
    long bytesPercVals[] = new long[] { 0, 10, 50, 100 };
    long randomSeedVals[] = new long[] { 0, 10, 100, Integer.MAX_VALUE, Long.MAX_VALUE };
    for (long bytesPerc: bytesPercVals) {
      String tblSmpl = String.format("tablesample system (%s)", bytesPerc);
      AnalyzesOk("select * from functional.alltypes  " + tblSmpl);
      for (long randomSeed: randomSeedVals) {
        String repTblSmpl = String.format("%s repeatable (%s)", tblSmpl, randomSeed);
        AnalyzesOk("select * from functional.alltypes  " + repTblSmpl);
      }
    }

    // Invalid bytes percent. Negative values do not parse.
    AnalysisError("select * from functional.alltypes tablesample system (101)",
        "Invalid percent of bytes value '101'. " +
        "The percent of bytes to sample must be between 0 and 100.");
    AnalysisError("select * from functional.alltypes tablesample system (1000)",
        "Invalid percent of bytes value '1000'. " +
        "The percent of bytes to sample must be between 0 and 100.");

    // Only applicable to HDFS base table refs.
    AnalysisError("select * from functional_kudu.alltypes tablesample system (10)",
        "TABLESAMPLE is only supported on HDFS tables: functional_kudu.alltypes");
    AnalysisError("select * from functional_hbase.alltypes tablesample system (10)",
        "TABLESAMPLE is only supported on HDFS tables: functional_hbase.alltypes");
    AnalysisError("select * from functional.alltypes_datasource tablesample system (10)",
        "TABLESAMPLE is only supported on HDFS tables: " +
        "functional.alltypes_datasource");
    AnalysisError("select * from (select * from functional.alltypes) v " +
        "tablesample system (10)",
        "TABLESAMPLE is only supported on HDFS tables: v");
    AnalysisError("with v as (select * from functional.alltypes) " +
        "select * from v tablesample system (10)",
        "TABLESAMPLE is only supported on HDFS tables: v");
    AnalysisError("select * from functional.alltypes_view tablesample system (10)",
        "TABLESAMPLE is only supported on HDFS tables: functional.alltypes_view");
    AnalysisError("select * from functional.allcomplextypes.int_array_col " +
        "tablesample system (10)",
        "TABLESAMPLE is only supported on HDFS tables: " +
        "functional.allcomplextypes.int_array_col");
    AnalysisError("select * from functional.allcomplextypes a, a.int_array_col " +
        "tablesample system (10)",
        "TABLESAMPLE is only supported on HDFS tables: a.int_array_col");
  }

  /**
   * Helper function that returns a list of integers used to improve readability
   * in the path-related tests below.
   */
  private List<Integer> path(Integer... p) { return Lists.newArrayList(p); }

  /**
   * Checks that the given SQL analyzes ok, and asserts that the last result expr in the
   * parsed SelectStmt is a non-collection SlotRef whose absolute path is identical to
   * the given expected one. Also asserts that the slot's absolute path is equal to its
   * materialized path. Intentionally allows multiple result exprs to be analyzed to test
   * absolute path caching, though only the last path is validated.
   */
  private void testSlotRefPath(String sql, List<Integer> expectedAbsPath) {
    AnalysisContext ctx = createAnalysisCtx();
    SelectStmt stmt = (SelectStmt) AnalyzesOk(sql, ctx);
    Expr e = stmt.getResultExprs().get(stmt.getResultExprs().size() - 1);
    Preconditions.checkState(e instanceof SlotRef);
    Preconditions.checkState(!e.getType().isCollectionType());
    SlotRef slotRef = (SlotRef) e;
    List<Integer> actualAbsPath = slotRef.getDesc().getPath().getAbsolutePath();
    Assert.assertEquals("Mismatched absolute paths.", expectedAbsPath, actualAbsPath);
    List<Integer> actualMatPath = slotRef.getDesc().getMaterializedPath();
    Assert.assertEquals("Mismatched absolute/materialized paths.",
        actualAbsPath, actualMatPath);
  }

  /**
   * Checks that the given SQL analyzes ok, and asserts that all result exprs in the
   * parsed SelectStmt are SlotRefs and that the absolute path of result expr at
   * position i matches expectedAbsPaths[i]. Also asserts for all SlotRefs that the
   * materialized path of its SlotDescriptor is identical to its absolute path.
   */
  private void testStarPath(String sql, List<Integer>... expectedAbsPaths) {
    SelectStmt stmt = (SelectStmt) AnalyzesOk(sql);
    List<List<Integer>> actualAbsPaths = new ArrayList<>();
    for (int i = 0; i < stmt.getResultExprs().size(); ++i) {
      Expr e = stmt.getResultExprs().get(i);
      Preconditions.checkState(e instanceof SlotRef);
      SlotRef slotRef = (SlotRef) e;
      List<Integer> actualAbsPath = slotRef.getDesc().getPath().getAbsolutePath();
      List<Integer> actualMatPath = slotRef.getDesc().getMaterializedPath();
      Assert.assertEquals("Mismatched paths.", actualAbsPath, actualMatPath);
      actualAbsPaths.add(actualAbsPath);
    }
    List<List<Integer>> expectedPaths = Lists.newArrayList(expectedAbsPaths);
    Assert.assertEquals("Mismatched absolute paths.", expectedPaths, actualAbsPaths);
  }

  /**
   * Checks that the given SQL analyzes ok. Asserts that the last table ref in the
   * parsed SelectStmt has an absolute path identical to the given expected one, and
   * likewise for the materialized path. For non-relative table refs the materialized
   * path is expected to be null, otherwise the given materialized path must be identical
   * to the materialized path of the slot that materializes the referenced collection in
   * the parent tuple.
   */
  private void testTableRefPath(String sql, List<Integer> expectedAbsPath,
      List<Integer> expectedMatPath) {
    SelectStmt stmt = (SelectStmt) AnalyzesOk(sql);
    TableRef lastTblRef = stmt.getTableRefs().get(stmt.getTableRefs().size() - 1);
    // Check absolute path.
    List<Integer> actualAbsPath = lastTblRef.getDesc().getPath().getAbsolutePath();
    Assert.assertEquals("Mismatched absolute paths.", expectedAbsPath, actualAbsPath);
    // Check materialized path.
    if (!lastTblRef.isRelative()) {
      Assert.assertNull("There is no materialized path for non-relative table refs.\n" +
          "The expected materialized path should be null.", expectedMatPath);
      return;
    }
    CollectionTableRef collectionTblRef = (CollectionTableRef) lastTblRef;
    Expr collectionExpr = collectionTblRef.getCollectionExpr();
    Preconditions.checkState(collectionExpr instanceof SlotRef);
    SlotRef collectionSlotRef = (SlotRef) collectionExpr;
    SlotDescriptor collectionSlotDesc = collectionSlotRef.getDesc();
    Assert.assertEquals("Mismatched paths.",
        actualAbsPath, collectionSlotDesc.getPath().getAbsolutePath());
    Assert.assertEquals("Mismatched materialized paths.",
        expectedMatPath, collectionSlotDesc.getMaterializedPath());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void TestImplicitAndExplicitPaths() {
    // Check that there are no implicit field names for base tables.
    String[] implicitFieldNames = new String[] {Path.ARRAY_POS_FIELD_NAME,
        Path.ARRAY_ITEM_FIELD_NAME, Path.MAP_KEY_FIELD_NAME, Path.MAP_VALUE_FIELD_NAME};
    for (String field: implicitFieldNames) {
      AnalysisError(String.format("select %s from functional.alltypes", field),
          String.format("Could not resolve column/field reference: '%s'", field));
    }

    addTestDb("d", null);

    // Test array of scalars. Only explicit paths make sense.
    addTestTable("create table d.t1 (c array<int>)");
    testSlotRefPath("select item from d.t1.c", path(0, 0));
    testSlotRefPath("select pos from d.t1.c", path(0, 1));
    AnalysisError("select item.item from d.t1.c",
        "Could not resolve column/field reference: 'item.item'");
    AnalysisError("select item.pos from d.t1.c",
        "Could not resolve column/field reference: 'item.pos'");
    // Test star expansion.
    testStarPath("select * from d.t1.c", path(0, 0));
    testStarPath("select c.* from d.t1.c", path(0, 0));

    // Array of structs. No name conflicts with implicit fields. Both implicit and
    // explicit paths are allowed.
    addTestTable("create table d.t2 (c array<struct<f:int>>) stored as orc");
    testSlotRefPath("select f from d.t2.c", path(0, 0, 0));
    testSlotRefPath("select item.f from d.t2.c", path(0, 0, 0));
    testSlotRefPath("select pos from d.t2.c", path(0, 1));
    testSlotRefPath("select item from d.t2.c", path(0, 0));
    AnalysisError("select item.pos from d.t2.c",
        "Could not resolve column/field reference: 'item.pos'");
    // Test star expansion.
    testStarPath("select * from d.t2.c", path(0, 0, 0));
    testStarPath("select c.* from d.t2.c", path(0, 0, 0));

    // Array of structs with name conflicts. Both implicit and explicit
    // paths are allowed.
    addTestTable("create table d.t3 (c array<struct<f:int,item:int,pos:int>>) " +
        "stored as orc");
    testSlotRefPath("select f from d.t3.c", path(0, 0, 0));
    testSlotRefPath("select item.f from d.t3.c", path(0, 0, 0));
    testSlotRefPath("select item.item from d.t3.c", path(0, 0, 1));
    testSlotRefPath("select item.pos from d.t3.c", path(0, 0, 2));
    testSlotRefPath("select pos from d.t3.c", path(0, 1));
    testSlotRefPath("select item from d.t3.c", path(0, 0));
    // Test star expansion.
    testStarPath("select * from d.t3.c", path(0, 0, 0), path(0, 0, 1), path(0, 0, 2));
    testStarPath("select c.* from d.t3.c", path(0, 0, 0), path(0, 0, 1), path(0, 0, 2));

    // Map with a scalar key and value. Only implicit paths make sense.
    addTestTable("create table d.t4 (c map<int,string>)");
    testSlotRefPath("select key from d.t4.c", path(0, 0));
    testSlotRefPath("select value from d.t4.c", path(0, 1));
    AnalysisError("select value.value from d.t4.c",
        "Could not resolve column/field reference: 'value.value'");
    // Test star expansion.
    testStarPath("select * from d.t4.c", path(0, 0), path(0, 1));
    testStarPath("select c.* from d.t4.c", path(0, 0), path(0, 1));

    // Map with a scalar key and struct value. No name conflicts. Both implicit and
    // explicit paths are allowed.
    addTestTable("create table d.t5 (c map<int,struct<f:int>>) stored as orc");
    testSlotRefPath("select key from d.t5.c", path(0, 0));
    testSlotRefPath("select f from d.t5.c", path(0, 1, 0));
    testSlotRefPath("select value.f from d.t5.c", path(0, 1, 0));
    AnalysisError("select value.value from d.t5.c",
        "Could not resolve column/field reference: 'value.value'");
    testSlotRefPath("select value from d.t5.c", path(0, 1));
    // Test star expansion.
    testStarPath("select * from d.t5.c", path(0, 0), path(0, 1, 0));
    testStarPath("select c.* from d.t5.c", path(0, 0), path(0, 1, 0));

    // Map with a scalar key and struct value with name conflicts. Both implicit and
    // explicit paths are allowed.
    addTestTable("create table d.t6 (c map<int,struct<f:int,key:int,value:int>>) " +
        "stored as orc");
    testSlotRefPath("select key from d.t6.c", path(0, 0));
    testSlotRefPath("select f from d.t6.c", path(0, 1, 0));
    testSlotRefPath("select value.f from d.t6.c", path(0, 1, 0));
    testSlotRefPath("select value.key from d.t6.c", path(0, 1, 1));
    testSlotRefPath("select value.value from d.t6.c", path(0, 1, 2));
    testSlotRefPath("select value from d.t6.c", path(0, 1));
    // Test star expansion.
    testStarPath("select * from d.t6.c",
        path(0, 0), path(0, 1, 0), path(0, 1, 1), path(0, 1, 2));
    testStarPath("select c.* from d.t6.c",
        path(0, 0), path(0, 1, 0), path(0, 1, 1), path(0, 1, 2));

    // Map with nested struct value with name conflict. Both implicit and explicit paths
    // are allowed.
    addTestTable("create table d.t6_nested (c map<int," +
        "struct<f:int,key:int,value:int,s:struct<f:int,key:int,value:int>>>)" +
        " stored as orc");
    testSlotRefPath("select key from d.t6_nested.c", path(0,0));
    testSlotRefPath("select value from d.t6_nested.c", path(0,1));
    testSlotRefPath("select f from d.t6_nested.c", path(0, 1, 0));
    testSlotRefPath("select value.key from d.t6_nested.c", path(0, 1, 1));
    testSlotRefPath("select value.value from d.t6_nested.c", path(0, 1, 2));
    testSlotRefPath("select value.s from d.t6_nested.c", path(0, 1, 3));
    testSlotRefPath("select value.s.f from d.t6_nested.c", path(0, 1, 3, 0));
    testSlotRefPath("select value.s.key from d.t6_nested.c", path(0, 1, 3, 1));
    testSlotRefPath("select value.s.value from d.t6_nested.c", path(0, 1, 3, 2));


    // Test implicit/explicit paths on a complicated schema.
    addTestTable("create table d.t7 (" +
        "c1 int, " +
        "c2 decimal(10, 4), " +
        "c3 array<struct<a1:array<int>,a2:array<struct<x:int,y:int,a3:array<int>>>>>, " +
        "c4 bigint, " +
        "c5 map<int,struct<m1:map<int,string>," +
        "                  m2:map<int,struct<x:int,y:int,m3:map<int,int>>>>>) " +
        "stored as orc");

    // Test paths with c3.
    testTableRefPath("select 1 from d.t7.c3.a1", path(2, 0, 0), null);
    testTableRefPath("select 1 from d.t7.c3.item.a1", path(2, 0, 0), null);
    testSlotRefPath("select item from d.t7.c3.a1", path(2, 0, 0, 0));
    testSlotRefPath("select item from d.t7.c3.item.a1", path(2, 0, 0, 0));
    testTableRefPath("select 1 from d.t7.c3.a2", path(2, 0, 1), null);
    testTableRefPath("select 1 from d.t7.c3.item.a2", path(2, 0, 1), null);
    testSlotRefPath("select x from d.t7.c3.a2", path(2, 0, 1, 0, 0));
    testSlotRefPath("select x from d.t7.c3.item.a2", path(2, 0, 1, 0, 0));
    testTableRefPath("select 1 from d.t7.c3.a2.a3", path(2, 0, 1, 0, 2), null);
    testTableRefPath("select 1 from d.t7.c3.item.a2.item.a3", path(2, 0, 1, 0, 2), null);
    testSlotRefPath("select item from d.t7.c3.a2.a3", path(2, 0, 1, 0, 2, 0));
    testSlotRefPath("select item from d.t7.c3.item.a2.item.a3", path(2, 0, 1, 0, 2, 0));
    testSlotRefPath("select item from d.t7.c3", path(2, 0));
    // Test path assembly with multiple tuple descriptors.
    testTableRefPath("select 1 from d.t7, t7.c3, c3.a2, a2.a3",
        path(2, 0, 1, 0, 2), path(2, 0, 1, 0, 2));
    testTableRefPath("select 1 from d.t7, t7.c3, c3.item.a2, a2.item.a3",
        path(2, 0, 1, 0, 2), path(2, 0, 1, 0, 2));
    testSlotRefPath("select y from d.t7, t7.c3, c3.a2, a2.a3", path(2, 0, 1, 0, 1));
    testSlotRefPath("select y, x from d.t7, t7.c3, c3.a2, a2.a3", path(2, 0, 1, 0, 0));
    testSlotRefPath("select x, y from d.t7, t7.c3.item.a2, a2.a3", path(2, 0, 1, 0, 1));
    testSlotRefPath("select a1.item from d.t7, t7.c3, c3.a1, c3.a2, a2.a3",
        path(2, 0, 0, 0));
    // Test materialized path.
    testTableRefPath("select 1 from d.t7, t7.c3.a1", path(2, 0, 0), path(2));
    testTableRefPath("select 1 from d.t7, t7.c3.a2", path(2, 0, 1), path(2));
    testTableRefPath("select 1 from d.t7, t7.c3.a2.a3", path(2, 0, 1, 0, 2), path(2));
    testTableRefPath("select 1 from d.t7, t7.c3, c3.a2.a3",
        path(2, 0, 1, 0, 2), path(2, 0, 1));

    // Test paths with c5.
    testTableRefPath("select 1 from d.t7.c5.m1", path(4, 1, 0), null);
    testTableRefPath("select 1 from d.t7.c5.value.m1", path(4, 1, 0), null);
    testSlotRefPath("select key from d.t7.c5.m1", path(4, 1, 0, 0));
    testSlotRefPath("select key from d.t7.c5.value.m1", path(4, 1, 0, 0));
    testSlotRefPath("select value from d.t7.c5.m1", path(4, 1, 0, 1));
    testSlotRefPath("select value from d.t7.c5.value.m1", path(4, 1, 0, 1));
    testTableRefPath("select 1 from d.t7.c5.m2", path(4, 1, 1), null);
    testTableRefPath("select 1 from d.t7.c5.value.m2", path(4, 1, 1), null);
    testSlotRefPath("select key from d.t7.c5.m2", path(4, 1, 1, 0));
    testSlotRefPath("select key from d.t7.c5.value.m2", path(4, 1, 1, 0));
    testSlotRefPath("select x from d.t7.c5.m2", path(4, 1, 1, 1, 0));
    testSlotRefPath("select x from d.t7.c5.value.m2", path(4, 1, 1, 1, 0));
    testTableRefPath("select 1 from d.t7.c5.m2.m3", path(4, 1, 1, 1, 2), null);
    testTableRefPath("select 1 from d.t7.c5.value.m2.value.m3",
        path(4, 1, 1, 1, 2), null);
    testSlotRefPath("select key from d.t7.c5.m2.m3", path(4, 1, 1, 1, 2, 0));
    testSlotRefPath("select key from d.t7.c5.value.m2.value.m3", path(4, 1, 1, 1, 2, 0));
    testSlotRefPath("select value from d.t7.c5.m2.m3", path(4, 1, 1, 1, 2, 1));
    testSlotRefPath("select value from d.t7.c5.value.m2.value.m3",
        path(4, 1, 1, 1, 2, 1));
    // Test path assembly with multiple tuple descriptors.
    testTableRefPath("select 1 from d.t7, t7.c5, c5.m2, m2.m3",
        path(4, 1, 1, 1, 2), path(4, 1, 1, 1, 2));
    testTableRefPath("select 1 from d.t7, t7.c5, c5.value.m2, m2.value.m3",
        path(4, 1, 1, 1, 2), path(4, 1, 1, 1, 2));
    testSlotRefPath("select y from d.t7, t7.c5, c5.m2, m2.m3",
        path(4, 1, 1, 1, 1));
    testSlotRefPath("select y, x from d.t7, t7.c5, c5.m2, m2.m3",
        path(4, 1, 1, 1, 0));
    testSlotRefPath("select x, y from d.t7, t7.c5.value.m2, m2.m3",
        path(4, 1, 1, 1, 1));
    testSlotRefPath("select m1.key from d.t7, t7.c5, c5.m1, c5.m2, m2.m3",
        path(4, 1, 0, 0));
    // Test materialized path.
    testTableRefPath("select 1 from d.t7, t7.c5.m1", path(4, 1, 0), path(4));
    testTableRefPath("select 1 from d.t7, t7.c5.m2", path(4, 1, 1), path(4));
    testTableRefPath("select 1 from d.t7, t7.c5.m2.m3", path(4, 1, 1, 1, 2), path(4));
    testTableRefPath("select 1 from d.t7, t7.c5, c5.m2.m3",
        path(4, 1, 1, 1, 2), path(4, 1, 1));

    // Tests that implicit references are not allowed through collection types.
    addTestTable("create table d.t8 ("
        + "c1 array<map<string, string>>,"
        + "c2 map<string, array<struct<a:int>>>,"
        + "c3 struct<s1:struct<a:array<array<struct<e:int, f:string>>>>>)");
    testImplicitPathFailure("d.t8", true, "c1", "key", "value");
    testImplicitPathFailure("d.t8", true, "c2", "pos");
    testImplicitPathFailure("d.t8.c3.s1", false, "a", "f");
  }

  private void testImplicitPathFailure(String parent, boolean parentIsCollection,
      String collection, String...fields) {
    String[] parentElements = parent.split("\\.");
    String implicitAlias = parentElements[parentElements.length - 1];
    String explicitAlias = "x";
    for (String field : fields) {
      // Tests that the path in the select list item does not resolve.
      AnalysisError(String.format("select %s from %s.%s", field, parent, collection),
          String.format("Could not resolve column/field reference: '%s'", field));
      AnalysisError(String.format("select %s.%s from %s.%s %s",
          explicitAlias, field, parent, collection, explicitAlias),
          String.format("Could not resolve column/field reference: '%s.%s'",
          explicitAlias, field));
      if (parentIsCollection) {
        AnalysisError(String.format("select %s from %s join %s.%s",
            field, parent, implicitAlias, collection),
            String.format("Could not resolve column/field reference: '%s'", field));
        AnalysisError(String.format("select %s.%s from %s %s join %s.%s",
            explicitAlias, field, parent, explicitAlias, explicitAlias, collection),
            String.format("Could not resolve column/field reference: '%s.%s'",
            explicitAlias, field));
      }
      // Tests that the path in the last table reference does not resolve.
      AnalysisError(String.format("select 1 from %s.%s join %s.%s",
          parent, collection, collection, field),
          String.format("Could not resolve table reference: '%s.%s'",
          collection, field));
      AnalysisError(String.format("select 1 from %s.%s %s join %s.%s",
          parent, collection, explicitAlias, explicitAlias, field),
          String.format("Could not resolve table reference: '%s.%s'",
          explicitAlias, field));
    }
  }

  @Test
  public void TestStructFields() throws AnalysisException {
    String[] tables = new String[] { "allcomplextypes" };
    String[] columns = new String[] { "id", "int_struct_col.f1",
        "nested_struct_col.f2.f12.f21" };
    testAllTableAliases(tables, columns);

    // Unknown struct fields.
    AnalysisError("select nested_struct_col.badfield from functional.allcomplextypes",
        "Could not resolve column/field reference: 'nested_struct_col.badfield'");
    AnalysisError("select nested_struct_col.f2.badfield from functional.allcomplextypes",
        "Could not resolve column/field reference: 'nested_struct_col.f2.badfield'");
    AnalysisError("select nested_struct_col.badfield.f2 from functional.allcomplextypes",
        "Could not resolve column/field reference: 'nested_struct_col.badfield.f2'");

    // Illegal intermediate reference to collection type.
    AnalysisError("select int_array_col.item from functional.allcomplextypes",
        "Illegal column/field reference 'int_array_col.item' with intermediate " +
        "collection 'int_array_col' of type 'ARRAY<INT>'");
    AnalysisError("select struct_array_col.f1 from functional.allcomplextypes",
        "Illegal column/field reference 'struct_array_col.f1' with intermediate " +
        "collection 'struct_array_col' of type 'ARRAY<STRUCT<f1:BIGINT,f2:STRING>>'");
    AnalysisError("select int_map_col.key from functional.allcomplextypes",
        "Illegal column/field reference 'int_map_col.key' with intermediate " +
        "collection 'int_map_col' of type 'MAP<STRING,INT>'");
    AnalysisError("select struct_map_col.f1 from functional.allcomplextypes",
        "Illegal column/field reference 'struct_map_col.f1' with intermediate " +
        "collection 'struct_map_col' of type 'MAP<STRING,STRUCT<f1:BIGINT,f2:STRING>>'");
    AnalysisError(
        "select complex_nested_struct_col.f2.f11 from functional.allcomplextypes",
        "Illegal column/field reference 'complex_nested_struct_col.f2.f11' with " +
        "intermediate collection 'f2' of type " +
        "'ARRAY<STRUCT<f11:BIGINT,f12:MAP<STRING,STRUCT<f21:BIGINT>>>>'");
    AnalysisError(
        "select complex_nested_struct_col.f2.f11 from functional.allcomplextypes",
        "Illegal column/field reference 'complex_nested_struct_col.f2.f11' with " +
        "intermediate collection 'f2' of type " +
        "'ARRAY<STRUCT<f11:BIGINT,f12:MAP<STRING,STRUCT<f21:BIGINT>>>>'");

    // Check the support of struct in the select list for different file formats.
    AnalysisContext ctx = createAnalysisCtx();
    AnalysisError("select int_struct_col from functional.allcomplextypes", ctx,
        "Querying STRUCT is only supported for ORC and Parquet file formats.");
    AnalyzesOk("select alltypes from functional_orc_def.complextypes_structs", ctx);

    // Check that a struct in the select list doesn't raise an error if it contains
    // collections.
    addTestTable(
        "create table nested_structs (s1 struct<s2:struct<i:int>>) stored as orc");
    addTestTable("create table nested_structs_with_list " +
        "(s1 struct<s2:struct<a:array<int>>>) stored as orc");
    AnalyzesOk("select s1 from nested_structs", ctx);
    AnalyzesOk("select s1.s2 from nested_structs", ctx);
    AnalyzesOk("select s1 from nested_structs_with_list", ctx);
    AnalyzesOk("select s1.s2 from nested_structs_with_list", ctx);
  }

  @Test
  public void TestSlotRefPathAmbiguity() {
    addTestDb("a", null);
    addTestTable("create table a.a (a struct<a:struct<a:int>>) stored as orc");

    // Slot path is not ambiguous.
    AnalyzesOk("select a.a.a.a.a from a.a");
    AnalyzesOk("select t.a.a.a from a.a t");

    // Slot path is not ambiguous and resolves to a struct.
    AnalysisContext ctx = createAnalysisCtx();
    AnalyzesOk("select a from a.a", ctx);
    AnalyzesOk("select t.a from a.a t", ctx);
    AnalyzesOk("select t.a.a from a.a t", ctx);

    // Slot paths are ambiguous. A slot path can legally resolve to a non-scalar type,
    // even though we currently do not support non-scalar SlotRefs in the select list
    // or in any exprs.
    AnalysisError("select a.a from a.a",
        "Column/field reference is ambiguous: 'a.a'");
    AnalysisError("select a.a.a from a.a",
        "Column/field reference is ambiguous: 'a.a.a'");
    AnalysisError("select a.a.a.a from a.a",
        "Column/field reference is ambiguous: 'a.a.a.a'");

    // Cannot resolve slot paths.
    AnalysisError("select a.a.a.a.a.a from a.a",
        "Could not resolve column/field reference: 'a.a.a.a.a.a'");
    AnalysisError("select t.a.a.a.a from a.a t",
        "Could not resolve column/field reference: 't.a.a.a.a'");

    // Paths resolve to an existing implicit table alias
    // (the unqualified path resolution would be illegal).
    addTestTable("create table a.array_test (a array<int>)");
    addTestTable("create table a.map_test (a map<int, int>)");
    AnalyzesOk("select a.item from a.array_test t, t.a");
    AnalyzesOk("select a.key, a.value from a.map_test t, t.a");
  }

  @Test
  public void TestStarPathAmbiguity() {
    addTestDb("a", null);
    addTestTable("create table a.a (a struct<a:struct<a:int>>)");

    // Star path is not ambiguous.
    AnalyzesOk("select a.a.a.a.* from a.a");
    AnalyzesOk("select t.a.a.* from a.a t");

    // Not ambiguous, but illegal.
    AnalysisError("select a.a.a.a.a.* from a.a",
        "Cannot expand star in 'a.a.a.a.a.*' because path 'a.a.a.a.a' " +
        "resolved to type 'INT'.");
    AnalysisError("select t.a.a.a.* from a.a t",
        "Cannot expand star in 't.a.a.a.*' because path 't.a.a.a' " +
        "resolved to type 'INT'.");
    // Not ambiguous, but expands to an empty select list.
    AnalysisError("select t.* from a.a t",
        "The star exprs expanded to an empty select list because the referenced " +
        "tables only have complex-typed columns.");

    // Star paths are ambiguous.
    AnalysisError("select a.* from a.a",
        "Star expression is ambiguous: 'a.*'");
    AnalysisError("select a.a.* from a.a",
        "Star expression is ambiguous: 'a.a.*'");
    AnalysisError("select a.a.a.* from a.a",
        "Star expression is ambiguous: 'a.a.a.*'");

    // Cannot resolve star paths.
    AnalysisError("select a.a.a.a.a.a.* from a.a",
        "Could not resolve star expression: 'a.a.a.a.a.a.*'");
    AnalysisError("select t.a.a.a.a.* from a.a t",
        "Could not resolve star expression: 't.a.a.a.a.*'");

    // Paths resolve to an existing implicit table alias
    // (the unqualified path resolution would be illegal).
    addTestTable("create table a.array_test (a array<int>)");
    addTestTable("create table a.map_test (a map<int, int>)");
    AnalyzesOk("select a.* from a.array_test t, t.a");
    AnalyzesOk("select a.* from a.map_test t, t.a");
  }

  @Test
  public void TestTableRefPathAmbiguity() {
    addTestDb("a", null);
    addTestTable("create table a.a (a array<struct<a:array<int>>>)");

    // Table paths are not ambiguous.
    AnalyzesOk("select 1 from a.a");
    AnalyzesOk("select 1 from a.a.a");
    AnalyzesOk("select 1 from a.a.a.a");
    AnalyzesOk("select 1 from a", createAnalysisCtx("a"));
    AnalyzesOk("select 1 from a.a.a.a", createAnalysisCtx("a"));

    // Table paths are ambiguous.
    AnalysisError("select 1 from a.a", createAnalysisCtx("a"),
        "Table reference is ambiguous: 'a.a'");
    AnalysisError("select 1 from a.a.a", createAnalysisCtx("a"),
        "Table reference is ambiguous: 'a.a.a'");

    // Ambiguous reference to registered table aliases.
    addTestTable("create table a.t1 (x array<struct<y:array<int>>>)");
    addTestTable("create table a.t2 (y array<int>)");
    AnalysisError("select 1 from a.t1 a, a.t2 `a.x`, a.x.y",
        "Table reference is ambiguous: 'a.x.y'");
  }

  @Test
  public void TestFromClause() throws AnalysisException {
    AnalyzesOk("select int_col from functional.alltypes");
    AnalysisError("select int_col from badtbl",
        "Could not resolve table reference: 'badtbl'");

    // case-insensitive
    AnalyzesOk("SELECT INT_COL FROM FUNCTIONAL.ALLTYPES");
    AnalyzesOk("SELECT INT_COL FROM functional.alltypes");
    AnalyzesOk("SELECT INT_COL FROM functional.aLLTYPES");
    AnalyzesOk("SELECT INT_COL FROM Functional.ALLTYPES");
    AnalyzesOk("SELECT INT_COL FROM FUNCTIONAL.ALLtypes");
    AnalyzesOk("SELECT INT_COL FROM FUNCTIONAL.alltypes");
    AnalyzesOk("select functional.AllTypes.Int_Col from functional.alltypes");
  }

  @Test
  public void TestNoFromClause() throws AnalysisException {
    AnalyzesOk("select 'test'");
    AnalyzesOk("select 1 + 1, -128, 'two', 1.28");
    AnalyzesOk("select -1, 1 - 1, 10 - -1, 1 - - - 1");
    AnalyzesOk("select -1.0, 1.0 - 1.0, 10.0 - -1.0, 1.0 - - - 1.0");
    AnalysisError("select a + 1", "Could not resolve column/field reference: 'a'");
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
    // expand '*' on a struct-typed column
    AnalyzesOk("select int_struct_col.* from functional.allcomplextypes");
    AnalyzesOk("select a.int_struct_col.* from functional.allcomplextypes a");
    AnalyzesOk("select allcomplextypes.int_struct_col.* from functional.allcomplextypes");
    AnalyzesOk("select functional.allcomplextypes.int_struct_col.* " +
        "from functional.allcomplextypes");

    // '*' without from clause has no meaning.
    AnalysisError("select *", "'*' expression in select list requires FROM clause.");
    AnalysisError("select 1, *, 2+4",
        "'*' expression in select list requires FROM clause.");
    AnalysisError("select a.*", "Could not resolve star expression: 'a.*'");

    // invalid star expansions
    AnalysisError("select functional.* from functional.alltypes",
        "Could not resolve star expression: 'functional.*'");
    AnalysisError("select int_col.* from functional.alltypes",
        "Cannot expand star in 'int_col.*' because " +
        "path 'int_col' resolved to type 'INT'.\n" +
        "Star expansion is only valid for paths to a struct type.");
    AnalysisError("select complex_struct_col.f2.* from functional.allcomplextypes",
        "Cannot expand star in 'complex_struct_col.f2.*' because " +
        "path 'complex_struct_col.f2' resolved to type 'ARRAY<INT>'.\n" +
        "Star expansion is only valid for paths to a struct type.");

    for (String joinType: new String[] { "left semi join", "left anti join" }) {
      // ignore semi-/anti-joined tables in unqualified '*' expansion
      SelectStmt stmt = (SelectStmt) AnalyzesOk(String.format(
          "select * from functional.alltypes a " +
          "%s functional.testtbl b on (a.id = b.id)", joinType));
      // expect to have as many result exprs as alltypes has columns
      assertEquals(13, stmt.getResultExprs().size());

      // cannot expand '*" for a semi-/anti-joined table
      AnalysisError(String.format("select a.*, b.* from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)", joinType),
          "Illegal star expression 'b.*' of semi-/anti-joined table 'b'");
    }
    for (String joinType: new String[] { "right semi join", "right anti join" }) {
      // ignore semi-/anti-joined tables in unqualified '*' expansion
      SelectStmt stmt = (SelectStmt) AnalyzesOk(String.format(
          "select * from functional.alltypes a " +
          "%s functional.testtbl b on (a.id = b.id)", joinType));
      // expect to have as many result exprs as testtbl has columns
      assertEquals(3, stmt.getResultExprs().size());

      // cannot expand '*" for a semi-/anti-joined table
      AnalysisError(String.format("select a.*, b.* from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)", joinType),
          "Illegal star expression 'a.*' of semi-/anti-joined table 'a'");
    }
  }

  /**
   * Test that complex types are supported in the select list.
   */
  @Test
  public void TestComplexTypesInSelectList() {
    // Star only expands to the scalar-typed fields.
    AnalyzesOk("select * from functional.allcomplextypes " +
        "cross join functional_parquet.alltypes");
    AnalyzesOk("select complex_struct_col.* from functional.allcomplextypes");
    // The result exprs could be empty after star expansion.
    addTestTable("create table only_complex_types " +
        "(a array<int>, b struct<x:int, y:int>, c map<string, int>)");
    AnalysisError("select * from only_complex_types",
        "The star exprs expanded to an empty select list because the referenced " +
        "tables only have complex-typed columns.");
    AnalysisError("select a.* from only_complex_types a, " +
        "functional.allcomplextypes b",
        "The star exprs expanded to an empty select list because the referenced " +
        "tables only have complex-typed columns.");
    // Empty star expansion, but non empty result exprs.
    AnalyzesOk("select 1, * from only_complex_types");

    // Struct in select list.
    AnalysisContext ctx = createAnalysisCtx();
    AnalyzesOk("select alltypes from functional_orc_def.complextypes_structs", ctx);
    AnalyzesOk("select int_array_col from functional.allcomplextypes");
    AnalyzesOk("select int_array_col from functional.allcomplextypes " +
        "union all select int_array_col from functional.allcomplextypes");
    AnalysisError("select int_array_col, item from functional.allcomplextypes", ctx,
        "Could not resolve column/field reference: 'item'");
    AnalysisError("select int_array_col, int_array_col.item " +
        "from functional.allcomplextypes", ctx,
        "Illegal column/field reference 'int_array_col.item' with intermediate " +
        "collection 'int_array_col' of type 'ARRAY<INT>'");
    AnalysisError("select tiny_struct from functional_orc_def.complextypes_structs " +
        "union all select tiny_struct from functional_orc_def.complextypes_structs", ctx,
        "Set operations don't support STRUCT types or types containing STRUCT types." +
        " STRUCT<b:BOOLEAN> in tiny_struct");
    AnalysisError("select all_mix from functional_parquet.collection_struct_mix " +
        "union all select all_mix from functional_parquet.collection_struct_mix", ctx,
        "Set operations don't support STRUCT types or types containing STRUCT types. " +
        "MAP<INT,STRUCT<big:STRUCT<arr:ARRAY<STRUCT<inner_arr:ARRAY<ARRAY<INT>>," +
        "m:TIMESTAMP>>,n:INT>,small:STRUCT<str:STRING,i:INT>>> in all_mix.");
    AnalyzesOk("select 1 from " +
        "(select int_array_col from functional.allcomplextypes) v");
    AnalyzesOk("select int_array_col from " +
        "(select int_array_col from functional.allcomplextypes) v");
    // Structs are allowed in an inline view.
    AnalyzesOk("select v.ts from (select tiny_struct as ts from " +
        "functional_orc_def.complextypes_structs) v;", ctx);
    // Illegal complex-typed expr in an insert.
    AnalysisError("insert into functional.allcomplextypes " +
        "select int_array_col from functional.allcomplextypes",
        "Unable to INSERT into target table (functional.allcomplextypes) because " +
        "the column 'int_array_col' has a complex type 'ARRAY<INT>' and Impala " +
        "doesn't support inserting into tables containing complex type columns");
    // Illegal complex-typed expr in a CTAS.
    AnalysisError("create table new_tbl as " +
        "select int_array_col from functional.allcomplextypes",
        "Unable to INSERT into target table (default.new_tbl) because the column " +
        "'int_array_col' has a complex type 'ARRAY<INT>' and Impala doesn't support " +
        "inserting into tables containing complex type columns");
    AnalysisError("create table new_tbl as " +
        "select tiny_struct from functional_orc_def.complextypes_structs", ctx,
        "Unable to INSERT into target table (default.new_tbl) because the column " +
            "'tiny_struct' has a complex type 'STRUCT<b:BOOLEAN>' and Impala doesn't " +
            "support inserting into tables containing complex type columns");
    // Binary in complex types is also supported.
    AnalyzesOk("select binary_item_col from functional_parquet.binary_in_complex_types");
    AnalyzesOk(
        "select binary_member_col from functional_parquet.binary_in_complex_types");
    AnalyzesOk("select binary_key_col from functional_parquet.binary_in_complex_types");
    AnalyzesOk("select binary_value_col from functional_parquet.binary_in_complex_types");

    //Make complex types available in star queries
    ctx.getQueryOptions().setExpand_complex_types(true);

    AnalyzesOk("select * from functional_parquet.complextypes_structs",ctx);
    AnalyzesOk("select * from functional_parquet.complextypes_nested_structs",ctx);
    AnalyzesOk("select * from functional_parquet.complextypes_maps_view",ctx);

    AnalyzesOk("select outer_struct.str, outer_struct.* from " +
            "functional_parquet.complextypes_nested_structs",ctx);
    AnalyzesOk("select * from (select * from " +
            "functional_parquet.complextypes_nested_structs) v",ctx);
    AnalyzesOk("select * from (select int_map, int_map_array from " +
            "functional_parquet.complextypestbl) v",ctx);

    AnalyzesOk("select * from functional_parquet.complextypes_arrays",ctx);
    AnalyzesOk("select * from " +
            "functional_parquet.complextypes_arrays_only_view",ctx);
    AnalyzesOk("select v.id, v.* from " +
            "(select * from functional_parquet.complextypes_arrays) v",ctx);

    // Allow also structs in collections and vice versa.
    AnalyzesOk("select * from functional_parquet.allcomplextypes", ctx);
    AnalyzesOk("select * from functional_orc_def.complextypestbl", ctx);
    AnalyzesOk("select * from functional_parquet.binary_in_complex_types", ctx);
  }

  @Test
  public void TestGroupByAliases() throws AnalysisException {
    // IMPALA-5191: GROUP BY, aliases must only be
    // substituted at the top level
    AnalyzesOk("select int_col / 2 as x from functional.alltypes group by x");
    // GROUP BY contains alias in subexpression
    AnalysisError(
        "select int_col / 2 as x from functional.alltypes group by x / 2",
        "Could not resolve column/field reference: 'x'");
    // Alias referring to aggregation output in GROUP BY
    AnalysisError("select count(*) a from functional.alltypes group by a",
        "GROUP BY expression must not contain aggregate functions: a");
    // Alias referring to predicate in GROUP BY
    AnalysisError("select count(*) > 10 a from functional.alltypes group by a",
        "GROUP BY expression must not contain aggregate functions: a");
    // Alias referring to analytic output in GROUP BY
    AnalysisError("select sum(id) over(order by id) a " +
        "from functional.alltypes group by a",
        "GROUP BY expression must not contain analytic expressions: " +
        "sum(id) OVER (ORDER BY id ASC)");
    // Alias referring to analytic output in GROUP BY
    AnalyzesOk("with w_test as (select '1' as `one`, 2 as two, '3' as three) " +
        "select `one` as `one`, substring(cast(two as string), 1, 1) as two, " +
        "three as three, count(1) as cnt " +
        "from w_test " +
        "group by `one`, substring(cast(two as string), 1, 1), three");
  }

  @Test
  public void TestGroupByOrdinals() throws AnalysisException {
    AnalysisError("select * from functional.alltypes group by 1",
        "cannot combine '*' in select list with grouping or aggregation");
    // IMPALA-5191: ORDER BY, ordinals must only be
    // substituted at the top level
    AnalyzesOk("select int_col / 2 as x from functional.alltypes group by 1");
    // Ordinal referring to aggregation output in GROUP BY
    AnalysisError("select count(*) from functional.alltypes group by 1",
        "GROUP BY expression must not contain aggregate functions: 1");
    // Ordinal referring to predicate in GROUP BY
    AnalysisError("select count(*) > 10 from functional.alltypes group by 1",
        "GROUP BY expression must not contain aggregate functions: 1");
    // Ordinal referring to analytic output in GROUP BY
    AnalysisError("select sum(id) over(order by id) " +
        "from functional.alltypes group by 1",
        "GROUP BY expression must not contain analytic expressions: " +
        "sum(id) OVER (ORDER BY id ASC)");
    // Constant exprs should not be interpreted as ordinals
    AnalyzesOk("select int_col, count(*) from functional.alltypes group by 1, 1 * 2");
    AnalyzesOk("select int_col, count(*) from functional.alltypes group by 1, " +
        "if(true, 2, int_col)");
  }

  @Test
  public void TestOrderByAliases() throws AnalysisException {
    // IMPALA-5191: ORDER BY, aliases must only be
    // substituted at the top level
    AnalyzesOk("select int_col / 2 as x from functional.alltypes order by x");
    // ORDER BY contains alias in subexpression
    AnalysisError(
        "select int_col / 2 as x from functional.alltypes order by -x",
        "Could not resolve column/field reference: 'x'");
    // Alias referring to aggregation output in ORDER BY
    AnalyzesOk("select count(*) a from functional.alltypes order by a");
    // Alias referring to predicate in ORDER BY
    AnalyzesOk("select count(*) > 10 a from functional.alltypes order by a");
    // Alias referring to analytic output in ORDER BY
    AnalyzesOk("select sum(id) over(order by id) a from functional.alltypes order by a");
  }

  @Test
  public void TestOrderByOrdinals() throws AnalysisException {
    AnalysisError("select * from functional.alltypes order by 14",
        "ORDER BY: ordinal exceeds the number of items in the SELECT list: 14");
    AnalyzesOk("select t.* from functional.alltypes t order by 1");
    AnalyzesOk("select t2.* from functional.alltypes t1, " +
        "functional.alltypes t2 order by 1");
    AnalyzesOk("select * from (select max(id) from functional.testtbl) t1 order by 1");
    AnalysisError("select * from (select max(id) from functional.testtbl) t1 order by 2",
        "ORDER BY: ordinal exceeds the number of items in the SELECT list: 2");
    // IMPALA-5191: ORDER BY, ordinals must only be
    // substituted at the top level
    AnalyzesOk("select int_col / 2 as x from functional.alltypes order by 1");
    // Ordinal referring to aggregation output in ORDER BY
    AnalyzesOk("select count(*) from functional.alltypes order by 1");
    // Ordinal referring to predicate in ORDER BY
    AnalyzesOk("select count(*) > 10 from functional.alltypes order by 1");
    // Ordinal referring to analytic output in ORDER BY
    AnalyzesOk("select sum(id) over(order by id) from functional.alltypes order by 1");
    // Constant exprs should not be interpreted as ordinals
    AnalyzesOk("select int_col, bigint_col from functional.alltypes order by 1 + 4");
    AnalyzesOk("select int_col, bigint_col from functional.alltypes order by " +
        "if(true, 7, int_col)");
  }

  @Test
  public void TestHavingAliases() throws AnalysisException {
    // Ambiguous alias in HAVING
    AnalysisError("select not bool_col m, min(smallint_col) m, max(bigint_col) m "
        + "from functional.alltypes group by bool_col having m",
        "HAVING: ambiguous alias: 'm'");
    // IMPALA-5191: HAVING, aliases must only be
    // substituted at the top level
    AnalyzesOk("select not bool_col as nb from functional.alltypes having nb");
    // HAVING contains alias in subexpression
    AnalysisError(
        "select int_col / 2 as x from functional.alltypes having x > 0",
        "Could not resolve column/field reference: 'x'");
    // Alias referring to aggregation output in HAVING
    AnalysisError("select count(*) a from functional.alltypes having a",
        "HAVING clause 'count(*)' requires return type 'BOOLEAN'. " +
        "Actual type is 'BIGINT'.");
    // Alias referring to predicate in HAVING
    AnalyzesOk("select count(*) > 10 a from functional.alltypes having a");
    // Alias referring to analytic output in HAVING
    AnalysisError("select sum(id) over(order by id) a from functional.alltypes having a",
        "HAVING clause must not contain analytic expressions: " +
        "sum(id) OVER (ORDER BY id ASC)");
  }

  // IMPALA-7844: Ordinals not supported in HAVING since
  // HAVING is an expression, not a list like GROUP BY or ORDER BY.
  // Verify that an integer is treated as a constant, not an ordinal.
  @Test
  public void TestHavingIntegers() {
    try {
      // Legacy 3.x functionality
      BackendConfig.INSTANCE.setAllowOrdinalsInHaving(true);
      AnalyzesOk("select not bool_col as nb from functional.alltypes having 1");
      AnalysisError("select count(*) from functional.alltypes having 1",
          "HAVING clause 'count(*)' requires return type 'BOOLEAN'. " +
          "Actual type is 'BIGINT'.");
      AnalyzesOk("select count(*) > 10 from functional.alltypes having 1");
      AnalysisError("select sum(id) over(order by id) from functional.alltypes having 1",
          "HAVING clause must not contain analytic expressions: " +
          "sum(id) OVER (ORDER BY id ASC)");
      AnalysisError("select sum(id) over(order by id) from functional.alltypes having -1",
          "HAVING: ordinal must be >= 1: -1");
      // Forward-looking, post 3.x functionality
      // IMPALA-7844: Ordinals not supported in HAVING since
      // HAVING is an expression, not a list like GROUP BY or ORDER BY.
      BackendConfig.INSTANCE.setAllowOrdinalsInHaving(false);
      AnalysisError("select not bool_col as nb from functional.alltypes having 1",
          "HAVING clause '1' requires return type 'BOOLEAN'. " +
          "Actual type is 'TINYINT'.");
      AnalysisError("select not bool_col as nb from functional.alltypes having -1",
          "HAVING clause '-1' requires return type 'BOOLEAN'. " +
          "Actual type is 'TINYINT'.");
      // Constant exprs should not be interpreted as ordinals
      AnalysisError("select int_col, bool_col, count(*) from functional.alltypes " +
          "group by 1, 2 having 1 + 1",
          "HAVING clause '1 + 1' requires return type 'BOOLEAN'. " +
          "Actual type is 'SMALLINT'.");
      AnalysisError("select int_col, bool_col, count(*) from functional.alltypes " +
          "group by 1, 2 having if(TRUE, 2, int_col)",
          "HAVING clause 'if(TRUE, 2, int_col)' requires return type 'BOOLEAN'. " +
          "Actual type is 'INT'.");
    } finally {
      BackendConfig.INSTANCE.setAllowOrdinalsInHaving(false);
    }
  }

  @Test
  public void TestInlineView() throws AnalysisException {
    AnalyzesOk("select y x from (select id y from functional_hbase.alltypessmall) a");
    AnalyzesOk("select id from (select id from functional_hbase.alltypessmall) a");
    AnalyzesOk("select * from (select id+2 from functional_hbase.alltypessmall) a");
    AnalyzesOk("select t1 c from " +
        "(select c t1 from (select id c from functional_hbase.alltypessmall) t1) a");
    AnalysisError("select id from (select id+2 from functional_hbase.alltypessmall) a",
        "Could not resolve column/field reference: 'id'");
    AnalyzesOk("select a.* from (select id+2 from functional_hbase.alltypessmall) a");

    // join test
    AnalyzesOk("select * from (select id+2 id from functional_hbase.alltypessmall) a " +
        "join (select * from functional.AllTypes where true) b");
    AnalyzesOk("select a.x from (select count(id) x from functional.AllTypes) a");
    AnalyzesOk("select a.* from (select count(id) from functional.AllTypes) a");
    AnalysisError("select a.id from (select id y from functional_hbase.alltypessmall) a",
        "Could not resolve column/field reference: 'a.id'");
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
        "(select rank() over(order by string_col) from functional.alltypes " +
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
        "Could not resolve column/field reference: 'tinyint_col'");

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
        "GROUP BY: ordinal exceeds the number of items in the SELECT list");
    AnalysisError("select * from " +
        "(select * from functional.alltypes group by 1) x",
        "cannot combine '*' in select list with grouping or aggregation");
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
    AnalyzesOk("select * from (select zip, count(*) " +
        "       from (select * from functional.testtbl) x " +
        "       group by 1 order by count(*) + min(zip)) x");
    AnalysisError("select * from (select zip, count(*) " +
        "       from (select * from functional.testtbl) x " +
        "       group by 1 offset 5) x",
        "OFFSET requires an ORDER BY clause: OFFSET 5");
    AnalysisError("select * from (select zip, count(*) " +
        "       from (select * from functional.testtbl) x " +
        "       group by 1 order by count(*) + min(zip) offset 5) x",
        "Order-by with offset without limit not supported in nested queries");
    AnalyzesOk("select c1, c2 from (select zip c1 , count(*) c2 " +
        "                     from (select * from functional.testtbl) x group by 1) x " +
        "        order by 2, 1 limit 5");
    AnalyzesOk("select c1, c2 from (select zip c1 , count(*) c2 " +
        "                     from (select * from functional.testtbl) x group by 1) x " +
        "        order by 2, 1");
    AnalyzesOk("select c1, c2 from (select zip c1 , count(*) c2 " +
        "                     from (select * from functional.testtbl) x group by 1) x " +
        "        order by 2, 1 offset 5");

    // test NULLs
    AnalyzesOk("select * from (select NULL) a");

    // test that auto-generated columns are not used by default
    AnalyzesOk("select `int_col * 1`, a, int_col, `NOT bool_col` from " +
        "(select int_col * 1, int_col as a, int_col, !bool_col, concat(string_col) " +
        "from functional.alltypes) t");
    // test auto-generated column labels by enforcing their use in inline views
    AnalyzesOk("select _c0, a, int_col, _c3 from " +
        "(select int_col * 1, int_col as a, int_col, !bool_col, concat(string_col) " +
        "from functional.alltypes) t", createAnalysisCtxUsingHiveColLabels());
    // test auto-generated column labels in group by and order by
    AnalyzesOk("select _c0, count(a), count(int_col), _c3 from " +
        "(select int_col * 1, int_col as a, int_col, !bool_col, concat(string_col) " +
        "from functional.alltypes) t group by _c0, _c3 order by _c0 limit 10",
        createAnalysisCtxUsingHiveColLabels());
    // test auto-generated column labels in multiple scopes
    AnalyzesOk("select x.front, x._c1, x._c2 from " +
        "(select y.back as front, y._c0 * 10, y._c2 + 2 from " +
        "(select int_col * 10, int_col as back, int_col + 2 from " +
        "functional.alltypestiny) y) x",
        createAnalysisCtxUsingHiveColLabels());
    // IMPALA-3537: Test that auto-generated column labels are only applied in
    // the appropriate child query blocks.
    SelectStmt colLabelsStmt =
        (SelectStmt) AnalyzesOk("select avg(int_col) from functional.alltypes_view");
    assertEquals("avg(int_col)", colLabelsStmt.getColLabels().get(0));
    AnalyzesOk("select `max(int_col)` from " +
        "(select max(int_col) from functional.alltypes_view) v");

    // ambiguous reference to an auto-generated column
    AnalysisError("select _c0 from " +
        "(select int_col * 2, id from functional.alltypes) a inner join " +
        "(select int_col + 6, id from functional.alltypes) b " +
        "on (a.id = b.id)",
        createAnalysisCtxUsingHiveColLabels(),
        "Column/field reference is ambiguous: '_c0'");
    // auto-generated column doesn't exist
    AnalysisError("select _c0, a, _c2, _c3 from " +
        "(select int_col * 1, int_col as a, int_col, !bool_col, concat(string_col) " +
        "from functional.alltypes) t",
        createAnalysisCtxUsingHiveColLabels(),
        "Could not resolve column/field reference: '_c2'");

    // Regression test for IMPALA-984.
    AnalyzesOk("SELECT 1 " +
        "FROM functional.decimal_tbl AS t1 LEFT JOIN " +
          "(SELECT SUM(t1.d2) - SUM(t1.d3) as double_col_3, " +
           "SUM(t1.d2) IS NULL " +
          "FROM functional.decimal_tbl AS t1) AS t3 " +
        "ON t3.double_col_3 = t1.d3");

    // Test that InlineViewRef.makeOutputNullable() preserves expr signatures when
    // substituting NULL literals for SlotRefs (IMPALA-1468).
    AnalyzesOk("select 1 from functional.alltypes a left outer join " +
        "(select id, upper(decode(string_col, NULL, date_string_col)) " +
        "from functional.alltypes) v on (a.id = v.id)");

    // Inline view with a subquery
    AnalyzesOk("select y x from " +
        "(select id y from functional.alltypestiny where id in " +
        "(select id from functional.alltypessmall)) a");
  }

  @Test
  public void TestCorrelatedInlineViews() {
    // Basic inline view with a single correlated table ref.
    AnalyzesOk("select cnt from functional.allcomplextypes t, " +
        "(select count(*) cnt from t.int_array_col) v");
    AnalyzesOk("select cnt from functional.allcomplextypes t, " +
        "(select cnt from (select count(*) cnt from t.int_array_col) v1) v2");
    AnalyzesOk("select item from functional.allcomplextypes t, " +
        "(select item from t.array_map_col.value) v");
    // Multiple nesting levels.
    AnalyzesOk("select item from functional.allcomplextypes t, " +
        "(select * from (select * from (select item from t.int_array_col) v1) v2) v3");
    // Mixing correlated and uncorrelated inline views in the same select block works.
    AnalyzesOk("select v.cnt from functional.allcomplextypes t1 " +
        "join (select * from functional.alltypes) t2 on (t1.id = t2.id) " +
        "cross join (select count(*) cnt from t1.int_map_col) v");
    // Multiple correlated table refs.
    AnalyzesOk("select avg from functional.allcomplextypes t, " +
        "(select avg(a1.item) avg from t.int_array_col a1, t.int_array_col a2) v");
    AnalyzesOk("select item, key, value from functional.allcomplextypes t, " +
        "(select * from t.int_array_col, t.int_map_col) v");
    // Correlated inline view inside uncorrelated inline view.
    AnalyzesOk("select cnt from functional.alltypes t1 inner join " +
        "(select id, cnt from functional.allcomplextypes t2, " +
        "(select count(1) cnt from t2.int_array_col) v1) v2");
    // Correlated table ref has child ref itself.
    AnalyzesOk("select key, item from functional.allcomplextypes t, " +
        "(select a1.key, a2.item from t.array_map_col a1, a1.value a2) v");
    AnalyzesOk("select key, av from functional.allcomplextypes t, " +
        "(select a1.key, av from t.array_map_col a1, " +
        "(select avg(item) av from a1.value a2) v1) v2");
    // TODO: Enable once we support complex-typed exprs in the select list.
    //AnalyzesOk("select key, av from functional.allcomplextypes t, " +
    //    "(select a1.key, a1.value from t.array_map_col a1) v1, " +
    //    "(select avg(item) av from v1.value) v2");
    // Multiple correlated table refs with different parents.
    AnalyzesOk("select t1.id, t2.id, cnt, av from functional.allcomplextypes t1 " +
        "left outer join functional.allcomplextypes t2 on (t1.id = t2.id), " +
        "(select count(*) cnt from t1.array_map_col) v1, " +
        "(select avg(item) av from t2.int_array_col) v2");
    // Correlated table refs in a union.
    AnalyzesOk("select item from functional.allcomplextypes t, " +
        "(select * from t.int_array_col union all select * from t.int_array_col) v");
    AnalyzesOk("select item from functional.allcomplextypes t, " +
        "(select item from t.int_array_col union distinct " +
        "select value from t.int_map_col) v");
    // Correlated inline view in WITH-clause.
    AnalyzesOk("with w as (select item from functional.allcomplextypes t, " +
        "(select item from t.int_array_col) v) " +
        "select * from w");
    AnalyzesOk("with w as (select key, av from functional.allcomplextypes t, " +
        "(select a1.key, av from t.array_map_col a1, " +
        "(select avg(item) av from a1.value a2) v1) v2) " +
        "select * from w");
    // TODO: Enable once we support complex-typed exprs in the select list.
    //AnalyzesOk("with w as (select key, av from functional.allcomplextypes t, " +
    //    "(select a1.key, a1.value from t.array_map_col a1) v1, " +
    //    "(select avg(item) av from v1.value) v2) " +
    //    "select * from w");

    // Test behavior of aliases in correlated inline views.
    // Inner reference resolves to the base table, not the implicit parent alias.
    AnalyzesOk("select cnt from functional.allcomplextypes t, " +
        "(select count(1) cnt from functional.allcomplextypes) v");
    AnalyzesOk("select cnt from functional.allcomplextypes, " +
        "(select count(1) cnt from functional.allcomplextypes) v");
    AnalyzesOk("select cnt from functional.allcomplextypes, " +
        "(select count(1) cnt from allcomplextypes) v", createAnalysisCtx("functional"));
    // Illegal correlated reference.
    AnalysisError("select cnt from functional.allcomplextypes t, " +
        "(select count(1) cnt from t) v",
        "Illegal table reference to non-collection type: 't'");
    AnalysisError("select cnt from functional.allcomplextypes, " +
        "(select count(1) cnt from allcomplextypes) v",
        "Illegal table reference to non-collection type: 'allcomplextypes'");

    // Un/correlated refs in a single nested query block.
    AnalysisError("select cnt from functional.allcomplextypes t, " +
        "(select count(1) cnt from functional.alltypes, t.int_array_col) v",
        "Nested query is illegal because it contains a table reference " +
        "'t.int_array_col' correlated with an outer block as well as an " +
        "uncorrelated one 'functional.alltypes':\n" +
        "SELECT count(1) cnt FROM functional.alltypes, t.int_array_col");
    AnalysisError("select cnt from functional.allcomplextypes t, " +
        "(select count(1) cnt from t.int_array_col, functional.alltypes) v",
        "Nested query is illegal because it contains a table reference " +
        "'t.int_array_col' correlated with an outer block as well as an " +
        "uncorrelated one 'functional.alltypes':\n" +
        "SELECT count(1) cnt FROM t.int_array_col, functional.alltypes");
    // Un/correlated refs across multiple nested query blocks.
    AnalysisError("select cnt from functional.allcomplextypes t, " +
        "(select * from functional.alltypes, " +
        "(select count(1) cnt from t.int_array_col) v1) v2",
        "Nested query is illegal because it contains a table reference " +
        "'t.int_array_col' correlated with an outer block as well as an " +
        "uncorrelated one 'functional.alltypes':\n" +
        "SELECT * FROM functional.alltypes, (SELECT count(1) cnt " +
        "FROM t.int_array_col) v1");
    // TODO: Enable once we support complex-typed exprs in the select list.
    // Correlated table ref has correlated inline view as parent.
    //AnalysisError("select cnt from functional.allcomplextypes t, " +
    //    "(select value arr from t.array_map_col) v1, " +
    //    "(select item from v1.arr, functional.alltypestiny) v2",
    //    "Nested query is illegal because it contains a table reference " +
    //    "'v1.arr' correlated with an outer block as well as an " +
    //    "uncorrelated one 'functional.alltypestiny':\n" +
    //    "SELECT item FROM v1.arr, functional.alltypestiny");
    // Un/correlated refs in union operands.
    AnalysisError("select cnt from functional.allcomplextypes t, " +
        "(select bigint_col from functional.alltypes " +
        "union select count(1) cnt from t.int_array_col) v1",
        "Nested query is illegal because it contains a table reference " +
        "'t.int_array_col' correlated with an outer block as well as an " +
        "uncorrelated one 'functional.alltypes':\n" +
        "SELECT bigint_col FROM functional.alltypes " +
        "UNION SELECT count(1) cnt FROM t.int_array_col");
    // Un/correlated refs in WITH-clause view.
    AnalysisError("with w as (select cnt from functional.allcomplextypes t, " +
        "(select count(1) cnt from t.int_array_col, functional.alltypes) v) " +
        "select * from w",
        "Nested query is illegal because it contains a table reference " +
        "'t.int_array_col' correlated with an outer block as well as an " +
        "uncorrelated one 'functional.alltypes':\n" +
        "SELECT count(1) cnt FROM t.int_array_col, functional.alltypes");

    // Test that joins without an ON clause analyze ok if the rhs table is correlated.
    for (JoinOperator joinOp: JoinOperator.values()) {
      if (joinOp.isNullAwareLeftAntiJoin() || joinOp.isIcebergDeleteJoin()) continue;
      AnalyzesOk(String.format("select 1 from functional.allcomplextypes a %s " +
          "(select item from a.int_array_col) v", joinOp));
      AnalyzesOk(String.format("select 1 from functional.allcomplextypes a %s " +
          "(select * from a.struct_array_col) v", joinOp));
      AnalyzesOk(String.format("select 1 from functional.allcomplextypes a %s " +
          "(select key, value from a.int_map_col) v", joinOp));
      AnalyzesOk(String.format("select 1 from functional.allcomplextypes a %s " +
          "(select * from a.struct_map_col) v", joinOp));
    }
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
    // wrong kind of expr
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (a.bigint_col = sum(b.int_col))",
        "aggregate function not allowed in ON clause");
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (a.bigint_col = " +
        "lag(b.int_col) over(order by a.bigint_col))",
        "analytic expression not allowed in ON clause");
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (a.id = b.id) and " +
        "a.int_col < (select min(id) from functional.alltypes c)",
        "Subquery is not allowed in ON clause");
    // unknown column
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (a.int_col = b.badcol)",
        "Could not resolve column/field reference: 'b.badcol'");
    // ambiguous col ref
    AnalysisError(
        "select a.int_col from functional.alltypes a " +
        "join functional.alltypes b on (int_col = int_col)",
        "Column/field reference is ambiguous: 'int_col'");
    // unknown alias
    AnalysisError(
        "select a.int_col from functional.alltypes a join functional.alltypes b on " +
        "(a.int_col = badalias.int_col)",
        "Could not resolve column/field reference: 'badalias.int_col'");
    // incompatible comparison
    AnalysisError(
        "select a.int_col from functional.alltypes a join " +
        "functional.alltypes b on a.bool_col = b.string_col",
        "operands of type BOOLEAN and STRING are not comparable: " +
        "a.bool_col = b.string_col");
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
        "Could not resolve column/field reference: 'c.int_col'");

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

    // BINARY columns can be used in joins.
    AnalyzesOk("select * from functional.binary_tbl a join " +
        "functional.binary_tbl b on a.binary_col = b.binary_col");
    AnalyzesOk("select * from functional.binary_tbl a join " +
        "functional.binary_tbl b using (binary_col)");
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

  /**
   * Tests the visibility of semi-/anti-joined table references.
   */
  @Test
  public void TestSemiJoins() {
    // Test left semi joins.
    for (String joinType: new String[] { "left semi join", "left anti join" }) {
      // semi/anti join requires ON/USING clause
      AnalyzesOk(String.format("select a.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)", joinType));
      AnalyzesOk(String.format("select a.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id) " +
          "%s functional.alltypes c on (a.id = c.id)", joinType, joinType));
      AnalyzesOk(String.format("select a.id from functional.alltypes a %s " +
          "functional.alltypes b using (id)", joinType));
      // unqualified column reference is not ambiguous outside of the On-clause
      // because a semi/anti-joined tuple is invisible
      AnalyzesOk(String.format("select int_col from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)", joinType));
      AnalysisError(String.format("select * from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id and a.int_col = int_col)", joinType),
          "Column/field reference is ambiguous: 'int_col'");
      // flip 'a' and 'b' aliases to test the unqualified column resolution logic
      AnalyzesOk(String.format("select int_col from functional.alltypes b " +
          "%s functional.alltypes a on (b.id = a.id)", joinType));
      AnalysisError(String.format("select * from functional.alltypes b " +
          "%s functional.alltypes a on (b.id = a.id and b.int_col = int_col)", joinType),
          "Column/field reference is ambiguous: 'int_col'");
      // unqualified column reference that matches two semi-/anti-joined tables
      // is not ambiguous outside of On-clause
      AnalyzesOk(String.format("select int_col from functional.alltypes c " +
          "%s functional.alltypes b on (c.id = b.id) " +
          "%s functional.jointbl a on (test_id = c.id)", joinType, joinType));
      AnalyzesOk(String.format("select int_col from functional.alltypes c " +
          "%s functional.alltypes b on (c.id = b.id) " +
          "%s functional.jointbl a on (test_id = id)", joinType, joinType));
      AnalysisError(String.format("select int_col from functional.alltypes c " +
          "%s functional.alltypes b on (c.id = b.id) " +
          "%s functional.jointbl a on (test_id = b.id)", joinType, joinType),
          "Illegal column/field reference 'b.id' of semi-/anti-joined table 'b'");
      // must not reference semi/anti-joined alias outside of join clause
      AnalysisError(String.format("select a.id, b.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)", joinType),
          "Illegal column/field reference 'b.id' of semi-/anti-joined table 'b'");
      AnalysisError(String.format("select a.id from functional.alltypes a " +
          "%s (select * from functional.alltypes) b " +
          "on (a.id = b.id) where b.int_col > 10", joinType),
          "Illegal column/field reference 'b.int_col' of semi-/anti-joined table 'b'");
      AnalysisError(String.format("select a.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id) group by b.bool_col", joinType),
          "Illegal column/field reference 'b.bool_col' of semi-/anti-joined table 'b'");
      AnalysisError(String.format("select a.id from functional.alltypes a " +
          "%s (select * from functional.alltypes) b " +
          "on (a.id = b.id) order by b.string_col", joinType),
          "Illegal column/field reference 'b.string_col' of " +
          "semi-/anti-joined table 'b'");
      // column of semi/anti-joined table is not visible in other On-clause
      AnalysisError(String.format("select a.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)" +
          "left outer join functional.testtbl c on (b.id = c.id)", joinType),
          "Illegal column/field reference 'b.id' of semi-/anti-joined table 'b'");
      // column of semi/anti-joined table is not visible in other On-clause
      AnalysisError(String.format("select a.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)" +
          "%s functional.testtbl c on (b.id = c.id)", joinType, joinType),
          "Illegal column/field reference 'b.id' of semi-/anti-joined table 'b'");
      // using clause always refers to lhs/rhs table
      AnalysisError(String.format("select a.id from functional.alltypes a " +
          "%s functional.alltypes b using(id) " +
          "%s functional.alltypes c using(int_col)", joinType, joinType),
          "Illegal column/field reference 'b.int_col' of semi-/anti-joined table 'b'");
      // unqualified column reference is ambiguous in the On-clause of a semi/anti join
      AnalysisError(String.format("select * from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id and a.int_col = int_col)", joinType),
          "Column/field reference is ambiguous: 'int_col'");
      // illegal unqualified column reference against semi/anti-joined table
      AnalysisError(String.format("select test_id from functional.alltypes a " +
          "%s functional.jointbl b on (a.id = b.alltypes_id)", joinType),
          "Illegal column/field reference 'test_id' of semi-/anti-joined table 'b'");
      // unqualified table ref is ambiguous even if semi/anti-joined
      AnalysisError(String.format("select alltypes.int_col from functional.alltypes " +
          "%s functional_parquet.alltypes " +
          "on (functional.alltypes.id = functional_parquet.alltypes.id)", joinType),
          "Unqualified table alias is ambiguous: 'alltypes'");
      // illegal collection table reference through semi/anti joined table
      AnalysisError(String.format("select 1 from functional.allcomplextypes a " +
          "%s functional.allcomplextypes b on (a.id = b.id) " +
          "inner join b.int_array_col", joinType),
          "Illegal table reference 'b.int_array_col' of semi-/anti-joined table 'b'");
    }

    // Test right semi joins. Do not combine these with the left semi join tests above
    // for better readability.
    for (String joinType: new String[] { "right semi join", "right anti join" }) {
      // semi/anti join requires ON/USING clause
      AnalyzesOk(String.format("select b.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)", joinType));
      AnalyzesOk(String.format("select c.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id) " +
          "%s functional.alltypes c on (b.id = c.id)", joinType, joinType));
      AnalyzesOk(String.format("select b.id from functional.alltypes a %s " +
          "functional.alltypes b using (id)", joinType));
      // unqualified column reference is not ambiguous outside of the On-clause
      // because a semi/anti-joined tuple is invisible
      AnalyzesOk(String.format("select int_col from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)", joinType));
      AnalysisError(String.format("select * from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id and a.int_col = int_col)", joinType),
          "Column/field reference is ambiguous: 'int_col'");
      // flip 'a' and 'b' aliases to test the unqualified column resolution logic
      AnalyzesOk(String.format("select int_col from functional.alltypes b " +
          "%s functional.alltypes a on (b.id = a.id)", joinType));
      AnalysisError(String.format("select * from functional.alltypes b " +
          "%s functional.alltypes a on (b.id = a.id and b.int_col = int_col)", joinType),
          "Column/field reference is ambiguous: 'int_col'");
      // unqualified column reference that matches two semi-/anti-joined tables
      // is not ambiguous outside of On-clause
      AnalyzesOk(String.format("select int_col from functional.jointbl c " +
          "%s functional.alltypes b on (test_id = b.id) " +
          "%s functional.alltypes a on (b.id = a.id)", joinType, joinType));
      AnalyzesOk(String.format("select int_col from functional.jointbl c " +
          "%s functional.alltypes b on (test_id = id) " +
          "%s functional.alltypes a on (b.id = a.id)", joinType, joinType));
      AnalysisError(String.format("select int_col from functional.jointbl c " +
          "%s functional.alltypes b on (test_id = a.id) " +
          "%s functional.alltypes a on (c.id = b.id)", joinType, joinType),
          "Could not resolve column/field reference: 'a.id'");
      // must not reference semi/anti-joined alias outside of join clause
      AnalysisError(String.format("select a.id, b.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)", joinType),
          "Illegal column/field reference 'a.id' of semi-/anti-joined table 'a'");
      AnalysisError(String.format("select b.id from functional.alltypes a " +
          "%s (select * from functional.alltypes) b " +
          "on (a.id = b.id) where a.int_col > 10", joinType),
          "Illegal column/field reference 'a.int_col' of semi-/anti-joined table 'a'");
      AnalysisError(String.format("select b.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id) group by a.bool_col", joinType),
          "Illegal column/field reference 'a.bool_col' of semi-/anti-joined table 'a'");
      AnalysisError(String.format("select b.id from functional.alltypes a " +
          "%s (select * from functional.alltypes) b " +
          "on (a.id = b.id) order by a.string_col", joinType),
          "Illegal column/field reference 'a.string_col' of " +
          "semi-/anti-joined table 'a'");
      // column of semi/anti-joined table is not visible in other On-clause
      AnalysisError(String.format("select b.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)" +
          "left outer join functional.testtbl c on (a.id = c.id)", joinType),
          "Illegal column/field reference 'a.id' of semi-/anti-joined table 'a'");
      // column of semi/anti-joined table is not visible in other On-clause
      AnalysisError(String.format("select b.id from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id)" +
          "%s functional.testtbl c on (a.id = c.id)", joinType, joinType),
          "Illegal column/field reference 'a.id' of semi-/anti-joined table 'a'");
      // using clause always refers to lhs/rhs table
      AnalysisError(String.format("select b.id from functional.alltypes a " +
          "%s functional.alltypes b using(id) " +
          "%s functional.alltypes c using(int_col)", joinType, joinType),
          "Illegal column/field reference 'b.id' of semi-/anti-joined table 'b'");
      // unqualified column reference is ambiguous in the On-clause of a semi/anti join
      AnalysisError(String.format("select * from functional.alltypes a " +
          "%s functional.alltypes b on (a.id = b.id and a.int_col = int_col)", joinType),
          "Column/field reference is ambiguous: 'int_col'");
      // illegal unqualified column reference against semi/anti-joined table
      AnalysisError(String.format("select test_id from functional.jointbl a " +
          "%s functional.alltypes b on (a.alltypes_id = b.id)", joinType),
          "Illegal column/field reference 'test_id' of semi-/anti-joined table 'a'");
      // unqualified table ref is ambiguous even if semi/anti-joined
      AnalysisError(String.format("select alltypes.int_col from functional.alltypes " +
          "%s functional_parquet.alltypes " +
          "on (functional.alltypes.id = functional_parquet.alltypes.id)", joinType),
          "Unqualified table alias is ambiguous: 'alltypes'");
      // illegal collection table reference through semi/anti joined table
      AnalysisError(String.format("select 1 from functional.allcomplextypes a " +
          "%s functional.allcomplextypes b on (a.id = b.id) " +
          "inner join a.int_array_col", joinType),
          "Illegal table reference 'a.int_array_col' of semi-/anti-joined table 'a'");
    }
  }

  /**
   * Return all supported hint styles.
   */
  private String[][] getHintStyles() {
    return new String[][] {
        new String[] { "/* +", "*/" }, // traditional commented hint
        new String[] { "\n-- +", "\n" }, // eol commented hint
        new String[] { "[", "]" } // legacy style
    };
  }

  @Test
  public void TestJoinHints() throws AnalysisException {
    for (String[] hintStyle: getHintStyles()) {
      String prefix = hintStyle[0];
      String suffix = hintStyle[1];
      AnalyzesOk(
          String.format("select * from functional.alltypes a join %sbroadcast%s " +
          "functional.alltypes b using (int_col)", prefix, suffix));
      AnalyzesOk(
          String.format("select * from functional.alltypes a join %sshuffle%s " +
          "functional.alltypes b using (int_col)", prefix, suffix));
      AnalyzesOk(
          String.format("select * from functional.alltypes a cross join %sbroadcast%s " +
          "functional.alltypes b", prefix, suffix));
      // Only warn on unrecognized hints for view-compatibility with Hive.
      AnalyzesOk(
          String.format("select * from functional.alltypes a join %sbadhint%s " +
              "functional.alltypes b using (int_col)", prefix, suffix),
          "JOIN hint not recognized: badhint");
      AnalysisError(
          String.format("select * from functional.alltypes a cross join %sshuffle%s " +
          "functional.alltypes b", prefix, suffix),
          "CROSS JOIN does not support SHUFFLE.");
      AnalysisError(String.format(
          "select * from functional.alltypes a right outer join %sbroadcast%s " +
              "functional.alltypes b using (int_col)", prefix, suffix),
          "RIGHT OUTER JOIN does not support BROADCAST.");
      AnalysisError(String.format(
          "select * from functional.alltypes a full outer join %sbroadcast%s " +
          "functional.alltypes b using (int_col)", prefix, suffix),
          "FULL OUTER JOIN does not support BROADCAST.");
      AnalysisError(String.format(
          "select * from functional.alltypes a right semi join %sbroadcast%s " +
          "functional.alltypes b using (int_col)", prefix, suffix),
          "RIGHT SEMI JOIN does not support BROADCAST.");
      AnalysisError(String.format(
          "select * from functional.alltypes a right anti join %sbroadcast%s " +
          "functional.alltypes b using (int_col)", prefix, suffix),
          "RIGHT ANTI JOIN does not support BROADCAST.");
      // Conflicting join hints.
      AnalysisError(String.format(
          "select * from functional.alltypes a join %sbroadcast,shuffle%s " +
              "functional.alltypes b using (int_col)", prefix, suffix),
          "Conflicting JOIN hint: shuffle");
    }
  }

  @Test
  public void TestTableHints() throws AnalysisException {
    for (String[] hintStyle: getHintStyles()) {
      String prefix = hintStyle[0];
      String suffix = hintStyle[1];
      for (String alias : new String[] { "", "a" }) {
        AnalyzesOk(
            String.format("select * from functional.alltypes %s %sschedule_cache_local%s",
            alias, prefix, suffix));
        AnalyzesOk(
            String.format("select * from functional.alltypes %s %sschedule_disk_local%s",
            alias, prefix, suffix));
        AnalyzesOk(
            String.format("select * from functional.alltypes %s %sschedule_remote%s",
            alias, prefix, suffix));
        AnalyzesOk(
            String.format("select * from functional.alltypes %s %sschedule_remote," +
            "schedule_random_replica%s", alias, prefix, suffix));
        AnalyzesOk(
            String.format("select * from functional.alltypes %s %s" +
            "schedule_random_replica,schedule_remote%s", alias, prefix, suffix));

        String name = alias.isEmpty() ? "functional.alltypes" : alias;
        AnalyzesOk(String.format("select * from functional.alltypes %s %sFOO%s", alias,
            prefix, suffix), String.format("Table hint not recognized for table %s: " +
            "FOO", name));

        // Table hints not supported for HBase tables
        AnalyzesOk(String.format("select * from functional_hbase.alltypes %s " +
              "%sschedule_random_replica%s", alias, prefix, suffix),
            "Table hints only supported for Hdfs/Kudu tables");
        // Table hints not supported for catalog views
        AnalyzesOk(String.format("select * from functional.alltypes_view %s " +
              "%sschedule_random_replica%s", alias, prefix, suffix),
            "Table hints not supported for inline view and collections");
        // Table hints not supported for with clauses
        AnalyzesOk(String.format("with t as (select 1) select * from t %s " +
              "%sschedule_random_replica%s", alias, prefix, suffix),
            "Table hints not supported for inline view and collections");
      }
      // Table hints not supported for inline views
      AnalyzesOk(String.format("select * from (select tinyint_col * 2 as c1 from " +
          "functional.alltypes) as v1 %sschedule_random_replica%s", prefix, suffix),
          "Table hints not supported for inline view and collections");
      // Table hints not supported for collection tables
      AnalyzesOk(String.format("select item from functional.allcomplextypes, " +
          "allcomplextypes.int_array_col %sschedule_random_replica%s", prefix, suffix),
          "Table hints not supported for inline view and collections");
    }
  }

  /**
  * Checks warning message if applicable and
  * returns true if straight_join hint is applied or false otherwise.
  */
  private boolean hasStraightJoin(String stmt, String expectedWarning){
    AnalysisContext ctx = createAnalysisCtx();
    if (expectedWarning == null) {
      AnalyzesOkWithoutWarnings(stmt, ctx);
    } else {
      AnalyzesOk(stmt, ctx, expectedWarning);
    }
    return ctx.getAnalyzer().isStraightJoin();
  }

  @Test
  public void TestSelectListHints() throws AnalysisException {
    for (String[] hintStyle: hintStyles_) {
      String prefix = hintStyle[0];
      String suffix = hintStyle[1];
      assertTrue(hasStraightJoin(String.format(
          "select %sstraight_join%s * from functional.alltypes", prefix, suffix), null));
      assertTrue(hasStraightJoin(String.format(
          "select %sStrAigHt_jOiN%s * from functional.alltypes", prefix, suffix), null));
      if (!prefix.equals("")) {
        // Only warn on unrecognized hints for view-compatibility with Hive.
        // Legacy hint style does not parse.
        assertFalse(hasStraightJoin(String.format(
            "select %sbadhint%s * from functional.alltypes", prefix, suffix),
            "PLAN hint not recognized: badhint"));
        assertTrue(hasStraightJoin(String.format(
             "select %sstraight_join%s * from functional.alltypes", prefix, suffix),
             null));
        // Multiple hints. Legacy hint style does not parse.
        assertTrue(hasStraightJoin(String.format(
             "select %sstraight_join,straight_join%s * from functional.alltypes",
              prefix, suffix), null));
      }
    }
  }

  @Test
  public void TestInsertHints() throws AnalysisException {
    // Test table to make sure that conflicting hints and table properties result in a
    // warning.
    addTestDb("test_sort_by", "Test DB for SORT BY clause.");
    addTestTable("create table test_sort_by.alltypes (id int, int_col int, " +
        "bool_col boolean) partitioned by (year int, month int) " +
        "sort by (int_col, bool_col) location '/'");
    for (String[] hintStyle: getHintStyles()) {
      String prefix = hintStyle[0];
      String suffix = hintStyle[1];
      // Test plan hints for partitioned Hdfs tables.
      AnalyzesOk(String.format("insert into functional.alltypessmall " +
          "partition (year, month) %sshuffle%s select * from functional.alltypes",
          prefix, suffix));
      AnalyzesOk(String.format("insert into table functional.alltypessmall " +
          "partition (year, month) %snoshuffle%s select * from functional.alltypes",
          prefix, suffix));
      // Only warn on unrecognized hints.
      AnalyzesOk(String.format("insert into functional.alltypessmall " +
          "partition (year, month) %sbadhint%s select * from functional.alltypes",
          prefix, suffix),
          "INSERT hint not recognized: badhint");
      // Insert hints are ok for unpartitioned tables.
      AnalyzesOk(String.format(
          "insert into table functional.alltypesnopart %sshuffle%s " +
          "select * from functional.alltypesnopart", prefix, suffix));
      // Insert hints are ok for Kudu tables.
      AnalyzesOk(String.format(
          "insert into table functional_kudu.alltypes %sshuffle%s " +
          "select * from functional_kudu.alltypes", prefix, suffix));
      // Plan hints do not make sense for inserting into HBase tables.
      AnalysisError(String.format(
          "insert into table functional_hbase.alltypes %sshuffle%s " +
          "select * from functional_hbase.alltypes", prefix, suffix),
          "INSERT hints are only supported for inserting into Hdfs and Kudu tables: " +
          "functional_hbase.alltypes");
      // Conflicting plan hints.
      AnalysisError("insert into table functional.alltypessmall " +
          "partition (year, month) /* +shuffle,noshuffle */ " +
          "select * from functional.alltypes",
          "Conflicting INSERT hints: shuffle and noshuffle");

      // Test clustered hint.
      AnalyzesOk(String.format(
          "insert into functional.alltypessmall partition (year, month) %sclustered%s " +
          "select * from functional.alltypes", prefix, suffix));
      AnalyzesOk(String.format(
          "insert into table functional.alltypesnopart %sclustered%s " +
          "select * from functional.alltypesnopart", prefix, suffix));
      // Test that noclustered is accepted.
      AnalyzesOk(String.format(
          "insert into table functional.alltypesnopart %snoclustered%s " +
          "select * from functional.alltypesnopart", prefix, suffix));
      // Conflicting clustered hints.
      AnalysisError(String.format(
          "insert into table functional.alltypessmall partition (year, month) " +
          "/* +clustered,noclustered */ select * from functional.alltypes", prefix,
          suffix), "Conflicting INSERT hints: clustered and noclustered");

      // noclustered hint on a table with sort.columns issues a warning.
      AnalyzesOk(String.format(
          "insert into test_sort_by.alltypes partition (year, month) " +
          "%snoclustered%s select id, int_col, bool_col, year, month from " +
          "functional.alltypes", prefix, suffix),
          "Insert statement has 'noclustered' hint, but table has 'sort.columns' " +
          "property. The 'noclustered' hint will be ignored.");

      // Default hints should be ignored when query has plan hints.
      testDefaultHintIgnored(String.format(
          "insert into functional.alltypes partition (year, month) " +
          "%snoclustered,shuffle%s select * from functional.alltypes",
          prefix, suffix),
          "CLUSTERED");
      testDefaultHintIgnored(String.format(
          "insert into functional_kudu.alltypes " +
          "%snoclustered,shuffle%s select * from functional.alltypes",
          prefix, suffix),
          "CLUSTERED : NOSHUFFLE ");
    }

    // Multiple non-conflicting hints and case insensitivity of hints.
    AnalyzesOk("insert into table functional.alltypessmall " +
        "partition (year, month) /* +shuffle, ShUfFlE */ " +
        "select * from functional.alltypes");
    AnalyzesOk("insert into table functional.alltypessmall " +
        "partition (year, month) [shuffle, ShUfFlE] " +
        "select * from functional.alltypes");

    // Test default hints.
    AnalysisContext insertCtx = createAnalysisCtx();
    // Bad hint returns a warning.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement("badhint");
    AnalyzesOk("insert into functional.alltypessmall partition (year, month) " +
        "select * from functional.alltypes",
        insertCtx,
        "INSERT hint not recognized: badhint");
    // Bad hint returns a warning.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement(
        "clustered:noshuffle:badhint");
    AnalyzesOk("insert into functional.alltypessmall partition (year, month) " +
        "select * from functional.alltypes",
        insertCtx,
        "INSERT hint not recognized: badhint");
    // Conflicting hints return an error.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement(
        "clustered:noclustered");
    AnalysisError("insert into functional.alltypessmall partition (year, month) " +
        "select * from functional.alltypes",
        insertCtx,
        "Conflicting INSERT hints: clustered and noclustered");
    // Conflicting hints return an error.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement("shuffle:noshuffle");
    AnalysisError("insert into functional.alltypessmall partition (year, month) " +
        "select * from functional.alltypes",
        insertCtx,
        "Conflicting INSERT hints: shuffle and noshuffle");
    // Default hints ignored for HBase table.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement("noclustered");
    AnalyzesOk("insert into table functional_hbase.alltypes " +
        "select * from functional_hbase.alltypes",
        insertCtx);
    // Default hints are ok for Kudu table.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement("clustered:noshuffle");
    AnalyzesOk(String.format("insert into table functional_kudu.alltypes " +
        "select * from functional_kudu.alltypes"),
        insertCtx);
    testDefaultHintApplied(insertCtx);
    // Default hints are ok for partitioned Hdfs tables.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement(
        "NOCLUSTERED:noshuffle");
    AnalyzesOk("insert into functional.alltypessmall partition (year, month) " +
        "select * from functional.alltypes",
        insertCtx);
    testDefaultHintApplied(insertCtx);
    // Default hints are ok for non partitioned Hdfs tables.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement("CLUSTERED:SHUFFLE");
    AnalyzesOk("insert into functional.alltypesnopart " +
        "select * from functional.alltypesnopart",
        insertCtx);
    testDefaultHintApplied(insertCtx);
  }

  @Test
  public void TestWhereClause() throws AnalysisException {
    AnalyzesOk("select zip, name from functional.testtbl where id > 15");
    AnalysisError("select zip, name from functional.testtbl where badcol > 15",
        "Could not resolve column/field reference: 'badcol'");
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

    // Check abs() retains type, originally abs() would return double,
    // which is incompatible with interval, see IMPALA-1424
    AnalyzesOk("select now() + interval abs(cast(1 as int)) days");
    AnalyzesOk("select now() + interval abs(cast(1 as smallint)) days");
    AnalyzesOk("select now() + interval abs(cast(1 as tinyint)) days");

    AnalyzesOk("select round(c1) from functional.decimal_tiny");
    AnalyzesOk("select round(c1, 2) from functional.decimal_tiny");
    AnalysisError("select round(c1, cast(c3 as int)) from functional.decimal_tiny",
        "round() must be called with a constant second argument.");
    AnalysisError("select truncate(c1, cast(c3 as int)) from functional.decimal_tiny",
        "truncate() must be called with a constant second argument.");
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
    addTestUda("AggFn", Type.BIGINT, Type.INT);
    addTestUda("AggFn", Type.BIGINT, Type.BIGINT);
    addTestUda("AggFn", Type.BIGINT, Type.DOUBLE);
    addTestUda("AggFn", Type.STRING, Type.STRING, Type.STRING);

    AnalyzesOk("select aggfn(int_col) from functional.alltypesagg");
    AnalysisError("select default.AggFn(1)",
        "aggregation without a FROM clause is not allowed");
    AnalysisError(
        "select aggfn(int_col) over (partition by int_col) from functional.alltypesagg",
        "Aggregate function 'default.aggfn(int_col) /* NATIVE UDF */' not supported " +
        "with OVER clause.");
    AnalysisError("select aggfn(distinct int_col) from functional.alltypesagg",
        "User defined aggregates do not support DISTINCT.");
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

    AnalysisError("select count() from functional.alltypes",
        "count() is not allowed.");
    AnalysisError("select min() from functional.alltypes",
        "No matching function with signature: min().");
    AnalysisError("select int_col from functional.alltypes order by count(*)",
        "select list expression not produced by aggregation output (missing from "
          + "GROUP BY clause?): int_col");
    AnalysisError("select functional.alltypes.*, max(string_col) from " +
        "functional.alltypes", "cannot combine '*' in select list with grouping or " +
        "aggregation");
    AnalysisError("select * from functional.alltypes order by count(*)",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?): *");

    // only count() allows '*'
    AnalysisError("select avg(*) from functional.testtbl",
        "'*' can only be used in conjunction with COUNT");
    AnalysisError("select min(*) from functional.testtbl",
        "'*' can only be used in conjunction with COUNT");
    AnalysisError("select max(*) from functional.testtbl",
        "'*' can only be used in conjunction with COUNT");

    // multiple args
    AnalysisError("select count(id, zip) from functional.testtbl",
        "COUNT must have DISTINCT for multiple arguments: count(id, zip)");
    AnalysisError("select min(id, zip) from functional.testtbl",
        "No matching function with signature: min(BIGINT, INT).");

    // nested aggregates
    AnalysisError("select sum(count(*)) from functional.testtbl",
        "aggregate function must not contain aggregate parameters");
    AnalysisError("select sum(rank() over (order by id)) from functional.testtbl",
        "aggregate function must not contain analytic parameters");
    AnalysisError("select min(aggfn(int_col)) from functional.alltypes",
        "aggregate function must not contain aggregate parameters: " +
        "min(default.aggfn(int_col) /* NATIVE UDF */)");

    // wrong type
    AnalysisError("select sum(timestamp_col) from functional.alltypes",
        "SUM requires a numeric parameter: sum(timestamp_col)");
    AnalysisError("select sum(date_col) from functional.date_tbl",
        "SUM requires a numeric parameter: sum(date_col)");
    AnalysisError("select sum(string_col) from functional.alltypes",
        "SUM requires a numeric parameter: sum(string_col)");
    AnalysisError("select avg(string_col) from functional.alltypes",
        "AVG requires a numeric or timestamp parameter: avg(string_col)");
    AnalysisError("select avg(binary_col) from functional.binary_tbl",
        "AVG requires a numeric or timestamp parameter: avg(binary_col)");


    // aggregate requires table in the FROM clause
    AnalysisError("select count(*)", "aggregation without a FROM clause is not allowed");
    AnalysisError("select min(1)", "aggregation without a FROM clause is not allowed");

    // Test distinct estimate
    for (Type type: typeToLiteralValue_.keySet()) {
      AnalyzesOk(String.format(
          "select ndv(%s) from functional.alltypes",
          typeToLiteralValue_.get(type)));
    }

    // Decimal
    AnalyzesOk("select min(d1), max(d2), count(d3), sum(d4) "
        + "from functional.decimal_tbl");
    AnalyzesOk("select ndv(d1), distinctpc(d2), distinctpcsa(d3), count(distinct d4) "
        + "from functional.decimal_tbl");
    AnalyzesOk("select avg(d5) from functional.decimal_tbl");

    // Date
    AnalyzesOk("select min(date_col), max(date_col), count(date_col) "
        + "from functional.date_tbl");
    AnalyzesOk("select ndv(date_col), distinctpc(date_col), distinctpcsa(date_col), "
        + "count(distinct date_col) from functional.date_tbl");

    // Binary
    AnalyzesOk("select min(binary_col), max(binary_col), count(binary_col), "
        + "max(length(binary_col)) from functional.binary_tbl");
    AnalysisError("select ndv(binary_col) from functional.binary_tbl",
        "No matching function with signature: ndv(BINARY).");

    // Test select stmt avg smap.
    AnalyzesOk("select cast(avg(c1) as decimal(10,4)) as c from " +
        "functional.decimal_tiny group by c3 having cast(avg(c1) as " +
        "decimal(10,4)) = 5.1106 order by 1");

    // check CHAR and VARCHAR aggregates
    checkExprType("select min(cast('foo' as char(5))) from functional.chars_tiny",
        ScalarType.STRING);
    checkExprType("select max(cast('foo' as varchar(5))) from functional.chars_tiny",
        ScalarType.STRING);
    checkExprType("select max(vc) from functional.chars_tiny", ScalarType.STRING);
    checkExprType("select max(cs) from functional.chars_tiny", ScalarType.STRING);
    checkExprType("select max(lower(cs)) from functional.chars_tiny",
        ScalarType.STRING);
  }

  /**
   * Test ROLLUP, CUBE and GROUPING SETS functionality.
   */
  @Test
  public void TestGroupingSets() throws AnalysisException {
    // Basic examples of each clause.
    AnalyzesOk("select count(*) from functional.alltypes " +
        "group by rollup(int_col, string_col)");
    AnalyzesOk("select count(*) from functional.alltypes " +
        "group by int_col, string_col with rollup");
    AnalyzesOk("select count(*) from functional.alltypes " +
        "group by int_col, string_col with cube");
    AnalyzesOk("select count(*) from functional.alltypes " +
        "group by GROUPING SETS((int_col), (string_col))");

    // Test limits for number of distinct expressions
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= 25; ++i) {
      sb.append("bool_col" + i + ", tinyint_col" + i + ", smallint_col" + i +
                ", int_col" + i + ", bigint_col" + i + ", float_col" + i +
                ", double_col" + i + ", string_col" + i);
      if (i != 25) {
        sb.append(",");
      }
    }
    String longColList = sb.toString();
    AnalysisError("select count(*) from functional.widetable_250_cols " +
        "group by rollup(" + longColList + ")",
        "Number of grouping columns (200) exceeds GROUP BY with ROLLUP limit of 63");
    AnalysisError("select count(*) from functional.widetable_250_cols " +
        "group by cube(" + longColList + ")",
        "Number of grouping columns (200) exceeds GROUP BY with CUBE limit of 63");
    AnalysisError("select count(*) from functional.widetable_250_cols " +
        "group by grouping sets((" + longColList + "))",
        "Number of grouping columns (200) exceeds GROUP BY with SETS limit of 63");

    // Duplicating the same column shouldn't increase distinct column count.
    AnalysisError("select count(*) from functional.widetable_250_cols " +
        "group by grouping sets((" + longColList + ", string_col1), " +
        "(bool_col1, int_col1))",
        "Number of grouping columns (200) exceeds GROUP BY with SETS limit of 63");

    // Grouping sets with overlapping expression lists doesn't hit limit, since exprs are
    // deduplicated.
    AnalyzesOk("select count(*) from functional.widetable_250_cols " +
      "group by grouping sets(( " +
      "  bool_col1, tinyint_col1, smallint_col1, int_col1, bigint_col1, " +
      "  float_col1, double_col1, string_col1, " +
      "  bool_col2, tinyint_col2, smallint_col2, int_col2, bigint_col2, " +
      "  float_col2, double_col2, string_col2, " +
      "  bool_col3, tinyint_col3, smallint_col3, int_col3, bigint_col3, " +
      "  float_col3, double_col3, string_col3, " +
      "  bool_col4, tinyint_col4, smallint_col4, int_col4, bigint_col4, " +
      "  float_col4, double_col4, string_col4, " +
      "  bool_col5, tinyint_col5, smallint_col5, int_col5, bigint_col5, " +
      "  float_col5, double_col5, string_col5, " +
      "  bool_col6, tinyint_col6, smallint_col6, int_col6, bigint_col6, " +
      "  float_col6, double_col6, string_col6, " +
      "  bool_col7, tinyint_col7, smallint_col7, int_col7, bigint_col7, " +
      "  float_col7, double_col7, string_col7), " +
      "  (bool_col1, tinyint_col1, smallint_col1, int_col1, bigint_col1, " +
      "  float_col1, double_col1, string_col1, " +
      "  bool_col2, tinyint_col2, smallint_col2, int_col2, bigint_col2, " +
      "  float_col2, double_col2, string_col2)); ");

    // Test limit of number of distinct grouping sets - 63. CUBE with 6 grouping exprs
    // results in 2^6 = 64 distinct grouping sets. CUBE with 5 grouping exprs and ROLLUP
    // with 6 grouping exprs are both under the limit.
    AnalyzesOk("select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, count(*) " +
        "from functional.alltypes " +
        "group by rollup(id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col)");
    AnalyzesOk("select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "count(*) " +
        "from functional.alltypes " +
        "group by cube(id, bool_col, tinyint_col, smallint_col, int_col, bigint_col)");
    AnalysisError("select id, bool_col, tinyint_col, smallint_col, int_col, " +
        "bigint_col, float_col, count(*) " +
        "from functional.alltypes " +
        "group by cube(id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col)",
        "Limit of 64 grouping sets exceeded");


    // Basic test cases where grouping exprs are subset and superset of select list.
    AnalyzesOk("select int_col, count(*) from functional.alltypes " +
        "group by rollup(int_col, string_col)");
    AnalyzesOk("select int_col, string_col, count(*) from functional.alltypes " +
        "group by rollup(int_col, string_col)");
    AnalysisError("select int_col, bool_col, string_col, count(*) " +
        "from functional.alltypes " +
        "group by rollup(int_col, string_col)",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?): bool_col");
    AnalyzesOk("select int_col, count(*) from functional.alltypes " +
        "group by cube(int_col, string_col)");
    AnalyzesOk("select int_col, string_col, count(*) from functional.alltypes " +
        "group by cube(int_col, string_col)");
    AnalysisError("select int_col, bool_col, string_col, count(*) " +
        "from functional.alltypes " +
        "group by cube(int_col, string_col)",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?): bool_col");
    AnalyzesOk("select int_col, count(*) from functional.alltypes " +
        "group by grouping sets((int_col, string_col), (int_col))");
    AnalyzesOk("select int_col, string_col, count(*) from functional.alltypes " +
        "group by grouping sets((int_col, string_col), (int_col))");
    AnalysisError("select int_col, bool_col, string_col, count(*) " +
        "from functional.alltypes " +
        "group by grouping sets((int_col, string_col), (int_col))",
        "select list expression not produced by aggregation output " +
        "(missing from GROUP BY clause?): bool_col");

    // Support for ordinals.
    AnalyzesOk("select int_col, string_col, count(*) from functional.alltypes " +
        "group by rollup(1, 2)");
    AnalyzesOk("select int_col, string_col, count(*) from functional.alltypes " +
        "group by cube(1, 2)");
    AnalyzesOk("select int_col, string_col, count(*) from functional.alltypes " +
        "group by grouping sets((1, 2), (1))");
    AnalysisError("select int_col, string_col, count(*) from functional.alltypes " +
        "group by rollup(1, 3)",
        "GROUP BY expression must not contain aggregate functions: 3");
    AnalysisError("select int_col, string_col, count(*) from functional.alltypes " +
        "group by cube(1, 3)",
        "GROUP BY expression must not contain aggregate functions: 3");
    AnalysisError("select int_col, string_col, count(*) from functional.alltypes " +
        "group by grouping sets((1, 3), (1))",
        "GROUP BY expression must not contain aggregate functions: 3");

    // Refer to same column by name and ordinal in grouping sets.
    AnalyzesOk("select int_col, string_col, count(*) from functional.alltypes " +
        "group by grouping sets((1, 2), (int_col))");

    // Group by non-trivial expressions not in select list
    AnalyzesOk("select count(*) from functional.alltypes " +
        "group by rollup(int_col, substring(string_col, 1, 2))");
    AnalyzesOk("select int_col, count(*) from functional.alltypes " +
        "group by cube(int_col, substring(string_col, 1, 2))");
    AnalyzesOk("select int_col, count(*) from functional.alltypes " +
        "group by grouping sets((int_col), (int_col, substring(string_col, 1, 2)))");

    // Group by non-trivial expressions in select list
    AnalyzesOk("select int_col, substring(string_col, 1, 2) str, count(*) " +
        "from functional.alltypes " +
        "group by rollup(int_col, substring(string_col, 1, 2))");
    AnalyzesOk("select int_col, substring(string_col, 1, 2) str, count(*) " +
        "from functional.alltypes " +
        "group by cube(int_col, str)");
    AnalyzesOk("select int_col, substring(string_col, 1, 2) str, count(*) " +
        "from functional.alltypes " +
        "group by grouping sets((int_col, str), (substring(string_col, 1, 2)))");

    // Expressions in select list must appear in group by.
    AnalysisError("select int_col, substring(string_col, 1, 3) str, count(*) " +
        "from functional.alltypes " +
        "group by rollup(int_col, substring(string_col, 1, 2))",
        "select list expression not produced by aggregation output (missing from " +
        "GROUP BY clause?): substring(string_col, 1, 3)");

    // Use the expression and an alias.
    AnalyzesOk("select int_col, substring(string_col, 1, 2) str, count(*) " +
        "from functional.alltypes " +
        "group by rollup(int_col, str, substring(string_col, 1, 2))");

    // Group by constant expression.
    AnalyzesOk("select int_col, count(*) " +
        "from functional.alltypes " +
        "group by rollup(int_col, 'constant')");
    AnalyzesOk("select int_col, count(*) " +
        "from functional.alltypes " +
        "group by cube(int_col, 'constant')");
    AnalyzesOk("select int_col, count(*) " +
        "from functional.alltypes " +
        "group by grouping sets((false), (int_col, 'constant'))");

    // Subqueries with grouping sets inside and outside subquery.
    AnalyzesOk("select g.int_col, count(*) " +
        "from functional.alltypesagg g " +
        "left outer join functional.alltypes a on g.id = a.id " +
        "where g.int_col < 100 and g.tinyint_col < ( " +
        "  select count(*) from functional.alltypes t " +
        "  where t.id = g.id and g.string_col = t.string_col and t.bool_col = true) " +
        "group by rollup(g.int_col, g.string_col) " +
        "having count(*) < 100");
    AnalysisError("select g.int_col, count(*) " +
        "from functional.alltypesagg g " +
        "where g.int_col < 100 and g.tinyint_col < ( " +
        "  select count(*) from functional.alltypes t " +
        "  where t.id = g.id and g.string_col = t.string_col and t.bool_col = true " +
        "  group by rollup(t.string_col, t.bool_col)) " +
        "  group by g.int_col",
        "Unsupported correlated subquery with grouping and/or aggregation");

    // Combining grouping sets and distinct aggregations is not supported at this point.
    // See IMPALA-9914.
    AnalysisError("select count(distinct id) from functional.alltypes " +
        "group by rollup(int_col, bool_col)",
        "Distinct aggregate functions and grouping sets are not supported in the same " +
        "query block.");
    AnalysisError("select count(distinct id), count(distinct string_col) " +
        "from functional.alltypes " +
        "group by rollup(int_col, bool_col)",
        "Distinct aggregate functions and grouping sets are not supported in the same " +
        "query block.");

    // Combining DISTINCT and GROUP BY is not supported in general, not just for grouping
    // sets.
    AnalysisError("select distinct int_col, bool_col, count(*) " +
        "from functional.alltypes " +
        "group by rollup(int_col, bool_col)",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");

    // grouping_id() and grouping() are aggregate functions that can be used in any
    // GROUP BY, even though they are only meaningful in conjunction with
    // CUBE/ROLLUP/GROUPING SETS. grouping() must reference exactly one of the grouping
    // columns.
    AnalyzesOk("select int_col, string_col, grouping_id(), grouping(int_col), " +
        "grouping(string_col) " +
        "from functional.alltypes " +
        "group by rollup(int_col, string_col)");
    AnalyzesOk("select int_col, string_col, grouping_id(), grouping(int_col), " +
        "grouping(string_col) " +
        "from functional.alltypes " +
        "group by cube(int_col, string_col)");
    AnalyzesOk("select int_col, string_col, grouping_id(), grouping(int_col), " +
        "grouping(string_col) " +
        "from functional.alltypes " +
        "group by grouping sets((), (int_col, string_col))");

    // grouping_id() can reference a variable number of grouping columns.
    AnalyzesOk("select int_col, string_col, grouping_id(int_col, string_col), " +
        "grouping(string_col) " +
        "from functional.alltypes " +
        "group by rollup(int_col, string_col)");

    // Non-trivial expressions involving grouping_id().
    AnalyzesOk("select bitand(grouping_id(), 1), cast(grouping(int_col) as string) " +
        "from functional.alltypes " +
        "group by rollup(int_col, string_col)");

    // grouping() can reference non-trivial grouping expressions
    AnalyzesOk("select int_col * 2, string_col, grouping(int_col * 2),  count(*) " +
        "from functional.alltypes " +
        "group by rollup(1, 2)");

    // grouping_id() and grouping() can be used in conjunction with a plain GROUP BY.
    AnalyzesOk("select int_col, string_col, grouping_id() " +
        "from functional.alltypes " +
        "group by int_col, string_col");
    AnalyzesOk("select int_col, string_col, grouping(int_col) " +
        "from functional.alltypes " +
        "group by int_col, string_col");

    // grouping() and grouping_id() can be used in the degenerate case of a single
    // grouping set.
    AnalyzesOk("select int_col, string_col, grouping_id(), grouping(int_col) " +
        "from functional.alltypes " +
        "group by grouping sets((int_col, string_col))");

    // grouping() raises analysis error if given wrong number of args.
    AnalysisError("select int_col, string_col, grouping(int_col, string_col) " +
        "from functional.alltypes " +
        "group by grouping sets((), (int_col, string_col))",
        "No matching function with signature: grouping(INT, STRING).");
    AnalysisError("select int_col, string_col, grouping() " +
        "from functional.alltypes " +
        "group by grouping sets((), (int_col, string_col))",
        "No matching function with signature: grouping().");

    // grouping() and grouping_id() must reference grouping expressions.
    AnalysisError("select int_col, grouping('test') from functional.alltypes " +
        "group by rollup(int_col, string_col)",
        "'test' is not a grouping expression");
    AnalysisError("select int_col, grouping(1) from functional.alltypes " +
        "group by rollup(int_col, string_col)",
        "1 is not a grouping expression");
    AnalysisError("select grouping(tinyint_col) from functional.alltypes " +
        "group by rollup(int_col, string_col)",
        "tinyint_col is not a grouping expression");
    AnalysisError("select count(*), grouping(count(*)) from functional.alltypes " +
        "group by rollup(int_col, string_col)",
        "aggregate function must not contain aggregate parameters: grouping(count(*))");
    AnalysisError("select int_col, grouping_id(int_col, 'test') " +
        "from functional.alltypes " +
        "group by rollup(int_col, string_col)",
        "'test' is not a grouping expression");
    AnalysisError("select int_col, grouping_id(1) from functional.alltypes " +
        "group by rollup(int_col, string_col)",
        "1 is not a grouping expression");
    AnalysisError("select grouping_id(int_col, tinyint_col) from functional.alltypes " +
        "group by rollup(int_col, string_col)",
        "tinyint_col is not a grouping expression");
    AnalysisError("select count(*), grouping_id(count(*)) from functional.alltypes " +
        "group by rollup(int_col, string_col)",
        "count(*) is not a grouping expression");

    // grouping() and grouping_id() require GROUP BY clause.
    AnalysisError("select int_col, grouping(int_col) from functional.alltypes",
        "grouping() or grouping_id() function requires a GROUP BY clause: '" +
        "grouping(int_col)'");
    AnalysisError("select int_col, grouping_id() from functional.alltypes",
        "grouping() or grouping_id() function requires a GROUP BY clause: '" +
        "grouping_id()'");
  }

  @Test
  public void TestDistinct() throws AnalysisException {
    AnalyzesOk("select count(distinct id) as sum_id from functional.testtbl");
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
    AnalyzesOk("select distinct id from functional.testtbl having id > 0");
    AnalysisError("select distinct id from functional.testtbl having max(id) > 0",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalyzesOk("select count(distinct id, zip) from functional.testtbl");
    AnalyzesOk("select tinyint_col, count(distinct int_col, bigint_col) "
        + "from functional.alltypesagg group by 1");
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "sum(distinct int_col) from functional.alltypesagg group by 1");
    AnalyzesOk("select avg(DISTINCT(tinyint_col)) from functional.alltypesagg");

    // SUM(DISTINCT) and AVG(DISTINCT) with duplicate grouping exprs (IMPALA-847).
    AnalyzesOk("select sum(distinct t1.bigint_col), avg(distinct t1.bigint_col) " +
        "from functional.alltypes t1 group by t1.int_col, t1.int_col");

    // min and max are ignored in terms of DISTINCT
    AnalyzesOk("select tinyint_col, count(distinct int_col),"
        + "min(distinct smallint_col), max(distinct string_col) "
        + "from functional.alltypesagg group by 1");

    // Test multiple distinct aggregations.
    Table alltypesTbl = catalog_.getOrLoadTable("functional", "alltypes");
    List<String> distinctFns = new ArrayList<>();
    for (Column col : alltypesTbl.getColumns()) {
      distinctFns.add(String.format("count(distinct %s)", col.getName()));
    }
    // Test a single query with a count(distinct) on all columns of alltypesTbl.
    AnalyzesOk(String.format(
        "select %s from functional.alltypes", Joiner.on(",").join(distinctFns)));

    // Test various mixes of distinct and non-distinct, including multiple distinct.
    Set<String> testAggExprs = Sets.newHashSet(
        "count(tinyint_col)",
        "count(string_col)",
        "avg(float_col)",
        "max(string_col)",
        "count(distinct tinyint_col)",
        "count(distinct tinyint_col, smallint_col, int_col)",
        "avg(distinct double_col)",
        "count(distinct string_col)",
        "sum(distinct smallint_col)"
        );
    List<String> testGroupByExprs = Lists.newArrayList(
        "int_col", "bigint_col", "string_col",
        "string_col, date_string_col",
        "id, double_col, date_string_col"
        );
    for (Set<String> aggExprs : Sets.powerSet(testAggExprs)) {
      if (aggExprs.isEmpty()) continue;
      String selectList = Joiner.on(",").join(aggExprs);
      AnalyzesOk(String.format("select %s from functional.alltypes", selectList));
      for (String groupByExprs : testGroupByExprs) {
        AnalyzesOk(String.format(
            "select %s from functional.alltypes group by %s", selectList, groupByExprs));
      }
    }

    // IMPALA-6114: Test that numeric literals having the same value, but different types are
    // considered distinct.
    AnalyzesOk("select distinct cast(0 as decimal(14)), 0 from functional.alltypes");
  }

  @Test
  public void TestSampledNdv() throws AnalysisException {
    Table allScalarTypes = addAllScalarTypesTestTable();
    String tblName = allScalarTypes.getFullName();

    // Positive tests: Test all scalar types and valid sampling percents.
    double validSamplePercs[] = new double[] { 0.0, 0.1, 0.2, 0.5, 0.8, 1.0 };
    for (double perc: validSamplePercs) {
      List<String> allAggFnCalls = new ArrayList<>();
      for (Column col: allScalarTypes.getColumns()) {
        String aggFnCall = String.format("sampled_ndv(%s, %s)", col.getName(), perc);
        allAggFnCalls.add(aggFnCall);
        String stmtSql = String.format("select %s from %s", aggFnCall, tblName);
        SelectStmt stmt = (SelectStmt) AnalyzesOk(stmtSql);
        // Verify that the resolved function signature matches as expected.
        AggregateInfo aggInfo = stmt.getMultiAggInfo().getAggClass(0);
        Type[] args = aggInfo.getAggregateExprs().get(0).getFn().getArgs();
        assertEquals(args.length, 2);
        assertTrue(col.getType().matchesType(args[0]) ||
            col.getType().isStringType() && args[0].equals(Type.STRING));
        assertEquals(Type.DOUBLE, args[1]);
      }
      // Test several calls in the same query block.
      AnalyzesOk(String.format(
          "select %s from %s", Joiner.on(",").join(allAggFnCalls), tblName));
    }

    // Negative tests: Incorrect number of args.
    AnalysisError(
        String.format("select sampled_ndv() from %s", tblName),
        "No matching function with signature: sampled_ndv().");
    AnalysisError(
        String.format("select sampled_ndv(int_col) from %s", tblName),
        "No matching function with signature: sampled_ndv(INT).");
    AnalysisError(
        String.format("select sampled_ndv(int_col, 0.1, 10) from %s", tblName),
        "No matching function with signature: sampled_ndv(INT, DECIMAL(1,1), TINYINT).");

    // Negative tests: Invalid sampling percent.
    String invalidSamplePercs[] = new String[] {
        "int_col", "double_col", "100 / 10", "-0.1", "1.1", "100", "50", "-50", "NULL"
    };
    for (String invalidPerc: invalidSamplePercs) {
      AnalysisError(
          String.format("select sampled_ndv(int_col, %s) from %s", invalidPerc, tblName),
          "Second parameter of SAMPLED_NDV() must be a numeric literal in [0,1]: " +
          invalidPerc);
    }
  }

  @Test
  public void TestGroupConcat() throws AnalysisException {
    // Test valid and invalid parameters
    AnalyzesOk("select group_concat(distinct name) from functional.testtbl");
    AnalysisError("select group_concat(distinct name, name) from functional.testtbl",
        "Second parameter in GROUP_CONCAT(DISTINCT) must be a constant expression" +
        " that returns a string.");
    AnalyzesOk("select group_concat(distinct name, cast(123 as string)) " +
        "from functional.testtbl");
    AnalysisError("select group_concat(distinct name, cast(id as string)) " +
        "from functional.testtbl",
         "Second parameter in GROUP_CONCAT(DISTINCT) must be a constant expression" +
         " that returns a string.");
    AnalyzesOk("select group_concat(distinct string_col, concat('-', '?')) " +
        "from functional.alltypesagg");

    AnalysisError("select group_concat(*) from functional.testtbl",
        "'*' can only be used in conjunction with COUNT");

    // test group_concat using a column as the custom separator
    AnalyzesOk("select group_concat(string_col, string_col) from functional.alltypes");

    // test group_concat without and with distinct
    String[] keywords = new String[] {"distinct", ""};
    for (String keyword: keywords) {
      AnalysisError(String.format("select group_concat(%s '')", keyword), "aggregation" +
          " without a FROM clause is not allowed");
      AnalysisError(String.format("select group_concat(%s name, '-', ',') " +
          "from functional.testtbl", keyword), "No matching function with signature: " +
          "group_concat(STRING, STRING, STRING)");

      // test all types as arguments
      for (Type type: typeToLiteralValue_.keySet()) {
        String literal = typeToLiteralValue_.get(type);
        String query1 = String.format(
            "select group_concat(%s %s) from functional.alltypes", keyword, literal);
        String query2 = String.format(
            "select group_concat(%s %s, '---') from functional.alltypes", keyword,
            literal);
        if (type.getPrimitiveType() == PrimitiveType.STRING || type.isNull()) {
          AnalyzesOk(query1);
          AnalyzesOk(query2);
        } else {
          AnalysisError(query1,
              "No matching function with signature: group_concat(");
          AnalysisError(query2,
              "No matching function with signature: group_concat(");
        }
      }
    }
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
    AnalyzesOk("select * from " +
        "(select count(distinct id, zip), count(distinct zip) " +
        "from functional.testtbl) x");
    AnalyzesOk("select * from " + "(select tinyint_col, count(distinct int_col)," +
        "sum(distinct bigint_col) from functional.alltypesagg group by 1) x");
    AnalyzesOk("select count(distinct id, zip) " +
        "from (select * from functional.testtbl) x");
    AnalyzesOk("select tinyint_col, count(distinct int_col, bigint_col) " +
        "from (select * from functional.alltypesagg) x group by 1");
    AnalyzesOk("select tinyint_col, count(distinct int_col)," +
        "sum(distinct int_col) from " +
        "(select * from functional.alltypesagg) x group by 1");
    AnalyzesOk("select count(distinct id, zip), count(distinct zip) " +
        " from (select * from functional.testtbl) x");
    AnalyzesOk("select tinyint_col, count(distinct int_col)," +
        "sum(distinct bigint_col) from " +
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

    // Error case when inline view is in the from clause
    AnalysisError("select distinct count(*) from (select * from functional.testtbl) x",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip from " +
        "(select * from functional.testtbl) x group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
    AnalysisError("select distinct id, zip, count(*) from " +
        "(select * from functional.testtbl) x group by 1, 2",
        "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
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
    AnalyzesOk("select d1, d2, count(*) from functional.decimal_tbl " +
        "group by 1, 2");
    AnalyzesOk("select date_part, date_col, count(*) from functional.date_tbl " +
        "group by 1, 2");
    AnalyzesOk("select binary_col, count(*) from functional.binary_tbl " +
        "group by binary_col");

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
        "HAVING clause '5 + 10 * 5.6' requires return type 'BOOLEAN'. " +
        "Actual type is 'DECIMAL(7,1)'.");
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
        "GROUP BY: ordinal exceeds the number of items in the SELECT list: 3");
    AnalysisError("select * from functional.alltypes group by 1",
        "cannot combine '*' in select list with grouping or aggregation");
    // picks up select item alias
    AnalyzesOk("select zip z, id iD1, id ID2, count(*) " +
        "from functional.testtbl group by z, ID1, id2");
    // same alias is not ambiguous if it refers to the same expr
    AnalyzesOk("select int_col, INT_COL from functional.alltypes group by int_col");
    AnalyzesOk("select bool_col a, bool_col A from functional.alltypes group by a");
    AnalyzesOk("select int_col A, bool_col b, int_col a, bool_col B " +
        "from functional.alltypes group by a, b");
    // ambiguous alias
    AnalysisError("select zip a, id a, count(*) from functional.testtbl group by a",
        "GROUP BY: ambiguous alias: 'a'");
    AnalysisError("select zip id, id, count(*) from functional.testtbl group by id",
        "GROUP BY: ambiguous alias: 'id'");

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

    // select expression refers to column with same name as its own explicit alias and
    // it's referred to by ordinal in group by (IMPALA-1898)
    // Trivial example
    AnalyzesOk("select bigint_col + 0 AS bigint_col, sum(smallint_col) " +
               "FROM functional.alltypes " +
               "GROUP BY 1");
    // More complex example
    AnalyzesOk("select extract(timestamp_col, 'hour') AS timestamp_col, string_col, " +
               "sum(double_col) AS double_total " +
               "FROM functional.alltypes " +
               "GROUP BY 1, 2");
  }

  @Test
  public void TestOrderBy() throws AnalysisException {
    AnalyzesOk("select zip, id from functional.testtbl order by zip");
    AnalyzesOk("select zip, id from functional.testtbl order by zip asc");
    AnalyzesOk("select zip, id from functional.testtbl order by zip desc");
    AnalyzesOk("select zip, id from functional.testtbl " +
        "order by true asc, false desc, NULL asc");
    AnalyzesOk("select d1, d2 from functional.decimal_tbl order by d1");
    AnalyzesOk("select date_col, date_part from functional.date_tbl order by date_col");
    AnalyzesOk("select string_col from functional.binary_tbl order by binary_col");

    // resolves ordinals
    AnalyzesOk("select zip, id from functional.testtbl order by 1");
    AnalyzesOk("select zip, id from functional.testtbl order by 2 desc, 1 asc");
    // ordinal out of range
    AnalysisError("select zip, id from functional.testtbl order by 0",
        "ORDER BY: ordinal must be >= 1");
    AnalysisError("select zip, id from functional.testtbl order by 3",
        "ORDER BY: ordinal exceeds the number of items in the SELECT list: 3");
    AnalyzesOk("select * from functional.alltypes order by 1");
    // picks up select item alias
    AnalyzesOk("select zip z, id C, id D from functional.testtbl order by z, C, d");

    // can introduce additional aggregates in order by clause
    AnalyzesOk("select zip, count(*) from functional.testtbl group by 1 " +
        " order by count(*)");
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

    // same alias is not ambiguous if it refers to the same expr
    AnalyzesOk("select int_col, INT_COL from functional.alltypes order by int_col");
    AnalyzesOk("select bool_col a, bool_col A from functional.alltypes order by a");
    AnalyzesOk("select int_col A, bool_col b, int_col a, bool_col B " +
        "from functional.alltypes order by a, b");
    // ambiguous alias causes error
    AnalysisError("select string_col a, int_col a from " +
        "functional.alltypessmall order by a limit 1",
        "ORDER BY: ambiguous alias: 'a'");
    AnalysisError("select string_col a, int_col A from " +
        "functional.alltypessmall order by a limit 1",
        "ORDER BY: ambiguous alias: 'a'");

    // Test if an ignored order by produces the expected warning.
    AnalyzesOk("select * from (select * from functional.alltypes order by int_col) A",
        "Ignoring ORDER BY clause without LIMIT or OFFSET: " +
        "ORDER BY int_col ASC");
    AnalyzesOk("select * from functional.alltypes order by int_col desc union all " +
        "select * from functional.alltypes",
        "Ignoring ORDER BY clause without LIMIT or OFFSET: " +
        "ORDER BY int_col DESC");
    AnalyzesOk("insert into functional.alltypes partition (year, month) " +
        "select * from functional.alltypes order by int_col",
        "Ignoring ORDER BY clause without LIMIT or OFFSET: " +
        "ORDER BY int_col ASC");
    AnalyzesOk("create table functional.alltypescopy as " +
        "select * from functional.alltypes order by int_col",
        "Ignoring ORDER BY clause without LIMIT or OFFSET: " +
        "ORDER BY int_col ASC");

    // select expression refers to column with same name as its own explicit alias and
    // it's referred to by ordinal in group by (IMPALA-1898)
    AnalyzesOk("select extract(timestamp_col, 'hour') AS timestamp_col " +
               "FROM functional.alltypes " +
               "ORDER BY timestamp_col");

    // Ordering by complex-typed expressions is not allowed.
    AnalysisError("select * from functional_orc_def.complextypes_structs " +
        "order by tiny_struct", "ORDER BY expression 'tiny_struct' with " +
        "complex type 'STRUCT<b:BOOLEAN>' is not supported.");
    AnalysisError("select * from functional_parquet.allcomplextypes " +
        "order by int_array_col", "ORDER BY expression 'int_array_col' with " +
        "complex type 'ARRAY<INT>' is not supported.");
  }

  @Test
  public void TestSetOperations() {
    // Tests for SetOperationStmt analysis which includes intersect, except and union
    AnalyzesOk("select rank() over (order by int_col) from functional.alltypes " +
        "intersect select int_col from functional.alltypessmall");
    // Selects on same table without aliases.
    AnalyzesOk("select int_col from functional.alltypes intersect " +
        "select int_col from functional.alltypes");
    // Longer union chain.
    AnalyzesOk("select int_col from functional.alltypes union " +
        "select int_col from functional.alltypes " +
        "intersect select int_col from functional.alltypes except " +
        "select int_col from functional.alltypes");

    // Nesting
    AnalyzesOk("(select int_col from functional.alltypes " +
        "intersect (select tinyint_col from functional.alltypessmall union " +
                  " select tinyint_col from functional.alltypessmall) except " +
                  " select tinyint_col from functional.alltypestiny where id = 1) " +
        "union (select tinyint_col from functional.alltypessmall) ");

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

    // With order by, offset and limit.
    AnalyzesOk("(select int_col from functional.alltypes) " +
        "union (select tinyint_col from functional.alltypessmall) " +
        "order by int_col limit 1");
    AnalyzesOk("(select int_col from functional.alltypes) " +
        "union (select tinyint_col from functional.alltypessmall) " +
        "order by int_col");
    AnalyzesOk("(select int_col from functional.alltypes) " +
        "union (select tinyint_col from functional.alltypessmall) " +
        "order by int_col offset 5");
    // Order by w/o limit is ignored in the union operand below.
    AnalyzesOk("select int_col from functional.alltypes order by int_col " +
        "union (select tinyint_col from functional.alltypessmall) ");
    AnalysisError("select int_col from functional.alltypes order by int_col offset 5 " +
        "union (select tinyint_col from functional.alltypessmall) ",
        "Order-by with offset without limit not supported in nested queries");
    AnalysisError("select int_col from functional.alltypes offset 5 " +
        "union (select tinyint_col from functional.alltypessmall) ",
        "OFFSET requires an ORDER BY clause: OFFSET 5");
    // Order by w/o limit is ignored in the union operand below.
    AnalyzesOk("select int_col from functional.alltypes " +
        "union (select tinyint_col from functional.alltypessmall " +
        "order by tinyint_col) ");
    AnalysisError("select int_col from functional.alltypes " +
        "union (select tinyint_col from functional.alltypessmall " +
        "order by tinyint_col offset 5) ",
        "Order-by with offset without limit not supported in nested queries");
    AnalysisError("select int_col from functional.alltypes " +
        "union (select tinyint_col from functional.alltypessmall offset 5) ",
        "OFFSET requires an ORDER BY clause: OFFSET 5");
    // Bigger order by.
    AnalyzesOk("(select tinyint_col, double_col from functional.alltypes) " +
        "union (select smallint_col, float_col from functional.alltypes) " +
        "union (select int_col, bigint_col from functional.alltypes) " +
        "union (select bigint_col, int_col from functional.alltypes) " +
        "order by double_col, tinyint_col");
    // Multiple union operands with valid order by clauses.
    AnalyzesOk("select int_col from functional.alltypes order by int_col " +
        "union select int_col from functional.alltypes order by int_col limit 10 " +
        "union (select int_col from functional.alltypes " +
        "order by int_col limit 10 offset 5) order by int_col offset 5");
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
        "union select lag(string_col) over(order by int_col) from functional.alltypes",
        "Incompatible return types 'BOOLEAN' and 'STRING' of exprs " +
        "'bool_col' and 'lag(string_col, 1, NULL)'.");
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
        "ORDER BY: ordinal exceeds the number of items in the SELECT list: 2");
    // Ambiguous order by.
    AnalysisError("(select int_col a, string_col a from functional.alltypes) " +
        "union (select int_col a, string_col a " +
        "from functional.alltypessmall) order by a",
        "ORDER BY: ambiguous alias: 'a'");
    // Ambiguous alias in the second union operand should work.
    AnalyzesOk("(select int_col a, string_col b from functional.alltypes) " +
        "union (select int_col a, string_col a " +
        "from functional.alltypessmall) order by a");
    // Ambiguous alias even though the exprs of the first operand are identical
    // (the corresponding in exprs in the other operand are different)
    AnalysisError("select int_col a, int_col a from functional.alltypes " +
        "union all (select 1, bigint_col from functional.alltypessmall) order by a",
        "ORDER BY: ambiguous alias: 'a'");

    // Column labels are inherited from first select block.
    // Order by references an invalid column
    AnalysisError("(select smallint_col from functional.alltypes) " +
        "union (select int_col from functional.alltypessmall) order by int_col",
        "Could not resolve column/field reference: 'int_col'");
    // Make sure table aliases aren't visible across union operands.
    AnalysisError("select a.smallint_col from functional.alltypes a " +
        "union select a.int_col from functional.alltypessmall",
        "Could not resolve column/field reference: 'a.int_col'");

    // Regression test for IMPALA-1128, union of decimal and an int type that converts
    // to the identical decimal.
    AnalyzesOk("select cast(1 as bigint) union select cast(1 as decimal(19, 0))");

    AnalysisContext decimalV1Ctx = createAnalysisCtx();
    decimalV1Ctx.getQueryOptions().setDecimal_v2(false);
    AnalysisContext decimalV2Ctx = createAnalysisCtx();
    decimalV2Ctx.getQueryOptions().setDecimal_v2(true);

    // IMPALA-6518: union of two incompatible decimal columns. There is no implicit cast
    // if decimal_v2 is enabled.
    String query = "select cast(123 as decimal(38, 0)) " +
        "union all select cast(0.789 as decimal(38, 38))";
    AnalyzesOk(query, decimalV1Ctx);
    AnalysisError(query, decimalV2Ctx, "Incompatible return types 'DECIMAL(38,0)' and " +
        "'DECIMAL(38,38)' of exprs 'CAST(123 AS DECIMAL(38,0))' and " +
        "'CAST(0.789 AS DECIMAL(38,38))'.");

    query = "select cast(123 as double) " +
        "union all select cast(0.456 as float)" +
        "union all select cast(0.789 as decimal(38, 38))";
    AnalyzesOk(query, decimalV1Ctx);
    AnalyzesOk(query, decimalV2Ctx);
  }


  @Test
  public void TestUnion() {
    // Selects on different tables.
    AnalyzesOk("select rank() over (order by int_col) from functional.alltypes union " +
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

    // With order by, offset and limit.
    AnalyzesOk("(select int_col from functional.alltypes) " +
        "union (select tinyint_col from functional.alltypessmall) " +
        "order by int_col limit 1");
    AnalyzesOk("(select int_col from functional.alltypes) " +
        "union (select tinyint_col from functional.alltypessmall) " +
        "order by int_col");
    AnalyzesOk("(select int_col from functional.alltypes) " +
        "union (select tinyint_col from functional.alltypessmall) " +
        "order by int_col offset 5");
    // Order by w/o limit is ignored in the union operand below.
    AnalyzesOk("select int_col from functional.alltypes order by int_col " +
        "union (select tinyint_col from functional.alltypessmall) ");
    AnalysisError("select int_col from functional.alltypes order by int_col offset 5 " +
        "union (select tinyint_col from functional.alltypessmall) ",
        "Order-by with offset without limit not supported in nested queries");
    AnalysisError("select int_col from functional.alltypes offset 5 " +
        "union (select tinyint_col from functional.alltypessmall) ",
        "OFFSET requires an ORDER BY clause: OFFSET 5");
    // Order by w/o limit is ignored in the union operand below.
    AnalyzesOk("select int_col from functional.alltypes " +
        "union (select tinyint_col from functional.alltypessmall " +
        "order by tinyint_col) ");
    AnalysisError("select int_col from functional.alltypes " +
        "union (select tinyint_col from functional.alltypessmall " +
        "order by tinyint_col offset 5) ",
        "Order-by with offset without limit not supported in nested queries");
    AnalysisError("select int_col from functional.alltypes " +
        "union (select tinyint_col from functional.alltypessmall offset 5) ",
        "OFFSET requires an ORDER BY clause: OFFSET 5");
    // Bigger order by.
    AnalyzesOk("(select tinyint_col, double_col from functional.alltypes) " +
        "union (select smallint_col, float_col from functional.alltypes) " +
        "union (select int_col, bigint_col from functional.alltypes) " +
        "union (select bigint_col, int_col from functional.alltypes) " +
        "order by double_col, tinyint_col");
    // Multiple union operands with valid order by clauses.
    AnalyzesOk("select int_col from functional.alltypes order by int_col " +
        "union select int_col from functional.alltypes order by int_col limit 10 " +
        "union (select int_col from functional.alltypes " +
        "order by int_col limit 10 offset 5) order by int_col offset 5");
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
        "union select lag(string_col) over(order by int_col) from functional.alltypes",
        "Incompatible return types 'BOOLEAN' and 'STRING' of exprs " +
        "'bool_col' and 'lag(string_col, 1, NULL)'.");
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
        "ORDER BY: ordinal exceeds the number of items in the SELECT list: 2");
    // Ambiguous order by.
    AnalysisError("(select int_col a, string_col a from functional.alltypes) " +
        "union (select int_col a, string_col a " +
        "from functional.alltypessmall) order by a",
        "ORDER BY: ambiguous alias: 'a'");
    // Ambiguous alias in the second union operand should work.
    AnalyzesOk("(select int_col a, string_col b from functional.alltypes) " +
        "union (select int_col a, string_col a " +
        "from functional.alltypessmall) order by a");
    // Ambiguous alias even though the exprs of the first operand are identical
    // (the corresponding in exprs in the other operand are different)
    AnalysisError("select int_col a, int_col a from functional.alltypes " +
        "union all (select 1, bigint_col from functional.alltypessmall) order by a",
        "ORDER BY: ambiguous alias: 'a'");

    // Column labels are inherited from first select block.
    // Order by references an invalid column
    AnalysisError("(select smallint_col from functional.alltypes) " +
        "union (select int_col from functional.alltypessmall) order by int_col",
        "Could not resolve column/field reference: 'int_col'");
    // Make sure table aliases aren't visible across union operands.
    AnalysisError("select a.smallint_col from functional.alltypes a " +
        "union select a.int_col from functional.alltypessmall",
        "Could not resolve column/field reference: 'a.int_col'");

    // Regression test for IMPALA-1128, union of decimal and an int type that converts
    // to the identical decimal.
    AnalyzesOk("select cast(1 as bigint) union select cast(1 as decimal(19, 0))");

    AnalysisContext decimalV1Ctx = createAnalysisCtx();
    decimalV1Ctx.getQueryOptions().setDecimal_v2(false);
    AnalysisContext decimalV2Ctx = createAnalysisCtx();
    decimalV2Ctx.getQueryOptions().setDecimal_v2(true);

    // IMPALA-6518: union of two incompatible decimal columns. There is no implicit cast
    // if decimal_v2 is enabled.
    String query = "select cast(123 as decimal(38, 0)) " +
        "union all select cast(0.789 as decimal(38, 38))";
    AnalyzesOk(query, decimalV1Ctx);
    AnalysisError(query, decimalV2Ctx, "Incompatible return types 'DECIMAL(38,0)' and " +
        "'DECIMAL(38,38)' of exprs 'CAST(123 AS DECIMAL(38,0))' and " +
        "'CAST(0.789 AS DECIMAL(38,38))'.");

    query = "select cast(123 as double) " +
        "union all select cast(0.456 as float)" +
        "union all select cast(0.789 as decimal(38, 38))";
    AnalyzesOk(query, decimalV1Ctx);
    AnalyzesOk(query, decimalV2Ctx);
  }

  @Test
  public void TestValuesStmt() throws AnalysisException {
    // Values stmt with a single row.
    AnalyzesOk("values(1, 2, 3)");
    AnalyzesOk("select * from (values('a', NULL, 'c')) as t");
    AnalyzesOk("values(1.0, 2, NULL) union all values(1, 2.0, 3)");
    AnalyzesOk("insert overwrite table functional.alltypes " +
        "partition (year=2009, month=10)" +
        "values(1, true, 1, 1, 1, 1, cast(1.0 as float), cast(1.0 as double), " +
        "'a', 'a', cast(0 as timestamp))");
    AnalyzesOk("insert overwrite table functional.alltypes " +
        "partition (year, month) " +
        "values(1, true, 1, 1, 1, 1, cast(1.0 as float), cast(1.0 as double), " +
        "'a', 'a', cast(0 as timestamp), 2009, 10)");

    assertEquals("SELECT 1", AnalyzesOk("values (1)").toSql(ToSqlOptions.REWRITTEN));

    // Values stmt with multiple rows.
    AnalyzesOk("values((1, 2, 3), (4, 5, 6))");
    AnalyzesOk("select * from (values('a', 'b', 'c')) as t");
    AnalyzesOk("select * from (values(('a', 'b', 'c'), ('d', 'e', 'f'))) as t");
    AnalyzesOk("values((1.0, 2, NULL), (2.0, 3, 4)) union all values(1, 2.0, 3)");
    AnalyzesOk("insert overwrite table functional.alltypes " +
        "partition (year=2009, month=10) " +
        "values(" +
        "(1, true, 1, 1, 1, 1, cast(1.0 as float), cast(1.0 as double), " +
        "'a', 'a', cast(0 as timestamp))," +
        "(2, false, 2, 2, NULL, 2, cast(2.0 as float), cast(2.0 as double), " +
        "'b', 'b', cast(0 as timestamp))," +
        "(3, true, 3, 3, 3, 3, cast(3.0 as float), cast(3.0 as double), " +
        "'c', 'c', cast(0 as timestamp)))");
    AnalyzesOk("insert overwrite table functional.alltypes " +
        "partition (year, month) " +
        "values(" +
        "(1, true, 1, 1, 1, 1, cast(1.0 as float), cast(1.0 as double), " +
        "'a', 'a', cast(0 as timestamp), 2009, 10)," +
        "(2, false, 2, 2, NULL, 2, cast(2.0 as float), cast(2.0 as double), " +
        "'b', 'b', cast(0 as timestamp), 2009, 2)," +
        "(3, true, 3, 3, 3, 3, cast(3.0 as float), cast(3.0 as double), " +
        "'c', 'c', cast(0 as timestamp), 2009, 3))");

    assertEquals("SELECT 1 UNION ALL SELECT 2",
        AnalyzesOk("values (1), (2)").toSql(ToSqlOptions.REWRITTEN));

    // Test multiple aliases. Values() is like union, the column labels are 'x' and 'y'.
    AnalyzesOk("values((1 as x, 'a' as y), (2 as k, 'b' as j))");
    // Test order by, offset and limit.
    AnalyzesOk("values(1 as x, 'a') order by 2 limit 10");
    AnalyzesOk("values(1 as x, 'a' as y), (2, 'b') order by y limit 10");
    AnalyzesOk("values((1, 'a'), (2, 'b')) order by 1 limit 10");
    AnalyzesOk("values((1, 'a'), (2, 'b')) order by 2");
    AnalyzesOk("values((1, 'a'), (2, 'b')) order by 1 offset 5");
    AnalysisError("values((1, 'a'), (2, 'b')) offset 5",
        "OFFSET requires an ORDER BY clause: OFFSET 5");

    AnalysisError("values(1, 'a', 1.0, *)",
        "'*' expression in select list requires FROM clause.");
    AnalysisError("values(sum(1), 'a', 1.0)",
        "aggregation without a FROM clause is not allowed");
    AnalysisError("values(1, id, 2)",
        "Could not resolve column/field reference: 'id'");
    AnalysisError("values((1 as x, 'a' as y), (2, 'b')) order by c limit 1",
        "Could not resolve column/field reference: 'c'");
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
        "Target table 'functional.alltypes' is incompatible with source expressions.\n" +
        "Expression ''a'' (type: STRING) is not compatible with column 'tinyint_col'" +
        " (type: TINYINT)");
    // Regression test for IMPALA-12042: Transitive compatibility is not
    // allowed (boolean -> tinyint -> decimal(4,1))
    AnalysisError("values (true), (123), (111.0)",
        "Incompatible return types 'BOOLEAN' and 'DECIMAL(4,1)'"
            + " of exprs 'TRUE' and '111.0'");
  }

  @Test
  public void TestWithClause() throws AnalysisException {
    // Single view in WITH clause.
    AnalyzesOk("with t as (select int_col x, bigint_col y from functional.alltypes) " +
        "select x, y from t");
    // Single view in WITH clause with column labels.
    AnalyzesOk("with t(c1, c2) as (select int_col x, bigint_col y " +
        "from functional.alltypes) " +
        "select c1, c2 from t");
    // Single view in WITH clause with the number of column labels less than the number
    // of columns.
    AnalyzesOk("with t(c1) as (select int_col, bigint_col y " +
        "from functional.alltypes) " +
        "select c1, y from t");
    // Multiple views in WITH clause. Only one view is used.
    AnalyzesOk("with t1 as (select int_col x, bigint_col y from functional.alltypes), " +
        "t2 as (select 1 x, 10 y), t3 as (values(2 x, 20 y), (3, 30)), " +
        "t4 as (select 4 x, 40 y union all select 5, 50), " +
        "t5 as (select * from (values(6 x, 60 y)) as a) " +
        "select x, y from t3");
    // Multiple views in WITH clause with column labels. Only one view is used.
    AnalyzesOk("with t1(c1, c2) as (select int_col, bigint_col " +
        "from functional.alltypes), " +
        "t2(c1, c2) as (select 1, 10), t3(a, b) as (values(2, 5), (3, 30)), " +
        "t4(c1, c2) as (select 4, 40 union all select 5, 50), " +
        "t5 as (select * from (values(6, 60)) as a) " +
        "select a, b from t3");
    // Multiple views in WITH clause. All views used in a union.
    AnalyzesOk("with t1 as (select int_col x, bigint_col y from functional.alltypes), " +
        "t2 as (select 1 x , 10 y), t3 as (values(2 x , 20 y), (3, 30)), " +
        "t4 as (select 4 x, 40 y union all select 5, 50), " +
        "t5 as (select * from (values(6 x, 60 y)) as a) " +
        "select * from t1 union all select * from t2 union all select * from t3 " +
        "union all select * from t4 union all select * from t5");
    // Multiple views in WITH clause. All views used in a join.
    AnalyzesOk("with t1 as (select int_col x, bigint_col y from functional.alltypes), " +
        "t2 as (select 1 x , 10 y), t3 as (values(2 x , 20 y), (3, 30)), " +
        "t4 as (select 4 x, 40 y union all select 5, 50), " +
        "t5 as (select * from (values(6 x, 60 y)) as a) " +
        "select t1.y, t2.y, t3.y, t4.y, t5.y from t1, t2, t3, t4, t5 " +
        "where t1.y = t2.y and t2.y = t3.y and t3.y = t4.y and t4.y = t5.y");
    // Multiple views in WITH clause with column labels. All views used in a join.
    AnalyzesOk("with t1(c1, c2) as (select int_col x, bigint_col y " +
        "from functional.alltypes), " +
        "t2(c1, c2) as (select 1 x , 10 y), t3 as (values(2 x , 20 y), (3, 30)), " +
        "t4 as (select 4 x, 40 y union all select 5, 50), " +
        "t5 as (select * from (values(6 x, 60 y)) as a) " +
        "select t1.c2, t2.c2, t3.y, t4.y, t5.y from t1, t2, t3, t4, t5 " +
        "where t1.c2 = t2.c2 and t2.c2 = t3.y and t3.y = t4.y and t4.y = t5.y");
    // WITH clause in insert statement.
    AnalyzesOk("with t1 as (select * from functional.alltypestiny)" +
        "insert into functional.alltypes partition(year, month) select * from t1");
    AnalyzesOk("with t1(c1, c2) as (select * from functional.alltypestiny)" +
        "insert into functional.alltypes partition(year, month) select * from t1");
    // WITH clause in insert statement with a select statement that has a WITH
    // clause and an inline view (IMPALA-1100)
    AnalyzesOk("with test_ctas_1 as (select * from functional.alltypestiny) insert " +
        "into functional.alltypes partition (year, month) with with_1 as " +
        "(select t1.* from test_ctas_1 as t1 right join (select 1 as int_col " +
        "from functional.alltypestiny as t1) as t2 ON t2.int_col = t1.int_col) " +
        "select * from with_1 limit 10");
    // Insert with a select statement containing a WITH clause and an inline
    // view
    AnalyzesOk("insert into functional.alltypes partition (year, month) with " +
        "with_1 as (select t1.* from functional.alltypes as t1 right " +
        "join (select * from functional.alltypestiny as t1) t2 on t1.int_col = " +
        "t2.int_col) select * from with_1 limit 10");
    // WITH-clause views belong to different scopes.
    AnalyzesOk("with t1 as (select id from functional.alltypestiny) " +
        "insert into functional.alltypes partition(year, month) " +
        "with t1 as (select * from functional.alltypessmall) select * from t1");
    AnalyzesOk("with t(c1, c2) as (select * from functional.alltypes) " +
        "select a.c1, a.c2 from t a");
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

    // Multiple WITH clauses. One for the SetOperationStmt and one for each union operand.
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
    // Column labels do not conflict because they are in different scopes.
    AnalyzesOk("with t1(c1) as (select 'a') " +
        "select c1 from (with t1(c1) as (select 'b') select c1 from t1) as t2");
    // Table aliases do not conflict because t1 from the inline view is never used.
    AnalyzesOk("with t1 as (select 1), t2 as (select 2)" +
        "select * from functional.alltypes as t1");
    AnalyzesOk("with t1 as (select 1), t2 as (select 2) select * from t2 as t1");
    AnalyzesOk("with t1 as (select 1) select * from (select 2) as t1");
    // Fully-qualified table does not conflict with WITH-clause table.
    AnalyzesOk("with alltypes as (select * from functional.alltypes) " +
        "select * from functional.alltypes union all select * from alltypes");
    // Column labels can be used with table aliases.
    AnalyzesOk("with t(c1) as (select id from functional.alltypes) " +
        "select a.c1 from t a");

    // Use a custom analyzer to change the default db to functional.
    // Recursion is prevented because 'alltypes' in t1 refers to the table
    // functional.alltypes, and 'alltypes' in the final query refers to the
    // view 'alltypes'.
    AnalyzesOk("with t1 as (select int_col x, bigint_col y from alltypes), " +
        "alltypes as (select x a, y b from t1)" +
        "select a, b from alltypes",
        createAnalysisCtx("functional"));
    // Recursion is prevented because of scoping rules. The inner 'complex_view'
    // refers to a view in the catalog.
    AnalyzesOk("with t1 as (select abc x, xyz y from complex_view), " +
        "complex_view as (select x a, y b from t1)" +
        "select a, b from complex_view",
        createAnalysisCtx("functional"));
    // Nested WITH clauses. Scoping prevents recursion.
    AnalyzesOk("with t1 as (with t1 as (select int_col x, bigint_col y from alltypes) " +
        "select x, y from t1), " +
        "alltypes as (select x a, y b from t1) " +
        "select a, b from alltypes",
        createAnalysisCtx("functional"));
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

    // Test analysis of WITH clause after subquery rewrite does not pollute
    // global state (IMPALA-1357).
    AnalyzesOk("select 1 from (with w as (select 1 from functional.alltypes " +
        "where exists (select 1 from functional.alltypes)) select 1 from w) tt");
    AnalyzesOk("create table test_with as select 1 from (with w as " +
        "(select 1 from functional.alltypes where exists " +
        "(select 1 from functional.alltypes)) select 1 from w) tt");
    AnalyzesOk("insert into functional.alltypesnopart (id) select 1 from " +
        "(with w as (select 1 from functional.alltypes where exists " +
        "(select 1 from functional.alltypes)) select 1 from w) tt");

    // Conflicting table aliases in WITH clause.
    AnalysisError("with t1 as (select 1), t1 as (select 2) select * from t1",
        "Duplicate table alias: 't1'");
    // Check that aliases from WITH-clause views conflict with other table aliases.
    AnalysisError("with t1 as (select 1 x), t2 as (select 2 y)" +
        "select * from functional.alltypes as t1 inner join t1",
        "Duplicate table alias: 't1'");
    AnalysisError("with t1 as (select 1), t2 as (select 2) " +
        "select * from t2 as t1 inner join t1",
        "Duplicate table alias: 't1'");
    AnalysisError("with t1 as (select 1) select * from (select 2) as t1 inner join t1",
        "Duplicate table alias: 't1'");
    // With clause column labels must be used intead of aliases.
    AnalysisError("with t1(c1) as (select id cnt from functional.alltypes) "+
        "select cnt from t1",
        "Could not resolve column/field reference: 'cnt'");
    // With clause column labels must not exceed the number of columns in the query.
    AnalysisError("with t(c1, c2) as (select id from functional.alltypes) " +
        "select * from t",
        "WITH-clause view 't' returns 1 columns, but 2 labels were specified. The " +
        "number of column labels must be smaller or equal to the number of returned " +
        "columns.");
    // Multiple references in same select statement require aliases.
    AnalysisError("with t1 as (select 'a' x) select * from t1 inner join t1",
        "Duplicate table alias: 't1'");
    // If one was given, we must use the explicit alias for column references.
    AnalysisError("with t1 as (select 'a' x) select t1.x from t1 as t2",
        "Could not resolve column/field reference: 't1.x'");
    // WITH-clause tables cannot be inserted into.
    AnalysisError("with t1 as (select 'a' x) insert into t1 values('b' x)",
        "Table does not exist: default.t1");

    // The inner alltypes_view gets resolved to the catalog view.
    AnalyzesOk("with alltypes_view as (select int_col x from alltypes_view) " +
        "select x from alltypes_view",
        createAnalysisCtx("functional"));
    // The inner 't' is resolved to a non-existent base table.
    AnalysisError("with t as (select int_col x, bigint_col y from t1) " +
        "select x, y from t",
        "Could not resolve table reference: 't1'");
    AnalysisError("with t as (select 1 as x, 2 as y union all select * from t) " +
        "select x, y from t",
        "Could not resolve table reference: 't'");
    AnalysisError("with t as (select a.* from (select * from t) as a) " +
        "select x, y from t",
        "Could not resolve table reference: 't'");
    // The inner 't1' in a nested WITH clause gets resolved to a non-existent base table.
    AnalysisError("with t1 as (with t2 as (select * from t1) select * from t2) " +
        "select * from t1 ",
        "Could not resolve table reference: 't1'");
    AnalysisError("with t1 as " +
        "(select * from (with t2 as (select * from t1) select * from t2) t3) " +
        "select * from t1",
        "Could not resolve table reference: 't1'");
    // The inner 't1' in the gets resolved to a non-existent base table.
    AnalysisError("with t1 as " +
        "(with t2 as (select * from t1) select * from t2 union all select * from t2)" +
        "select * from t1",
        "Could not resolve table reference: 't1'");
    AnalysisError("with t1 as " +
        "(select 'x', 'y' union all (with t2 as (select * from t1) select * from t2))" +
        "select * from t1",
        "Could not resolve table reference: 't1'");
    // The 't2' inside 't1's definition gets resolved to a non-existent base table.
    AnalysisError("with t1 as (select int_col x, bigint_col y from t2), " +
        "t2 as (select int_col x, bigint_col y from t1) select x, y from t1",
        "Could not resolve table reference: 't2'");

    // WITH clause with subqueries
    AnalyzesOk("with t as (select * from functional.alltypesagg where id in " +
        "(select id from functional.alltypes)) select int_col from t");
    AnalyzesOk("with t as (select * from functional.alltypes) select * from " +
        "functional.alltypesagg a where exists (select id from t where t.id = a.id)");
    AnalyzesOk("with t as (select * from functional.alltypes) select * from " +
        "functional.alltypesagg where 10 > (select count(*) from t) and " +
        "100 < (select max(int_col) from t)");
    AnalyzesOk("with t as (select * from functional.alltypes a where exists " +
        "(select * from functional.alltypesagg t where t.id = 1 and a.id = t.id) " +
        "and not exists (select * from functional.alltypesagg b where b.id = 1 " +
        "and b.int_col = a.int_col)) select * from t");

    // WITH clause with a collection table ref.
    AnalyzesOk(
        "with w as (select t.id, a.item from functional.allcomplextypes t, " +
        "t.int_array_col a) select * from w");

    // Deeply nested WITH clauses (see IMPALA-1106)
    AnalyzesOk("with with_1 as (select 1 as int_col_1), with_2 as " +
        "(select 1 as int_col_1 from (with with_3 as (select 1 as int_col_1 from " +
        "with_1) select 1 as int_col_1 from with_3) as t1) select 1 as int_col_1 " +
        "from with_2");
    AnalyzesOk("with with_1 as (select 1 as int_col_1), with_2 as (select 1 as " +
        "int_col_1 from (with with_3 as (select 1 as int_col_1 from with_1) " +
        "select 1 as int_col_1 from with_3) as t1), with_4 as (select 1 as " +
        "int_col_1 from with_2) select 1 as int_col_1 from with_4");
    AnalyzesOk("with with_1 as (select 1 as int_col_1), with_2 as (with with_3 " +
        "as (select 1 as int_col_1 from (with with_4 as (select 1 as int_col_1 " +
        "from with_1) select 1 as int_col_1 from with_4) as t1) select 1 as " +
        "int_col_1 from with_3) select 1 as int_col_1 from with_2");

    // WITH clause with a between predicate
    AnalyzesOk("with with_1 as (select int_col from functional.alltypestiny " +
        "where int_col between 0 and 10) select * from with_1");
    // WITH clause with a between predicate in the select list
    AnalyzesOk("with with_1 as (select int_col between 0 and 10 " +
        "from functional.alltypestiny) select * from with_1");
    // WITH clause with a between predicate in the select list that
    // uses casting
    AnalyzesOk("with with_1 as (select timestamp_col between " +
        "cast('2001-01-01' as timestamp) and " +
        "(cast('2001-01-01' as timestamp) + interval 10 days) " +
        "from functional.alltypestiny) select * from with_1");
    // WITH clause with a between predicate that uses explicit casting
    AnalyzesOk("with with_1 as (select * from functional.alltypestiny " +
        "where timestamp_col between cast('2001-01-01' as timestamp) and " +
        "(cast('2001-01-01' as timestamp) + interval 10 days)) " +
        "select * from with_1");
    AnalyzesOk("with with_1 as (select 1 as col_name), " +
        "with_2 as (select 1 as col_name) " +
        "select a.tinyint_col from functional.alltypes a " +
        "where not exists (select 1 from with_1) ");
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
        "Impala does not support INSERTing into views: functional.alltypes_view");
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
        "Could not resolve column/field reference: 'int_col'");
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
          "contain non-hidden subdirectories.");

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

      // Source must be HDFS or S3A.
      AnalysisError(String.format("load data inpath '%s' %s into table " +
          "tpch.lineitem", "file:///test-warehouse/test.out", overwrite),
          "INPATH location 'file:/test-warehouse/test.out' must point to one of the " +
          "supported filesystem URI scheme (" +
          FileSystemUtil.getValidLoadDataInpathSchemes() + ").");

      // File type / table type mismatch.
      AnalyzesOk(String.format("load data inpath '%s' %s into table " +
          "tpch.lineitem",
          "/test-warehouse/alltypes_text_gzip/year=2009/month=4", overwrite));
      // When table type matches, analysis passes for partitioned and unpartitioned
      // tables.
      AnalyzesOk(String.format("load data inpath '%s' %s into table " +
          "functional_text_gzip.alltypes partition(year=2009, month=4)",
          "/test-warehouse/alltypes_text_gzip/year=2009/month=4", overwrite));
      AnalyzesOk(String.format("load data inpath '%s' %s into table " +
          "functional_text_gzip.jointbl",
          "/test-warehouse/alltypes_text_gzip/year=2009/month=4", overwrite));

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
  public void TestIcebergLoadData() throws AnalysisException {
    AnalyzesOk("load data inpath "
        + "'/test-warehouse/iceberg_test/iceberg_non_partitioned/data' into table "
        + "functional_parquet.iceberg_non_partitioned");
    AnalyzesOk("load data inpath "
        + "'/test-warehouse/iceberg_test/iceberg_non_partitioned/data' overwrite into "
        + "table functional_parquet.iceberg_non_partitioned");
    AnalysisError("load data inpath "
        + "'/test-warehouse/iceberg_test/iceberg_partitioned/data/"
        + "event_time_hour=2020-01-01-08/action=view/' into table "
        + "functional_parquet.iceberg_partitioned partition "
        + "(event_time_hour='2020-01-01-08', action='view');", "PARTITION clause is not "
        + "supported for Iceberg tables.");
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
        "does not have WRITE access to HDFS location: " +
        "hdfs://localhost:20500/test-warehouse/alltypes_seq");

    // Insert with a correlated inline view.
    AnalyzesOk("insert into table functional.alltypessmall " +
        "partition (year, month)" +
        "select a.id, bool_col, tinyint_col, smallint_col, item, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, a.year, " +
        "b.month from functional.alltypes a, functional.allcomplextypes b, " +
        "(select item from b.int_array_col) v1 " +
        "where a.id = b.id");
    AnalysisError("insert into table functional.alltypessmall " +
        "partition (year, month)" +
        "select a.id, a.bool_col, a.tinyint_col, a.smallint_col, item, a.bigint_col, " +
        "a.float_col, a.double_col, a.date_string_col, a.string_col, a.timestamp_col, " +
        "a.year, b.month from functional.alltypes a, functional.allcomplextypes b, " +
        "(select item from b.int_array_col, functional.alltypestiny) v1 " +
        "where a.id = b.id",
        "Nested query is illegal because it contains a table reference " +
        "'b.int_array_col' correlated with an outer block as well as an " +
        "uncorrelated one 'functional.alltypestiny':\n" +
        "SELECT item FROM b.int_array_col, functional.alltypestiny");

    // Key columns missing from permutation
    AnalysisError("insert into functional_kudu.testtbl(zip) values(1)",
        "All primary key columns must be specified for INSERTing into Kudu tables. " +
        "Missing columns are: id");
    // Mixed column name case, on both primary key and non-primary key cols.
    AnalyzesOk("insert into functional_kudu.alltypes (ID, BOOL_COL) values (0, true)");

    addTestDb("d", null);
    addTestTable("create table d.dec1 (c decimal(38,37)) location '/'");
    addTestTable("create table d.dec2 (c decimal(38,1)) location '/'");
    addTestTable("create table d.dbl (c double) location '/'");
    addTestTable("create table d.flt (c float) location '/'");

    AnalysisContext decimalV1Ctx = createAnalysisCtx("d");
    decimalV1Ctx.getQueryOptions().setDecimal_v2(false);
    AnalysisContext decimalV2Ctx = createAnalysisCtx("d");
    decimalV2Ctx.getQueryOptions().setDecimal_v2(true);

    AnalyzesOk("insert into d.dec1 select cast(1 as decimal(38, 0))", decimalV1Ctx);
    AnalysisError("insert into d.dec1 select cast(1 as decimal(38, 0))", decimalV2Ctx,
        "Target table 'd.dec1' is incompatible with source expressions.\n" +
        "Expression 'CAST(1 AS DECIMAL(38,0))' (type: DECIMAL(38,0)) is not " +
        "compatible with column 'c' (type: DECIMAL(38,37))");

    AnalysisError("insert into d.dec2 select cast(11.1 as decimal(38, 20));",
        decimalV1Ctx, "Possible loss of precision for target table 'd.dec2'.\n" +
        "Expression 'CAST(11.1 AS DECIMAL(38,20))' (type: DECIMAL(38,20)) would need " +
        "to be cast to DECIMAL(38,1) for column 'c'");
    AnalysisError("insert into d.dec2 select cast(11.1 as decimal(38, 20));",
        decimalV2Ctx, "Target table 'd.dec2' is incompatible with source expressions.\n" +
        "Expression 'CAST(11.1 AS DECIMAL(38,20))' (type: DECIMAL(38,20)) is not " +
        "compatible with column 'c' (type: DECIMAL(38,1))");

    AnalysisError("insert into d.dec1 select cast(1 as double)", decimalV1Ctx,
        "Possible loss of precision for target table 'd.dec1'.\n" +
        "Expression 'CAST(1 AS DOUBLE)' (type: DOUBLE) would need to be cast to " +
        "DECIMAL(38,37) for column 'c'");
    AnalysisError("insert into d.dec1 select cast(1 as double)", decimalV2Ctx,
        "Possible loss of precision for target table 'd.dec1'.\n" +
        "Expression 'CAST(1 AS DOUBLE)' (type: DOUBLE) would need to be cast to " +
        "DECIMAL(38,37) for column 'c'");

    AnalysisError("insert into d.dec1 select cast(1 as float)", decimalV1Ctx,
        "Possible loss of precision for target table 'd.dec1'.\n" +
            "Expression 'CAST(1 AS FLOAT)' (type: FLOAT) would need to be cast to " +
            "DECIMAL(38,37) for column 'c'");
    AnalysisError("insert into d.dec1 select cast(1 as float)", decimalV2Ctx,
        "Possible loss of precision for target table 'd.dec1'.\n" +
            "Expression 'CAST(1 AS FLOAT)' (type: FLOAT) would need to be cast to " +
            "DECIMAL(38,37) for column 'c'");

    AnalyzesOk("insert into d.dbl select cast(1 as decimal(20, 10))", decimalV1Ctx);
    AnalyzesOk("insert into d.dbl select cast(1 as decimal(20, 10))", decimalV2Ctx);

    AnalyzesOk("insert into d.flt select cast(1 as decimal(20, 10))", decimalV1Ctx);
    AnalyzesOk("insert into d.flt select cast(1 as decimal(20, 10))", decimalV2Ctx);

    // IMPALA-966: Test insertion of incompatible expressions. Error should blame the
    // first widest (highest precision) incompatible type expression.
    // Test insert multiple values with compatible and incompatible types into a column
    String query = "insert into functional.testtbl (id) "
        + "values (10), (cast(1 as float)), (cast(3 as double))";
    AnalysisError(query,
        "Possible loss of precision "
            + "for target table 'functional.testtbl'.\n"
            + "Expression 'CAST(3 AS DOUBLE)' (type: DOUBLE) "
            + "would need to be cast to BIGINT for column 'id'");
    // Test insert multiple values with the same incompatible type into a column
    query = "insert into functional.testtbl (id) "
        + "values (cast(1 as float)), (cast(2 as float)), (cast(3 as float))";
    AnalysisError(query,
        "Possible loss of precision "
            + "for target table 'functional.testtbl'.\n"
            + "Expression 'CAST(1 AS FLOAT)' (type: FLOAT) "
            + "would need to be cast to BIGINT for column 'id'");
    // Test insert unions of multiple compatible and incompatible types expressions
    // into multiple columns
    query = "insert into functional.alltypes (int_col, float_col) "
        + "partition(year=2019, month=4) "
        + "(select int_col, float_col from functional.alltypes union "
        + "select float_col, double_col from functional.alltypes union "
        + "select double_col, int_col from functional.alltypes)";
    AnalysisError(query,
        "Possible loss of precision "
            + "for target table 'functional.alltypes'.\n"
            + "Expression 'double_col' (type: DOUBLE) "
            + "would need to be cast to INT for column 'int_col'");
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
    // Non-const partition value
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=rank() over(order by int_col), month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, month " +
        "from functional.alltypes",
        "Non-constant expressions are not supported as static partition-key values " +
        "in 'year=rank() OVER (ORDER BY int_col ASC)'");

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

    // Mixed column name case in the partition clause.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (YEAR, month)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col, year, " +
        "month from functional.alltypes");

    // Insert with dynamic partitioning clause into a table that has unsupported partition
    AnalysisError("insert into table functional.multipartformat partition (p) " +
       "select 'parquet', 1", "Destination table 'functional.multipartformat' " +
       "contains partition format(s) that are not supported to write: 'ORC', " +
       "dynamic partition clauses are forbidden.");
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
        "functional_hbase.insertalltypesagg select id, bigint_col, " +
        "cast(string_col as binary), bool_col, " +
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
        "from functional.alltypesnopart",
        "Database does not exist: UNKNOWNDB");
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
        "Target table 'functional.alltypessmall' is incompatible with source " +
        "expressions.\nExpression 'bool_col' (type: BOOLEAN) is not compatible with " +
        "column 'string_col' (type: STRING)");
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
        "Target table 'functional.alltypessmall' is incompatible with source " +
        "expressions.\nExpression ''should be an int'' (type: STRING) is not compatible" +
        " with column 'year' (type: INT)");
    // Arbitrary exprs as partition key values. Non-constant exprs should fail.
    AnalysisError("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=-1, month=int_col)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Non-constant expressions are not supported as static partition-key values " +
        "in 'month=int_col'.");
    // Mixed column case in the partition clause.
    AnalyzesOk("insert " + qualifier + " table functional.alltypessmall " +
        "partition (year=2009, MONTH=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");

    // Insert with static partitioning clause into a partition that has unsupported
    // partition format.
    AnalysisError("insert into table functional.multipartformat partition (p='orc') " +
        "select 1", "Writing the destination partition format 'ORC' is not supported.");

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
        "Target table 'functional.alltypesnopart' is incompatible with source " +
        "expressions.\nExpression 'functional.alltypesnopart.tinyint_col' " +
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
        "Target table 'functional.alltypes' is incompatible with source expressions.\n" +
        "Expression '`month`' (type: INT) is not compatible with column 'string_col'" +
        " (type: STRING)");

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

    // Mixed column case in permutation, both partition and non-partition cols.
    AnalyzesOk("insert " + qualifier + " table functional.alltypes (ID, YEAR, month)" +
        "values (0, 0, 0)");

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
      // Mixed column case on both row-key and non-row-key cols.
      AnalyzesOk("insert " + qualifier + " table functional_hbase.alltypesagg" +
          "(ID, bool_col, tinyint_col, smallint_col, INT_COL, bigint_col, " +
          "float_col, double_col, date_string_col, string_col, timestamp_col) " +
          "select * from functional.alltypesnopart");
    }

    // Insert into/overwrite a table with complex columns should return error.
    AnalysisError("insert " + qualifier + " table functional.allcomplextypes" +
        "(id, year, month) values (57, 2019, 11)",
        "Unable to INSERT into target table (functional.allcomplextypes) because the " +
        "column 'int_array_col' has a complex type 'ARRAY<INT>' and Impala doesn't " +
        "support inserting into tables containing complex type columns");
    AnalysisError("insert " + qualifier + " table functional.allcomplextypes " +
        "select * from functional.allcomplextypes",
        "Unable to INSERT into target table (functional.allcomplextypes) because the " +
        "column 'int_array_col' has a complex type 'ARRAY<INT>' and Impala doesn't " +
        "support inserting into tables containing complex type columns");
  }

  /**
   * Simple test that checks the number of members of statements and table refs
   * against a fixed expected value. The intention is alarm developers to
   * properly change the clone() method when adding members to statements.
   * Once the clone() method has been appropriately changed, the expected
   * number of members should be updated to make the test pass.
   */
  @Test
  public void TestClone() {
    testNumberOfMembers(QueryStmt.class, 11);
    testNumberOfMembers(SetOperationStmt.class, 13);
    testNumberOfMembers(ValuesStmt.class, 0);

    // Also check TableRefs.
    testNumberOfMembers(TableRef.class, 30);
    testNumberOfMembers(BaseTableRef.class, 0);
    testNumberOfMembers(InlineViewRef.class, 10);
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

  @Test
  public void TestSetQueryOption() {
    AnalyzesOk("set foo=true");
    AnalyzesOk("set");
    AnalyzesOk("unset all");
  }

  @Test
  public void TestFunctionPaths() throws ImpalaException {
    // The statement here does not matter since we just need to get a dummy analyzer
    // to be able to call FuntionName.analyze(Analyzer, boolean) method.
    Analyzer dummyAnalyzer = ((StatementBase) AnalyzesOk("select 1")).getAnalyzer();
    FunctionName fnName = new FunctionName(null, "sin");
    fnName.analyze(dummyAnalyzer, false);
    assertFalse(fnName.isBuiltin());

    fnName = new FunctionName(null, "f");
    fnName.analyze(dummyAnalyzer, false);
    assertFalse(fnName.isBuiltin());

    fnName = new FunctionName("db", "sin");
    fnName.analyze(dummyAnalyzer, false);
    assertFalse(fnName.isBuiltin());

    fnName = new FunctionName("db", "f");
    fnName.analyze(dummyAnalyzer, false);
    assertFalse(fnName.isBuiltin());

    fnName = new FunctionName("_impala_builtins", "sin");
    fnName.analyze(dummyAnalyzer, false);
    assertTrue(fnName.isBuiltin());

    fnName = new FunctionName("_impala_builtins", "f");
    fnName.analyze(dummyAnalyzer, false);
    assertFalse(fnName.isBuiltin());

    fnName = new FunctionName(null, "sin");
    fnName.analyze(dummyAnalyzer, true);
    assertTrue(fnName.isBuiltin());

    fnName = new FunctionName(null, "f");
    fnName.analyze(dummyAnalyzer, true);
    assertFalse(fnName.isBuiltin());

    fnName = new FunctionName("db", "sin");
    fnName.analyze(dummyAnalyzer, false);
    assertFalse(fnName.isBuiltin());

    fnName = new FunctionName("db", "f");
    fnName.analyze(dummyAnalyzer, true);
    assertFalse(fnName.isBuiltin());

    fnName = new FunctionName("_impala_builtins", "sin");
    fnName.analyze(dummyAnalyzer, false);
    assertTrue(fnName.isBuiltin());

    fnName = new FunctionName("_impala_builtins", "f");
    fnName.analyze(dummyAnalyzer, false);
    assertFalse(fnName.isBuiltin());
  }

  @Test
  public void TestAdminFns() throws ImpalaException {
    AnalyzesOk(": shutdown()");
    AnalyzesOk(":sHuTdoWn()");
    AnalyzesOk(":   SHUTDOWN()");
    AnalyzesOk(": sHuTdoWn('hostname')");
    AnalyzesOk(": sHuTdoWn(\"hostname\")");
    AnalyzesOk(": sHuTdoWn(\"hostname:1234\")");
    AnalyzesOk(": shutdown(10)");
    AnalyzesOk(": shutdown('hostname', 10)");
    AnalyzesOk(": shutdown('hostname:11', 10)");
    AnalyzesOk(": shutdown('hostname:11', 10 * 60)");
    AnalyzesOk(": shutdown(10 * 60)");
    AnalyzesOk(": shutdown(0)");

    // Unknown admin functions.
    AnalysisError(": foobar()", "Unknown admin function: foobar");
    AnalysisError(": 1a()", "Unknown admin function: 1a");
    AnalysisError(": foobar(1,2,3)", "Unknown admin function: foobar");

    // Invalid number of shutdown params.
    AnalysisError(": shutdown('a', 'b', 'c', 'd')",
        "Shutdown takes 0, 1 or 2 arguments: :shutdown('a', 'b', 'c', 'd')");
    AnalysisError(": shutdown(1, 2, 3)",
        "Shutdown takes 0, 1 or 2 arguments: :shutdown(1, 2, 3)");

    // Invalid type of shutdown params.
    AnalysisError(": shutdown(a)",
        "Could not resolve column/field reference: 'a'");
    AnalysisError(": shutdown(1, 2)",
        "Invalid backend, must be a string literal: 1");
    AnalysisError(": shutdown(concat('host:', '1234'), 2)",
        "Invalid backend, must be a string literal: concat('host:', '1234')");
    AnalysisError(": shutdown('backend:1234', '...')",
        "deadline expression must be an integer type but is 'STRING': '...'");
    AnalysisError(": shutdown(true)",
        "deadline expression must be an integer type but is 'BOOLEAN': TRUE");

    // Invalid host/port.
    AnalysisError(": shutdown('foo:bar')",
        "Invalid port number in backend address: foo:bar");
    AnalysisError(": shutdown('foo:bar:1234')",
        "Invalid backend address: foo:bar:1234");

    // Invalid deadline value.
    AnalysisError(": shutdown(-1)", "deadline must be a non-negative integer: -1 = -1");
    AnalysisError(": shutdown(1.234)",
        "deadline expression must be an integer type but is 'DECIMAL(4,3)': 1.234");
  }

  @Test
  public void TestImpalaBuiltinCastFunctions() throws ImpalaException {
    // Other builtins work
    AnalyzesOk("select random(1000)");
    // Builtin cast functions throw exception.
    String expectedErrorSuffix =
        " is reserved for internal use only. Use 'cast(expr AS type)' instead.";
    List<Function> fns = BuiltinsDb.getInstance().getFunctions(TFunctionCategory.SCALAR,
        "castto");
    for (Function fn : fns) {
      // The analysis throws an exception on the function name before it even gets to
      // checking the argument types, so it doesnt matter what argument we pass
      // to it.
      String fn_sql_str = fn.getName() + "(\"foo\")";
      AnalysisError("select " + fn_sql_str, fn_sql_str + expectedErrorSuffix);
      AnalysisError("select _impala_builtins." + fn_sql_str, fn_sql_str +
          expectedErrorSuffix);
    }
    // Function that starts with 'castto' but does not exist should throw the
    // right error msg.
    AnalysisError("select casttobar(\"foo\")", "default.casttobar() unknown");
  }

  @Test
  public void testCompoundVerticalBarExprAnalysis() throws ImpalaException {
    // Valid uses of || when using as logical OR
    AnalyzesOk("SELECT bool_col || bool_col FROM functional.alltypessmall");
    AnalyzesOk("SELECT TRUE || bool_col FROM functional.alltypessmall");
    AnalyzesOk("SELECT bool_col || FALSE FROM functional.alltypessmall");
    AnalyzesOk("SELECT TRUE || FALSE FROM functional.alltypessmall");
    AnalyzesOk("SELECT NULL || bool_col FROM functional.alltypessmall");
    AnalyzesOk("SELECT NULL || NULL FROM functional.alltypessmall");

    // Valid uses of || when using as concat function call
    AnalyzesOk("SELECT string_col || string_col FROM functional.alltypessmall");
    AnalyzesOk("SELECT 'literal' || string_col FROM functional.alltypessmall");
    AnalyzesOk("SELECT string_col || 'literal' FROM functional.alltypessmall");
    AnalyzesOk("SELECT 'literal1' || 'literal2' FROM functional.alltypessmall");
    AnalyzesOk("SELECT 'literal' || cs FROM functional.chars_tiny");
    AnalyzesOk("SELECT cl || 'literal' FROM functional.chars_tiny");
    AnalyzesOk("SELECT vc || cs FROM functional.chars_tiny");

    // Invalid operands result exception during analysis
    AnalysisError("SELECT string_col || bool_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'string_col || bool_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'STRING' and 'BOOLEAN'.");

    AnalysisError("SELECT bool_col || string_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'bool_col || string_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'BOOLEAN' and 'STRING'.");

    AnalysisError("SELECT string_col || FALSE FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'string_col || FALSE' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'STRING' and 'BOOLEAN'.");

    AnalysisError("SELECT TRUE || string_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'TRUE || string_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'BOOLEAN' and 'STRING'.");

    AnalysisError("SELECT 'literal1' || bool_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "''literal1' || bool_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'STRING' and 'BOOLEAN'.");

    AnalysisError("SELECT bool_col || 'literal2' FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'bool_col || 'literal2'' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'BOOLEAN' and 'STRING'.");

    AnalysisError("SELECT vc || TRUE FROM functional.chars_tiny",
        "Operands of CompoundVerticalBarExpr "
            + "'vc || TRUE' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'VARCHAR(32)' and 'BOOLEAN'.");

    AnalysisError("SELECT FALSE || cl FROM functional.chars_tiny",
        "Operands of CompoundVerticalBarExpr "
            + "'FALSE || cl' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'BOOLEAN' and 'CHAR(140)'.");

    AnalysisError("SELECT bool_col || 'literal2' FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'bool_col || 'literal2'' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'BOOLEAN' and 'STRING'.");

    AnalysisError("SELECT string_col || int_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'string_col || int_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'STRING' and 'INT'.");

    AnalysisError("SELECT int_col || string_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'int_col || string_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'INT' and 'STRING'.");

    AnalysisError("SELECT bool_col || int_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'bool_col || int_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'BOOLEAN' and 'INT'.");

    AnalysisError("SELECT int_col || bool_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'int_col || bool_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'INT' and 'BOOLEAN'.");

    AnalysisError("SELECT string_col || timestamp_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'string_col || timestamp_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'STRING' and 'TIMESTAMP'.");

    AnalysisError("SELECT timestamp_col || string_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'timestamp_col || string_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'TIMESTAMP' and 'STRING'.");

    AnalysisError("SELECT bool_col || timestamp_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'bool_col || timestamp_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'BOOLEAN' and 'TIMESTAMP'.");

    AnalysisError("SELECT timestamp_col || bool_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'timestamp_col || bool_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'TIMESTAMP' and 'BOOLEAN'.");

    AnalysisError("SELECT bool_col || double_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'bool_col || double_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'BOOLEAN' and 'DOUBLE'.");

    AnalysisError("SELECT double_col || string_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'double_col || string_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'DOUBLE' and 'STRING'.");

    AnalysisError("SELECT string_col || NULL FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'string_col || NULL' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'STRING' and 'NULL_TYPE'.");

    AnalysisError("SELECT NULL || string_col FROM functional.alltypessmall",
        "Operands of CompoundVerticalBarExpr "
            + "'NULL || string_col' should both return 'BOOLEAN' type "
            + "or they should both return 'STRING' or 'VARCHAR' or 'CHAR' types, "
            + "but they return types 'NULL_TYPE' and 'STRING'.");
  }

  @Test
  public void testIcebergTimeTravel() throws ImpalaException {
    TableName iceT = new TableName("functional_parquet", "iceberg_non_partitioned");
    TableName nonIceT = new TableName("functional", "allcomplextypes");

    TblsAnalyzeOk("select * from $TBL for system_time as of now()", iceT);
    TblsAnalyzeOk("select * from $TBL for system_time as of '2021-08-09 15:52:45'", iceT);
    TblsAnalyzeOk("select * from $TBL for system_time as of " +
        "cast('2021-08-09 15:52:45' as timestamp) - interval 2 days + interval 3 hours",
        iceT);
    TblsAnalyzeOk("select * from $TBL for system_time as of now() + interval 3 days",
        iceT);
    // Use a legal snapshot id '93996984692289973' from the testdata.
    TblsAnalyzeOk("select * from $TBL for system_version as of 93996984692289973", iceT);

    TblsAnalysisError("select * from $TBL for system_time as of 42", iceT,
        "FOR SYSTEM_TIME AS OF <expression> must be a timestamp type");
    TblsAnalysisError("select * from $TBL for system_time as of id", iceT,
        "FOR SYSTEM_TIME AS OF <expression> must be a constant expression");
    TblsAnalysisError("select * from $TBL for system_time as of '2021-02-32 15:52:45'",
        iceT, "Invalid TIMESTAMP expression");

    TblsAnalysisError("select * from $TBL for system_version as of 3.14",
        iceT, "FOR SYSTEM_VERSION AS OF <expression> must be an integer type but is");

    TblsAnalysisError("select * from $TBL for system_time as of now()", nonIceT,
        "FOR SYSTEM_TIME AS OF clause is only supported for Iceberg tables.");
    TblsAnalysisError("select * from $TBL for system_version as of 123", nonIceT,
        "FOR SYSTEM_VERSION AS OF clause is only supported for Iceberg tables.");
  }

  @Test
  public void testIcebergDescribeHistory() throws ImpalaException {
    TableName iceT = new TableName("functional_parquet", "iceberg_non_partitioned");
    TableName nonIceT = new TableName("functional", "allcomplextypes");

    // Analyze without predicate.
    TblsAnalyzeOk("DESCRIBE HISTORY $TBL", iceT);

    // Analyze with predicates.
    TblsAnalyzeOk("DESCRIBE HISTORY $TBL FROM \"2022-02-14 13:31:09.819\"", iceT);
    TblsAnalyzeOk("DESCRIBE HISTORY $TBL FROM " +
        "cast('2021-08-09 15:52:45' as timestamp) - interval 2 days + interval 3 days",
        iceT);
    TblsAnalyzeOk("DESCRIBE HISTORY $TBL FROM now() + interval 3 days", iceT);
    TblsAnalyzeOk("DESCRIBE HISTORY $TBL BETWEEN '2021-02-22' AND '2021-02-22'", iceT);

    // Analyze should fail with unsupported expression types.
    TblsAnalysisError("DESCRIBE HISTORY $TBL FROM 42 ", iceT,
        "FROM <expression> must be a timestamp type");
    TblsAnalysisError("DESCRIBE HISTORY $TBL FROM id", iceT,
        "Unsupported expression: 'id'");
    TblsAnalysisError("DESCRIBE HISTORY $TBL FROM '2021-02-32 15:52:45'", iceT,
        "Invalid TIMESTAMP expression");

    // DESCRIBE HISTORY is only supported for Iceberg tables.
    TblsAnalysisError("DESCRIBE HISTORY $TBL", nonIceT,
        "DESCRIBE HISTORY must specify an Iceberg table:");
  }

  @Test
  public void testCreatePartitionedIcebergTable() throws ImpalaException {
    String tblProperties = " TBLPROPERTIES ('iceberg.catalog'='hadoop.tables')";
    AnalyzesOk("CREATE TABLE tbl1 (i int, p1 int, p2 timestamp) " +
        "PARTITIONED BY SPEC (BUCKET(10, p1), TRUNCATE(5, p1), DAY(p2)) " +
        "STORED AS ICEBERG" + tblProperties);
    AnalyzesOk("CREATE TABLE tbl1 (ts timestamp) " +
        "PARTITIONED BY SPEC (YEAR(ts), MONTH(ts), DAY(ts), HOUR(ts)) " +
        "STORED AS ICEBERG" + tblProperties);
    AnalyzesOk("CREATE TABLE tbl1 (ts timestamp) " +
        "PARTITIONED BY SPEC (YEARS(ts), MONTHS(ts), DAYS(ts), HOURS(ts)) " +
        "STORED AS ICEBERG" + tblProperties);
    AnalysisError("CREATE TABLE tbl1 (i int, p1 int, p2 timestamp) " +
        "PARTITIONED BY SPEC (BUCKET(p1), DAY(p2)) STORED AS ICEBERG" + tblProperties,
        "BUCKET and TRUNCATE partition transforms should have a parameter.");
    AnalysisError("CREATE TABLE tbl1 (i int, p1 int) " +
        "PARTITIONED BY SPEC (TRUNCATE(p1)) STORED AS ICEBERG",
        "BUCKET and TRUNCATE partition transforms should have a parameter.");
    AnalysisError("CREATE TABLE tbl1 (i int, p1 int, p2 timestamp) " +
        "PARTITIONED BY SPEC (BUCKET(0, p1), DAY(p2)) STORED AS ICEBERG" + tblProperties,
        "The parameter of a partition transform should be greater than zero.");
    AnalysisError("CREATE TABLE tbl1 (i int, p1 int, p2 timestamp) " +
        "PARTITIONED BY SPEC (TRUNCATE(0, p1), DAY(p2)) STORED AS ICEBERG" +
        tblProperties,
        "The parameter of a partition transform should be greater than zero.");
    AnalysisError("CREATE TABLE tbl1 (i int, p1 int, p2 timestamp) " +
        "PARTITIONED BY SPEC (BUCKET(10, p1), DAY(10, p2)) STORED AS ICEBERG" +
        tblProperties,
        "Only BUCKET and TRUNCATE partition transforms accept a parameter.");
  }

  @Test
  public void testPredicateHint() {
    // Legal hint return correct results without warning, we currently only support
    // 'ALWAYS_TRUE' hint.
    AnalyzesOk("select * from tpch.lineitem where /* +ALWAYS_TRUE */ " +
            "l_shipdate <= (select '1998-09-02')");

    // Illegal hint return correct results with a warning.
    AnalyzesOk("select * from tpch.lineitem where /* +ALWAYS_TRUE_TEST */ " +
            "l_shipdate <= (select '1998-09-02')",
        "Predicate hint not recognized: ALWAYS_TRUE_TEST");
    AnalyzesOk("select * from tpch.lineitem where /* +ILLEGAL_HINT_TEST */ " +
            "l_shipdate <= (select '1998-09-02')",
        "Predicate hint not recognized: ILLEGAL_HINT_TEST");

    // Multiple hints with legal and illegal hints also output a warning.
    AnalyzesOk("select * from tpch.lineitem where /* +ALWAYS_TRUE,ILLEGAL_HINT_TEST */ "
            + "l_shipdate <= (select '1998-09-02')",
        "Predicate hint not recognized: ILLEGAL_HINT_TEST");

    // Multiple illegal hints will output each hint warnings.
    AnalyzesOk("select * from tpch.lineitem where " +
            "/* +ILLEGAL_HINT_TEST1,ILLEGAL_HINT_TEST2,ILLEGAL_HINT_TEST3 */ " +
            "l_shipdate <= (select '1998-09-02')",
        "Predicate hint not recognized: ILLEGAL_HINT_TEST1");
    AnalyzesOk("select * from tpch.lineitem where " +
            "/* +ILLEGAL_HINT_TEST1,ILLEGAL_HINT_TEST2,ILLEGAL_HINT_TEST3 */ " +
            "l_shipdate <= (select '1998-09-02')",
        "Predicate hint not recognized: ILLEGAL_HINT_TEST2");
    AnalyzesOk("select * from tpch.lineitem where " +
            "/* +ILLEGAL_HINT_TEST1,ILLEGAL_HINT_TEST2,ILLEGAL_HINT_TEST3 */ " +
            "l_shipdate <= (select '1998-09-02')",
        "Predicate hint not recognized: ILLEGAL_HINT_TEST3");
  }

  @Test
  public void testTableCardinalityHintNegative() {
    // Cannot set cardinality hint with non long type parameter
    AnalysisError("select * from functional.alltypes /* +TABLE_NUM_ROWS(aa) */",
        "For input string: \"aa\"");
    AnalysisError("select * from functional.alltypes /* +TABLE_NUM_ROWS(-1) */",
        "Syntax error in line 1");
    AnalysisError("select * from functional.alltypes /* +TABLE_NUM_ROWS(1.0) */",
        "Syntax error in line 1");
    // Cannot set cardinality hint with multiple parameters
    AnalysisError("select * from functional.alltypes /* +TABLE_NUM_ROWS(10, 20) */",
        "Syntax error in line 1");
  }

  @Test
  public void testTableCardinalityHintPositive() {
    // Cannot set cardinality hint without parameter
    AnalyzesOk("select * from functional.alltypes /* +TABLE_NUM_ROWS */",
        "Table hint not recognized for table functional.alltypes: TABLE_NUM_ROWS");
    // 'TABLE_NUM_ROWS' is only valid for hdfs and kudu table now
    AnalyzesOk("select * from functional.alltypes /* +TABLE_NUM_ROWS(100) */");
    AnalyzesOk("select * from functional_kudu.alltypes /* +TABLE_NUM_ROWS(100) */");
    // Kudu table only support 'TABLE_NUM_ROWS' hint
    AnalyzesOk("select * from functional_kudu.alltypes /* +SCHEDULE_CACHE_LOCAL */",
        "Kudu table only support 'TABLE_NUM_ROWS' hint.");
    // Only hdfs and kudu tables can use this hint
    AnalyzesOk("select * from functional_hbase.alltypes /* +TABLE_NUM_ROWS(100) */",
        "Table hint not recognized for table " +
            "functional_hbase.alltypes: TABLE_NUM_ROWS(100)");
  }

  @Test
  public void testSelectivityHintNegative() {
    // Selectivity hint must use bracket, even for single predicate
    AnalysisError("select * from t1 where a > 1 and b > 2 /* +SELECTIVITY(0.1) */",
        "Syntax error in line 1");
    AnalysisError("select * from t1 where a > 1 /* +SELECTIVITY(0.1) */",
        "Syntax error in line 1");

    // Cannot set selectivity hint exists predicate
    AnalysisError("select * from t1 where exists (select x from t2) " +
            "/* +SELECTIVITY(0.1) */",
        "Syntax error in line 1");

    // Selectivity hint only accept one parameter with decimal type
    // Negative number and zero are not allowed
    AnalysisError("select * from t1 where (a > 1) /* +SELECTIVITY('0.1') */",
        "Syntax error in line 1");
    AnalysisError("select * from t1 where (a > 1) /* +SELECTIVITY(0) */",
        "Syntax error in line 1");
    AnalysisError("select * from t1 where (a > 1) /* +SELECTIVITY(-1.0) */",
        "Syntax error in line 1");
    AnalysisError("select * from t1 where (a > 1) /* +SELECTIVITY(0.1, 0.2) */",
        "Syntax error in line 1");
    AnalysisError("select * from t1 where (a > 1) /* +SELECTIVITY(1/3) */",
        "Syntax error in line 1");
  }

  @Test
  public void testSelectivityHintPositive() {
    // Selectivity hint legal value is (0,1]
    AnalyzesOk("select * from tpch.lineitem where (l_shipdate <= '1998-09-02') " +
            "/* +SELECTIVITY(1.1) */",
        "Invalid selectivity hint value: 1.1, allowed value should be a double value in "
            + "(0, 1].");
    AnalyzesOk("select * from tpch.lineitem where (l_shipdate <= '1998-09-02') " +
            "/* +SELECTIVITY(0.0) */",
        "Invalid selectivity hint value: 0.0, allowed value should be a double value in "
            + "(0, 1].");

    // Also valid for a very long decimal value
    AnalyzesOk("select * from functional.alltypes where (id > 1000)" +
        "/* +SELECTIVITY(0.3333333333333333333333333333333333) */");
    // Set selectivity hint for compound predicate
    AnalyzesOk("select * from functional.alltypes where (id > 1000 and int_col = 1)" +
        "/* +SELECTIVITY(0.1) */");
    AnalyzesOk("select * from functional.alltypes where (id > 1000 or int_col = 1)" +
        "/* +SELECTIVITY(0.1) */");

    // Selectivity hint is invalid for 'AND' compound predicate.
    AnalyzesOk("select * from tpch.lineitem where (l_shipdate <= '1998-09-02' and " +
            "l_shipdate >= '1997-09-02')/* +SELECTIVITY(0.5) */",
        "Selectivity hints are ignored for 'AND' compound predicates, either in the SQL "
            + "query or internally generated.");
  }

  @Test
  public void TestConvertTable() {
    AnalyzesOk("alter table functional_parquet.alltypes convert to iceberg");
    AnalyzesOk("alter table functional_parquet.alltypes convert to iceberg"
            + " tblproperties('iceberg.catalog'='hadoop.tables')");
    AnalyzesOk("alter table functional_parquet.alltypes convert to iceberg"
            + " tblproperties('iceberg.catalog'='hive.catalog')");
    AnalysisError("alter table functional_parquet.alltypes convert to iceberg"
            + " tblproperties('iceberg.catalog'='hadoop.catalog')",
        "The Hadoop Catalog is not supported because the location may change");
    AnalysisError("alter table functional_kudu.alltypes convert to iceberg",
        "CONVERT TO ICEBERG is not supported for KuduTable");
    AnalysisError("alter table functional.alltypes convert to iceberg",
        "CONVERT TO ICEBERG is not supported for " +
        "org.apache.hadoop.mapred.TextInputFormat");
    AnalysisError("alter table functional_parquet.alltypes convert to iceberg"
            + " tblproperties('metadata.generator.threads'='a1')",
        "CONVERT TO ICEBERG only accepts 'iceberg.catalog' as TBLPROPERTY.");
  }
}
