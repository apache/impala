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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.junit.Test;
import org.mockito.Mockito;

public class MergeInsertTest {

  @Test
  public void testAnalyzeColumnPermutation() {
    List<String> columns = Lists.newArrayList("a", "b", "c", "d");
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.SMALLINT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING);
    MergeInsert merge = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      List<Expr> resultExprs = merge.analyzeColumnPermutation();
      assertEquals(4, resultExprs.size());
    } catch (AnalysisException e) {
      fail();
    }
  }

  @Test
  public void testForeignColumn() {
    List<String> columns = Lists.newArrayList("a", "b", "c", "d",
        "e");
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      mergeInsert.analyzeColumnPermutation();
    } catch (AnalysisException e) {
      assertTrue(
          e.getMessage().contains("Unknown column(s) in column permutation"));
      return;
    }
    fail();
  }

  @Test
  public void testUnmatchedSelectItem() {
    List<String> columns = Lists.newArrayList("a", "b", "c", "d");
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      mergeInsert.analyzeColumnPermutation();
    } catch (AnalysisException e) {
      assertEquals("Column permutation mentions fewer columns (4) than the VALUES "
              + "clause returns (5): WHEN MATCHED THEN INSERT (a, b, c, d) VALUES"
              + " (a, b, c, d, e)",
          e.getMessage());
      return;
    }
    fail();
  }

  @Test
  public void testUnmatchedColumn() {
    List<String> columns = Lists.newArrayList("a", "b", "c", "d", "e");
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      mergeInsert.analyzeColumnPermutation();
    } catch (AnalysisException e) {
      assertEquals(
          "Column permutation mentions more columns (5) than the VALUES clause returns"
              + " (4): WHEN MATCHED THEN INSERT (a, b, c, d, e) VALUES (a, b, c, d)",
          e.getMessage());
      return;
    }
    fail();
  }

  @Test
  public void testColumnsInRandomOrder() {
    List<String> columns = Lists.newArrayList("e", "d", "c", "a", "b");
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    Map<String, Type> selectItems = ImmutableMap.of("e", Type.INT, "d",
        Type.STRING, "c", Type.DOUBLE, "a", Type.INT, "b", Type.STRING);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      List<Expr> exprs = mergeInsert.analyzeColumnPermutation();
      assertEquals(5, exprs.size());
    } catch (AnalysisException e) {
      fail();
    }
  }

  @Test
  public void testDuplicateColumnInPermutation() {
    List<String> columns = Lists.newArrayList("a", "b", "a", "a", "b");
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      mergeInsert.analyzeColumnPermutation();
    } catch (AnalysisException e) {
      assertEquals("Duplicate column(s) in column permutation: a, b",
          e.getMessage());
      return;
    }
    fail();
  }

  @Test
  public void testEmptyColumnPermutation() {
    List<String> columns = Lists.newArrayList();
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      mergeInsert.analyzeColumnPermutation();
    } catch (AnalysisException e) {
      assertEquals(
          "Column permutation mentions fewer columns (0) than the VALUES clause returns"
              + " (5): WHEN MATCHED THEN INSERT () VALUES (a, b, c, d, e)",
          e.getMessage());
      return;
    }
    fail();
  }

  @Test
  public void testImplicitCastAfterAnalyze() {
    List<String> columns = Lists.newArrayList("a", "b", "c", "d",
        "e");
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.SMALLINT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.TINYINT);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      List<Expr> resultExprs = mergeInsert.analyzeColumnPermutation();
      assertEquals(Type.INT, resultExprs.get(0).type_);
      assertEquals(Type.INT, resultExprs.get(4).type_);
    } catch (AnalysisException e) {
      fail();
    }
  }

  @Test
  public void testNeedsExplicitCastAtAnalyze() {
    List<String> columns = Lists.newArrayList("a", "b", "c", "d",
        "e");
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.SMALLINT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.INT, "e", Type.TINYINT);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      mergeInsert.analyzeColumnPermutation();
    } catch (AnalysisException e) {
      assertTrue(e.getMessage().contains(
          "Expression 'd' (type: INT) is not compatible with column 'd' (type: STRING)"));
      return;
    }
    fail();
  }

  @Test
  public void testAnalyzeSparseSelectList() {
    List<String> columns = Lists.newArrayList("a", "c", "e");
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.SMALLINT, "c",
        Type.DOUBLE, "e", Type.TINYINT);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      List<Expr> resultExprs = mergeInsert.analyzeColumnPermutation();
      assertTrue(resultExprs.get(1) instanceof NullLiteral);
      assertTrue(resultExprs.get(3) instanceof NullLiteral);
      assertEquals(5, resultExprs.size());
    } catch (AnalysisException e) {
      fail();
    }
  }

  @Test
  public void testAnalyzeImplicitMatch() {
    List<String> columns = Collections.EMPTY_LIST;
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.INT);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.SMALLINT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.TINYINT);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      List<Expr> resultExprs  = mergeInsert.analyzeColumnPermutation();
      assertEquals(5, resultExprs.size());
    } catch (AnalysisException e) {
      fail();
    }
  }

  @Test
  public void testAnalyzeImplicitMatchWithUnmatchedSelectItem() {
    List<String> columns = Collections.EMPTY_LIST;
    Map<String, Type> columnMap = ImmutableMap.of("a", Type.INT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING);
    Map<String, Type> selectItems = ImmutableMap.of("a", Type.SMALLINT, "b",
        Type.STRING, "c", Type.DOUBLE, "d", Type.STRING, "e", Type.TINYINT);
    MergeInsert mergeInsert = prepareMergeInsert(columnMap, columns, selectItems);
    try {
      mergeInsert.analyzeColumnPermutation();
    } catch (AnalysisException e) {
      assertTrue(e.getMessage().contains(
          "Target table 'test' has fewer columns (4) than the VALUES"
              + " clause returns (5)"));
      return;
    }
    fail();
  }

  private Column mockColumnWithName(String name, Type type) {
    Column column = Mockito.mock(Column.class);
    Mockito.when(column.getName()).thenReturn(name);
    Mockito.when(column.getType()).thenReturn(type);
    return column;
  }

  private MergeInsert prepareMergeInsert(Map<String, Type> columnMap,
      List<String> columnPermutation, Map<String, Type> selectItems) {
    List<Column> columns = columnMap.entrySet().stream()
        .map(entry -> mockColumnWithName(entry.getKey(), entry.getValue()))
        .collect(
            Collectors.toList());
    SelectList selectList = prepareSelectList(selectItems);
    MergeStmt mergeStmt = Mockito.mock(MergeStmt.class);
    Analyzer analyzer = Mockito.mock(Analyzer.class);
    FeTable table = Mockito.mock(IcebergTable.class);
    TableName tableName = Mockito.mock(TableName.class);
    TableRef tableRef = Mockito.mock(TableRef.class);
    Mockito.when(table.getColumns()).thenReturn(columns);
    Mockito.when(table.getTableName()).thenReturn(tableName);
    Mockito.when(mergeStmt.getTargetTable()).thenReturn(table);
    Mockito.when(mergeStmt.getTargetTableRef()).thenReturn(tableRef);
    Mockito.when(tableName.toString()).thenReturn("test");
    Mockito.when(analyzer.getPermissiveCompatibilityLevel()).thenReturn(
        TypeCompatibility.DEFAULT);
    Mockito.when(analyzer.getRegularCompatibilityLevel())
        .thenReturn(TypeCompatibility.DEFAULT);
    MergeInsert mergeInsert = new MergeInsert(columnPermutation, selectList);
    mergeInsert.setParent(mergeStmt);
    mergeStmt.analyzer_ = analyzer;
    mergeInsert.analyzer_ = analyzer;
    return mergeInsert;
  }

  private SelectList prepareSelectList(Map<String, Type> items) {
    List<SelectListItem> selectListItems = items.entrySet().stream()
        .map(s -> {
          SlotRef expr = new SlotRef(s.getKey());
          expr.type_ = s.getValue();
          return new SelectListItem(expr, s.getKey());
        }).collect(
            Collectors.toList());
    return new SelectList(selectListItems);
  }
}
