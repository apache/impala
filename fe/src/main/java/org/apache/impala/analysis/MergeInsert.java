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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.impala.catalog.Column;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TMergeCaseType;
import org.apache.impala.thrift.TMergeMatchType;

/**
 * Insert clause for MERGE statements. This clause supports 2 different usage modes:
 * 1. INSERT VALUES (s.a, s.b, s.c): In this case, the analyzer collates the select list
 *    with all columns of the target table.
 * 2. INSERT (a, b, c) VALUES (s.a, s.b, s.c): only the explicitly written out columns
 *    are collated with select list, every other column value is treated as null.
 */
public class MergeInsert extends MergeCase {
  // Stores the column names targeted by the merge insert case.
  protected List<String> columnPermutation_;
  protected final SelectList selectList_;

  public MergeInsert(List<String> columnPermutation, SelectList selectList) {
    columnPermutation_ = columnPermutation;
    selectList_ = selectList;
  }

  protected MergeInsert(List<Expr> resultExprs, List<Expr> filterExprs,
      TableName targetTableName, List<Column> targetTableColumns, TableRef targetTableRef,
      List<String> columnPermutation, SelectList selectList, TMergeMatchType matchType,
      TableRef sourceTableRef) {
    super(resultExprs, filterExprs, targetTableName, targetTableColumns, targetTableRef,
        matchType, sourceTableRef);
    columnPermutation_ = columnPermutation;
    selectList_ = selectList;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    for (SelectListItem selectListItem : selectList_.getItems()) {
      if (selectListItem.isStar()) {
        throw new AnalysisException(
            String.format("Invalid expression: %s", selectListItem));
      }
      selectListItem.getExpr().analyze(analyzer_);
    }
    resultExprs_ = analyzeColumnPermutation();
    for (Expr expr : resultExprs_) { expr.analyze(analyzer); }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    String parent = super.toSql(options);
    StringBuilder builder = new StringBuilder(parent);
    builder.append("INSERT ");
    StringJoiner assignmentJoiner = new StringJoiner(", ");
    if (columnPermutation_ != Collections.EMPTY_LIST) {
      StringJoiner columnJoiner = new StringJoiner(", ");
      for (String column : columnPermutation_) { columnJoiner.add(column); }
      builder.append(String.format("(%s) ", columnJoiner));
    }
    builder.append(assignmentJoiner);
    builder.append("VALUES ");
    StringJoiner selectItemJoiner = new StringJoiner(", ");
    for (SelectListItem item : selectList_.getItems()) {
      selectItemJoiner.add(item.getExpr().toSql(options));
    }
    builder.append(String.format("(%s)", selectItemJoiner));
    return builder.toString();
  }

  @Override
  public TMergeCaseType caseType() { return TMergeCaseType.INSERT; }

  @Override
  public void reset() {
    super.reset();
    selectList_.reset();
  }

  @Override
  public MergeInsert clone() {
    return new MergeInsert(Expr.cloneList(resultExprs_), Expr.cloneList(getFilterExprs()),
        targetTableName_, targetTableColumns_, targetTableRef_, columnPermutation_,
        selectList_, matchType_, sourceTableRef_);
  }

  /**
   * Validates the provided column permutation and creates the result expressions
   * by the following steps:
   * 1. Checks whether the list contains any duplicates
   * 2. Checks if the size of the column permutation or the select list differs.
   * 3. Creates the result expression list:
   *  a) If the column could be located in the table's column list, the expression is
   *     retrieved through the column permutation's index
   *  b) If the column permutation is not provided, the expression is retrieved by the
   *     index of the table's column list
   *  c) If the column cannot be located, a NullLiteral expression is created
   * @return - matched expression and optionally null literals.
   * @throws AnalysisException if a duplicate column is found, the column cannot be
   *  located, the size of select list and column permutation differs
   *  or the type of the column is unsupported.
   */
  @VisibleForTesting
  public List<Expr> analyzeColumnPermutation() throws AnalysisException {
    // Comparing by reference
    boolean emptyColumnPermutation = columnPermutation_ == Collections.EMPTY_LIST;
    permutationSizeCheck(emptyColumnPermutation);
    duplicateColumnCheck();

    Map<String, Integer> indexedColumnPermutation = new HashMap<>();
    for (int i = 0; i < columnPermutation_.size(); ++i) {
      indexedColumnPermutation.put(columnPermutation_.get(i), i);
    }
    List<Expr> resultExprs = Lists.newArrayList();
    for (int i = 0; i < targetTableColumns_.size(); ++i) {
      Column column = targetTableColumns_.get(i);
      InsertStmt.checkSupportedColumn(column, "MERGE INSERT", targetTableName_);
      String name = column.getName();
      Expr expr;

      if (indexedColumnPermutation.containsKey(name)) {
        expr = selectList_.getItems().get(indexedColumnPermutation.get(name)).getExpr();
      } else if (emptyColumnPermutation) {
        expr = selectList_.getItems().get(i).getExpr();
      } else {
        expr = new NullLiteral();
      }

      expr = StatementBase.checkTypeCompatibility(
          targetTableName_.toSql(), column, expr, analyzer_, null);
      resultExprs.add(expr);
      indexedColumnPermutation.remove(name);
    }
    if (!indexedColumnPermutation.isEmpty()) {
      String foreignColumns = String.join(", ", indexedColumnPermutation.keySet());
      throw new AnalysisException(
          String.format("Unknown column(s) in column permutation: %s", foreignColumns));
    }
    return resultExprs;
  }

  private void permutationSizeCheck(boolean emptyColumnPermutation)
      throws AnalysisException {
    int selectListSize = selectList_.getItems().size();
    int columnPermutationSize = columnPermutation_.size();
    String target;

    if (emptyColumnPermutation) {
      target = String.format("Target table '%s' has", targetTableName_);
      columnPermutationSize = targetTableColumns_.size();
    } else {
      target = "Column permutation mentions";
    }

    if (columnPermutationSize > selectListSize) {
      throw new AnalysisException(
          String.format(moreColumnsMessageTemplate(),
              target, columnPermutationSize, selectListSize, toSql()));
    }
    if (columnPermutationSize < selectListSize) {
      throw new AnalysisException(
          (String.format(fewerColumnsMessageTemplate(),
              target, columnPermutationSize, selectListSize, toSql())));
    }
  }

  protected String moreColumnsMessageTemplate() {
    return "%s more columns (%d) than the VALUES clause returns (%d): %s";
  }

  protected String fewerColumnsMessageTemplate() {
    return "%s fewer columns (%d) than the VALUES clause returns (%d): %s";
  }

  private void duplicateColumnCheck() throws AnalysisException {
    Set<String> columnNames = new HashSet<>();
    Set<String> duplicatedColumnNames = new HashSet<>();

    for (String columnName : columnPermutation_) {
      if (columnNames.contains(columnName)) {
        duplicatedColumnNames.add(columnName);
      } else {
        columnNames.add(columnName);
      }
    }
    if (!duplicatedColumnNames.isEmpty()) {
      throw new AnalysisException(
          String.format("Duplicate column(s) in column permutation: %s",
              String.join(", ", duplicatedColumnNames)));
    }
  }
}
