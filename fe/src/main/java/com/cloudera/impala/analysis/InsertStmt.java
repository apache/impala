// Copyright 2012 Cloudera Inc.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.planner.DataSink;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Representation of a single insert statement, including the select statement
 * whose results are to be inserted.
 *
 */
public class InsertStmt extends StatementBase {
  // List of inline views that may be referenced in queryStmt.
  private final WithClause withClause;
  // Target table name as seen by the parser
  private final TableName originalTableName;
  // Target table into which to insert. May be qualified by analyze()
  private TableName targetTableName;
  // Differentiates between INSERT INTO and INSERT OVERWRITE.
  private final boolean overwrite;
  // List of column:value elements from the PARTITION (...) clause.
  // Set to null if no partition was given.
  private final List<PartitionKeyValue> partitionKeyValues;
  // Select or union whose results are to be inserted. If null, will be set after
  // analysis.
  private QueryStmt queryStmt;
  // False if the original insert statement had a query statement, true if we need to
  // auto-generate one (for insert into tbl();) during analysis.
  private final boolean needsGeneratedQueryStatement;
  // Set in analyze(). Contains metadata of target table to determine type of sink.
  private Table table;
  // Set in analyze(). Exprs corresponding to the partitionKeyValues,
  private final List<Expr> partitionKeyExprs = new ArrayList<Expr>();
  // True if this InsertStmt is the top level query from an EXPLAIN <query>
  private boolean isExplain = false;

  // The column permutation is specified by writing INSERT INTO tbl(col3, col1, col2...)
  //
  // It is a mapping from select-list expr index to (non-partition) output column. If
  // null, will be set to the default permutation of all non-partition columns in Hive
  // order.
  //
  // A column is said to be 'mentioned' if it occurs either in the column permutation, or
  // the PARTITION clause. If columnPermutation is null, all non-partition columns are
  // considered mentioned.
  //
  // Between them, the columnPermutation and the set of partitionKeyValues must mention to
  // every partition column in the target table exactly once. Other columns, if not
  // explicitly mentioned, will be assigned NULL values. Partition columns are not
  // defaulted to NULL by design, and are not just for NULL-valued partition slots.
  //
  // Dynamic partition keys may occur in either the permutation or the PARTITION
  // clause. Partition columns with static values may only be mentioned in the PARTITION
  // clause, where the static value is specified.
  private final List<String> columnPermutation;

  public InsertStmt(WithClause withClause, TableName targetTable, boolean overwrite,
      List<PartitionKeyValue> partitionKeyValues, QueryStmt queryStmt,
      List<String> columnPermutation) {
    this.withClause = withClause;
    this.targetTableName = targetTable;
    this.originalTableName = targetTableName;
    this.overwrite = overwrite;
    this.partitionKeyValues = partitionKeyValues;
    this.queryStmt = queryStmt;
    needsGeneratedQueryStatement = (queryStmt == null);
    this.columnPermutation = columnPermutation;
    table = null;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (withClause != null) withClause.analyze(analyzer);

    analyzer.setIsExplain(isExplain);
    if (queryStmt != null) queryStmt.setIsExplain(isExplain);

    List<Expr> selectListExprs;
    if (!needsGeneratedQueryStatement) {
      queryStmt.analyze(analyzer);
      selectListExprs = queryStmt.getResultExprs();
    } else {
      selectListExprs = Lists.newArrayList();
    }

    if (!targetTableName.isFullyQualified()) {
      this.targetTableName =
          new TableName(analyzer.getDefaultDb(), targetTableName.getTbl());
    }

    table = analyzer.getTable(targetTableName, Privilege.INSERT);

    // We do not support inserting into views.
    if (table instanceof View) {
      throw new AnalysisException(
          String.format("Impala does not support inserting into views: %s",
          table.getFullName()));
    }

    // Add target table to descriptor table.
    analyzer.getDescTbl().addReferencedTable(table);

    boolean isHBaseTable = (table instanceof HBaseTable);
    int numClusteringCols = isHBaseTable ? 0 : table.getNumClusteringCols();

    if (partitionKeyValues != null && numClusteringCols == 0) {
      if (isHBaseTable) {
        throw new AnalysisException("PARTITION clause is not valid for INSERT into " +
            "HBase tables. '" + targetTableName + "' is an HBase table");

      } else {
        // Unpartitioned table, but INSERT has PARTITION clause
        throw new AnalysisException("PARTITION clause is only valid for INSERT into " +
            "partitioned table. '" + targetTableName + "' is not partitioned");
      }
    }

    if (isHBaseTable && overwrite) {
      throw new AnalysisException("HBase doesn't have a way to perform INSERT OVERWRITE");
    }

    // Analysis of the INSERT statement from this point is basically the act of matching
    // the set of output columns (which come from a column permutation, perhaps
    // implicitly, and the PARTITION clause) to the set of input columns (which come from
    // the select-list and any statically-valued columns in the PARTITION clause).
    //
    // First, we compute the set of mentioned columns, and reject statements that refer to
    // non-existant columns, or duplicates (we must check both the column permutation, and
    // the set of partition keys). Next, we check that all partition columns are
    // mentioned. During this process we build the map from select-list expr index to
    // column in the targeted table.
    //
    // Then we check that the select-list contains exactly the right number of expressions
    // for all mentioned columns which are not statically-valued partition columns (which
    // get their expressions from partitionKeyValues).
    //
    // Finally, prepareExpressions analyzes the expressions themselves, and confirms that
    // they are type-compatible with the target columns. Where columns are not mentioned
    // (and by this point, we know that missing columns are not partition columns),
    // prepareExpressions assigns them a NULL literal expressions.

    // An null permutation clause is the same as listing all non-partition columns in
    // order.
    List<String> analysisColumnPermutation = columnPermutation;
    if (analysisColumnPermutation == null) {
      analysisColumnPermutation = Lists.newArrayList();
      ArrayList<Column> tableColumns = table.getColumns();
      for (int i = numClusteringCols; i < tableColumns.size(); ++i) {
        analysisColumnPermutation.add(tableColumns.get(i).getName());
      }
    }

    // selectExprTargetColumns maps from select expression index to a column in the target
    // table. It will eventually include all mentioned columns that aren't static-valued
    // partition columns.
    ArrayList<Column> selectExprTargetColumns = Lists.newArrayList();

    // Tracks the name of all columns encountered in either the permutation clause or the
    // partition clause to detect duplicates.
    Set<String> mentionedColumnNames = Sets.newHashSet();
    for (String columnName: analysisColumnPermutation) {
      Column column = table.getColumn(columnName);
      if (column == null) {
        throw new AnalysisException(
            "Unknown column '" + columnName + "' in column permutation");
      }

      if (!mentionedColumnNames.add(columnName)) {
        throw new AnalysisException(
            "Duplicate column '" + columnName + "' in column permutation");
      }
      selectExprTargetColumns.add(column);
    }

    int numStaticPartitionExprs = 0;
    if (partitionKeyValues != null) {
      for (PartitionKeyValue pkv: partitionKeyValues) {
        Column column = table.getColumn(pkv.getColName());
        if (column == null) {
          throw new AnalysisException("Unknown column '" + pkv.getColName() +
                                      "' in partition clause");
        }

        if (column.getPosition() >= numClusteringCols) {
          throw new AnalysisException(
              "Column '" + pkv.getColName() + "' is not a partition column");
        }

        if (!mentionedColumnNames.add(pkv.getColName())) {
          throw new AnalysisException(
              "Duplicate column '" + pkv.getColName() + "' in partition clause");
        }
        if (!pkv.isDynamic()) {
          numStaticPartitionExprs++;
        } else {
          selectExprTargetColumns.add(column);
        }
      }
    }

    // Check that all columns are mentioned by the permutation and partition clauses
    if (selectExprTargetColumns.size() + numStaticPartitionExprs !=
        table.getColumns().size()) {
      // We've already ruled out too many columns in the permutation and partition clauses
      // by checking that there are no duplicates and that every column mentioned actually
      // exists. So all columns aren't mentioned in the query. If the unmentioned columns
      // include partition columns, this is an error.
      List<String> missingColumnNames = Lists.newArrayList();
      for (Column column: table.getColumns()) {
        if (!mentionedColumnNames.contains(column.getName())) {
          // HBase tables have a single row-key column which is always in position 0. It
          // must be mentioned, since it is invalid to set it to NULL (which would
          // otherwise happen by default).
          if (isHBaseTable && column.getPosition() == 0) {
            throw new AnalysisException("Row-key column '" + column.getName() +
                "' must be explicitly mentioned in column permutation.");
          }
          if (column.getPosition() < numClusteringCols) {
            missingColumnNames.add(column.getName());
          }
        }
      }

      if (!missingColumnNames.isEmpty()) {
        throw new AnalysisException(
            "Not enough partition columns mentioned in query. Missing columns are: " +
            Joiner.on(", ").join(missingColumnNames));
      }
    }

    // Expect the selectListExpr to have entries for every target column
    if (selectExprTargetColumns.size() != selectListExprs.size()) {
      String comparator =
          (selectExprTargetColumns.size() < selectListExprs.size()) ? "fewer" : "more";
      String partitionClause =
          (partitionKeyValues == null) ? "returns" : "and PARTITION clause return";

      // If there was no column permutation provided, the error is that the select-list
      // has the wrong number of expressions compared to the number of columns in the
      // table. If there was a column permutation, then the mismatch is between the
      // select-list and the permutation itself.
      if (columnPermutation == null) {
        int totalColumnsMentioned = selectListExprs.size() + numStaticPartitionExprs;
        throw new AnalysisException(String.format(
            "Target table '%s' has %s columns (%s) than the SELECT / VALUES clause %s" +
            " (%s)", table.getFullName(), comparator,
            table.getColumns().size(), partitionClause, totalColumnsMentioned));
      } else {
        String partitionPrefix =
            (partitionKeyValues == null) ? "mentions" : "and PARTITION clause mention";
        throw new AnalysisException(String.format(
            "Column permutation %s %s columns (%s) than " +
            "the SELECT / VALUES clause %s (%s)", partitionPrefix, comparator,
            selectExprTargetColumns.size(), partitionClause, selectListExprs.size()));
      }
    }

    // Make sure static partition key values only contain const exprs.
    if (partitionKeyValues != null) {
      for (PartitionKeyValue kv: partitionKeyValues) {
        kv.analyze(analyzer);
      }
    }

    // Populate partitionKeyExprs from partitionKeyValues and selectExprTargetColumns
    prepareExpressions(selectExprTargetColumns, selectListExprs, table, analyzer);
  }


  /**
   * Performs three final parts of the analysis:
   * 1. Checks type compatibility between all expressions and their targets
   *
   * 2. Populates partitionKeyExprs with type-compatible expressions, in Hive
   * partition-column order, for all partition columns
   *
   * 3. Replaces selectListExprs with type-compatible expressions, in Hive column order,
   * for all expressions in the select-list. Unmentioned columns are assigned NULL literal
   * expressions.
   *
   * If necessary, adds casts to the expressions to make them compatible with the type of
   * the corresponding column.
   *
   * @throws AnalysisException
   *           If an expression is not compatible with its target column
   */
  private void prepareExpressions(List<Column> selectExprTargetColumns,
      List<Expr> selectListExprs, Table tbl, Analyzer analyzer)
      throws AnalysisException, AuthorizationException {
    // Temporary lists of partition key exprs and names in an arbitrary order.
    List<Expr> tmpPartitionKeyExprs = new ArrayList<Expr>();
    List<String> tmpPartitionKeyNames = new ArrayList<String>();

    int numClusteringCols = (tbl instanceof HBaseTable) ? 0 : tbl.getNumClusteringCols();

    // Check dynamic partition columns for type compatibility.
    for (int i = 0; i < selectListExprs.size(); ++i) {
      Column targetColumn = selectExprTargetColumns.get(i);
      Expr compatibleExpr = checkTypeCompatibility(targetColumn, selectListExprs.get(i));
      if (targetColumn.getPosition() < numClusteringCols) {
        // This is a dynamic clustering column
        tmpPartitionKeyExprs.add(compatibleExpr);
        tmpPartitionKeyNames.add(targetColumn.getName());
      }
      selectListExprs.set(i, compatibleExpr);
    }

    // Check static partition columns, dynamic entries in partitionKeyValues will already
    // be in selectExprTargetColumns and therefore are ignored in this loop
    if (partitionKeyValues != null) {
      for (PartitionKeyValue pkv: partitionKeyValues) {
        if (pkv.isStatic()) {
          // tableColumns is guaranteed to exist after the earlier analysis checks
          Column tableColumn = table.getColumn(pkv.getColName());
          Expr compatibleExpr = checkTypeCompatibility(tableColumn, pkv.getValue());
          tmpPartitionKeyExprs.add(compatibleExpr);
          tmpPartitionKeyNames.add(pkv.getColName());
        }
      }
    }

    // Reorder the partition key exprs and names to be consistent with the target table
    // declaration.  We need those exprs in the original order to create the corresponding
    // Hdfs folder structure correctly.
    for (Column c: table.getColumns()) {
      for (int j = 0; j < tmpPartitionKeyNames.size(); ++j) {
        if (c.getName().equals(tmpPartitionKeyNames.get(j))) {
          partitionKeyExprs.add(tmpPartitionKeyExprs.get(j));
          break;
        }
      }
    }

    Preconditions.checkState(partitionKeyExprs.size() == numClusteringCols);
    // Make sure we have stats for partitionKeyExprs
    for (Expr expr: partitionKeyExprs) {
      expr.analyze(analyzer);
    }

    // Finally, 'undo' the permutation so that the selectListExprs are in Hive column
    // order, and add NULL expressions to all missing columns.
    List<Expr> permutedSelectListExprs = Lists.newArrayList();
    for (Column tblColumn: table.getColumnsInHiveOrder()) {
      boolean matchFound = false;
      for (int i = 0; i < selectListExprs.size(); ++i) {
        if (selectExprTargetColumns.get(i).getName().equals(tblColumn.getName())) {
          permutedSelectListExprs.add(selectListExprs.get(i));
          matchFound = true;
          break;
        }
      }
      // If no match is found, either the column is a clustering column with a static
      // value, or it was unmentioned and therefore should have a NULL select-list
      // expression.
      if (!matchFound) {
        if (tblColumn.getPosition() >= numClusteringCols) {
          // Unmentioned non-clustering columns get NULL expressions. Note that we do not
          // analyze them, nor do we type-check them, on the assumption that neither is
          // necessary.
          permutedSelectListExprs.add(new NullLiteral());
        }
      }
    }
    // TODO: Check that HBase row-key columns are not NULL? See IMPALA-406
    if (needsGeneratedQueryStatement) {
      // Build a query statement that returns NULL for every column
      List<SelectListItem> selectListItems = Lists.newArrayList();
      for(Expr e: permutedSelectListExprs) {
        selectListItems.add(new SelectListItem(e, null));
      }
      SelectList selectList = new SelectList(selectListItems);
      queryStmt = new SelectStmt(selectList, null, null, null, null, null, -1);
      queryStmt.analyze(analyzer);
    }
    queryStmt.setResultExprs(permutedSelectListExprs);
  }

  /**
   * Checks for type compatibility of column and expr.
   * Returns compatible (possibly cast) expr.
   *
   * @param column
   *          Table column.
   * @param expr
   *          Expr to be checked for type compatibility with column,
   * @throws AnalysisException
   *           If the column and expr type are incompatible, or if casting the
   *           expr would lead to loss of precision.
   */
  private Expr checkTypeCompatibility(Column column, Expr expr)
      throws AnalysisException {
    // Check for compatible type, and add casts to the selectListExprs if necessary.
    // We don't allow casting to a lower precision type.
    PrimitiveType colType = column.getType();
    PrimitiveType exprType = expr.getType();
    // Trivially compatible.
    if (colType == exprType) {
      return expr;
    }
    PrimitiveType compatibleType =
        PrimitiveType.getAssignmentCompatibleType(colType, exprType);
    // Incompatible types.
    if (!compatibleType.isValid()) {
      throw new AnalysisException(
          String.format("Target table '%s' is incompatible with SELECT / PARTITION " +
                        "expressions.\nExpression '%s' (type: %s) is not compatible " +
                        "with column '%s' (type: %s)",
                        targetTableName, expr.toSql(), exprType,
                        column.getName(), colType));
    }
    // Loss of precision when inserting into the table.
    if (compatibleType != colType && !compatibleType.isNull()) {
      throw new AnalysisException(
          String.format("Possible loss of precision for target table '%s'.\n" +
                        "Expression '%s' (type: %s) would need to be cast to %s" +
                        " for column '%s'",
                        targetTableName, expr.toSql(), exprType, colType,
                        column.getName()));
    }
    // Add a cast to the selectListExpr to the higher type.
    Expr castExpr = expr.castTo(compatibleType);
    return castExpr;
  }

  public TableName getTargetTableName() {
    return targetTableName;
  }

  public Table getTargetTable() {
    return table;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  /**
   * Only valid after analysis
   */
  public QueryStmt getQueryStmt() {
    return queryStmt;
  }

  public List<Expr> getPartitionKeyExprs() {
    return partitionKeyExprs;
  }

  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(table != null);
    return table.createDataSink(partitionKeyExprs, overwrite);
  }

  @Override
  public String toSql() {
    StringBuilder strBuilder = new StringBuilder();

    if (withClause != null) {
      strBuilder.append(withClause.toSql() + " ");
    }

    strBuilder.append("INSERT ");
    if (overwrite) {
      strBuilder.append("OVERWRITE ");
    } else {
      strBuilder.append("INTO ");
    }
    strBuilder.append("TABLE " + originalTableName);
    if (columnPermutation != null) {
      strBuilder.append("(");
      strBuilder.append(Joiner.on(", ").join(columnPermutation));
      strBuilder.append(")");
    }
    if (partitionKeyValues != null) {
      List<String> values = Lists.newArrayList();
      for (PartitionKeyValue pkv: partitionKeyValues) {
        values.add(pkv.getColName() +
            (pkv.getValue() != null ? ("=" + pkv.getValue().toSql()) : ""));
      }
      strBuilder.append(" PARTITION (" + Joiner.on(", ").join(values) + ")");
    }
    if (!needsGeneratedQueryStatement) {
      strBuilder.append(" " + queryStmt.toSql());
    }
    return strBuilder.toString();
  }

  public void setIsExplain(boolean isExplain) {
    this.isExplain = isExplain;
  }

  public boolean isExplain() {
    return isExplain;
  }
}
