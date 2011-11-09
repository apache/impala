// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.HdfsTable.Partition;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * Representation of a single insert statement, including the select statement
 * whose results are to be inserted.
 *
 */
public class InsertStmt extends ParseNodeBase {

  // Target table into which to insert.
  private final TableName targetTableName;
  // Differentiates between INSERT INTO and INSERT OVERWRITE.
  private final boolean overwrite;
  // List of column:value elements from the PARTITION (...) clause.
  // Set to null if no partition was given.
  private final List<PartitionKeyValue> partitionKeyValues;
  // Select statement whose results are to be inserted.
  private final SelectStmt selectStmt;
  // Set in analyze(). Contains metadata of target table to determine type of sink.
  private Table table;
  // Set in analyze(). Exprs corresponding to the partitionKeyValues,
  private final List<Expr> partitionKeyExprs = new ArrayList<Expr>();

  public InsertStmt(TableName targetTable, boolean overwrite,
      List<PartitionKeyValue> partitionKeyValues, SelectStmt selectStmt) {
    this.targetTableName = targetTable;
    this.overwrite = overwrite;
    this.partitionKeyValues = partitionKeyValues;
    this.selectStmt = selectStmt;
    table = null;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    selectStmt.analyze(analyzer);
    List<Expr> selectListExprs = selectStmt.getSelectListExprs();
    Catalog catalog = analyzer.getCatalog();
    table = catalog.getDb(targetTableName.getDb()).getTable(
        targetTableName.getTbl());

    // Deal with unpartitioned tables. We expect no partition clause.
    int numClusteringCols = table.getNumClusteringCols();
    if (partitionKeyValues == null) {
      // Unpartitioned table and no partition clause.
      if (numClusteringCols == 0) {
        return;
      }
      // Partitioned table but no partition clause.
      throw new AnalysisException("No PARTITION clause given for insertion into " +
          "partitioned table '" + targetTableName.getTbl() + "'.");
    }
    // Specifying partitions for an HBase table does not make sense.
    if (table instanceof HBaseTable) {
      Preconditions.checkState(partitionKeyValues != null);
      throw new AnalysisException("PARTITION clause is not allowed for HBase tables.");
    }

    // Check that the partition clause mentions all the table's partitioning columns.
    PartitionKeyValue[] orderedPartKeyValues = checkPartitionClauseCompleteness();
    // Check that all dynamic partition keys are at the end of the selectListExprs.
    int numDynamicPartKeys = fillPartitionKeyExprs();
    // Check that the static partition key values refer to an existing partition.
    int numStaticPartKeys = partitionKeyValues.size() - numDynamicPartKeys;
    checkPartitionExists(orderedPartKeyValues, numStaticPartKeys);

    // Check union compatibility, ignoring partitioning columns for dynamic partitions.
    checkUnionCompatibility(table, selectListExprs, numDynamicPartKeys);
  }

  /**
   * Checks whether all partitioning columns in table are mentioned in
   * partitionKeyValues, and that all partitionKeyValues have a match in table.
   *
   * @return
   *         A list of partition key values, ordered the same was as the
   *         corresponding partitioning columns in the table.
   * @throws AnalysisException
   *           If the partitionKeyValues don't mention all partitioning columns in
   *           table, or if they mention extra columns.
   */
  private PartitionKeyValue[] checkPartitionClauseCompleteness()
      throws AnalysisException {
    List<Column> columns = table.getColumns();
    int numClusteringCols = table.getNumClusteringCols();
    // Copy the partition key values a temporary list.
    // We remove items as we match them against partitioning columns in the table.
    List<PartitionKeyValue> unmatchedPartKeyVals = new LinkedList<PartitionKeyValue>();
    unmatchedPartKeyVals.addAll(partitionKeyValues);
    // Check that all partitioning columns were mentioned in the partition clause.
    // Remove matching items from unmatchedPartKeyVals
    // to detect superfluous columns in the partition clause.
    int partKeyIndex = 0;
    PartitionKeyValue[] orderedPartKeyValues =
        new PartitionKeyValue[partitionKeyValues.size()];
    for (int i = 0; i < numClusteringCols; ++i) {
      PartitionKeyValue matchingPartKeyVal = null;
      Iterator<PartitionKeyValue> clauseIter = unmatchedPartKeyVals.iterator();
      while (clauseIter.hasNext()) {
        PartitionKeyValue pkv = clauseIter.next();
        if (pkv.getColName().equals(columns.get(i).getName())) {
          matchingPartKeyVal = pkv;
          orderedPartKeyValues[partKeyIndex++] = pkv;
          clauseIter.remove();
          break;
        }
      }
      if (matchingPartKeyVal == null) {
        throw new AnalysisException("Missing partition column '"
            + columns.get(i).getName() + "' from PARTITION clause.");
      }
    }
    // All partitioning columns of the table were matched.
    // Check for superfluous columns in the partition clause.
    if (!unmatchedPartKeyVals.isEmpty()) {
      StringBuilder strBuilder = new StringBuilder();
      for (PartitionKeyValue pkv : unmatchedPartKeyVals) {
        strBuilder.append(pkv.getColName() + ",");
      }
      strBuilder.deleteCharAt(strBuilder.length() - 1);
      throw new AnalysisException("Superfluous columns in PARTITION clause: "
          + strBuilder.toString() + ".");
    }
    return orderedPartKeyValues;
  }

  /**
   * Fills the partitionKeyExprs class member, by positionally
   * matching the dynamic partition keys
   * against the last numDynamicPartKeys selectListExprs.
   * If necessary, adds casts to the selectListExprs to make them compatible
   * with the type of the corresponding partitioning column.
   *
   * @return Number of dynamic partition keys.
   * @throws AnalysisException
   *           If not all dynamic partition keys are mentioned in the selectListExprs.
   */
  private int fillPartitionKeyExprs() throws AnalysisException {
    // Count the number of dynamic partition keys.
    int numDynamicPartKeys = 0;
    for (PartitionKeyValue pkv : partitionKeyValues) {
      if (pkv.isDynamic()) {
        ++numDynamicPartKeys;
      }
    }
    List<Expr> selectListExprs = selectStmt.getSelectListExprs();
    // Position of selectListExpr corresponding to the next dynamic partition column.
    int exprMatchPos = table.getColumns().size() - table.getNumClusteringCols();
    for (PartitionKeyValue pkv : partitionKeyValues) {
      if (pkv.isStatic()) {
        partitionKeyExprs.add(pkv.getValue());
        continue;
      }
      if (exprMatchPos >= selectListExprs.size()) {
        throw new AnalysisException("No matching select list item found for "
            + "dynamic partition '" + pkv.getColName() + "'.\n"
            + "The select list items corresponding to dynamic partition "
            + "keys must be at the end of the select list.");
      }
      Column tableColumn = table.getColumn(pkv.getColName());
      Expr expr = selectListExprs.get(exprMatchPos);
      Expr compatibleExpr = checkTypeCompatibility(tableColumn, expr);
      partitionKeyExprs.add(compatibleExpr);
    }
    return numDynamicPartKeys;
  }

  /**
   * Check that the static partition key values refer to an existing partition.
   * TODO: Eventually we want to support adding new partitions. This will require
   * changing the table metadata in the metastore.
   *
   * @param orderedPartKeyValues
   *          Array of PartitionKeyValues ordered the same way as in the table metadata.
   * @param numStaticPartKeys
   *          Number of static partition key values which should
   *          match an existing partition.
   * @throws AnalysisException
   *           If no existing partition that matches
   *           all static partition key values was found.
   */
  private void checkPartitionExists(PartitionKeyValue[] orderedPartKeyValues,
      int numStaticPartKeys) throws AnalysisException {
    // Check that the static partition keys values refer to an existing partition.
    // TODO: Eventually we want to support adding new partitions. This will require
    // changing the table metadata in the metastore.
    Preconditions.checkState(table instanceof HdfsTable);
    HdfsTable hdfsTable = (HdfsTable) table;
    int numClusteringCols = table.getNumClusteringCols();
    List<Partition> partitions = hdfsTable.getPartitions();
    boolean partitionExists = false;
    for (int i = 0; i < partitions.size(); ++i) {
      Partition p = partitions.get(i);
      Preconditions.checkState(p.keyValues.size() == numClusteringCols);
      // Recall that orderedPartKeyValues has a compatible ordering.
      int matchingStaticPartKeys = 0;
      for (int j = 0; j < numClusteringCols; ++j) {
        PartitionKeyValue pkv = orderedPartKeyValues[j];
        if (pkv.isDynamic()) {
          continue;
        }
        if (p.keyValues.get(j).equals(pkv.getValue())) {
          ++matchingStaticPartKeys;
        }
      }
      if (matchingStaticPartKeys == numStaticPartKeys) {
        partitionExists = true;
        break;
      }
    }
    if (!partitionExists) {
      throw new AnalysisException("PARTITION clause specifies a " +
      		"non-existent partition.\n" +
          "Can only insert or overwrite an existing partition.");
    }
  }

  /**
   * Checks for union compatibility of a table and a list of exprs.
   * May cast the exprs to higher precision types,
   * if necessary, to make them compatible with their corresponding table columns.
   *
   * @param table
   *          Table from the metadata
   * @param selectListExprs
   *          In/Out: List of expressions from a select statement.
   *          Possibly modified with casts.
   * @param numDynamicPartKeys
   *          The number of dynamic partition keys. We assume that the partition keys
   *          have a verified match at the end of the select list.
   * @throws AnalysisException
   *           If the columns and exprs are not union compatible,
   *           or if making them union compatible
   *           would lose precision in at least one column.
   */
  private void checkUnionCompatibility(Table table, List<Expr> selectListExprs,
      int numDynamicPartKeys)
      throws AnalysisException {
    List<Column> columns = table.getColumns();
    int numClusteringCols = table.getNumClusteringCols();
    int numNonClusteringCols = columns.size() - numClusteringCols;
    if (numNonClusteringCols != selectListExprs.size() - numDynamicPartKeys) {
      throw new AnalysisException("Target table '" + targetTableName.getTbl()
          + "' and result of select statement are not union compatible.\n"
          + "Target table expects "
          + numNonClusteringCols + " columns but the select statement returns "
          + selectListExprs.size() + ".");
    }
    for (int i = numClusteringCols; i < columns.size(); ++i) {
      int selectListIndex = i - numClusteringCols;
      // Check for compatible type, and add casts to the selectListExprs if necessary.
      Expr expr = checkTypeCompatibility(columns.get(i),
          selectListExprs.get(selectListIndex));
      selectListExprs.set(selectListIndex, expr);
    }
  }

  /**
   * Checks for type compatibility of column and expr.
   * Returns compatible (possibly cast) expr.
   *
   * @param column
   *          Table column.
   * @param expr
   *          Expr to be checked for type compatibility with column,
   * @return
   *         Possibly cast compatible expr.
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
      throw new AnalysisException("Target table '" + targetTableName.getTbl()
          + "' and result of select statement are not union compatible.\n"
          + "Incompatible types '" + colType.toString() + "' and '"
          + exprType.toString() + "' in column '" + expr.toSql() + "'.");
    }
    // Loss of precision when inserting into the table.
    if (compatibleType != colType) {
      throw new AnalysisException("Inserting into target table '"
          + targetTableName.getTbl() + "' may result in loss of precision.\n"
          + "Would need to cast '"
          + expr.toSql() + "' to '"
          + colType.toString() + "'.");
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

  public SelectStmt getSelectStmt() {
    return selectStmt;
  }

  public List<PartitionKeyValue> getPartitionList() {
    return partitionKeyValues;
  }

  public List<Expr> getPartitionKeyExprs() {
    return partitionKeyExprs;
  }
}
