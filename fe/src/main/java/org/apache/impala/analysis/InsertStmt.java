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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.View;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.rewrite.ExprRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Representation of a single insert or upsert statement, including the select statement
 * whose results are to be inserted.
 */
public class InsertStmt extends StatementBase {
  // Target table name as seen by the parser
  private final TableName originalTableName_;

  // Differentiates between INSERT INTO and INSERT OVERWRITE.
  private final boolean overwrite_;

  // List of column:value elements from the PARTITION (...) clause.
  // Set to null if no partition was given.
  private final List<PartitionKeyValue> partitionKeyValues_;

  // User-supplied hints to control hash partitioning before the table sink in the plan.
  private List<PlanHint> planHints_ = Lists.newArrayList();

  // False if the original insert statement had a query statement, true if we need to
  // auto-generate one (for insert into tbl()) during analysis.
  private final boolean needsGeneratedQueryStatement_;

  // The column permutation is specified by writing:
  //     (INSERT|UPSERT) INTO tbl(col3, col1, col2...)
  //
  // It is a mapping from select-list expr index to (non-partition) output column. If
  // null, will be set to the default permutation of all non-partition columns in Hive
  // order or all columns for Kudu tables.
  //
  // A column is said to be 'mentioned' if it occurs either in the column permutation, or
  // the PARTITION clause. If columnPermutation is null, all non-partition columns are
  // considered mentioned.
  //
  // Between them, the columnPermutation and the set of partitionKeyValues must mention
  // every partition column in the target table exactly once. Other columns, if not
  // explicitly mentioned, will be assigned NULL values for INSERTs and left unassigned
  // for UPSERTs. Partition columns are not defaulted to NULL by design, and are not just
  // for NULL-valued partition slots.
  //
  // Dynamic partition keys may occur in either the permutation or the PARTITION
  // clause. Partition columns with static values may only be mentioned in the PARTITION
  // clause, where the static value is specified.
  private final List<String> columnPermutation_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // List of inline views that may be referenced in queryStmt.
  private final WithClause withClause_;

  // Target table into which to insert. May be qualified by analyze()
  private TableName targetTableName_;

  // Select or union whose results are to be inserted. If null, will be set after
  // analysis.
  private QueryStmt queryStmt_;

  // Set in analyze(). Contains metadata of target table to determine type of sink.
  private Table table_;

  // Set in analyze(). Exprs corresponding to the partitionKeyValues.
  private List<Expr> partitionKeyExprs_ = Lists.newArrayList();

  // Indicates whether this insert stmt has a shuffle or noshuffle plan hint.
  // Both flags may be false. Only one of them may be true, not both.
  // Shuffle forces data repartitioning before the data sink, and noshuffle
  // prevents it. Set in analyze() based on planHints_.
  private boolean hasShuffleHint_ = false;
  private boolean hasNoShuffleHint_ = false;

  // Indicates whether this insert stmt has a clustered or noclustered hint. If clustering
  // is requested, we add a clustering phase before the data sink, so that partitions can
  // be written sequentially. The default behavior is to not perform an additional
  // clustering step.
  private boolean hasClusteredHint_ = false;

  // For every column of the target table that is referenced in the optional 'sortby()'
  // hint, this list will contain the corresponding result expr from 'resultExprs_'.
  // Before insertion, all rows will be sorted by these exprs. If the list is empty, no
  // additional sorting by non-partitioning columns will be performed. For Hdfs tables,
  // the 'sortby()' hint must not contain partition columns. For Kudu tables, it must not
  // contain primary key columns.
  private List<Expr> sortByExprs_ = Lists.newArrayList();

  // Stores the indices into the list of non-clustering columns of the target table that
  // are mentioned in the 'sortby()' hint. This is sent to the backend to populate the
  // RowGroup::sorting_columns list in parquet files.
  private List<Integer> sortByColumns_ = Lists.newArrayList();

  // Output expressions that produce the final results to write to the target table. May
  // include casts. Set in prepareExpressions().
  // If this is an INSERT on a non-Kudu table, it will contain one Expr for all
  // non-partition columns of the target table with NullLiterals where an output
  // column isn't explicitly mentioned. The i'th expr produces the i'th column of
  // the target table.
  //
  // For Kudu tables (INSERT and UPSERT operations), it will contain one Expr per column
  // mentioned in the query and mentionedColumns_ is used to map between the Exprs
  // and columns in the target table.
  private ArrayList<Expr> resultExprs_ = Lists.newArrayList();

  // Position mapping of exprs in resultExprs_ to columns in the target table -
  // resultExprs_[i] produces the mentionedColumns_[i] column of the target table.
  // Only used for Kudu tables, set in prepareExpressions().
  private final List<Integer> mentionedColumns_ = Lists.newArrayList();

  // Set in analyze(). Exprs corresponding to key columns of Kudu tables. Empty for
  // non-Kudu tables.
  private ArrayList<Expr> primaryKeyExprs_ = Lists.newArrayList();

  // END: Members that need to be reset()
  /////////////////////////////////////////

  // True iff this is an UPSERT operation. Only supported for Kudu tables.
  private final boolean isUpsert_;

  public static InsertStmt createInsert(WithClause withClause, TableName targetTable,
      boolean overwrite, List<PartitionKeyValue> partitionKeyValues,
      List<PlanHint> planHints, QueryStmt queryStmt, List<String> columnPermutation) {
    return new InsertStmt(withClause, targetTable, overwrite, partitionKeyValues,
        planHints, queryStmt, columnPermutation, false);
  }

  public static InsertStmt createUpsert(WithClause withClause, TableName targetTable,
      List<PlanHint> planHints, QueryStmt queryStmt, List<String> columnPermutation) {
    return new InsertStmt(withClause, targetTable, false, null, planHints, queryStmt,
        columnPermutation, true);
  }

  protected InsertStmt(WithClause withClause, TableName targetTable, boolean overwrite,
      List<PartitionKeyValue> partitionKeyValues, List<PlanHint> planHints,
      QueryStmt queryStmt, List<String> columnPermutation, boolean isUpsert) {
    Preconditions.checkState(!isUpsert || (!overwrite && partitionKeyValues == null));
    withClause_ = withClause;
    targetTableName_ = targetTable;
    originalTableName_ = targetTableName_;
    overwrite_ = overwrite;
    partitionKeyValues_ = partitionKeyValues;
    planHints_ = (planHints != null) ? planHints : new ArrayList<PlanHint>();
    queryStmt_ = queryStmt;
    needsGeneratedQueryStatement_ = (queryStmt == null);
    columnPermutation_ = columnPermutation;
    table_ = null;
    isUpsert_ = isUpsert;
  }

  /**
   * C'tor used in clone().
   */
  private InsertStmt(InsertStmt other) {
    super(other);
    withClause_ = other.withClause_ != null ? other.withClause_.clone() : null;
    targetTableName_ = other.targetTableName_;
    originalTableName_ = other.originalTableName_;
    overwrite_ = other.overwrite_;
    partitionKeyValues_ = other.partitionKeyValues_;
    planHints_ = other.planHints_;
    queryStmt_ = other.queryStmt_ != null ? other.queryStmt_.clone() : null;
    needsGeneratedQueryStatement_ = other.needsGeneratedQueryStatement_;
    columnPermutation_ = other.columnPermutation_;
    table_ = other.table_;
    isUpsert_ = other.isUpsert_;
  }

  @Override
  public void reset() {
    super.reset();
    if (withClause_ != null) withClause_.reset();
    targetTableName_ = originalTableName_;
    queryStmt_.reset();
    table_ = null;
    partitionKeyExprs_.clear();
    hasShuffleHint_ = false;
    hasNoShuffleHint_ = false;
    hasClusteredHint_ = false;
    sortByExprs_.clear();
    resultExprs_.clear();
    mentionedColumns_.clear();
    primaryKeyExprs_.clear();
  }

  @Override
  public InsertStmt clone() { return new InsertStmt(this); }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    try {
      if (withClause_ != null) withClause_.analyze(analyzer);
    } catch (AnalysisException e) {
      // Ignore AnalysisExceptions if tables are missing to ensure the maximum number
      // of missing tables can be collected before failing analyze().
      if (analyzer.getMissingTbls().isEmpty()) throw e;
    }

    List<Expr> selectListExprs = null;
    if (!needsGeneratedQueryStatement_) {
      try {
        // Use a child analyzer for the query stmt to properly scope WITH-clause
        // views and to ignore irrelevant ORDER BYs.
        Analyzer queryStmtAnalyzer = new Analyzer(analyzer);
        queryStmt_.analyze(queryStmtAnalyzer);
        // Use getResultExprs() and not getBaseTblResultExprs() here because the final
        // substitution with TupleIsNullPredicate() wrapping happens in planning.
        selectListExprs = Expr.cloneList(queryStmt_.getResultExprs());
      } catch (AnalysisException e) {
        if (analyzer.getMissingTbls().isEmpty()) throw e;
      }
    } else {
      selectListExprs = Lists.newArrayList();
    }

    // Set target table and perform table-type specific analysis and auth checking.
    // Also checks if the target table is missing.
    analyzeTargetTable(analyzer);

    // Abort analysis if there are any missing tables beyond this point.
    if (!analyzer.getMissingTbls().isEmpty()) {
      throw new AnalysisException("Found missing tables. Aborting analysis.");
    }

    boolean isHBaseTable = (table_ instanceof HBaseTable);
    int numClusteringCols = isHBaseTable ? 0 : table_.getNumClusteringCols();

    // Analysis of the INSERT statement from this point is basically the act of matching
    // the set of output columns (which come from a column permutation, perhaps
    // implicitly, and the PARTITION clause) to the set of input columns (which come from
    // the select-list and any statically-valued columns in the PARTITION clause).
    //
    // First, we compute the set of mentioned columns, and reject statements that refer to
    // non-existent columns, or duplicates (we must check both the column permutation, and
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
    // prepareExpressions assigns them a NULL literal expressions, unless the target is
    // a Kudu table, in which case we don't want to overwrite unmentioned columns with
    // NULL.

    // An null permutation clause is the same as listing all non-partition columns in
    // order.
    List<String> analysisColumnPermutation = columnPermutation_;
    if (analysisColumnPermutation == null) {
      analysisColumnPermutation = Lists.newArrayList();
      ArrayList<Column> tableColumns = table_.getColumns();
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
      Column column = table_.getColumn(columnName);
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
    if (partitionKeyValues_ != null) {
      for (PartitionKeyValue pkv: partitionKeyValues_) {
        Column column = table_.getColumn(pkv.getColName());
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

    // Checks that exactly all columns in the target table are assigned an expr.
    checkColumnCoverage(selectExprTargetColumns, mentionedColumnNames,
        selectListExprs.size(), numStaticPartitionExprs);

    // Make sure static partition key values only contain const exprs.
    if (partitionKeyValues_ != null) {
      for (PartitionKeyValue kv: partitionKeyValues_) {
        kv.analyze(analyzer);
      }
    }

    // Populate partitionKeyExprs from partitionKeyValues and selectExprTargetColumns
    prepareExpressions(selectExprTargetColumns, selectListExprs, table_, analyzer);
    // Analyze plan hints at the end to prefer reporting other error messages first
    // (e.g., the PARTITION clause is not applicable to unpartitioned and HBase tables).
    analyzePlanHints(analyzer);
  }

  /**
   * Sets table_ based on targetTableName_ and performs table-type specific analysis:
   * - Cannot (in|up)sert into a view
   * - Cannot (in|up)sert into a table with unsupported column types
   * - Analysis specific to insert and upsert operations
   * Adds table_ to the analyzer's descriptor table if analysis succeeds.
   */
  private void analyzeTargetTable(Analyzer analyzer) throws AnalysisException {
    // If the table has not yet been set, load it from the Catalog. This allows for
    // callers to set a table to analyze that may not actually be created in the Catalog.
    // One example use case is CREATE TABLE AS SELECT which must run analysis on the
    // INSERT before the table has actually been created.
    if (table_ == null) {
      if (!targetTableName_.isFullyQualified()) {
        targetTableName_ =
            new TableName(analyzer.getDefaultDb(), targetTableName_.getTbl());
      }
      table_ = analyzer.getTable(targetTableName_, Privilege.INSERT);
    } else {
      targetTableName_ = new TableName(table_.getDb().getName(), table_.getName());
      PrivilegeRequestBuilder pb = new PrivilegeRequestBuilder();
      analyzer.registerPrivReq(pb.onTable(table_.getDb().getName(), table_.getName())
          .allOf(Privilege.INSERT).toRequest());
    }

    // We do not support (in|up)serting into views.
    if (table_ instanceof View) {
      throw new AnalysisException(
          String.format("Impala does not support %sing into views: %s", getOpName(),
              table_.getFullName()));
    }

    // We do not support (in|up)serting into tables with unsupported column types.
    for (Column c: table_.getColumns()) {
      if (!c.getType().isSupported()) {
        throw new AnalysisException(String.format("Unable to %s into target table " +
            "(%s) because the column '%s' has an unsupported type '%s'.",
            getOpName(), targetTableName_, c.getName(), c.getType().toSql()));
      }
    }

    // Perform operation-specific analysis.
    if (isUpsert_) {
      if (!(table_ instanceof KuduTable)) {
        throw new AnalysisException("UPSERT is only supported for Kudu tables");
      }
    } else {
      analyzeTableForInsert(analyzer);
    }

    // Add target table to descriptor table.
    analyzer.getDescTbl().setTargetTable(table_);
  }

  /**
   * Performs INSERT-specific table analysis:
   * - Partition clause is invalid for unpartitioned or HBase tables
   * - Check INSERT privileges as well as write access to Hdfs paths
   * - Overwrite is invalid for HBase and Kudu tables
   */
  private void analyzeTableForInsert(Analyzer analyzer) throws AnalysisException {
    boolean isHBaseTable = (table_ instanceof HBaseTable);
    int numClusteringCols = isHBaseTable ? 0 : table_.getNumClusteringCols();

    if (partitionKeyValues_ != null && numClusteringCols == 0) {
      if (isHBaseTable) {
        throw new AnalysisException("PARTITION clause is not valid for INSERT into " +
            "HBase tables. '" + targetTableName_ + "' is an HBase table");

      } else {
        // Unpartitioned table, but INSERT has PARTITION clause
        throw new AnalysisException("PARTITION clause is only valid for INSERT into " +
            "partitioned table. '" + targetTableName_ + "' is not partitioned");
      }
    }

    if (table_ instanceof HdfsTable) {
      HdfsTable hdfsTable = (HdfsTable) table_;
      if (!hdfsTable.hasWriteAccess()) {
        throw new AnalysisException(String.format("Unable to INSERT into target table " +
            "(%s) because Impala does not have WRITE access to at least one HDFS path" +
            ": %s", targetTableName_, hdfsTable.getFirstLocationWithoutWriteAccess()));
      }
      StringBuilder error = new StringBuilder();
      hdfsTable.parseSkipHeaderLineCount(error);
      if (error.length() > 0) throw new AnalysisException(error.toString());
      try {
        if (!FileSystemUtil.isImpalaWritableFilesystem(hdfsTable.getLocation())) {
          throw new AnalysisException(String.format("Unable to INSERT into target " +
              "table (%s) because %s is not a supported filesystem.", targetTableName_,
              hdfsTable.getLocation()));
        }
      } catch (IOException e) {
        throw new AnalysisException(String.format("Unable to INSERT into target " +
            "table (%s): %s.", targetTableName_, e.getMessage()), e);
      }
      for (int colIdx = 0; colIdx < numClusteringCols; ++colIdx) {
        Column col = hdfsTable.getColumns().get(colIdx);
        // Hive has a number of issues handling BOOLEAN partition columns (see HIVE-6590).
        // Instead of working around the Hive bugs, INSERT is disabled for BOOLEAN
        // partitions in Impala. Once the Hive JIRA is resolved, we can remove this
        // analysis check.
        if (col.getType() == Type.BOOLEAN) {
          throw new AnalysisException(String.format("INSERT into table with BOOLEAN " +
              "partition column (%s) is not supported: %s", col.getName(),
              targetTableName_));
        }
      }
    }

    if (table_ instanceof KuduTable) {
      if (overwrite_) {
        throw new AnalysisException("INSERT OVERWRITE not supported for Kudu tables.");
      }
      if (partitionKeyValues_ != null && !partitionKeyValues_.isEmpty()) {
        throw new AnalysisException(
            "Partition specifications are not supported for Kudu tables.");
      }
    }

    if (isHBaseTable && overwrite_) {
      throw new AnalysisException("HBase doesn't have a way to perform INSERT OVERWRITE");
    }
  }

  /**
   * Checks that the column permutation + select list + static partition exprs + dynamic
   * partition exprs collectively cover exactly all required columns in the target table,
   * depending on the table type.
   */
  private void checkColumnCoverage(ArrayList<Column> selectExprTargetColumns,
      Set<String> mentionedColumnNames, int numSelectListExprs,
      int numStaticPartitionExprs) throws AnalysisException {
    // Check that all required cols are mentioned by the permutation and partition clauses
    if (selectExprTargetColumns.size() + numStaticPartitionExprs !=
        table_.getColumns().size()) {
      // We've already ruled out too many columns in the permutation and partition clauses
      // by checking that there are no duplicates and that every column mentioned actually
      // exists. So all columns aren't mentioned in the query.
      if (table_ instanceof KuduTable) {
        checkRequiredKuduColumns(mentionedColumnNames);
      } else if (table_ instanceof HBaseTable) {
        checkRequiredHBaseColumns(mentionedColumnNames);
      } else if (table_.getNumClusteringCols() > 0) {
        checkRequiredPartitionedColumns(mentionedColumnNames);
      }
    }

    // Expect the selectListExpr to have entries for every target column
    if (selectExprTargetColumns.size() != numSelectListExprs) {
      String comparator =
          (selectExprTargetColumns.size() < numSelectListExprs) ? "fewer" : "more";
      String partitionClause =
          (partitionKeyValues_ == null) ? "returns" : "and PARTITION clause return";

      // If there was no column permutation provided, the error is that the select-list
      // has the wrong number of expressions compared to the number of columns in the
      // table. If there was a column permutation, then the mismatch is between the
      // select-list and the permutation itself.
      if (columnPermutation_ == null) {
        int totalColumnsMentioned = numSelectListExprs + numStaticPartitionExprs;
        throw new AnalysisException(String.format(
            "Target table '%s' has %s columns (%s) than the SELECT / VALUES clause %s" +
            " (%s)", table_.getFullName(), comparator,
            table_.getColumns().size(), partitionClause, totalColumnsMentioned));
      } else {
        String partitionPrefix =
            (partitionKeyValues_ == null) ? "mentions" : "and PARTITION clause mention";
        throw new AnalysisException(String.format(
            "Column permutation %s %s columns (%s) than " +
            "the SELECT / VALUES clause %s (%s)", partitionPrefix, comparator,
            selectExprTargetColumns.size(), partitionClause, numSelectListExprs));
      }
    }
  }

  /**
   * For a Kudu table, checks that all key columns are mentioned.
   */
  private void checkRequiredKuduColumns(Set<String> mentionedColumnNames)
      throws AnalysisException {
    Preconditions.checkState(table_ instanceof KuduTable);
    List<String> keyColumns = ((KuduTable) table_).getPrimaryKeyColumnNames();
    List<String> missingKeyColumnNames = Lists.newArrayList();
    for (Column column : table_.getColumns()) {
      if (!mentionedColumnNames.contains(column.getName())
          && keyColumns.contains(column.getName())) {
        missingKeyColumnNames.add(column.getName());
      }
    }

    if (!missingKeyColumnNames.isEmpty()) {
      throw new AnalysisException(String.format(
          "All primary key columns must be specified for %sing into Kudu tables. " +
          "Missing columns are: %s", getOpName(),
          Joiner.on(", ").join(missingKeyColumnNames)));
    }
  }

  /**
   * For an HBase table, checks that the row-key column is mentioned.
   * HBase tables have a single row-key column which is always in position 0. It
   * must be mentioned, since it is invalid to set it to NULL (which would
   * otherwise happen by default).
   */
  private void checkRequiredHBaseColumns(Set<String> mentionedColumnNames)
      throws AnalysisException {
    Preconditions.checkState(table_ instanceof HBaseTable);
    Column column = table_.getColumns().get(0);
    if (!mentionedColumnNames.contains(column.getName())) {
      throw new AnalysisException("Row-key column '" + column.getName() +
          "' must be explicitly mentioned in column permutation.");
    }
  }

  /**
   * For partitioned tables, checks that all partition columns are mentioned.
   */
  private void checkRequiredPartitionedColumns(Set<String> mentionedColumnNames)
      throws AnalysisException {
    int numClusteringCols = table_.getNumClusteringCols();
    List<String> missingPartitionColumnNames = Lists.newArrayList();
    for (Column column : table_.getColumns()) {
      if (!mentionedColumnNames.contains(column.getName())
          && column.getPosition() < numClusteringCols) {
        missingPartitionColumnNames.add(column.getName());
      }
    }

    if (!missingPartitionColumnNames.isEmpty()) {
      throw new AnalysisException(
          "Not enough partition columns mentioned in query. Missing columns are: " +
          Joiner.on(", ").join(missingPartitionColumnNames));
    }
  }

  /**
   * Performs four final parts of the analysis:
   * 1. Checks type compatibility between all expressions and their targets
   *
   * 2. Populates partitionKeyExprs with type-compatible expressions, in Hive
   * partition-column order, for all partition columns
   *
   * 3. Populates resultExprs_ with type-compatible expressions, in Hive column order,
   * for all expressions in the select-list. Unmentioned columns are assigned NULL literal
   * expressions, unless the target is a Kudu table.
   *
   * 4. Result exprs for key columns of Kudu tables are stored in primaryKeyExprs_.
   *
   * If necessary, adds casts to the expressions to make them compatible with the type of
   * the corresponding column.
   *
   * @throws AnalysisException
   *           If an expression is not compatible with its target column
   */
  private void prepareExpressions(List<Column> selectExprTargetColumns,
      List<Expr> selectListExprs, Table tbl, Analyzer analyzer)
      throws AnalysisException {
    // Temporary lists of partition key exprs and names in an arbitrary order.
    List<Expr> tmpPartitionKeyExprs = new ArrayList<Expr>();
    List<String> tmpPartitionKeyNames = new ArrayList<String>();

    int numClusteringCols = (tbl instanceof HBaseTable) ? 0 : tbl.getNumClusteringCols();

    // Check dynamic partition columns for type compatibility.
    for (int i = 0; i < selectListExprs.size(); ++i) {
      Column targetColumn = selectExprTargetColumns.get(i);
      Expr compatibleExpr = checkTypeCompatibility(
          targetTableName_.toString(), targetColumn, selectListExprs.get(i));
      if (targetColumn.getPosition() < numClusteringCols) {
        // This is a dynamic clustering column
        tmpPartitionKeyExprs.add(compatibleExpr);
        tmpPartitionKeyNames.add(targetColumn.getName());
      }
      selectListExprs.set(i, compatibleExpr);
    }

    // Check static partition columns, dynamic entries in partitionKeyValues will already
    // be in selectExprTargetColumns and therefore are ignored in this loop
    if (partitionKeyValues_ != null) {
      for (PartitionKeyValue pkv: partitionKeyValues_) {
        if (pkv.isStatic()) {
          // tableColumns is guaranteed to exist after the earlier analysis checks
          Column tableColumn = table_.getColumn(pkv.getColName());
          Expr compatibleExpr = checkTypeCompatibility(
              targetTableName_.toString(), tableColumn, pkv.getLiteralValue());
          tmpPartitionKeyExprs.add(compatibleExpr);
          tmpPartitionKeyNames.add(pkv.getColName());
        }
      }
    }

    // Reorder the partition key exprs and names to be consistent with the target table
    // declaration.  We need those exprs in the original order to create the corresponding
    // Hdfs folder structure correctly.
    for (Column c: table_.getColumns()) {
      for (int j = 0; j < tmpPartitionKeyNames.size(); ++j) {
        if (c.getName().equals(tmpPartitionKeyNames.get(j))) {
          partitionKeyExprs_.add(tmpPartitionKeyExprs.get(j));
          break;
        }
      }
    }

    Preconditions.checkState(partitionKeyExprs_.size() == numClusteringCols);
    // Make sure we have stats for partitionKeyExprs
    for (Expr expr: partitionKeyExprs_) {
      expr.analyze(analyzer);
    }

    boolean isKuduTable = table_ instanceof KuduTable;
    // Finally, 'undo' the permutation so that the selectListExprs are in Hive column
    // order, and add NULL expressions to all missing columns, unless this is an UPSERT.
    ArrayList<Column> columns = table_.getColumnsInHiveOrder();
    for (int col = 0; col < columns.size(); ++col) {
      Column tblColumn = columns.get(col);
      boolean matchFound = false;
      for (int i = 0; i < selectListExprs.size(); ++i) {
        if (selectExprTargetColumns.get(i).getName().equals(tblColumn.getName())) {
          resultExprs_.add(selectListExprs.get(i));
          if (isKuduTable) mentionedColumns_.add(col);
          matchFound = true;
          break;
        }
      }
      // If no match is found, either the column is a clustering column with a static
      // value, or it was unmentioned and therefore should have a NULL select-list
      // expression if this is an INSERT and the target is not a Kudu table.
      if (!matchFound) {
        if (tblColumn.getPosition() >= numClusteringCols) {
          if (isKuduTable) {
            Preconditions.checkState(tblColumn instanceof KuduColumn);
            KuduColumn kuduCol = (KuduColumn) tblColumn;
            if (!kuduCol.hasDefaultValue() && !kuduCol.isNullable()) {
              throw new AnalysisException("Missing values for column that is not " +
                  "nullable and has no default value " + kuduCol.getName());
            }
          } else {
            // Unmentioned non-clustering columns get NULL literals with the appropriate
            // target type because Parquet cannot handle NULL_TYPE (IMPALA-617).
            resultExprs_.add(NullLiteral.create(tblColumn.getType()));
          }
        }
      }
      // Store exprs for Kudu key columns.
      if (matchFound && isKuduTable) {
        KuduTable kuduTable = (KuduTable) table_;
        if (kuduTable.isPrimaryKeyColumn(tblColumn.getName())) {
          primaryKeyExprs_.add(Iterables.getLast(resultExprs_));
        }
      }
    }

    if (table_ instanceof KuduTable) {
      Preconditions.checkState(!primaryKeyExprs_.isEmpty());
    }

    // TODO: Check that HBase row-key columns are not NULL? See IMPALA-406
    if (needsGeneratedQueryStatement_) {
      // Build a query statement that returns NULL for every column
      List<SelectListItem> selectListItems = Lists.newArrayList();
      for(Expr e: resultExprs_) {
        selectListItems.add(new SelectListItem(e, null));
      }
      SelectList selectList = new SelectList(selectListItems);
      queryStmt_ = new SelectStmt(selectList, null, null, null, null, null, null);
      queryStmt_.analyze(analyzer);
    }
  }

  private void analyzePlanHints(Analyzer analyzer) throws AnalysisException {
    if (!planHints_.isEmpty() && table_ instanceof HBaseTable) {
      throw new AnalysisException(String.format("INSERT hints are only supported for " +
          "inserting into Hdfs and Kudu tables: %s", getTargetTableName()));
    }
    boolean hasNoClusteredHint = false;
    for (PlanHint hint: planHints_) {
      if (hint.is("SHUFFLE")) {
        hasShuffleHint_ = true;
        analyzer.setHasPlanHints();
      } else if (hint.is("NOSHUFFLE")) {
        hasNoShuffleHint_ = true;
        analyzer.setHasPlanHints();
      } else if (hint.is("CLUSTERED")) {
        hasClusteredHint_ = true;
        analyzer.setHasPlanHints();
      } else if (hint.is("NOCLUSTERED")) {
        hasNoClusteredHint = true;
        analyzer.setHasPlanHints();
      } else if (hint.is("SORTBY")) {
        analyzeSortByHint(hint);
        analyzer.setHasPlanHints();
      } else {
        analyzer.addWarning("INSERT hint not recognized: " + hint);
      }
    }
    // Both flags may be false or one of them may be true, but not both.
    if (hasShuffleHint_ && hasNoShuffleHint_) {
      throw new AnalysisException("Conflicting INSERT hints: shuffle and noshuffle");
    }
    if (hasClusteredHint_ && hasNoClusteredHint) {
      throw new AnalysisException("Conflicting INSERT hints: clustered and noclustered");
    }
  }

  private void analyzeSortByHint(PlanHint hint) throws AnalysisException {
    // HBase tables don't support insert hints at all (must be enforced by the caller).
    Preconditions.checkState(!(table_ instanceof HBaseTable));

    if (isUpsert_) {
      throw new AnalysisException("SORTBY hint is not supported in UPSERT statements.");
    }

    List<String> columnNames = hint.getArgs();
    Preconditions.checkState(!columnNames.isEmpty());
    for (String columnName: columnNames) {
      // Make sure it's not a Kudu primary key column or Hdfs partition column.
      if (table_ instanceof KuduTable) {
        KuduTable kuduTable = (KuduTable) table_;
        if (kuduTable.isPrimaryKeyColumn(columnName)) {
          throw new AnalysisException(String.format("SORTBY hint column list must not " +
              "contain Kudu primary key column: '%s'", columnName));
        }
      } else {
        for (Column tableColumn: table_.getClusteringColumns()) {
          if (tableColumn.getName().equals(columnName)) {
            throw new AnalysisException(String.format("SORTBY hint column list must " +
                "not contain Hdfs partition column: '%s'", columnName));
          }
        }
      }

      // Find the matching column in the target table's column list (by name) and store
      // the corresponding result expr in sortByExprs_.
      boolean foundColumn = false;
      List<Column> columns = table_.getNonClusteringColumns();
      for (int i = 0; i < columns.size(); ++i) {
        if (columns.get(i).getName().equals(columnName)) {
          sortByExprs_.add(resultExprs_.get(i));
          sortByColumns_.add(i);
          foundColumn = true;
          break;
        }
      }
      if (!foundColumn) {
        throw new AnalysisException(String.format("Could not find SORTBY hint column " +
            "'%s' in table.", columnName));
      }
    }
  }

  @Override
  public ArrayList<Expr> getResultExprs() { return resultExprs_; }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    queryStmt_.rewriteExprs(rewriter);
  }

  private String getOpName() { return isUpsert_ ? "UPSERT" : "INSERT"; }

  public List<PlanHint> getPlanHints() { return planHints_; }
  public TableName getTargetTableName() { return targetTableName_; }
  public Table getTargetTable() { return table_; }
  public void setTargetTable(Table table) { this.table_ = table; }
  public boolean isOverwrite() { return overwrite_; }

  /**
   * Only valid after analysis
   */
  public QueryStmt getQueryStmt() { return queryStmt_; }
  public List<Expr> getPartitionKeyExprs() { return partitionKeyExprs_; }
  public boolean hasShuffleHint() { return hasShuffleHint_; }
  public boolean hasNoShuffleHint() { return hasNoShuffleHint_; }
  public boolean hasClusteredHint() { return hasClusteredHint_; }
  public ArrayList<Expr> getPrimaryKeyExprs() { return primaryKeyExprs_; }
  public List<Expr> getSortByExprs() { return sortByExprs_; }

  public List<String> getMentionedColumns() {
    List<String> result = Lists.newArrayList();
    List<Column> columns = table_.getColumns();
    for (Integer i: mentionedColumns_) result.add(columns.get(i).getName());
    return result;
  }

  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(table_ != null);
    return TableSink.create(table_, isUpsert_ ? TableSink.Op.UPSERT : TableSink.Op.INSERT,
        partitionKeyExprs_, mentionedColumns_, overwrite_, hasClusteredHint_,
        sortByColumns_);
  }

  /**
   * Substitutes the result expressions, the partition key expressions, and the primary
   * key expressions with smap. Preserves the original types of those expressions during
   * the substitution.
   */
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    resultExprs_ = Expr.substituteList(resultExprs_, smap, analyzer, true);
    partitionKeyExprs_ = Expr.substituteList(partitionKeyExprs_, smap, analyzer, true);
    primaryKeyExprs_ = Expr.substituteList(primaryKeyExprs_, smap, analyzer, true);
    sortByExprs_ = Expr.substituteList(sortByExprs_, smap, analyzer, true);
  }

  @Override
  public String toSql() {
    StringBuilder strBuilder = new StringBuilder();

    if (withClause_ != null) strBuilder.append(withClause_.toSql() + " ");

    strBuilder.append(getOpName() + " ");
    if (overwrite_) {
      strBuilder.append("OVERWRITE ");
    } else {
      strBuilder.append("INTO ");
    }
    strBuilder.append("TABLE " + originalTableName_);
    if (columnPermutation_ != null) {
      strBuilder.append("(");
      strBuilder.append(Joiner.on(", ").join(columnPermutation_));
      strBuilder.append(")");
    }
    if (partitionKeyValues_ != null) {
      List<String> values = Lists.newArrayList();
      for (PartitionKeyValue pkv: partitionKeyValues_) {
        values.add(pkv.getColName() +
            (pkv.getValue() != null ? ("=" + pkv.getValue().toSql()) : ""));
      }
      strBuilder.append(" PARTITION (" + Joiner.on(", ").join(values) + ")");
    }
    if (!planHints_.isEmpty()) {
      strBuilder.append(" " + ToSqlUtils.getPlanHintsSql(getPlanHints()));
    }
    if (!needsGeneratedQueryStatement_) {
      strBuilder.append(" " + queryStmt_.toSql());
    }
    return strBuilder.toString();
  }
}
