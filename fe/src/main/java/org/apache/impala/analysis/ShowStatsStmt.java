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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.impala.analysis.paimon.PaimonAnalyzer;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.Pair;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.IcebergPartitionPredicateConverter;
import org.apache.impala.planner.HdfsPartitionPruner;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.rewrite.ExtractCompoundVerticalBarExprRule;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TShowStatsOp;
import org.apache.impala.thrift.TShowStatsParams;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;

/**
 * Representation of a SHOW TABLE/COLUMN STATS statement for
 * displaying column and table/partition statistics for a given table.
 */
public class ShowStatsStmt extends StatementBase implements SingleTableStmt {
  protected final TShowStatsOp op_;
  protected final TableName tableName_;
  protected boolean show_column_minmax_stats_ = false;
  // Optional WHERE predicate for SHOW PARTITIONS.
  // Supported for HDFS and Iceberg tables.
  protected Expr whereClause_;

  // Computed during analysis if whereClause_ is set for HDFS tables.
  protected List<Long> filteredPartitionIds_ = null;

  // For Iceberg tables with WHERE clause, store the pre-computed filtered
  // iceberg partition stats.
  // This is computed during analysis and serialized through Thrift.
  protected TResultSet filteredIcebergPartitionStats_ = null;

  // Set during analysis.
  protected FeTable table_;

  public ShowStatsStmt(TableName tableName, TShowStatsOp op) {
    this(tableName, op, null);
  }

  public ShowStatsStmt(TableName tableName, TShowStatsOp op, Expr whereExpr) {
    op_ = Preconditions.checkNotNull(op);
    tableName_ = Preconditions.checkNotNull(tableName);
    whereClause_ = whereExpr;
  }

  @Override
  public TableName getTableName() { return tableName_; }

  /**
   * Returns true if this SHOW PARTITIONS statement has a WHERE clause.
   */
  public boolean hasWhereClause() { return whereClause_ != null; }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder();
    sb.append(getSqlPrefix()).append(" ").append(tableName_.toString());
    if (whereClause_ != null && op_ == TShowStatsOp.PARTITIONS) {
      sb.append(" WHERE ").append(whereClause_.toSql(options));
    }
    return sb.toString();
  }

  protected String getSqlPrefix() {
    if (op_ == TShowStatsOp.TABLE_STATS) {
      return "SHOW TABLE STATS";
    } else if (op_ == TShowStatsOp.COLUMN_STATS) {
      return "SHOW COLUMN STATS";
    } else if (op_ == TShowStatsOp.PARTITIONS) {
      return "SHOW PARTITIONS";
    } else if (op_ == TShowStatsOp.RANGE_PARTITIONS) {
      return "SHOW RANGE PARTITIONS";
    } else if (op_ == TShowStatsOp.HASH_SCHEMA) {
      return "SHOW HASH_SCHEMA";
    } else {
      Preconditions.checkState(false);
      return "";
    }
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    table_ = analyzer.getTable(tableName_, Privilege.VIEW_METADATA);
    Preconditions.checkNotNull(table_);
    if (table_ instanceof FeView) {
      throw new AnalysisException(String.format(
          "%s not applicable to a view: %s", getSqlPrefix(), table_.getFullName()));
    }
    if (table_ instanceof FeFsTable) {
      // There two cases here: Non-partitioned hdfs table and non-partitioned
      // iceberg table
      boolean partitioned = true;
      if (op_ == TShowStatsOp.PARTITIONS) {
        if (table_ instanceof FeIcebergTable) {
          FeIcebergTable feIcebergTable = (FeIcebergTable) table_;
          // We only get latest partition spec from Iceberg now, so this list only
          // contains one partition spec member.
          // Iceberg snapshots chosen maybe supported in the future.
          Preconditions.checkNotNull(feIcebergTable.getPartitionSpecs());
          // Partition spec without partition fields is non-partitioned.
          if (!(feIcebergTable.getDefaultPartitionSpec().hasPartitionFields())) {
            partitioned = false;
          }
        } else {
          if (table_.getNumClusteringCols() == 0) {
            partitioned = false;
          }
        }
      }
      if (!partitioned) {
        throw new AnalysisException("Table is not partitioned: " + table_.getFullName());
      }
      if (op_ == TShowStatsOp.RANGE_PARTITIONS || op_ == TShowStatsOp.HASH_SCHEMA) {
        throw new AnalysisException(getSqlPrefix() + " must target a Kudu table: " +
            table_.getFullName());
      }
    } else if (table_ instanceof FePaimonTable) {
      PaimonAnalyzer.analyzeShowStatStmt(this, (FePaimonTable) table_, analyzer);
    } else if (table_ instanceof FeKuduTable) {
      FeKuduTable kuduTable = (FeKuduTable) table_;
      if ((op_ == TShowStatsOp.RANGE_PARTITIONS || op_ == TShowStatsOp.HASH_SCHEMA) &&
          FeKuduTable.Utils.getRangePartitioningColNames(kuduTable).isEmpty()) {
        throw new AnalysisException(getSqlPrefix() + " requested but table does not " +
            "have range partitions: " + table_.getFullName());
      }
    } else {
      if (op_ == TShowStatsOp.RANGE_PARTITIONS || op_ == TShowStatsOp.HASH_SCHEMA) {
        throw new AnalysisException(getSqlPrefix() + " must target a Kudu table: " +
            table_.getFullName());
      } else if (op_ == TShowStatsOp.PARTITIONS) {
        throw new AnalysisException(getSqlPrefix() +
            " must target an HDFS or Kudu table: " + table_.getFullName());
      }
    }
    show_column_minmax_stats_ =
        analyzer.getQueryOptions().isShow_column_minmax_stats();

    // If WHERE clause is present for SHOW PARTITIONS on HDFS table, analyze and compute
    // filtered IDs.
    if (whereClause_ != null && op_ == TShowStatsOp.PARTITIONS) {
      analyzeWhereClause(analyzer);
    }
  }

  /**
   * Analyzes the WHERE clause for SHOW PARTITIONS on HDFS tables and computes
   * the filtered partition IDs.
   */
  private void analyzeWhereClause(Analyzer analyzer) throws AnalysisException {
    if (!(table_ instanceof FeFsTable)) {
      throw new AnalysisException(
        "WHERE clause in SHOW PARTITIONS is only supported for HDFS and Iceberg tables");
    }

    // Disable authorization checks for internal analysis of WHERE clause
    analyzer.setEnablePrivChecks(false);
    try {
      TableName qualifiedName = new TableName(
        table_.getDb().getName(), table_.getName());
      TableRef tableRef = new TableRef(
        qualifiedName.toPath(), null, Privilege.VIEW_METADATA);
      tableRef = analyzer.resolveTableRef(tableRef);
      tableRef.analyze(analyzer);

      // Analyze the WHERE predicate if not already analyzed
      if (!whereClause_.isAnalyzed()) {
        whereClause_.analyze(analyzer);
      }
      whereClause_.checkReturnsBool("WHERE clause", true);

      // Check if the WHERE clause contains CompoundVerticalBarExpr (||) that needs
      // rewriting.
      List<CompoundVerticalBarExpr> compoundVerticalBarExprs = new ArrayList<>();
      whereClause_.collectAll(Predicates.instanceOf(CompoundVerticalBarExpr.class),
          compoundVerticalBarExprs);
      if (!compoundVerticalBarExprs.isEmpty()) {
        // Expression needs rewriting - defer partition filtering to second analysis.
        return;
      }
      // Check if the WHERE clause contains Subquery or AnalyticExpr.
      if (whereClause_.contains(Subquery.class)) {
        throw new AnalysisException(
          "Subqueries are not allowed in SHOW PARTITIONS WHERE");
      }
      if (whereClause_.contains(AnalyticExpr.class)) {
        throw new AnalysisException(
          "Analytic expressions are not allowed in SHOW PARTITIONS WHERE");
      }
      // Aggregate functions cannot be evaluated per-partition.
      if (whereClause_.contains(Expr.IS_AGGREGATE)) {
        throw new AnalysisException(
          "Aggregate functions are not allowed in SHOW PARTITIONS WHERE");
      }

      // Handle Iceberg tables separately
      if (table_ instanceof FeIcebergTable) {
        analyzeIcebergWhereClause(analyzer, (FeIcebergTable) table_);
      } else {
        // Handle HDFS tables
        analyzeHdfsWhereClause(analyzer, tableRef);
      }
    } finally {
      // Re-enable authorization checks
      analyzer.setEnablePrivChecks(true);
    }
  }

  /**
   * Analyzes the WHERE clause for Iceberg tables and computes filtered partition stats.
   * Converts the WHERE expression to an Iceberg Expression and evaluates it against
   * the table's manifest files. Following the IMPALA-12243 pattern, we compute results
   * during analysis (when Analyzer exists) and serialize the results rather than the
   * expression, to avoid recreating QueryContext/Analyzer.
   *
   * Note: Only simple predicates on partition columns are supported for Iceberg tables.
   * Functions (deterministic or non-deterministic), aggregates, analytics, and subqueries
   * are not supported. For complex queries with functions, use Iceberg metadata tables:
   *   SHOW PARTITIONS functional_parquet.iceberg_partitioned
   *   WHERE upper(action) = 'CLICK'
   *     -- Not supported due to upper() function
   * Instead query the metadata table directly:
   *   SELECT `partition` FROM functional_parquet.iceberg_partitioned.`partitions`
   *   WHERE upper(`partition`.action) = 'CLICK'
   *
   * TODO: IMPALA-14675: Add support for more complex predicates by evaluating them
   * per-partition using FeSupport.EvalPredicateBatch(), similar to HDFS tables.
   */
  private void analyzeIcebergWhereClause(Analyzer analyzer, FeIcebergTable table)
      throws AnalysisException {
    // Rewrite expressions for Iceberg partition transforms and column references
    // This converts Iceberg partition transform functions to IcebergPartitionExpr
    // and also handles BETWEEN rewriting internally
    IcebergPartitionExpressionRewriter rewriter =
        new IcebergPartitionExpressionRewriter(analyzer,
            table.getIcebergApiTable().spec());

    try {
      Expr rewrittenExpr = rewriter.rewrite(whereClause_);
      rewrittenExpr.analyze(analyzer);

      // Defensive check: all FunctionCallExprs should have been converted by the rewriter
      // If any remain, it indicates an internal error in the rewriter logic
      Preconditions.checkState(!rewrittenExpr.contains(FunctionCallExpr.class),
          "Rewriter should have converted or rejected all function calls, but found: %s",
          rewrittenExpr.toSql());

      // Constant-fold the expression
      Expr foldedExpr = analyzer.getConstantFolder().rewrite(rewrittenExpr, analyzer);

      // Convert the Impala expression to an Iceberg expression
      // BoolLiterals are handled by the converter and optimized in getPartitionStats
      IcebergPartitionPredicateConverter converter =
          new IcebergPartitionPredicateConverter(table.getIcebergSchema(), analyzer);
      org.apache.iceberg.expressions.Expression icebergExpr =
          converter.convert(foldedExpr);

      // Compute the filtered partition stats using the Iceberg Expression
      filteredIcebergPartitionStats_ =
          FeIcebergTable.Utils.getPartitionStats(table, icebergExpr);

    } catch (org.apache.impala.common.ImpalaException e) {
      // Catch errors from Iceberg expression conversion or partition stats computation
      throw new AnalysisException(
          "Invalid partition filtering expression: " + whereClause_.toSql() +
          ".\n" + e.getMessage());
    }
  }

  /**
   * Analyzes the WHERE clause for HDFS tables and computes filtered partition IDs.
   */
  private void analyzeHdfsWhereClause(Analyzer analyzer, TableRef tableRef)
      throws AnalysisException {
    Preconditions.checkArgument(tableRef.getTable() instanceof FeFsTable);
    // Ensure all conjuncts reference only partition columns.
    List<SlotId> partitionSlots = tableRef.getDesc().getPartitionSlots();
    if (!whereClause_.isBoundBySlotIds(partitionSlots)) {
      throw new AnalysisException(
        "SHOW PARTITIONS WHERE supports only partition columns");
    }

    // Prune the partitions using the HdfsPartitionPruner.
    HdfsPartitionPruner pruner = new HdfsPartitionPruner(tableRef.getDesc());

    try {
      // Clone the conjuncts because the pruner will modify the original list.
      List<Expr> conjunctsCopy = new ArrayList<>(whereClause_.getConjuncts());
      // Pass evalAllFuncs=true to ensure non-deterministic functions are evaluated
      // per-partition instead of being skipped.
      Pair<List<? extends FeFsPartition>, List<Expr>> res =
          pruner.prunePartitions(analyzer, conjunctsCopy, true, true, tableRef);
      Preconditions.checkState(conjunctsCopy.isEmpty(),
          "All conjuncts should be evaluated");

      // All partitions from the pruner have matched - collect their IDs.
      Set<Long> ids = new HashSet<>();
      for (FeFsPartition p : res.first) ids.add(p.getId());
      filteredPartitionIds_ = new ArrayList<>(ids);
    } catch (org.apache.impala.common.ImpalaException e) {
      throw new AnalysisException(
        "Failed to evaluate WHERE clause for SHOW PARTITIONS: " + e.getMessage(), e);
    }
  }

  @Override
  public void reset() {
    super.reset();
    // Clear computed partition IDs and filtered stats so they'll be
    // recomputed after re-analysis
    filteredPartitionIds_ = null;
    filteredIcebergPartitionStats_ = null;
    // Reset the whereExpr if it exists
    if (whereClause_ != null) whereClause_ = whereClause_.reset();
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    // For SHOW PARTITIONS with WHERE clause, we need to do mandatory rewrites for ||
    // operator as the partition pruner cannot execute it directly.
    if (whereClause_ != null && op_ == TShowStatsOp.PARTITIONS) {
      ExprRewriter mandatoryRewriter = new ExprRewriter(
        ExtractCompoundVerticalBarExprRule.INSTANCE);
      whereClause_ = mandatoryRewriter.rewrite(whereClause_, analyzer_);
      rewriter.addNumChanges(mandatoryRewriter);
    }
  }

  public TShowStatsParams toThrift() {
    // Ensure the DB is set in the table_name field by using table and not tableName.
    TShowStatsParams showStatsParam = new TShowStatsParams(op_,
        new TableName(table_.getDb().getName(), table_.getName()).toThrift());
    showStatsParam.setShow_column_minmax_stats(show_column_minmax_stats_);
    // Always set filteredPartitionIds if it exists (even if empty) to distinguish
    // between "no matches" (empty list) and "all partitions" (null)
    if (filteredPartitionIds_ != null) {
      showStatsParam.setFiltered_partition_ids(filteredPartitionIds_);
    }
    // For Iceberg tables with WHERE clause, pass the pre-computed
    // filtered partition stats.
    if (filteredIcebergPartitionStats_ != null) {
      showStatsParam.setFiltered_iceberg_partition_stats(filteredIcebergPartitionStats_);
    }
    return showStatsParam;
  }
}
