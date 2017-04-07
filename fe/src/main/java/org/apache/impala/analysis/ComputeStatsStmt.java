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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TComputeStatsParams;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TTableName;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Represents the following statements for statistics collection. Which statistics
 * are computed and stored depends on the statement type (incremental or not), the
 * clauses used (sampling, partition spec), as well as whether stats extrapolation
 * is enabled or not.
 * Stats extrapolation can be configured:
 * - at the impalad level with --enable_stats_extrapolation
 * - at the table level HdfsTable.TBL_PROP_ENABLE_STATS_EXTRAPOLATION
 *
 * 1. COMPUTE STATS <table> [(col_list)] [TABLESAMPLE SYSTEM(<perc>) [REPEATABLE(<seed>)]]
 * - Stats extrapolation enabled:
 *   Computes and replaces the table-level row count and total file size, as well as all
 *   table-level column statistics. Existing partition-objects and their row count are
 *   not modified at all. The TABLESAMPLE clause can be used to limit the scanned data
 *   volume to a desired percentage. When sampling, the COMPUTE STATS queries directly
 *   produce extrapolated stats which are then stored in the HMS via the CatalogServer.
 *   We store extrapolated stats in the HMS so as not to confuse other engines like
 *   Hive/SparkSQL which may rely on the shared HMS fields representing to the whole
 *   table and not a sample. See {@link CatalogOpExecutor#getExtrapolatedStatsVal}.
 * - Stats extrapolation disabled:
 *   Computes and replaces the table-level row count and total file size, the row counts
 *   for all partitions (if applicable), as well as all table-level column statistics.
 *   The TABLESAMPLE clause is not supported to simplify implementation and testing. In
 *   particular, we want to avoid implementing and testing code for updating all HMS
 *   partitions to set the extrapolated numRows statistic. Altering many partitions is
 *   expensive and so should be avoided in favor of enabling extrapolation.
 *
 *   By default, statistics are computed for all columns. To control which columns are
 *   analyzed, a whitelist of columns names can be optionally specified.
 *
 * 2. COMPUTE INCREMENTAL STATS <table> [PARTITION <part_spec>]
 * - Stats extrapolation enabled:
 *   Not supported for now to keep the logic/code simple.
 * - Stats extrapolation disabled:
 *   Computes and replaces the table and partition-level row counts. Computes mergeable
 *   per-partition column statistics (HLL intermediate state) and stores them in the HMS.
 *   Computes and replaces the table-level column statistics by merging the
 *   partition-level column statistics.
 *   Instead of recomputing those statistics for all partitions, this command reuses
 *   existing statistics from partitions which already have incremental statistics.
 *   If a set of partitions is specified, then the incremental statistics for those
 *   partitions are recomputed (then merged into the table-level statistics).
 *
 * TODO: Allow more coarse (db)
 * TODO: Compute stats on complex types.
 */
public class ComputeStatsStmt extends StatementBase {
  private static final Logger LOG = Logger.getLogger(ComputeStatsStmt.class);

  private static String AVRO_SCHEMA_MSG_PREFIX = "Cannot COMPUTE STATS on Avro table " +
      "'%s' because its column definitions do not match those in the Avro schema.";
  private static String AVRO_SCHEMA_MSG_SUFFIX = "Please re-create the table with " +
          "column definitions, e.g., using the result of 'SHOW CREATE TABLE'";

  protected final TableName tableName_;
  protected final TableSampleClause sampleParams_;

  // Set during analysis.
  protected Table table_;

  // Effective sampling percent based on the total number of bytes in the files sample.
  // Set to -1 for non-HDFS tables or if TABLESAMPLE was not specified.
  // We run the regular COMPUTE STATS for 0.0 and 1.0 where sampling has no benefit.
  protected double effectiveSamplePerc_ = -1;

  // The Null count is not currently being used in optimization or run-time,
  // and compute stats runs 2x faster in many cases when not counting NULLs.
  private static final boolean COUNT_NULLS = false;

  // Query for getting the per-partition row count and the total row count.
  // Set during analysis.
  protected String tableStatsQueryStr_;

  // Query for getting the per-column NDVs and number of NULLs.
  // Set during analysis.
  protected String columnStatsQueryStr_;

  // If true, stats will be gathered incrementally per-partition.
  private boolean isIncremental_ = false;

  // If true, expect the compute stats process to produce output for all partitions in the
  // target table. In that case, 'expectedPartitions_' will be empty. The point of this
  // flag is to optimize the case where all partitions are targeted.
  // False for unpartitioned HDFS tables, non-HDFS tables or when stats extrapolation
  // is enabled.
  private boolean expectAllPartitions_ = false;

  // The list of valid partition statistics that can be used in an incremental computation
  // without themselves being recomputed. Populated in analyze().
  private final List<TPartitionStats> validPartStats_ = Lists.newArrayList();

  // For incremental computations, the list of partitions (identified by list of partition
  // column values) that we expect to receive results for. Used to ensure that even empty
  // partitions emit results.
  // TODO: Consider using partition IDs (and adding them to the child queries with a
  // PARTITION_ID() builtin)
  private final List<List<String>> expectedPartitions_ = Lists.newArrayList();

  // If non-null, partitions that an incremental computation might apply to. Must be
  // null if this is a non-incremental computation.
  private PartitionSet partitionSet_ = null;

  // If non-null, represents the user-specified list of columns for computing statistics.
  // Not supported for incremental statistics.
  private List<String> columnWhitelist_ = null;

  // The set of columns to be analyzed. Each column is valid: it must exist in the table
  // schema, it must be of a type that can be analyzed, and cannot refer to a partitioning
  // column for HDFS tables. If the set is null, no columns are restricted.
  private Set<Column> validatedColumnWhitelist_ = null;

  // The maximum number of partitions that may be explicitly selected by filter
  // predicates. Any query that selects more than this automatically drops back to a full
  // incremental stats recomputation.
  // TODO: We can probably do better than this, e.g. running several queries, each of
  // which selects up to MAX_INCREMENTAL_PARTITIONS partitions.
  private static final int MAX_INCREMENTAL_PARTITIONS = 1000;

  /**
   * Should only be constructed via static creation functions.
   */
  private ComputeStatsStmt(TableName tableName, TableSampleClause sampleParams,
      boolean isIncremental, PartitionSet partitionSet, List<String> columns) {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    Preconditions.checkState(isIncremental || partitionSet == null);
    Preconditions.checkState(!isIncremental || sampleParams == null);
    Preconditions.checkState(!isIncremental || columns == null);
    tableName_ = tableName;
    sampleParams_ = sampleParams;
    table_ = null;
    isIncremental_ = isIncremental;
    partitionSet_ = partitionSet;
    columnWhitelist_ = columns;
    if (partitionSet_ != null) {
      partitionSet_.setTableName(tableName);
      partitionSet_.setPrivilegeRequirement(Privilege.ALTER);
    }
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  /**
   * Returns a stmt for COMPUTE STATS. The optional 'sampleParams' indicates whether the
   * stats should be computed with table sampling.
   */
  public static ComputeStatsStmt createStatsStmt(TableName tableName,
      TableSampleClause sampleParams, List<String> columns) {
    return new ComputeStatsStmt(tableName, sampleParams, false, null, columns);
  }

  /**
   * Returns a stmt for COMPUTE INCREMENTAL STATS. The optional 'partitionSet' specifies a
   * set of partitions whose stats should be computed.
   */
  public static ComputeStatsStmt createIncrementalStatsStmt(TableName tableName,
      PartitionSet partitionSet) {
    return new ComputeStatsStmt(tableName, null, true, partitionSet, null);
  }

  private List<String> getBaseColumnStatsQuerySelectList(Analyzer analyzer) {
    List<String> columnStatsSelectList = Lists.newArrayList();
    // For Hdfs tables, exclude partition columns from stats gathering because Hive
    // cannot store them as part of the non-partition column stats. For HBase tables,
    // include the single clustering column (the row key).
    int startColIdx = (table_ instanceof HBaseTable) ? 0 : table_.getNumClusteringCols();

    for (int i = startColIdx; i < table_.getColumns().size(); ++i) {
      Column c = table_.getColumns().get(i);
      if (validatedColumnWhitelist_ != null && !validatedColumnWhitelist_.contains(c)) {
        continue;
      }
      if (ignoreColumn(c)) continue;

      // NDV approximation function. Add explicit alias for later identification when
      // updating the Metastore.
      String colRefSql = ToSqlUtils.getIdentSql(c.getName());
      if (isIncremental_) {
        columnStatsSelectList.add("NDV_NO_FINALIZE(" + colRefSql + ") AS " + colRefSql);
      } else if (isSampling()) {
        columnStatsSelectList.add(String.format("SAMPLED_NDV(%s, %.10f) AS %s",
            colRefSql, effectiveSamplePerc_, colRefSql));
      } else {
        // Regular (non-incremental) compute stats without sampling.
        columnStatsSelectList.add("NDV(" + colRefSql + ") AS " + colRefSql);
      }

      if (COUNT_NULLS) {
        // Count the number of NULL values.
        columnStatsSelectList.add("COUNT(IF(" + colRefSql + " IS NULL, 1, NULL))");
      } else {
        // Using -1 to indicate "unknown". We need cast to BIGINT because backend expects
        // an i64Val as the number of NULLs returned by the COMPUTE STATS column stats
        // child query. See CatalogOpExecutor::SetColumnStats(). If we do not cast, then
        // the -1 will be treated as TINYINT resulting a 0 to be placed in the #NULLs
        // column (see IMPALA-1068).
        columnStatsSelectList.add("CAST(-1 as BIGINT)");
      }

      // For STRING columns also compute the max and avg string length.
      Type type = c.getType();
      if (type.isStringType()) {
        columnStatsSelectList.add("MAX(length(" + colRefSql + "))");
        columnStatsSelectList.add("AVG(length(" + colRefSql + "))");
      } else {
        // For non-STRING columns we use the fixed size of the type.
        // We store the same information for all types to avoid having to
        // treat STRING columns specially in the BE CatalogOpExecutor.
        Integer typeSize = type.getPrimitiveType().getSlotSize();
        columnStatsSelectList.add(typeSize.toString());
        columnStatsSelectList.add("CAST(" + typeSize.toString() + " as DOUBLE)");
      }

      if (isIncremental_) {
        // Need the count in order to properly combine per-partition column stats
        columnStatsSelectList.add("COUNT(" + colRefSql + ")");
      }
    }
    return columnStatsSelectList;
  }

  /**
   * Constructs two SQL queries for computing the row-count and column statistics and
   * sets them in 'tableStatsQueryStr_' and 'columnStatsQueryStr_', respectively.
   * The queries are generated as follows.
   *
   * 1. Regular COMPUTE STATS (not incremental and no sampling)
   * 1.1 Row counts:
   * SELECT COUNT(*) FROM tbl [GROUP BY part_col1, part_col2 ...]
   * The GROUP BY clause is added if the target is a partitioned HDFS table and
   * stats extrapolation is disabled. Otherwise, no GROUP BY is used.
   *
   * 1.2 Column stats:
   * SELECT NDV(c1), CAST(-1 as typeof(c1)), MAX(length(c1)), AVG(length(c1)),
   *        NDV(c2), CAST(-1 as typeof(c2)), MAX(length(c2)), AVG(length(c2)),
   *        ...
   * FROM tbl
   *
   * 2. COMPUTE STATS with TABLESAMPLE
   * 2.1 Row counts:
   * SELECT ROUND(COUNT(*) / <effective_sample_perc>)
   * FROM tbl TABLESAMPLE SYSTEM(<sample_perc>) REPEATABLE (<random_seed>)
   *
   * 2.1 Column stats:
   * SELECT SAMPLED_NDV(c1, p), CAST(-1 as typeof(c1)), MAX(length(c1)), AVG(length(c1)),
   *        SAMPLED_NDV(c2, p), CAST(-1 as typeof(c2)), MAX(length(c2)), AVG(length(c2)),
   *        ...
   * FROM tbl TABLESAMPLE SYSTEM(<sample_perc>) REPEATABLE (<random_seed>)
   * SAMPLED_NDV() is a specialized aggregation function that estimates the NDV based on
   * a sample. The "p" passed to the SAMPLED_NDV() is the effective sampling rate.
   *
   * 3. COMPUTE INCREMENTAL STATS
   * 3.1 Row counts:
   * SELECT COUNT(*) FROM tbl GROUP BY part_col1, part_col2 ...
   * [WHERE ((part_col1 = p1_val1) AND (part_col2 = p1_val2)) OR
   *        ((part_col1 = p2_val1) AND (part_col2 = p2_val2)) OR ...]
   * The WHERE clause is constructed to select the relevant partitions.
   *
   * 3.2 Column stats:
   * SELECT NDV_NO_FINALIZE(c1), <nulls, max, avg>, COUNT(c1),
   *        NDV_NO_FINALIZE(c2), <nulls, max, avg>, COUNT(c2),
   *        ...
   * FROM tbl
   * GROUP BY part_col1, part_col2, ...
   * [WHERE ((part_col1 = p1_val1) AND (part_col2 = p1_val2)) OR
   *       ((part_col1 = p2_val1) AND (part_col2 = p2_val2)) OR ...]
   * The WHERE clause is constructed to select the relevant partitions.
   * NDV_NO_FINALIZE() produces a non-finalized HyperLogLog intermediate byte array.
   *
   * 4. For all COMPUTE STATS variants:
   * - The MAX() and AVG() for the column stats queries are only relevant for var-len
   *   columns like STRING. For fixed-len columns MAX() and AVG() are replaced with the
   *   appropriate literals.
   * - Queries will be set to null if we can detect that no work needs to be performed.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Resolve and analyze this table ref so we can evaluate partition predicates.
    TableRef tableRef = new TableRef(tableName_.toPath(), null, Privilege.ALTER);
    tableRef = analyzer.resolveTableRef(tableRef);
    Preconditions.checkNotNull(tableRef);
    tableRef.analyze(analyzer);
    if (tableRef instanceof InlineViewRef) {
      throw new AnalysisException(String.format(
          "COMPUTE STATS not supported for view: %s", tableName_));
    }
    if (tableRef instanceof CollectionTableRef) {
      throw new AnalysisException(String.format(
          "COMPUTE STATS not supported for nested collection: %s", tableName_));
    }
    table_ = analyzer.getTable(tableName_, Privilege.ALTER);

    if (!(table_ instanceof HdfsTable)) {
      if (partitionSet_ != null) {
        throw new AnalysisException("COMPUTE INCREMENTAL ... PARTITION not supported " +
            "for non-HDFS table " + tableName_);
      }
      isIncremental_ = false;
    }

    if (columnWhitelist_ != null) {
      validatedColumnWhitelist_ = Sets.newHashSet();
      for (String colName : columnWhitelist_) {
        Column col = table_.getColumn(colName);
        if (col == null) {
          throw new AnalysisException(colName + " not found in table: " +
              table_.getName());
        }
        if (table_ instanceof HdfsTable && table_.isClusteringColumn(col)) {
          throw new AnalysisException("COMPUTE STATS not supported for partitioning " +
              "column " + col.getName() + " of HDFS table.");
        }
        if (ignoreColumn(col)) {
          throw new AnalysisException("COMPUTE STATS not supported for column " +
              col.getName() + " of complex type:" + col.getType().toSql());
        }
        validatedColumnWhitelist_.add(col);
      }
    }

    HdfsTable hdfsTable = null;
    if (table_ instanceof HdfsTable) {
      hdfsTable = (HdfsTable)table_;
      if (hdfsTable.isAvroTable()) checkIncompleteAvroSchema(hdfsTable);
      if (isIncremental_ && hdfsTable.getNumClusteringCols() == 0 &&
          partitionSet_ != null) {
        throw new AnalysisException(String.format(
            "Can't compute PARTITION stats on an unpartitioned table: %s",
            tableName_));
      } else if (partitionSet_ != null) {
        Preconditions.checkState(tableRef instanceof BaseTableRef);
        partitionSet_.setPartitionShouldExist();
        partitionSet_.analyze(analyzer);
      }

      // For incremental stats, estimate the size of intermediate stats and report an
      // error if the estimate is greater than --inc_stats_size_limit_bytes in bytes
      if (isIncremental_) {
        long incStatMaxSize = BackendConfig.INSTANCE.getIncStatsMaxSize();
        long statsSizeEstimate = hdfsTable.getColumns().size() *
            hdfsTable.getPartitions().size() * HdfsTable.STATS_SIZE_PER_COLUMN_BYTES;
        if (statsSizeEstimate > incStatMaxSize) {
          LOG.error("Incremental stats size estimate for table " + hdfsTable.getName() +
              " exceeded " + incStatMaxSize + ", estimate = "
              + statsSizeEstimate);
          throw new AnalysisException("Incremental stats size estimate exceeds "
              + PrintUtils.printBytes(incStatMaxSize)
              + ". Please try COMPUTE STATS instead.");
        }
      }
    }

    // Build partition filters that only select partitions without valid statistics for
    // incremental computation.
    List<String> filterPreds = Lists.newArrayList();
    if (isIncremental_) {
      if (partitionSet_ == null) {
        // If any column does not have stats, we recompute statistics for all partitions
        // TODO: need a better way to invalidate stats for all partitions, so that we can
        // use this logic to only recompute new / changed columns.
        boolean tableIsMissingColStats = false;

        // We'll warn the user if a column is missing stats (and therefore we rescan the
        // whole table), but if all columns are missing stats, the table just doesn't
        // have any stats and there's no need to warn.
        boolean allColumnsMissingStats = true;
        String exampleColumnMissingStats = null;
        // Partition columns always have stats, so exclude them from this search
        for (Column col: table_.getNonClusteringColumns()) {
          if (ignoreColumn(col)) continue;
          if (!col.getStats().hasStats()) {
            if (!tableIsMissingColStats) {
              tableIsMissingColStats = true;
              exampleColumnMissingStats = col.getName();
            }
          } else {
            allColumnsMissingStats = false;
          }
        }

        if (tableIsMissingColStats && !allColumnsMissingStats) {
          analyzer.addWarning("Column " + exampleColumnMissingStats +
              " does not have statistics, recomputing stats for the whole table");
        }

        for (HdfsPartition p: hdfsTable.getPartitions()) {
          if (p.isDefaultPartition()) continue;
          TPartitionStats partStats = p.getPartitionStats();
          if (!p.hasIncrementalStats() || tableIsMissingColStats) {
            if (partStats == null) {
              if (LOG.isTraceEnabled()) LOG.trace(p.toString() + " does not have stats");
            }
            if (!tableIsMissingColStats) filterPreds.add(p.getConjunctSql());
            List<String> partValues = Lists.newArrayList();
            for (LiteralExpr partValue: p.getPartitionValues()) {
              partValues.add(PartitionKeyValue.getPartitionKeyValueString(partValue,
                  "NULL"));
            }
            expectedPartitions_.add(partValues);
          } else {
            if (LOG.isTraceEnabled()) LOG.trace(p.toString() + " does have statistics");
            validPartStats_.add(partStats);
          }
        }
        if (expectedPartitions_.size() == hdfsTable.getPartitions().size() - 1) {
          expectedPartitions_.clear();
          expectAllPartitions_ = true;
        }
      } else {
        // Always compute stats on a set of partitions when told to.
        for (HdfsPartition targetPartition: partitionSet_.getPartitions()) {
          filterPreds.add(targetPartition.getConjunctSql());
          List<String> partValues = Lists.newArrayList();
          for (LiteralExpr partValue: targetPartition.getPartitionValues()) {
            partValues.add(PartitionKeyValue.getPartitionKeyValueString(partValue,
                "NULL"));
          }
          expectedPartitions_.add(partValues);
        }
        // Create a hash set out of partitionSet_ for O(1) lookups.
        HashSet<HdfsPartition> targetPartitions =
            Sets.newHashSet(partitionSet_.getPartitions());
        for (HdfsPartition p : hdfsTable.getPartitions()) {
          if (p.isDefaultPartition()) continue;
          if (targetPartitions.contains(p)) continue;
          TPartitionStats partStats = p.getPartitionStats();
          if (partStats != null) validPartStats_.add(partStats);
        }
      }

      if (filterPreds.size() == 0 && validPartStats_.size() != 0) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("No partitions selected for incremental stats update");
        }
        analyzer.addWarning("No partitions selected for incremental stats update");
        return;
      }
    } else {
      // Not computing incremental stats.
      expectAllPartitions_ = true;
      if (table_ instanceof HdfsTable) {
        expectAllPartitions_ = !((HdfsTable) table_).isStatsExtrapolationEnabled();
      }
    }

    if (filterPreds.size() > MAX_INCREMENTAL_PARTITIONS) {
      // TODO: Consider simply running for MAX_INCREMENTAL_PARTITIONS partitions, and then
      // advising the user to iterate.
      analyzer.addWarning(
          "Too many partitions selected, doing full recomputation of incremental stats");
      filterPreds.clear();
      validPartStats_.clear();
    }

    // Tablesample clause to be used for all child queries.
    String tableSampleSql = analyzeTableSampleClause(analyzer);

    // Query for getting the per-partition row count and the total row count.
    StringBuilder tableStatsQueryBuilder = new StringBuilder("SELECT ");
    String countSql = "COUNT(*)";
    if (isSampling()) {
      // Extrapolate the count based on the effective sampling rate.
      countSql = String.format("ROUND(COUNT(*) / %.10f)", effectiveSamplePerc_);
    }
    List<String> tableStatsSelectList = Lists.newArrayList(countSql);
    // Add group by columns for incremental stats or with extrapolation disabled.
    List<String> groupByCols = Lists.newArrayList();
    if (!updateTableStatsOnly()) {
      for (Column partCol: hdfsTable.getClusteringColumns()) {
        groupByCols.add(ToSqlUtils.getIdentSql(partCol.getName()));
      }
      tableStatsSelectList.addAll(groupByCols);
    }
    tableStatsQueryBuilder.append(Joiner.on(", ").join(tableStatsSelectList));
    tableStatsQueryBuilder.append(" FROM " + tableName_.toSql() + tableSampleSql);

    // Query for getting the per-column NDVs and number of NULLs.
    List<String> columnStatsSelectList = getBaseColumnStatsQuerySelectList(analyzer);

    if (isIncremental_) columnStatsSelectList.addAll(groupByCols);

    StringBuilder columnStatsQueryBuilder = new StringBuilder("SELECT ");
    columnStatsQueryBuilder.append(Joiner.on(", ").join(columnStatsSelectList));
    columnStatsQueryBuilder.append(" FROM " + tableName_.toSql() + tableSampleSql);

    // Add the WHERE clause to filter out partitions that we don't want to compute
    // incremental stats for. While this is a win in most situations, we would like to
    // avoid this where it does no useful work (i.e. it selects all rows). This happens
    // when there are no existing valid partitions (so all partitions will have been
    // selected in) and there is no partition spec (so no single partition was explicitly
    // selected in).
    if (filterPreds.size() > 0 &&
        (validPartStats_.size() > 0 || partitionSet_ != null)) {
      String filterClause = " WHERE " + Joiner.on(" OR ").join(filterPreds);
      columnStatsQueryBuilder.append(filterClause);
      tableStatsQueryBuilder.append(filterClause);
    }

    if (groupByCols.size() > 0) {
      String groupBy = " GROUP BY " + Joiner.on(", ").join(groupByCols);
      if (isIncremental_) columnStatsQueryBuilder.append(groupBy);
      tableStatsQueryBuilder.append(groupBy);
    }

    tableStatsQueryStr_ = tableStatsQueryBuilder.toString();
    if (LOG.isTraceEnabled()) LOG.trace("Table stats query: " + tableStatsQueryStr_);

    if (columnStatsSelectList.isEmpty()) {
      // Table doesn't have any columns that we can compute stats for.
      if (LOG.isTraceEnabled()) {
        LOG.trace("No supported column types in table " + table_.getTableName() +
            ", no column statistics will be gathered.");
      }
      columnStatsQueryStr_ = null;
      return;
    }

    columnStatsQueryStr_ = columnStatsQueryBuilder.toString();
    if (LOG.isTraceEnabled()) LOG.trace("Column stats query: " + columnStatsQueryStr_);
  }

  /**
   * Analyzes the TABLESAMPLE clause and computes the files sample to set
   * 'effectiveSamplePerc_'.
   * Returns the TABLESAMPLE SQL to be used for all child queries or an empty string if
   * not sampling. If sampling, the returned SQL includes a fixed random seed so all
   * child queries generate a consistent sample, even if the user did not originally
   * specify REPEATABLE.
   * Returns the empty string if this statement has no TABLESAMPLE clause or if
   * the effective sampling rate is 0.0 or 1.0 (see isSampling()).
   */
  private String analyzeTableSampleClause(Analyzer analyzer) throws AnalysisException {
    if (sampleParams_ == null) return "";
    if (!(table_ instanceof HdfsTable)) {
      throw new AnalysisException("TABLESAMPLE is only supported on HDFS tables.");
    }
    HdfsTable hdfsTable = (HdfsTable) table_;
    if (!hdfsTable.isStatsExtrapolationEnabled()) {
      throw new AnalysisException(String.format(
          "COMPUTE STATS TABLESAMPLE requires stats extrapolation which is disabled.\n" +
          "Stats extrapolation can be enabled service-wide with %s=true or by altering " +
          "the table to have tblproperty %s=true",
          "--enable_stats_extrapolation",
          HdfsTable.TBL_PROP_ENABLE_STATS_EXTRAPOLATION));
    }
    sampleParams_.analyze(analyzer);
    long sampleSeed;
    if (sampleParams_.hasRandomSeed()) {
      sampleSeed = sampleParams_.getRandomSeed();
    } else {
      sampleSeed = System.currentTimeMillis();
    }

    // Compute the sample of files and set 'sampleFileBytes_'.
    long minSampleBytes = analyzer.getQueryOptions().compute_stats_min_sample_size;
    long samplePerc = sampleParams_.getPercentBytes();
    Map<Long, List<FileDescriptor>> sample = hdfsTable.getFilesSample(
        hdfsTable.getPartitions(), samplePerc, minSampleBytes, sampleSeed);
    long sampleFileBytes = 0;
    for (List<FileDescriptor> fds: sample.values()) {
      for (FileDescriptor fd: fds) sampleFileBytes += fd.getFileLength();
    }

    // Compute effective sampling percent.
    long totalFileBytes = ((HdfsTable)table_).getTotalHdfsBytes();
    if (totalFileBytes > 0) {
      effectiveSamplePerc_ = (double) sampleFileBytes / (double) totalFileBytes;
    } else {
      effectiveSamplePerc_ = 0;
    }
    Preconditions.checkState(effectiveSamplePerc_ >= 0.0 && effectiveSamplePerc_ <= 1.0);

    // Warn if we will ignore TABLESAMPLE and run the regular COMPUTE STATS.
    if (effectiveSamplePerc_ == 1.0) {
      Preconditions.checkState(!isSampling());
      analyzer.addWarning(String.format(
          "Ignoring TABLESAMPLE because the effective sampling rate is 100%%.\n" +
          "The minimum sample size is COMPUTE_STATS_MIN_SAMPLE_SIZE=%s " +
          "and the table size %s",
          PrintUtils.printBytes(minSampleBytes), PrintUtils.printBytes(totalFileBytes)));
    }
    if (!isSampling()) return "";

    return " " + sampleParams_.toSql(sampleSeed);
  }

  /**
   * Checks whether the column definitions from the CREATE TABLE stmt match the columns
   * in the Avro schema. If there is a mismatch, then COMPUTE STATS cannot update the
   * statistics in the Metastore's backend DB due to HIVE-6308. Throws an
   * AnalysisException for such ill-created Avro tables. Does nothing if
   * the column definitions match the Avro schema exactly.
   */
  private void checkIncompleteAvroSchema(HdfsTable table) throws AnalysisException {
    Preconditions.checkState(table.isAvroTable());
    org.apache.hadoop.hive.metastore.api.Table msTable = table.getMetaStoreTable();
    // The column definitions from 'CREATE TABLE (column definitions) ...'
    Iterator<FieldSchema> colDefs = msTable.getSd().getCols().iterator();
    // The columns derived from the Avro schema file or literal schema.
    // Inconsistencies between the Avro-schema columns and the column definitions
    // are sometimes resolved in the CREATE TABLE, and sometimes not (see below).
    Iterator<Column> avroSchemaCols = table.getColumns().iterator();
    // Skip partition columns from 'table' since those are not present in
    // the msTable field schemas.
    for (int i = 0; i < table.getNumClusteringCols(); ++i) {
      if (avroSchemaCols.hasNext()) avroSchemaCols.next();
    }
    int pos = 0;
    while (colDefs.hasNext() || avroSchemaCols.hasNext()) {
      if (colDefs.hasNext() && avroSchemaCols.hasNext()) {
        FieldSchema colDef = colDefs.next();
        Column avroSchemaCol = avroSchemaCols.next();
        // Check that the column names are identical. Ignore mismatched types
        // as those will either fail in the scan or succeed.
        if (!colDef.getName().equalsIgnoreCase(avroSchemaCol.getName())) {
          throw new AnalysisException(
              String.format(AVRO_SCHEMA_MSG_PREFIX +
                  "\nDefinition of column '%s' of type '%s' does not match " +
                  "the Avro-schema column '%s' of type '%s' at position '%s'.\n" +
                  AVRO_SCHEMA_MSG_SUFFIX,
                  table.getName(), colDef.getName(), colDef.getType(),
                  avroSchemaCol.getName(), avroSchemaCol.getType(), pos));
        }
      }
      // The following two cases are typically not possible because Hive resolves
      // inconsistencies between the column-definition list and the Avro schema if a
      // column-definition list was given in the CREATE TABLE (having no column
      // definitions at all results in HIVE-6308). Even so, we check these cases for
      // extra safety. COMPUTE STATS could be made to succeed in special instances of
      // the cases below but we chose to throw an AnalysisException to avoid confusion
      // because this scenario "should" never arise as mentioned above.
      if (colDefs.hasNext() && !avroSchemaCols.hasNext()) {
        FieldSchema colDef = colDefs.next();
        throw new AnalysisException(
            String.format(AVRO_SCHEMA_MSG_PREFIX +
                "\nMissing Avro-schema column corresponding to column " +
                "definition '%s' of type '%s' at position '%s'.\n" +
                AVRO_SCHEMA_MSG_SUFFIX,
                table.getName(), colDef.getName(), colDef.getType(), pos));
      }
      if (!colDefs.hasNext() && avroSchemaCols.hasNext()) {
        Column avroSchemaCol = avroSchemaCols.next();
        throw new AnalysisException(
            String.format(AVRO_SCHEMA_MSG_PREFIX +
                "\nMissing column definition corresponding to Avro-schema " +
                "column '%s' of type '%s' at position '%s'.\n" +
                AVRO_SCHEMA_MSG_SUFFIX,
                table.getName(), avroSchemaCol.getName(), avroSchemaCol.getType(), pos));
      }
      ++pos;
    }
  }

  /**
   * Returns true if we are only updating statistics at the table level and not at
   * the partition level.
   */
  private boolean updateTableStatsOnly() {
    if (!(table_ instanceof HdfsTable)) return true;
    return !isIncremental_ && ((HdfsTable) table_).isStatsExtrapolationEnabled();
  }

  /**
   * Returns true if this COMPUTE STATS statement should perform sampling.
   * Returns false if TABLESAMPLE was not specified (effectiveSamplePerc_ == -1)
   * or if the effective sampling percent is 0% or 100% where sampling has no benefit.
   */
  private boolean isSampling() {
    Preconditions.checkState(effectiveSamplePerc_ == -1
        || effectiveSamplePerc_ >= 0.0 || effectiveSamplePerc_ <= 1.0);
    return effectiveSamplePerc_ > 0.0 && effectiveSamplePerc_ < 1.0;
  }

  /**
   * Returns true if the given column should be ignored for the purpose of computing
   * column stats. Columns with an invalid/unsupported/complex type are ignored.
   * For example, complex types in an HBase-backed table will appear as invalid types.
   */
  private boolean ignoreColumn(Column c) {
    Type t = c.getType();
    return !t.isValid() || !t.isSupported() || t.isComplexType();
  }

  public double getEffectiveSamplingPerc() { return effectiveSamplePerc_; }

  /**
   * For testing.
   */
  public String getTblStatsQuery() { return tableStatsQueryStr_; }
  public String getColStatsQuery() { return columnStatsQueryStr_; }
  public Set<Column> getValidatedColumnWhitelist() { return validatedColumnWhitelist_; }

  /**
   * Returns true if this statement computes stats on Parquet partitions only,
   * false otherwise.
   */
  public boolean isParquetOnly() {
    if (!(table_ instanceof HdfsTable)) return false;
    Collection<HdfsPartition> affectedPartitions = null;
    if (partitionSet_ != null) {
      affectedPartitions = partitionSet_.getPartitions();
    } else {
      affectedPartitions = ((HdfsTable) table_).getPartitions();
    }
    for (HdfsPartition partition: affectedPartitions) {
      if (partition.getFileFormat() != HdfsFileFormat.PARQUET) return false;
    }
    return true;
  }

  @Override
  public String toSql() {
    if (!isIncremental_) {
      StringBuilder columnList = new StringBuilder();
      if (columnWhitelist_ != null) {
        columnList.append("(");
        columnList.append(Joiner.on(", ").join(columnWhitelist_));
        columnList.append(")");
      }
      String tblsmpl = "";
      if (sampleParams_ != null) tblsmpl = " " + sampleParams_.toSql();
      return "COMPUTE STATS " + tableName_.toSql() + columnList.toString() + tblsmpl;
    } else {
      return "COMPUTE INCREMENTAL STATS " + tableName_.toSql() +
          partitionSet_ == null ? "" : partitionSet_.toSql();
    }
  }

  public TComputeStatsParams toThrift() {
    TComputeStatsParams params = new TComputeStatsParams();
    params.setTable_name(new TTableName(table_.getDb().getName(), table_.getName()));
    params.setTbl_stats_query(tableStatsQueryStr_);
    if (columnStatsQueryStr_ != null) {
      params.setCol_stats_query(columnStatsQueryStr_);
    } else {
      params.setCol_stats_queryIsSet(false);
    }
    params.setIs_incremental(isIncremental_);
    params.setExisting_part_stats(validPartStats_);
    params.setExpect_all_partitions(expectAllPartitions_);
    if (!expectAllPartitions_) params.setExpected_partitions(expectedPartitions_);
    if (isIncremental_) {
      params.setNum_partition_cols(((HdfsTable)table_).getNumClusteringCols());
    }
    if (table_ instanceof HdfsTable) {
      params.setTotal_file_bytes(((HdfsTable)table_).getTotalHdfsBytes());
    }
    return params;
  }
}
