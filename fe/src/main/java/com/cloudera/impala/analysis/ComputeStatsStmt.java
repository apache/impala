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

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.log4j.Logger;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.thrift.TComputeStatsParams;
import com.cloudera.impala.thrift.TPartitionStats;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a COMPUTE STATS <table> and COMPUTE INCREMENTAL STATS <table> [PARTITION
 * <part_spec>] statement for statistics collection. The former statement gathers all
 * table and column stats for a given table and stores them in the Metastore via the
 * CatalogService. All existing stats for that table are replaced and no existing stats
 * are reused. The latter, incremental form, similarly computes stats for the whole table
 * but does so by re-using stats from partitions which have 'valid' statistics. Statistics
 * are 'valid' currently if they exist, in the future they may be expired based on recency
 * etc.
 *
 * TODO: Allow more coarse/fine grained (db, column)
 * TODO: Compute stats on complex types.
 */
public class ComputeStatsStmt extends StatementBase {
  private static final Logger LOG = Logger.getLogger(ComputeStatsStmt.class);

  private static String AVRO_SCHEMA_MSG_PREFIX = "Cannot COMPUTE STATS on Avro table " +
      "'%s' because its column definitions do not match those in the Avro schema.";
  private static String AVRO_SCHEMA_MSG_SUFFIX = "Please re-create the table with " +
          "column definitions, e.g., using the result of 'SHOW CREATE TABLE'";

  protected final TableName tableName_;

  // Set during analysis.
  protected Table table_;

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
  // target table (only meaningful, therefore, if partitioned). This is always true for
  // non-incremental computations. If set, expectedPartitions_ will be empty - the point
  // of this flag is to optimise the case where all partitions are targeted.
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

  // If non-null, the partition that an incremental computation might apply to. Must be
  // null if this is a non-incremental computation.
  private PartitionSpec partitionSpec_ = null;

  // The maximum number of partitions that may be explicitly selected by filter
  // predicates. Any query that selects more than this automatically drops back to a full
  // incremental stats recomputation.
  // TODO: We can probably do better than this, e.g. running several queries, each of
  // which selects up to MAX_INCREMENTAL_PARTITIONS partitions.
  private static final int MAX_INCREMENTAL_PARTITIONS = 1000;

  /**
   * Constructor for the non-incremental form of COMPUTE STATS.
   */
  protected ComputeStatsStmt(TableName tableName) {
    this(tableName, false, null);
  }

  /**
   * Constructor for the incremental form of COMPUTE STATS. If isIncremental is true,
   * statistics will be recomputed incrementally; if false they will be recomputed for the
   * whole table. The partition spec partSpec can specify a single partition whose stats
   * should be recomputed.
   */
  protected ComputeStatsStmt(TableName tableName, boolean isIncremental,
      PartitionSpec partSpec) {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    Preconditions.checkState(isIncremental || partSpec == null);
    this.tableName_ = tableName;
    this.table_ = null;
    this.isIncremental_ = isIncremental;
    this.partitionSpec_ = partSpec;
    if (partitionSpec_ != null) {
      partitionSpec_.setTableName(tableName);
      partitionSpec_.setPrivilegeRequirement(Privilege.ALTER);
    }
  }

  /**
   * Utility method for constructing the child queries to add partition columns to both a
   * select list and a group-by list; the former are wrapped in a cast to a string.
   */
  private void addPartitionCols(HdfsTable table, List<String> selectList,
      List<String> groupByCols) {
    for (int i = 0; i < table.getNumClusteringCols(); ++i) {
      String colRefSql = ToSqlUtils.getIdentSql(table.getColumns().get(i).getName());
      groupByCols.add(colRefSql);
      // For the select list, wrap the group by columns in a cast to string because
      // the Metastore stores them as strings.
      selectList.add(colRefSql);
    }
  }

  private List<String> getBaseColumnStatsQuerySelectList(Analyzer analyzer) {
    List<String> columnStatsSelectList = Lists.newArrayList();
    // For Hdfs tables, exclude partition columns from stats gathering because Hive
    // cannot store them as part of the non-partition column stats. For HBase tables,
    // include the single clustering column (the row key).
    int startColIdx = (table_ instanceof HBaseTable) ? 0 : table_.getNumClusteringCols();
    final String ndvUda = isIncremental_ ? "NDV_NO_FINALIZE" : "NDV";

    for (int i = startColIdx; i < table_.getColumns().size(); ++i) {
      Column c = table_.getColumns().get(i);
      Type type = c.getType();

      // Ignore columns with an invalid/unsupported type. For example, complex types in
      // an HBase-backed table will appear as invalid types.
      if (!type.isValid() || !type.isSupported()
          || c.getType().isComplexType()) {
        continue;
      }
      // NDV approximation function. Add explicit alias for later identification when
      // updating the Metastore.
      String colRefSql = ToSqlUtils.getIdentSql(c.getName());
      columnStatsSelectList.add(ndvUda + "(" + colRefSql + ") AS " + colRefSql);

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
   * Constructs two queries to compute statistics for 'tableName_', if that table exists
   * (although if we can detect that no work needs to be done for either query, that query
   * will be 'null' and not executed).
   *
   * The first query computes the number of rows (on a per-partition basis if the table is
   * partitioned) and has the form "SELECT COUNT(*) FROM tbl GROUP BY part_col1,
   * part_col2...", with an optional WHERE clause for incremental computation (see below).
   *
   * The second query computes the NDV estimate, the average width, the maximum width and,
   * optionally, the number of nulls for each column. For non-partitioned tables (or
   * non-incremental computations), the query is simple:
   *
   * SELECT NDV(col), COUNT(<nulls>), MAX(length(col)), AVG(length(col)) FROM tbl
   *
   * (For non-string columns, the widths are hard-coded as they are known at query
   * construction time).
   *
   * If computation is incremental (i.e. the original statement was COMPUTE INCREMENTAL
   * STATS.., and the underlying table is a partitioned HdfsTable), some modifications are
   * made to the non-incremental per-column query. First, a different UDA,
   * NDV_NO_FINALIZE() is used to retrieve and serialise the intermediate state from each
   * column. Second, the results are grouped by partition, as with the row count query, so
   * that the intermediate NDV computation state can be stored per-partition. The number
   * of rows per-partition are also recorded.
   *
   * For both the row count query, and the column stats query, the query's WHERE clause is
   * used to restrict execution only to partitions that actually require new statistics to
   * be computed.
   *
   * SELECT NDV_NO_FINALIZE(col), <nulls, max, avg>, COUNT(col) FROM tbl
   * GROUP BY part_col1, part_col2, ...
   * WHERE ((part_col1 = p1_val1) AND (part_col2 = p1_val2)) OR
   *       ((part_col1 = p2_val1) AND (part_col2 = p2_val2)) OR ...
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    table_ = analyzer.getTable(tableName_, Privilege.ALTER);
    String sqlTableName = table_.getTableName().toSql();
    if (table_ instanceof View) {
      throw new AnalysisException(String.format(
          "COMPUTE STATS not supported for view %s", sqlTableName));
    }

    if (!(table_ instanceof HdfsTable)) {
      if (partitionSpec_ != null) {
        throw new AnalysisException("COMPUTE INCREMENTAL ... PARTITION not supported " +
            "for non-HDFS table " + table_.getTableName());
      }
      isIncremental_ = false;
    }

    // Ensure that we write an entry for every partition if this isn't incremental
    if (!isIncremental_) expectAllPartitions_ = true;

    HdfsTable hdfsTable = null;
    if (table_ instanceof HdfsTable) {
      hdfsTable = (HdfsTable)table_;
      if (isIncremental_ && hdfsTable.getNumClusteringCols() == 0 &&
          partitionSpec_ != null) {
          throw new AnalysisException(String.format(
              "Can't compute PARTITION stats on an unpartitioned table: %s",
              sqlTableName));
      } else if (partitionSpec_ != null) {
          partitionSpec_.setPartitionShouldExist();
          partitionSpec_.analyze(analyzer);
          for (PartitionKeyValue kv: partitionSpec_.getPartitionSpecKeyValues()) {
            // TODO: We could match the dynamic keys (i.e. as wildcards) as well, but that
            // would involve looping over all partitions and seeing which match the
            // partition spec.
            if (!kv.isStatic()) {
              throw new AnalysisException("All partition keys must have values: " +
                  kv.toString());
            }
          }
      }
      // For incremental stats, estimate the size of intermediate stats and report an
      // error if the estimate is greater than MAX_INCREMENTAL_STATS_SIZE_BYTES.
      if (isIncremental_) {
        long statsSizeEstimate = hdfsTable.getColumns().size() *
            hdfsTable.getPartitions().size() * HdfsTable.STATS_SIZE_PER_COLUMN_BYTES;
        if (statsSizeEstimate > HdfsTable.MAX_INCREMENTAL_STATS_SIZE_BYTES) {
          LOG.error("Incremental stats size estimate for table " + hdfsTable.getName() +
              " exceeded " + HdfsTable.MAX_INCREMENTAL_STATS_SIZE_BYTES + ", estimate = "
              + statsSizeEstimate);
          throw new AnalysisException("Incremental stats size estimate exceeds "
              + PrintUtils.printBytes(HdfsTable.MAX_INCREMENTAL_STATS_SIZE_BYTES)
              + ". Please try COMPUTE STATS instead.");
        }
      }
    }

    // Build partition filters that only select partitions without valid statistics for
    // incremental computation.
    List<String> filterPreds = Lists.newArrayList();
    if (isIncremental_) {
      if (partitionSpec_ == null) {
        // If any column does not have stats, we recompute statistics for all partitions
        // TODO: need a better way to invalidate stats for all partitions, so that we can
        // use this logic to only recompute new / changed columns.
        boolean tableIsMissingColStats = false;

        // We'll warn the user if a column is missing stats (and therefore we rescan the
        // whole table), but if all columns are missing stats, the table just doesn't have
        // any stats and there's no need to warn.
        boolean allColumnsMissingStats = true;
        String exampleColumnMissingStats = null;
        // Partition columns always have stats, so exclude them from this search
        for (Column col: table_.getNonClusteringColumns()) {
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
            if (partStats == null) LOG.trace(p.toString() + " does not have stats");
            if (!tableIsMissingColStats) filterPreds.add(p.getConjunctSql());
            List<String> partValues = Lists.newArrayList();
            for (LiteralExpr partValue: p.getPartitionValues()) {
              partValues.add(PartitionKeyValue.getPartitionKeyValueString(partValue,
                  "NULL"));
            }
            expectedPartitions_.add(partValues);
          } else {
            LOG.trace(p.toString() + " does have statistics");
            validPartStats_.add(partStats);
          }
        }
        if (expectedPartitions_.size() == hdfsTable.getPartitions().size() - 1) {
          expectedPartitions_.clear();
          expectAllPartitions_ = true;
        }
      } else {
        // Always compute stats on a particular partition when told to.
        List<String> partitionConjuncts = Lists.newArrayList();
        for (PartitionKeyValue kv: partitionSpec_.getPartitionSpecKeyValues()) {
          partitionConjuncts.add(kv.toPredicateSql());
        }
        filterPreds.add("(" + Joiner.on(" AND ").join(partitionConjuncts) + ")");
        HdfsPartition targetPartition =
            hdfsTable.getPartition(partitionSpec_.getPartitionSpecKeyValues());
        List<String> partValues = Lists.newArrayList();
        for (LiteralExpr partValue: targetPartition.getPartitionValues()) {
          partValues.add(PartitionKeyValue.getPartitionKeyValueString(partValue,
              "NULL"));
        }
        expectedPartitions_.add(partValues);
        for (HdfsPartition p: hdfsTable.getPartitions()) {
          if (p.isDefaultPartition()) continue;
          if (p == targetPartition) continue;
          TPartitionStats partStats = p.getPartitionStats();
          if (partStats != null) validPartStats_.add(partStats);
        }
      }

      if (filterPreds.size() == 0 && validPartStats_.size() != 0) {
        LOG.info("No partitions selected for incremental stats update");
        analyzer.addWarning("No partitions selected for incremental stats update");
        return;
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

    List<String> groupByCols = Lists.newArrayList();
    List<String> partitionColsSelectList = Lists.newArrayList();
    // Only add group by clause for HdfsTables.
    if (hdfsTable != null) {
      if (hdfsTable.isAvroTable()) checkIncompleteAvroSchema(hdfsTable);
      addPartitionCols(hdfsTable, partitionColsSelectList, groupByCols);
    }

    // Query for getting the per-partition row count and the total row count.
    StringBuilder tableStatsQueryBuilder = new StringBuilder("SELECT ");
    List<String> tableStatsSelectList = Lists.newArrayList();
    tableStatsSelectList.add("COUNT(*)");

    tableStatsSelectList.addAll(partitionColsSelectList);
    tableStatsQueryBuilder.append(Joiner.on(", ").join(tableStatsSelectList));
    tableStatsQueryBuilder.append(" FROM " + sqlTableName);

    // Query for getting the per-column NDVs and number of NULLs.
    List<String> columnStatsSelectList = getBaseColumnStatsQuerySelectList(analyzer);

    if (isIncremental_) columnStatsSelectList.addAll(partitionColsSelectList);

    StringBuilder columnStatsQueryBuilder = new StringBuilder("SELECT ");
    columnStatsQueryBuilder.append(Joiner.on(", ").join(columnStatsSelectList));
    columnStatsQueryBuilder.append(" FROM " + sqlTableName);

    // Add the WHERE clause to filter out partitions that we don't want to compute
    // incremental stats for. While this is a win in most situations, we would like to
    // avoid this where it does no useful work (i.e. it selects all rows). This happens
    // when there are no existing valid partitions (so all partitions will have been
    // selected in) and there is no partition spec (so no single partition was explicitly
    // selected in).
    if (filterPreds.size() > 0 &&
        (validPartStats_.size() > 0 || partitionSpec_ != null)) {
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
    LOG.debug("Table stats query: " + tableStatsQueryStr_);

    if (columnStatsSelectList.isEmpty()) {
      // Table doesn't have any columns that we can compute stats for.
      LOG.info("No supported column types in table " + table_.getTableName() +
          ", no column statistics will be gathered.");
      columnStatsQueryStr_ = null;
      return;
    }

    columnStatsQueryStr_ = columnStatsQueryBuilder.toString();
    LOG.debug("Column stats query: " + columnStatsQueryStr_);
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

  public String getTblStatsQuery() { return tableStatsQueryStr_; }
  public String getColStatsQuery() { return columnStatsQueryStr_; }

  @Override
  public String toSql() {
    if (!isIncremental_) {
      return "COMPUTE STATS " + tableName_.toSql();
    } else {
      return "COMPUTE INCREMENTAL STATS " + tableName_.toSql() +
          partitionSpec_ == null ? "" : partitionSpec_.toSql();
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
    return params;
  }
}
