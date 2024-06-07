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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.types.Types;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.MaterializedViewHdfsTable;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.View;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.HdfsTableSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.ExprUtil;
import org.apache.impala.util.IcebergUtil;

/**
 * Representation of a single insert or upsert statement, including the select statement
 * whose results are to be inserted.
 */
public class InsertStmt extends DmlStatementBase {
  // Determines the location of optional hints. The "Start" option is motivated by
  // Oracle's hint placement at the start of the statement and the "End" option places
  // the hint right before the query (if specified).
  //
  // Examples:
  //   Start: INSERT /* +my hint */ <tablename> ... SELECT ...
  //   End:   INSERT <tablename> /* +my hint */ SELECT ...
  public enum HintLocation { Start, End };

  // Target table name as seen by the parser
  private final TableName originalTableName_;

  // Differentiates between INSERT INTO and INSERT OVERWRITE.
  private final boolean overwrite_;

  // List of column:value elements from the PARTITION (...) clause.
  // Set to null if no partition was given.
  private final List<PartitionKeyValue> partitionKeyValues_;

  // User-supplied hints to control hash partitioning before the table sink in the plan.
  private List<PlanHint> planHints_ = new ArrayList<>();

  // The location of given hints.
  private HintLocation hintLoc_;

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

  // Set in analyze(). Exprs correspond to the partitionKeyValues, if specified, or to
  // the partition columns for Kudu tables.
  private List<Expr> partitionKeyExprs_ = new ArrayList<>();

  // Set in analyze(). Maps exprs in partitionKeyExprs_ to their column's position in the
  // table, eg. partitionKeyExprs_[i] corresponds to table_.columns(partitionKeyIdx_[i]).
  // For Kudu tables, the primary keys are a leading subset of the cols, and the partition
  // cols can be any subset of the primary keys, meaning that this list will be in
  // ascending order from '0' to '# primary key cols - 1' but may leave out some numbers.
  private final List<Integer> partitionColPos_ = new ArrayList<>();

  // Indicates whether this insert stmt has a shuffle or noshuffle plan hint.
  // Both flags may be false. Only one of them may be true, not both.
  // Shuffle forces data repartitioning before the data sink, and noshuffle
  // prevents it. Set in analyze() based on planHints_.
  private boolean hasShuffleHint_ = false;
  private boolean hasNoShuffleHint_ = false;

  // Indicates whether this insert stmt has a clustered or noclustered hint. Only one of
  // them may be true, not both. If clustering is requested, we add a clustering phase
  // before the data sink, so that partitions can be written sequentially. The default
  // behavior is to not perform an additional clustering step. Both are required to detect
  // conflicting hints.
  private boolean hasClusteredHint_ = false;
  private boolean hasNoClusteredHint_ = false;

  // For every column of the target table that is referenced in the optional
  // 'sort.columns' table property, this list will
  // contain the corresponding result expr from 'resultExprs_'. Before insertion, all rows
  // will be sorted by these exprs. If the list is empty, no additional sorting by
  // non-partitioning columns will be performed. The column list must not contain
  // partition columns and must be empty for non-Hdfs tables.
  private List<Expr> sortExprs_ = new ArrayList<>();

  // Stores the indices into the list of non-clustering columns of the target table that
  // are mentioned in the 'sort.columns' table property. This is
  // sent to the backend to populate the RowGroup::sorting_columns list in parquet files.
  private List<Integer> sortColumns_ = new ArrayList<>();

  // The order used in SORT BY queries.
  private TSortingOrder sortingOrder_ = TSortingOrder.LEXICAL;

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
  private List<Expr> resultExprs_ = new ArrayList<>();

  // Position mapping of exprs in resultExprs_ to columns in the target table -
  // resultExprs_[i] produces the mentionedColumns_[i] column of the target table.
  // Only used for Kudu tables, set in prepareExpressions().
  private final List<Integer> mentionedColumns_ = new ArrayList<>();

  // Set in analyze(). Exprs corresponding to key columns of Kudu tables. Empty for
  // non-Kudu tables.
  private List<Expr> primaryKeyExprs_ = new ArrayList<>();

  // Set by the Frontend if the target table is transactional.
  private long writeId_ = -1;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  // True iff this is an UPSERT operation. Only supported for Kudu tables.
  private final boolean isUpsert_;

  public static InsertStmt createInsert(WithClause withClause, TableName targetTable,
      boolean overwrite, List<PartitionKeyValue> partitionKeyValues,
      List<PlanHint> planHints, HintLocation hintLoc, QueryStmt queryStmt,
      List<String> columnPermutation) {
    return new InsertStmt(withClause, targetTable, overwrite, partitionKeyValues,
        planHints, hintLoc, queryStmt, columnPermutation, false);
  }

  public static InsertStmt createUpsert(WithClause withClause, TableName targetTable,
      List<PlanHint> planHints, HintLocation hintLoc, QueryStmt queryStmt,
      List<String> columnPermutation) {
    return new InsertStmt(withClause, targetTable, false, null, planHints, hintLoc,
        queryStmt, columnPermutation, true);
  }

  protected InsertStmt(WithClause withClause, TableName targetTable, boolean overwrite,
      List<PartitionKeyValue> partitionKeyValues, List<PlanHint> planHints,
      HintLocation hintLoc, QueryStmt queryStmt, List<String> columnPermutation,
      boolean isUpsert) {
    Preconditions.checkState(!isUpsert || (!overwrite && partitionKeyValues == null));
    withClause_ = withClause;
    targetTableName_ = targetTable;
    originalTableName_ = targetTableName_;
    overwrite_ = overwrite;
    partitionKeyValues_ = partitionKeyValues;
    planHints_ = (planHints != null) ? planHints : new ArrayList<>();
    hintLoc_ = (hintLoc != null) ? hintLoc : HintLocation.End;
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
    hintLoc_ = other.hintLoc_;
    queryStmt_ = other.queryStmt_ != null ? other.queryStmt_.clone() : null;
    needsGeneratedQueryStatement_ = other.needsGeneratedQueryStatement_;
    columnPermutation_ = other.columnPermutation_;
    isUpsert_ = other.isUpsert_;
    writeId_ = other.writeId_;
  }

  @Override
  public void reset() {
    super.reset();
    if (withClause_ != null) withClause_.reset();
    targetTableName_ = originalTableName_;
    queryStmt_.reset();
    partitionKeyExprs_.clear();
    partitionColPos_.clear();
    hasShuffleHint_ = false;
    hasNoShuffleHint_ = false;
    hasClusteredHint_ = false;
    hasNoClusteredHint_ = false;
    sortExprs_.clear();
    sortColumns_.clear();
    sortingOrder_ = TSortingOrder.LEXICAL;
    resultExprs_.clear();
    mentionedColumns_.clear();
    primaryKeyExprs_.clear();
    writeId_ = -1;
  }

  @Override
  public InsertStmt clone() { return new InsertStmt(this); }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    if (withClause_ != null) withClause_.analyze(analyzer);

    List<Expr> selectListExprs = null;
    if (!needsGeneratedQueryStatement_) {
      // Use a child analyzer for the query stmt to properly scope WITH-clause
      // views and to ignore irrelevant ORDER BYs.
      Analyzer queryStmtAnalyzer = new Analyzer(analyzer);
      queryStmt_.analyze(queryStmtAnalyzer);
      // Use getResultExprs() and not getBaseTblResultExprs() here because the final
      // substitution with TupleIsNullPredicate() wrapping happens in planning.
      selectListExprs = Expr.cloneList(queryStmt_.getResultExprs());
    } else {
      selectListExprs = new ArrayList<>();
    }

    // Make sure static partition key values only contain const exprs.
    if (partitionKeyValues_ != null) {
      for (PartitionKeyValue kv: partitionKeyValues_) {
        kv.analyze(analyzer);
      }
    }

    // Set target table and perform table-type specific analysis and auth checking.
    // Also checks if the target table is missing.
    analyzeTargetTable(analyzer);

    boolean isHBaseTable = (table_ instanceof FeHBaseTable);
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
      analysisColumnPermutation = new ArrayList<>();
      List<Column> tableColumns = table_.getColumns();
      for (int i = numClusteringCols; i < tableColumns.size(); ++i) {
        Column c = tableColumns.get(i);
        // Omit auto-incrementing column for Kudu table since the values of the column
        // will be assigned by Kudu engine.
        if (c instanceof KuduColumn && ((KuduColumn)c).isAutoIncrementing()) continue;
        analysisColumnPermutation.add(c.getName());
      }
    }

    // selectExprTargetColumns maps from select expression index to a column in the target
    // table. It will eventually include all mentioned columns that aren't static-valued
    // partition columns.
    List<Column> selectExprTargetColumns = new ArrayList<>();

    // Tracks the name of all columns encountered in either the permutation clause or the
    // partition clause to detect duplicates.
    Set<String> mentionedColumnNames = new HashSet<>();
    for (String columnName: analysisColumnPermutation) {
      Column column = table_.getColumn(columnName);
      if (column == null) {
        throw new AnalysisException(
            "Unknown column '" + columnName + "' in column permutation");
      }

      if (!mentionedColumnNames.add(column.getName())) {
        throw new AnalysisException(
            "Duplicate column '" + columnName + "' in column permutation");
      }
      selectExprTargetColumns.add(column);
    }

    int numStaticPartitionExprs = 0;
    if (partitionKeyValues_ != null && !isIcebergTarget()) {
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

    // Check that we can write to the target table/partition. This must be
    // done after the partition expression has been analyzed above.
    analyzeWriteAccess();

    // Populate partitionKeyExprs from partitionKeyValues and selectExprTargetColumns
    prepareExpressions(selectExprTargetColumns, selectListExprs, table_, analyzer);

    // Analyze 'sort.columns' and 'sort.order' table properties and populate
    // sortColumns_, sortExprs_, and sortingOrder_.
    analyzeSortColumns();

    // Analyze plan hints at the end to prefer reporting other error messages first
    // (e.g., the PARTITION clause is not applicable to unpartitioned and HBase tables).
    analyzePlanHints(analyzer);

    if (hasNoClusteredHint_ && !sortExprs_.isEmpty()) {
      analyzer.addWarning(String.format("Insert statement has 'noclustered' hint, but " +
          "table has '%s' property. The 'noclustered' hint will be ignored.",
          AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS));
    }
  }

  /**
   * Sets table_ based on targetTableName_ and performs table-type specific analysis:
   * - Cannot (in|up)sert into a view
   * - Cannot (in|up)sert into a table with unsupported column types
   * - Analysis specific to insert and upsert operations
   * Adds table_ to the analyzer's descriptor table if analysis succeeds.
   */
  private void analyzeTargetTable(Analyzer analyzer) throws AnalysisException {
    // Fine-grained privileges for UPSERT do not exist yet, so they require ALL for now.
    Privilege privilegeRequired = isUpsert_ ? Privilege.ALL : Privilege.INSERT;

    // If the table has not yet been set, load it from the Catalog. This allows for
    // callers to set a table to analyze that may not actually be created in the Catalog.
    // One example use case is CREATE TABLE AS SELECT which must run analysis on the
    // INSERT before the table has actually been created.
    if (table_ == null) {
      if (!targetTableName_.isFullyQualified()) {
        targetTableName_ =
            new TableName(analyzer.getDefaultDb(), targetTableName_.getTbl());
      }
      table_ = analyzer.getTable(targetTableName_, privilegeRequired);
    } else {
      targetTableName_ = new TableName(table_.getDb().getName(), table_.getName());
      analyzer.registerPrivReq(builder ->
          builder.onTable(table_).allOf(privilegeRequired).build());
    }

    // We do not support (in|up)serting into views and iceberg table
    if (table_ instanceof FeView || table_ instanceof MaterializedViewHdfsTable) {
      throw new AnalysisException(
          String.format("Impala does not support %sing into views: %s", getOpName(),
              table_.getFullName()));
    }

    Analyzer.ensureTableNotFullAcid(table_, "INSERT");
    Analyzer.checkTableCapability(table_, Analyzer.OperationType.WRITE);

    // We do not support (in|up)serting into tables with unsupported column types.
    for (Column c: table_.getColumns()) {
      if (!c.getType().isSupported()) {
        throw new AnalysisException(String.format("Unable to %s into target table " +
            "(%s) because the column '%s' has an unsupported type '%s'.",
            getOpName(), targetTableName_, c.getName(), c.getType().toSql()));
      }
      if (c.getType().isComplexType()) {
        throw new AnalysisException(String.format("Unable to %s into target table " +
            "(%s) because the column '%s' has a complex type '%s' and Impala doesn't " +
            "support inserting into tables containing complex type columns",
            getOpName(), targetTableName_, c.getName(), c.getType().toSql()));
      }
    }

    // Perform operation-specific analysis.
    if (isUpsert_) {
      if (!(table_ instanceof FeKuduTable)) {
        throw new AnalysisException("UPSERT is only supported for Kudu tables");
      } else if (((FeKuduTable)table_).hasAutoIncrementingColumn()) {
        throw new AnalysisException(
            "UPSERT is not supported for Kudu tables with auto-incrementing column");
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
    boolean isHBaseTable = (table_ instanceof FeHBaseTable);
    int numClusteringCols = isHBaseTable ? 0 : table_.getNumClusteringCols();

    if (partitionKeyValues_ != null && numClusteringCols == 0) {
      if (isHBaseTable) {
        throw new AnalysisException("PARTITION clause is not valid for INSERT into " +
            "HBase tables. '" + targetTableName_ + "' is an HBase table");
      } else {
        if (isIcebergTarget()) {
          IcebergPartitionSpec partSpec =
              ((FeIcebergTable)table_).getDefaultPartitionSpec();
          if (partSpec == null || !partSpec.hasPartitionFields()) {
            throw new AnalysisException("PARTITION clause is only valid for INSERT " +
                "into partitioned table. '" + targetTableName_ + "' is not partitioned");
          }
          for (PartitionKeyValue pkv: partitionKeyValues_) {
            if (pkv.isStatic()) {
              throw new AnalysisException("Static partitioning is not supported for " +
                  "Iceberg tables.");
            }
          }
        } else {
          // Unpartitioned table, but INSERT has PARTITION clause
          throw new AnalysisException("PARTITION clause is only valid for INSERT into " +
              "partitioned table. '" + targetTableName_ + "' is not partitioned");
        }
      }
    }

    if (table_ instanceof FeFsTable) {
      setMaxTableSinks(analyzer_.getQueryOptions().getMax_fs_writers());
      FeFsTable fsTable = (FeFsTable) table_;
      StringBuilder error = new StringBuilder();
      fsTable.parseSkipHeaderLineCount(error);
      if (error.length() > 0) throw new AnalysisException(error.toString());
      try {
        if (!FileSystemUtil.isImpalaWritableFilesystem(fsTable.getLocation())) {
          throw new AnalysisException(String.format("Unable to INSERT into target " +
              "table (%s) because %s is not a supported filesystem.", targetTableName_,
              fsTable.getLocation()));
        }
      } catch (IOException e) {
        throw new AnalysisException(String.format("Unable to INSERT into target " +
            "table (%s): %s.", targetTableName_, e.getMessage()), e);
      }
      for (int colIdx = 0; colIdx < numClusteringCols; ++colIdx) {
        Column col = fsTable.getColumns().get(colIdx);
        // Hive 1.x has a number of issues handling BOOLEAN partition columns (see HIVE-6590).
        // Instead of working around the Hive bugs, INSERT is disabled for BOOLEAN
        // partitions in Impala when built against Hive 1. HIVE-6590 is currently resolved,
        // but not in Hive 2.3.2, the latest release as of 3/17/2018.
        if (col.getType() == Type.BOOLEAN) {
          throw new AnalysisException(String.format("INSERT into table with BOOLEAN " +
              "partition column (%s) is not supported: %s", col.getName(),
              targetTableName_));
        }
      }
      // Check if the target partition is supported to write. For static partitioning the
      // file format is available on partition level, however for dynamic partitioning
      // the target partition formats are unknown during analysis and planning, therefore
      // it will be verified based on the table metadata.
      if (isStaticPartitionTarget()) {
        PrunablePartition partition =
            HdfsTable.getPartition(fsTable, partitionKeyValues_);
        if (partition != null && partition instanceof FeFsPartition) {
          HdfsFileFormat fileFormat = ((FeFsPartition) partition).getFileFormat();
          Boolean notSupported =
              !HdfsTableSink.SUPPORTED_FILE_FORMATS.contains(fileFormat);
          if (notSupported) {
            throw new AnalysisException(String.format("Writing the destination " +
                "partition format '" + fileFormat + "' is not supported."));
          }
        }
      } else {
        Set<HdfsFileFormat> formats = fsTable.getFileFormats();
        Set<HdfsFileFormat> unsupportedFormats =
            Sets.difference(formats, HdfsTableSink.SUPPORTED_FILE_FORMATS);
        if (!unsupportedFormats.isEmpty()) {
          throw new AnalysisException(String.format("Destination table '" +
              fsTable.getFullName() + "' contains partition format(s) that are not " +
              "supported to write: '" + Joiner.on(',').join(unsupportedFormats) + "', " +
              "dynamic partition clauses are forbidden."));
        }
      }
    }

    if (table_ instanceof FeKuduTable) {
      if (overwrite_) {
        throw new AnalysisException("INSERT OVERWRITE not supported for Kudu tables.");
      }
      if (partitionKeyValues_ != null && !partitionKeyValues_.isEmpty()) {
        throw new AnalysisException(
            "Partition specifications are not supported for Kudu tables.");
      }
    }

    if (table_ instanceof FeIcebergTable) {
      FeIcebergTable iceTable = (FeIcebergTable)table_;
      if (overwrite_) {
        if (iceTable.getPartitionSpecs().size() > 1) {
          throw new AnalysisException("The Iceberg table has multiple partition specs. " +
              "This means the outcome of dynamic partition overwrite is unforeseeable. " +
              "Consider using TRUNCATE then INSERT INTO from the previous snapshot " +
              "to overwrite your table.");
        }
        validateBucketTransformForOverwrite(iceTable);
      }
      IcebergUtil.validateIcebergTableForInsert(iceTable);
    }

    if (isHBaseTable && overwrite_) {
      throw new AnalysisException("HBase doesn't have a way to perform INSERT OVERWRITE");
    }
  }

  /**
   * Validate if INSERT OVERWRITE could be allowed when the table has bucket partition
   * transform. 'INSERT OVERWRITE tbl SELECT * FROM tbl' can be allowed because the source
   * and target table is the same and the partitions are known.
   */
  private void validateBucketTransformForOverwrite(FeIcebergTable iceTable)
      throws AnalysisException {
    Preconditions.checkState(overwrite_ == true);
    IcebergPartitionSpec spec = iceTable.getDefaultPartitionSpec();
    if (!spec.hasPartitionFields()) return;
    for (IcebergPartitionField field : spec.getIcebergPartitionFields()) {
      if (field.getTransformType() != TIcebergPartitionTransformType.BUCKET) continue;
      if (queryStmt_ instanceof ValuesStmt) {
        throw new AnalysisException("The Iceberg table has BUCKET partitioning. " +
            "The outcome of static partition overwrite is unforeseeable. Consider " +
            "using TRUNCATE and INSERT INTO to overwrite your table.");
      }
      List<TableRef> tblRefs = queryStmt_.collectTableRefs();
      List<String> sourceTableAliases = tblRefs.size() <= 0 ? new ArrayList(0) :
          Arrays.asList(tblRefs.get(0).getAliases());
      String targetTableName = iceTable.getFullName();
      if (!(tblRefs.size() == 1 && sourceTableAliases.contains(targetTableName))) {
        throw new AnalysisException("The Iceberg table has BUCKET partitioning and " +
            "the source table does not match the target table. This means the " +
            "outcome of dynamic partition overwrite is unforeseeable. Consider using " +
            "TRUNCATE and INSERT INTO to overwrite your table.");
      }
      SelectList selectList = ((SelectStmt)queryStmt_).selectList_;
      if (selectList.getItems().size() != 1 && !selectList.getItems().get(0).isStar()) {
        throw new AnalysisException("The Iceberg table has BUCKET partitioning. " +
            "The outcome of dynamic partition overwrite is unforeseeable with the " +
            "given select list, only '*' allowed. Otherwise consider using TRUNCATE " +
            "and INSERT INTO to overwrite your table.");
      }
      if (((SelectStmt)queryStmt_).whereClause_ != null) {
        throw new AnalysisException("The Iceberg table has BUCKET partitioning. " +
            "The outcome of dynamic partition overwrite is unforeseeable with the " +
            "given select query with WHERE clause, selective overwrite is not " +
            "supported. Consider using TRUNCATE and INSERT INTO to overwrite your " +
            "table.");
      }
    }
  }

  private void analyzeWriteAccess() throws AnalysisException {
    if (!(table_ instanceof FeFsTable)) return;
    FeFsTable fsTable = (FeFsTable) table_;

    // If the partition target is fully static, then check for write access against
    // the specific partition. Otherwise, check the whole table.
    FeFsTable.Utils.checkWriteAccess(fsTable,
        isStaticPartitionTarget() ? partitionKeyValues_ : null, "INSERT");
  }

  /**
   * Returns true if all the partition key values of the target table are static.
   */
  public boolean isStaticPartitionTarget() {
    if (partitionKeyValues_ == null) return false;
    for (PartitionKeyValue pkv : partitionKeyValues_) {
      if (pkv.isDynamic()) return false;
    }
    return true;
  }

  public List<PartitionKeyValue> getPartitionKeyValues() {
    return partitionKeyValues_;
  }

  private boolean isIcebergTarget() {
    return table_ instanceof FeIcebergTable;
  }

  /**
   * Checks that the column permutation + select list + static partition exprs + dynamic
   * partition exprs collectively cover exactly all required columns in the target table,
   * depending on the table type.
   */
  private void checkColumnCoverage(List<Column> selectExprTargetColumns,
      Set<String> mentionedColumnNames, int numSelectListExprs,
      int numStaticPartitionExprs) throws AnalysisException {
    // Check that all required cols are mentioned by the permutation and partition clauses
    if (selectExprTargetColumns.size() + numStaticPartitionExprs !=
        table_.getColumns().size()) {
      // We've already ruled out too many columns in the permutation and partition clauses
      // by checking that there are no duplicates and that every column mentioned actually
      // exists. So all columns aren't mentioned in the query.
      if (table_ instanceof FeKuduTable) {
        checkRequiredKuduColumns(mentionedColumnNames);
      } else if (table_ instanceof FeHBaseTable) {
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
    Preconditions.checkState(table_ instanceof FeKuduTable);
    List<String> keyColumns = ((FeKuduTable) table_).getPrimaryKeyColumnNames();
    List<String> missingKeyColumnNames = new ArrayList<>();
    for (Column column : table_.getColumns()) {
      Preconditions.checkState(column instanceof KuduColumn);
      // Omit auto-incrementing column for Kudu table since the values of the column
      // will be assigned by Kudu engine.
      if (!mentionedColumnNames.contains(column.getName())
          && keyColumns.contains(column.getName())
          && !((KuduColumn)column).isAutoIncrementing()) {
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
    Preconditions.checkState(table_ instanceof FeHBaseTable);
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
    List<String> missingPartitionColumnNames = new ArrayList<>();
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
      List<Expr> selectListExprs, FeTable tbl, Analyzer analyzer)
      throws AnalysisException {
    // Temporary lists of partition key exprs and names in an arbitrary order.
    List<Expr> tmpPartitionKeyExprs = new ArrayList<>();
    List<String> tmpPartitionKeyNames = new ArrayList<>();

    int numClusteringCols = (tbl instanceof FeHBaseTable) ? 0
        : tbl.getNumClusteringCols();
    boolean isKuduTable = table_ instanceof FeKuduTable;
    Set<String> kuduPartitionColumnNames = null;
    if (isKuduTable) {
      kuduPartitionColumnNames = getKuduPartitionColumnNames((FeKuduTable) table_);
    }
    IcebergPartitionSpec icebergPartSpec = null;
    if (isIcebergTarget()) {
      icebergPartSpec = ((FeIcebergTable)table_).getDefaultPartitionSpec();
    }

    SetOperationStmt unionStmt =
        (queryStmt_ instanceof SetOperationStmt) ? (SetOperationStmt) queryStmt_ : null;
    List<Expr> widestTypeExprList = null;
    if (unionStmt != null && unionStmt.getWidestExprs() != null
        && unionStmt.getWidestExprs().size() > 0) {
      widestTypeExprList = unionStmt.getWidestExprs();
    }

    boolean convertToUtc =
        isKuduTable && analyzer.getQueryOptions().isWrite_kudu_utc_timestamps();

    // Check dynamic partition columns for type compatibility.
    for (int i = 0; i < selectListExprs.size(); ++i) {
      Column targetColumn = selectExprTargetColumns.get(i);
      // widestTypeExpr is widest type expression for column i
      Expr widestTypeExpr =
          (widestTypeExprList != null) ? widestTypeExprList.get(i) : null;
      Expr compatibleExpr = checkTypeCompatibility(targetTableName_.toString(),
          targetColumn, selectListExprs.get(i), analyzer, widestTypeExpr);
      if (targetColumn.getPosition() < numClusteringCols) {
        // This is a dynamic clustering column
        tmpPartitionKeyExprs.add(compatibleExpr);
        tmpPartitionKeyNames.add(targetColumn.getName());
      } else if (isKuduTable) {
        if (kuduPartitionColumnNames.contains(targetColumn.getName())) {
          tmpPartitionKeyExprs.add(compatibleExpr);
          tmpPartitionKeyNames.add(targetColumn.getName());
        }
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
          Expr compatibleExpr = checkTypeCompatibility(targetTableName_.toString(),
              tableColumn, pkv.getLiteralValue(), analyzer, null);
          tmpPartitionKeyExprs.add(compatibleExpr);
          tmpPartitionKeyNames.add(pkv.getColName());
        }
      }
    }

    if (isIcebergTarget()) {
      // Add partition key expressions in the order of the Iceberg partition fields.
      IcebergUtil.populatePartitionExprs(
          analyzer, widestTypeExprList, selectExprTargetColumns, selectListExprs,
          (FeIcebergTable)table_, partitionKeyExprs_, partitionColPos_);
    } else {
      // Reorder the partition key exprs and names to be consistent with the target table
      // declaration, and store their column positions.  We need those exprs in the
      // original order to create the corresponding Hdfs folder structure correctly, or
      // the indexes to construct rows to pass to the Kudu partitioning API.
      for (int i = 0; i < table_.getColumns().size(); ++i) {
        Column c = table_.getColumns().get(i);
        for (int j = 0; j < tmpPartitionKeyNames.size(); ++j) {
          if (c.getName().equals(tmpPartitionKeyNames.get(j))) {
            Expr expr = tmpPartitionKeyExprs.get(j);
            if (convertToUtc && expr.getType().isTimestamp()) {
              expr = ExprUtil.toUtcTimestampExpr(
                  analyzer, expr, true /*expectPreIfNonUnique*/);
            }
            partitionKeyExprs_.add(expr);
            partitionColPos_.add(i);
            break;
          }
        }
      }
    }

    if (isIcebergTarget() && icebergPartSpec.hasPartitionFields()) {
      int parts = 0;
      for (IcebergPartitionField pField : icebergPartSpec.getIcebergPartitionFields()) {
        if (pField.getTransformType() != TIcebergPartitionTransformType.VOID) {
          ++parts;
        }
      }
      if (CollectionUtils.isEmpty(columnPermutation_)) {
        Preconditions.checkState(partitionKeyExprs_.size() == parts);
      }
    }
    else if (isKuduTable) {
      Preconditions.checkState(
          partitionKeyExprs_.size() == kuduPartitionColumnNames.size());
    } else {
      Preconditions.checkState(partitionKeyExprs_.size() == numClusteringCols);
    }

    // Make sure we have stats for partitionKeyExprs
    for (Expr expr: partitionKeyExprs_) {
      expr.analyze(analyzer);
    }

    // Finally, 'undo' the permutation so that the selectListExprs are in Hive column
    // order, and add NULL expressions to all missing columns, unless this is an UPSERT.
    List<Column> columns = table_.getColumnsInHiveOrder();
    for (int col = 0; col < columns.size(); ++col) {
      Column tblColumn = columns.get(col);
      boolean matchFound = false;
      for (int i = 0; i < selectListExprs.size(); ++i) {
        if (selectExprTargetColumns.get(i).getName().equals(tblColumn.getName())) {
          Expr expr = selectListExprs.get(i);
          if (convertToUtc && expr.getType().isTimestamp()) {
            expr = ExprUtil.toUtcTimestampExpr(
                analyzer, expr, true /*expectPreIfNonUnique*/);
          }
          resultExprs_.add(expr);
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
            if (!kuduCol.hasDefaultValue() && !kuduCol.isNullable()
                && !kuduCol.isAutoIncrementing()) {
              throw new AnalysisException("Missing values for column that is not " +
                  "nullable and has no default value " + kuduCol.getName());
            }
          } else {
            // Unmentioned non-clustering columns get NULL literals with the appropriate
            // target type because Parquet cannot handle NULL_TYPE (IMPALA-617).
            NullLiteral nullExpr = NullLiteral.create(tblColumn.getType());
            resultExprs_.add(nullExpr);
            // In the case of INSERT INTO iceberg_tbl (col_a, col_b, ...), if the
            // partition columns are not in the columnPermutation_, we should fill it
            // with NullLiteral to partitionKeyExprs_ (IMPALA-11408).
            if (isIcebergTarget() && !CollectionUtils.isEmpty(columnPermutation_)
                && icebergPartSpec != null) {
              IcebergColumn targetColumn = (IcebergColumn) tblColumn;
              if (IcebergUtil.isPartitionColumn(targetColumn, icebergPartSpec)) {
                partitionKeyExprs_.add(nullExpr);
                partitionColPos_.add(targetColumn.getPosition());
              }
            }
          }
        }
      }
      // Store exprs for Kudu key columns.
      if (matchFound && isKuduTable) {
        FeKuduTable kuduTable = (FeKuduTable) table_;
        if (kuduTable.getPrimaryKeyColumnNames().contains(tblColumn.getName())) {
          primaryKeyExprs_.add(Iterables.getLast(resultExprs_));
        }
      }
    }

    // In the case of INSERT INTO iceberg_tbl (col_a, col_b, ...), to ensure that data is
    // written to the correct partition, we need to make sure that the partitionKeyExprs_
    // is in ascending order according to the column position of the Iceberg tables.
    if (isIcebergTarget() && !CollectionUtils.isEmpty(columnPermutation_)) {
      List<Pair<Integer, Expr>> exprPairs = Lists.newArrayList();
      for (int i = 0; i < partitionColPos_.size(); i++) {
        exprPairs.add(Pair.create(partitionColPos_.get(i), partitionKeyExprs_.get(i)));
      }
      exprPairs.sort(Comparator.comparingInt(p -> p.first));
      partitionColPos_.clear();
      partitionKeyExprs_.clear();
      for (Pair<Integer, Expr> exprPair : exprPairs) {
        partitionColPos_.add(exprPair.first);
        partitionKeyExprs_.add(exprPair.second);
      }
    }

    if (table_ instanceof FeKuduTable) {
      Preconditions.checkState(!primaryKeyExprs_.isEmpty());
    }

    // TODO: Check that HBase row-key columns are not NULL? See IMPALA-406
    if (needsGeneratedQueryStatement_) {
      // Build a query statement that returns NULL for every column
      List<SelectListItem> selectListItems = new ArrayList<>();
      for(Expr e: resultExprs_) {
        selectListItems.add(new SelectListItem(e, null));
      }
      SelectList selectList = new SelectList(selectListItems);
      queryStmt_ = new SelectStmt(selectList, null, null, null, null, null, null);
      queryStmt_.analyze(analyzer);
    }
  }

  private static Set<String> getKuduPartitionColumnNames(FeKuduTable table) {
    Set<String> ret = new HashSet<>();
    for (KuduPartitionParam partitionParam : table.getPartitionBy()) {
      ret.addAll(partitionParam.getColumnNames());
    }
    return ret;
  }

  /**
   * Analyzes the 'sort.columns' table property if it is set, and populates
   * sortColumns_ and sortExprs_. If there are errors during the analysis, this will throw
   * an AnalysisException.
   */
  private void analyzeSortColumns() throws AnalysisException {
    if (!(table_ instanceof FeFsTable)) return;

    Pair<List<Integer>, TSortingOrder> sortProperties =
        AlterTableSetTblProperties.analyzeSortColumns(table_,
        table_.getMetaStoreTable().getParameters());
    sortColumns_ = sortProperties.first;
    sortingOrder_ = sortProperties.second;

    // Assign sortExprs_ based on sortColumns_.
    for (Integer colIdx: sortColumns_) sortExprs_.add(resultExprs_.get(colIdx));
  }

  private void analyzePlanHints(Analyzer analyzer) throws AnalysisException {
    // If there are no hints then early exit.
    if (planHints_.isEmpty() &&
        !(analyzer.getQueryOptions().isSetDefault_hints_insert_statement())) return;

    if (table_ instanceof FeHBaseTable) {
      if (!planHints_.isEmpty()) {
        throw new AnalysisException(String.format("INSERT hints are only supported for " +
            "inserting into Hdfs and Kudu tables: %s", getTargetTableName()));
      }
      // Insert hints are not supported for HBase table so ignore any default hints.
      return;
    }

    // Set up the plan hints from query option DEFAULT_HINTS_INSERT_STATEMENT.
    // Default hint is ignored, if the original statement had hints.
    if (planHints_.isEmpty() &&
        analyzer.getQueryOptions().isSetDefault_hints_insert_statement()) {
      String defaultHints =
        analyzer.getQueryOptions().getDefault_hints_insert_statement();
      for (String hint: defaultHints.trim().split(":")) {
        planHints_.add(new PlanHint(hint.trim()));
      }
    }

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
        hasNoClusteredHint_ = true;
        analyzer.setHasPlanHints();
      } else {
        analyzer.addWarning("INSERT hint not recognized: " + hint);
      }
    }
    // Both flags may be false or one of them may be true, but not both.
    if (hasShuffleHint_ && hasNoShuffleHint_) {
      throw new AnalysisException("Conflicting INSERT hints: shuffle and noshuffle");
    }
    if (hasClusteredHint_ && hasNoClusteredHint_) {
      throw new AnalysisException("Conflicting INSERT hints: clustered and noclustered");
    }
  }

  @Override
  public List<Expr> getResultExprs() { return resultExprs_; }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    queryStmt_.rewriteExprs(rewriter);
  }

  private String getOpName() { return isUpsert_ ? "UPSERT" : "INSERT"; }

  public List<PlanHint> getPlanHints() { return planHints_; }
  public TableName getTargetTableName() { return targetTableName_; }
  public void setTargetTable(FeTable table) { this.table_ = table; }
  public boolean isTargetTableKuduTable() { return (table_ instanceof FeKuduTable); }
  public void setWriteId(long writeId) { this.writeId_ = writeId; }
  public boolean isOverwrite() { return overwrite_; }
  @Override
  public TSortingOrder getSortingOrder() { return sortingOrder_; }

  /**
   * Only valid after analysis
   */
  public QueryStmt getQueryStmt() { return queryStmt_; }
  @Override
  public List<Expr> getPartitionKeyExprs() { return partitionKeyExprs_; }
  public List<Integer> getPartitionColPos() { return partitionColPos_; }
  @Override
  public boolean hasShuffleHint() { return hasShuffleHint_; }
  @Override
  public boolean hasNoShuffleHint() { return hasNoShuffleHint_; }
  @Override
  public boolean hasClusteredHint() { return hasClusteredHint_; }
  @Override
  public boolean hasNoClusteredHint() { return hasNoClusteredHint_; }
  public List<Expr> getPrimaryKeyExprs() { return primaryKeyExprs_; }
  @Override
  public List<Expr> getSortExprs() { return sortExprs_; }
  public long getWriteId() { return writeId_; }

  // Clustering is enabled by default. If the table has a 'sort.columns' property and the
  // query has a 'noclustered' hint, we issue a warning during analysis and ignore the
  // 'noclustered' hint.
  public boolean requiresClustering() {
    return !hasNoClusteredHint_ || !sortExprs_.isEmpty();
  }

  public List<String> getMentionedColumns() {
    List<String> result = new ArrayList<>();
    List<Column> columns = table_.getColumns();
    for (Integer i: mentionedColumns_) result.add(columns.get(i).getName());
    return result;
  }

  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(table_ != null);
    return TableSink.create(table_, isUpsert_ ? TableSink.Op.UPSERT : TableSink.Op.INSERT,
        partitionKeyExprs_, resultExprs_, mentionedColumns_, overwrite_,
        requiresClustering(), new Pair<>(sortColumns_, sortingOrder_), writeId_,
        kuduTxnToken_, maxTableSinks_);
  }

  /**
   * Substitutes the result expressions, the partition key expressions, and the primary
   * key expressions with smap. Preserves the original types of those expressions during
   * the substitution.
   */
  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    resultExprs_ = Expr.substituteList(resultExprs_, smap, analyzer, true);
    partitionKeyExprs_ = Expr.substituteList(partitionKeyExprs_, smap, analyzer, true);
    primaryKeyExprs_ = Expr.substituteList(primaryKeyExprs_, smap, analyzer, true);
    sortExprs_ = Expr.substituteList(sortExprs_, smap, analyzer, true);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder strBuilder = new StringBuilder();

    if (withClause_ != null) strBuilder.append(withClause_.toSql(options) + " ");

    strBuilder.append(getOpName());
    if (!planHints_.isEmpty() && hintLoc_ == HintLocation.Start) {
      strBuilder.append(" " + ToSqlUtils.getPlanHintsSql(options, getPlanHints()));
    }
    if (overwrite_) {
      strBuilder.append(" OVERWRITE");
    } else {
      strBuilder.append(" INTO");
    }
    strBuilder.append(" TABLE " + originalTableName_);
    if (columnPermutation_ != null) {
      strBuilder.append("(");
      String sep = "";
      for (String col : columnPermutation_) {
        strBuilder.append(sep);
        strBuilder.append(ToSqlUtils.getIdentSql(col));
        sep = ", ";
      }
      strBuilder.append(")");
    }
    if (partitionKeyValues_ != null) {
      List<String> values = new ArrayList<>();
      for (PartitionKeyValue pkv: partitionKeyValues_) {
        values.add(ToSqlUtils.getIdentSql(pkv.getColName())
            + (pkv.getValue() != null ? ("=" + pkv.getValue().toSql(options)) : ""));
      }
      strBuilder.append(" PARTITION (" + Joiner.on(", ").join(values) + ")");
    }
    if (!planHints_.isEmpty() && hintLoc_ == HintLocation.End) {
      strBuilder.append(" " + ToSqlUtils.getPlanHintsSql(options, getPlanHints()));
    }
    if (!needsGeneratedQueryStatement_) {
      strBuilder.append(" " + queryStmt_.toSql(options));
    }
    return strBuilder.toString();
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    if (withClause_ != null) {
      for (View v: withClause_.getViews()) {
        v.getQueryStmt().collectTableRefs(tblRefs);
      }
    }
    tblRefs.add(new TableRef(targetTableName_.toPath(), null));
    if (queryStmt_ != null) queryStmt_.collectTableRefs(tblRefs);
  }

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    boolean hasChange = false;
    if (withClause_ != null) hasChange = withClause_.resolveTableMask(analyzer);
    if (queryStmt_ != null) hasChange |= queryStmt_.resolveTableMask(analyzer);
    return hasChange;
  }
}
