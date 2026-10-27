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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ByteUnits;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TIcebergOptimizationMode;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.ExprUtil;
import org.apache.impala.util.IcebergOptimizeFileFilter;
import org.apache.impala.util.IcebergUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Representation of an OPTIMIZE TABLE statement used to execute table maintenance tasks
 * in Iceberg tables. It operates in 2+1 modes: full table compaction, partial
 * compaction and no-operation. The mode is decided during analysis.
 * 1. Full table compaction: It rewrites the entire table
 * - compacting small files,
 * - merging delete deltas,
 * - rewriting the table according to the latest partition spec and schema.
 * 2. Partial table compaction: only a subset of the table's files is rewritten, all
 * others remain unchanged. The subset is narrowed by either or both of:
 * - 'FILE_SIZE_THRESHOLD_MB', which puts an upper limit on the size of the data files
 *   without deletes to be selected for compaction, and
 * - a PARTITION clause, which restricts the operation to the selected partition(s).
 * Within that scope all delete files and all data files with deletes are also rewritten,
 * since the delete deltas have to be merged. Note that the use of the latest schema and
 * partition spec for the entire table is not guaranteed in partial compaction mode.
 * 3. No-operation: covers every case when there is no change made to the table:
 * - the table is empty, or
 * - there were no files that qualified for the selection criteria in partial mode.
 * In partial mode the coordinator sends the catalog the complete set of files it scanned
 * and the catalog removes exactly those from the manifest. See OptimizeMode.
 */
public class OptimizeStmt extends DmlStatementBase {
  // Target table name as seen by the parser.
  private final TableName originalTableName_;
  // Data files larger than the given value will only be compacted if they are referenced
  // from delete files.
  // The value comes from FILE_SIZE_THRESHOLD_MB, but is converted to bytes.
  private final long fileSizeThreshold_;
  // Optional PARTITION clause. When set, only the selected partition(s) are compacted
  // and the operation runs in PARTIAL mode. Null if no PARTITION clause was specified.
  private final PartitionSet partitionSet_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Target table that should be compacted. May be qualified by analyze().
  private TableName tableName_;
  private TableRef tableRef_;
  // SELECT statement that reads the data that we want to compact.
  private SelectStmt sourceStmt_;
  // Output expressions that produce the final results to write to the table.
  // Set in prepareExpressions().
  // It will contain one Expr for each column of the table.
  // The i'th expr produces the i'th column of the table.
  private List<Expr> resultExprs_ = new ArrayList<>();
  // For every column of the target table that is referenced in the optional
  // 'sort.columns' table property, this list will contain the corresponding
  // result expr from 'resultExprs_'. Before insertion, all rows
  // will be sorted by these exprs. If the list is empty, no additional sorting by
  // non-partitioning columns will be performed. The column list must not contain
  // partition columns.
  private List<Expr> sortExprs_ = new ArrayList<>();
  private List<Integer> sortColumns_;
  private TSortingOrder sortingOrder_ = TSortingOrder.LEXICAL;;
  // Exprs corresponding to the partition fields of the table.
  protected List<Expr> partitionKeyExprs_ = new ArrayList<>();
  // Describes the mode of this OPTIMIZE operation. Decided during analysis in
  // selectFiles(). Null if the operation is a no-op (see isNoOp()), i.e. the table was
  // empty or no files were selected for compaction, so the operation has no effect.
  // Otherwise it is a RewriteAll or a Partial instance; see OptimizeMode:
  // - RewriteAll (REWRITE_ALL): rewrite all files of the table, ensuring the optimized
  //   table has the latest schema and partition spec. Used if neither
  //   FILE_SIZE_THRESHOLD_MB nor a PARTITION clause is set in the SQL command, or the
  //   threshold is large enough that all files get selected.
  // - Partial (PARTIAL): rewrite only the selected files, all others remain unchanged.
  //   Used if FILE_SIZE_THRESHOLD_MB or a PARTITION clause, or both, narrow the scope.
  private OptimizeMode mode_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  /**
   * Represents the mode of an OPTIMIZE operation that has an effect on the table. It is
   * decided during analysis, once the set of files to rewrite is known (see
   * selectFiles()), which is why it is modeled as an object instantiated at that point
   * rather than as a flag set at construction time.
   *
   * The mode also owns the data that is specific to it: only {@link Partial} restricts
   * the operation to a subset of the table, so only it carries the scoped file store the
   * scan reads and the paths of the files the commit replaces. This keeps the "a
   * selection exists only in PARTIAL mode" invariant type-enforced instead of spread
   * across nullable fields and mode checks.
   *
   * The NOOP case is not represented here; it is tracked by
   * {@link StatementBase#isNoOp()} and leaves mode_ null.
   */
  private abstract static class OptimizeMode {
    abstract TIcebergOptimizationMode toThrift();

    // Iceberg file path strings of the selected files this mode replaces: data files with
    // and without deletes, position delete files and equality delete files.
    // Deletion vectors are deliberately not listed. A DV lives in a Puffin file that may
    // also hold the DVs of data files in other partitions, so the catalog cannot remove
    // DVs by location; it removes a DV iff the data file it belongs to is replaced, and
    // that data file is in this set by construction.
    abstract Set<String> getFilesToReplace();

    // File store the compaction scan should read, holding exactly the selected files.
    // Null for RewriteAll, in which case the scan reads the table's own file store.
    IcebergContentFileStore getScopedFileStoreForScan() { return null; }
  }

  /**
   * Rewrites all files of the table, ensuring the optimized table has the latest schema
   * and partition spec. Used when neither FILE_SIZE_THRESHOLD_MB nor a PARTITION clause
   * narrows the operation, or when the threshold is large enough that all files get
   * selected.
   */
  private static final class RewriteAll extends OptimizeMode {
    @Override
    TIcebergOptimizationMode toThrift() { return TIcebergOptimizationMode.REWRITE_ALL; }

    // RewriteAll replaces the whole table, so no need to define the file selection.
    @Override
    Set<String> getFilesToReplace() { return Collections.emptySet(); }
  }

  /**
   * Rewrites only the selected files, leaving all others unchanged. The scope is narrowed
   * by a PARTITION clause (to the files of the selected partition(s)), by
   * FILE_SIZE_THRESHOLD_MB (to the data files without deletes below the threshold), or by
   * both. Within that scope all delete files and all data files with deletes are always
   * rewritten so that the delete deltas get merged. Note that the use of the latest
   * schema and partition spec for the entire table is not guaranteed in this mode.
   */
  private static final class Partial extends OptimizeMode {
    // File store the compaction scan reads. Holds exactly the selected files, in all
    // content file categories.
    private final IcebergContentFileStore scopedFileStore_;
    // Iceberg locations of the files the commit must remove by path.
    // Deletion vectors are not included; see OptimizeMode.getFilesToReplace().
    private final Set<String> filesToReplace_;

    Partial(IcebergContentFileStore scopedFileStore, Set<String> filesToReplace) {
      scopedFileStore_ = scopedFileStore;
      filesToReplace_ = filesToReplace;
    }

    @Override
    TIcebergOptimizationMode toThrift() { return TIcebergOptimizationMode.PARTIAL; }

    @Override
    IcebergContentFileStore getScopedFileStoreForScan() { return scopedFileStore_; }

    @Override
    Set<String> getFilesToReplace() { return filesToReplace_; }
  }

  public OptimizeStmt(TableName tableName) {
    this(tableName, -1, null);
  }

  public OptimizeStmt(TableName tableName, int fileSizeMb) {
    this(tableName, ((long) fileSizeMb) * ByteUnits.MEGABYTE, null);
  }

  public OptimizeStmt(TableName tableName, PartitionSet partitionSet) {
    this(tableName, -1, partitionSet);
  }

  public OptimizeStmt(TableName tableName, PartitionSet partitionSet, int fileSizeMb) {
    this(tableName, ((long) fileSizeMb) * ByteUnits.MEGABYTE, partitionSet);
  }

  private OptimizeStmt(TableName tableName, long fileSizeThreshold,
      PartitionSet partitionSet) {
    tableName_ = tableName;
    originalTableName_ = tableName_;
    fileSizeThreshold_ = fileSizeThreshold;
    partitionSet_ = partitionSet;
  }

  private OptimizeStmt(OptimizeStmt other) {
    super(other);
    tableName_ = other.tableName_;
    originalTableName_ = other.originalTableName_;
    fileSizeThreshold_ = other.fileSizeThreshold_;
    partitionSet_ = other.partitionSet_;
    tableRef_ = other.tableRef_;
    sourceStmt_ = other.sourceStmt_;
    resultExprs_ = other.resultExprs_;
    sortExprs_ = other.sortExprs_;
    sortColumns_ = other.sortColumns_;
    sortingOrder_ = other.sortingOrder_;
    partitionKeyExprs_ = other.partitionKeyExprs_;
    mode_ = other.mode_;
  }

  @Override
  public OptimizeStmt clone() { return new OptimizeStmt(this); }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);

    Preconditions.checkState(table_ == null);
    if (!tableName_.isFullyQualified()) {
      tableName_ = new TableName(analyzer.getDefaultDb(), tableName_.getTbl());
    }
    tableRef_ = TableRef.newTableRef(analyzer, tableName_.toPath(), null);
    // For OPTIMIZE, ALL privileges are required.
    table_ = analyzer.getTable(tableName_, Privilege.ALL);
    // Check that the referenced table has Iceberg format and is not a view.
    if (!(table_ instanceof FeIcebergTable)) {
      throw new AnalysisException("OPTIMIZE is only supported for Iceberg tables.");
    }
    FeIcebergTable iceTable = (FeIcebergTable) table_;
    Analyzer.ensureTableWriteSupported(iceTable, true);
    IcebergUtil.validateIcebergTableForInsert(iceTable);
    IcebergUtil.warnIfInvalidParquetBloomFilterProperties(iceTable, analyzer);

    if (iceTable.getFormatVersion() > 3) {
      throw new AnalysisException(String.format(
          "Impala does not support OPTIMIZE statements on Iceberg tables with format " +
              "version %d", iceTable.getFormatVersion()));
    }

    if (partitionSet_ != null) {
      partitionSet_.setTableName(tableName_);
      partitionSet_.setPrivilegeRequirement(Privilege.ALL);
      partitionSet_.analyze(analyzer);
    }

    selectFiles(iceTable, analyzer);
    if (isNoOp()) return;

    prepareExpressions(analyzer);
    createSourceStmt(analyzer);
    setMaxTableSinks(analyzer_.getQueryOptions().getMax_fs_writers());

    // Analyze 'sort.columns' and 'sort.order' table properties and populate
    // sortColumns_, sortExprs_, and sortingOrder_.
    analyzeSortColumns();

    // Add target table to descriptor table.
    analyzer.getDescTbl().setTargetTable(table_);
  }

  @Override
  public void reset() {
    super.reset();
    tableName_ = originalTableName_;
    tableRef_.reset();
    resultExprs_.clear();
    sortExprs_.clear();
    sortingOrder_ = TSortingOrder.LEXICAL;
    partitionKeyExprs_.clear();
    mode_ = null;
    if (partitionSet_ != null) {
      for (Expr e : partitionSet_.getPartitionExprs()) e.reset();
    }
    if (isNoOp()) return;
    sourceStmt_.reset();
    sortColumns_.clear();
  }

  public DataSink createDataSink() {
    TableSink tableSink = TableSink.create(table_, TableSink.Op.INSERT,
        partitionKeyExprs_, resultExprs_, new ArrayList<>(), false, true,
        new Pair<>(sortColumns_, sortingOrder_), -1, null,
        maxTableSinks_);
    return tableSink;
  }

  // First applies the PARTITION clause if present.
  // Then applies the FILE_SIZE_THRESHOLD_MB on the files narrowed by the first step.
  private void selectFiles(FeIcebergTable iceTable, Analyzer analyzer)
      throws AnalysisException {
    try {
      GroupedContentFiles contentFiles;
      if (partitionSet_ != null) {
        // Restrict the candidate files to the selected partition(s).
        IcebergUtil.PartitionFilesResult partitionResult = IcebergUtil.planPartitionFiles(
            iceTable, partitionSet_.getPartitionExprs(), analyzer);
        if (partitionResult.numPartitions == 0) {
          throw new AnalysisException("No matching partition(s) found");
        }
        contentFiles = new GroupedContentFiles(
            CloseableIterable.withNoopClose(partitionResult.fileScanTasks));
      } else {
        contentFiles = IcebergUtil.getIcebergFiles(iceTable, Lists.newArrayList(), null);
      }
      if (contentFiles.isEmpty()) {
        setIsNoOp();
        return;
      }
      IcebergOptimizeFileFilter.FilterArgs args =
          new IcebergOptimizeFileFilter.FilterArgs(contentFiles, fileSizeThreshold_);
      IcebergOptimizeFileFilter.FileFilteringResult filterResult =
          IcebergOptimizeFileFilter.filterFilesBySize(args);
      if (filterResult.isNoOp()) {
        setIsNoOp();
        return;
      }
      // Only a query that restricts neither the partitions nor the file sizes rewrites
      // the whole table; a partition-scoped query that selects all of its partitions'
      // files is still a partial operation.
      boolean rewriteWholeTable =
          partitionSet_ == null && filterResult.allFilesSelected();
      mode_ = rewriteWholeTable ?
          new RewriteAll() : buildPartialMode(iceTable, contentFiles, filterResult);
    } catch (AnalysisException e) {
      throw e;
    } catch (Exception e) {
      throw new AnalysisException(e);
    }
  }

  /**
   * Builds the PARTIAL-mode selection from the files that are in scope: the whole table
   * for an OPTIMIZE that only has FILE_SIZE_THRESHOLD_MB, or the selected partition(s)
   * for one with a PARTITION clause. The data files without deletes to rewrite are the
   * size-filtered subset of those; all other file categories (data files with deletes
   * and delete files) are always rewritten. The scan is restricted via a scoped file
   * store, and the exact set of replaced files is collected for the commit.
   */
  private Partial buildPartialMode(FeIcebergTable iceTable,
      GroupedContentFiles filesInScope,
      IcebergOptimizeFileFilter.FileFilteringResult filterResult)
      throws AnalysisException, ImpalaRuntimeException {
    GroupedContentFiles scopedFiles = new GroupedContentFiles();
    // The filter owns its selection: it does not modify 'filesInScope'.
    scopedFiles.dataFilesWithoutDeletes = filterResult.getSelectedFilesWithoutDeletes();
    scopedFiles.dataFilesWithDeletes = filesInScope.dataFilesWithDeletes;
    scopedFiles.positionDeleteFiles = filesInScope.positionDeleteFiles;
    scopedFiles.equalityDeleteFiles = filesInScope.equalityDeleteFiles;
    scopedFiles.dataFileToDV = filesInScope.dataFileToDV;

    // Check for delete files out of partition.
    if (partitionSet_ != null) {
      DeleteFile globalDelete = IcebergUtil.findGlobalDeleteFile(
          iceTable.getIcebergApiTable().specs(), scopedFiles);
      if (globalDelete != null) {
        throw new AnalysisException(String.format(
            "Cannot OPTIMIZE selected partition(s) of Iceberg table '%s': delete file " +
            "'%s' is not restricted to a partition and may apply to data outside the " +
            "selection. Run OPTIMIZE TABLE on the whole table instead.",
            iceTable.getFullName(), globalDelete.location()));
      }
    }

    // The scan reads all file categories from this store. The descriptors are reused from
    // the table's content file store, which is neither reloaded nor decoded.
    IcebergContentFileStore scopedStore =
        iceTable.getContentFileStore().createScopedStore(scopedFiles);
    // A content file the table's store has no descriptor for is dropped from the scoped
    // store and recorded as missing instead (see createScopedStore()). Such a file would
    // not be scanned, but its path would still be reported as replaced, removing live
    // data at commit time. Deletion vectors need no check: they are built straight
    // from the DeleteFile, with no descriptor lookup.
    if (scopedStore.hasMissingFile()) {
      throw new ImpalaRuntimeException(String.format("Cannot OPTIMIZE Iceberg table" +
          " '%s' due to unavailable files: %s.",
          iceTable.getFullName(), scopedStore.getMissingFiles()));
    }
    // Iceberg's own path strings are used so they match the files re-planned at commit
    // time by IcebergCatalogOpExecutor. getAllContentFiles() covers every category the
    // catalog removes by path and omits the deletion vectors, which it must not remove by
    // path; see OptimizeMode.getFilesToReplace().
    Set<String> filesToReplace = new HashSet<>();
    for (ContentFile<?> file : scopedFiles.getAllContentFiles()) {
      filesToReplace.add(file.location());
    }
    return new Partial(scopedStore, filesToReplace);
  }

  private void createSourceStmt(Analyzer analyzer) throws AnalysisException {
    List<TableRef> tableRefs = Arrays.asList(tableRef_);
    List<Column> columns = table_.getColumns();
    List<SelectListItem> selectListItems = new ArrayList<>();
    for (Column col : columns) {
      selectListItems.add(
          new SelectListItem(
              IcebergUtil.createSlotRef(analyzer, tableRef_, col.getName()), null));
    }
    SelectList selectList = new SelectList(selectListItems);
    sourceStmt_ = new SelectStmt(selectList, new FromClause(tableRefs), null,
        null, null, null, null);
    sourceStmt_.analyze(analyzer);
    sourceStmt_.getSelectList().getItems().addAll(
        ExprUtil.exprsAsSelectList(partitionKeyExprs_));
  }

  private void prepareExpressions(Analyzer analyzer) throws AnalysisException {
    List<Column> columns = table_.getColumnsInHiveOrder();
    for (Column col : columns) {
      resultExprs_.add(IcebergUtil.createSlotRef(analyzer, tableRef_, col.getName()));
    }
    IcebergUtil.addRowLineageExprs(analyzer, tableRef_, resultExprs_);
    IcebergUtil.populatePartitionExprs(analyzer, null, columns,
        resultExprs_, (FeIcebergTable) table_, partitionKeyExprs_, null);
  }

  /**
   * Analyzes the 'sort.columns' table property if it is set, and populates
   * sortColumns_ and sortExprs_. If there are errors during the analysis, this will throw
   * an AnalysisException.
   */
  private void analyzeSortColumns() throws AnalysisException {
    Pair<List<Integer>, TSortingOrder> sortProperties =
        AlterTableSetTblProperties.analyzeSortColumns(table_,
            table_.getMetaStoreTable().getParameters());
    sortColumns_ = sortProperties.first;
    sortingOrder_ = sortProperties.second;
    // Assign sortExprs_ based on sortColumns_.
    for (Integer colIdx: sortColumns_) sortExprs_.add(resultExprs_.get(colIdx));
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("OPTIMIZE TABLE");
    sb.append(options == ToSqlOptions.DEFAULT ?
        originalTableName_.toSql() : tableName_.toSql());
    if (partitionSet_ != null) {
      sb.append(" ").append(partitionSet_.toSql(options));
    }
    if (fileSizeThreshold_ >= 0) {
      sb.append(" (FILE_SIZE_THRESHOLD_MB=")
          .append(fileSizeThreshold_ / ByteUnits.MEGABYTE).append(")");
    }
    return sb.toString();
  }

  @Override
  public List<String> getNoopSummary() throws AnalysisException {
    return Collections.singletonList(partitionSet_ == null ?
        "The table is already optimized." :
        "The selected partition(s) are already optimized.");
  }

  public QueryStmt getQueryStmt() {
    return sourceStmt_;
  }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    if (isNoOp()) return;
    sourceStmt_.substituteResultExprs(smap, analyzer);
    resultExprs_ = Expr.substituteList(resultExprs_, smap, analyzer, true);
    partitionKeyExprs_ = Expr.substituteList(partitionKeyExprs_, smap, analyzer, true);
    sortExprs_ = Expr.substituteList(sortExprs_, smap, analyzer, true);
  }

  @Override
  public TSortingOrder getSortingOrder() { return sortingOrder_; }

  @Override
  public List<Expr> getSortExprs() { return sortExprs_; }

  @Override
  public List<Expr> getPartitionKeyExprs() { return partitionKeyExprs_; }

  /**
   * Always hash-partitions the rows on the partition keys, so that each partition is
   * written by a single writer. Otherwise the planner may skip the shuffle, e.g. when the
   * partition columns have few distinct values, and every scan instance then writes its
   * own file into the same partition, leaving it as fragmented as before.
   */
  @Override
  public List<Expr> getShuffleExprs() { return partitionKeyExprs_; }

  @Override
  public List<Expr> getResultExprs() { return resultExprs_; }

  public TIcebergOptimizationMode getOptimizationMode() {
    Preconditions.checkState(isAnalyzed());
    Preconditions.checkState(!isNoOp());
    return mode_.toThrift();
  }

  public Set<String> getFilesToReplace() {
    Preconditions.checkState(isAnalyzed());
    return mode_ == null ? Collections.emptySet() : mode_.getFilesToReplace();
  }

  /**
   * Returns the file store the compaction scan should read, or null if the whole table
   * should be scanned (i.e. not PARTIAL mode). Read by the planner via the analysis
   * result to build the Iceberg scan.
   */
  public IcebergContentFileStore getScopedFileStoreForScan() {
    Preconditions.checkState(isAnalyzed());
    return mode_ == null ? null : mode_.getScopedFileStoreForScan();
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    if (isNoOp()) return;
    sourceStmt_.rewriteExprs(rewriter);
  }
}
