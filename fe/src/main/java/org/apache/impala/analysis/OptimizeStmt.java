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
import org.apache.iceberg.DataFile;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.catalog.IcebergFileDescriptor;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
 * 2. Partial table compaction: This mode has an additional parameter,
 * 'FILE_SIZE_THRESHOLD_MB', that puts an upper limit on the size of data files without
 * deletes to be selected for compaction. The operation executes the following tasks:
 * - compacting data files without deletes that are smaller than the given threshold,
 * - merging all delete deltas and compacting all data files with deletes,
 * - rewriting the selected files according to the latest partition spec and schema.
 * Note that the use of the latest schema and partition spec for the entire table is not
 * guaranteed in partial compaction mode.
 * 3. No-operation: covers every case when there is no change made to the table:
 * - the table is empty, or
 * - there were no files that qualified for the selection criteria in partial mode.
 */
public class OptimizeStmt extends DmlStatementBase {
  // Target table name as seen by the parser.
  private final TableName originalTableName_;
  // Data files larger than the given value will only be compacted if they are referenced
  // from delete files.
  // The value comes from FILE_SIZE_THRESHOLD_MB, but is converted to bytes.
  private final long fileSizeThreshold_;

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
  // File paths of data files without deletes selected for compaction after file size
  // filtering.
  private Set<String> selectedIcebergFilePaths_ = new HashSet<>();
  // Describes the mode of this OPTIMIZE operation. Decided during analysis.
  // NOOP: The table was empty or no files were selected for compaction. This means that
  // the operation has no effect.
  // PARTIAL: In this mode only the selected files are compacted, all others will remain
  // unchanged. Files that will be selected:
  // - data files without deletes that are smaller than fileSizeThreshold_,
  // - all delete files,
  // - all data files with deletes.
  // Possible only if FILE_SIZE_THRESHOLD_MB is set.
  // REWRITE_ALL: Rewrite all files of the table. This will ensure that the optimized
  // table has the latest schema and partition spec. Possible if FILE_SIZE_THRESHOLD_MB is
  // not set in the SQL command or large enough that all files get selected.
  private TIcebergOptimizationMode mode_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  public OptimizeStmt(TableName tableName) {
    tableName_ = tableName;
    originalTableName_ = tableName_;
    fileSizeThreshold_ = -1;
  }

  public OptimizeStmt(TableName tableName, int fileSizeMb) {
    tableName_ = tableName;
    originalTableName_ = tableName_;
    fileSizeThreshold_ = ((long) fileSizeMb) * ByteUnits.MEGABYTE;
  }

  private OptimizeStmt(OptimizeStmt other) {
    super(other);
    tableName_ = other.tableName_;
    originalTableName_ = other.originalTableName_;
    fileSizeThreshold_ = other.fileSizeThreshold_;
    tableRef_ = other.tableRef_;
    sourceStmt_ = other.sourceStmt_;
    resultExprs_ = other.resultExprs_;
    sortExprs_ = other.sortExprs_;
    sortColumns_ = other.sortColumns_;
    sortingOrder_ = other.sortingOrder_;
    partitionKeyExprs_ = other.partitionKeyExprs_;
    selectedIcebergFilePaths_ = other.selectedIcebergFilePaths_;
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
    IcebergUtil.validateIcebergTableForInsert(iceTable);

    selectFiles(iceTable);

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
    sourceStmt_.reset();
    resultExprs_.clear();
    sortExprs_.clear();
    sortColumns_.clear();
    sortingOrder_ = TSortingOrder.LEXICAL;
    partitionKeyExprs_.clear();
    selectedIcebergFilePaths_.clear();
    mode_ = null;
  }

  public DataSink createDataSink() {
    TableSink tableSink = TableSink.create(table_, TableSink.Op.INSERT,
        partitionKeyExprs_, resultExprs_, new ArrayList<>(), false, true,
        new Pair<>(sortColumns_, sortingOrder_), -1, null,
        maxTableSinks_);
    return tableSink;
  }

  private void selectFiles(FeIcebergTable iceTable) throws AnalysisException {
    try {
      GroupedContentFiles contentFiles = IcebergUtil.getIcebergFiles(
          iceTable, Lists.newArrayList(), null);
      if (contentFiles.isEmpty()) {
        mode_ = TIcebergOptimizationMode.NOOP;
      } else {
        IcebergOptimizeFileFilter.FilterArgs args =
            new IcebergOptimizeFileFilter.FilterArgs(contentFiles, fileSizeThreshold_);
        IcebergOptimizeFileFilter.FileFilteringResult filterResult =
            IcebergOptimizeFileFilter.filterFilesBySize(args);
        mode_ = filterResult.getOptimizationMode();

        if (mode_ == TIcebergOptimizationMode.PARTIAL) {
          List<IcebergFileDescriptor> selectedDataFilesWithoutDeletes =
              dataFilesWithoutDeletesToFileDescriptors(
                  filterResult.getSelectedFilesWithoutDeletes(), iceTable);
          tableRef_.setSelectedDataFilesForOptimize(selectedDataFilesWithoutDeletes);
          collectAbsolutePaths(selectedDataFilesWithoutDeletes);
        }
      }
    } catch (Exception e) {
      throw new AnalysisException(e);
    }
  }

  private List<IcebergFileDescriptor> dataFilesWithoutDeletesToFileDescriptors(
      List<DataFile> contentFiles, FeIcebergTable iceTable)
      throws IOException, ImpalaRuntimeException {
    GroupedContentFiles selectedContentFiles = new GroupedContentFiles();
    selectedContentFiles.dataFilesWithoutDeletes = contentFiles;
    IcebergContentFileStore selectedFiles =
        new IcebergContentFileStore(iceTable.getIcebergApiTable(),
            iceTable.getContentFileStore().getDataFilesWithoutDeletes(),
            selectedContentFiles);
    return selectedFiles.getDataFilesWithoutDeletes();
  }

  private void collectAbsolutePaths(List<IcebergFileDescriptor> selectedFiles) {
    for (IcebergFileDescriptor fileDesc : selectedFiles) {
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(
          fileDesc.getAbsolutePath(((FeIcebergTable) table_).getHdfsBaseDir()));
      selectedIcebergFilePaths_.add(path.toUri().toString());
    }
  }

  private void createSourceStmt(Analyzer analyzer) throws AnalysisException {
    List<TableRef> tableRefs = Arrays.asList(tableRef_);
    List<Column> columns = table_.getColumns();
    List<SelectListItem> selectListItems = new ArrayList<>();
    for (Column col : columns) {
      selectListItems.add(
          new SelectListItem(createSlotRef(analyzer, col.getName()), null));
    }
    SelectList selectList = new SelectList(selectListItems);
    sourceStmt_ = new SelectStmt(selectList, new FromClause(tableRefs), null,
        null, null, null, null);
    sourceStmt_.analyze(analyzer);
    sourceStmt_.getSelectList().getItems().addAll(
        ExprUtil.exprsAsSelectList(partitionKeyExprs_));
  }

  private void prepareExpressions(Analyzer analyzer) throws AnalysisException {
    List<Column> columns = table_.getColumns();
    for (Column col : columns) {
      resultExprs_.add(createSlotRef(analyzer, col.getName()));
    }
    IcebergUtil.populatePartitionExprs(analyzer, null, columns,
        resultExprs_, (FeIcebergTable) table_, partitionKeyExprs_, null);
  }

  private SlotRef createSlotRef(Analyzer analyzer, String colName)
      throws AnalysisException {
    List<String> path = org.apache.impala.analysis.Path
        .createRawPath(tableRef_.getUniqueAlias(), colName);
    SlotRef ref = new SlotRef(path);
    ref.analyze(analyzer);
    return ref;
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
    if (options == ToSqlOptions.DEFAULT) {
      return "OPTIMIZE TABLE" + originalTableName_.toSql();
    }
    return "OPTIMIZE TABLE" + tableName_.toSql();
  }

  public QueryStmt getQueryStmt() {
    return sourceStmt_;
  }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
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

  @Override
  public List<Expr> getResultExprs() { return resultExprs_; }

  public TIcebergOptimizationMode getOptimizationMode() {
    Preconditions.checkState(isAnalyzed());
    return mode_;
  }

  public Set<String> getSelectedIcebergFilePaths() {
    Preconditions.checkState(isAnalyzed());
    return selectedIcebergFilePaths_;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    sourceStmt_.rewriteExprs(rewriter);
  }
}