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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.MetaStoreUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Represents the table parameters in a CREATE TABLE statement. These parameters
 * correspond to the following clauses in a CREATE TABLE statement:
 * - EXTERNAL
 * - IF NOT EXISTS
 * - PARTITIONED BY
 * - PARTITION BY
 * - ROWFORMAT
 * - FILEFORMAT
 * - COMMENT
 * - SERDEPROPERTIES
 * - TBLPROPERTIES
 * - LOCATION
 * - CACHED IN
 * - SORT BY
 */
class TableDef {
  // Name of the new table
  private final TableName tableName_;

  // List of column definitions
  private final List<ColumnDef> columnDefs_ = new ArrayList<>();

  // Names of primary key columns. Populated by the parser. An empty value doesn't
  // mean no primary keys were specified as the columnDefs_ could contain primary keys.
  private final List<String> primaryKeyColNames_ = new ArrayList<>();

  // If true, the table's data will be preserved if dropped.
  private final boolean isExternal_;

  // If true, no errors are thrown if the table already exists.
  private final boolean ifNotExists_;

  // Partitioning parameters.
  private final TableDataLayout dataLayout_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Authoritative list of primary key column definitions populated during analysis.
  private final List<ColumnDef> primaryKeyColDefs_ = new ArrayList<>();

  // True if analyze() has been called.
  private boolean isAnalyzed_ = false;

  // Generated Kudu properties set during analysis.
  private Map<String, String> generatedKuduProperties_ = new HashMap<>();

  // END: Members that need to be reset()
  /////////////////////////////////////////

  /**
   * Set of table options. These options are grouped together for convenience while
   * parsing CREATE TABLE statements. They are typically found at the end of CREATE
   * TABLE statements.
   */
  static class Options {
    // Optional list of columns to sort data by when inserting into this table.
    final List<String> sortCols;

    // Comment to attach to the table
    final String comment;

    // Custom row format of the table. Leave null to specify default row format.
    final RowFormat rowFormat;

    // Key/values to persist with table serde metadata.
    final Map<String, String> serdeProperties;

    // File format of the table
    final THdfsFileFormat fileFormat;

    // The HDFS location of where the table data will stored.
    final HdfsUri location;

    // The HDFS caching op that should be applied to this table.
    final HdfsCachingOp cachingOp;

    // Key/values to persist with table metadata.
    final Map<String, String> tblProperties;

    Options(List<String> sortCols, String comment, RowFormat rowFormat,
        Map<String, String> serdeProperties, THdfsFileFormat fileFormat, HdfsUri location,
        HdfsCachingOp cachingOp, Map<String, String> tblProperties,
        TQueryOptions queryOptions) {
      this.sortCols = sortCols;
      this.comment = comment;
      this.rowFormat = rowFormat;
      Preconditions.checkNotNull(serdeProperties);
      this.serdeProperties = serdeProperties;
      // The file format passed via STORED AS <file format> has a higher precedence than
      // the one set in query options.
      this.fileFormat = (fileFormat != null) ?
          fileFormat : queryOptions.getDefault_file_format();
      this.location = location;
      this.cachingOp = cachingOp;
      Preconditions.checkNotNull(tblProperties);
      this.tblProperties = tblProperties;
    }

    public Options(String comment, TQueryOptions queryOptions) {
      // Passing null to file format so that it uses the file format from the query option
      // if specified, otherwise it will use the default file format, which is TEXT.
      this(ImmutableList.of(), comment, RowFormat.DEFAULT_ROW_FORMAT,
          new HashMap<>(), /*file format*/ null, null, null, new HashMap<>(),
          queryOptions);
    }
  }

  private Options options_;

  // Result of analysis.
  private TableName fqTableName_;

  TableDef(TableName tableName, boolean isExternal, boolean ifNotExists) {
    tableName_ = tableName;
    isExternal_ = isExternal;
    ifNotExists_ = ifNotExists;
    dataLayout_ = TableDataLayout.createEmptyLayout();
  }

  public void reset() {
    primaryKeyColDefs_.clear();
    columnDefs_.clear();
    isAnalyzed_ = false;
    generatedKuduProperties_.clear();
  }

  public TableName getTblName() {
    return fqTableName_ != null ? fqTableName_ : tableName_;
  }
  public String getTbl() { return tableName_.getTbl(); }
  public boolean isAnalyzed() { return isAnalyzed_; }
  List<ColumnDef> getColumnDefs() { return columnDefs_; }
  List<String> getColumnNames() { return ColumnDef.toColumnNames(columnDefs_); }

  List<String> getPartitionColumnNames() {
    return ColumnDef.toColumnNames(getPartitionColumnDefs());
  }

  List<ColumnDef> getPartitionColumnDefs() {
    return dataLayout_.getPartitionColumnDefs();
  }

  boolean isKuduTable() { return options_.fileFormat == THdfsFileFormat.KUDU; }
  List<String> getPrimaryKeyColumnNames() { return primaryKeyColNames_; }
  List<ColumnDef> getPrimaryKeyColumnDefs() { return primaryKeyColDefs_; }
  boolean isExternal() { return isExternal_; }
  boolean getIfNotExists() { return ifNotExists_; }
  Map<String, String> getGeneratedKuduProperties() { return generatedKuduProperties_; }
  void putGeneratedKuduProperty(String key, String value) {
    Preconditions.checkNotNull(key);
    generatedKuduProperties_.put(key, value);
  }
  List<KuduPartitionParam> getKuduPartitionParams() {
    return dataLayout_.getKuduPartitionParams();
  }
  void setOptions(Options options) {
    Preconditions.checkNotNull(options);
    options_ = options;
  }
  List<String> getSortColumns() { return options_.sortCols; }
  String getComment() { return options_.comment; }
  Map<String, String> getTblProperties() { return options_.tblProperties; }
  HdfsCachingOp getCachingOp() { return options_.cachingOp; }
  HdfsUri getLocation() { return options_.location; }
  Map<String, String> getSerdeProperties() { return options_.serdeProperties; }
  THdfsFileFormat getFileFormat() { return options_.fileFormat; }
  RowFormat getRowFormat() { return options_.rowFormat; }

  /**
   * Analyzes the parameters of a CREATE TABLE statement.
   */
  void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    Preconditions.checkState(tableName_ != null && !tableName_.isEmpty());
    fqTableName_ = analyzer.getFqTableName(getTblName());
    fqTableName_.analyze();
    // Disallow creation of full ACID table.
    analyzer.ensureTableNotFullAcid(options_.tblProperties, fqTableName_.toString());
    analyzeColumnDefs(analyzer);
    analyzePrimaryKeys();

    if (analyzer.dbContainsTable(getTblName().getDb(), getTbl(), Privilege.CREATE)
        && !getIfNotExists()) {
      throw new AnalysisException(Analyzer.TBL_ALREADY_EXISTS_ERROR_MSG + getTblName());
    }

    analyzer.addAccessEvent(new TAccessEvent(fqTableName_.toString(),
        TCatalogObjectType.TABLE, Privilege.CREATE.toString()));

    Preconditions.checkNotNull(options_);
    analyzeOptions(analyzer);
    isAnalyzed_ = true;
  }

  /**
   * Analyzes table and partition column definitions, checking whether all column
   * names are unique.
   */
  private void analyzeColumnDefs(Analyzer analyzer) throws AnalysisException {
    Set<String> colNames = new HashSet<>();
    for (ColumnDef colDef: columnDefs_) {
      colDef.analyze(analyzer);
      if (!colNames.add(colDef.getColName().toLowerCase())) {
        throw new AnalysisException("Duplicate column name: " + colDef.getColName());
      }
      if (!isKuduTable() && colDef.hasKuduOptions()) {
        throw new AnalysisException(String.format("Unsupported column options for " +
            "file format '%s': '%s'", getFileFormat().name(), colDef.toString()));
      }
    }
    for (ColumnDef colDef: getPartitionColumnDefs()) {
      colDef.analyze(analyzer);
      if (!colDef.getType().supportsTablePartitioning()) {
        throw new AnalysisException(
            String.format("Type '%s' is not supported as partition-column type " +
                "in column: %s", colDef.getType().toSql(), colDef.getColName()));
      }
      if (!colNames.add(colDef.getColName().toLowerCase())) {
        throw new AnalysisException("Duplicate column name: " + colDef.getColName());
      }
    }
  }

  /**
   * Analyzes the primary key columns. Checks if the specified primary key columns exist
   * in the table column definitions and if composite primary keys are properly defined
   * using the PRIMARY KEY (col,..col) clause.
   */
  private void analyzePrimaryKeys() throws AnalysisException {
    for (ColumnDef colDef: columnDefs_) {
      if (colDef.isPrimaryKey()) primaryKeyColDefs_.add(colDef);
    }
    if (primaryKeyColDefs_.size() > 1) {
      throw new AnalysisException("Multiple primary keys specified. " +
          "Composite primary keys can be specified using the " +
          "PRIMARY KEY (col1, col2, ...) syntax at the end of the column definition.");
    }
    if (primaryKeyColNames_.isEmpty()) return;
    if (!primaryKeyColDefs_.isEmpty()) {
      throw new AnalysisException("Multiple primary keys specified. " +
          "Composite primary keys can be specified using the " +
          "PRIMARY KEY (col1, col2, ...) syntax at the end of the column definition.");
    }
    Map<String, ColumnDef> colDefsByColName = ColumnDef.mapByColumnNames(columnDefs_);
    for (String colName: primaryKeyColNames_) {
      colName = colName.toLowerCase();
      ColumnDef colDef = colDefsByColName.remove(colName);
      if (colDef == null) {
        if (ColumnDef.toColumnNames(primaryKeyColDefs_).contains(colName)) {
          throw new AnalysisException(String.format("Column '%s' is listed multiple " +
              "times as a PRIMARY KEY.", colName));
        }
        throw new AnalysisException(String.format(
            "PRIMARY KEY column '%s' does not exist in the table", colName));
      }
      if (colDef.isExplicitNullable()) {
        throw new AnalysisException("Primary key columns cannot be nullable: " +
            colDef.toString());
      }
      primaryKeyColDefs_.add(colDef);
    }
  }

  /**
   * Analyzes the list of columns in 'sortCols' against the columns of 'table'. Each
   * column of 'sortCols' must occur in 'table' as a non-partitioning column. 'table'
   * must be an HDFS table. If there are errors during the analysis, this will throw an
   * AnalysisException.
   */
  public static void analyzeSortColumns(List<String> sortCols, FeTable table)
      throws AnalysisException {
    Preconditions.checkState(table instanceof FeFsTable);
    analyzeSortColumns(sortCols, Column.toColumnNames(table.getNonClusteringColumns()),
        Column.toColumnNames(table.getClusteringColumns()));
  }

  /**
   * Analyzes the list of columns in 'sortCols' and returns their matching positions in
   * 'tableCols'. Each column must occur in 'tableCols' and must not occur in
   * 'partitionCols'. If there are errors during the analysis, this will throw an
   * AnalysisException.
   */
  public static List<Integer> analyzeSortColumns(List<String> sortCols,
      List<String> tableCols, List<String> partitionCols)
      throws AnalysisException {
    // The index of each sort column in the list of table columns.
    Set<Integer> colIdxs = new LinkedHashSet<>();

    int numColumns = 0;
    for (String sortColName: sortCols) {
      ++numColumns;
      // Make sure it's not a partition column.
      if (partitionCols.contains(sortColName)) {
        throw new AnalysisException(String.format("SORT BY column list must not " +
            "contain partition column: '%s'", sortColName));
      }

      // Determine the index of each sort column in the list of table columns.
      boolean foundColumn = false;
      for (int j = 0; j < tableCols.size(); ++j) {
        if (tableCols.get(j).equalsIgnoreCase(sortColName)) {
          if (colIdxs.contains(j)) {
            throw new AnalysisException(String.format("Duplicate column in SORT BY " +
                "list: %s", sortColName));
          }
          colIdxs.add(j);
          foundColumn = true;
          break;
        }
      }
      if (!foundColumn) {
        throw new AnalysisException(String.format("Could not find SORT BY column '%s' " +
            "in table.", sortColName));
      }
    }
    Preconditions.checkState(numColumns == colIdxs.size());
    return Lists.newArrayList(colIdxs);
  }

  private void analyzeOptions(Analyzer analyzer) throws AnalysisException {
    MetaStoreUtil.checkShortPropertyMap("Property", options_.tblProperties);
    MetaStoreUtil.checkShortPropertyMap("Serde property", options_.serdeProperties);

    if (options_.location != null) {
      options_.location.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);
    }

    if (options_.cachingOp != null) {
      options_.cachingOp.analyze(analyzer);
      if (options_.cachingOp.shouldCache() && options_.location != null &&
          !FileSystemUtil.isPathCacheable(options_.location.getPath())) {
        throw new AnalysisException(String.format("Location '%s' cannot be cached. " +
            "Please retry without caching: CREATE TABLE ... UNCACHED",
            options_.location));
      }
    }

    // Analyze 'skip.header.line.format' property.
    AlterTableSetTblProperties.analyzeSkipHeaderLineCount(options_.tblProperties);
    analyzeRowFormat(analyzer);

    String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
    if (options_.tblProperties.containsKey(sortByKey)) {
      throw new AnalysisException(String.format("Table definition must not contain the " +
          "%s table property. Use SORT BY (...) instead.", sortByKey));
    }

    // Analyze sort columns.
    if (options_.sortCols == null) return;
    if (isKuduTable()) {
      throw new AnalysisException("SORT BY is not supported for Kudu tables.");
    }
    analyzeSortColumns(options_.sortCols, getColumnNames(),
        getPartitionColumnNames());
  }

  private void analyzeRowFormat(Analyzer analyzer) throws AnalysisException {
    if (options_.rowFormat == null) return;
    if (isKuduTable()) {
      throw new AnalysisException(String.format(
          "ROW FORMAT cannot be specified for file format %s.", options_.fileFormat));
    }

    Byte fieldDelim = analyzeRowFormatValue(options_.rowFormat.getFieldDelimiter());
    Byte lineDelim = analyzeRowFormatValue(options_.rowFormat.getLineDelimiter());
    Byte escapeChar = analyzeRowFormatValue(options_.rowFormat.getEscapeChar());
    if (options_.fileFormat == THdfsFileFormat.TEXT) {
      if (fieldDelim == null) fieldDelim = HdfsStorageDescriptor.DEFAULT_FIELD_DELIM;
      if (lineDelim == null) lineDelim = HdfsStorageDescriptor.DEFAULT_LINE_DELIM;
      if (escapeChar == null) escapeChar = HdfsStorageDescriptor.DEFAULT_ESCAPE_CHAR;
      if (fieldDelim.equals(lineDelim)) {
        throw new AnalysisException("Field delimiter and line delimiter have same " +
            "value: byte " + fieldDelim);
      }
      if (fieldDelim.equals(escapeChar)) {
        analyzer.addWarning("Field delimiter and escape character have same value: " +
            "byte " + fieldDelim + ". Escape character will be ignored");
      }
      if (lineDelim.equals(escapeChar)) {
        analyzer.addWarning("Line delimiter and escape character have same value: " +
            "byte " + lineDelim + ". Escape character will be ignored");
      }
    }
  }

  private Byte analyzeRowFormatValue(String value) throws AnalysisException {
    if (value == null) return null;
    Byte byteVal = HdfsStorageDescriptor.parseDelim(value);
    if (byteVal == null) {
      throw new AnalysisException("ESCAPED BY values and LINE/FIELD " +
          "terminators must be specified as a single character or as a decimal " +
          "value in the range [-128:127]: " + value);
    }
    return byteVal;
  }
}
