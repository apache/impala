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
import java.util.Map;
import java.util.Set;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsStorageDescriptor;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAccessEvent;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TCreateTableParams;
import com.cloudera.impala.thrift.THdfsFileFormat;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.util.AvroSchemaParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Represents a CREATE TABLE statement.
 */
public class CreateTableStmt extends StatementBase {
  private final ArrayList<ColumnDesc> columnDefs_;
  private final String comment_;
  private final boolean isExternal_;
  private final boolean ifNotExists_;
  private final THdfsFileFormat fileFormat_;
  private final ArrayList<ColumnDesc> partitionColDefs_;
  private final RowFormat rowFormat_;
  private final TableName tableName_;
  private final Map<String, String> tblProperties_;
  private final Map<String, String> serdeProperties_;
  private final HdfsCachingOp cachingOp_;
  private HdfsUri location_;

  // Set during analysis
  private String dbName_;
  private String owner_;

  /**
   * Builds a CREATE TABLE statement
   * @param tableName - Name of the new table
   * @param columnDefs - List of column definitions for the table
   * @param partitionColumnDefs - List of partition column definitions for the table
   * @param isExternal - If true, the table's data will be preserved if dropped.
   * @param comment - Comment to attach to the table
   * @param rowFormat - Custom row format of the table. Use RowFormat.DEFAULT_ROW_FORMAT
   *          to specify default row format.
   * @param fileFormat - File format of the table
   * @param location - The HDFS location of where the table data will stored.
   * @param cachingOp - The HDFS caching op that should be applied to this table.
   * @param ifNotExists - If true, no errors are thrown if the table already exists.
   * @param tblProperties - Optional map of key/values to persist with table metadata.
   * @param serdeProperties - Optional map of key/values to persist with table serde
   *                          metadata.
   */
  public CreateTableStmt(TableName tableName, List<ColumnDesc> columnDefs,
      List<ColumnDesc> partitionColumnDefs, boolean isExternal, String comment,
      RowFormat rowFormat, THdfsFileFormat fileFormat, HdfsUri location,
      HdfsCachingOp cachingOp, boolean ifNotExists, Map<String, String> tblProperties,
      Map<String, String> serdeProperties) {
    Preconditions.checkNotNull(columnDefs);
    Preconditions.checkNotNull(partitionColumnDefs);
    Preconditions.checkNotNull(fileFormat);
    Preconditions.checkNotNull(rowFormat);
    Preconditions.checkNotNull(tableName);

    this.columnDefs_ = Lists.newArrayList(columnDefs);
    this.comment_ = comment;
    this.isExternal_ = isExternal;
    this.ifNotExists_ = ifNotExists;
    this.fileFormat_ = fileFormat;
    this.location_ = location;
    this.cachingOp_ = cachingOp;
    this.partitionColDefs_ = Lists.newArrayList(partitionColumnDefs);
    this.rowFormat_ = rowFormat;
    this.tableName_ = tableName;
    this.tblProperties_ = tblProperties;
    this.serdeProperties_ = serdeProperties;
    unescapeProperties(tblProperties_);
    unescapeProperties(serdeProperties_);
  }

  /**
   * Copy c'tor.
   */
  public CreateTableStmt(CreateTableStmt other) {
    columnDefs_ = Lists.newArrayList(other.columnDefs_);
    comment_ = other.comment_;
    isExternal_ = other.isExternal_;
    ifNotExists_ = other.ifNotExists_;
    fileFormat_ = other.fileFormat_;
    location_ = other.location_;
    cachingOp_ = other.cachingOp_;
    partitionColDefs_ = Lists.newArrayList(other.partitionColDefs_);
    rowFormat_ = other.rowFormat_;
    tableName_ = other.tableName_;
    tblProperties_ = other.tblProperties_;
    serdeProperties_ = other.serdeProperties_;
  }

  @Override
  public CreateTableStmt clone() { return new CreateTableStmt(this); }

  public String getTbl() { return tableName_.getTbl(); }
  public TableName getTblName() { return tableName_; }
  public List<ColumnDesc> getColumnDefs() { return columnDefs_; }
  public List<ColumnDesc> getPartitionColumnDefs() { return partitionColDefs_; }
  public String getComment() { return comment_; }
  public boolean isExternal() { return isExternal_; }
  public boolean getIfNotExists() { return ifNotExists_; }
  public HdfsUri getLocation() { return location_; }
  public void setLocation(HdfsUri location) { this.location_ = location; }
  public THdfsFileFormat getFileFormat() { return fileFormat_; }
  public RowFormat getRowFormat() { return rowFormat_; }
  public Map<String, String> getTblProperties() { return tblProperties_; }
  public Map<String, String> getSerdeProperties() { return serdeProperties_; }

  /**
   * Can only be called after analysis, returns the owner of this table (the user from
   * the current session).
   */
  public String getOwner() {
    Preconditions.checkNotNull(owner_);
    return owner_;
  }

  /**
   * Can only be called after analysis, returns the name of the database the table will
   * be created within.
   */
  public String getDb() {
    Preconditions.checkNotNull(dbName_);
    return dbName_;
  }

  @Override
  public String toSql() {
    return ToSqlUtils.getCreateTableSql(this);
  }

  public TCreateTableParams toThrift() {
    TCreateTableParams params = new TCreateTableParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    for (ColumnDesc col: getColumnDefs()) {
      params.addToColumns(col.toThrift());
    }
    for (ColumnDesc col: getPartitionColumnDefs()) {
      params.addToPartition_columns(col.toThrift());
    }
    params.setOwner(getOwner());
    params.setIs_external(isExternal());
    params.setComment(comment_);
    params.setLocation(location_ == null ? null : location_.toString());
    if (cachingOp_ != null) params.setCache_op(cachingOp_.toThrift());
    params.setRow_format(rowFormat_.toThrift());
    params.setFile_format(fileFormat_);
    params.setIf_not_exists(getIfNotExists());
    if (tblProperties_ != null) params.setTable_properties(tblProperties_);
    if (serdeProperties_ != null) params.setSerde_properties(serdeProperties_);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(tableName_ != null && !tableName_.isEmpty());
    tableName_.analyze();
    dbName_ = analyzer.getTargetDbName(tableName_);
    owner_ = analyzer.getUser().getName();

    if (analyzer.dbContainsTable(dbName_, tableName_.getTbl(), Privilege.CREATE) &&
        !ifNotExists_) {
      throw new AnalysisException(Analyzer.TBL_ALREADY_EXISTS_ERROR_MSG +
          String.format("%s.%s", dbName_, getTbl()));
    }

    analyzer.addAccessEvent(new TAccessEvent(dbName_ + "." + tableName_.getTbl(),
        TCatalogObjectType.TABLE, Privilege.CREATE.toString()));

    // Only Avro tables can have empty column defs because they can infer them from
    // the Avro schema.
    if (columnDefs_.isEmpty() && fileFormat_ != THdfsFileFormat.AVRO) {
      throw new AnalysisException("Table requires at least 1 column");
    }

    if (location_ != null) location_.analyze(analyzer, Privilege.ALL);

    analyzeRowFormatValue(rowFormat_.getFieldDelimiter());
    analyzeRowFormatValue(rowFormat_.getLineDelimiter());
    analyzeRowFormatValue(rowFormat_.getEscapeChar());

    // Check that all the column names are valid and unique.
    analyzeColumnDefs(analyzer);

    if (fileFormat_ == THdfsFileFormat.AVRO) {
      List<ColumnDesc> newColumnDefs = analyzeAvroSchema(analyzer);
      if (newColumnDefs != columnDefs_) {
        // Replace the old column defs with the new ones and analyze them.
        columnDefs_.clear();
        columnDefs_.addAll(newColumnDefs);
        analyzeColumnDefs(analyzer);
      }
    }

    if (cachingOp_ != null) cachingOp_.analyze(analyzer);
  }

  /**
   * Analyzes columnDefs_ and partitionColumnDescs_ checking whether all column
   * names are unique.
   */
  private void analyzeColumnDefs(Analyzer analyzer) throws AnalysisException {
    Set<String> colNames = Sets.newHashSet();
    for (ColumnDesc colDef: columnDefs_) {
      colDef.analyze();
      if (!colNames.add(colDef.getColName().toLowerCase())) {
        throw new AnalysisException("Duplicate column name: " + colDef.getColName());
      }
    }
    for (ColumnDesc colDef: partitionColDefs_) {
      colDef.analyze();
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
   * Analyzes the Avro schema and compares it with the columnDefs_ to detect
   * inconsistencies. Returns a list of column descriptors that should be
   * used for creating the table (possibly identical to columnDefs_).
   */
  private List<ColumnDesc> analyzeAvroSchema(Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(fileFormat_ == THdfsFileFormat.AVRO);
    // Look for the schema in TBLPROPERTIES and in SERDEPROPERTIES, with the latter
    // taking precedence.
    List<Map<String, String>> schemaSearchLocations = Lists.newArrayList();
    String fullTblName = dbName_ + "." + tableName_.getTbl();
    schemaSearchLocations.add(serdeProperties_);
    schemaSearchLocations.add(tblProperties_);
    String avroSchema = null;
    try {
      avroSchema = HdfsTable.getAvroSchema(schemaSearchLocations,
          dbName_ + "." + tableName_.getTbl(), true);
    } catch (TableLoadingException e) {
      throw new AnalysisException("Error loading Avro schema: " + e.getMessage(), e);
    }

    if (Strings.isNullOrEmpty(avroSchema)) {
      throw new AnalysisException("Avro schema is null or empty: " + fullTblName);
    }

    // List of columns parsed from the Avro schema.
    List<Column> avroColumns = null;
    try {
      avroColumns = AvroSchemaParser.parse(avroSchema);
    } catch (Exception e) {
      throw new AnalysisException(String.format(
          "Error parsing Avro schema for table '%s': %s", fullTblName,
          e.getMessage()));
    }
    Preconditions.checkNotNull(avroColumns);

    // Analyze the Avro schema to detect inconsistencies with the columnDefs_.
    // In case of inconsistencies, the column defs are ignored in favor of the Avro
    // schema for simplicity and, in particular, to enable COMPUTE STATS (IMPALA-1104).
    String warnStr = null; // set if inconsistency detected
    if (avroColumns.size() != columnDefs_.size() && !columnDefs_.isEmpty()) {
      warnStr = String.format(
          "Ignoring column definitions in favor of Avro schema.\n" +
          "The Avro schema has %s column(s) but %s column definition(s) were given.",
           avroColumns.size(), columnDefs_.size());
    } else {
      // Determine whether the column names and the types match.
      for (int i = 0; i < columnDefs_.size(); ++i) {
        ColumnDesc colDesc = columnDefs_.get(i);
        Column avroCol = avroColumns.get(i);
        avroCol.getType().analyze();
        String warnDetail = null;
        if (!colDesc.getColName().equalsIgnoreCase(avroCol.getName())) {
          warnDetail = "name";
        }
        if (colDesc.getType().isStringType() &&
            avroCol.getType().isStringType()) {
          // This is OK -- avro types for CHAR, VARCHAR, and STRING are "string"
        } else if (!colDesc.getType().equals(avroCol.getType())) {
          warnDetail = "type";
        }
        if (warnDetail != null) {
          warnStr = String.format(
              "Ignoring column definitions in favor of Avro schema due to a " +
              "mismatched column %s at position %s.\n" +
              "Column definition: %s\n" +
              "Avro schema column: %s", warnDetail, i + 1,
              colDesc.getColName() + " " + colDesc.getType().toSql(),
              avroCol.getName() + " " + avroCol.getType().toSql());
          break;
        }
      }
    }

    if (warnStr != null || columnDefs_.isEmpty()) {
      analyzer.addWarning(warnStr);
      // Create new columnDefs_ based on the Avro schema and return them.
      List<ColumnDesc> avroSchemaColDefs =
          Lists.newArrayListWithCapacity(avroColumns.size());
      for (Column avroCol: avroColumns) {
        avroSchemaColDefs.add(new ColumnDesc(avroCol.getName(), avroCol.getType(),
            avroCol.getComment()));
      }
      return avroSchemaColDefs;
    }
    // The existing col defs are consistent with the Avro schema.
    return columnDefs_;
  }

  private void analyzeRowFormatValue(String value) throws AnalysisException {
    if (value == null) return;
    if (HdfsStorageDescriptor.parseDelim(value) == null) {
      throw new AnalysisException("ESCAPED BY values and LINE/FIELD " +
          "terminators must be specified as a single character or as a decimal " +
          "value in the range [-128:127]: " + value);
    }
  }

  /**
   * Unescapes all values in the property map.
   */
  public static void unescapeProperties(Map<String, String> propertyMap) {
    if (propertyMap == null) return;
    for (Map.Entry<String, String> kv : propertyMap.entrySet()) {
      propertyMap.put(kv.getKey(),
          new StringLiteral(kv.getValue()).getUnescapedValue());
    }
  }
}
