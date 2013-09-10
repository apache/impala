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
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.FileFormat;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAccessEvent;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TCreateTableParams;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Represents a CREATE TABLE statement.
 */
public class CreateTableStmt extends StatementBase {
  private final ArrayList<ColumnDef> columnDefs_;
  private final String comment_;
  private final boolean isExternal_;
  private final boolean ifNotExists_;
  private final FileFormat fileFormat_;
  private final ArrayList<ColumnDef> partitionColumnDefs_;
  private final RowFormat rowFormat_;
  private final TableName tableName_;
  private final Map<String, String> tblProperties_;
  private HdfsURI location_;

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
   * @param ifNotExists - If true, no errors are thrown if the table already exists.
   * @param tblProperties - Optional map of key/values to persist with table metadata.
   */
  public CreateTableStmt(TableName tableName, List<ColumnDef> columnDefs,
      List<ColumnDef> partitionColumnDefs, boolean isExternal, String comment,
      RowFormat rowFormat, FileFormat fileFormat, HdfsURI location,
      boolean ifNotExists, Map<String, String> tblProperties) {
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
    this.partitionColumnDefs_ = Lists.newArrayList(partitionColumnDefs);
    this.rowFormat_ = rowFormat;
    this.tableName_ = tableName;
    this.tblProperties_ = tblProperties;
  }

  public String getTbl() { return tableName_.getTbl(); }
  public TableName getTblName() { return tableName_; }
  public List<ColumnDef> getColumnDefs() { return columnDefs_; }
  public List<ColumnDef> getPartitionColumnDefs() { return partitionColumnDefs_; }
  public String getComment() { return comment_; }
  public boolean isExternal() { return isExternal_; }
  public boolean getIfNotExists() { return ifNotExists_; }
  public HdfsURI getLocation() { return location_; }
  public void setLocation(HdfsURI location) { this.location_ = location; }
  public FileFormat getFileFormat() { return fileFormat_; }
  public RowFormat getRowFormat() { return rowFormat_; }

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
    StringBuilder sb = new StringBuilder("CREATE ");
    if (isExternal_) {
      sb.append("EXTERNAL ");
    }
    sb.append("TABLE ");
    if (ifNotExists_) {
      sb.append("IF NOT EXISTS ");
    }
    if (tableName_.getDb() != null) {
      sb.append(tableName_.getDb() + ".");
    }
    sb.append(tableName_.getTbl() + " (");
    sb.append(Joiner.on(", ").join(columnDefs_));
    sb.append(")");
    if (comment_ != null) {
      sb.append(" COMMENT = '" + comment_ + "'");
    }

    if (partitionColumnDefs_.size() > 0) {
      sb.append(String.format(" PARTITIONED BY (%s)",
          Joiner.on(", ").join(partitionColumnDefs_)));
    }

    if (rowFormat_ != RowFormat.DEFAULT_ROW_FORMAT) {
      sb.append(" ROW FORMAT DELIMITED");
      if (rowFormat_.getFieldDelimiter() != null) {
        sb.append(" FIELDS TERMINATED BY '" + rowFormat_.getFieldDelimiter() + "'");
      }
      if (rowFormat_.getEscapeChar() != null) {
        sb.append(" ESCAPED BY '" + rowFormat_.getEscapeChar() + "'");
      }
      if (rowFormat_.getLineDelimiter() != null) {
        sb.append(" LINES TERMINATED BY '" + rowFormat_.getLineDelimiter() + "'");
      }
    }

    sb.append(" STORED AS " + fileFormat_.getDescription());

    if (location_ != null) {
      sb.append(" LOCATION = '" + location_ + "'");
    }
    if (tblProperties_ != null) {
      sb.append(" TBLPROPERTIES (");
      List<String> properties = Lists.newArrayList();
      for (Map.Entry<String, String> entry: tblProperties_.entrySet()) {
        properties.add(String.format("'{0}'='{1}'", entry.getKey(), entry.getValue()));
      }
      sb.append(Joiner.on(", ").join(properties) + ")");
    }
    return sb.toString();
  }

  public TCreateTableParams toThrift() {
    TCreateTableParams params = new TCreateTableParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    for (ColumnDef col: getColumnDefs()) {
      params.addToColumns(col.toThrift());
    }
    for (ColumnDef col: getPartitionColumnDefs()) {
      params.addToPartition_columns(col.toThrift());
    }
    params.setOwner(getOwner());
    params.setIs_external(isExternal());
    params.setComment(comment_);
    params.setLocation(location_ == null ? null : location_.toString());
    params.setRow_format(rowFormat_.toThrift());
    params.setFile_format(fileFormat_.toThrift());
    params.setIf_not_exists(getIfNotExists());
    if (tblProperties_ != null) {
      params.setTable_properties(tblProperties_);
    }
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
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

    if (columnDefs_.size() == 0) {
      throw new AnalysisException("A table requires at least 1 column");
    }

    if (location_ != null) location_.analyze(analyzer, Privilege.ALL);

    analyzeRowFormatValue(rowFormat_.getFieldDelimiter());
    analyzeRowFormatValue(rowFormat_.getLineDelimiter());
    analyzeRowFormatValue(rowFormat_.getEscapeChar());

    // Check that all the column names are valid and unique.
    Set<String> colNames = Sets.newHashSet();
    for (ColumnDef colDef: columnDefs_) {
      colDef.analyze();
      if (!colNames.add(colDef.getColName().toLowerCase())) {
        throw new AnalysisException("Duplicate column name: " + colDef.getColName());
      }
    }
    for (ColumnDef colDef: partitionColumnDefs_) {
      colDef.analyze();
      if (!colDef.getColType().supportsTablePartitioning()) {
        throw new AnalysisException(
            String.format("Type '%s' is not supported as partition-column type " +
                "in column: %s", colDef.getColType().toString(), colDef.getColName()));
      }
      if (!colNames.add(colDef.getColName().toLowerCase())) {
        throw new AnalysisException("Duplicate column name: " + colDef.getColName());
      }
    }
  }

  private void analyzeRowFormatValue(String value) throws AnalysisException {
    if (value == null) return;
    if (value.length() != 1) {
      throw new AnalysisException("ESCAPED BY values and LINE/FIELD " +
          "terminators must have length of 1: " + value);
    }
  }
}
