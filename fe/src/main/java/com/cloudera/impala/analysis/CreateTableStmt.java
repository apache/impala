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

import java.util.List;
import java.util.ArrayList;

import com.cloudera.impala.catalog.FileFormat;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TCreateTableParams;
import com.cloudera.impala.thrift.TFileFormat;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a CREATE TABLE statement.
 * TODO: Add support for creating partitioned tables (IMPALA-102)
 */
public class CreateTableStmt extends ParseNodeBase {
  private final ArrayList<ColumnDef> columnDefs;
  private final String comment;
  private final boolean isExternal;
  private final boolean ifNotExists;
  private final FileFormat fileFormat;
  private final String location;
  private final ArrayList<ColumnDef> partitionColumnDefs;
  private final RowFormat rowFormat;
  private final TableName tableName;

  // Set during analysis
  private String dbName;

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
   */
  public CreateTableStmt(TableName tableName, List<ColumnDef> columnDefs,
      List<ColumnDef> partitionColumnDefs, boolean isExternal, String comment,
      RowFormat rowFormat, FileFormat fileFormat, String location, boolean ifNotExists) {
    Preconditions.checkNotNull(columnDefs);
    Preconditions.checkNotNull(partitionColumnDefs);
    Preconditions.checkNotNull(fileFormat);
    Preconditions.checkNotNull(rowFormat);
    Preconditions.checkNotNull(tableName);

    this.columnDefs = Lists.newArrayList(columnDefs);
    this.comment = comment;
    this.isExternal = isExternal;
    this.ifNotExists = ifNotExists;
    this.fileFormat = fileFormat;
    this.location = location;
    this.partitionColumnDefs = Lists.newArrayList(partitionColumnDefs);
    this.rowFormat = rowFormat;
    this.tableName = tableName;
  }

  public String getTbl() {
    return tableName.getTbl();
  }

  /**
   * Can only be called after analysis, returns the name of the database the table will
   * be created within.
   */
  public String getDb() {
    Preconditions.checkNotNull(dbName);
    return dbName;
  }

  public List<ColumnDef> getColumnDefs() {
    return columnDefs;
  }

  public List<ColumnDef> getPartitionColumnDefs() {
    return partitionColumnDefs;
  }

  public String getComment() {
    return comment;
  }

  public boolean isExternal() {
    return isExternal;
  }

  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public String getLocation() {
    return location;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public RowFormat getRowFormat() {
    return rowFormat;
  }

  public String debugString() {
    return toSql();
  }

  public String toSql() {
    StringBuilder sb = new StringBuilder("CREATE ");
    if (isExternal) {
      sb.append("EXTERNAL ");
    }
    sb.append("TABLE ");
    if (ifNotExists) { 
      sb.append("IF NOT EXISTS ");
    }
    if (tableName.getDb() != null) {
      sb.append(tableName.getDb() + ".");
    }
    sb.append(tableName.getTbl() + " (");
    sb.append(Joiner.on(", ").join(columnDefs));
    sb.append(")");
    if (comment != null) {
      sb.append(" COMMENT = '" + comment + "'");
    }

    if (partitionColumnDefs.size() > 0) {
      sb.append(String.format(" PARTITIONED BY (%s)",
          Joiner.on(", ").join(partitionColumnDefs)));
    }

    if (rowFormat != RowFormat.DEFAULT_ROW_FORMAT) {
      sb.append(" ROW FORMAT DELIMITED");
      if (rowFormat.getFieldDelimiter() != null) {
        sb.append(" FIELDS TERMINATED BY '" + rowFormat.getFieldDelimiter() + "'");
      }
      if (rowFormat.getLineDelimiter() != null) {
        sb.append(" LINES TERMINATED BY '" + rowFormat.getLineDelimiter() + "'");
      }
    }

    sb.append(" STORED AS " + fileFormat.getDescription());

    if (location != null) {
      sb.append(" LOCATION = '" + location + "'");
    }
    return sb.toString();
  }

  public TCreateTableParams toThrift() {
    TCreateTableParams params = new TCreateTableParams();
    params.setTable_name(getTbl());
    params.setDb(getDb());
    for (ColumnDef col: getColumnDefs()) {
      params.addToColumns(col.toThrift());
    }
    for (ColumnDef col: getPartitionColumnDefs()) {
      params.addToPartition_columns(col.toThrift());
    }
    params.setIs_external(isExternal());
    params.setComment(comment);
    params.setLocation(location);
    params.setField_terminator(rowFormat.getFieldDelimiter());
    params.setLine_terminator(rowFormat.getLineDelimiter());
    params.setFile_format(fileFormat.toThrift());
    params.setIf_not_exists(getIfNotExists());
    return params;
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    dbName = tableName.getDb();
    if (dbName == null) {
      dbName = analyzer.getDefaultDb();
    }

    if (analyzer.getCatalog().getDb(dbName) == null) {
      throw new AnalysisException("Database does not exist: " + dbName);
    }

    if (analyzer.getCatalog().containsTable(dbName, getTbl()) && !ifNotExists) {
      throw new AnalysisException(String.format("Table already exists: %s.%s",
          dbName, getTbl())); 
    }

    if (columnDefs.size() == 0) {
      throw new AnalysisException("A table requires at least 1 column");
    }
  }
}
