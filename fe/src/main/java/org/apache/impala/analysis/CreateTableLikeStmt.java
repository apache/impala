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

import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCreateTableLikeParams;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TTableName;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Represents a CREATE TABLE LIKE statement which creates a new table based on
 * a copy of an existing table definition.
 */
public class CreateTableLikeStmt extends StatementBase {
  private final TableName tableName_;
  private final List<String> sortColumns_;
  private final TableName srcTableName_;
  private final boolean isExternal_;
  private final String comment_;
  private final THdfsFileFormat fileFormat_;
  private final HdfsUri location_;
  private final boolean ifNotExists_;

  // Set during analysis
  private String dbName_;
  private String srcDbName_;
  private String owner_;

  /**
   * Builds a CREATE TABLE LIKE statement
   * @param tableName - Name of the new table
   * @param sortColumns - List of columns to sort by during inserts
   * @param srcTableName - Name of the source table (table to copy)
   * @param isExternal - If true, the table's data will be preserved if dropped.
   * @param comment - Comment to attach to the table
   * @param fileFormat - File format of the table
   * @param location - The HDFS location of where the table data will stored.
   * @param ifNotExists - If true, no errors are thrown if the table already exists
   */
  public CreateTableLikeStmt(TableName tableName, List<String> sortColumns,
      TableName srcTableName, boolean isExternal, String comment,
      THdfsFileFormat fileFormat, HdfsUri location, boolean ifNotExists) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(srcTableName);
    this.tableName_ = tableName;
    this.sortColumns_ = sortColumns;
    this.srcTableName_ = srcTableName;
    this.isExternal_ = isExternal;
    this.comment_ = comment;
    this.fileFormat_ = fileFormat;
    this.location_ = location;
    this.ifNotExists_ = ifNotExists;
  }

  public String getTbl() { return tableName_.getTbl(); }
  public String getSrcTbl() { return srcTableName_.getTbl(); }
  public boolean isExternal() { return isExternal_; }
  public boolean getIfNotExists() { return ifNotExists_; }
  public THdfsFileFormat getFileFormat() { return fileFormat_; }
  public HdfsUri getLocation() { return location_; }

  /**
   * Can only be called after analysis, returns the name of the database the table will
   * be created within.
   */
  public String getDb() {
    Preconditions.checkNotNull(dbName_);
    return dbName_;
  }

  /**
   * Can only be called after analysis, returns the name of the database the table will
   * be created within.
   */
  public String getSrcDb() {
    Preconditions.checkNotNull(srcDbName_);
    return srcDbName_;
  }

  public String getOwner() {
    Preconditions.checkNotNull(owner_);
    return owner_;
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("CREATE ");
    if (isExternal_) sb.append("EXTERNAL ");
    sb.append("TABLE ");
    if (ifNotExists_) sb.append("IF NOT EXISTS ");
    if (tableName_.getDb() != null) sb.append(tableName_.getDb() + ".");
    sb.append(tableName_.getTbl() + " ");
    if (sortColumns_ != null && !sortColumns_.isEmpty()) {
      sb.append("SORT BY (" + Joiner.on(",").join(sortColumns_) + ") ");
    }
    sb.append("LIKE ");
    if (srcTableName_.getDb() != null) sb.append(srcTableName_.getDb() + ".");
    sb.append(srcTableName_.getTbl());
    if (comment_ != null) sb.append(" COMMENT '" + comment_ + "'");
    if (fileFormat_ != null) sb.append(" STORED AS " + fileFormat_);
    if (location_ != null) sb.append(" LOCATION '" + location_ + "'");
    return sb.toString();
  }

  public TCreateTableLikeParams toThrift() {
    TCreateTableLikeParams params = new TCreateTableLikeParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    params.setSrc_table_name(new TTableName(getSrcDb(), getSrcTbl()));
    params.setOwner(getOwner());
    params.setIs_external(isExternal());
    params.setComment(comment_);
    if (fileFormat_ != null) params.setFile_format(fileFormat_);
    params.setLocation(location_ == null ? null : location_.toString());
    params.setIf_not_exists(getIfNotExists());
    params.setSort_columns(sortColumns_);
    return params;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
    tblRefs.add(new TableRef(srcTableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(tableName_ != null && !tableName_.isEmpty());
    Preconditions.checkState(srcTableName_ != null && !srcTableName_.isEmpty());
    // We currently don't support creating a Kudu table using a CREATE TABLE LIKE
    // statement (see IMPALA-4052).
    if (fileFormat_ == THdfsFileFormat.KUDU) {
      throw new AnalysisException("CREATE TABLE LIKE is not supported for Kudu tables");
    }

    // Make sure the source table exists and the user has permission to access it.
    Table srcTable = analyzer.getTable(srcTableName_, Privilege.VIEW_METADATA);
    if (KuduTable.isKuduTable(srcTable.getMetaStoreTable())) {
      throw new AnalysisException("Cloning a Kudu table using CREATE TABLE LIKE is " +
          "not supported.");
    }
    srcDbName_ = srcTable.getDb().getName();
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

    if (location_ != null) {
      location_.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);
    }

    if (sortColumns_ != null) {
      TableDef.analyzeSortColumns(sortColumns_, srcTable);
    }
  }
}
