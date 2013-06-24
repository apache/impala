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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.FileFormat;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TCreateTableLikeParams;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

/**
 * Represents a CREATE TABLE LIKE statement which creates a new table based on
 * a copy of an existing table definition.
 */
public class CreateTableLikeStmt extends StatementBase {
  private final TableName tableName;
  private final TableName srcTableName;
  private final boolean isExternal;
  private final String comment;
  private final FileFormat fileFormat;
  private final HdfsURI location;
  private final boolean ifNotExists;

  // Set during analysis
  private String dbName;
  private String srcDbName;
  private String owner;

  /**
   * Builds a CREATE TABLE LIKE statement
   * @param tableName - Name of the new table
   * @param srcTableName - Name of the source table (table to copy)
   * @param isExternal - If true, the table's data will be preserved if dropped.
   * @param comment - Comment to attach to the table
   * @param fileFormat - File format of the table
   * @param location - The HDFS location of where the table data will stored.
   * @param ifNotExists - If true, no errors are thrown if the table already exists
   */
  public CreateTableLikeStmt(TableName tableName, TableName srcTableName,
      boolean isExternal, String comment, FileFormat fileFormat, HdfsURI location,
      boolean ifNotExists) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(srcTableName);
    this.tableName = tableName;
    this.srcTableName = srcTableName;
    this.isExternal = isExternal;
    this.comment = comment;
    this.fileFormat = fileFormat;
    this.location = location;
    this.ifNotExists = ifNotExists;
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

  public String getSrcTbl() {
    return srcTableName.getTbl();
  }

  /**
   * Can only be called after analysis, returns the name of the database the table will
   * be created within.
   */
  public String getSrcDb() {
    Preconditions.checkNotNull(srcDbName);
    return srcDbName;
  }

  public boolean isExternal() {
    return isExternal;
  }

  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public String getComment() {
    return comment;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public HdfsURI getLocation() {
    return location;
  }

  public String getOwner() {
    Preconditions.checkNotNull(owner);
    return owner;
  }

  @Override
  public String debugString() {
    return toSql();
  }

  @Override
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
    sb.append(tableName.getTbl() + " LIKE ");
    if (srcTableName.getDb() != null) {
      sb.append(srcTableName.getDb() + ".");
    }
    sb.append(srcTableName.getTbl());
    if (comment != null) {
      sb.append(" COMMENT '" + comment + "'");
    }
    if (fileFormat != null) {
      sb.append(" STORED AS " + fileFormat);
    }
    if (location != null) {
      sb.append(" LOCATION '" + location + "'");
    }
    return sb.toString();
  }

  public TCreateTableLikeParams toThrift() {
    TCreateTableLikeParams params = new TCreateTableLikeParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    params.setSrc_table_name(new TTableName(getSrcDb(), getSrcTbl()));
    params.setOwner(getOwner());
    params.setIs_external(isExternal());
    params.setComment(comment);
    if (fileFormat != null) {
      params.setFile_format(fileFormat.toThrift());
    }
    params.setLocation(location == null ? null : location.toString());
    params.setIf_not_exists(getIfNotExists());
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    Preconditions.checkState(srcTableName != null && !srcTableName.isEmpty());

    // Make sure the source table exists and the user has permission to access it.
    srcDbName = analyzer
        .getTable(srcTableName, Privilege.VIEW_METADATA)
        .getDb().getName();
    dbName = analyzer.getTargetDbName(tableName);

    if (analyzer.dbContainsTable(dbName, tableName.getTbl(), Privilege.CREATE) &&
        !ifNotExists) {
      throw new AnalysisException(Analyzer.TBL_ALREADY_EXISTS_ERROR_MSG +
          String.format("%s.%s", dbName, getTbl()));
    }
    owner = analyzer.getUser().getName();
  }
}
