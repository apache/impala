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

import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.catalog.FileFormat;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.thrift.TCreateTableLikeParams;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a CREATE TABLE LIKE statement which creates a new table based on
 * a copy of an existing table definition.
 */
public class CreateTableLikeStmt extends ParseNodeBase {
  private final TableName tableName;
  private final TableName srcTableName;
  private final boolean isExternal;
  private final String location;
  private final boolean ifNotExists;

  // Set during analysis
  private String dbName;
  private String srcDbName;

  /**
   * Builds a CREATE TABLE LIKE statement
   * @param tableName - Name of the new table
   * @param srcTableName - Name of the source table (table to copy)
   * @param isExternal - If true, the table's data will be preserved if dropped.
   * @param location - The HDFS location of where the table data will stored.
   * @param ifNotExists - If true, no errors are thrown if the table already exists
   */
  public CreateTableLikeStmt(TableName tableName, TableName srcTableName,
      boolean isExternal, String location, boolean ifNotExists) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(srcTableName);
    this.tableName = tableName;
    this.srcTableName = srcTableName;
    this.isExternal = isExternal;
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

  public String getLocation() {
    return location;
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
    sb.append(tableName.getTbl() + " LIKE ");
    if (srcTableName.getDb() != null) {
      sb.append(srcTableName.getDb() + ".");
    }
    sb.append(srcTableName.getTbl());

    if (location != null) {
      sb.append(" LOCATION = '" + location + "'");
    }
    return sb.toString();
  }

  public TCreateTableLikeParams toThrift() {
    TCreateTableLikeParams params = new TCreateTableLikeParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    params.setSrc_table_name(new TTableName(getSrcDb(), getSrcTbl()));
    params.setIs_external(isExternal());
    params.setLocation(location);
    params.setIf_not_exists(getIfNotExists());
    return params;
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    dbName = tableName.isFullyQualified() ? tableName.getDb() : analyzer.getDefaultDb();
    srcDbName =
        srcTableName.isFullyQualified() ? srcTableName.getDb() : analyzer.getDefaultDb();

    if (analyzer.getCatalog().getDb(dbName) == null) {
      throw new AnalysisException("Database does not exist: " + dbName);
    }

    if (analyzer.getCatalog().getDb(srcDbName) == null) {
      throw new AnalysisException("Database does not exist: " + srcDbName);
    }

    if (!analyzer.getCatalog().containsTable(srcDbName, getSrcTbl())) {
      throw new AnalysisException(String.format("Source table does not exist: %s.%s",
          srcDbName, getSrcTbl())); 
    }

    if (analyzer.getCatalog().containsTable(dbName, getTbl()) && !ifNotExists) {
      throw new AnalysisException(String.format("Table already exists: %s.%s",
          dbName, getTbl())); 
    }
  }
}
