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

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TDropTableParams;
import com.cloudera.impala.thrift.TTableName;

import com.google.common.base.Preconditions;

/**
 * Represents a DROP [IF EXISTS] TABLE statement
 */
public class DropTableStmt extends ParseNodeBase {
  private final TableName tableName;
  private final boolean ifExists;

  // Set during analysis
  private String dbName;

  /**
   * Constructor for building the DROP TABLE statement
   */
  public DropTableStmt(TableName tableName, boolean ifExists) {
    this.tableName = tableName;
    this.ifExists = ifExists;
  }

  public String getTbl() {
    return tableName.getTbl();
  }

  /**
   * Can only be called after analysis. Returns the name of the database that
   * the target drop table resides in.
   */
  public String getDb() {
    Preconditions.checkNotNull(dbName);
    return dbName;
  }

  public boolean getIfExists() {
    return this.ifExists;
  }

  public String debugString() {
    return toSql();
  }

  public String toSql() {
    StringBuilder sb = new StringBuilder("DROP TABLE ");
    if (ifExists) {
      sb.append("IF EXISTS ");
    }

    if (tableName.getDb() != null) {
      sb.append(tableName.getDb() + ".");
    }
    sb.append(tableName.getTbl());
    return sb.toString();
  }

  public TDropTableParams toThrift() {
    TDropTableParams params = new TDropTableParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    params.setIf_exists(getIfExists());
    return params;
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    dbName = tableName.isFullyQualified() ? tableName.getDb() : analyzer.getDefaultDb();
    
    // Only run this analysis if the user did not specify the IF EXISTS clause
    if (!ifExists && analyzer.getCatalog().getDb(dbName) == null) {
      throw new AnalysisException("Unknown database: " + dbName);
    }

    if (!ifExists && !analyzer.getCatalog().containsTable(dbName, getTbl())) {
      throw new AnalysisException(String.format("Unknown table: %s.%s",
          dbName, getTbl()));
    }
  }
}
