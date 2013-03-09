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
import com.cloudera.impala.thrift.TDropDbParams;

import com.google.common.base.Preconditions;

/**
 * Represents a DROP [IF EXISTS] DATABASE statement
 */
public class DropDbStmt extends ParseNodeBase {
  private final String dbName;
  private final boolean ifExists;

  /**
   * Constructor for building the drop statement. If ifExists is true, an error will not
   * be thrown if the database does not exist.
   */
  public DropDbStmt(String dbName, boolean ifExists) {
    this.dbName = dbName;
    this.ifExists = ifExists;
  }

  public String getDb() {
    return dbName;
  }

  public boolean getIfExists() {
    return ifExists;
  }

  public String debugString() {
    return toSql();
  }

  public String toSql() {
    StringBuilder sb = new StringBuilder("DROP DATABASE");
    if (ifExists) {
      sb.append(" IF EXISTS ");
    }
    sb.append(getDb());
    return sb.toString();
  }

  public TDropDbParams toThrift() {
    TDropDbParams params = new TDropDbParams();
    params.setDb(getDb());
    params.setIf_exists(getIfExists());
    return params;
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (!ifExists && analyzer.getCatalog().getDb(dbName) == null) {
      throw new AnalysisException("Unknown database: " + dbName);
    }

    if (analyzer.getDefaultDb().equals(getDb())) {
      throw new AnalysisException("Cannot drop current default database: " + getDb());
    }
  }
}
