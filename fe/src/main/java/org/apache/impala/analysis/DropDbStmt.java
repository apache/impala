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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TDropDbParams;

/**
 * Represents a DROP [IF EXISTS] DATABASE [CASCADE | RESTRICT] statement
 */
public class DropDbStmt extends StatementBase {
  private final String dbName_;
  private final boolean ifExists_;
  private final boolean cascade_;

  // Server name needed for privileges. Set during analysis.
  private String serverName_;

  /**
   * Constructor for building the drop statement. If ifExists is true, an error will not
   * be thrown if the database does not exist. If cascade is true, all the tables in the
   * database will be dropped.
   */
  public DropDbStmt(String dbName, boolean ifExists, boolean cascade) {
    this.dbName_ = dbName;
    this.ifExists_ = ifExists;
    this.cascade_ = cascade;
  }

  public String getDb() { return dbName_; }
  public boolean getIfExists() { return ifExists_; }
  public boolean getCascade() { return cascade_; }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("DROP DATABASE");
    if (ifExists_) sb.append(" IF EXISTS ");
    sb.append(getDb());
    if (cascade_) sb.append(" CASCADE");
    return sb.toString();
  }

  public TDropDbParams toThrift() {
    TDropDbParams params = new TDropDbParams();
    params.setDb(getDb());
    params.setIf_exists(getIfExists());
    params.setCascade(getCascade());
    params.setServer_name(serverName_);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    FeDb db = analyzer.getDb(dbName_, Privilege.DROP, false);
    if (db == null && !ifExists_) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + dbName_);
    }

    if (analyzer.getDefaultDb().toLowerCase().equals(dbName_.toLowerCase())) {
      throw new AnalysisException("Cannot drop current default database: " + dbName_);
    }
    if (db != null && db.numFunctions() > 0 && !cascade_) {
      throw new AnalysisException("Cannot drop non-empty database: " + dbName_);
    }
    // Set the servername here if authorization is enabled because analyzer_ is not
    // available in the toThrift() method.
    serverName_ = analyzer.getServerName();
  }
}
