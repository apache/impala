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
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TUseDbParams;

/**
 * Representation of a USE db statement.
 */
public class UseStmt extends StatementBase {
  private final String database;

  public UseStmt(String db) {
    database = db;
  }

  public String getDatabase() {
    return database;
  }

  @Override
  public String toSql() {
    return "USE " + database;
  }

  @Override
  public String debugString() {
    return toSql();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (analyzer.getCatalog().getDb(
        database, analyzer.getUser(), Privilege.ANY) == null) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + getDatabase());
    }
  }

  public TUseDbParams toThrift() {
    TUseDbParams params = new TUseDbParams();
    params.setDb(getDatabase());
    return params;
  }
}
