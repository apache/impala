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

import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TShowTablesParams;

import com.google.common.collect.Sets;

public class ShowViewsStmt extends ShowTablesOrViewsStmt {
  public ShowViewsStmt() { super(null, null); }

  public ShowViewsStmt(String pattern) { super(null, pattern); }

  public ShowViewsStmt(String database, String pattern) { super(database, pattern); }

  @Override
  public String toSql(ToSqlOptions options) {
    if (getPattern() == null) {
      if (getParsedDb() == null) {
        return "SHOW VIEWS";
      } else {
        return "SHOW VIEWS IN " + getParsedDb();
      }
    } else {
      if (getParsedDb() == null) {
        return "SHOW VIEWS LIKE '" + getPattern() + "'";
      } else {
        return "SHOW VIEWS IN " + getParsedDb() + " LIKE '" + getPattern() + "'";
      }
    }
  }

  @Override
  public TShowTablesParams toThrift() {
    TShowTablesParams params = super.toThrift();
    params.setTable_types(Sets.newHashSet(TImpalaTableType.VIEW));
    return params;
  }
}
