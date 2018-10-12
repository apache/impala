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
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TDescribeDbParams;
import org.apache.impala.thrift.TDescribeOutputStyle;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Represents a DESCRIBE DATABASE statement which returns metadata on
 * a specified database:
 * Syntax: DESCRIBE DATABASE [FORMATTED|EXTENDED] <db>
 *
 * If FORMATTED|EXTENDED is not specified, the statement only returns the given
 * database's location and comment.
 * If FORMATTED|EXTENDED is specified, extended metadata on the database is returned.
 * This metadata includes info about the database's parameters, owner info
 * and privileges.
 */
public class DescribeDbStmt extends StatementBase {
  private final TDescribeOutputStyle outputStyle_;
  private final String dbName_;

  public DescribeDbStmt(String dbName, TDescribeOutputStyle outputStyle) {
    Preconditions.checkState(!Strings.isNullOrEmpty(dbName), "Invalid database name");
    dbName_ = dbName;
    outputStyle_ = outputStyle;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("DESCRIBE DATABASE ");
    if (outputStyle_ != TDescribeOutputStyle.MINIMAL) {
      sb.append(outputStyle_.toString() + " ");
    }
    return sb.toString() + dbName_;
  }

  public String getDb() { return dbName_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    analyzer.getDb(dbName_, Privilege.VIEW_METADATA);
  }

  public TDescribeDbParams toThrift() {
    TDescribeDbParams params = new TDescribeDbParams();
    params.setDb(dbName_);
    params.setOutput_style(outputStyle_);
    return params;
  }
}
