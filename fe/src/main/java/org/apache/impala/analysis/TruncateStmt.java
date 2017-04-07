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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TTruncateParams;

import com.google.common.base.Preconditions;

/**
 * Representation of a TRUNCATE statement.
 * Acceptable syntax:
 *
 * TRUNCATE [TABLE] [IF EXISTS] [database.]table
 *
 */
public class TruncateStmt extends StatementBase {
  private TableName tableName_;
  private final boolean ifExists_;

  // Set in analyze().
  private Table table_;

  public TruncateStmt(TableName tableName, boolean ifExists) {
    Preconditions.checkNotNull(tableName);
    tableName_ = tableName;
    table_ = null;
    ifExists_ = ifExists;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    tableName_ = analyzer.getFqTableName(tableName_);
    try {
      table_ = analyzer.getTable(tableName_, Privilege.INSERT);
    } catch (AnalysisException e) {
      if (ifExists_) return;
      throw e;
    }
    // We only support truncating hdfs tables now.
    if (!(table_ instanceof HdfsTable)) {
      throw new AnalysisException(String.format(
          "TRUNCATE TABLE not supported on non-HDFS table: %s", table_.getFullName()));
    }
  }

  @Override
  public String toSql() {
    return "TRUNCATE TABLE " + (ifExists_ ? " IF EXISTS " : "") + tableName_;
  }

  public TTruncateParams toThrift() {
    TTruncateParams params = new TTruncateParams();
    params.setTable_name(tableName_.toThrift());
    params.setIf_exists(ifExists_);
    return params;
  }
}
