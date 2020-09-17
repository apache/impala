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
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TDescribeHistoryParams;

import com.google.common.base.Preconditions;

/**
 * Representation of a DESCRIBE HISTORY statement.
 */
public class DescribeHistoryStmt extends StatementBase {
  protected final TableName tableName_;

  // Set during analysis.
  protected FeTable table_;

  public DescribeHistoryStmt(TableName tableName) {
    tableName_ = Preconditions.checkNotNull(tableName);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return "DESCRIBE HISTORY " + tableName_.toString();
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    table_ = analyzer.getTable(tableName_, Privilege.VIEW_METADATA);
    Preconditions.checkNotNull(table_);
    if (!(table_ instanceof FeIcebergTable)) {
      throw new AnalysisException(String.format(
          "DESCRIBE HISTORY must specify an Iceberg table: %s", table_.getFullName()));
    }
  }

  public TDescribeHistoryParams toThrift() {
    return new TDescribeHistoryParams(new TableName(table_.getDb().getName(),
        table_.getName()).toThrift());
  }
}
