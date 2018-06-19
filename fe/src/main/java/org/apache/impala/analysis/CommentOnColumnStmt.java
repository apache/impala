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

import com.google.common.base.Preconditions;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TColumnName;
import org.apache.impala.thrift.TCommentOnParams;

import java.util.List;

/**
 * Represents a COMMENT ON COLUMN table_or_view.col IS 'comment' statement.
 */
public class CommentOnColumnStmt extends CommentOnStmt {
  private TableName tableName_;
  private final String columnName_;

  public CommentOnColumnStmt(ColumnName columnName, String comment) {
    super(comment);
    Preconditions.checkNotNull(columnName);
    this.tableName_ = columnName.getTableName();
    this.columnName_ = columnName.getColumnName();
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    tableName_ = analyzer.getFqTableName(tableName_);
    // Although it makes sense to use column-level privilege, column-level privilege is
    // only supported on tables and not views.
    TableRef tableRef = new TableRef(tableName_.toPath(), null, Privilege.ALTER);
    tableRef = analyzer.resolveTableRef(tableRef);
    Preconditions.checkNotNull(tableRef);
    tableRef.analyze(analyzer);
    FeTable feTable;
    String tableRefType = "";
    if (tableRef instanceof InlineViewRef) {
      InlineViewRef inlineViewRef = (InlineViewRef) tableRef;
      feTable = inlineViewRef.getView();
      tableRefType = "view";
    } else {
      feTable = tableRef.getTable();
      tableRefType = "table";
    }
    Column column = feTable.getColumn(columnName_);
    if (column == null) {
      throw new AnalysisException(String.format(
          "Column '%s' does not exist in %s: %s", columnName_, tableRefType, tableName_));
    }
  }

  @Override
  public TCommentOnParams toThrift() {
    TCommentOnParams params = super.toThrift();
    params.setColumn_name(new TColumnName(tableName_.toThrift(), columnName_));
    return params;
  }
}
