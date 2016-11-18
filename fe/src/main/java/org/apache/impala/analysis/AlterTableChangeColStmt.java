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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableChangeColParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Represents an ALTER TABLE CHANGE COLUMN colName newColDef statement.
 * Note: It would be fairly simple to reuse this class to support ALTER TABLE MODIFY
 * newColDef statements in the future my making colName optional.
 */
public class AlterTableChangeColStmt extends AlterTableStmt {
  private final String colName_;
  private final ColumnDef newColDef_;

  public AlterTableChangeColStmt(TableName tableName, String colName,
      ColumnDef newColDef) {
    super(tableName);
    Preconditions.checkNotNull(newColDef);
    Preconditions.checkState(!Strings.isNullOrEmpty(colName));
    colName_ = colName;
    newColDef_ = newColDef;
  }

  public String getColName() { return colName_; }
  public ColumnDef getNewColDef() { return newColDef_; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.CHANGE_COLUMN);
    TAlterTableChangeColParams colParams = new TAlterTableChangeColParams();
    colParams.setCol_name(colName_);
    colParams.setNew_col_def(newColDef_.toThrift());
    params.setChange_col_params(colParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Table t = getTargetTable();
    // TODO: Support column-level DDL on HBase tables. Requires updating the column
    // mappings along with the table columns.
    if (t instanceof HBaseTable) {
      throw new AnalysisException("ALTER TABLE CHANGE COLUMN not currently supported " +
          "on HBase tables.");
    }
    String tableName = getDb() + "." + getTbl();

    // Verify there are no conflicts with partition columns.
    for (FieldSchema fs: t.getMetaStoreTable().getPartitionKeys()) {
      if (fs.getName().toLowerCase().equals(colName_.toLowerCase())) {
        throw new AnalysisException("Cannot modify partition column: " + colName_);
      }
      if (fs.getName().toLowerCase().equals(newColDef_.getColName().toLowerCase())) {
        throw new AnalysisException(
            "Column name conflicts with existing partition column: " +
            newColDef_.getColName());
      }
    }

    // Verify the column being modified exists in the table
    if (t.getColumn(colName_) == null) {
      throw new AnalysisException(String.format(
          "Column '%s' does not exist in table: %s", colName_, tableName));
    }

    // Check that the new column def's name is valid.
    newColDef_.analyze(analyzer);
    // Verify that if the column name is being changed, the new name doesn't conflict
    // with an existing column.
    if (!colName_.toLowerCase().equals(newColDef_.getColName().toLowerCase()) &&
        t.getColumn(newColDef_.getColName()) != null) {
      throw new AnalysisException("Column already exists: " + newColDef_.getColName());
    }
    if (newColDef_.hasKuduOptions()) {
      throw new AnalysisException("Unsupported column options in ALTER TABLE CHANGE " +
          "COLUMN statement: " + newColDef_.toString());
    }
    if (t instanceof KuduTable) {
      Column col = t.getColumn(colName_);
      if (!col.getType().equals(newColDef_.getType())) {
        throw new AnalysisException(String.format("Cannot change the type of a Kudu " +
            "column using an ALTER TABLE CHANGE COLUMN statement: (%s vs %s)",
            col.getType().toSql(), newColDef_.getType().toSql()));
      }
    }
  }
}
