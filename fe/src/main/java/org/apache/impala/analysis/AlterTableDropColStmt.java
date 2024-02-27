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
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableDropColParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE DROP COLUMN statement.
 * Note: Hive does not support this syntax for droppping columns, but it is supported
 * by mysql.
 */
public class AlterTableDropColStmt extends AlterTableStmt {
  private final String colName_;

  public AlterTableDropColStmt(TableName tableName, String colName) {
    super(tableName);
    Preconditions.checkState(colName != null && !colName.isEmpty());
    colName_ = colName;
  }

  public String getColName() { return colName_; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.DROP_COLUMN);
    TAlterTableDropColParams dropColParams = new TAlterTableDropColParams(colName_);
    params.setDrop_col_params(dropColParams);
    return params;
  }

  @Override
  public String getOperation() { return "DROP COLUMN"; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable t = getTargetTable();
    // TODO: Support column-level DDL on HBase tables. Requires updating the column
    // mappings along with the table columns.
    if (t instanceof FeHBaseTable) {
      throw new AnalysisException("ALTER TABLE DROP COLUMN not currently supported " +
          "on HBase tables.");
    }
    String tableName = getDb() + "." + getTbl();

    for (FieldSchema fs: t.getMetaStoreTable().getPartitionKeys()) {
      if (fs.getName().toLowerCase().equals(colName_.toLowerCase())) {
        throw new AnalysisException("Cannot drop partition column: " + fs.getName());
      }
    }

    if (t.getColumns().size() - t.getMetaStoreTable().getPartitionKeysSize() <= 1) {
      throw new AnalysisException(String.format(
          "Cannot drop column '%s' from %s. Tables must contain at least 1 column.",
          colName_, tableName));
    }

    if (t.getColumn(colName_) == null) {
      throw new AnalysisException(String.format(
          "Column '%s' does not exist in table: %s", colName_, tableName));
    }
  }
}
