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
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableDropColParams;
import com.cloudera.impala.thrift.TAlterTableType;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE DROP COLUMN statement.
 * Note: Hive does not support this syntax for droppping columns, but it is supported 
 * by mysql. 
 */
public class AlterTableDropColStmt extends AlterTableStmt {
  private final String colName;

  public AlterTableDropColStmt(TableName tableName, String colName) {
    super(tableName); 
    Preconditions.checkState(colName != null && !colName.isEmpty());
    this.colName = colName;
  }

  public String getColName() {
    return colName;
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.DROP_COLUMN);
    TAlterTableDropColParams dropColParams = new TAlterTableDropColParams(colName);
    params.setDrop_col_params(dropColParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Table t = getTargetTable();
    String tableName = getDb() + "." + getTbl();

    for (FieldSchema fs: t.getMetaStoreTable().getPartitionKeys()) {
      if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
        throw new AnalysisException("Cannot drop partition column: " + fs.getName());
      } 
    }
   
    if (t.getColumns().size() - t.getMetaStoreTable().getPartitionKeysSize() <= 1) {
      throw new AnalysisException(String.format(
          "Cannot drop column '%s' from %s. Tables must contain at least 1 column.",
          colName, tableName));
    }

    if (t.getColumn(colName) == null) {
      throw new AnalysisException(String.format(
          "Column '%s' does not exist in table: %s", colName, tableName));
    }
  }
}
