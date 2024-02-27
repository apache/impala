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
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableReplaceColsParams;
import org.apache.impala.thrift.TAlterTableType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents an ALTER TABLE REPLACE COLUMNS (colDef1, colDef2, ...) statement.
 */
public class AlterTableReplaceColsStmt extends AlterTableStmt {
  private final List<ColumnDef> columnDefs_;

  public AlterTableReplaceColsStmt(TableName tableName, List<ColumnDef> columnDefs) {
    super(tableName);
    Preconditions.checkState(columnDefs != null && columnDefs.size() > 0);
    columnDefs_ = Lists.newArrayList(columnDefs);
  }

  @Override
  public String getOperation() { return "REPLACE COLUMNS"; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable t = getTargetTable();
    // TODO: Support column-level DDL on HBase tables. Requires updating the column
    // mappings along with the table columns.
    if (t instanceof FeHBaseTable) {
      throw new AnalysisException("ALTER TABLE REPLACE COLUMNS not currently " +
          "supported on HBase tables.");
    }

    boolean isKuduTable = t instanceof FeKuduTable;
    if (isKuduTable) {
      throw new AnalysisException("ALTER TABLE REPLACE COLUMNS is not " +
          "supported on Kudu tables.");
    }

    if (t instanceof FeIcebergTable) {
      throw new AnalysisException("ALTER TABLE REPLACE COLUMNS is not " +
          "supported on Iceberg tables.");
    }

    // Build a set of the partition keys for the table.
    Set<String> existingPartitionKeys = new HashSet<>();
    for (FieldSchema fs: t.getMetaStoreTable().getPartitionKeys()) {
      existingPartitionKeys.add(fs.getName().toLowerCase());
    }

    // Make sure the new columns don't already exist in the table, that the names
    // are all valid and unique, and that none of the columns conflict with
    // partition columns.
    Set<String> colNames = new HashSet<>();
    for (ColumnDef c: columnDefs_) {
      c.analyze(analyzer);
      String colName = c.getColName().toLowerCase();
      if (existingPartitionKeys.contains(colName)) {
        throw new AnalysisException(
            "Column name conflicts with existing partition column: " + colName);
      }

      if (!colNames.add(colName)) {
        throw new AnalysisException("Duplicate column name: " + colName);
      }
    }
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.REPLACE_COLUMNS);
    TAlterTableReplaceColsParams colParams = new TAlterTableReplaceColsParams();
    for (ColumnDef col: columnDefs_) {
      colParams.addToColumns(col.toThrift());
    }
    params.setReplace_cols_params(colParams);
    return params;
  }
}
