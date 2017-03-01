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
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableAddReplaceColsParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Represents an ALTER TABLE ADD|REPLACE COLUMNS (colDef1, colDef2, ...) statement.
 */
public class AlterTableAddReplaceColsStmt extends AlterTableStmt {
  private final List<ColumnDef> columnDefs_;
  private final boolean replaceExistingCols_;

  public AlterTableAddReplaceColsStmt(TableName tableName, List<ColumnDef> columnDefs,
      boolean replaceExistingCols) {
    super(tableName);
    Preconditions.checkState(columnDefs != null && columnDefs.size() > 0);
    columnDefs_ = Lists.newArrayList(columnDefs);
    replaceExistingCols_ = replaceExistingCols;
  }

  public List<ColumnDef> getColumnDescs() { return columnDefs_; }

  // Replace columns instead of appending new columns.
  public boolean getReplaceExistingCols() {
    return replaceExistingCols_;
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.ADD_REPLACE_COLUMNS);
    TAlterTableAddReplaceColsParams colParams = new TAlterTableAddReplaceColsParams();
    for (ColumnDef col: getColumnDescs()) {
      colParams.addToColumns(col.toThrift());
    }
    colParams.setReplace_existing_cols(replaceExistingCols_);
    params.setAdd_replace_cols_params(colParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Table t = getTargetTable();
    // TODO: Support column-level DDL on HBase tables. Requires updating the column
    // mappings along with the table columns.
    if (t instanceof HBaseTable) {
      throw new AnalysisException("ALTER TABLE ADD|REPLACE COLUMNS not currently " +
          "supported on HBase tables.");
    }

    boolean isKuduTable = t instanceof KuduTable;
    if (isKuduTable && replaceExistingCols_) {
      throw new AnalysisException("ALTER TABLE REPLACE COLUMNS is not " +
          "supported on Kudu tables.");
    }

    // Build a set of the partition keys for the table.
    Set<String> existingPartitionKeys = Sets.newHashSet();
    for (FieldSchema fs: t.getMetaStoreTable().getPartitionKeys()) {
      existingPartitionKeys.add(fs.getName().toLowerCase());
    }

    // Make sure the new columns don't already exist in the table, that the names
    // are all valid and unique, and that none of the columns conflict with
    // partition columns.
    Set<String> colNames = Sets.newHashSet();
    for (ColumnDef c: columnDefs_) {
      c.analyze(analyzer);
      String colName = c.getColName().toLowerCase();
      if (existingPartitionKeys.contains(colName)) {
        throw new AnalysisException(
            "Column name conflicts with existing partition column: " + colName);
      }

      Column col = t.getColumn(colName);
      if (col != null && !replaceExistingCols_) {
        throw new AnalysisException("Column already exists: " + colName);
      } else if (!colNames.add(colName)) {
        throw new AnalysisException("Duplicate column name: " + colName);
      }

      if (isKuduTable) {
        if (c.getType().isComplexType()) {
          throw new AnalysisException("Kudu tables do not support complex types: " +
              c.toString());
        }
        if (c.isPrimaryKey()) {
          throw new AnalysisException("Cannot add a primary key using an ALTER TABLE " +
              "ADD COLUMNS statement: " + c.toString());
        }
        if (c.isExplicitNotNullable() && !c.hasDefaultValue()) {
          throw new AnalysisException("A new non-null column must have a default " +
              "value: " + c.toString());
        }
      } else if (c.hasKuduOptions()) {
        throw new AnalysisException("The specified column options are only supported " +
            "in Kudu tables: " + c.toString());
      }
    }
  }
}
