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
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableAddColsParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.util.KuduUtil;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Represents
 * - ALTER TABLE ADD [IF NOT EXISTS] COLUMNS (colDef1, colDef2, ...)
 * - ALTER TABLE ADD COLUMN [IF NOT EXISTS] colDef
 * statements.
 */
public class AlterTableAddColsStmt extends AlterTableStmt {
  private final boolean ifNotExists_;
  private final List<ColumnDef> columnDefs_;

  public AlterTableAddColsStmt(TableName tableName, boolean ifNotExists,
      List<ColumnDef> columnDefs) {
    super(tableName);
    ifNotExists_ = ifNotExists;
    Preconditions.checkState(columnDefs != null && columnDefs.size() > 0);
    columnDefs_ = Lists.newArrayList(columnDefs);
  }

  @Override
  public String getOperation() { return "ADD COLUMNS"; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable t = getTargetTable();
    // TODO: Support column-level DDL on HBase tables. Requires updating the column
    // mappings along with the table columns.
    if (t instanceof FeHBaseTable) {
      throw new AnalysisException("ALTER TABLE ADD COLUMNS not currently " +
          "supported on HBase tables.");
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
    Iterator<ColumnDef> iterator = columnDefs_.iterator();
    while (iterator.hasNext()){
      ColumnDef c = iterator.next();
      c.analyze(analyzer);
      String colName = c.getColName().toLowerCase();
      if (existingPartitionKeys.contains(colName)) {
        throw new AnalysisException(
            "Column name conflicts with existing partition column: " + colName);
      }

      Column col = t.getColumn(colName);
      if (col != null) {
        if (!ifNotExists_) {
          throw new AnalysisException("Column already exists: " + colName);
        }
        // remove the existing column
        iterator.remove();
        continue;
      }
      if (!colNames.add(colName)) {
        throw new AnalysisException("Duplicate column name: " + colName);
      }

      if (t instanceof FeKuduTable) {
        if (c.getType().isComplexType()) {
          throw new AnalysisException("Kudu tables do not support complex types: " +
              c.toString());
        }
        if (c.isPrimaryKey()) {
          throw new AnalysisException("Cannot add a " +
              KuduUtil.getPrimaryKeyString(c.isPrimaryKeyUnique()) +
              " using an ALTER TABLE ADD COLUMNS statement: " + c.toString());
        }
        if (c.isExplicitNotNullable() && !c.hasDefaultValue()) {
          throw new AnalysisException("A new non-null column must have a default " +
              "value: " + c.toString());
        }
      } else if (c.hasKuduOptions()) {
        throw new AnalysisException("The specified column options are only supported " +
            "in Kudu tables: " + c.toString());
      } else if (t instanceof FeDataSourceTable) {
        if (!DataSourceTable.isSupportedColumnType(c.getType())) {
          throw new AnalysisException("Tables stored by JDBC do not support the " +
              "column type: " + c.getType());
        }
      }
    }
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.ADD_COLUMNS);
    TAlterTableAddColsParams colParams = new TAlterTableAddColsParams();
    for (ColumnDef col: columnDefs_) {
      colParams.addToColumns(col.toThrift());
    }
    colParams.setIf_not_exists(ifNotExists_);
    params.setAdd_cols_params(colParams);
    return params;
  }
}
