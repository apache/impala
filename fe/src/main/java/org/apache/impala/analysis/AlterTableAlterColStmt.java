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

import java.util.HashMap;
import java.util.Map;

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableAlterColParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Represents DDL statements that alter column properties:
 * - ALTER TABLE table CHANGE COLUMN colName newColDef
 *   which is used to update a column's name, type, and comment.
 * - ALTER TABLE table ALTER COLUMN colName SET colOptions
 *   ALTER TABLE table ALTER COLUMN colName DROP DEFAULT
 *   which is used to update column options such as the encoding type.
 */
public class AlterTableAlterColStmt extends AlterTableStmt {
  private final String colName_;
  private final ColumnDef newColDef_;
  private final boolean isDropDefault_;

  /**
   * Creates and returns a new AlterTableAlterColStmt for the operation:
   * ALTER TABLE <tableName> CHANGE [COLUMN] <colName> <newColDef>
   */
  public static AlterTableAlterColStmt createChangeColStmt(TableName tableName,
      String colName, ColumnDef newColDef) {
    return new AlterTableAlterColStmt(tableName, colName, newColDef);
  }

  /**
   * Creates and returns a new AlterTableAlterColStmt for the operation:
   * ALTER TABLE <tableName> ALTER [COLUMN] <colName> DROP DEFAULT
   * which is represented as setting the default to NULL.
   */
  public static AlterTableAlterColStmt createDropDefaultStmt(
      TableName tableName, String colName) {
    Map<ColumnDef.Option, Object> option = new HashMap<>();
    option.put(ColumnDef.Option.DEFAULT, new NullLiteral());
    return new AlterTableAlterColStmt(
        tableName, colName, new ColumnDef(colName, null, option), true);
  }

  public AlterTableAlterColStmt(TableName tableName, String colName,
      ColumnDef newColDef) {
    this(tableName, colName, newColDef, false);
  }

  public AlterTableAlterColStmt(TableName tableName, String colName,
      ColumnDef newColDef, boolean isDropDefault) {
    super(tableName);
    Preconditions.checkNotNull(newColDef);
    Preconditions.checkState(!Strings.isNullOrEmpty(colName));
    colName_ = colName;
    newColDef_ = newColDef;
    isDropDefault_ = isDropDefault;
  }

  public String getColName() { return colName_; }
  public ColumnDef getNewColDef() { return newColDef_; }

  @Override
  public String getOperation() { return "CHANGE COLUMN"; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.ALTER_COLUMN);
    TAlterTableAlterColParams colParams = new TAlterTableAlterColParams();
    colParams.setCol_name(colName_);
    colParams.setNew_col_def(newColDef_.toThrift());
    params.setAlter_col_params(colParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable t = getTargetTable();
    if (t instanceof FeHBaseTable) {
      throw new AnalysisException(
          "ALTER TABLE CHANGE/ALTER COLUMN not currently supported on HBase tables.");
    }
    String tableName = getDb() + "." + getTbl();

    Column column = t.getColumn(colName_);
    // Verify the column being modified exists in the table
    if (column == null) {
      throw new AnalysisException(String.format(
          "Column '%s' does not exist in table: %s", colName_, tableName));
    }
    // Verify the column being modified isn't a partition column.
    if (t.isClusteringColumn(column)) {
      throw new AnalysisException("Cannot modify partition column: " + colName_);
    }
    boolean alterColumnSetStmt = false;
    // If the type wasn't set, then this was a ALTER COLUMN SET/DROP stmt. We aren't
    // changing the type, so set it to the existing type.
    if (newColDef_.getTypeDef() == null) {
      newColDef_.setType(column.getType());
      alterColumnSetStmt = true;
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
      // Disallow Kudu options on non-Kudu tables.
      if (!(t instanceof FeKuduTable)) {
        if (isDropDefault_) {
          throw new AnalysisException(String.format(
              "Unsupported column option for non-Kudu table: DROP DEFAULT"));
        } else {
          throw new AnalysisException(
              String.format("Unsupported column options for non-Kudu table: '%s'",
                  newColDef_.toString()));
        }
      }
      // Disallow Kudu options on Kudu tables in a CHANGE statement. We need to keep
      // CHANGE for compatability, but we don't want to add new functionality to it.
      if (!alterColumnSetStmt) {
        throw new AnalysisException(
            String.format("Unsupported column options in ALTER TABLE CHANGE COLUMN "
                + "statement: '%s'. Use ALTER TABLE ALTER COLUMN instead.",
                newColDef_.toString()));
      }
    }
    if (t instanceof FeKuduTable) {
      KuduColumn col = (KuduColumn) t.getColumn(colName_);
      boolean isSystemGeneratedColumn = col.isAutoIncrementing();
      if (!col.getType().equals(newColDef_.getType())) {
        throw new AnalysisException(String.format("Cannot change the type of a Kudu " +
            "column using an ALTER TABLE CHANGE COLUMN statement: (%s vs %s)",
            col.getType().toSql(), newColDef_.getType().toSql()));
      }
      if (col.isKey() && newColDef_.hasDefaultValue()) {
        throw new AnalysisException(String.format(
            "Cannot %s default value for %sprimary key column '%s'",
            isDropDefault_ ? "drop" : "set",
            isSystemGeneratedColumn ? "system generated " : "", colName_));
      }
      if (newColDef_.isPrimaryKey()) {
        throw new AnalysisException(
            "Altering a column to be a primary key is not supported.");
      }
      if (newColDef_.isNullabilitySet()) {
        throw new AnalysisException(
            "Altering the nullability of a column is not supported.");
      }
    }

    if (t instanceof FeIcebergTable) {
      // We cannot update column from primitive type to complex type or
      // from complex type to primitive type
      if (t.getColumn(colName_).getType().isComplexType() ||
          newColDef_.getType().isComplexType()) {
        throw new AnalysisException(String.format("ALTER TABLE CHANGE COLUMN " +
            "is not supported for complex types in Iceberg tables."));
      }
    }

    if (t instanceof FeDataSourceTable) {
      if (!DataSourceTable.isSupportedColumnType(newColDef_.getType())) {
        throw new AnalysisException("Tables stored by JDBC do not support the " +
            "column type: " + newColDef_.getType());
      }
    }
  }
}
