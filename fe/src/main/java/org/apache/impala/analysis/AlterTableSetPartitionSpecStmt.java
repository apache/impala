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

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableSetPartitionSpecParams;
import org.apache.impala.thrift.TAlterTableType;

import com.google.common.base.Preconditions;

/**
* Represents an ALTER TABLE tbl SET PARTITION SPEC (part_spec) statement
* (applicable to Iceberg tables only),
*/
public class AlterTableSetPartitionSpecStmt extends AlterTableStmt {
  private final IcebergPartitionSpec icebergPartSpec_;

  public AlterTableSetPartitionSpecStmt(TableName tableName,
      IcebergPartitionSpec icebergPartSpec) {
    super(tableName);
    // Partition spec cannot be empty
    Preconditions.checkNotNull(icebergPartSpec);
    Preconditions.checkState(icebergPartSpec.hasPartitionFields());
    icebergPartSpec_ = icebergPartSpec;
  }

  @Override
  public String getOperation() { return "SET PARTITION SPEC"; }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("ALTER TABLE ");
    if (getDb() != null) sb.append(getDb() + ".");
    sb.append(getTbl()).append(" SET PARTITION SPEC ")
        .append(icebergPartSpec_.toSql(options));
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableSetPartitionSpecParams setPartSpecParams =
        new TAlterTableSetPartitionSpecParams();
    setPartSpecParams.setPartition_spec(icebergPartSpec_.toThrift());

    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.SET_PARTITION_SPEC);
    params.setSet_partition_spec_params(setPartSpecParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable table = getTargetTable();
    if (!(table instanceof FeIcebergTable)) {
      throw new AnalysisException("ALTER TABLE SET PARTITION SPEC is only supported " +
          "for Iceberg tables: " + table.getTableName());
    }

    icebergPartSpec_.analyze(analyzer);

    // Check partition field names and source column types.
    for (IcebergPartitionField partField : icebergPartSpec_.getIcebergPartitionFields()) {
      Column col = table.getColumn(partField.getFieldName());
      if (col == null) {
        throw new AnalysisException(String.format(
            "Source column '%s' does not exist in table: %s", partField.getFieldName(),
            table.getTableName()));
      }
      if (col.getType().isComplexType()) {
        throw new AnalysisException(String.format(
            "Source column '%s' in table %s must be a primitive type",
            partField.getFieldName(), table.getTableName()));
      }
    }
  }
}
