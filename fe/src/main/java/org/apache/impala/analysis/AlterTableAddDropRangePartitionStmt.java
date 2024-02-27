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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableAddDropRangePartitionParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TRangePartitionOperationType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents an ALTER TABLE ADD/DROP RANGE PARTITION statement.
 */
public class AlterTableAddDropRangePartitionStmt extends AlterTableStmt {
  private final boolean ignoreErrors_;
  private final RangePartition rangePartitionSpec_;

  public enum Operation {
    ADD("IF NOT EXISTS", TRangePartitionOperationType.ADD),
    DROP("IF EXISTS", TRangePartitionOperationType.DROP);

    private final String option_;
    private final TRangePartitionOperationType type_;
    Operation(String option, TRangePartitionOperationType type) {
      option_ = option;
      type_ = type;
    }
    String option() { return option_; }
    TRangePartitionOperationType type() { return type_; }
  }

  private final Operation operation_;

  public AlterTableAddDropRangePartitionStmt(TableName tableName,
      RangePartition rangePartitionSpec, boolean ignoreErrors, Operation op) {
    super(tableName);
    Preconditions.checkNotNull(rangePartitionSpec);
    rangePartitionSpec_ = rangePartitionSpec;
    ignoreErrors_ = ignoreErrors;
    operation_ = op;
  }

  @Override
  public String getOperation() { return operation_.name().toUpperCase(); }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("ALTER TABLE " + getTbl());
    sb.append(" " + operation_.name());
    if (ignoreErrors_) sb.append(" " + operation_.option());
    sb.append(" " + rangePartitionSpec_.toSql(options));
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.ADD_DROP_RANGE_PARTITION);
    TAlterTableAddDropRangePartitionParams partParams =
        new TAlterTableAddDropRangePartitionParams();
    partParams.setRange_partition_spec(rangePartitionSpec_.toThrift());
    partParams.setIgnore_errors(ignoreErrors_);
    partParams.setType(operation_.type());
    params.setAdd_drop_range_partition_params(partParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable table = getTargetTable();
    if (!(table instanceof FeKuduTable)) {
      throw new AnalysisException(String.format("Table %s does not support range " +
          "partitions: RANGE %s", table.getFullName(), rangePartitionSpec_.toSql()));
    }
    FeKuduTable kuduTable = (FeKuduTable) table;
    List<String> colNames = FeKuduTable.Utils.getRangePartitioningColNames(kuduTable);
    if (colNames.isEmpty()) {
      throw new AnalysisException(String.format("Cannot add/drop partition %s: " +
          "Kudu table %s doesn't have a range-based partitioning.",
          rangePartitionSpec_.toSql(), kuduTable.getName()));
    }
    List<ColumnDef> rangeColDefs = Lists.newArrayListWithCapacity(colNames.size());
    for (String colName: colNames) {
      Column col = kuduTable.getColumn(colName);
      ColumnDef colDef = new ColumnDef(col.getName(), new TypeDef(col.getType()));
      colDef.analyze(analyzer);
      rangeColDefs.add(colDef);
    }

    List<ColumnDef> pkColumnDefs = new ArrayList<>();
    for (String colName: kuduTable.getPrimaryKeyColumnNames()) {
      Column col = kuduTable.getColumn(colName);
      ColumnDef colDef = new ColumnDef(col.getName(), new TypeDef(col.getType()));
      colDef.analyze(analyzer);
      pkColumnDefs.add(colDef);
    }
    rangePartitionSpec_.setPkColumnDefMap(ColumnDef.mapByColumnNames(pkColumnDefs));
    rangePartitionSpec_.analyze(analyzer, rangeColDefs);
  }
}
