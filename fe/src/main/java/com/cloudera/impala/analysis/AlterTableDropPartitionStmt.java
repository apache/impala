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

import java.util.List;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableDropPartitionParams;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents an ALTER TABLE DROP PARTITION statement.
 */
public class AlterTableDropPartitionStmt extends AlterTablePartitionSpecStmt {
  private final boolean ifExists;

  public AlterTableDropPartitionStmt(TableName tableName,
      List<PartitionKeyValue> partitionSpec, boolean ifExists) {
    super(tableName, partitionSpec);
    this.ifExists = ifExists;
  }

  public boolean getIfNotExists() {
    return ifExists;
  }

  @Override
  public String debugString() {
    return toSql();
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("ALTER TABLE " + getTbl());
    sb.append(" DROP ");
    if (ifExists) {
      sb.append("IF EXISTS ");
    }
    List<String> partitionSpecStr = Lists.newArrayList();
    for (PartitionKeyValue kv: partitionSpec) {
      partitionSpecStr.add(kv.getColName() + "="  + kv.getValue());
    }
    sb.append(String.format(" DROP PARTITION (%s) ",
          Joiner.on(", ").join(partitionSpecStr)));
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.DROP_PARTITION);
    TAlterTableDropPartitionParams addPartParams = new TAlterTableDropPartitionParams();
    for (PartitionKeyValue kv: partitionSpec) {
      String value = kv.getPartitionKeyValueString(getNullPartitionKeyValue());
      addPartParams.addToPartition_spec(new TPartitionKeyValue(kv.getColName(), value));
    }
    addPartParams.setIf_exists(ifExists);
    params.setDrop_partition_params(addPartParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);
    Table targetTable = getTargetTable();
    Preconditions.checkState(targetTable != null && targetTable instanceof HdfsTable);
    HdfsTable hdfsTable = (HdfsTable) targetTable;
    if (!ifExists && hdfsTable.getPartition(partitionSpec) == null) {
      throw new AnalysisException("Partition spec does not exist: (" +
          Joiner.on(", ").join(partitionSpec) + ").");
    }
  }
}
