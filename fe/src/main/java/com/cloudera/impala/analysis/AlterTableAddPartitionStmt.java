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
import java.util.Set;
import java.util.ArrayList;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableAddPartitionParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.cloudera.impala.thrift.TPartitionKeyValue;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Represents an ALTER TABLE ADD PARTITION statement.
 */
public class AlterTableAddPartitionStmt extends AlterTablePartitionSpecStmt {
  private final String location;
  private final boolean ifNotExists;

  public AlterTableAddPartitionStmt(TableName tableName,
      List<PartitionKeyValue> partitionSpec, String location, boolean ifNotExists) {
    super(tableName, partitionSpec); 
    Preconditions.checkState(partitionSpec != null && partitionSpec.size() > 0);
    this.location = location;
    this.ifNotExists = ifNotExists;
  }

  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public String getLocation() {
    return location;
  }

  public String debugString() {
    return toSql();
  }

  public String toSql() {
    StringBuilder sb = new StringBuilder("ALTER TABLE " + getTbl());
    sb.append(" ADD ");
    if (ifNotExists) {
      sb.append("IF NOT EXISTS ");
    }
    List<String> partitionSpecStr = Lists.newArrayList();
    for (PartitionKeyValue kv: partitionSpec) {
      partitionSpecStr.add(kv.getColName() + "=" + kv.getValue());
    }
    sb.append(String.format(" PARTITION (%s) ",
          Joiner.on(", ").join(partitionSpecStr)));
    if (location != null) { 
      sb.append(String.format("LOCATION '%s'", location));
    }
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.ADD_PARTITION);
    TAlterTableAddPartitionParams addPartParams = new TAlterTableAddPartitionParams();
    for (PartitionKeyValue kv: partitionSpec) {
      String value =
          PartitionKeyValue.getPartitionKeyValueString(kv, getNullPartitionKeyValue());
      addPartParams.addToPartition_spec(new TPartitionKeyValue(kv.getColName(), value));
    }
    addPartParams.setLocation(getLocation());
    addPartParams.setIf_not_exists(ifNotExists);
    params.setAdd_partition_params(addPartParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Table targetTable = getTargetTable();
    Preconditions.checkState(targetTable != null && targetTable instanceof HdfsTable);
    HdfsTable hdfsTable = (HdfsTable) targetTable;
    if (!ifNotExists && hdfsTable.getPartition(partitionSpec) != null) {
      throw new AnalysisException("Partition spec already exists: (" + 
          Joiner.on(", ").join(partitionSpec) + ").");
    }
  }
}
