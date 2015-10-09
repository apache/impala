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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableDropPartitionParams;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE DROP PARTITION statement.
 */
public class AlterTableDropPartitionStmt extends AlterTableStmt {
  private final boolean ifExists_;
  private final PartitionSpec partitionSpec_;

  // Setting this value causes dropped partition(s) to be permanently
  // deleted. For example, for HDFS tables it skips the trash mechanism
  private final boolean purgePartition_;

  public AlterTableDropPartitionStmt(TableName tableName,
      PartitionSpec partitionSpec, boolean ifExists, boolean purgePartition) {
    super(tableName);
    Preconditions.checkNotNull(partitionSpec);
    partitionSpec_ = partitionSpec;
    partitionSpec_.setTableName(tableName);
    ifExists_ = ifExists;
    purgePartition_ = purgePartition;
  }

  public boolean getIfNotExists() { return ifExists_; }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("ALTER TABLE " + getTbl());
    sb.append(" DROP ");
    if (ifExists_) sb.append("IF EXISTS ");
    sb.append(" DROP " + partitionSpec_.toSql());
    if (purgePartition_) sb.append(" PURGE");
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.DROP_PARTITION);
    TAlterTableDropPartitionParams addPartParams = new TAlterTableDropPartitionParams();
    addPartParams.setPartition_spec(partitionSpec_.toThrift());
    addPartParams.setIf_exists(ifExists_);
    addPartParams.setPurge(purgePartition_);
    params.setDrop_partition_params(addPartParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (!ifExists_) partitionSpec_.setPartitionShouldExist();
    partitionSpec_.setPrivilegeRequirement(Privilege.ALTER);
    partitionSpec_.analyze(analyzer);
  }
}
