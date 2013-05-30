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
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableDropPartitionParams;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE DROP PARTITION statement.
 */
public class AlterTableDropPartitionStmt extends AlterTableStmt {
  private final boolean ifExists;
  private final PartitionSpec partitionSpec;

  public AlterTableDropPartitionStmt(TableName tableName,
      PartitionSpec partitionSpec, boolean ifExists) {
    super(tableName);
    Preconditions.checkNotNull(partitionSpec);
    this.partitionSpec = partitionSpec;
    this.partitionSpec.setTableName(tableName);
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
    sb.append(" DROP " + partitionSpec.toSql());
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.DROP_PARTITION);
    TAlterTableDropPartitionParams addPartParams = new TAlterTableDropPartitionParams();
    addPartParams.setPartition_spec(partitionSpec.toThrift());
    addPartParams.setIf_exists(ifExists);
    params.setDrop_partition_params(addPartParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);
    if (!ifExists) {
      partitionSpec.setPartitionShouldExist();
    }
    partitionSpec.setPrivilegeRequirement(Privilege.ALTER);
    partitionSpec.analyze(analyzer);
  }
}
