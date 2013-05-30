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
import com.cloudera.impala.thrift.TAlterTableAddPartitionParams;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE ADD PARTITION statement.
 */
public class AlterTableAddPartitionStmt extends AlterTableStmt {
  private final String location;
  private final boolean ifNotExists;
  private final PartitionSpec partitionSpec;

  public AlterTableAddPartitionStmt(TableName tableName,
      PartitionSpec partitionSpec, String location, boolean ifNotExists) {
    super(tableName);
    Preconditions.checkState(partitionSpec != null);
    this.location = location;
    this.ifNotExists = ifNotExists;
    this.partitionSpec = partitionSpec;
    this.partitionSpec.setTableName(tableName);
  }

  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public String getLocation() {
    return location;
  }

  @Override
  public String debugString() {
    return toSql();
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("ALTER TABLE " + getTbl());
    sb.append(" ADD ");
    if (ifNotExists) {
      sb.append("IF NOT EXISTS ");
    }
    sb.append(" " + partitionSpec.toSql());
    if (location != null) {
      sb.append(String.format(" LOCATION '%s'", location));
    }
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.ADD_PARTITION);
    TAlterTableAddPartitionParams addPartParams = new TAlterTableAddPartitionParams();
    addPartParams.setPartition_spec(partitionSpec.toThrift());
    addPartParams.setLocation(getLocation());
    addPartParams.setIf_not_exists(ifNotExists);
    params.setAdd_partition_params(addPartParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);
    if (!ifNotExists) {
      partitionSpec.setPartitionShouldNotExist();
    }
    partitionSpec.setPrivilegeRequirement(Privilege.ALTER);
    partitionSpec.analyze(analyzer);
  }
}
