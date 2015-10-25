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
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.thrift.TAlterTableAddPartitionParams;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * Represents an ALTER TABLE ADD PARTITION statement.
 */
public class AlterTableAddPartitionStmt extends AlterTableStmt {
  private final HdfsUri location_;
  private final boolean ifNotExists_;
  private final PartitionSpec partitionSpec_;
  private final HdfsCachingOp cacheOp_;

  public AlterTableAddPartitionStmt(TableName tableName,
      PartitionSpec partitionSpec, HdfsUri location, boolean ifNotExists,
      HdfsCachingOp cacheOp) {
    super(tableName);
    Preconditions.checkState(partitionSpec != null);
    location_ = location;
    ifNotExists_ = ifNotExists;
    partitionSpec_ = partitionSpec;
    partitionSpec_.setTableName(tableName);
    cacheOp_ = cacheOp;
  }

  public boolean getIfNotExists() { return ifNotExists_; }
  public HdfsUri getLocation() { return location_; }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("ALTER TABLE " + getTbl());
    sb.append(" ADD ");
    if (ifNotExists_) {
      sb.append("IF NOT EXISTS ");
    }
    sb.append(" " + partitionSpec_.toSql());
    if (location_ != null) {
      sb.append(String.format(" LOCATION '%s'", location_));
    }
    if (cacheOp_ != null) sb.append(cacheOp_.toSql());
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.ADD_PARTITION);
    TAlterTableAddPartitionParams addPartParams = new TAlterTableAddPartitionParams();
    addPartParams.setPartition_spec(partitionSpec_.toThrift());
    addPartParams.setLocation(location_ == null ? null : location_.toString());
    addPartParams.setIf_not_exists(ifNotExists_);
    if (cacheOp_ != null) addPartParams.setCache_op(cacheOp_.toThrift());
    params.setAdd_partition_params(addPartParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if (!ifNotExists_) partitionSpec_.setPartitionShouldNotExist();
    partitionSpec_.setPrivilegeRequirement(Privilege.ALTER);
    partitionSpec_.analyze(analyzer);

    if (location_ != null) {
      location_.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);
    }

    boolean shouldCache = false;
    Table table = getTargetTable();
    if (cacheOp_ != null) {
      cacheOp_.analyze(analyzer);
      shouldCache = cacheOp_.shouldCache();
    } else if (table instanceof HdfsTable) {
      shouldCache = ((HdfsTable)table).isMarkedCached();
    }
    if (shouldCache) {
      if (!(table instanceof HdfsTable)) {
        throw new AnalysisException("Caching must target a HDFS table: " +
            table.getFullName());
      }
      HdfsTable hdfsTable = (HdfsTable)table;
      if ((location_ != null && !FileSystemUtil.isPathCacheable(location_.getPath())) ||
          (location_ == null && !hdfsTable.isLocationCacheable())) {
        throw new AnalysisException(String.format("Location '%s' cannot be cached. " +
            "Please retry without caching: ALTER TABLE %s ADD PARTITION ... UNCACHED",
            (location_ != null) ? location_.toString() : hdfsTable.getLocation(),
            table.getFullName()));
      }
    }
  }
}
