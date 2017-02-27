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

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TPartitionDef;

/**
 * Represents a partition definition used in ALTER TABLE ADD PARTITION consisting of
 * partition key-value pairs and an optional location and optional caching options.
 */
public class PartitionDef implements ParseNode {
  private final PartitionSpec partitionSpec_;
  private final HdfsUri location_;
  private final HdfsCachingOp cacheOp_;

  public PartitionDef(PartitionSpec partitionSpec, HdfsUri location,
      HdfsCachingOp cacheOp) {
    Preconditions.checkNotNull(partitionSpec);
    partitionSpec_ = partitionSpec;
    location_ = location;
    cacheOp_ = cacheOp;
  }

  public void setTableName(TableName tableName) {
    partitionSpec_.setTableName(tableName);
  }
  public void setPartitionShouldNotExist() {
    partitionSpec_.setPartitionShouldNotExist();
  }

  public HdfsUri getLocation() { return location_; }

  public PartitionSpec getPartitionSpec() { return partitionSpec_; }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder(partitionSpec_.toSql());
    if (location_ != null) sb.append(String.format(" LOCATION '%s'", location_));
    if (cacheOp_ != null) sb.append(" " + cacheOp_.toSql());
    return sb.toString();
  }

  public TPartitionDef toThrift() {
    TPartitionDef params = new TPartitionDef();
    params.setPartition_spec(partitionSpec_.toThrift());
    if (location_ != null) params.setLocation(location_.toString());
    if (cacheOp_ != null) params.setCache_op(cacheOp_.toThrift());
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    partitionSpec_.setPrivilegeRequirement(Privilege.ALTER);
    partitionSpec_.analyze(analyzer);

    if (location_ != null) {
      location_.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);
    }

    Table table;
    try {
      table = analyzer.getTable(partitionSpec_.getTableName(), Privilege.ALTER,
          false);
    } catch (TableLoadingException e) {
      throw new AnalysisException(e.getMessage(), e);
    }

    Preconditions.checkState(table instanceof HdfsTable);
    HdfsTable hdfsTable = (HdfsTable)table;

    boolean shouldCache;
    if (cacheOp_ != null) {
      cacheOp_.analyze(analyzer);
      shouldCache = cacheOp_.shouldCache();
    } else {
      shouldCache = hdfsTable.isMarkedCached();
    }
    if (shouldCache) {
      if ((location_ != null && !FileSystemUtil.isPathCacheable(location_.getPath())) ||
          (location_ == null && !hdfsTable.isLocationCacheable())) {
        throw new AnalysisException(String.format("Location '%s' cannot be cached. " +
            "Please retry without caching: ALTER TABLE %s ADD PARTITION ... UNCACHED",
            (location_ != null) ? location_.toString() : hdfsTable.getLocation(),
            hdfsTable.getFullName()));
      }
    }
  }
}
