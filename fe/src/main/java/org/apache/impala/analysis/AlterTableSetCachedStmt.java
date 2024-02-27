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

import java.util.List;

import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableSetCachedParams;
import org.apache.impala.thrift.TAlterTableType;
import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE [PARTITION partitionSet] SET [UNCACHED|CACHED 'pool'].
 */
public class AlterTableSetCachedStmt extends AlterTableSetStmt {
  private final HdfsCachingOp cacheOp_;

  public AlterTableSetCachedStmt(TableName tableName,
      PartitionSet partitionSet, HdfsCachingOp cacheOp) {
    super(tableName, partitionSet);
    Preconditions.checkNotNull(cacheOp);
    cacheOp_ = cacheOp;
  }

  @Override
  public String getOperation() {
    return cacheOp_.shouldCache() ? "SET CACHED" : "SET UNCACHED";
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.SET_CACHED);
    TAlterTableSetCachedParams cachingParams =
        new TAlterTableSetCachedParams();
    if (getPartitionSet() != null) {
      cachingParams.setPartition_set(getPartitionSet().toThrift());
    }
    cachingParams.setCache_op(cacheOp_.toThrift());
    params.setSet_cached_params(cachingParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    cacheOp_.analyze(analyzer);

    FeTable table = getTargetTable();
    Preconditions.checkNotNull(table);
    if (!(table instanceof FeFsTable)) {
      throw new AnalysisException("ALTER TABLE SET [CACHED|UNCACHED] must target an " +
          "HDFS table: " + table.getFullName());
    }

    if (cacheOp_.shouldCache()) {
      boolean isCacheable = true;
      PartitionSet partitionSet = getPartitionSet();
      FeFsTable hdfsTable = (FeFsTable)table;
      StringBuilder nameSb = new StringBuilder();
      if (partitionSet != null) {
        List<? extends FeFsPartition> parts = partitionSet.getPartitions();
        nameSb.append("Partition(s) (");
        for(FeFsPartition part: parts) {
          isCacheable = isCacheable && part.isCacheable();
          if(!part.isCacheable()) nameSb.append(part.getPartitionName());
        }
        nameSb.append(")");
      } else {
        isCacheable = hdfsTable.isCacheable();
        nameSb.append("Table ").append(table.getFullName());
      }
      if (!isCacheable) {
        throw new AnalysisException(nameSb.toString() + " cannot be cached. Please " +
            "check if the table or partitions are on a filesystem which supports " +
            "caching.");
      }
    }
  }
}
