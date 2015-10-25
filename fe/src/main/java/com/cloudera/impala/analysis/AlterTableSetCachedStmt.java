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

import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableSetCachedParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE [PARTITION partitionSpec] SET [UNCACHED|CACHED 'pool'].
 */
public class AlterTableSetCachedStmt extends AlterTableSetStmt {
  private final HdfsCachingOp cacheOp_;

  public AlterTableSetCachedStmt(TableName tableName,
      PartitionSpec partitionSpec, HdfsCachingOp cacheOp) {
    super(tableName, partitionSpec);
    Preconditions.checkNotNull(cacheOp);
    cacheOp_ = cacheOp;
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.SET_CACHED);
    TAlterTableSetCachedParams cachingParams =
        new TAlterTableSetCachedParams();
    if (getPartitionSpec() != null) {
      cachingParams.setPartition_spec(getPartitionSpec().toThrift());
    }
    cachingParams.setCache_op(cacheOp_.toThrift());
    params.setSet_cached_params(cachingParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    cacheOp_.analyze(analyzer);

    Table table = getTargetTable();
    Preconditions.checkNotNull(table);
    if (!(table instanceof HdfsTable)) {
      throw new AnalysisException("ALTER TABLE SET [CACHED|UNCACHED] must target an " +
          "HDFS table: " + table.getFullName());
    }

    if (cacheOp_.shouldCache()) {
      boolean isCacheable;
      PartitionSpec partSpec = getPartitionSpec();
      HdfsTable hdfsTable = (HdfsTable)table;
      StringBuilder nameSb = new StringBuilder();
      if (partSpec != null) {
        HdfsPartition part = hdfsTable.getPartition(partSpec.getPartitionSpecKeyValues());
        if (part == null) {
          throw new AnalysisException("Partition spec does not exist: " +
              partSpec.toSql());
        }
        isCacheable = part.isCacheable();
        nameSb.append("Partition (" + part.getPartitionName() + ")");
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
