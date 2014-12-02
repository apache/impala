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
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableSetLocationParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * Represents an ALTER TABLE [PARTITION partitionSpec] SET LOCATION statement.
 */
public class AlterTableSetLocationStmt extends AlterTableSetStmt {
  private final HdfsUri location_;

  public AlterTableSetLocationStmt(TableName tableName,
      PartitionSpec partitionSpec, HdfsUri location) {
    super(tableName, partitionSpec);
    Preconditions.checkNotNull(location);
    this.location_ = location;
  }

  public HdfsUri getLocation() { return location_; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.SET_LOCATION);
    TAlterTableSetLocationParams locationParams =
        new TAlterTableSetLocationParams(location_.toString());
    if (getPartitionSpec() != null) {
      locationParams.setPartition_spec(getPartitionSpec().toThrift());
    }
    params.setSet_location_params(locationParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    location_.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);

    Table table = getTargetTable();
    Preconditions.checkNotNull(table);
    if (table instanceof HdfsTable) {
      HdfsTable hdfsTable = (HdfsTable) table;
      if (getPartitionSpec() != null) {
        // Targeting a partition rather than a table.
        PartitionSpec partitionSpec = getPartitionSpec();
        HdfsPartition partition = hdfsTable.getPartition(
            partitionSpec.getPartitionSpecKeyValues());
        Preconditions.checkNotNull(partition);
        if (partition.isMarkedCached()) {
          throw new AnalysisException(String.format("Target partition is cached, " +
              "please uncache before changing the location using: ALTER TABLE %s %s " +
              "SET UNCACHED", table.getFullName(), partitionSpec.toSql()));
        }
      } else if (hdfsTable.isMarkedCached()) {
        throw new AnalysisException(String.format("Target table is cached, please " +
            "uncache before changing the location using: ALTER TABLE %s SET UNCACHED",
            table.getFullName()));
      }
    }
  }
}
