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
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;

/**
 * Base class for all ALTER TABLE ... SET statements
 */
public class AlterTableSetStmt extends AlterTableStmt {
  protected final PartitionSpec partitionSpec_;

  public AlterTableSetStmt(TableName tableName, PartitionSpec partitionSpec) {
    super(tableName);
    partitionSpec_ = partitionSpec;
    if (partitionSpec_ != null) partitionSpec_.setTableName(tableName);
  }

  public PartitionSpec getPartitionSpec() { return partitionSpec_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Table t = getTargetTable();
    // TODO: Support ALTER TABLE SET on HBase tables. Requires validating changes
    // to the SERDEPROPERTIES and TBLPROPERTIES to ensure the table metadata does not
    // become invalid.
    if (t instanceof HBaseTable) {
      throw new AnalysisException("ALTER TABLE SET not currently supported on " +
          "HBase tables.");
    }

    // Altering the table rather than the partition.
    if (partitionSpec_ == null) return;

    partitionSpec_.setPartitionShouldExist();
    partitionSpec_.setPrivilegeRequirement(Privilege.ALTER);
    partitionSpec_.analyze(analyzer);
  }
}
