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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;

/**
 * Base class for all ALTER TABLE ... SET statements
 */
public abstract class AlterTableSetStmt extends AlterTableStmt {
  protected final PartitionSet partitionSet_;

  protected AlterTableSetStmt(TableName tableName, PartitionSet partitionSet) {
    super(tableName);
    partitionSet_ = partitionSet;
    if (partitionSet_ != null) partitionSet_.setTableName(tableName);
  }

  PartitionSet getPartitionSet() { return partitionSet_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable t = getTargetTable();
    // TODO: Support ALTER TABLE SET on HBase tables. Requires validating changes
    // to the SERDEPROPERTIES and TBLPROPERTIES to ensure the table metadata does not
    // become invalid.
    if (t instanceof FeHBaseTable) {
      throw new AnalysisException("ALTER TABLE SET not currently supported on " +
          "HBase tables.");
    }

    // Altering the table rather than the partition.
    if (partitionSet_ == null) return;

    partitionSet_.setPartitionShouldExist();
    partitionSet_.setPrivilegeRequirement(Privilege.ALTER);
    partitionSet_.analyze(analyzer);
  }
}
