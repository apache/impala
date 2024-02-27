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

import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;

/**
 * Represents an ALTER TABLE RECOVER PARTITIONS statement.
 */
public class AlterTableRecoverPartitionsStmt extends AlterTableStmt {

  public AlterTableRecoverPartitionsStmt(TableName tableName) {
    super(tableName);
  }

  @Override
  public String getOperation() { return "RECOVER PARTITIONS"; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.RECOVER_PARTITIONS);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    // Make sure the target table is an FS-backed Table.
    if (!(table_ instanceof FeFsTable)) {
      throw new AnalysisException("ALTER TABLE RECOVER PARTITIONS " +
          "must target an HDFS table: " + tableName_);
    }

    if (table_ instanceof FeIcebergTable) {
      throw new AnalysisException("ALTER TABLE RECOVER PARTITIONS is not supported " +
          "on Iceberg tables: " + table_.getFullName());
    }

    // Make sure the target table is partitioned.
    if (table_.getMetaStoreTable().getPartitionKeysSize() == 0) {
      throw new AnalysisException("Table is not partitioned: " + tableName_);
    }
  }
}
