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

import hiveexec.com.google.common.base.Preconditions;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableExecuteParams;
import org.apache.impala.thrift.TAlterTableExecuteRepairMetadataParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;

public class AlterTableExecuteRepairMetadataStmt extends AlterTableExecuteStmt {

  protected final static String USAGE = "EXECUTE REPAIR_METADATA()";

  protected AlterTableExecuteRepairMetadataStmt(TableName tableName, Expr fnCallExpr) {
    super(tableName, fnCallExpr);
  }

  @Override
  public String getOperation() { return "EXECUTE REPAIR_METADATA"; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable table = getTargetTable();
    if (!(table instanceof FeIcebergTable)) {
      throw new AnalysisException(
          "ALTER TABLE EXECUTE REPAIR_METADATA is only supported "
          + "for Iceberg tables: " + table.getTableName());
    }
    analyzeFunctionCallExpr(analyzer, USAGE);
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.EXECUTE);
    TAlterTableExecuteParams executeParams = new TAlterTableExecuteParams();
    TAlterTableExecuteRepairMetadataParams repairMetadataParams =
        new TAlterTableExecuteRepairMetadataParams();
    executeParams.setRepair_metadata_params(repairMetadataParams);
    params.setSet_execute_params(executeParams);
    return params;
  }
}
