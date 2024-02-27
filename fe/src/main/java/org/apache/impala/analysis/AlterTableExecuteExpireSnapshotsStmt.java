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

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TAlterTableExecuteExpireSnapshotsParams;
import org.apache.impala.thrift.TAlterTableExecuteParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an ALTER TABLE <tbl> EXECUTE EXPIRE_SNAPSHOTS(<parameters>) statement on
 * Iceberg tables, the parameter is (<timestamp>).
 */
public class AlterTableExecuteExpireSnapshotsStmt extends AlterTableExecuteStmt {
  private final static Logger LOG =
      LoggerFactory.getLogger(AlterTableExecuteExpireSnapshotsStmt.class);

  protected final static String USAGE = "EXPIRE_SNAPSHOTS(<expression>)";

  protected AlterTableExecuteExpireSnapshotsStmt(TableName tableName, Expr fnCallExpr) {
    super(tableName, fnCallExpr);
  }

  @Override
  public String getOperation() { return "EXECUTE EXPIRE_SNAPSHOTS"; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Preconditions.checkState(getTargetTable() instanceof FeIcebergTable);
    analyzeFunctionCallExpr(analyzer, USAGE);
    analyzeOlderThan(analyzer);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return fnCallExpr_.toSql();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.EXECUTE);
    TAlterTableExecuteParams executeParams = new TAlterTableExecuteParams();
    TAlterTableExecuteExpireSnapshotsParams executeExpireSnapshotsParams =
        new TAlterTableExecuteExpireSnapshotsParams();
    executeParams.setExpire_snapshots_params(executeExpireSnapshotsParams);
    executeExpireSnapshotsParams.setOlder_than_millis(olderThanMillis_);
    params.setSet_execute_params(executeParams);
    return params;
  }

  protected void analyzeOlderThan(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(fnParamValue_);
    fnParamValue_.analyze(analyzer);
    if (!fnParamValue_.isConstant()) {
      throw new AnalysisException(USAGE + " must be a constant expression: " + toSql());
    }
    if (fnParamValue_.getType().isStringType()) {
      fnParamValue_ = new CastExpr(Type.TIMESTAMP, fnParamValue_);
    }
    if (!fnParamValue_.getType().isTimestamp()) {
      throw new AnalysisException(USAGE + " must be a timestamp type but is '"
          + fnParamValue_.getType() + "': " + fnParamValue_.toSql());
    }
    try {
      olderThanMillis_ =
          ExprUtil.localTimestampToUnixTimeMicros(analyzer, fnParamValue_) / 1000;
      LOG.debug(USAGE + " millis: " + olderThanMillis_);
    } catch (InternalException ie) {
      throw new AnalysisException("Invalid TIMESTAMP expression has been given to "
              + USAGE + ": " + ie.getMessage(),
          ie);
    }
  }
}
