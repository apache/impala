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

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TAlterTableExecuteParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Represents an ALTER TABLE <tbl> EXECUTE <operation>(<parameters>) statement on Iceberg
 * tables, supported operations:
 *  - expire_snapshots(<timestamp>): uses the ExpireSnapshot API to expire snaphosts,
 *    calls the ExpireSnapshot.expireOlderThan(timestampMillis) method.
 *    TableProperties.MIN_SNAPSHOTS_TO_KEEP table property manages how many snapshots
 *    should be retained even when all snapshots are selected by expireOlderThan().
 */
public class AlterTableExecuteStmt extends AlterTableStmt {
  private final static Logger LOG = LoggerFactory.getLogger(AlterTableExecuteStmt.class);

  private final static String USAGE = "EXPIRE_SNAPSHOTS(<expression>)";

  // Expression of the function call after EXECUTE keyword. Parsed into an operation and
  // a value of that operation.
  private FunctionCallExpr fnCallExpr_;

  // Value expression from fnCallExpr_.
  private Expr fnParamValue_;

  // The value after extracted from fnParamValue_ expression.
  private long olderThanMillis_ = -1;

  protected AlterTableExecuteStmt(TableName tableName, Expr fnCallExpr) {
    super(tableName);
    fnCallExpr_ = (FunctionCallExpr)fnCallExpr;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Preconditions.checkState(getTargetTable() instanceof FeIcebergTable);
    analyzeFunctionCallExpr(analyzer);
    analyzeOlderThan(analyzer);
  }

  private void analyzeFunctionCallExpr(Analyzer analyzer) throws AnalysisException {
    // fnCallExpr_ analyzed here manually, because it is not an actual function but a
    // catalog operation.
    String fnName = fnCallExpr_.getFnName().toString();
    if (!fnName.toUpperCase().equals("EXPIRE_SNAPSHOTS")) {
      throw new AnalysisException(String.format("'%s' is not supported by ALTER " +
          "TABLE <table> EXECUTE. Supported operation is %s.", fnName, USAGE));
    }
    if (fnCallExpr_.getParams().size() != 1) {
      throw new AnalysisException(USAGE + " must have one parameter: " + toSql());
    }
    fnParamValue_ = fnCallExpr_.getParams().exprs().get(0);
  }

  private void analyzeOlderThan(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(fnParamValue_);
    fnParamValue_.analyze(analyzer);
    if (!fnParamValue_.isConstant()) {
      throw new AnalysisException(USAGE + " must be a constant expression: " + toSql());
    }
    if (fnParamValue_.getType().isStringType()) {
      fnParamValue_ = new CastExpr(Type.TIMESTAMP, fnParamValue_);
    }
    if (!fnParamValue_.getType().isTimestamp()) {
      throw new AnalysisException(USAGE + " must be a timestamp type but is '" +
          fnParamValue_.getType() + "': " + fnParamValue_.toSql());
    }
    try {
      olderThanMillis_ =
          ExprUtil.localTimestampToUnixTimeMicros(analyzer, fnParamValue_) / 1000;
      LOG.debug(USAGE + " millis: " + String.valueOf(olderThanMillis_));
    } catch (InternalException ie) {
      throw new AnalysisException("Invalid TIMESTAMP expression has been given to " +
          USAGE + ": " + ie.getMessage(), ie);
    }
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
    executeParams.setOlder_than_millis(olderThanMillis_);
    params.setSet_execute_params(executeParams);
    return params;
  }
}
