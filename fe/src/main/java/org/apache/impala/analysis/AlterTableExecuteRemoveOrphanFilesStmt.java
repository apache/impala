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
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TAlterTableExecuteParams;
import org.apache.impala.thrift.TAlterTableExecuteRemoveOrphanFilesParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an ALTER TABLE EXECUTE REMOVE_ORPHAN_FILES(<parameter>) statement.
 * Valid <parameter> can either be:
 * - Timestamp expression, such as (NOW() - interval 5 days).
 * - Timestamp literal, such as '2025-01-01 10:00:00'.
 * However, user must be careful to not pass NOW() as timestamp expression because
 * it will remove files of in-progress operations.
 */
public class AlterTableExecuteRemoveOrphanFilesStmt extends AlterTableExecuteStmt {
  public static final String USAGE = "EXECUTE REMOVE_ORPHAN_FILES(<expression>):";
  private final static Logger LOG =
      LoggerFactory.getLogger(AlterTableExecuteRemoveOrphanFilesStmt.class);

  public AlterTableExecuteRemoveOrphanFilesStmt(
      TableName tableName, FunctionCallExpr fnCallExpr) {
    super(tableName, fnCallExpr);
  }

  @Override
  public String getOperation() {
    return "EXECUTE REMOVE_ORPHAN_FILES";
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable table = getTargetTable();
    if (!(table instanceof FeIcebergTable)) {
      throw new AnalysisException(
          "ALTER TABLE EXECUTE REMOVE_ORPHAN_FILES is only supported "
          + "for Iceberg tables: " + table.getTableName());
    }
    analyzeFunctionCallExpr(analyzer, USAGE);
    analyzeParameter(analyzer);
  }

  private void analyzeParameter(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(fnParamValue_);
    fnParamValue_.analyze(analyzer);

    if (!fnParamValue_.isConstant()) {
      throw new AnalysisException(
          USAGE + " <expression> must be a constant expression: " + fnCallExpr_.toSql());
    }

    Expr timestampExpr = getParamConvertibleToTimestamp();
    if (timestampExpr == null) {
      throw new AnalysisException(USAGE + " <expression> must be a timestamp, but is '"
          + fnParamValue_.getType() + "': " + fnCallExpr_.toSql());
    }

    try {
      olderThanMillis_ =
          ExprUtil.localTimestampToUnixTimeMicros(analyzer, timestampExpr) / 1000;
      LOG.debug(USAGE + " millis: " + olderThanMillis_);
    } catch (InternalException ie) {
      throw new AnalysisException("An invalid TIMESTAMP expression has been given "
              + "to " + USAGE + " the expression " + fnParamValue_.toSql()
              + " cannot be converted to a TIMESTAMP",
          ie);
    }
  }

  /**
   * If field fnParamValue_ is a Timestamp, or can be cast to a Timestamp,
   * then return an Expr for the Timestamp.
   * @return null if the fnParamValue_ cannot be converted to a Timestamp.
   */
  private Expr getParamConvertibleToTimestamp() {
    Preconditions.checkNotNull(fnParamValue_);
    Expr timestampExpr = fnParamValue_;
    if (timestampExpr.getType().isStringType()) {
      timestampExpr = new CastExpr(Type.TIMESTAMP, fnParamValue_);
    }
    if (timestampExpr.getType().isTimestamp()) { return timestampExpr; }
    return null;
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.EXECUTE);
    TAlterTableExecuteParams executeParams = new TAlterTableExecuteParams();
    TAlterTableExecuteRemoveOrphanFilesParams removeOrphanParams =
        new TAlterTableExecuteRemoveOrphanFilesParams();
    executeParams.setRemove_orphan_files_params(removeOrphanParams);
    removeOrphanParams.setOlder_than_millis(olderThanMillis_);
    params.setSet_execute_params(executeParams);
    return params;
  }
}
