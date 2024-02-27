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
import org.apache.impala.thrift.TAlterTableExecuteRollbackParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TRollbackType;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an ALTER TABLE EXECUTE ROLLBACK(<parameter>) statement.
 * The parameter can be a snapshot id, or a timestamp.
 * A rollback to a snapshot id causes a new snapshot to be
 * created with the same snapshot id, but with a new creation timestamp.
 * A rollback to a timestamp rolls back to the latest snapshot
 * that has a creation timestamp that is older than the specified
 * timestamp.
 */
public class AlterTableExecuteRollbackStmt extends AlterTableExecuteStmt {
  public static final String USAGE = "EXECUTE ROLLBACK(<expression>):";
  private final static Logger LOG =
      LoggerFactory.getLogger(AlterTableExecuteRollbackStmt.class);
  private long snapshotVersion_;
  private TRollbackType kind_;

  public AlterTableExecuteRollbackStmt(TableName tableName, FunctionCallExpr fnCallExpr) {
    super(tableName, fnCallExpr);
  }

  @Override
  public String getOperation() { return "EXECUTE ROLLBACK"; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable table = getTargetTable();
    if (!(table instanceof FeIcebergTable)) {
      throw new AnalysisException("ALTER TABLE EXECUTE ROLLBACK is only supported "
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
          USAGE + " <expression> must be a constant expression: EXECUTE " + toSql());
    }
    if ((fnParamValue_ instanceof LiteralExpr)
        && (fnParamValue_.getType().isIntegerType())) {
      // Parameter is a snapshot id
      kind_ = TRollbackType.VERSION_ID;
      snapshotVersion_ = fnParamValue_.evalToInteger(analyzer, USAGE);
      if (snapshotVersion_ < 0) {
        throw new AnalysisException("Invalid version number has been given to " + USAGE
            + ": " + snapshotVersion_);
      }
      LOG.debug(USAGE + " version: " + snapshotVersion_);
    } else {
      Expr timestampEpr = getParamConvertibleToTimestamp();
      if (timestampEpr != null) {
        // Parameter is a timestamp.
        kind_ = TRollbackType.TIME_ID;
        try {
          olderThanMillis_ =
              ExprUtil.localTimestampToUnixTimeMicros(analyzer, timestampEpr) / 1000;
          LOG.debug(USAGE + " millis: " + olderThanMillis_);
        } catch (InternalException ie) {
          throw new AnalysisException("An invalid TIMESTAMP expression has been given "
                  + "to " + USAGE + " the expression " + fnParamValue_.toSql()
                  + " cannot be converted to a TIMESTAMP",
              ie);
        }
      } else {
        throw new AnalysisException(USAGE
            + " <expression> must be an integer type or a timestamp, but is '"
            + fnParamValue_.getType() + "': EXECUTE " + toSql());
      }
    }
  }

  /**
   * If field fnParamValue_ is a Timestamp, or can be cast to a Timestamp,
   * then return an Expr for the Timestamp.
   * @return null if the fnParamValue_ cannot be converted to a Timestamp.
   */
  private Expr getParamConvertibleToTimestamp() {
    Expr timestampExpr = fnParamValue_;
    if (timestampExpr.getType().isStringType()) {
      timestampExpr = new CastExpr(Type.TIMESTAMP, fnParamValue_);
    }
    if (timestampExpr.getType().isTimestamp()) {
      return timestampExpr;
    }
    return null;
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
    TAlterTableExecuteRollbackParams executeRollbackParams =
        new TAlterTableExecuteRollbackParams();
    executeParams.setExecute_rollback_params(executeRollbackParams);
    executeRollbackParams.setKind(kind_);
    switch (kind_) {
      case TIME_ID: executeRollbackParams.setTimestamp_millis(olderThanMillis_); break;
      case VERSION_ID: executeRollbackParams.setSnapshot_id(snapshotVersion_); break;
      default: throw new IllegalStateException("Bad kind of execute rollback " + kind_);
    }
    params.setSet_execute_params(executeParams);
    return params;
  }
}
