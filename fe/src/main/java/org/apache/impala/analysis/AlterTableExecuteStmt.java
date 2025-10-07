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

import org.apache.impala.common.AnalysisException;

/**
 * Represents an ALTER TABLE <tbl> EXECUTE <operation>(<parameters>) statement on Iceberg
 * tables. For supported operations see the subclasses.
 */
public class AlterTableExecuteStmt extends AlterTableStmt {

  // Expression of the function call after EXECUTE keyword. Parsed into an operation and
  // a value of that operation.
  protected FunctionCallExpr fnCallExpr_;
  // Value expression from fnCallExpr_.
  protected Expr fnParamValue_;
  // The value after extracted from fnParamValue_ expression.
  protected long olderThanMillis_ = -1;

  protected AlterTableExecuteStmt(TableName tableName, Expr fnCallExpr) {
    super(tableName);
    fnCallExpr_ = (FunctionCallExpr) fnCallExpr;
  }

  @Override
  public String getOperation() { return "EXECUTE"; }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("ALTER TABLE ")
                           .append(getDb())
                           .append(".")
                           .append(getTbl())
                           .append(" EXECUTE ")
                           .append(fnCallExpr_.toSql(options));
    return sb.toString();
  }

  /**
   * Return an instance of a subclass of AlterTableExecuteStmt that can analyze the
   * execute statement for the function call expression in 'expr'.
   */
  public static AlterTableStmt createExecuteStmt(TableName tableName, Expr expr)
      throws AnalysisException {
    FunctionCallExpr fnCallExpr = (FunctionCallExpr) expr;
    String functionNameOrig = fnCallExpr.getFnName().toString();
    String functionName = functionNameOrig.toUpperCase();
    switch (functionName) {
      case "EXPIRE_SNAPSHOTS":
        return new AlterTableExecuteExpireSnapshotsStmt(tableName, fnCallExpr);
      case "ROLLBACK": return new AlterTableExecuteRollbackStmt(tableName, fnCallExpr);
      case "REMOVE_ORPHAN_FILES":
        return new AlterTableExecuteRemoveOrphanFilesStmt(tableName, fnCallExpr);
      case "REPAIR_METADATA":
        return new AlterTableExecuteRepairMetadataStmt(tableName, fnCallExpr);
      default:
        throw new AnalysisException(String.format("'%s' is not supported by ALTER "
                + "TABLE <table> EXECUTE. Supported operations are: "
                + "EXPIRE_SNAPSHOTS(<expression>), "
                + "ROLLBACK(<expression>), "
                + "REMOVE_ORPHAN_FILES(<expression>)."
                + "REPAIR_METADATA(), ",
            functionNameOrig));
    }
  }

  protected void analyzeFunctionCallExpr(Analyzer ignoredAnalyzer, String usage)
      throws AnalysisException {
    // fnCallExpr_ analyzed here manually, because it is not an actual function but a
    // catalog operation.
    String fnName = fnCallExpr_.getFnName().toString().toUpperCase();
    switch (fnName) {
      case "EXPIRE_SNAPSHOTS":
      case "ROLLBACK":
      case "REMOVE_ORPHAN_FILES":
        if (fnCallExpr_.getParams().size() != 1) {
          throw new AnalysisException(
              usage + " must have one parameter: " + fnCallExpr_.toSql());
        }
        fnParamValue_ = fnCallExpr_.getParams().exprs().get(0);
        break;
      case "REPAIR_METADATA":
        if (fnCallExpr_.getParams().size() != 0) {
          throw new AnalysisException(
              usage + " should have no parameter: " + fnCallExpr_.toSql());
        }
        break;
      default:
        Preconditions.checkState(false, "Invalid function call in ALTER TABLE EXECUTE.");
    }
  }

}
