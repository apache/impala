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

package org.apache.impala.authorization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A TableMask instance contains all the information for generating a subquery for table
 * masking (column masking / row filtering).
 */
public class TableMask {
  private static final Logger LOG = LoggerFactory.getLogger(TableMask.class);

  private final AuthorizationChecker authChecker_;
  private final String dbName_;
  private final String tableName_;
  private final List<Column> requiredColumns_;
  private final List<String> requiredColumnNames_;
  private final User user_;

  private boolean hasComplexColumnMask_;

  public TableMask(AuthorizationChecker authzChecker, String dbName, String tableName,
      List<Column> requiredColumns, User user) {
    this.authChecker_ = Preconditions.checkNotNull(authzChecker);
    this.user_ = Preconditions.checkNotNull(user);
    this.dbName_ = Preconditions.checkNotNull(dbName);
    this.tableName_ = Preconditions.checkNotNull(tableName);
    this.requiredColumns_ = Preconditions.checkNotNull(requiredColumns);
    this.requiredColumnNames_ = requiredColumns.stream().map(Column::getName)
        .collect(Collectors.toList());
  }

  public List<Column> getRequiredColumns() { return requiredColumns_; }
  public boolean hasComplexColumnMask () { return hasComplexColumnMask_; }

  /**
   * Returns whether the table/view has column masking or row filtering policies.
   */
  public boolean needsMaskingOrFiltering() throws InternalException {
    return authChecker_.needsMaskingOrFiltering(user_, dbName_, tableName_,
        requiredColumnNames_);
  }

  /**
   * Returns whether the table/view has row filtering policies.
   */
  public boolean needsRowFiltering() throws InternalException {
    return authChecker_.needsRowFiltering(user_, dbName_, tableName_);
  }

  public SelectStmt createColumnMaskStmt(String colName, Type colType,
      AuthorizationContext authzCtx) throws InternalException,
      AnalysisException {
    String maskedValue = authChecker_.createColumnMask(user_, dbName_, tableName_,
        colName, authzCtx);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Performing column masking on table {}.{}: {} => {}",
          dbName_, tableName_, colName, maskedValue);
    }
    if (maskedValue == null || maskedValue.equals(colName)) {  // Don't need masking.
      return null;
    }
    if (colType.isComplexType() || colName.contains(".")) {
      // Ignore column masks on complex types or their children.
      // TODO: RANGER-3525: Clarify handling of column masks on nested types
      LOG.warn("Ignoring column mask on complex type {}.{}: {} => {}",
          dbName_, tableName_, colName, maskedValue);
      return null;
    }
    SelectStmt maskStmt = (SelectStmt) Parser.parse(
        String.format("SELECT CAST(%s AS %s)", maskedValue, colType));
    if (maskStmt.getSelectList().getItems().size() != 1 || maskStmt.hasGroupByClause()
        || maskStmt.hasHavingClause() || maskStmt.hasWhereClause()) {
      // TODO (IMPALA-11019): is it ok to throw this exception during table loading,
      // before column privilege requests are registered?
      throw new AnalysisException("Illegal column masked value: " + maskedValue);
    }
    return maskStmt;
  }

  /**
   * Return the masked Expr of the given column
   */
  public Expr createColumnMask(String colName, Type colType,
      AuthorizationContext authzCtx) throws InternalException,
      AnalysisException {
    SelectStmt maskStmt = createColumnMaskStmt(colName, colType, authzCtx);
    if (maskStmt == null) return new SlotRef(Arrays.asList(colName.split("\\.")));
    Expr res = maskStmt.getSelectList().getItems().get(0).getExpr();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Returned Expr: " + res.toSql());
    }
    return res;
  }

  public SelectStmt createRowFilterStmt(AuthorizationContext authzCtx)
      throws InternalException, AnalysisException {
    String rowFilter = authChecker_.createRowFilter(user_, dbName_, tableName_, authzCtx);
    if (rowFilter == null) return null;
    // Parse the row filter string to AST by using it in a fake query.
    String stmtSql = String.format("SELECT 1 FROM foo WHERE %s", rowFilter);
    return (SelectStmt) Parser.parse(stmtSql);
  }

  /**
   * Return the row filter Expr
   */
  public Expr createRowFilter(AuthorizationContext authzCtx)
      throws InternalException, AnalysisException {
    SelectStmt selectStmt = createRowFilterStmt(authzCtx);
    if (selectStmt == null) return null;
    return selectStmt.getWhereClause();
  }
}
