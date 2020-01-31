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
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A TableMask instance contains all the information for generating a subquery for table
 * masking (column masking / row filtering).
 */
public class TableMask {
  private static final Logger LOG = LoggerFactory.getLogger(TableMask.class);

  private AuthorizationChecker authChecker_;
  private String dbName_;
  private String tableName_;
  private List<String> requiredColumns_;
  private User user_;

  public TableMask(AuthorizationChecker authzChecker, FeTable table, User user) {
    this.authChecker_ = authzChecker;
    this.dbName_ = table.getDb().getName();
    this.tableName_ = table.getName();
    // TODO: only require materialize columns to avoid unneccessary masking so we won't
    //       hit IMPALA-9223
    this.requiredColumns_ = table.getColumnNames();
    this.user_ = user;
  }

  /**
   * Returns whether the table/view has column masking or row filtering policies.
   */
  public boolean needsMaskingOrFiltering() throws InternalException {
    return authChecker_.needsMaskingOrFiltering(user_, dbName_, tableName_,
        requiredColumns_);
  }

  /**
   * Return the masked Expr of the given column
   */
  public Expr createColumnMask(String colName, Type colType)
      throws InternalException, AnalysisException {
    Preconditions.checkState(!colType.isComplexType());
    String maskedValue = authChecker_.createColumnMask(user_, dbName_, tableName_,
        colName);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Performing column masking on table {}.{}: {} => {}",
          dbName_, tableName_, colName, maskedValue);
    }
    if (maskedValue == null || maskedValue.equals(colName)) {  // Don't need masking.
      return new SlotRef(Lists.newArrayList(colName));
    }
    SelectStmt maskStmt = (SelectStmt) Parser.parse(
        String.format("SELECT CAST(%s AS %s)", maskedValue, colType));
    if (maskStmt.getSelectList().getItems().size() != 1 || maskStmt.hasGroupByClause()
        || maskStmt.hasHavingClause() || maskStmt.hasWhereClause()) {
      throw new AnalysisException("Illegal column masked value: " + maskedValue);
    }
    Expr res = maskStmt.getSelectList().getItems().get(0).getExpr();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Returned Expr: " + res.toSql());
    }
    return res;
  }
}
