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

import static org.apache.impala.analysis.DmlStatementBase.createSlotRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.impala.catalog.Column;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TMergeCaseType;
import org.apache.impala.thrift.TMergeMatchType;

public class MergeUpdate extends MergeCase {
  protected List<Pair<SlotRef, Expr>> assignmentExprs_;

  public MergeUpdate(List<Pair<SlotRef, Expr>> assignmentExprs) {
    assignmentExprs_ = assignmentExprs;
  }

  protected MergeUpdate(List<Expr> resultExprs, List<Expr> filterExprs,
      TableName targetTableName, List<Column> targetTableColumns, TableRef targetTableRef,
      List<Pair<SlotRef, Expr>> assignmentExprs, TMergeMatchType matchType,
      TableRef sourceTableRef) {
    super(resultExprs, filterExprs, targetTableName, targetTableColumns, targetTableRef,
        matchType, sourceTableRef);
    assignmentExprs_ = assignmentExprs;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    Map<Integer, Expr> colToExprs = new HashMap<>();
    for (Pair<SlotRef, Expr> entry : assignmentExprs_) {
      SlotRef lhs = entry.first;
      Expr rhs = entry.second;
      lhs = disambiguateLhs(lhs);
      rhs.analyze(analyzer);

      Preconditions.checkNotNull(targetTableRef_);
      DmlStatementBase.checkCorrectTargetTable(lhs, rhs, targetTableRef_);
      DmlStatementBase.checkLhsIsColumnRef(lhs, rhs);
      DmlStatementBase.checkSubQuery(lhs, rhs);
      Preconditions.checkState(lhs.isAnalyzed());
      Column c = lhs.getResolvedPath().destColumn();
      rhs = DmlStatementBase.checkTypeCompatibility(analyzer, c, rhs, targetTableRef_);
      DmlStatementBase.checkLhsOnlyAppearsOnce(colToExprs, c, lhs, rhs);
      colToExprs.put(c.getPosition(), rhs);
    }
    resultExprs_ = Lists.newArrayList();
    Preconditions.checkNotNull(targetTableColumns_);
    Preconditions.checkNotNull(targetTableName_);
    for (Column col : targetTableColumns_) {
      InsertStmt.checkSupportedColumn(col, "MERGE UPDATE", targetTableName_);
      Expr expr = colToExprs.get(col.getPosition());
      if (expr == null) {
        expr = createSlotRef(analyzer, targetTableRef_.getUniqueAlias(), col.getName());
      }
      resultExprs_.add(expr);
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    String parent = super.toSql(options);
    return parent + "UPDATE SET " + listAssignments(options);
  }

  protected String listAssignments(ToSqlOptions options) {
    StringJoiner assignmentJoiner = new StringJoiner(", ");
    for (Pair<SlotRef, Expr> entry : assignmentExprs_) {
      assignmentJoiner.add(String.format(
          "%s = %s", entry.first.toSql(options), entry.second.toSql(options)));
    }
    return assignmentJoiner.toString();
  }

  @Override
  public TMergeCaseType caseType() { return TMergeCaseType.UPDATE; }

  /**
   * This method analyzes 'lhs' and if the analysis fails, it retries by specifying the
   * fully qualified name of it by appending the unique alias of the target table.
   * @param lhs left-hand-side expression of a SET clause
   * @return analyzed 'lhs'
   * @throws AnalysisException if the provided 'lhs' is already a fully qualified SlotRef,
   * and it fails the analysis.
   */
  private SlotRef disambiguateLhs(SlotRef lhs) throws AnalysisException {
    try {
      lhs.analyze(analyzer_);
      return lhs;
    } catch (AnalysisException e) {
      List<String> rawPath = lhs.rawPath_;
      if (rawPath.size() != 1) {
        throw new AnalysisException(
            String.format("Could not resolve column/field reference: '%s'", lhs));
      }
      String columnName = rawPath.get(0);
      return createSlotRef(analyzer_, targetTableRef_.getUniqueAlias(), columnName);
    }
  }

  @Override
  public void reset() {
    super.reset();
    for (Pair<SlotRef, Expr> expr : assignmentExprs_) {
      expr.first.reset();
      expr.second.reset();
    }
  }

  @Override
  public MergeUpdate clone() {
    return new MergeUpdate(Expr.cloneList(resultExprs_), Expr.cloneList(getFilterExprs()),
        targetTableName_, targetTableColumns_, targetTableRef_, assignmentExprs_,
        matchType_, sourceTableRef_);
  }
}
