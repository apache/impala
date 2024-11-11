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
import java.util.List;
import org.apache.impala.catalog.Column;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TMergeCaseType;
import org.apache.impala.thrift.TMergeMatchType;

/**
 * Delete clause for MERGE statement. This clause does not have any extra clauses, it
 * simply signals the deletion of the matched row. The number of result expressions must
 * match the number of columns of the target table as the expression materialization uses
 * the same row descriptor for every case.
 */
public class MergeDelete extends MergeCase {

  public MergeDelete() {}

  protected MergeDelete(List<Expr> resultExprs, List<Expr> filterExprs,
      TableName targetTableName, List<Column> targetTableColumns,
      TableRef targetTableRef, TMergeMatchType matchType, TableRef sourceTableRef) {
    super(resultExprs, filterExprs, targetTableName, targetTableColumns, targetTableRef,
        matchType, sourceTableRef);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    resultExprs_ = Lists.newArrayList();
    Preconditions.checkNotNull(targetTableColumns_);
    for (Column col : targetTableColumns_) {
      resultExprs_.add(
          createSlotRef(analyzer, targetTableRef_.getUniqueAlias(), col.getName()));
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    String parent = super.toSql(options);
    return String.format("%sDELETE", parent);
  }

  @Override
  public TMergeCaseType caseType() { return TMergeCaseType.DELETE; }

  @Override
  public MergeDelete clone() {
    return new MergeDelete(Expr.cloneList(resultExprs_), Expr.cloneList(getFilterExprs()),
        targetTableName_, targetTableColumns_, targetTableRef_, matchType_,
        sourceTableRef_);
  }
}
