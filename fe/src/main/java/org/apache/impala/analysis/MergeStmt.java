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

import java.util.List;

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TMergeCaseType;
import org.apache.impala.thrift.TMergeMatchType;
import org.apache.impala.thrift.TSortingOrder;

/**
 * Representation of the MERGE statement. The MERGE statement has one target table, and a
 * source expression that is joined with an ON clause. The statement consists of
 * WHEN MATCHED / WHEN NOT MATCHED clauses referenced as merge cases, the evaluation of
 * these cases are following the order of their definition. The merge cases can have
 * additional filter expressions, for example: WHEN MATCHED AND s.id > 10. One MERGE
 * statement can have at most 1000 cases. The statement can insert, update and delete rows
 * of the target table by conditions defined by merge cases.
 */
public class MergeStmt extends DmlStatementBase {
  private static final int MERGE_CASE_LIMIT = 1000;
  private TableRef targetTableRef_;
  private TableRef sourceTableRef_;
  private final List<MergeCase> cases_;
  private final Expr onClause_;
  private MergeImpl impl_;

  public MergeStmt(TableRef target, TableRef source, Expr onClause,
      List<MergeCase> cases) {
    targetTableRef_ = target;
    sourceTableRef_ = source;
    onClause_ = onClause;
    cases_ = cases;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    if (targetTableRef_ instanceof InlineViewRef) {
      throw new AnalysisException(
          String.format("Cannot modify view: %s", targetTableRef_.toSql()));
    }

    if (cases_.size() > MERGE_CASE_LIMIT) {
      String sql = toSql();
      String sqlSubstr = sql.substring(0, Math.min(80, sql.length()));
      throw new AnalysisException(String.format("Exceeded the maximum number of cases " +
          "(%s).\nStatement has %s cases:\n%s...",
          MERGE_CASE_LIMIT, cases_.size(), sqlSubstr));
    }

    sourceTableRef_ = analyzer.resolveTableRef(sourceTableRef_);
    targetTableRef_ = analyzer.resolveTableRef(targetTableRef_);

    table_ = targetTableRef_.getTable();

    if (impl_ == null) {
      if (table_ instanceof FeIcebergTable) {
        impl_ = new IcebergMergeImpl(this, targetTableRef_, sourceTableRef_, onClause_);
        setMaxTableSinks(analyzer_.getQueryOptions().getMax_fs_writers());
      } else {
        throw new AnalysisException(String.format(
            "Target table must be an Iceberg table: %s", table_.getFullName()));
      }
    }

    impl_.analyze(analyzer);

    for (MergeCase mergeCase : getCases()) {
      mergeCase.setParent(this);
      mergeCase.analyze(analyzer);
    }
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    super.collectTableRefs(tblRefs);
    if (sourceTableRef_ instanceof InlineViewRef) {
      ((InlineViewRef) sourceTableRef_).queryStmt_.collectTableRefs(tblRefs);
    } else {
      tblRefs.add(sourceTableRef_);
    }
    if (!(targetTableRef_ instanceof InlineViewRef)) { tblRefs.add(targetTableRef_); }
  }

  @Override
  public DataSink createDataSink() { return impl_.createDataSink(); }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    impl_.substituteResultExprs(smap, analyzer);
  }

  @Override
  public List<Expr> getResultExprs() { return impl_.getResultExprs(); }
  @Override
  public List<Expr> getPartitionKeyExprs() { return impl_.getPartitionKeyExprs(); }

  @Override
  public List<Expr> getSortExprs() { return impl_.getSortExprs(); }

  @Override
  public TSortingOrder getSortingOrder() { return impl_.getSortingOrder(); }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder builder = new StringBuilder();
    builder.append("MERGE INTO ");
    builder.append(targetTableRef_.toSql(options));
    builder.append(" USING ");
    builder.append(sourceTableRef_.toSql(options));
    builder.append(" ON ");
    builder.append(onClause_.toSql(options));
    for (MergeCase mergeCase : cases_) {
      builder.append(" ");
      builder.append(mergeCase.toSql(options));
    }
    return builder.toString();
  }

  @Override
  public void reset() {
    super.reset();
    impl_.reset();
    onClause_.reset();
    for (MergeCase mergeCase : cases_) {
      mergeCase.reset();
    }
  }
  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    return getQueryStmt().resolveTableMask(analyzer);
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    getQueryStmt().rewriteExprs(rewriter);
    for (MergeCase mergeCase : cases_) {
      mergeCase.rewriteExprs(rewriter);
    }
  }

  public QueryStmt getQueryStmt() { return impl_.getQueryStmt(); }

  public TableRef getTargetTableRef() { return targetTableRef_; }

  public PlanNode getPlanNode(PlannerContext ctx, PlanNode child, Analyzer analyzer)
      throws ImpalaException {
    return impl_.getPlanNode(ctx, child, analyzer);
  }

  public List<MergeCase> getCases() { return cases_; }

  public boolean hasOnlyMatchedCases() {
    return cases_.stream().allMatch(mergeCase -> mergeCase.matchType().equals(
        TMergeMatchType.MATCHED));
  }

  public boolean hasOnlyInsertCases() {
    return cases_.stream().allMatch(
        mergeCase -> mergeCase.caseType().equals(TMergeCaseType.INSERT));
  }
  public boolean hasOnlyDeleteCases() {
    return cases_.stream().allMatch(
        mergeCase -> mergeCase.caseType().equals(TMergeCaseType.DELETE));
  }

  public TableRef getSourceTableRef() {
    return sourceTableRef_;
  }
}
