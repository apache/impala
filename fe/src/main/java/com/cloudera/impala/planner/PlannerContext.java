// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.thrift.TQueryOptions;

/**
 * Contains the analysis result of a query as well as planning-specific
 * parameters and state such as plan-node and plan-fragment id generators.
 */
public class PlannerContext {
  // Estimate of the overhead imposed by storing data in a hash tbl;
  // used for determining whether a broadcast join is feasible.
  public final static double HASH_TBL_SPACE_OVERHEAD = 1.1;

  // The maximum fraction of remaining memory that a sort node can use during execution.
  public final static double SORT_MEM_MAX_FRACTION = 0.80;

  private final IdGenerator<PlanNodeId> nodeIdGenerator_ = PlanNodeId.createGenerator();
  private final IdGenerator<PlanFragmentId> fragmentIdGenerator_ =
      PlanFragmentId.createGenerator();

  private final TQueryCtx queryCtx_;
  private final AnalysisContext.AnalysisResult analysisResult_;
  private final QueryStmt queryStmt_;

  public PlannerContext (AnalysisContext.AnalysisResult analysisResult,
      TQueryCtx queryCtx) {
    analysisResult_ = analysisResult;
    queryCtx_ = queryCtx;
    if (isInsertOrCtas()) {
      queryStmt_ = analysisResult.getInsertStmt().getQueryStmt();
    } else if (analysisResult.isUpdateStmt()) {
      queryStmt_ = analysisResult.getUpdateStmt().getQueryStmt();
    } else {
      queryStmt_ = analysisResult.getQueryStmt();
    }
  }

  public QueryStmt getQueryStmt() { return queryStmt_; }
  public TQueryCtx getQueryCtx() { return queryCtx_; }
  public TQueryOptions getQueryOptions() {
    return queryCtx_.getRequest().getQuery_options();
  }
  public AnalysisContext.AnalysisResult getAnalysisResult() { return analysisResult_; }
  public Analyzer getRootAnalyzer() { return analysisResult_.getAnalyzer(); }
  public boolean isSingleNodeExec() { return getQueryOptions().num_nodes == 1; }
  public PlanNodeId getNextNodeId() { return nodeIdGenerator_.getNextId(); }
  public PlanFragmentId getNextFragmentId() { return fragmentIdGenerator_.getNextId(); }
  public boolean isInsertOrCtas() {
    return analysisResult_.isInsertStmt() || analysisResult_.isCreateTableAsSelectStmt();
  }
  public boolean isUpdate() { return analysisResult_.isUpdateStmt(); }
}
