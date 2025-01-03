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

package org.apache.impala.service;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisDriverImpl;
import org.apache.impala.analysis.AnalysisDriver;
import org.apache.impala.analysis.ParsedStatement;
import org.apache.impala.analysis.ParsedStatementImpl;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.SingleNodePlanner;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;


/**
 * Factory implementation of the original Impala planner which creates
 * Compiler implementation classes.
 */
public class CompilerFactoryImpl implements CompilerFactory {

  public static final String PLANNER = "OriginalPlanner";

  public ParsedStatement createParsedStatement(TQueryCtx queryCtx)
      throws ImpalaException {
    return new ParsedStatementImpl(queryCtx);
  }

  public AnalysisDriver createAnalysisDriver(AnalysisContext ctx,
      ParsedStatement parsedStmt, StmtTableCache stmtTableCache,
      AuthorizationContext authzCtx) throws AnalysisException {
    return new AnalysisDriverImpl(ctx, parsedStmt, stmtTableCache, authzCtx);
  }

  public SingleNodePlannerIntf createSingleNodePlanner(PlannerContext ctx)
      throws ImpalaException {
    return new SingleNodePlanner(ctx);
  }

  public String getPlannerString() {
    return PLANNER;
  }
}
