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

package org.apache.impala.calcite.service;

import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisDriver;
import org.apache.impala.analysis.ParsedStatement;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.service.CompilerFactory;
import org.apache.impala.thrift.TQueryCtx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory implementation which creates Compiler implementation classes.
 */
public class CalciteCompilerFactory implements CompilerFactory {
  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteCompilerFactory.class.getName());

  static {
    System.setProperty("calcite.default.charset", "UTF8");
  }

  private static final String PLANNER = "CalcitePlanner";

  static {
    ImpalaOperatorTable.create(BuiltinsDb.getInstance());
  }

  public ParsedStatement createParsedStatement(TQueryCtx queryCtx)
      throws ImpalaException {

    // check that all options are supported, throws Exception if not.
    checkOptionSupportedInCalcite(queryCtx);

    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

    // Needed for Calcite's JaninoRelMetadataProvider
    RelMetadataQuery.THREAD_PROVIDERS.set(
        JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE));
    return new CalciteParsedStatement(queryCtx);
  }

  public AnalysisDriver createAnalysisDriver(AnalysisContext ctx,
      ParsedStatement parsedStmt, StmtTableCache stmtTableCache,
      AuthorizationContext authzCtx) throws AnalysisException {
    return new CalciteAnalysisDriver(ctx, parsedStmt, stmtTableCache, authzCtx);
  }

  public SingleNodePlannerIntf createSingleNodePlanner(PlannerContext ctx)
      throws ImpalaException {
    return new CalciteSingleNodePlanner(ctx);
  }

  public String getPlannerString() {
    return PLANNER;
  }

  public static void checkOptionSupportedInCalcite(TQueryCtx queryCtx)
      throws ImpalaException {
    // IMPALA-13530
    if (!queryCtx.getClient_request().getQuery_options().isDecimal_v2()) {
      throw new UnsupportedFeatureException("Decimal v1 not supported in Calcite, " +
          "falling back to Impala compiler.");
    }

    // IMPALA-13529
    if (queryCtx.getClient_request().getQuery_options()
        .isAppx_count_distinct()) {
      throw new UnsupportedFeatureException("Approximate count distinct is not " +
          "supported in Calcite, falling back to Impala compiler.");
    }
  }
}
