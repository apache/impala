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

import org.apache.impala.util.EventSequence;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlNode;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.calcite.rel.node.NodeWithExprs;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.ParseException;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.FrontendProfile;
import org.apache.impala.service.JniFrontend;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.thrift.TException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CalciteJniFrontend. This is a frontend that uses the Calcite code
 * to walk through all the steps of compiling the query (e.g. parsing, validating,
 * etc... to generate a TExecRequest that can be used by the execution engine.
 */
public class CalciteJniFrontend extends JniFrontend {

  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteJniFrontend.class.getName());

  private static Pattern SEMI_JOIN = Pattern.compile("\\bsemi\\sjoin\\b",
      Pattern.CASE_INSENSITIVE);

  private static Pattern ANTI_JOIN = Pattern.compile("\\banti\\sjoin\\b",
      Pattern.CASE_INSENSITIVE);

  public CalciteJniFrontend(byte[] thriftBackendConfig, boolean isBackendTest)
      throws ImpalaException, TException {
    super(thriftBackendConfig, isBackendTest);
    loadCalciteImpalaFunctions();
  }

  /**
   * Jni wrapper for Frontend.createExecRequest(). Accepts a serialized
   * TQueryContext; returns a serialized TQueryExecRequest.
   */
  @Override
  public byte[] createExecRequest(byte[] thriftQueryContext)
      throws ImpalaException {
    // Needed for Calcite's JaninoRelMetadataProvider
    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

    QueryContext queryCtx = new QueryContext(thriftQueryContext, getFrontend());

    if (!optionSupportedInCalcite(queryCtx)) {
      return runThroughOriginalPlanner(thriftQueryContext, queryCtx);
    }

    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      LOG.info("Using Calcite Planner for the following query: " + queryCtx.getStmt());

      // Parse the query
      RelMetadataQuery.THREAD_PROVIDERS.set(
          JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE));
      CalciteQueryParser queryParser = new CalciteQueryParser(queryCtx);
      SqlNode parsedSqlNode = queryParser.parse();
      markEvent(queryParser, parsedSqlNode, queryCtx, "Parsed query");

      // Make sure the metadata cache has all the info for the query.
      CalciteMetadataHandler mdHandler =
          new CalciteMetadataHandler(parsedSqlNode, queryCtx);
      markEvent(mdHandler, null, queryCtx, "Loaded tables");

      boolean isExplain = false;
      if (parsedSqlNode instanceof SqlExplain) {
        isExplain = true;
        parsedSqlNode = ((SqlExplain) parsedSqlNode).getExplicandum();
      }
      // Validate the parsed query
      CalciteValidator validator = new CalciteValidator(mdHandler, queryCtx);
      SqlNode validatedNode = validator.validate(parsedSqlNode);
      markEvent(mdHandler, validatedNode, queryCtx, "Validated query");

      // Convert the query to RelNodes which can be optimized
      CalciteRelNodeConverter relNodeConverter = new CalciteRelNodeConverter(validator);
      RelNode logicalPlan = relNodeConverter.convert(validatedNode);
      markEvent(mdHandler, logicalPlan, queryCtx, "Created initial logical plan");

      // Optimize the query
      CalciteOptimizer optimizer = new CalciteOptimizer(validator);
      ImpalaPlanRel optimizedPlan = optimizer.optimize(logicalPlan);
      markEvent(mdHandler, optimizedPlan, queryCtx, "Optimized logical plan");

      // Create Physical Impala PlanNodes
      CalcitePhysPlanCreator physPlanCreator =
          new CalcitePhysPlanCreator(mdHandler, queryCtx);
      NodeWithExprs rootNode = physPlanCreator.create(optimizedPlan);
      markEvent(mdHandler, rootNode, queryCtx, "Created physical plan");

      // Create exec request for the server
      ExecRequestCreator execRequestCreator =
          new ExecRequestCreator(physPlanCreator, queryCtx, mdHandler, isExplain);
      TExecRequest execRequest = execRequestCreator.create(rootNode);
      markEvent(mdHandler, execRequest, queryCtx, "Created exec request");

      byte[] serializedRequest = JniUtil.serializeToThrift(execRequest);
      queryCtx.getTimeline().markEvent("Serialized request");

      return serializedRequest;
    } catch (ParseException e) {
      // do a quick parse just to make sure it's not a select stmt. If it is
      // a select statement, we fail the query since all select statements
      // should be run through the Calcite Planner.
      if (Parser.parse(queryCtx.getStmt()) instanceof SelectStmt) {
        throw e;
      }
      LOG.info("Calcite planner failed to parse query: " + queryCtx.getStmt());
      LOG.info("Going to use original Impala planner.");
      return runThroughOriginalPlanner(thriftQueryContext, queryCtx);
    }
  }

  /**
   * Fallback planner method
   */
  public byte[] runThroughOriginalPlanner(byte[] thriftQueryContext,
      QueryContext queryCtx) throws ImpalaException {
    LOG.info("Using Impala Planner for the following query: " + queryCtx.getStmt());
    return super.createExecRequest(thriftQueryContext);
  }

  private void markEvent(CompilerStep compilerStep, Object stepResult,
      QueryContext queryCtx, String stepMessage) {
    LOG.info(stepMessage);
    queryCtx.getTimeline().markEvent(stepMessage);
    if (LOG.isDebugEnabled()) {
      compilerStep.logDebug(stepResult);
    }
  }

  private boolean optionSupportedInCalcite(QueryContext queryCtx) {
    // IMPALA-13530
    if (!queryCtx.getTQueryCtx().getClient_request().getQuery_options().isDecimal_v2()) {
      return false;
    }

    // IMPALA-13529
    if (queryCtx.getTQueryCtx().getClient_request().getQuery_options()
        .isAppx_count_distinct()) {
      return false;
    }
    return true;
  }

  private static void loadCalciteImpalaFunctions() {
    ImpalaOperatorTable.create(BuiltinsDb.getInstance());
  }

  public static class QueryContext {
    private final TQueryCtx queryCtx_;
    private final String stmt_;
    private final String currentDb_;
    private final Frontend frontend_;
    private final EventSequence timeline_;

    public QueryContext(byte[] thriftQueryContext,
        Frontend frontend) throws ImpalaException {
      this.queryCtx_ = new TQueryCtx();
      JniUtil.deserializeThrift(queryCtx_, thriftQueryContext);

      // hack to match the code in Frontend.java:
      // If unset, set MT_DOP to 0 to simplify the rest of the code.
      if (queryCtx_.getClient_request() != null &&
          queryCtx_.getClient_request().getQuery_options() != null) {
        if (!queryCtx_.getClient_request().getQuery_options().isSetMt_dop()) {
          queryCtx_.getClient_request().getQuery_options().setMt_dop(0);
        }
      }

      this.frontend_ = frontend;
      this.stmt_ = queryCtx_.getClient_request().getStmt();
      this.currentDb_ = queryCtx_.getSession().getDatabase();
      this.timeline_ = new EventSequence("Frontend Timeline (Calcite Planner)");
    }

    public TQueryCtx getTQueryCtx() {
     return queryCtx_;
    }

    public Frontend getFrontend() {
      return frontend_;
    }

    public String getStmt() {
      return stmt_;
    }

    public String getCurrentDb() {
      return currentDb_;
    }

    public EventSequence getTimeline() {
      return timeline_;
    }
  }
}
