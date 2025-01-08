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

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.AnalysisDriver;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ParsedStatement;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.calcite.schema.ImpalaCalciteCatalogReader;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.calcite.validate.ImpalaConformance;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.thrift.TQueryCtx;

/**
 * The CalciteAnalysisDriver is the implementation of AnalysisDriver which validates
 * the AST produced by Calcite.
 */
public class CalciteAnalysisDriver implements AnalysisDriver {

  public final TQueryCtx queryCtx_;

  public final AuthorizationFactory authzFactory_;

  public final CalciteParsedStatement parsedStmt_;

  // Calcite AST
  public SqlNode validatedNode_;

  public RelDataTypeFactory typeFactory_;

  public SqlValidator sqlValidator_;

  // CalciteCatalogReader is a context class that holds global information that
  // may be needed by the CalciteTable object
  private CalciteCatalogReader reader_;

  private final AnalysisContext ctx_;

  private final StmtTableCache stmtTableCache_;

  private final AuthorizationContext authzCtx_;

  private final Analyzer analyzer_;

  public CalciteAnalysisDriver(AnalysisContext ctx, ParsedStatement parsedStmt,
      StmtTableCache stmtTableCache,
      AuthorizationContext authzCtx) {
    queryCtx_ = ctx.getQueryCtx();
    authzFactory_ = ctx.getAuthzFactory();
    parsedStmt_ = (CalciteParsedStatement) parsedStmt;
    ctx_ = ctx;
    stmtTableCache_ = stmtTableCache;
    authzCtx_ = authzCtx;
    analyzer_ = createAnalyzer(ctx, stmtTableCache, authzCtx);
  }

  @Override
  public AnalysisResult analyze() {
    try {
      reader_ = CalciteMetadataHandler.createCalciteCatalogReader(stmtTableCache_,
          queryCtx_, queryCtx_.session.database);
      CalciteMetadataHandler.populateCalciteSchema(reader_, ctx_.getCatalog(),
          parsedStmt_.getTablesInQuery(null));

      typeFactory_ = new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());
      sqlValidator_ = SqlValidatorUtil.newValidator(
          ImpalaOperatorTable.getInstance(),
          reader_, typeFactory_,
          SqlValidator.Config.DEFAULT
              // Impala requires identifier expansion (tpcds test queries fail
              // without this)
              .withIdentifierExpansion(true)
              .withConformance(ImpalaConformance.INSTANCE)
              );
      validatedNode_ = sqlValidator_.validate(parsedStmt_.getParsedSqlNode());
      return new CalciteAnalysisResult(this);
    } catch (ImpalaException e) {
      return new CalciteAnalysisResult(this, e);
    } catch (CalciteContextException e) {
      return new CalciteAnalysisResult(this,
          new AnalysisException(e.getMessage(), e.getCause()));
    }
  }

  public Analyzer createAnalyzer(AnalysisContext ctx, StmtTableCache stmtTableCache,
      AuthorizationContext authzCtx) {
    return new SimplifiedAnalyzer(stmtTableCache, ctx.getQueryCtx(),
        ctx.getAuthzFactory(), null);
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory_;
  }

  public CalciteCatalogReader getCatalogReader() {
    return reader_;
  }

  public SqlValidator getSqlValidator() {
    return sqlValidator_;
  }

  public SqlNode getValidatedNode() {
    return validatedNode_;
  }

  public Analyzer getAnalyzer() {
    return analyzer_;
  }

  public ParsedStatement getParsedStmt() {
    return parsedStmt_;
  }
}
