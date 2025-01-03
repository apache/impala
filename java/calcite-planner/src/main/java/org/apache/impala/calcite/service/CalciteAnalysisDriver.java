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
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.calcite.schema.ImpalaCalciteCatalogReader;
import org.apache.impala.calcite.type.ImpalaTypeCoercionFactory;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.calcite.validate.ImpalaConformance;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ParseException;
import org.apache.impala.common.UnsupportedFeatureException;
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

  public ImpalaSqlValidatorImpl sqlValidator_;

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
      if (stmtTableCache_.needsAnyTableMasksInQuery_) {
        throw new UnsupportedFeatureException("Column masking and row filtering are " +
            "not yet supported by the Calcite planner.");
      }

      reader_ = CalciteMetadataHandler.createCalciteCatalogReader(stmtTableCache_,
          queryCtx_, queryCtx_.session.database);
      // When CalciteRelNodeConverter#convert() is called to convert the valid AST into a
      // logical plan, ViewTable#expandView() in Apache Calcite would be invoked if a
      // regular view is involved in the query. expandView() validates the SQL statement
      // defining the view. During the validation, all referenced tables by the regular
      // view are required. Thus, we need all the tables in 'stmtTableCache_'.
      // Recall that parsedStmt_.getTablesInQuery(null) only contains TableName's in the
      // given query but not the underlying tables referenced by a regular view.
      CalciteMetadataHandler.populateCalciteSchema(reader_, ctx_.getCatalog(),
          stmtTableCache_, analyzer_);

      typeFactory_ = new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());
      sqlValidator_ = new ImpalaSqlValidatorImpl(
          ImpalaOperatorTable.getInstance(),
          reader_, typeFactory_,
          SqlValidator.Config.DEFAULT
              // Impala requires identifier expansion (tpcds test queries fail
              // without this)
              .withIdentifierExpansion(true)
              .withConformance(ImpalaConformance.INSTANCE)
              .withTypeCoercionEnabled(true)
              .withTypeCoercionFactory(new ImpalaTypeCoercionFactory()),
          analyzer_);

      // Register the privilege requests for the tables directly referenced in the given
      // query as well as those for the tables and columns referenced by the views.
      // We do not pass stmtTableCache_.tables.keySet() to registerPrivReqsInTables()
      // because given a table from stmtTableCache_.tables.keySet(), we do not know
      // whether the table is referenced directly in the query or is referenced by a view
      // in the query.
      registerPrivReqsInTables(parsedStmt_.getTablesInQuery(/* loader */ null),
          /* shouldMaskPrivChecks */ false, stmtTableCache_.catalog, sqlValidator_);

      // sqlValidator_.validate() would also register privilege requests for the columns
      // directly referenced in the query. We do this after the above call because in the
      // case when some columns directly referenced in the query could not be resolved,
      // we still need to register the privilege requests for tables and columns
      // referenced by views assuming that those tables and columns referenced by views
      // could be successfully resolved.
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

  /**
   * This method registers the privilege requests for the tables referenced in the given
   * query as well as the tables and columns referenced by the views in the given query.
   */
  private void registerPrivReqsInTables(Set<TableName> tableNamesInQuery,
      boolean shouldMaskPrivChecks, FeCatalog catalog, ImpalaSqlValidatorImpl validator)
      throws ParseException {

    for (TableName tableName : tableNamesInQuery) {
      FeTable feTable = registerTablePrivReq(tableName, catalog);

      if (feTable instanceof FeView) {
        String sql = ((FeView) feTable).getQueryStmt().toSql();
        CalciteQueryParser queryParser = new CalciteQueryParser(sql);
        SqlNode parsedSqlNode = queryParser.parse();
        CalciteMetadataHandler.TableVisitor tableVisitor =
            new CalciteMetadataHandler.TableVisitor(/* currentDb */ "default");
        parsedSqlNode.accept(tableVisitor);

        boolean childViewCreatedBySuperuser =
            !PrivilegeRequestBuilder.isViewCreatedByNonSuperuser(feTable);
        // Recall that 'shouldMaskPrivChecks' denotes whether we should mask the
        // privilege requests during privilege request registration for the given
        // TableName's. We should mask the privilege requests for a child view if
        // 'shouldMaskPrivChecks' is true, or the child view was created by a superuser.
        // This matches what the classic Impala frontend does. Refer to
        // InlineViewRef#analyze() for what the classic Impala frontend does to analyze
        // a regular view (or a view whose isCatalogView() evaluates to true) and to
        // IMPALA-10122 (Part 2) for further details.
        if (shouldMaskPrivChecks || childViewCreatedBySuperuser) {
          // This sets 'maskPrivChecks_' of 'analyzer_' to true so that during the
          // following calls to validator.validate() and registerTablePrivReq() in the
          // recursive call, the privilege requests would be registered as masked ones,
          // which allows the requesting user to SELECT the requested columns and tables
          // even though the requesting user does not have the SELECT privilege on them.
          analyzer_.setMaskPrivChecks(null);
        }
        // Register privilege requests for columns referenced by the child view.
        validator.validate(parsedSqlNode);

        // Recurse if 'feTable' is also a view. Note that the privilege requests for the
        // tables referenced by 'feTable' will be registered within the recursive call.
        registerPrivReqsInTables(tableVisitor.tableNames_,
            shouldMaskPrivChecks || childViewCreatedBySuperuser, catalog, validator);

        // Set 'maskPrivChecks_' back to false in this case because we do not know if
        // other child views were created by a superuser too.
        if (!shouldMaskPrivChecks && childViewCreatedBySuperuser) {
          analyzer_.unsetMaskPrivChecks();
        }
      }
    }
  }

  private FeTable registerTablePrivReq(TableName tableName, FeCatalog catalog) {
    FeTable feTable = getFeTable(tableName, catalog);
    if (feTable == null) {
      analyzer_.registerPrivReq(builder -> {
        builder.onTableUnknownOwner(tableName.getDb(), tableName.getTbl())
            .allOf(Privilege.SELECT);
        return builder.build();
      });
    } else {
      analyzer_.registerAuthAndAuditEvent(feTable, Privilege.SELECT);
    }
    return feTable;
  }

  private FeTable getFeTable(TableName tableName, FeCatalog catalog) {
    FeDb db = catalog.getDb(tableName.getDb());
    if (db == null) {
      return null;
    }
    return db.getTable(tableName.getTbl());
  }
}
