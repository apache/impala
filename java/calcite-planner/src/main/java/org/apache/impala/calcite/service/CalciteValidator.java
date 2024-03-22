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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.calcite.validate.ImpalaConformance;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SqlValidator. Responsible for validating the parsed SQL AST.
 */
public class CalciteValidator implements CompilerStep {
  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteValidator.class.getName());

  private final CalciteMetadataHandler mdHandler;
  private final CalciteJniFrontend.QueryContext queryCtx;
  private final RelDataTypeFactory typeFactory;
  private final CalciteCatalogReader catalogReader;
  private final SqlValidator sqlValidator;

  public CalciteValidator(CalciteMetadataHandler mdHandler,
      CalciteJniFrontend.QueryContext queryCtx) {
    this.mdHandler = mdHandler;
    this.queryCtx = queryCtx;
    this.typeFactory = new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());
    this.catalogReader = mdHandler.getCalciteCatalogReader();

    this.sqlValidator = SqlValidatorUtil.newValidator(
        ImpalaOperatorTable.getInstance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT
            .withConformance(ImpalaConformance.INSTANCE)
            );
  }

  public SqlNode validate(SqlNode parsedNode) {
    // Validate the initial AST
    SqlNode node = sqlValidator.validate(parsedNode);
    return node;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public SqlValidator getSqlValidator() {
    return sqlValidator;
  }

  public CalciteCatalogReader getCatalogReader() {
    return catalogReader;
  }

  @Override
  public void logDebug(Object resultObject) {
    if (!(resultObject instanceof SqlNode)) {
      LOG.debug("Finished validator step, but unknown result: " + resultObject);
      return;
    }
    LOG.debug("Validated node: " + resultObject);
  }
}
