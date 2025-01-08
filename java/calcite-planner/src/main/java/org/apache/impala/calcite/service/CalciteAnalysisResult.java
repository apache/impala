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

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.AnalysisDriver;
import org.apache.impala.common.ImpalaException;

/**
 * CalciteAnalysisResult is an AnalysisResult with added analysis result
 * members produced by the CalciteAnalyzer
 */
public class CalciteAnalysisResult extends AnalysisResult {
  // Calcite AST
  private final SqlNode validatedNode_;

  private final RelDataTypeFactory typeFactory_;

  private final SqlValidator sqlValidator_;

  // CalciteCatalogReader is a context class that holds global information that
  // may be needed by the CalciteTable object
  private final CalciteCatalogReader reader_;

  public CalciteAnalysisResult(CalciteAnalysisDriver analysisDriver) {
    this(analysisDriver, null);
  }

  public CalciteAnalysisResult(CalciteAnalysisDriver analysisDriver,
      ImpalaException e) {
    super(analysisDriver.getParsedStmt(), analysisDriver.getAnalyzer(), e);
    validatedNode_ = analysisDriver.getValidatedNode();
    typeFactory_ = analysisDriver.getTypeFactory();
    sqlValidator_ = analysisDriver.getSqlValidator();
    reader_ = analysisDriver.getCatalogReader();
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

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory_;
  }


}
