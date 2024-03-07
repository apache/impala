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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.impala.calcite.parser.ImpalaSqlParserImpl;
import org.apache.impala.calcite.validate.ImpalaConformance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CalciteQueryParser. Responsible for turning a String query statement into
 * a Calcite SqlNode.
 */
public class CalciteQueryParser implements CompilerStep {
  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteQueryParser.class.getName());

  private final CalciteJniFrontend.QueryContext queryCtx_;

  public CalciteQueryParser(CalciteJniFrontend.QueryContext queryCtx) {
    this.queryCtx_ = queryCtx;
  }

  public SqlNode parse() throws SqlParseException {
    // Create an SQL parser
    SqlParser parser = SqlParser.create(queryCtx_.getStmt(),
        SqlParser.config().withParserFactory(ImpalaSqlParserImpl.FACTORY)
                .withConformance(ImpalaConformance.INSTANCE));

    // Parse the query into an AST
    SqlNode sqlNode = parser.parseQuery();
    return sqlNode;
  }

  public void logDebug(Object resultObject) {
    if (!(resultObject instanceof SqlNode)) {
      LOG.debug("Parser produced an unknown output: " + resultObject);
      return;
    }
    LOG.debug("Parsed node: " + resultObject);
  }
}
