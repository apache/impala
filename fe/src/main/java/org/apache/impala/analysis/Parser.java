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

import java.io.StringReader;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TQueryOptions;

/**
 * Wrapper around the generated SQL parser. Translates output to
 * the expected class and exceptions to an ParseException.
 */
public class Parser {
  @SuppressWarnings("serial")
  public static class ParseException extends AnalysisException {
    public ParseException(String msg, Exception e) {
      super(msg, e);
    }
  }

  public static StatementBase parse(String query) throws ParseException {
    return parse(query, new TQueryOptions());
  }

  public static StatementBase parse(String query, TQueryOptions options)
      throws ParseException {
    SqlScanner input = new SqlScanner(new StringReader(query));
    SqlParser parser = new SqlParser(input);
    parser.setQueryOptions(options);
    try {
      return (StatementBase) parser.parse().value;
    } catch (Exception e) {
      // Annoyingly, the parser uses a generic Exception to report
      // errors. Translate it to something more helpful.
      throw new ParseException(parser.getErrorMsg(query), e);
    }
  }
}
