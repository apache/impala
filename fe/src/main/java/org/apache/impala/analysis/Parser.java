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

  /**
   * Parse the statement using default options. Used for testing and for
   * parsing internally-generated statements. See the full version for details.
   */
  public static StatementBase parse(String query) throws AnalysisException {
    return parse(query, new TQueryOptions());
  }

  /**
   * Run the parser with the given input and options, returning an
   * abstract syntax tree (AST) for the statement, ready for semantic analysis.
   *
   * @param query the query: a valid SQL statement
   * @param options query options
   * @return the AST for the statement
   * @throws AnalysisException for errors which are of two kinds:
   * {@link ParseException} for syntactic errors, or the generic
   * AnalysisException for semantic errors (such as overflow of a
   * numeric literal)
   */
  public static StatementBase parse(String query, TQueryOptions options)
      throws AnalysisException {
    SqlScanner input = new SqlScanner(new StringReader(query));
    SqlParser parser = new SqlParser(input);
    parser.setQueryOptions(options);
    try {
      return (StatementBase) parser.parse().value;
    } catch (AnalysisException e) {
      // Pass along any analysis exceptions as they are. Since AnalysisException
      // is a kind of Exception, we have to special-case it here
      throw e;
    } catch (Exception e) {
      // Annoyingly, the parser uses a generic Exception to report
      // syntax errors. Translate it to something more helpful.
      throw new ParseException(parser.getErrorMsg(query), e);
    }
  }
}
