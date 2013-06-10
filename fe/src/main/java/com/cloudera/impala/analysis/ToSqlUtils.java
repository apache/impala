// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.Token;
import org.apache.hadoop.hive.ql.parse.HiveLexer;

/**
 * Contains utility methods for creating SQL strings, for example,
 * for creating identifier strings that are compatible with Hive or Impala.
 */
public class ToSqlUtils {
  /**
   * Given an unquoted identifier string, returns an identifier lexable by Hive, possibly
   * by enclosing the original identifier in "`" quotes.
   * For example, Hive cannot parse its own auto-generated column
   * names "_c0", "_c1" etc. unless they are quoted.
   *
   * Impala's lexer recognizes a superset of the unquoted identifiers that Hive can.
   * This method always returns an identifier that Impala can recognize, although for
   * some identifiers the quotes may not be strictly necessary for Impala.
   */
  public static String getHiveIdentSql(String ident) {
    HiveLexer hiveLexer = new HiveLexer(new ANTLRStringStream(ident));
    try {
      Token t = hiveLexer.nextToken();
      // Check that the lexer recognizes an identifier and then EOF.
      boolean identFound = t.getType() == HiveLexer.Identifier;
      t = hiveLexer.nextToken();
      // No enclosing quotes are necessary.
      if (identFound && t.getType() == HiveLexer.EOF) return ident;
    } catch (Exception e) {
      // Ignore exception and just quote the identifier to be safe.
    }
    // Hive's lexer does not recognize impalaIdent, so enclose it in quotes.
    return "`" + ident + "`";
  }
}
