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

package org.apache.impala.util;

import com.google.common.base.Preconditions;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumnValue;

public class ExprUtil {
  /**
   * Converts a UTC timestamp to UNIX microseconds.
   */
  public static long utcTimestampToUnixTimeMicros(Analyzer analyzer, Expr timestampExpr)
      throws AnalysisException, InternalException {
    Preconditions.checkArgument(timestampExpr.isAnalyzed());
    Preconditions.checkArgument(timestampExpr.isConstant());
    Preconditions.checkArgument(timestampExpr.getType() == Type.TIMESTAMP);
    Expr toUnixTimeExpr = new FunctionCallExpr("utc_to_unix_micros",
        Lists.newArrayList(timestampExpr));
    toUnixTimeExpr.analyze(analyzer);
    TColumnValue result = FeSupport.EvalExprWithoutRow(toUnixTimeExpr,
        analyzer.getQueryCtx());
    if (!result.isSetLong_val()) {
      throw new InternalException("Error converting timestamp expression: " +
          timestampExpr.debugString());
    }
    return result.getLong_val();
  }

  /**
   * Converts a UTC timestamp to string value for a specified time zone.
   */
  public static String utcTimestampToSpecifiedTimeZoneTimestamp(Analyzer analyzer,
      Expr timestampExpr) throws AnalysisException, InternalException {
    Preconditions.checkArgument(timestampExpr.isAnalyzed());
    Preconditions.checkArgument(timestampExpr.isConstant());
    Preconditions.checkArgument(timestampExpr.getType() == Type.TIMESTAMP);
    Expr fromUtcTimestampExpr = new FunctionCallExpr("from_utc_timestamp", Lists
        .newArrayList(timestampExpr,
            new StringLiteral(analyzer.getQueryCtx().getLocal_time_zone())));
    fromUtcTimestampExpr.analyze(analyzer);
    TColumnValue result = FeSupport
        .EvalExprWithoutRow(fromUtcTimestampExpr, analyzer.getQueryCtx());
    if (!result.isSetString_val()) {
      throw new InternalException("Error converting timestamp expression: " +
          timestampExpr.debugString());
    }
    return result.getString_val();
  }

  /**
   * Converts a timestamp in local timezone to UTC, then to UNIX microseconds.
   */
  public static long localTimestampToUnixTimeMicros(Analyzer analyzer, Expr timestampExpr)
      throws AnalysisException, InternalException {
    return utcTimestampToUnixTimeMicros(analyzer,
        toUtcTimestampExpr(analyzer, timestampExpr));
  }


  /**
   * Converts a timestamp in local timezone to string value.
   */
  public static String localTimestampToString(Analyzer analyzer, Expr timestampExpr)
      throws AnalysisException, InternalException {
    return utcTimestampToSpecifiedTimeZoneTimestamp(analyzer,
        toUtcTimestampExpr(analyzer, timestampExpr));
  }

  private static Expr toUtcTimestampExpr(Analyzer analyzer, Expr timestampExpr)
      throws AnalysisException {
    Preconditions.checkArgument(timestampExpr.isAnalyzed());
    Preconditions.checkArgument(timestampExpr.isConstant());
    Preconditions.checkArgument(timestampExpr.getType() == Type.TIMESTAMP);
    Expr toUtcTimestamp = new FunctionCallExpr("to_utc_timestamp",
        Lists.newArrayList(timestampExpr,
            new StringLiteral(analyzer.getQueryCtx().getLocal_time_zone())));
    toUtcTimestamp.analyze(analyzer);
    return toUtcTimestamp;
  }
}
