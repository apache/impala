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
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.SelectListItem;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumnValue;

import java.util.List;
import java.util.stream.Collectors;

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
    Preconditions.checkArgument(timestampExpr.isConstant());
    return utcTimestampToUnixTimeMicros(analyzer,
        toUtcTimestampExpr(analyzer, timestampExpr, null));
  }

  /**
   * Converts a timestamp in local timezone to UTC, then to UNIX microseconds.
   * If expectPreIfNonUnique is null, expect the conversion to be unique and returns
   * the unique value, otherwise it returns null.
   * In cases of ambiguous conversion (e.g., when the timestamp falls within the DST
   * repeated interval), if 'expectPreIfNonUnique' is true, it returns the previous
   * possible value, otherwise it returns the posterior possible value.
   * In cases of invalid conversion (e.g., when the timestamp falls within the DST
   * skipped interval), if 'expectPreIfNonUnique' is true, it returns the transition point
   * value, otherwise it returns null.
   */
  public static Long localTimestampToUnixTimeMicros(Analyzer analyzer, Expr timestampExpr,
      Boolean expectPreIfNonUnique) throws AnalysisException, InternalException {
    Preconditions.checkArgument(timestampExpr.isConstant());
    Expr toUtcTimestampExpr = toUtcTimestampExpr(analyzer, timestampExpr,
        expectPreIfNonUnique);
    Expr toUnixTimeExpr = new FunctionCallExpr("utc_to_unix_micros",
        Lists.newArrayList(toUtcTimestampExpr));
    toUnixTimeExpr.analyze(analyzer);
    TColumnValue result = FeSupport.EvalExprWithoutRow(toUnixTimeExpr,
        analyzer.getQueryCtx());
    if (!result.isSetLong_val()) return null;
    return result.getLong_val();
  }

  /**
   * Converts a timestamp in local timezone to string value.
   */
  public static String localTimestampToString(Analyzer analyzer, Expr timestampExpr)
      throws AnalysisException, InternalException {
    Preconditions.checkArgument(timestampExpr.isConstant());
    return utcTimestampToSpecifiedTimeZoneTimestamp(analyzer,
        toUtcTimestampExpr(analyzer, timestampExpr, null));
  }

  /**
   * Wraps 'timestampExpr' in to_utc_timestamp() that converts it to UTC from local
   * time zone. If the conversion is ambigious, the earlier/later result will be returned
   * based on 'expectPreIfNonUnique'.
   */
  public static Expr toUtcTimestampExpr(Analyzer analyzer, Expr timestampExpr,
      Boolean expectPreIfNonUnique) throws AnalysisException {
    Preconditions.checkArgument(timestampExpr.isAnalyzed());
    Preconditions.checkArgument(timestampExpr.getType() == Type.TIMESTAMP);
    List<Expr> params = Lists.newArrayList(timestampExpr,
        new StringLiteral(analyzer.getQueryCtx().getLocal_time_zone()));
    if (expectPreIfNonUnique != null) {
      params.add(new BoolLiteral(expectPreIfNonUnique));
    }
    FunctionCallExpr toUtcTimestamp = new FunctionCallExpr("to_utc_timestamp", params);
    toUtcTimestamp.setIsInternalFnCall(true);
    toUtcTimestamp.analyze(analyzer);
    return toUtcTimestamp;
  }

  // Compute total cost for a list of expressions. Return 0 for a null list.
  public static float computeExprsTotalCost(List<? extends Expr> exprs) {
    // TODO: Implement the cost for conjunts once the implemetation for
    // 'Expr' is in place.
    if (exprs == null) return 0;
    if (BackendConfig.INSTANCE.isProcessingCostUseEqualExprWeight()) {
      return exprs.size();
    } else {
      float totalCost = 0;
      for (Expr e : exprs) {
        totalCost += e.hasCost() ? e.getCost() : 1;
      }
      return totalCost;
    }
  }

  public static float computeExprCost(Expr e) {
    if (e == null) return 0;
    if (BackendConfig.INSTANCE.isProcessingCostUseEqualExprWeight()) {
      return 1;
    } else {
      return e.hasCost() ? e.getCost() : 1;
    }
    // TODO Implement a function that can take into consideration of data types,
    // expressions and potentially LLVM translation in BE. The function must also
    // run fast.
  }

  public static List<SelectListItem> exprsAsSelectList(List<Expr> exprs) {
    return exprs.stream().map(
        e -> new SelectListItem(e, null)).collect(Collectors.toList());
  }
}
