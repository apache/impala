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

package com.cloudera.impala.analysis;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TDistributeByHashParam;
import com.cloudera.impala.thrift.TDistributeByRangeParam;
import com.cloudera.impala.thrift.TDistributeParam;
import com.cloudera.impala.thrift.TDistributeType;
import com.cloudera.impala.thrift.TRangeLiteral;
import com.cloudera.impala.thrift.TRangeLiteralList;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents the information of
 *
 * DISTRIBUTE BY HASH[(col_def_list)] INTO n BUCKETS
 * DISTRIBUTE BY RANGE[(col_def_list)] SPLIT ROWS ( (v1,v2,v3), ...)
 *
 * clauses in CREATE TABLE statements, where available, e.g. Kudu.
 *
 * A table can be hash or range partitioned, or combinations of both. A distribute
 * clause represents one particular distribution rule. For both HASH and RANGE types,
 * some of the error checking is done during the analysis, but most of it is deferred
 * until the table is actually created.
  */
public class DistributeParam implements ParseNode {

  /**
   * Creates a DistributeParam partitioned by hash.
   */
  public static DistributeParam createHashParam(List<String> cols, BigDecimal buckets) {
    return new DistributeParam(Type.HASH, cols, buckets);
  }

  /**
   * Creates a DistributeParam partitioned by range.
   */
  public static DistributeParam createRangeParam(List<String> cols,
      ArrayList<ArrayList<LiteralExpr>> splitRows) {
    return new DistributeParam(Type.RANGE, cols, splitRows);
  }

  private static final int NO_BUCKETS = -1;

  /**
   * The type of the distribution rule.
   */
  public enum Type {
    HASH, RANGE
  };

  private List<String> columns_;

  private final Type type_;

  // Only relevant for hash partitioning, -1 otherwise
  private final int num_buckets_;

  // Only relevant for range partitioning, null otherwise
  private final ArrayList<ArrayList<LiteralExpr>> splitRows_;

  // Set in analyze()
  private TDistributeByRangeParam rangeParam_;

  private DistributeParam(Type t, List<String> cols, BigDecimal buckets) {
    type_ = t;
    columns_ = cols;
    num_buckets_ = buckets.intValue();
    splitRows_ = null;
  }

  private DistributeParam(Type t, List<String> cols,
      ArrayList<ArrayList<LiteralExpr>> splitRows) {
    type_ = t;
    columns_ = cols;
    splitRows_ = splitRows;
    num_buckets_ = NO_BUCKETS;
  }

  /**
   * TODO Refactor the logic below to analyze 'columns_'. This analysis should output
   * a vector of column types that would then be used during the analysis of the split
   * rows.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (type_ == Type.HASH && num_buckets_ <= 1) {
      throw new AnalysisException(String.format(
          "Number of buckets in DISTRIBUTE BY clause '%s' must be larger than 1.",
          toSql()));
    } else if (type_ == Type.RANGE) {
      // Creating the thrift structure simultaneously checks for semantic errors
      rangeParam_ = new TDistributeByRangeParam();
      rangeParam_.setColumns(columns_);

      for (ArrayList<LiteralExpr> splitRow : splitRows_) {
        TRangeLiteralList list = new TRangeLiteralList();
        if (splitRow.size() != columns_.size()) {
          throw new AnalysisException(String.format(
              "SPLIT ROWS has different size than number of projected key columns: %d. "
                  + "Split row: %s", columns_.size(), splitRowToString(splitRow)));
        }
        for (LiteralExpr expr : splitRow) {
          expr.analyze(analyzer);
          TRangeLiteral literal = new TRangeLiteral();
          if (expr instanceof NumericLiteral) {
            NumericLiteral num = (NumericLiteral) expr;
            if (num.getType().isDecimal() || num.getType().isFloatingPointType()) {
              throw new AnalysisException("Only integral and string values allowed for" +
                  " split rows.");
            } else {
              literal.setInt_literal(num.getIntValue());
            }
          } else if (expr instanceof StringLiteral) {
            StringLiteral string = (StringLiteral) expr;
            literal.setString_literal(string.getStringValue());
          } else if (expr instanceof BoolLiteral) {
            BoolLiteral bool = (BoolLiteral) expr;
            literal.setBool_literal(bool.getValue());
          } else {
            throw new AnalysisException(String.format("Split row value is not supported: "
                + "%s (Type: %s).", expr.getStringValue(), expr.getType().toSql()));
          }
          list.addToValues(literal);
        }
        rangeParam_.addToSplit_rows(list);
      }
    }
  }

  @Override
  public String toSql() {
    if (num_buckets_ == NO_BUCKETS) {
      StringBuilder builder = new StringBuilder();
      for (ArrayList<LiteralExpr> splitRow : splitRows_) {
        splitRowToString(splitRow);
      }
      return String.format("RANGE(%s) INTO RANGES(%s)", Joiner.on(", ").join(columns_),
          builder.toString());
    } else {
      return String.format("HASH(%s) INTO %d BUCKETS", Joiner.on(", ").join(columns_),
          num_buckets_);
    }
  }

  private String splitRowToString(ArrayList<LiteralExpr> splitRow) {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    List<String> rangeElementStrings = Lists.newArrayList();
    for (LiteralExpr rangeElement : splitRow) {
      rangeElementStrings.add(rangeElement.getStringValue());
    }
    builder.append(Joiner.on(",").join(rangeElementStrings));
    builder.append("]");
    return builder.toString();
  }

  TDistributeParam toThrift() {
    TDistributeParam result = new TDistributeParam();
    if (type_ == Type.HASH) {
      TDistributeByHashParam hash = new TDistributeByHashParam();
      hash.setNum_buckets(num_buckets_);
      hash.setColumns(columns_);
      result.setBy_hash_param(hash);
    } else {
      Preconditions.checkState(type_ == Type.RANGE);

      result.setBy_range_param(rangeParam_);
    }
    return result;
  }

  public List<String> getColumns() { return columns_; }
  public void setColumns(List<String> cols) { columns_ = cols; }
  public Type getType_() { return type_; }
  public int getNumBuckets() { return num_buckets_; }
}
