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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TDistributeByHashParam;
import org.apache.impala.thrift.TDistributeByRangeParam;
import org.apache.impala.thrift.TDistributeParam;
import org.apache.impala.thrift.TRangeLiteral;
import org.apache.impala.thrift.TRangeLiteralList;
import org.apache.impala.util.KuduUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
  public static DistributeParam createHashParam(List<String> cols, int buckets) {
    return new DistributeParam(Type.HASH, cols, buckets, null);
  }

  /**
   * Creates a DistributeParam partitioned by range.
   */
  public static DistributeParam createRangeParam(List<String> cols,
      List<List<LiteralExpr>> splitRows) {
    return new DistributeParam(Type.RANGE, cols, NO_BUCKETS, splitRows);
  }

  private static final int NO_BUCKETS = -1;

  /**
   * The type of the distribution rule.
   */
  public enum Type {
    HASH, RANGE
  }

  // May be empty indicating that all keys in the table should be used.
  private final List<String> colNames_ = Lists.newArrayList();

  // Map of primary key column names to the associated column definitions. Must be set
  // before the call to analyze().
  private Map<String, ColumnDef> pkColumnDefByName_;

  // Distribution type
  private final Type type_;

  // Only relevant for hash partitioning, -1 otherwise
  private final int numBuckets_;

  // Only relevant for range partitioning, null otherwise
  private final List<List<LiteralExpr>> splitRows_;

  private DistributeParam(Type t, List<String> colNames, int buckets,
      List<List<LiteralExpr>> splitRows) {
    type_ = t;
    for (String name: colNames) colNames_.add(name.toLowerCase());
    numBuckets_ = buckets;
    splitRows_ = splitRows;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(!colNames_.isEmpty());
    Preconditions.checkNotNull(pkColumnDefByName_);
    Preconditions.checkState(!pkColumnDefByName_.isEmpty());
    // Validate the columns specified in the DISTRIBUTE BY clause
    for (String colName: colNames_) {
      if (!pkColumnDefByName_.containsKey(colName)) {
        throw new AnalysisException(String.format("Column '%s' in '%s' is not a key " +
            "column. Only key columns can be used in DISTRIBUTE BY.", colName, toSql()));
      }
    }

    if (type_ == Type.RANGE) {
      for (List<LiteralExpr> splitRow : splitRows_) {
        if (splitRow.size() != colNames_.size()) {
          throw new AnalysisException(String.format(
              "SPLIT ROWS has different size than number of projected key columns: %d. "
                  + "Split row: %s", colNames_.size(), splitRowToString(splitRow)));
        }
        for (int i = 0; i < splitRow.size(); ++i) {
          LiteralExpr expr = splitRow.get(i);
          ColumnDef colDef = pkColumnDefByName_.get(colNames_.get(i));
          org.apache.impala.catalog.Type colType = colDef.getType();
          Preconditions.checkState(KuduUtil.isSupportedKeyType(colType));
          expr.analyze(analyzer);
          org.apache.impala.catalog.Type exprType = expr.getType();
          if (exprType.isNull()) {
            throw new AnalysisException("Split values cannot be NULL. Split row: " +
                splitRowToString(splitRow));
          }
          if (!org.apache.impala.catalog.Type.isImplicitlyCastable(exprType, colType,
              true)) {
            throw new AnalysisException(String.format("Split value %s (type: %s) is " +
                "not type compatible with column '%s' (type: %s).", expr.toSql(),
                exprType, colDef.getColName(), colType.toSql()));
          }
        }
      }
    }
  }

  @Override
  public String toSql() {
    StringBuilder builder = new StringBuilder(type_.toString());
    if (!colNames_.isEmpty()) {
      builder.append(" (");
      Joiner.on(", ").appendTo(builder, colNames_).append(")");
    }
    if (type_ == Type.HASH) {
      builder.append(" INTO ");
      Preconditions.checkState(numBuckets_ != NO_BUCKETS);
      builder.append(numBuckets_).append(" BUCKETS");
    } else {
      builder.append(" SPLIT ROWS (");
      if (splitRows_ == null) {
        builder.append("...");
      } else {
        for (List<LiteralExpr> splitRow: splitRows_) {
          builder.append(splitRowToString(splitRow));
        }
      }
      builder.append(")");
    }
    return builder.toString();
  }

  @Override
  public String toString() { return toSql(); }

  private String splitRowToString(List<LiteralExpr> splitRow) {
    StringBuilder builder = new StringBuilder("(");
    for (LiteralExpr expr: splitRow) {
      if (builder.length() > 1) builder.append(", ");
      builder.append(expr.toSql());
    }
    return builder.append(")").toString();
  }

  public TDistributeParam toThrift() {
    TDistributeParam result = new TDistributeParam();
    // TODO: Add a validate() function to ensure the validity of distribute params.
    if (type_ == Type.HASH) {
      TDistributeByHashParam hash = new TDistributeByHashParam();
      Preconditions.checkState(numBuckets_ != NO_BUCKETS);
      hash.setNum_buckets(numBuckets_);
      hash.setColumns(colNames_);
      result.setBy_hash_param(hash);
    } else {
      Preconditions.checkState(type_ == Type.RANGE);
      TDistributeByRangeParam rangeParam = new TDistributeByRangeParam();
      rangeParam.setColumns(colNames_);
      if (splitRows_ == null) {
        result.setBy_range_param(rangeParam);
        return result;
      }
      for (List<LiteralExpr> splitRow : splitRows_) {
        TRangeLiteralList list = new TRangeLiteralList();
        for (int i = 0; i < splitRow.size(); ++i) {
          LiteralExpr expr = splitRow.get(i);
          TRangeLiteral literal = new TRangeLiteral();
          if (expr instanceof NumericLiteral) {
            literal.setInt_literal(((NumericLiteral)expr).getIntValue());
          } else {
            String exprValue = expr.getStringValue();
            Preconditions.checkState(!Strings.isNullOrEmpty(exprValue));
            literal.setString_literal(exprValue);
          }
          list.addToValues(literal);
        }
        rangeParam.addToSplit_rows(list);
      }
      result.setBy_range_param(rangeParam);
    }
    return result;
  }

  void setPkColumnDefMap(Map<String, ColumnDef> pkColumnDefByName) {
    pkColumnDefByName_ = pkColumnDefByName;
  }

  boolean hasColumnNames() { return !colNames_.isEmpty(); }

  void setColumnNames(Collection<String> colNames) {
    Preconditions.checkState(colNames_.isEmpty());
    colNames_.addAll(colNames);
  }

  public Type getType() { return type_; }
}
