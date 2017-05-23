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

import java.math.BigInteger;
import java.util.List;

import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TRangePartition;
import org.apache.impala.util.KuduUtil;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a range partition of a Kudu table.
 *
 * The following cases are supported:
 * - Bounded on both ends:
 *   PARTITION l_val <[=] VALUES <[=] u_val
 * - Unbounded lower:
 *   PARTITION VALUES <[=] u_val
 * - Unbounded upper:
 *   PARTITION l_val <[=] VALUES
 * - Single value (no range):
 *   PARTITION VALUE = val
 * - Multi-value:
 *   PARTITION VALUE = (val1, val2, ..., valn)
 *
 * Internally, all these cases are represented using the quadruplet:
 * [(l_val1,..., l_valn), l_bound_type, (u_val1,..., u_valn), u_bound_type],
 * where l_bound_type, u_bound_type are boolean values indicating if the associated bounds
 * are inclusive (true) or exclusive (false).
 */
public class RangePartition implements ParseNode {

  // Upper and lower bound exprs contain literals of the target column type post-analysis.
  // For TIMESTAMPs those are Kudu UNIXTIME_MICROS, i.e. int64s.
  private final List<Expr> lowerBound_;
  private final List<Expr> upperBound_;
  private final boolean lowerBoundInclusive_;
  private final boolean upperBoundInclusive_;
  private final boolean isSingletonRange_;

  // Set true when this partition has been analyzed.
  private boolean isAnalyzed_ = false;

  private RangePartition(List<Expr> lowerBoundValues, boolean lowerBoundInclusive,
      List<Expr> upperBoundValues, boolean upperBoundInclusive) {
    Preconditions.checkNotNull(lowerBoundValues);
    Preconditions.checkNotNull(upperBoundValues);
    Preconditions.checkState(!lowerBoundValues.isEmpty() || !upperBoundValues.isEmpty());
    lowerBound_ = lowerBoundValues;
    lowerBoundInclusive_ = lowerBoundInclusive;
    upperBound_ = upperBoundValues;
    upperBoundInclusive_ = upperBoundInclusive;
    isSingletonRange_ = (upperBoundInclusive && lowerBoundInclusive
        && lowerBoundValues == upperBoundValues);
  }

  /**
   * Constructs a range partition. The range is specified in the CREATE TABLE statement
   * using the 'PARTITION <expr> OP VALUES OP <expr>' clause. 'lower' corresponds to
   * the '<expr> OP' pair which defines an optional lower bound. 'upper' corresponds to
   * the 'OP <expr>' pair which defines an optional upper bound. Since only '<' and
   * '<=' operators are allowed, operators are represented with boolean values that
   * indicate inclusive or exclusive bounds.
   */
  public static RangePartition createFromRange(Pair<Expr, Boolean> lower,
      Pair<Expr, Boolean> upper) {
    List<Expr> lowerBoundExprs = Lists.newArrayListWithCapacity(1);
    boolean lowerBoundInclusive = false;
    List<Expr> upperBoundExprs = Lists.newArrayListWithCapacity(1);
    boolean upperBoundInclusive = false;
    if (lower != null) {
      lowerBoundExprs.add(lower.first);
      lowerBoundInclusive = lower.second;
    }
    if (upper != null) {
      upperBoundExprs.add(upper.first);
      upperBoundInclusive = upper.second;
    }
    return new RangePartition(lowerBoundExprs, lowerBoundInclusive, upperBoundExprs,
        upperBoundInclusive);
  }

  /**
   * Constructs a range partition from a set of values. The values are specified in the
   * CREATE TABLE statement using the 'PARTITION VALUE = <expr>' or the
   * 'PARTITION VALUE = (<expr>,...,<expr>)' clause. For both cases, the generated
   * range partition has the same lower and upper bounds.
   */
  public static RangePartition createFromValues(List<Expr> values) {
    return new RangePartition(values, true, values, true);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    throw new IllegalStateException("Not implemented");
  }

  public void analyze(Analyzer analyzer, List<ColumnDef> partColDefs)
      throws AnalysisException {
    // Reanalyzing not supported because TIMESTAMPs are converted to BIGINT (unixtime
    // micros) in place.
    Preconditions.checkArgument(!isAnalyzed_);
    analyzeBoundaryValues(lowerBound_, partColDefs, analyzer);
    if (!isSingletonRange_) {
      analyzeBoundaryValues(upperBound_, partColDefs, analyzer);
    }
    isAnalyzed_ = true;
  }

  private void analyzeBoundaryValues(List<Expr> boundaryValues,
      List<ColumnDef> partColDefs, Analyzer analyzer) throws AnalysisException {
    if (!boundaryValues.isEmpty()
        && boundaryValues.size() != partColDefs.size()) {
      throw new AnalysisException(String.format("Number of specified range " +
          "partition values is different than the number of partitioning " +
          "columns: (%d vs %d). Range partition: '%s'", boundaryValues.size(),
          partColDefs.size(), toSql()));
    }
    for (int i = 0; i < boundaryValues.size(); ++i) {
      LiteralExpr literal = analyzeBoundaryValue(boundaryValues.get(i),
          partColDefs.get(i), analyzer);
      Preconditions.checkNotNull(literal);
      boundaryValues.set(i, literal);
    }
  }

  private LiteralExpr analyzeBoundaryValue(Expr value, ColumnDef pkColumn,
      Analyzer analyzer) throws AnalysisException {
    try {
      value.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new AnalysisException(String.format("Only constant values are allowed " +
          "for range-partition bounds: %s", value.toSql()), e);
    }
    if (!value.isConstant()) {
      throw new AnalysisException(String.format("Only constant values are allowed " +
          "for range-partition bounds: %s", value.toSql()));
    }
    LiteralExpr literal = LiteralExpr.create(value, analyzer.getQueryCtx());
    if (literal == null) {
      throw new AnalysisException(String.format("Only constant values are allowed " +
          "for range-partition bounds: %s", value.toSql()));
    }
    if (literal.getType().isNull()) {
      throw new AnalysisException(String.format("Range partition values cannot be " +
          "NULL. Range partition: '%s'", toSql()));
    }
    org.apache.impala.catalog.Type colType = pkColumn.getType();
    Preconditions.checkState(KuduUtil.isSupportedKeyType(colType));

    // Special case string literals in timestamp columns for convenience.
    if (literal.getType().isStringType() && colType.isTimestamp()) {
      // Add an explicit cast to TIMESTAMP
      Expr e = new CastExpr(new TypeDef(Type.TIMESTAMP), literal);
      e.analyze(analyzer);
      literal = LiteralExpr.create(e, analyzer.getQueryCtx());
      Preconditions.checkNotNull(literal);
      if (literal.isNullLiteral()) {
        throw new AnalysisException(String.format("Range partition value %s cannot be " +
            "cast to target TIMESTAMP partitioning column.", value.toSql()));
      }
    }

    org.apache.impala.catalog.Type literalType = literal.getType();
    if (!org.apache.impala.catalog.Type.isImplicitlyCastable(literalType, colType,
        true)) {
      throw new AnalysisException(String.format("Range partition value %s " +
          "(type: %s) is not type compatible with partitioning column '%s' (type: %s).",
          literal.toSql(), literalType, pkColumn.getColName(), colType.toSql()));
    }
    if (!literalType.equals(colType)) {
      Expr castLiteral = literal.uncheckedCastTo(colType);
      Preconditions.checkNotNull(castLiteral);
      literal = LiteralExpr.create(castLiteral, analyzer.getQueryCtx());
    }
    Preconditions.checkNotNull(literal);

    // TODO: Remove when Impala supports a 64-bit TIMESTAMP type.
    if (colType.isTimestamp()) {
      try {
        long unixTimeMicros = KuduUtil.timestampToUnixTimeMicros(analyzer, literal);
        literal = new NumericLiteral(BigInteger.valueOf(unixTimeMicros), Type.BIGINT);
      } catch (InternalException e) {
        throw new AnalysisException(
            "Error converting timestamp in range definition: " + toSql(), e);
      }
    }
    return literal;
  }

  @Override
  public String toSql() {
    StringBuilder output = new StringBuilder();
    output.append("PARTITION ");
    if (isSingletonRange_) {
      output.append("VALUE = ");
      if (lowerBound_.size() > 1) output.append("(");
      List<String> literals = Lists.newArrayList();
      for (Expr literal: lowerBound_) literals.add(literal.toSql());
      output.append(Joiner.on(",").join(literals));
      if (lowerBound_.size() > 1) output.append(")");
    } else {
      if (!lowerBound_.isEmpty()) {
        Preconditions.checkState(lowerBound_.size() == 1);
        output.append(lowerBound_.get(0).toSql() + " " +
            (lowerBoundInclusive_ ? "<=" : "<"));
        output.append(" ");
      }
      output.append("VALUES");
      if (!upperBound_.isEmpty()) {
        Preconditions.checkState(upperBound_.size() == 1);
        output.append(" ");
        output.append((upperBoundInclusive_ ? "<=" : "<") + " " +
            upperBound_.get(0).toSql() );
      }
    }
    return output.toString();
  }

  public TRangePartition toThrift() {
    TRangePartition tRangePartition = new TRangePartition();
    for (Expr literal: lowerBound_) {
      tRangePartition.addToLower_bound_values(literal.treeToThrift());
    }
    if (!lowerBound_.isEmpty()) {
      tRangePartition.setIs_lower_bound_inclusive(lowerBoundInclusive_);
    }
    for (Expr literal: upperBound_) {
      tRangePartition.addToUpper_bound_values(literal.treeToThrift());
    }
    if (!upperBound_.isEmpty()) {
      tRangePartition.setIs_upper_bound_inclusive(upperBoundInclusive_);
    }
    Preconditions.checkState(tRangePartition.isSetLower_bound_values()
        || tRangePartition.isSetUpper_bound_values());
    return tRangePartition;
  }
}
