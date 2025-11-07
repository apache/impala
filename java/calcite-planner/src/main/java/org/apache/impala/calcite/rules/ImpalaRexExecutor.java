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

package org.apache.impala.calcite.rules;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.TimestampString;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TQueryCtx;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaRexExecutor does the constant folding of Impala RexNode objects.
 */
public class ImpalaRexExecutor implements RexExecutor {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaRexExecutor.class.getName());

  private final Analyzer analyzer_;
  private final TQueryCtx queryCtx_;

  // Reducer class used for testing purposes for injection.
  private final Reducer reducer_;

  public ImpalaRexExecutor(Analyzer analyzer, TQueryCtx queryCtx,
      Reducer reducer) {
    analyzer_ = analyzer;
    queryCtx_ = queryCtx;
    reducer_ = reducer;
  }

  /**
   * The main routine that gets called when doing the constant folding. The
   * constExps contains the RexNodes that will be constant folded and the
   * reducedValues contain the RexNodes after constant folding. The size of these
   * two arrays must match at the end of this method.
   */
  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps,
      List<RexNode> reducedValues) {
    ReduceRexNodeShuttle shuttle =
        new ReduceRexNodeShuttle(analyzer_, queryCtx_, rexBuilder, reducer_);
    for (RexNode rexNode : constExps) {
      RexNode newProject = rexNode.accept(shuttle);
      if (rexNode.getType() != newProject.getType()) {
        newProject = rexBuilder.makeCast(rexNode.getType(), newProject,
            rexNode.getType().isNullable());
      }
      reducedValues.add(newProject);
    }
    Preconditions.checkState(reducedValues.size() == constExps.size());
  }

  private static boolean isReducible(RexNode rexNode) {
    // may already be reduced to a literal
    if (!(rexNode instanceof RexCall)) {
      return false;
    }

    RexCall call = (RexCall) rexNode;

    // cannot reduce interval operation by itself. An example of This will look like
    // *(14 INT : 86400000 INTERVAL) with a return type of INTERVAL. This rexCall
    // can be a parameter of some date time expression (e.g. time_add(time, interval))
    // which can be folded if the time portion is constant.
    if (SqlTypeUtil.isInterval(call.getType())) {
      return false;
    }

    // operands should all be literals or an implicit cast of a literal
    for (RexNode operand : call.getOperands()) {
      if (!isLiteralOrCastOfLiteral(operand) && !isIntervalConst(operand)) {
        return false;
      }
    }

    // special cast to ignore: An implicit cast from a decimal to a double
    // needs to be kept as/is. If there is a partition on the double column,
    // the exact value is needed. Converting to a double causes an inexact
    // value to be created and interferes with the partitioned directory name.
    if (isImplicitCastDecimalToInexact(call)) {
      return false;
    }

    return true;
  }

  public static boolean isImplicitCastDecimalToInexact(RexCall call) {
    if (call.getKind() != SqlKind.CAST) {
      return false;
    }
    SqlTypeName sqlTypeName = call.getType().getSqlTypeName();
    if (sqlTypeName != SqlTypeName.DOUBLE && sqlTypeName != SqlTypeName.FLOAT) {
      return false;
    }
    RexNode operand = call.getOperands().get(0);
    if (!(operand instanceof RexLiteral)) {
      return false;
    }
    if (operand.getType().getSqlTypeName() != SqlTypeName.DECIMAL) {
      return false;
    }
    return true;
  }

  private static boolean isLiteralOrCastOfLiteral(RexNode operand) {
    while ((operand instanceof RexCall) &&
        ((RexCall) operand).getKind() == SqlKind.CAST) {
      operand = ((RexCall) operand).getOperands().get(0);
    }
    return (operand instanceof RexLiteral);
  }

  private static boolean isIntervalConst(RexNode operand) {
    if (!(operand instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) operand;
    if (!call.getKind().equals(SqlKind.TIMES)) {
      return false;
    }
    if (!SqlTypeUtil.isInterval(call.getType())) {
      return false;
    }
    if (!(call.getOperands().get(0) instanceof RexLiteral) &&
      !(call.getOperands().get(1) instanceof RexLiteral)) {
      return false;
    }
    return true;
  }

  private static RexNode getRexNodeFromColumnValue(RexBuilder builder, RexNode constExp,
      TColumnValue colVal) {
    RelDataType returnType = constExp.getType();
    if (colVal.isSetBool_val()) {
      return builder.makeLiteral(colVal.bool_val, returnType);
    } else if (colVal.isSetByte_val()) {
      return builder.makeLiteral(colVal.byte_val, returnType);
    } else if (colVal.isSetShort_val()) {
      return builder.makeLiteral(colVal.short_val, returnType);
    } else if (colVal.isSetInt_val()) {
      return builder.makeLiteral(colVal.int_val, returnType);
    } else if (colVal.isSetLong_val()) {
      return builder.makeLiteral(colVal.long_val, returnType);
    } else if (colVal.isSetDouble_val()) {
      return builder.makeLiteral(colVal.double_val, returnType);
    } else if (colVal.isSetString_val()) {
      if (returnType.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
        RexNode timestamp = builder.makeTimestampLiteral(
            new TimestampString(colVal.string_val),
            constExp.getType().getPrecision());
        RelDataType type = builder.getTypeFactory().createTypeWithNullability(
            timestamp.getType(), constExp.getType().isNullable());
        return builder.makeCast(type, timestamp, true);
      }
      // decimal is in string val, strings are in binaryVal
      return builder.makeLiteral(new BigDecimal(colVal.string_val), returnType, true);
    } else if (colVal.isSetBinary_val()) {
      byte[] bytes = new byte[colVal.binary_val.remaining()];
      colVal.binary_val.get(bytes);

      // Converting strings between the BE/FE does not work properly for the
      // extended ASCII characters above 127. Bail in such cases to avoid
      // producing incorrect results.
      for (byte b: bytes) {
        if (b < 0) {
          return constExp;
        }
      }
      try {
        String newString = new String(bytes, "US-ASCII");
        return builder.makeLiteral(newString, returnType, true);
      } catch (UnsupportedEncodingException e) {
        LOG.debug("Could not interpret return value for " + colVal);
        return constExp;
      }
    }
    Preconditions.checkState(!colVal.isSetTimestamp_val(),
        "Simplified into timestamp constant but this should not happen");
    Preconditions.checkState(!colVal.isSetDecimal_val(),
        "Simplified into decimal constant but this should not happen");
    return builder.makeNullLiteral(returnType);
  }

  /**
   * The RexNode shuttle that walks through the expression.
   */
  private static class ReduceRexNodeShuttle extends RexShuttle {
    private final Analyzer analyzer_;
    private final TQueryCtx queryCtx_;
    private final RexBuilder rexBuilder_;
    private final Reducer reducer_;

    private ReduceRexNodeShuttle(Analyzer analyzer, TQueryCtx queryCtx,
        RexBuilder rexBuilder, Reducer reducer) {
      this.analyzer_ = analyzer;
      this.queryCtx_ = queryCtx;
      this.rexBuilder_ = rexBuilder;
      this.reducer_ = reducer;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      // recursively call children first.
      RexNode reducedNode = super.visitCall(call);
      if (!isReducible(reducedNode)) {
        return reducedNode;
      }
      RexCall reducedCall = (RexCall) reducedNode;

      try {
         // convert RexNode into Expr objects
         ExprConjunctsConverter converter = new ExprConjunctsConverter(
             reducedCall, null, rexBuilder_, analyzer_, false);
         Preconditions.checkState(converter.getImpalaConjuncts().size() == 1);
         Expr expr = converter.getImpalaConjuncts().get(0);
         if (expr instanceof FunctionCallExpr) {
           FunctionCallExpr funcCallExpr = (FunctionCallExpr) expr;
           if (!funcCallExpr.isConstantImpl()) {
             return reducedCall;
           }
         }
         TColumnValue colValue = reducer_.reduce(expr, queryCtx_);
         return getRexNodeFromColumnValue(rexBuilder_, reducedCall, colValue);
       } catch (Exception e) {
         LOG.debug("Exception thrown in constant folding, constant folding " +
             "not done: " + e);
         LOG.debug(ExceptionUtils.getStackTrace(e));
         return reducedCall;
       }
    }
  }

  /**
   * Interface which allows a hook for JUnit tests, avoiding the necessity of
   * a call to the backend.
   */
  public static interface Reducer {
    public TColumnValue reduce(Expr expr, TQueryCtx queryCtx) throws ImpalaException;
  }

  /**
   * Implementation of the Reducer class which calls the backend via FeSupport.
   */
  public static class ReducerImpl implements Reducer {
    @Override
    public TColumnValue reduce(Expr expr, TQueryCtx queryCtx) throws ImpalaException {
      return FeSupport.EvalExprWithoutRowBounded(expr, queryCtx,
          LiteralExpr.MAX_STRING_LITERAL_SIZE);
    }
  }

}
