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

package org.apache.impala.calcite.functions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.util.List;

/**
 * Static Helper class that returns Exprs for RexLiteral nodes.
 */
public class RexLiteralConverter {
  protected static final Logger LOG =
      LoggerFactory.getLogger(RexLiteralConverter.class.getName());

  /*
   * Returns Expr object for ImpalaRexLiteral
   */
  public static Expr getExpr(RexLiteral rexLiteral) {
    if (SqlTypeName.INTERVAL_TYPES.contains(rexLiteral.getTypeName())) {
      return NumericLiteral.create(
          new BigDecimal(rexLiteral.getValueAs(Long.class)), Type.BIGINT);
    }
    switch (rexLiteral.getTypeName()) {
      case NULL:
        Type type = ImpalaTypeConverter.createImpalaType(rexLiteral.getType());
        return new AnalyzedNullLiteral(type);
      case BOOLEAN:
        Expr boolExpr = new BoolLiteral((Boolean) rexLiteral.getValueAs(Boolean.class));
        return boolExpr;
      case BIGINT:
      case DECIMAL:
      case DOUBLE:
        Expr numericExpr = NumericLiteral.create(rexLiteral.getValueAs(BigDecimal.class),
            ImpalaTypeConverter.createImpalaType(rexLiteral.getType()));
        return numericExpr;
      case CHAR:
      case VARCHAR:
        ScalarType charType = rexLiteral.getType().getSqlTypeName() == SqlTypeName.VARCHAR
            ? Type.STRING
            : ScalarType.createCharType(rexLiteral.getValueAs(String.class).length());
        if (charType.getPrimitiveType() == PrimitiveType.CHAR) {
          // ensure no wildcards or char length 0 which will crash impalad
          Preconditions.checkState(charType.getLength() > 0);
        }
        Expr charExpr = new StringLiteral(rexLiteral.getValueAs(String.class),
            charType, false);
        return charExpr;
      case DATE:
        DateString dateStringClass = rexLiteral.getValueAs(DateString.class);
        String dateString = (dateStringClass == null) ? null : dateStringClass.toString();
        Expr dateExpr = new DateLiteral(rexLiteral.getValueAs(Integer.class), dateString);
        return dateExpr;
      case TIMESTAMP:
          return createCastTimestampExpr(rexLiteral);
      default:
        Preconditions.checkState(false, "Unsupported RexLiteral: "
            + rexLiteral.getTypeName());
        return null;
    }
  }

  /**
   * Create a cast timestamp expression from a String to a Timestamp.
   * The only way to create a TimestampLiteral directly in Impala is by accessing
   * the backend. This will normally be done earlier in Calcite via constant folding.
   * If constant folding was not allowed, it means we did not have access to the backend
   * and thus need to do a cast in order to support conversion to a Timestamp.
   */
  private static Expr createCastTimestampExpr(RexLiteral rexLiteral) {
    List<RelDataType> typeNames =
        ImmutableList.of(ImpalaTypeConverter.getRelDataType(Type.STRING));

    String timestamp = rexLiteral.getValueAs(TimestampString.class).toString();
    List<Expr> argList =
        Lists.newArrayList(new StringLiteral(timestamp, Type.STRING, false));
    Function castFunc = FunctionResolver.getFunction("casttotimestamp", typeNames);
    return new AnalyzedFunctionCallExpr(castFunc, argList, null, Type.TIMESTAMP);
  }
}
