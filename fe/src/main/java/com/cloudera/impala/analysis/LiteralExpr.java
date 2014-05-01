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

import java.math.BigInteger;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TQueryContext;
import com.google.common.base.Preconditions;

/**
 * Representation of a literal expression. Literals are comparable to allow
 * ordering of HdfsPartitions whose partition-key values are represented as literals.
 */
public abstract class LiteralExpr extends Expr implements Comparable<LiteralExpr> {

  public LiteralExpr() {
    numDistinctValues_ = 1;
    isAnalyzed_ = true;
  }

  public static LiteralExpr create(String value, ColumnType type)
      throws AnalysisException, AuthorizationException {
    Preconditions.checkArgument(type.isValid());
    switch (type.getPrimitiveType()) {
      case NULL_TYPE:
        return new NullLiteral();
      case BOOLEAN:
        return new BoolLiteral(value);
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        return new IntLiteral(value);
      case FLOAT:
      case DOUBLE:
        return new FloatLiteral(value);
      case STRING:
        return new StringLiteral(value);
      case DECIMAL:
      case DATE:
      case DATETIME:
      case TIMESTAMP:
        // TODO: we support DECIMAL and TIMESTAMP but no way to specify it in
        // SQL.
        throw new AnalysisException(
            "DATE/DATETIME/TIMESTAMP/DECIMAL literals not supported: " + value);
      default:
        Preconditions.checkState(false);
    }
    return null;
  }

  // Returns the string representation of the literal's value. Used when passing
  // literal values to the metastore rather than to Impala backends. This is similar to
  // the toSql() method, but does not perform any formatting of the string values. Neither
  // method unescapes string values.
  public abstract String getStringValue();

  // Swaps the sign of numeric literals.
  // Throws for non-numeric literals.
  public void swapSign() throws NotImplementedException {
    throw new NotImplementedException("swapSign() only implemented for numeric" +
        "literals");
  }

  /**
   * Evaluates the given constant expr and returns its result as a LiteralExpr.
   * Assumes expr has been analyzed. Returns constExpr if is it already a LiteralExpr.
   */
  public static LiteralExpr create(Expr constExpr, TQueryContext queryCtxt)
      throws AnalysisException {
    Preconditions.checkState(constExpr.isConstant());
    Preconditions.checkState(constExpr.getType().isValid());
    if (constExpr instanceof LiteralExpr) return (LiteralExpr) constExpr;

    TColumnValue val = null;
    try {
      val = FeSupport.EvalConstExpr(constExpr, queryCtxt);
    } catch (InternalException e) {
      throw new AnalysisException(String.format("Failed to evaluate expr '%s'",
          constExpr.toSql()), e);
    }

    LiteralExpr result = null;
    switch (constExpr.getType().getPrimitiveType()) {
      case NULL_TYPE:
        result = new NullLiteral();
        break;
      case BOOLEAN:
        if (val.isBool_val()) result = new BoolLiteral(val.bool_val);
        break;
      case TINYINT:
        if (val.isSetByte_val()) {
          result = new IntLiteral(BigInteger.valueOf(val.byte_val));
        }
        break;
      case SMALLINT:
        if (val.isSetShort_val()) {
          result = new IntLiteral(BigInteger.valueOf(val.short_val));
        }
        break;
      case INT:
        if (val.isSetInt_val()) result = new IntLiteral(BigInteger.valueOf(val.int_val));
        break;
      case BIGINT:
        if (val.isSetLong_val()) {
          result = new IntLiteral(BigInteger.valueOf(val.long_val));
        }
        break;
      case FLOAT:
      case DOUBLE:
        if (val.isSetDouble_val()) result = new FloatLiteral(val.double_val);
        break;
      case STRING:
        if (val.isSetString_val()) result = new StringLiteral(val.string_val);
        break;
      case DATE:
      case DATETIME:
      case TIMESTAMP:
        throw new AnalysisException(
            "DATE/DATETIME/TIMESTAMP literals not supported: " + constExpr.toSql());
    }
    // None of the fields in the thrift struct were set indicating a NULL.
    if (result == null) result = new NullLiteral();

    return result;
  }
}
