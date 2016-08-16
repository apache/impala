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

import com.google.common.base.Preconditions;
import org.apache.impala.common.AnalysisException;
import java.util.Comparator;

/**
 * Representation of a single column:value element in the PARTITION (...) clause of an
 * insert or alter table statement.
 */
public class PartitionKeyValue {
  // Name of partitioning column.
  private final String colName_;
  // Value of partitioning column. Set to null for dynamic inserts.
  private final Expr value_;
  // Evaluation of value for static partition keys, null otherwise. Set in analyze().
  private LiteralExpr literalValue_;

  public PartitionKeyValue(String colName, Expr value) {
    this.colName_ = colName.toLowerCase();
    this.value_ = value;
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isStatic() && !value_.isConstant()) {
      throw new AnalysisException(
          String.format("Non-constant expressions are not supported " +
              "as static partition-key values in '%s'.", toString()));
    }
    if (value_ == null) return;
    value_.analyze(analyzer);
    literalValue_ = LiteralExpr.create(value_, analyzer.getQueryCtx());
  }

  public String getColName() { return colName_; }
  public Expr getValue() { return value_; }
  public LiteralExpr getLiteralValue() { return literalValue_; }
  public boolean isDynamic() { return value_ == null; }
  public boolean isStatic() { return !isDynamic(); }

  @Override
  public String toString() {
    return isStatic() ? colName_ + "=" + value_.toSql() : colName_;
  }

  /**
   * Returns a binary predicate as a SQL string which matches the column and value of this
   * PartitionKeyValue. If the value is null, correctly substitutes 'IS' as the operator.
   */
  public String toPredicateSql() {
    String ident = ToSqlUtils.getIdentSql(colName_);
    if (literalValue_ instanceof NullLiteral ||
        literalValue_.getStringValue().isEmpty()) {
      return ident + " IS NULL";
    }
    return isStatic() ? ident + "=" + value_.toSql() : ident;
  }

  /**
   * Utility method that returns the string value for the given partition key. For
   * NULL values (a NullLiteral type) or empty literal values this will return the
   * given null partition key value.
   */
  public static String getPartitionKeyValueString(LiteralExpr literalValue,
      String nullPartitionKeyValue) {
    Preconditions.checkNotNull(literalValue);
    if (literalValue instanceof NullLiteral || literalValue.getStringValue().isEmpty()) {
      return nullPartitionKeyValue;
    }
    return literalValue.getStringValue();
  }

  public static Comparator<PartitionKeyValue> getColNameComparator() {
    return new Comparator<PartitionKeyValue>() {
      @Override
      public int compare(PartitionKeyValue t, PartitionKeyValue o) {
        return t.colName_.compareTo(o.colName_);
      }
    };
  }
}
