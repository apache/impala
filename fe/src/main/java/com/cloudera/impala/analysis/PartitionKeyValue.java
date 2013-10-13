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

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * Representation of a single column:value element in the PARTITION (...) clause of an
 * insert or alter table statement.
 */
public class PartitionKeyValue {
  // Name of partitioning column.
  private final String colName;
  // Value of partitioning column. Set to null for dynamic inserts.
  private final Expr value;
  // Evaluation of value for static partition keys, null otherwise. Set in analyze().
  private LiteralExpr literalValue;

  public PartitionKeyValue(String colName, Expr value) {
    this.colName = colName.toLowerCase();
    this.value = value;
  }

  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isStatic() && !value.isConstant()) {
      throw new AnalysisException(
          String.format("Non-constant expressions are not supported " +
              "as static partition-key values in '%s'.", toString()));
    }
    if (value == null) return;
    value.analyze(analyzer);
    literalValue = LiteralExpr.create(value, analyzer.getQueryGlobals());
  }

  public String getColName() {
    return colName;
  }

  public Expr getValue() {
    return value;
  }

  public boolean isDynamic() {
    return value == null;
  }

  public boolean isStatic() {
    return !isDynamic();
  }

  @Override
  public String toString() {
    return isStatic() ? colName + "=" + value.toSql() : colName;
  }

  /**
   * Utility method that returns the string value for the given partition key. For
   * NULL values (a NullLiteral type) or empty strings this will return the
   * null partition key value.
   */
  public String getPartitionKeyValueString(String nullPartitionKeyValue) {
    Preconditions.checkNotNull(literalValue);
    if (literalValue instanceof NullLiteral
        || literalValue.getStringValue().isEmpty()) {
      return nullPartitionKeyValue;
    }
    return literalValue.getStringValue();
  }
}
