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

import java.util.Objects;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;

import com.google.common.base.Preconditions;

/**
 * Class representing a [NOT] EXISTS predicate.
 */
public class ExistsPredicate extends Predicate {
  private boolean notExists_ = false;

  public boolean isNotExists() { return notExists_; }

  /**
   * C'tor that initializes an ExistsPredicate from a Subquery.
   */
  public ExistsPredicate(Subquery subquery, boolean notExists) {
    Preconditions.checkNotNull(subquery);
    children_.add(subquery);
    notExists_ = notExists;
  }

  @Override
  public Expr negate() {
    return new ExistsPredicate((Subquery)getChild(0), !notExists_);
  }

  /**
   * Copy c'tor used in clone.
   */
  public ExistsPredicate(ExistsPredicate other) {
    super(other);
    notExists_ = other.notExists_;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
    ((Subquery)children_.get(0)).getStatement().setIsRuntimeScalar(false);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    // Cannot serialize a nested predicate
    Preconditions.checkState(false);
  }

  @Override
  protected boolean localEquals(Expr that) {
    return super.localEquals(that) && notExists_ == ((ExistsPredicate)that).notExists_;
  }

  @Override
  protected int localHash() {
    return Objects.hash(super.localHash(), notExists_);
  }

  @Override
  public Expr clone() { return new ExistsPredicate(this); }

  @Override
  protected float computeEvalCost() { return UNKNOWN_COST; }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    StringBuilder strBuilder = new StringBuilder();
    if (notExists_) strBuilder.append("NOT ");
    strBuilder.append("EXISTS ");
    strBuilder.append(getChild(0).toSql(options));
    return strBuilder.toString();
  }

  // Return false since existence can be expensive to determine.
  @Override
  public boolean shouldConvertToCNF() { return false; }
}
