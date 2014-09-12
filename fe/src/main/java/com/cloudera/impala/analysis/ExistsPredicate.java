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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.google.common.base.Preconditions;

/**
 * Class representing a [NOT] EXISTS predicate.
 */
public class ExistsPredicate extends Predicate {
  private final static Logger LOG = LoggerFactory.getLogger(
      ExistsPredicate.class);
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
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    // Cannot serialize a nested predicate
    Preconditions.checkState(false);
  }

  @Override
  public Expr clone() { return new ExistsPredicate(this); }

  @Override
  public String toSqlImpl() {
    StringBuilder strBuilder = new StringBuilder();
    if (notExists_) strBuilder.append("NOT ");
    strBuilder.append("EXISTS ");
    strBuilder.append(getChild(0).toSql());
    return strBuilder.toString();
  }
}
