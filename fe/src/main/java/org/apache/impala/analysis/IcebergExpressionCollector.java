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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.expressions.BoundAggregate;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import org.apache.iceberg.expressions.UnboundAggregate;
import org.apache.iceberg.expressions.UnboundPredicate;

/**
 * Visitor implementation to traverse Iceberg expression tree and locate predicates.
 */
public class IcebergExpressionCollector extends ExpressionVisitor<List<Expression>> {
  @Override
  public List<Expression> alwaysTrue() {
    return Collections.emptyList();
  }

  @Override
  public List<Expression> alwaysFalse() {
    return Collections.emptyList();
  }

  @Override
  public List<Expression> not(List<Expression> result) {
    return result;
  }

  @Override
  public List<Expression> and(List<Expression> leftResult, List<Expression> rightResult) {
    leftResult.addAll(rightResult);
    return leftResult;
  }

  @Override
  public List<Expression> or(List<Expression> leftResult, List<Expression> rightResult) {
    leftResult.addAll(rightResult);
    return leftResult;
  }

  @Override
  public <T> List<Expression> predicate(BoundPredicate<T> pred) {
    List<Expression> expressions = new ArrayList<>();
    expressions.add(pred);
    return expressions;
  }

  @Override
  public <T> List<Expression> predicate(UnboundPredicate<T> pred) {
    List<Expression> expressions = new ArrayList<>();
    expressions.add(pred);
    return expressions;
  }

  @Override
  public <T, C> List<Expression> aggregate(BoundAggregate<T, C> agg) {
    return Collections.emptyList();
  }

  @Override
  public <T> List<Expression> aggregate(UnboundAggregate<T> agg) {
    return Collections.emptyList();
  }
}
