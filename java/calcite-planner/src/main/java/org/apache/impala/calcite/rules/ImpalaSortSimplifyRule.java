/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.impala.calcite.operators.ImpalaRexSimplify;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * ImpalaSortSimplifyRule is a rule that simplifies the offset and limit clauses. Impala
 * allows constant expressions within these clauses (e.g. "limit 1 + 1", and this rule
 * will simplify this expression to a literal (e.g. "limit 2").
 */
public class ImpalaSortSimplifyRule extends RelOptRule {
  private final ImpalaRexSimplify simplifier_;

  public ImpalaSortSimplifyRule(ImpalaRexSimplify simplifier) {
    super(operand(Sort.class, none()));
    this.simplifier_ = simplifier;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Sort sort = call.rel(0);
    // the common case, most times a limit is not an expression.
    if ((sort.fetch == null || sort.fetch instanceof RexLiteral) &&
        (sort.offset == null || sort.offset instanceof RexLiteral)) {
      verifyValue(sort.fetch, "LIMIT");
      verifyValue(sort.offset, "OFFSET");
      return;
    }
    RelOptCluster cluster = sort.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    // Use an Impala executor that will not create a cast around
    // the result. Only a RexLiteral is allowed in the limit and
    // offset clauses.
    RexExecutor executor = new ImpalaRexExecutor(
        (ImpalaRexExecutor) simplifier_.getRexExecutor(), false);

    RexNode newFetch = sort.fetch;
    RexNode newOffset = sort.offset;
    List<RexNode> reducedExprs = new ArrayList<>();
    boolean somethingChanged = false;
    if (sort.fetch != null) {
      executor.reduce(rexBuilder, ImmutableList.of(sort.fetch), reducedExprs);
      newFetch = reducedExprs.get(0);
      verifyValue(newFetch, "LIMIT");
      if (!sort.fetch.equals(newFetch)) {
        somethingChanged = true;
      }
    }
    reducedExprs.clear();
    if (sort.offset != null) {
      executor.reduce(rexBuilder, ImmutableList.of(sort.offset), reducedExprs);
      newOffset = reducedExprs.get(0);
      verifyValue(newOffset, "OFFSET");
      if (!sort.offset.equals(newOffset)) {
        somethingChanged = true;
      }
    }
    if (!somethingChanged) {
      return;
    }

    Sort newSort = sort.copy(sort.getTraitSet(), sort.getInput(0), sort.getCollation(),
        newOffset, newFetch);
    call.transformTo(newSort);
  }

  private void verifyValue(RexNode reducedNode, String exprType) {
    if (reducedNode == null) {
      return;
    }
    if (!SqlTypeUtil.isIntType(reducedNode.getType())) {
      throw new RuntimeException(exprType + " expression must be an integer type.");
    }
    if (!(reducedNode instanceof RexLiteral)) {
      throw new RuntimeException(exprType + " must be a constant expression.");
    }
    BigDecimal bd = ((RexLiteral) reducedNode).getValueAs(BigDecimal.class);
    if (bd == null) {
      throw new RuntimeException(exprType + " must be a constant expression.");
    }
    if (bd.compareTo(BigDecimal.ZERO) < 0) {
      throw new RuntimeException(exprType + " must be a non-negative integer.");
    }
  }
}
