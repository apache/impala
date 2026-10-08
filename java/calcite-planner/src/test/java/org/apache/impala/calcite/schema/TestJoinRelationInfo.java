/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.calcite.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/** Tests when join equalities can use the specialized row-count estimate. */
public class TestJoinRelationInfo {
  private RelNode left_;
  private RelNode right_;
  private RexBuilder rexBuilder_;
  private RexNode leftColumn_;
  private RexNode secondLeftColumn_;
  private RexNode rightColumn_;
  private RexNode secondRightColumn_;

  @Before
  public void setUp() {
    RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder().build());
    left_ = builder.values(new String[] {"a", "b"}, 1, 2).build();
    right_ = builder.values(new String[] {"c", "d"}, 3, 4).build();
    rexBuilder_ = left_.getCluster().getRexBuilder();
    leftColumn_ = rexBuilder_.makeInputRef(left_, 0);
    secondLeftColumn_ = rexBuilder_.makeInputRef(left_, 1);
    rightColumn_ = rexBuilder_.makeInputRef(
        right_.getRowType().getFieldList().get(0).getType(), 2);
    secondRightColumn_ = rexBuilder_.makeInputRef(
        right_.getRowType().getFieldList().get(1).getType(), 3);
  }

  @Test
  public void testSimpleEqualityUsesStatistics() {
    assertUsesStatistics(leftColumn_, rightColumn_);
    assertUsesStatistics(rightColumn_, leftColumn_);
  }

  @Test
  public void testSingleColumnExpressionUsesStatistics() {
    RexNode expression = rexBuilder_.makeCall(SqlStdOperatorTable.PLUS,
        leftColumn_, rexBuilder_.makeExactLiteral(BigDecimal.ONE));
    assertUsesStatistics(expression, rightColumn_);
  }

  @Test
  public void testMultipleColumnExpressionUsesDefaultEstimate() {
    RexNode leftExpression = rexBuilder_.makeCall(
        SqlStdOperatorTable.PLUS, leftColumn_, secondLeftColumn_);
    assertUsesDefaultEstimate(SqlStdOperatorTable.EQUALS,
        leftExpression, rightColumn_);
    RexNode rightExpression = rexBuilder_.makeCall(
        SqlStdOperatorTable.PLUS, rightColumn_, secondRightColumn_);
    assertUsesDefaultEstimate(SqlStdOperatorTable.EQUALS,
        leftColumn_, rightExpression);
  }

  @Test
  public void testConstantOperandUsesDefaultEstimate() {
    RexNode constant = rexBuilder_.makeExactLiteral(BigDecimal.ONE);
    assertUsesDefaultEstimate(SqlStdOperatorTable.EQUALS, constant, rightColumn_);
    assertUsesDefaultEstimate(SqlStdOperatorTable.EQUALS, leftColumn_, constant);
  }

  @Test
  public void testNonEqualityUsesDefaultEstimate() {
    assertUsesDefaultEstimate(SqlStdOperatorTable.LESS_THAN,
        leftColumn_, rightColumn_);
  }

  @Test
  public void testSameInputEqualityUsesDefaultEstimate() {
    assertUsesDefaultEstimate(SqlStdOperatorTable.EQUALS,
        leftColumn_, secondLeftColumn_);
    assertUsesDefaultEstimate(SqlStdOperatorTable.EQUALS,
        rightColumn_, secondRightColumn_);
  }

  @Test
  public void testUnsupportedConjunctionPreservesUsableEquality() {
    RexNode usable = rexBuilder_.makeCall(
        SqlStdOperatorTable.EQUALS, leftColumn_, rightColumn_);
    RexNode sameInput = rexBuilder_.makeCall(
        SqlStdOperatorTable.EQUALS, leftColumn_, secondLeftColumn_);
    RexNode expression = rexBuilder_.makeCall(
        SqlStdOperatorTable.PLUS, leftColumn_, secondLeftColumn_);
    RexNode compound = rexBuilder_.makeCall(
        SqlStdOperatorTable.EQUALS, expression, rightColumn_);
    JoinRelationInfo info = createInfo(
        rexBuilder_.makeCall(SqlStdOperatorTable.AND, usable, sameInput, compound));
    assertFalse(info.useDefaultRowCount());
    assertTrue(info.hasEqualityConjunctions());
    assertEquals(1.0, info.getRowCount(), 0.0);
  }

  private void assertUsesStatistics(RexNode lhs, RexNode rhs) {
    JoinRelationInfo info = createInfo(SqlStdOperatorTable.EQUALS, lhs, rhs);
    assertFalse(info.useDefaultRowCount());
    assertTrue(info.hasEqualityConjunctions());
    assertEquals(1.0, info.getRowCount(), 0.0);
  }

  private void assertUsesDefaultEstimate(SqlOperator operator,
      RexNode lhs, RexNode rhs) {
    JoinRelationInfo info = createInfo(operator, lhs, rhs);
    assertTrue(info.useDefaultRowCount());
    assertFalse(info.hasEqualityConjunctions());
  }

  private JoinRelationInfo createInfo(SqlOperator operator, RexNode lhs, RexNode rhs) {
    return createInfo(rexBuilder_.makeCall(operator, lhs, rhs));
  }

  private JoinRelationInfo createInfo(RexNode condition) {
    LogicalJoin join = LogicalJoin.create(left_, right_, ImmutableList.of(),
        condition, ImmutableSet.of(), JoinRelType.INNER);
    return new JoinRelationInfo(join, rexBuilder_, left_.getCluster().getMetadataQuery());
  }
}
