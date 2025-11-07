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

import org.apache.calcite.DataContexts;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoveUnraggedCharCastRexExecutor is a workaround needed until a
 * solution is found for CALCITE-7244. Calcite always creates a string
 * RexLiteral as type CHAR<x>, whereas Impala needs the string literal as
 * STRING (a "create table my_string_tbl as select 'hello'" will create a
 * STRING type column). The conversion from CHAR to STRING is handled in
 * the CoercedNodes part of the code which is run after the SqlNode abstract
 * syntax tree has been converted into the Logical Node tree.
 *
 * However, there is a special case that causes problems that occurs before
 * the RelNode tree creation, at SqlToRelNodeConverter time. In the case of
 * the following query: "values(('big'), ('bigger'))", the SqlToRelNodeConverter
 * needs to ensure that both rows are the same size. So a char cast of the bigger
 * size is added around the first string.
 *
 * After Calcite creates this cast, the string changes when it is reduced. The
 * 'big' string becomes a space padded 'big   ' string.  This Calcite reduction
 * causes results inconsistent from what the original Impala planner produces.
 *
 * In order to avoid this, this executor removes the outer cast if the operand
 * of a CHAR cast expression is a CHAR cast.
 *
 * Note that the only possible place this expression can happen is through this
 * mechanism. The CAST operator is only used for implicit chars, that is, casts
 * that are created internally. If the SQL actually contained a cast, the
 * EXPLICIT_CAST operator would be used and the logic here would not affect
 * that expression.
 */
public class RemoveUnraggedCharCastRexExecutor extends RexExecutorImpl {
  protected static final Logger LOG =
      LoggerFactory.getLogger(RemoveUnraggedCharCastRexExecutor.class.getName());

  public RemoveUnraggedCharCastRexExecutor() {
    super(DataContexts.EMPTY);
  }

  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps,
      List<RexNode> reducedValues) {

    if (!rexNodeIsCharCastOfChar(constExps)) {
      super.reduce(rexBuilder, constExps, reducedValues);
      return;
    }
    reducedValues.add(((RexCall)constExps.get(0)).getOperands().get(0));
  }

  private boolean rexNodeIsCharCastOfChar(List<RexNode> rexNodes) {
    if (rexNodes.size() > 1 || rexNodes.size() == 0) {
      return false;
    }
    if (!(rexNodes.get(0) instanceof RexCall)) {
      return false;
    }
    RexCall constExp = (RexCall) rexNodes.get(0);
    if (!constExp.getKind().equals(SqlKind.CAST)) {
      return false;
    }
    if (!constExp.getType().getSqlTypeName().equals(SqlTypeName.CHAR)) {
      return false;
    }
    RexNode operand = constExp.getOperands().get(0);
    if (!(operand instanceof RexLiteral)) {
      return false;
    }
    if (!operand.getType().getSqlTypeName().equals(SqlTypeName.CHAR)) {
      return false;
    }
    return true;
  }
}
