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
package org.apache.impala.calcite.rel.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.common.ImpalaException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ExprConjunctsConverter takes a RexNode conjunct and converts it
 * into an Impala Expr object. The child node input refs are included
 * as an input.
 */
public class ExprConjunctsConverter {
  private static final Logger LOG = LoggerFactory.getLogger(ExprConjunctsConverter.class);

  private final List<Expr> allConjuncts_;

  public ExprConjunctsConverter(RexNode conjunct, List<Expr> inputExprs,
      RexBuilder rexBuilder, Analyzer analyzer) throws ImpalaException {
    ImmutableList.Builder<Expr> builder = new ImmutableList.Builder();
    if (conjunct != null) {
      CreateExprVisitor visitor =
          new CreateExprVisitor(rexBuilder, inputExprs, analyzer);

      List<RexNode> andOperands = getAndConjuncts(conjunct);
      for (RexNode andOperand : andOperands) {
        Expr convertedExpr = CreateExprVisitor.getExpr(visitor, andOperand);
        builder.add(convertedExpr);
      }
    }

    this.allConjuncts_ = builder.build();
  }

  public List<Expr> getImpalaConjuncts() {
    return allConjuncts_;
  }

  /**
   * Break the list up by its AND conjuncts.  We only care about
   * AND clauses on the first level. Calcite does not treat AND
   * clauses as binary (e.g. <clause1> AND <clause2> AND <clause3>
   * will have all 3 clauses on the first level), so we do not
   * need to recursively search for clauses.
   */
  private static List<RexNode> getAndConjuncts(RexNode conjunct) {
    if (conjunct == null) {
      return ImmutableList.of();
    }

    // If it's not a RexCall, there's no AND operator and we can
    // just return the conjunct.
    if (!(conjunct instanceof RexCall)) {
      return ImmutableList.of(conjunct);
    }

    RexCall rexCallConjunct = (RexCall) conjunct;
    if (rexCallConjunct.getKind() != SqlKind.AND) {
      return ImmutableList.of(conjunct);
    }
    // If it's an AND conjunct, then all the operands represent individual
    // AND clauses.
    return rexCallConjunct.getOperands();
  }
}
