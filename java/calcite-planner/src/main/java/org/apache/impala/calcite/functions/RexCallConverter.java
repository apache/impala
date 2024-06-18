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

package org.apache.impala.calcite.functions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

/**
 * Static Helper class that returns Exprs for RexCall nodes.
 */
public class RexCallConverter {
  protected static final Logger LOG =
      LoggerFactory.getLogger(RexCallConverter.class.getName());

  /*
   * Returns the Impala Expr object for RexCallConverter.
   */
  public static Expr getExpr(RexCall rexCall, List<Expr> params, RexBuilder rexBuilder) {

    String funcName = rexCall.getOperator().getName().toLowerCase();

    Function fn = getFunction(rexCall);

    Type impalaRetType = ImpalaTypeConverter.createImpalaType(fn.getReturnType(),
        rexCall.getType().getPrecision(), rexCall.getType().getScale());

    return new AnalyzedFunctionCallExpr(fn, params, rexCall, impalaRetType);
  }

  private static Function getFunction(RexCall call) {
    List<RelDataType> argTypes = Lists.transform(call.getOperands(), RexNode::getType);
    String name = call.getOperator().getName();
    Function fn = FunctionResolver.getFunction(name, call.getKind(), argTypes);
    Preconditions.checkNotNull(fn, "Could not find function \"" + name + "\" in Impala "
          + "with args " + argTypes + " and return type " + call.getType());
    return fn;
  }
}
