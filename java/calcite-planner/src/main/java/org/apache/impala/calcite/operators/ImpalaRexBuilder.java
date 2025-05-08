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
package org.apache.impala.calcite.operators;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

/**
 * Factory for row expressions.
 *
 * See the base class in Calcite for the details about RexBuilder. This extension
 * allows Impala to override functions that need extra work to make RexBuilder
 * Impala compatible.
 */
public class ImpalaRexBuilder extends RexBuilder {

  public ImpalaRexBuilder(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  /**
   * Only override the makeCall.  The SEARCH operator is not supported at this time,
   * so we "expand" it to something Impala can understand.
   */
  @Override
  public RexNode makeCall(
      SqlOperator op,
      List<? extends RexNode> exprs) {
    RexNode retNode = super.makeCall(op, exprs);
    return op.getKind() == SqlKind.SEARCH
        ? RexUtil.expandSearch(this, null, retNode)
        : retNode;
  }
}
