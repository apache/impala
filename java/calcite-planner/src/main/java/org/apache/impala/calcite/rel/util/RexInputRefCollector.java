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

package org.apache.impala.calcite.rel.util;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

/**
 * Collects all inputrefs for a given RexNodeExpr through the only
 * public method, RexInputRefCollector.getInputRefs(RexNode).
 */
public class RexInputRefCollector extends RexVisitorImpl<Void> {

  private final Set<Integer> inputRefSet = new HashSet<Integer>();

  private RexInputRefCollector(boolean deep) {
    super(deep);
  }

  @Override
  public Void visitInputRef(RexInputRef inputRef) {
    inputRefSet.add(inputRef.getIndex());
    return null;
  }

  public static Set<Integer> getInputRefs(RexNode expr) {
    RexInputRefCollector collector = new RexInputRefCollector(true);
    expr.accept(collector);
    return collector.inputRefSet;
  }

}
