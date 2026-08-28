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
package org.apache.impala.calcite.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Traverses a {@link RelNode} tree and extracts the maximal subtrees that are
 * valid for {@link org.apache.calcite.rel.rules.materialize.MaterializedViewRule}.
 *
 * <p>A subtree is valid if it matches one of these patterns:
 * <ul>
 *   <li>All nodes are from {TableScan, Project, Filter, Join} and all Joins
 *   are INNER.</li>
 *   <li>The root is a simple {@link Aggregate} whose input satisfies the
 *   pattern above.</li>
 * </ul>
 *
 * <p>The returned subtrees are maximal: no two subtrees in the result contain
 * each other. The algorithm applies a postorder traversal and visits each
 * node exactly once (O(n) in the number of nodes).
 */
public class MaterializedViewSubTreeSplitter {
  private final Set<RelNode> result = new LinkedHashSet<>();

  /**
   * Returns the maximal subtrees of {@code root} that are valid for
   * materialized view rewriting.
   */
  public static Set<RelNode> maximalSubTrees(RelNode root) {
    final MaterializedViewSubTreeSplitter splitter =
        new MaterializedViewSubTreeSplitter();
    if (splitter.visitNode(root)) { return Collections.singleton(root); }
    return splitter.result;
  }

  /**
   * Checks whether the subtree rooted at {@code node} is valid.
   *
   * <p>Returns {@code true} if the entire subtree is valid. In that case
   * nothing is added to {@link #result}; the caller decides whether to add
   * the subtree root.
   *
   * <p>Returns {@code false} if the subtree is not valid. In that case all
   * maximal valid subtrees within have already been added to {@link #result}
   * as a side effect.
   */
  private boolean visitNode(RelNode node) {
    if (node instanceof TableScan) { return true; }
    if (node instanceof Project p) { return visitNode(p.getInput()); }
    if (node instanceof Filter f) { return visitNode(f.getInput()); }
    if (node instanceof Join j) { return visitJoin(j); }
    if (node instanceof Aggregate a) { return visitAggregate(a); }

    // The current node is not valid, check the inputs individually
    for (RelNode input : node.getInputs()) {
      if (visitNode(input)) { result.add(input); }
    }
    return false;
  }

  private boolean visitJoin(Join join) {
    final boolean leftValid = visitNode(join.getLeft());
    final boolean rightValid = visitNode(join.getRight());
    if (join.getJoinType() == JoinRelType.INNER && leftValid && rightValid) {
      // The whole join tree is valid so let the caller decide if its maximal
      return true;
    }
    if (leftValid) { result.add(join.getLeft()); }
    if (rightValid) { result.add(join.getRight()); }
    return false;
  }

  private boolean visitAggregate(Aggregate agg) {
    final boolean validInput = visitNode(agg.getInput());
    if (agg.getGroupType() == Aggregate.Group.SIMPLE && validInput) {
      result.add(agg);
    } else if (validInput) {
      result.add(agg.getInput());
    }
    // Aggregates start their own subtrees and are not composable thus
    // we always return false.
    return false;
  }
}
