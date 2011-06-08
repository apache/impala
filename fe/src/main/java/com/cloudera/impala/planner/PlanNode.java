// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.common.TreeNode;

/**
 * Each PlanNode represents a single relational operator
 * and encapsulates the information needed by the planner to
 * make optimization decisions.
 *
 */
abstract public class PlanNode extends TreeNode<PlanNode> {

  public String debugString() {
    return "";
  }
}
