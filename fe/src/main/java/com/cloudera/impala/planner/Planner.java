// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.parser.ParseNode;

/**
 * The planner is responsible for turning parse trees into plan fragments that
 * can be shipped off to backends for execution.
 *
 */
public class Planner {
  public Planner() {
  }

  public PlanNode createPlan(ParseNode parseTree) throws NotImplementedException {
    return null;
  }
}
