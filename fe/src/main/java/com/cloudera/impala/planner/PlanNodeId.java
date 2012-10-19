// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.common.Id;
import com.cloudera.impala.common.IdGenerator;

public class PlanNodeId extends Id<PlanNodeId> {
  public PlanNodeId() {
    super();
  }

  public PlanNodeId(int id) {
    super(id);
  }

  public PlanNodeId(IdGenerator<PlanNodeId> idGenerator) {
    super(idGenerator.getNextId());
  }
}
