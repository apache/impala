// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.common.Id;
import com.cloudera.impala.common.IdGenerator;

public class PlanFragmentId extends Id<PlanFragmentId> {
  public PlanFragmentId() {
    super();
  }

  public PlanFragmentId(int id) {
    super(id);
  }

  public PlanFragmentId(IdGenerator<PlanFragmentId> idGenerator) {
    super(idGenerator.getNextId());
  }
}
