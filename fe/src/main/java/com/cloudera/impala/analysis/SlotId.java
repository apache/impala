// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.Id;
import com.cloudera.impala.common.IdGenerator;

public class SlotId extends Id<SlotId> {
  public SlotId() {
    super();
  }

  public SlotId(int id) {
    super(id);
  }

  public SlotId(IdGenerator<SlotId> idGenerator) {
    super(idGenerator.getNextId());
  }
}
