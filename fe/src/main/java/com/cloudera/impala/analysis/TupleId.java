// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.Id;
import com.cloudera.impala.common.IdGenerator;

public class TupleId extends Id<TupleId> {
  public TupleId() {
    super();
  }

  public TupleId(int id) {
    super(id);
  }

  public TupleId(IdGenerator<TupleId> idGenerator) {
    super(idGenerator.getNextId());
  }

}
