// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.HashMap;

/**
 * Repository for tuple (and slot) descriptors.
 * Descriptors should only be created through this class, which assigns
 * them unique ids..
 *
 */
public class DescriptorTable {
  private final HashMap<TupleId, TupleDescriptor> tupleDescs;
  private int nextTupleId;
  private int nextSlotId;

  public DescriptorTable() {
    tupleDescs = new HashMap<TupleId, TupleDescriptor>();
    nextTupleId = 0;
    nextSlotId = 0;
  }

  public TupleDescriptor createTupleDescriptor() {
    TupleDescriptor d = new TupleDescriptor(nextTupleId++);
    tupleDescs.put(d.getId(), d);
    return d;
  }

  public SlotDescriptor addSlotDescriptor(TupleDescriptor d) {
    SlotDescriptor result = new SlotDescriptor(nextSlotId++, d);
    d.addSlot(result);
    return result;
  }

  public TupleDescriptor getTupleDesc(TupleId id) {
    return tupleDescs.get(id);
  }
}
