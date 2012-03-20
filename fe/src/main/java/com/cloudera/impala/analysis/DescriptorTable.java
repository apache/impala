// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TDescriptorTable;
import com.google.common.collect.Sets;

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

  public Collection<TupleDescriptor> getTupleDescs() {
    return tupleDescs.values();
  }

  public TupleId getMaxTupleId() {
    return new TupleId(nextTupleId - 1);
  }

  // Computes physical layout parameters of all descriptors.
  // Call this only after the last descriptor was added.
  public void computeMemLayout() {
    for (TupleDescriptor d: tupleDescs.values()) {
      d.computeMemLayout();
    }
  }

  public TDescriptorTable toThrift() {
    TDescriptorTable result = new TDescriptorTable();
    HashSet<Table> referencedTbls = Sets.newHashSet();
    for (TupleDescriptor tupleD: tupleDescs.values()) {
      // inline view has a non-materialized tuple descriptor in the descriptor table
      // just for type checking. So, skip it when generating Thrift. Further, inline view
      // doesn't have required Thrift attribute.
      if (tupleD.getIsMaterialized()) {
        result.addToTupleDescriptors(tupleD.toThrift());
        if (tupleD.getTable() != null) {
          referencedTbls.add(tupleD.getTable());
        }
        for (SlotDescriptor slotD: tupleD.getSlots()) {
          result.addToSlotDescriptors(slotD.toThrift());
        }
      }
    }
    for (Table tbl: referencedTbls) {
      result.addToTableDescriptors(tbl.toThrift());
    }
    return result;
  }
}
