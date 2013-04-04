// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

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
  // List of referenced tables with no associated TupleDescriptor to ship to the BE.
  // For example, the output table of an insert query.
  private final List<Table> referencedTables;
  private int nextTupleId;
  private int nextSlotId;

  public DescriptorTable() {
    tupleDescs = new HashMap<TupleId, TupleDescriptor>();
    referencedTables = new ArrayList<Table>();
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

  public void addReferencedTable(Table table) {
    referencedTables.add(table);
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
      // inline view of a non-constant select has a non-materialized tuple descriptor
      // in the descriptor table just for type checking, which we need to skip
      if (tupleD.getIsMaterialized()) {
        result.addToTupleDescriptors(tupleD.toThrift());
        // an inline view of a constant select has a materialized tuple
        // but its table has no id
        if (tupleD.getTable() != null && tupleD.getTable().getId() != null) {
          referencedTbls.add(tupleD.getTable());
        }
        for (SlotDescriptor slotD: tupleD.getSlots()) {
          result.addToSlotDescriptors(slotD.toThrift());
        }
      }
    }
    for (Table table : referencedTables) {
      referencedTbls.add(table);
    }
    for (Table tbl: referencedTbls) {
      result.addToTableDescriptors(tbl.toThrift());
    }
    return result;
  }

  public String debugString() {
    StringBuilder out = new StringBuilder();
    out.append("tuples:\n");
    for (TupleDescriptor desc: tupleDescs.values()) {
      out.append(desc.debugString() + "\n");
    }
    return out.toString();
  }
}
