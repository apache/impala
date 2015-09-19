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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.thrift.TDescriptorTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Repository for tuple (and slot) descriptors.
 * Descriptors should only be created through this class, which assigns
 * them unique ids.
 */
public class DescriptorTable {
  private final HashMap<TupleId, TupleDescriptor> tupleDescs_ = Maps.newHashMap();
  private final HashMap<SlotId, SlotDescriptor> slotDescs_ = Maps.newHashMap();
  private final IdGenerator<TupleId> tupleIdGenerator_ = TupleId.createGenerator();
  private final IdGenerator<SlotId> slotIdGenerator_ = SlotId.createGenerator();
  // List of referenced tables with no associated TupleDescriptor to ship to the BE.
  // For example, the output table of an insert query.
  private final List<Table> referencedTables_ = Lists.newArrayList();
  // For each table, the set of partitions that are referenced by at least one scan range.
  private final HashMap<Table, HashSet<Long>> referencedPartitionsPerTable_ =
      Maps.newHashMap();

  public TupleDescriptor createTupleDescriptor(String debugName) {
    TupleDescriptor d = new TupleDescriptor(tupleIdGenerator_.getNextId(), debugName);
    tupleDescs_.put(d.getId(), d);
    return d;
  }

  /**
   * Create copy of src with new id. The returned descriptor has its mem layout
   * computed.
   */
  public TupleDescriptor copyTupleDescriptor(TupleId srcId, String debugName) {
    TupleDescriptor d = new TupleDescriptor(tupleIdGenerator_.getNextId(), debugName);
    tupleDescs_.put(d.getId(), d);
    // create copies of slots
    TupleDescriptor src = tupleDescs_.get(srcId);
    for (SlotDescriptor slot: src.getSlots()) {
      copySlotDescriptor(d, slot);
    }
    d.computeMemLayout();
    Preconditions.checkState(d.getByteSize() == src.getByteSize());
    return d;
  }

  public SlotDescriptor addSlotDescriptor(TupleDescriptor d) {
    SlotDescriptor result = new SlotDescriptor(slotIdGenerator_.getNextId(), d);
    d.addSlot(result);
    slotDescs_.put(result.getId(), result);
    return result;
  }

  /**
   * Append copy of src to dest.
   */
  public SlotDescriptor copySlotDescriptor(TupleDescriptor dest, SlotDescriptor src) {
    SlotDescriptor result = new SlotDescriptor(slotIdGenerator_.getNextId(), dest, src);
    dest.addSlot(result);
    slotDescs_.put(result.getId(), result);
    return result;
  }

  public TupleDescriptor getTupleDesc(TupleId id) { return tupleDescs_.get(id); }
  public SlotDescriptor getSlotDesc(SlotId id) { return slotDescs_.get(id); }
  public Collection<TupleDescriptor> getTupleDescs() { return tupleDescs_.values(); }
  public Collection<SlotDescriptor> getSlotDescs() { return slotDescs_.values(); }
  public TupleId getMaxTupleId() { return tupleIdGenerator_.getMaxId(); }
  public SlotId getMaxSlotId() { return slotIdGenerator_.getMaxId(); }

  public void addReferencedTable(Table table) {
    referencedTables_.add(table);
  }

  /**
   * Find the set of referenced partitions for the given table.  Allocates a set if
   * none has been allocated for the table yet.
   */
  private HashSet<Long> getReferencedPartitions(Table table) {
    HashSet<Long> refPartitions = referencedPartitionsPerTable_.get(table);
    if (refPartitions == null) {
      refPartitions = new HashSet<Long>();
      referencedPartitionsPerTable_.put(table, refPartitions);
    }
    return refPartitions;
  }

  /**
   * Add the partition with ID partitionId to the set of referenced partitions for the
   * given table.
   */
  public void addReferencedPartition(Table table, long partitionId) {
    getReferencedPartitions(table).add(partitionId);
  }

  /**
   * Marks all slots in list as materialized.
   */
  public void markSlotsMaterialized(List<SlotId> ids) {
    for (SlotId id: ids) {
      getSlotDesc(id).setIsMaterialized(true);
    }
  }

  /**
   * Return all ids in slotIds that belong to tupleId.
   */
  public List<SlotId> getTupleSlotIds(List<SlotId> slotIds, TupleId tupleId) {
    List<SlotId> result = Lists.newArrayList();
    for (SlotId id: slotIds) {
      if (getSlotDesc(id).getParent().getId().equals(tupleId)) result.add(id);
    }
    return result;
  }

  // Computes physical layout parameters of all descriptors.
  // Call this only after the last descriptor was added.
  // Test-only.
  public void computeMemLayout() {
    for (TupleDescriptor d: tupleDescs_.values()) {
      d.computeMemLayout();
    }
  }

  public TDescriptorTable toThrift() {
    TDescriptorTable result = new TDescriptorTable();
    HashSet<Table> referencedTbls = Sets.newHashSet();
    HashSet<Table> allPartitionsTbls = Sets.newHashSet();
    for (TupleDescriptor tupleDesc: tupleDescs_.values()) {
      // inline view of a non-constant select has a non-materialized tuple descriptor
      // in the descriptor table just for type checking, which we need to skip
      if (tupleDesc.isMaterialized()) {
        // TODO: Ideally, we should call tupleDesc.checkIsExecutable() here, but there
        // currently are several situations in which we send materialized tuples without
        // a mem layout to the BE, e.g., when unnesting unions or when replacing plan
        // trees with an EmptySetNode.
        result.addToTupleDescriptors(tupleDesc.toThrift());
        Table table = tupleDesc.getTable();
        if (table != null && !(table instanceof View)) referencedTbls.add(table);
        for (SlotDescriptor slotD: tupleDesc.getSlots()) {
          result.addToSlotDescriptors(slotD.toThrift());
        }
      }
    }
    for (Table table: referencedTables_) {
      referencedTbls.add(table);
      // We don't know which partitions are needed for INSERT, so include them all.
      allPartitionsTbls.add(table);
    }
    for (Table tbl: referencedTbls) {
      HashSet<Long> referencedPartitions = null; // null means include all partitions.
      if (!allPartitionsTbls.contains(tbl)) {
        referencedPartitions = getReferencedPartitions(tbl);
      }
      result.addToTableDescriptors(tbl.toThriftDescriptor(referencedPartitions));
    }
    return result;
  }

  public String debugString() {
    StringBuilder out = new StringBuilder();
    out.append("tuples:\n");
    for (TupleDescriptor desc: tupleDescs_.values()) {
      out.append(desc.debugString() + "\n");
    }
    return out.toString();
  }
}
