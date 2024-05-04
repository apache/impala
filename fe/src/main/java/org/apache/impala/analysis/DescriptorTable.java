// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.IcebergTimeTravelTable;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.IdGenerator;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TDescriptorTable;
import org.apache.impala.thrift.TDescriptorTableSerialized;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Repository for tuple (and slot) descriptors.
 * Descriptors should only be created through this class, which assigns
 * them unique ids.
 */
public class DescriptorTable {
  private final Map<TupleId, TupleDescriptor> tupleDescs_ = new HashMap<>();
  private final Map<SlotId, SlotDescriptor> slotDescs_ = new HashMap<>();
  private final IdGenerator<TupleId> tupleIdGenerator_ = TupleId.createGenerator();
  private final IdGenerator<SlotId> slotIdGenerator_ = SlotId.createGenerator();
  // The target table of a table sink, may be null.
  // Table id 0 is reserved for it. Set in QueryStmt.analyze() that produces a table sink,
  // e.g. InsertStmt.analyze(), ModifyStmt.analyze().
  private FeTable targetTable_;
  // Sometimes we have multiple target tables (e.g. Iceberg UPDATEs), in which case
  // we can reserve multiple ids for the target tables.
  private final Map<FeTable, Integer> additionalTargetTableIds_ = new HashMap<>();
  // For each table, the set of partitions that are referenced by at least one
  // scan range.
  private final Map<FeTable, Set<Long>> referencedPartitionsPerTable_ = new HashMap<>();
  // 0 is reserved for table sinks
  public static final int TABLE_SINK_ID = 0;
  // Table id counter for a single query.
  private int nextTableId_ = TABLE_SINK_ID + 1;

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
    // Copy path from source and create copies of slots
    TupleDescriptor src = tupleDescs_.get(srcId);
    if (src.getPath() != null) d.setPath(src.getPath());
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
  public Collection<SlotDescriptor> getSlotDescs() { return slotDescs_.values(); }
  public SlotId getMaxSlotId() { return slotIdGenerator_.getMaxId(); }

  public void setTargetTable(FeTable table) { targetTable_ = table; }

  public int addTargetTable(FeTable table) {
    int id = nextTableId_++;
    additionalTargetTableIds_.put(table, id);
    return id;
  }

  /**
   * Find the set of referenced partitions for the given table.  Allocates a set if
   * none has been allocated for the table yet.
   */
  private Set<Long> getReferencedPartitions(FeTable table) {
    Set<Long> refPartitions = referencedPartitionsPerTable_.get(table);
    if (refPartitions == null) {
      refPartitions = new HashSet<>();
      referencedPartitionsPerTable_.put(table, refPartitions);
    }
    return refPartitions;
  }

  /**
   * Add the partition with ID partitionId to the set of referenced partitions for the
   * given table.
   */
  public void addReferencedPartition(FeTable table, long partitionId) {
    getReferencedPartitions(table).add(Long.valueOf(partitionId));
  }

  /**
   * Marks all slots in list as materialized and return the affected Tuples.
   */
  public Set<TupleDescriptor> markSlotsMaterialized(List<SlotId> ids) {
    Set<TupleDescriptor> affectedTuples = Sets.newHashSet();
    for (SlotId id: ids) {
      SlotDescriptor slotDesc = getSlotDesc(id);
      if (slotDesc.isMaterialized()) continue;
      slotDesc.setIsMaterialized(true);
      // If we are inside a struct, we need to materialise all enclosing struct
      // SlotDescriptors, otherwise these descriptors will be hanging in the air.
      materializeParentStructSlots(slotDesc);
      // Don't add the TupleDescriptor that is for struct children.
      if (slotDesc.getParent().getParentSlotDesc() == null) {
        affectedTuples.add(slotDesc.getParent());
      }
    }
    return affectedTuples;
  }

  private void materializeParentStructSlots(SlotDescriptor desc) {
    for (SlotDescriptor slotDesc : desc.getEnclosingStructSlotDescs()) {
      slotDesc.setIsMaterialized(true);
    }
  }

  /**
   * Computes physical layout parameters of all descriptors.
   * Call this only after the last descriptor was added.
   * Test-only.
   */
  public void computeMemLayout() {
    for (TupleDescriptor d: tupleDescs_.values()) d.computeMemLayout();
  }

  /**
   * Returns the thrift representation of this DescriptorTable. Assign unique ids to all
   * distinct tables and set them in tuple descriptors as necessary.
   */
  public TDescriptorTable toThrift() {
    TDescriptorTable result = new TDescriptorTable();
    ThriftSerializationCtx serialCtx = new ThriftSerializationCtx();
    // Maps from base table to its table id used in the backend.
    Map<FeTable, Integer> tableIdMap = new HashMap<>();
    // Used to check table level consistency
    Map<TableName, FeTable> referencedTables = new HashMap<>();

    if (targetTable_ != null) {
      tableIdMap.put(targetTable_, TABLE_SINK_ID);
      referencedTables.put(targetTable_.getTableName(), targetTable_);
    }
    for (Map.Entry<FeTable, Integer> tableIdEntry :
        additionalTargetTableIds_.entrySet()) {
      FeTable targetTable = tableIdEntry.getKey();
      tableIdMap.put(targetTable, tableIdEntry.getValue());
      referencedTables.put(targetTable.getTableName(), targetTable);
    }
    for (TupleDescriptor tupleDesc: tupleDescs_.values()) {
      // inline view of a non-constant select has a non-materialized tuple descriptor
      // in the descriptor table just for type checking, which we need to skip
      if (!tupleDesc.isMaterialized()) continue;
      FeTable table = tupleDesc.getTable();
      Integer tableId = tableIdMap.get(table);
      if (table != null && !(table instanceof FeView)) {
        TableName tblName = table.getTableName();
        // Verify table level consistency in the same query by checking that references to
        // the same Table refer to the same table instance.
        FeTable checkTable = referencedTables.get(tblName);
        Preconditions.checkState(checkTable == null || isSameTableRef(table, checkTable));
        if (tableId == null) {
          tableId = nextTableId_++;
          tableIdMap.put(table, tableId);
          referencedTables.put(tblName, table);
        }
      }
      // TODO: Ideally, we should call tupleDesc.checkIsExecutable() here, but there
      // currently are several situations in which we send materialized tuples without
      // a mem layout to the BE, e.g., when unnesting unions or when replacing plan
      // trees with an EmptySetNode.
      result.addToTupleDescriptors(tupleDesc.toThrift(tableId, serialCtx));
      // Only serialize materialized slots
      for (SlotDescriptor slotD: tupleDesc.getMaterializedSlots()) {
        result.addToSlotDescriptors(slotD.toThrift(serialCtx));
      }
    }
    for (FeTable tbl: tableIdMap.keySet()) {
      Set<Long> referencedPartitions = null; // null means include all partitions.
      // We don't know which partitions are needed for INSERT, so do not prune partitions.
      if (tbl != targetTable_ && !additionalTargetTableIds_.containsKey(tbl)) {
        referencedPartitions = getReferencedPartitions(tbl);
      }
      result.addToTableDescriptors(
          tbl.toThriftDescriptor(tableIdMap.get(tbl), referencedPartitions));
    }
    return result;
  }

  /**
   * @return true if the two tables are the same.
   * For Iceberg Time Travel tables we compare the underlying base table.
   */
  private boolean isSameTableRef(FeTable first, FeTable second) {
    if (first instanceof IcebergTimeTravelTable) {
      first = ((IcebergTimeTravelTable) first).getBase();
    }
    if (second instanceof IcebergTimeTravelTable) {
      second = ((IcebergTimeTravelTable) second).getBase();
    }
    return first == second;
  }

  public TDescriptorTableSerialized toSerializedThrift() throws ImpalaException {
    TDescriptorTableSerialized result = new TDescriptorTableSerialized();
    TDescriptorTable desc_tbl = toThrift();
    result.setThrift_desc_tbl(JniUtil.serializeToThrift(desc_tbl));
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

  /**
   * Creates a thrift descriptor table for testing. Each entry in 'slotTypes' is a list
   * of slot types for one tuple.
   */
  public static TDescriptorTable buildTestDescriptorTable(
      List<List<TColumnType>> slotTypes) {
    DescriptorTable descTbl = new DescriptorTable();
    for (List<TColumnType> ttupleSlots: slotTypes) {
      List<StructField> fields = Lists.newArrayListWithCapacity(ttupleSlots.size());
      for (TColumnType ttype: ttupleSlots) {
        fields.add(new StructField("testField", Type.fromThrift(ttype)));
      }
      StructType tupleType = new StructType(fields);
      createTupleDesc(tupleType, descTbl);
    }
    descTbl.computeMemLayout();
    return descTbl.toThrift();
  }

  /**
   * Recursive helper for buildTestDescriptorTable(). Returns a TupleDescriptor
   * corresponding to the given struct. The struct may contain scalar and array fields.
   */
  private static TupleDescriptor createTupleDesc(StructType tupleType,
      DescriptorTable descTbl) {
    TupleDescriptor tupleDesc = descTbl.createTupleDescriptor("testDescTbl");
    for (StructField field: tupleType.getFields()) {
      Type type = field.getType();
      SlotDescriptor slotDesc = descTbl.addSlotDescriptor(tupleDesc);
      slotDesc.setIsMaterialized(true);
      slotDesc.setType(type);
      if (!type.isCollectionType()) continue;

      // Set item tuple descriptor for the collection.
      Preconditions.checkState(type.isArrayType());
      ArrayType arrayType = (ArrayType) type;
      Type itemType = arrayType.getItemType();
      StructType itemStruct = null;
      if (itemType.isStructType()) {
        itemStruct = (StructType) itemType;
      } else {
        List<StructField> itemFields = Lists.newArrayListWithCapacity(1);
        itemFields.add(new StructField("item", itemType));
        itemStruct = new StructType(itemFields);
      }
      TupleDescriptor itemTuple = createTupleDesc(itemStruct, descTbl);
      slotDesc.setItemTupleDesc(itemTuple);
    }
    return tupleDesc;
  }
}
