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

package org.apache.impala.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.planner.TupleCacheInfo.HashTraceElement;
import org.apache.impala.thrift.TUniqueId;

import org.junit.Test;

/**
 * Basic unit tests for TupleCacheInfo
 */
public class TupleCacheInfoTest {

  @Test
  public void testHashThrift() {
    // This test doesn't need a DescriptorTable, so it just sets it to null.
    TupleCacheInfo info1 = new TupleCacheInfo(null);
    info1.hashThrift("info1", new TUniqueId(1L, 2L));
    info1.finalizeHash();
    List<HashTraceElement> info1HashTraces = info1.getHashTraces();
    assertEquals(info1HashTraces.size(), 1);
    assertEquals("info1", info1HashTraces.get(0).getComment());
    assertEquals("TUniqueId(hi:1, lo:2)", info1HashTraces.get(0).getHashTrace());

    TupleCacheInfo info2 = new TupleCacheInfo(null);
    info2.hashThrift("info2", new TUniqueId(1L, 2L));
    info2.finalizeHash();
    List<HashTraceElement> info2HashTraces = info2.getHashTraces();
    assertEquals(info1HashTraces.size(), info2HashTraces.size());
    assertEquals("info2", info2HashTraces.get(0).getComment());
    assertEquals(info1HashTraces.get(0).getHashTrace(),
        info2HashTraces.get(0).getHashTrace());

    // Hashes are stable over time, so check the actual hash value
    assertEquals("b3f5384f81770c6adb83209b2a171dfa", info1.getHashString());
    assertEquals(info1.getHashString(), info2.getHashString());
  }

  @Test
  public void testMergeHash() {
    // This test doesn't need a DescriptorTable, so it just sets it to null.
    TupleCacheInfo child1 = new TupleCacheInfo(null);
    child1.hashThrift("child1", new TUniqueId(1L, 2L));
    child1.finalizeHash();

    TupleCacheInfo child2 = new TupleCacheInfo(null);
    child2.hashThrift("child2", new TUniqueId(3L, 4L));
    child2.finalizeHash();

    TupleCacheInfo parent = new TupleCacheInfo(null);
    parent.mergeChild("child1", child1);
    parent.mergeChild("child2", child2);
    parent.hashThrift("parent", new TUniqueId(5L, 6L));
    parent.finalizeHash();

    // The hash trace includes the hash of the child, but not the full hash trace
    List<HashTraceElement> hashTraces = parent.getHashTraces();
    assertEquals(hashTraces.size(), 3);
    assertEquals("child1", hashTraces.get(0).getComment());
    assertEquals(child1.getHashString(), hashTraces.get(0).getHashTrace());
    assertEquals("child2", hashTraces.get(1).getComment());
    assertEquals(child2.getHashString(), hashTraces.get(1).getHashTrace());
    assertEquals("parent", hashTraces.get(2).getComment());
    assertEquals("TUniqueId(hi:5, lo:6)", hashTraces.get(2).getHashTrace());
    // Hashes are stable over time, so check the actual hash value
    assertEquals("edf5633bed2280c3c3edb703182f3122", parent.getHashString());
  }

  @Test
  public void testMergeEligibility() {
    // This test doesn't need a DescriptorTable, so it just sets it to null.
    // Child 1 is eligible
    TupleCacheInfo child1 = new TupleCacheInfo(null);
    child1.hashThrift("child1", new TUniqueId(1L, 2L));
    child1.finalizeHash();
    assertTrue(child1.isEligible());

    // Child 2 is ineligible
    TupleCacheInfo child2 = new TupleCacheInfo(null);
    child2.setIneligible(TupleCacheInfo.IneligibilityReason.NOT_IMPLEMENTED);
    child2.finalizeHash();
    assertTrue(!child2.isEligible());

    TupleCacheInfo parent = new TupleCacheInfo(null);
    parent.mergeChild("child1", child1);
    // Still eligible after adding child1 without child2
    assertTrue(parent.isEligible());
    parent.mergeChild("child2", child2);
    // It is allowed to check eligibility before finalizeHash()
    assertTrue(!parent.isEligible());
    parent.finalizeHash();

    assertTrue(!parent.isEligible());
  }

  @Test
  public void testIdTranslation() {
    // Create a DescriptorTable and add two tuples each with one integer slot.
    DescriptorTable descTbl = new DescriptorTable();
    TupleDescriptor tuple1 = descTbl.createTupleDescriptor("tuple1");
    assertEquals(tuple1.getId().asInt(), 0);
    SlotDescriptor t1slot = descTbl.addSlotDescriptor(tuple1);
    t1slot.setType(ScalarType.createType(PrimitiveType.INT));
    t1slot.setLabel("t1slot");
    assertEquals(t1slot.getId().asInt(), 0);
    TupleDescriptor tuple2 = descTbl.createTupleDescriptor("tuple2");
    assertEquals(tuple2.getId().asInt(), 1);
    SlotDescriptor t2slot = descTbl.addSlotDescriptor(tuple2);
    t2slot.setType(ScalarType.createType(PrimitiveType.INT));
    t2slot.setLabel("t2slot");
    assertEquals(t2slot.getId().asInt(), 1);

    tuple1.materializeSlots();
    tuple2.materializeSlots();
    descTbl.computeMemLayout();

    TupleCacheInfo child1 = new TupleCacheInfo(descTbl);
    child1.hashThrift("child1", new TUniqueId(1L, 2L));
    child1.registerTuple(tuple1.getId());
    child1.finalizeHash();
    assertEquals(child1.getLocalTupleId(tuple1.getId()).asInt(), 0);
    assertEquals(child1.getLocalSlotId(t1slot.getId()).asInt(), 0);
    List<HashTraceElement> child1HashTraces = child1.getHashTraces();
    assertEquals(3, child1HashTraces.size());
    assertEquals("child1", child1HashTraces.get(0).getComment());
    assertEquals("TUniqueId(hi:1, lo:2)", child1HashTraces.get(0).getHashTrace());
    assertEquals("TTupleDescriptor(id:0, byteSize:5, numNullBytes:1)",
        child1HashTraces.get(1).getHashTrace());
    assertEquals(
        "TSlotDescriptor(id:0, parent:0, slotType:TColumnType(types:[" +
        "TTypeNode(type:SCALAR, scalar_type:TScalarType(type:INT))]), " +
        "materializedPath:[], byteOffset:0, nullIndicatorByte:4, nullIndicatorBit:0, " +
        "slotIdx:0, virtual_col_type:NONE)",
        child1HashTraces.get(2).getHashTrace());

    // To demonstrate why we're doing this, child2 uses the same TUniqueId as
    // child1, but different tuple / slot ids.
    TupleCacheInfo child2 = new TupleCacheInfo(descTbl);
    child2.hashThrift("child2", new TUniqueId(1L, 2L));
    child2.registerTuple(tuple2.getId());
    child2.finalizeHash();
    // Note: we expect the id's to be translated to local ids, so even though this is
    // tuple 2 and slot 2, this will still have TupleId=0 and SlotId=0. In fact, at this
    // point the only difference between child1 and child2 is the TUniqueId.
    assertEquals(child2.getLocalTupleId(tuple2.getId()).asInt(), 0);
    assertEquals(child2.getLocalSlotId(t2slot.getId()).asInt(), 0);
    // Because of the translation, child2's hash is the same as child1.
    List<HashTraceElement> child2HashTraces = child1.getHashTraces();
    assertEquals(child2HashTraces, child1HashTraces);
    assertEquals(child2.getHashString(), child1.getHashString());

    // Merge the children in opposite order. This means that every index is different
    // from its original index in the descriptor table.
    TupleCacheInfo parent = new TupleCacheInfo(descTbl);
    parent.mergeChild("child2", child2);
    parent.mergeChild("child1", child1);
    parent.finalizeHash();

    // Tuple1 = second index
    // Tuple2 = first index
    // Slot1 = second index
    // Slot2 = first index
    assertEquals(parent.getLocalTupleId(tuple1.getId()).asInt(), 1);
    assertEquals(parent.getLocalTupleId(tuple2.getId()).asInt(), 0);
    assertEquals(parent.getLocalSlotId(t1slot.getId()).asInt(), 1);
    assertEquals(parent.getLocalSlotId(t2slot.getId()).asInt(), 0);

    // Parent hash trace only has entries for the two children
    List<HashTraceElement> parentHashTraces = parent.getHashTraces();
    assertEquals(parentHashTraces.size(), 2);
    assertEquals("child2", parentHashTraces.get(0).getComment());
    assertEquals(child2.getHashString(), parentHashTraces.get(0).getHashTrace());
    assertEquals("child1", parentHashTraces.get(1).getComment());
    assertEquals(child1.getHashString(), parentHashTraces.get(1).getHashTrace());
  }
}
