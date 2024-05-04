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

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
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
    info1.hashThrift(new TUniqueId(1L, 2L));
    info1.finalize();

    TupleCacheInfo info2 = new TupleCacheInfo(null);
    info2.hashThrift(new TUniqueId(1L, 2L));
    info2.finalize();

    assertEquals(info1.getHashTrace(), "TUniqueId(hi:1, lo:2)");
    assertEquals(info1.getHashTrace(), info2.getHashTrace());
    // Hashes are stable over time, so check the actual hash value
    assertEquals(info1.getHashString(), "b3f5384f81770c6adb83209b2a171dfa");
    assertEquals(info1.getHashString(), info2.getHashString());
  }

  @Test
  public void testMergeHash() {
    // This test doesn't need a DescriptorTable, so it just sets it to null.
    TupleCacheInfo child1 = new TupleCacheInfo(null);
    child1.hashThrift(new TUniqueId(1L, 2L));
    child1.finalize();

    TupleCacheInfo child2 = new TupleCacheInfo(null);
    child2.hashThrift(new TUniqueId(3L, 4L));
    child2.finalize();

    TupleCacheInfo parent = new TupleCacheInfo(null);
    parent.mergeChild(child1);
    parent.mergeChild(child2);
    parent.hashThrift(new TUniqueId(5L, 6L));
    parent.finalize();

    assertEquals(parent.getHashTrace(),
        "TUniqueId(hi:1, lo:2)TUniqueId(hi:3, lo:4)TUniqueId(hi:5, lo:6)");
    // Hashes are stable over time, so check the actual hash value
    assertEquals(parent.getHashString(), "edf5633bed2280c3c3edb703182f3122");
  }

  @Test
  public void testMergeEligibility() {
    // This test doesn't need a DescriptorTable, so it just sets it to null.
    // Child 1 is eligible
    TupleCacheInfo child1 = new TupleCacheInfo(null);
    child1.hashThrift(new TUniqueId(1L, 2L));
    child1.finalize();
    assertTrue(child1.isEligible());

    // Child 2 is ineligible
    TupleCacheInfo child2 = new TupleCacheInfo(null);
    child2.setIneligible(TupleCacheInfo.IneligibilityReason.NOT_IMPLEMENTED);
    child2.finalize();
    assertTrue(!child2.isEligible());

    TupleCacheInfo parent = new TupleCacheInfo(null);
    parent.mergeChild(child1);
    // Still eligible after adding child1 without child2
    assertTrue(parent.isEligible());
    parent.mergeChild(child2);
    // It is allowed to check eligibility before finalize()
    assertTrue(!parent.isEligible());
    parent.finalize();

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
    child1.hashThrift(new TUniqueId(1L, 2L));
    child1.registerTuple(tuple1.getId());
    child1.finalize();
    assertEquals(child1.getLocalTupleId(tuple1.getId()).asInt(), 0);
    assertEquals(child1.getLocalSlotId(t1slot.getId()).asInt(), 0);
    String child1ExpectedHashTrace = "TUniqueId(hi:1, lo:2)" +
        "TTupleDescriptor(id:0, byteSize:5, numNullBytes:1)" +
        "TSlotDescriptor(id:0, parent:0, slotType:TColumnType(types:[" +
        "TTypeNode(type:SCALAR, scalar_type:TScalarType(type:INT))]), " +
        "materializedPath:[], byteOffset:0, nullIndicatorByte:4, nullIndicatorBit:0, " +
        "slotIdx:0, virtual_col_type:NONE)";
    assertEquals(child1.getHashTrace(), child1ExpectedHashTrace);

    // To demonstrate why we're doing this, child2 uses the same TUniqueId as
    // child1, but different tuple / slot ids.
    TupleCacheInfo child2 = new TupleCacheInfo(descTbl);
    child2.hashThrift(new TUniqueId(1L, 2L));
    child2.registerTuple(tuple2.getId());
    child2.finalize();
    // Note: we expect the id's to be translated to local ids, so even though this is
    // tuple 2 and slot 2, this will still have TupleId=0 and SlotId=0. In fact, at this
    // point the only difference between child1 and child2 is the TUniqueId.
    assertEquals(child2.getLocalTupleId(tuple2.getId()).asInt(), 0);
    assertEquals(child2.getLocalSlotId(t2slot.getId()).asInt(), 0);
    // Because of the translation, child2's hash is the same as child1.
    assertEquals(child2.getHashTrace(), child1ExpectedHashTrace);
    assertEquals(child2.getHashString(), child1.getHashString());

    // Merge the children in opposite order. This means that every index is different
    // from its original index in the descriptor table.
    TupleCacheInfo parent = new TupleCacheInfo(descTbl);
    parent.mergeChild(child2);
    parent.mergeChild(child1);
    parent.finalize();

    // Tuple1 = second index
    // Tuple2 = first index
    // Slot1 = second index
    // Slot2 = first index
    assertEquals(parent.getLocalTupleId(tuple1.getId()).asInt(), 1);
    assertEquals(parent.getLocalTupleId(tuple2.getId()).asInt(), 0);
    assertEquals(parent.getLocalSlotId(t1slot.getId()).asInt(), 1);
    assertEquals(parent.getLocalSlotId(t2slot.getId()).asInt(), 0);
    assertEquals(parent.getHashTrace(),
        child1ExpectedHashTrace + child1ExpectedHashTrace);
  }
}
