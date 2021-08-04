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

package org.apache.impala.hive.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;

public class MutableValidReaderWriteIdListTest {
  private final String tableName = "t1";

  @Test
  public void noExceptions() {
    ValidReaderWriteIdList writeIdList = new ValidReaderWriteIdList(
        tableName, new long[0], new BitSet(), 1, Long.MAX_VALUE);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);
    String str = mutableWriteIdList.writeToString();
    assertEquals(tableName + ":1:" + Long.MAX_VALUE + "::", str);
    assertTrue(mutableWriteIdList.isWriteIdValid(1));
    assertFalse(mutableWriteIdList.isWriteIdValid(2));
  }

  @Test
  public void exceptions() {
    ValidWriteIdList writeIdList =
        new ValidReaderWriteIdList(tableName, new long[] {2L, 4L}, new BitSet(), 5, 4L);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);
    String str = mutableWriteIdList.writeToString();
    assertEquals(tableName + ":5:4:2,4:", str);
    assertTrue(mutableWriteIdList.isWriteIdValid(1));
    assertFalse(mutableWriteIdList.isWriteIdValid(2));
    assertTrue(mutableWriteIdList.isWriteIdValid(3));
    assertFalse(mutableWriteIdList.isWriteIdValid(4));
    assertTrue(mutableWriteIdList.isWriteIdValid(5));
    assertFalse(mutableWriteIdList.isWriteIdValid(6));
  }

  @Test
  public void testRangeResponse() {
    long[] exceptions = new long[1000];
    for (int i = 0; i < 1000; i++) {
      exceptions[i] = i + 100;
    }
    ValidWriteIdList writeIdList =
        new ValidReaderWriteIdList(tableName, exceptions, new BitSet(), 2000, 900);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);
    for (int i = 0; i < 100; i++) {
      assertTrue(mutableWriteIdList.isWriteIdValid(i));
    }
    assertEquals(ValidWriteIdList.RangeResponse.ALL,
        mutableWriteIdList.isWriteIdRangeValid(0, 99));
    for (int i = 100; i < 1100; i++) {
      assertFalse(mutableWriteIdList.isWriteIdValid(i));
    }
    assertEquals(ValidWriteIdList.RangeResponse.NONE,
        mutableWriteIdList.isWriteIdRangeValid(100, 1099));
    for (int i = 1100; i < 2001; i++) {
      assertTrue(mutableWriteIdList.isWriteIdValid(i));
    }
    assertFalse(mutableWriteIdList.isWriteIdValid(2001));
    assertEquals(ValidWriteIdList.RangeResponse.SOME,
        mutableWriteIdList.isWriteIdRangeValid(1100, 2001));

    // test for aborted write ids
    BitSet abortedBits = new BitSet();
    for (int i = 0; i < 1000; i++) {
      abortedBits.set(i);
    }
    writeIdList =
        new ValidReaderWriteIdList(tableName, exceptions, abortedBits, 2000, 900);
    mutableWriteIdList = new MutableValidReaderWriteIdList(writeIdList);
    for (int i = 0; i < 100; i++) {
      assertFalse(mutableWriteIdList.isWriteIdAborted(i));
    }
    assertEquals(ValidWriteIdList.RangeResponse.NONE,
        mutableWriteIdList.isWriteIdRangeAborted(0, 99));
    for (int i = 100; i < 1100; i++) {
      assertTrue(mutableWriteIdList.isWriteIdAborted(i));
    }
    assertEquals(ValidWriteIdList.RangeResponse.ALL,
        mutableWriteIdList.isWriteIdRangeAborted(100, 1099));
    for (int i = 1100; i < 2000; i++) {
      assertFalse(mutableWriteIdList.isWriteIdAborted(i));
    }
    assertEquals(ValidWriteIdList.RangeResponse.SOME,
        mutableWriteIdList.isWriteIdRangeAborted(100, 2000));
  }

  @Test
  public void testAbortedTxn() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidWriteIdList writeIdList =
        new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);

    assertTrue(mutableWriteIdList.isWriteIdAborted(2));
    assertFalse(mutableWriteIdList.isWriteIdAborted(4));
    assertFalse(mutableWriteIdList.isWriteIdAborted(6));
    assertTrue(mutableWriteIdList.isWriteIdAborted(8));
    assertFalse(mutableWriteIdList.isWriteIdAborted(10));
  }

  @Test
  public void testAddOpenWriteId() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidWriteIdList writeIdList =
        new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);

    assertFalse(mutableWriteIdList.addOpenWriteId(4));
    assertTrue(mutableWriteIdList.addOpenWriteId(13));
    String str = mutableWriteIdList.writeToString();
    assertEquals(tableName + ":13:4:4,6,10,12,13:2,8", str);
    assertTrue(mutableWriteIdList.isWriteIdValid(11));
    assertFalse(mutableWriteIdList.isWriteIdValid(12));
    assertTrue(mutableWriteIdList.isWriteIdOpen(12));
    assertFalse(mutableWriteIdList.isWriteIdValid(13));
    assertTrue(mutableWriteIdList.isWriteIdOpen(13));
  }

  @Test
  public void testAddAbortedWriteIds() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidWriteIdList writeIdList =
        new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);

    mutableWriteIdList.addOpenWriteId(13);
    assertFalse(mutableWriteIdList.addAbortedWriteIds(Collections.singletonList(2L)));
    assertTrue(mutableWriteIdList.addAbortedWriteIds(Arrays.asList(4L, 12L)));
    String str = mutableWriteIdList.writeToString();
    assertEquals(tableName + ":13:6:6,10,13:2,4,8,12", str);
    assertFalse(mutableWriteIdList.isWriteIdValid(4));
    assertTrue(mutableWriteIdList.isWriteIdAborted(4));
    assertFalse(mutableWriteIdList.isWriteIdValid(12));
    assertTrue(mutableWriteIdList.isWriteIdAborted(12));
    assertFalse(mutableWriteIdList.isWriteIdValid(13));
    assertFalse(mutableWriteIdList.isWriteIdAborted(13));
  }

  @Test
  public void testAddCommittedWriteIds() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidWriteIdList writeIdList =
        new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);

    assertFalse(mutableWriteIdList.addCommittedWriteIds(Collections.singletonList(1L)));
    assertTrue(mutableWriteIdList.addCommittedWriteIds(Arrays.asList(4L, 10L)));
    String str = mutableWriteIdList.writeToString();
    assertEquals(tableName + ":11:6:6:2,8", str);
    assertTrue(mutableWriteIdList.isWriteIdAborted(2));
    assertTrue(mutableWriteIdList.isWriteIdValid(4));
    assertFalse(mutableWriteIdList.isWriteIdValid(6));
    assertTrue(mutableWriteIdList.isWriteIdValid(10));
    assertTrue(mutableWriteIdList.isWriteIdValid(11));
  }

  @Test(expected = IllegalStateException.class)
  public void testAddAbortedToCommitted() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidWriteIdList writeIdList =
        new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);
    mutableWriteIdList.addCommittedWriteIds(Collections.singletonList(2L));
  }

  @Test
  public void testAddNotOpenToCommitted() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidWriteIdList writeIdList =
        new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);

    // write id "13L" is not open before, it should mark "12L" & "13L" open and then
    // mark "13L" committed
    assertTrue(mutableWriteIdList.addCommittedWriteIds(Collections.singletonList(13L)));
    String str = mutableWriteIdList.writeToString();
    assertEquals(tableName + ":13:4:4,6,10,12:2,8", str);
    assertTrue(mutableWriteIdList.isWriteIdOpen(12));
    assertTrue(mutableWriteIdList.isWriteIdValid(13));
  }

  @Test
  public void testAddNotOpenToAborted() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidWriteIdList writeIdList =
        new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList =
        new MutableValidReaderWriteIdList(writeIdList);

    // write id "13L" is not open before, it should mark "12L" & "13L" open and then
    // mark "13L" committed
    assertTrue(mutableWriteIdList.addAbortedWriteIds(Collections.singletonList(13L)));
    String str = mutableWriteIdList.writeToString();
    assertEquals(tableName + ":13:4:4,6,10,12:2,8,13", str);
    assertTrue(mutableWriteIdList.isWriteIdOpen(12));
    assertTrue(mutableWriteIdList.isWriteIdAborted(13));
  }
}