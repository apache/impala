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

package org.apache.impala.util;
import org.junit.Test;
import static org.junit.Assert.*;

public class IntArrayListTest {
  private void assertNaturalNumberSequence(IntArrayList arr, int size) {
    // Test data()
    assert arr.data().length >= size;
    // Test size()
    assertEquals(arr.size(), size);
    // Test get()
    for (int i = 0; i < size; ++i) assertEquals(arr.get(i), i);
    try {
      arr.get(-1);
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {}
    try {
      arr.get(arr.size());
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {}
    // Test iterator()
    IntIterator it = arr.iterator();
    for (int i = 0; i < size; ++i) {
      assert it.hasNext();
      assert it.peek() == i;
      assert it.next() == i;
    }
    assert !it.hasNext();
    try {
      it.peek();
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {}
    try {
      it.next();
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {}
  }

  private void fillNaturalNumberSequence(IntArrayList arr, int end) {
    for (int i = arr.size(); i < end; ++i) arr.add(i);
  }

  @Test
  public void testIntArrayList() {
    // Test add()
    IntArrayList arr = new IntArrayList();
    fillNaturalNumberSequence(arr, 10);
    assertNaturalNumberSequence(arr, 10);
    // Test removeLast()
    arr.removeLast(5);
    assertNaturalNumberSequence(arr, 5);
    fillNaturalNumberSequence(arr, 32);
    assertNaturalNumberSequence(arr, 32);
    try {
      arr.removeLast(33);
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {}
    try {
      arr.removeLast(Integer.MAX_VALUE);
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {}
    try {
      arr.removeLast(-1);
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {}
    arr.removeLast(0);
    assertNaturalNumberSequence(arr, 32);
    // Test set()
    arr.set(0, -1);
    assert arr.get(0) == -1;
    arr.set(31, -1);
    assert arr.get(31) == -1;
    try {
      arr.set(-1, -1);
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {}
    try {
      arr.set(32, -1);
      fail();
    } catch (ArrayIndexOutOfBoundsException e) {}
    // Test clear()
    arr.clear();
    assertNaturalNumberSequence(arr, 0);
  }
}
