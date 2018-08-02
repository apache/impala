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

package org.apache.impala.catalog;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

/**
 * Perform various operations on CatalogObjectVersionSet while applying the
 * same operations to a simple O(n) list structure. The semantics should be
 * the same.
 */
public class CatalogObjectVersionSetTest {
  private CatalogObjectVersionSet set_ = new CatalogObjectVersionSet();
  private List<Long> list_ = new ArrayList<>();

  private void doAdd(long v) {
    set_.addVersion(v);
    list_.add(v);
    checkConsistency();
  }

  private void doRemove(long v) {
    set_.removeVersion(v);
    list_.remove(v);
    checkConsistency();
  }

  private void doUpdate(long from, long to) {
    set_.updateVersions(from, to);
    list_.remove(from);
    list_.add(to);
    checkConsistency();
  }

  private void checkConsistency() {
    // We only need to check that the mininum element API returns the correct result
    // matching the minimum in the built-in collection implementation. The
    // CatalogObjectVersionSet doesn't expose other APIs like iteration, contains, etc.
    if (list_.isEmpty()) {
      assertEquals(0, set_.getMinimumVersion());
    } else {
      assertEquals((long)Collections.min(list_), set_.getMinimumVersion());
    }
  }

  @Test
  public void testAddRemove() {
    assertEquals(0, set_.getMinimumVersion());
    doAdd(10);
    doAdd(20);
    doAdd(5);
    // Another entry matching the minimum.
    doAdd(5);
    // Removing it once should still leave a second copy of the element there.
    doRemove(5);

    // Removing a second time should yield the next minimum version.
    doRemove(5);
    doRemove(10);
    doRemove(20);
  }

  @Test
  public void testUpdate() {
    doAdd(10);
    doUpdate(10, 20);
    doUpdate(20, 30);
    doUpdate(30, 10);
    doAdd(10);
    doUpdate(10, 20);
    doRemove(10);
    doRemove(20);
  }

  @Test
  public void testRemoveNonExistentVersion() {
    // This currently does not throw.
    doRemove(10);
  }

}
