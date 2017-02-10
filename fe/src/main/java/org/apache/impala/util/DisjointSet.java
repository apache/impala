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

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Basic implementation of the disjoint-set data structure.
 * Stores a set of disjoint item sets and provides efficient implementations of mainly
 * two operations:
 * 1. Find the item set corresponding to a given member item (get() function)
 * 2. Compute the union of two item sets (union() function)
 */
public class DisjointSet<T> {
  // Maps from an item to its item set.
  private final Map<T, Set<T>> itemSets_ = Maps.newHashMap();

  // Stores the set of item sets in its keySet(). All keys are mapped to the same
  // dummy value which is only used for validating the removal of entries.
  // An IdentityHashMap is needed here instead of a regular HashSet because the Set<T>
  // elements are mutated after inserting them. In a conventional HashSet mutating
  // elements after they are inserted may cause problems because the hashCode() of the
  // object during insertion may be different than its hashCode() after mutation. So,
  // after changing the element it may not be possible to remove/find it again in the
  // HashSet (looking in the wrong hash bucket), even using the same object reference.
  private final IdentityHashMap<Set<T>, Object> uniqueSets_ =
      new IdentityHashMap<Set<T>, Object>();
  private static final Object DUMMY_VALUE = new Object();

  /**
   * Returns the item set corresponding to the given item or null if it
   * doesn't exist.
   */
  public Set<T> get(T item) { return itemSets_.get(item); }

  public Set<Set<T>> getSets() { return uniqueSets_.keySet(); }

  /**
   * Registers a new item set with a single item. Returns the new item set.
   * Throws if such an item set already exists.
   */
  public Set<T> makeSet(T item) {
    if (itemSets_.containsKey(item)) {
      throw new IllegalStateException(
          "Item set for item already exists: " + item.toString());
    }
    Set<T> s = Sets.newHashSet(item);
    itemSets_.put(item, s);
    uniqueSets_.put(s, DUMMY_VALUE);
    return s;
  }

  /**
   * Merges the two item sets belonging to the members a and b. The merged set contains
   * at least a and b even if a or b did not have an associated item set.
   * Returns false if the item sets of a and b are non-empty and already identical,
   * true otherwise.
   */
  public boolean union(T a, T b) {
    Set<T> aItems = itemSets_.get(a);
    Set<T> bItems = itemSets_.get(b);
    // check if the sets are already identical
    if (aItems != null && bItems != null && aItems == bItems) return false;

    // union(x, x) is equivalent to makeSet(x)
    if (a.equals(b) && aItems == null) {
      makeSet(a);
      return true;
    }

    // create sets for a or b if not present already
    if (aItems == null) aItems = makeSet(a);
    if (bItems == null) bItems = makeSet(b);

    // will contain the union of aItems and bItems
    Set<T> mergedItems = aItems;
    // always the smaller of the two sets to be merged
    Set<T> updateItems = bItems;
    if (bItems.size() > aItems.size()) {
      mergedItems = bItems;
      updateItems = aItems;
    }
    for (T item: updateItems) {
      mergedItems.add(item);
      itemSets_.put(item, mergedItems);
    }
    Object removedValue = uniqueSets_.remove(updateItems);
    Preconditions.checkState(removedValue == DUMMY_VALUE);
    return true;
  }

  /**
   * Merges all the item sets corresponding to the given items. Returns true if any item
   * sets were merged or created, false otherwise (item sets are already identical).
   */
  public boolean bulkUnion(Collection<T> items) {
    if (items.isEmpty()) return false;
    Iterator<T> it = items.iterator();
    T head = it.next();
    // bulkUnion(x) is equivalent to makeSet(x)
    if (!it.hasNext()) {
      if (get(head) != null) return false;
      makeSet(head);
      return true;
    }
    boolean result = false;
    while(it.hasNext()) {
      boolean changed = union(head, it.next());
      result = result || changed;
    }
    return result;
  }

  /**
   * Checks the internal consistency of this data structure.
   * Throws an IllegalStateException if an inconsistency is detected.
   */
  public void checkConsistency() {
    // Validate map from item to item set.
    Set<Set<T>> validatedSets = Sets.newHashSet();
    for (Set<T> itemSet: itemSets_.values()) {
      // Avoid checking the same item set multiple times.
      if (validatedSets.contains(itemSet)) continue;
      // Validate that all items in this set are properly mapped to
      // the set itself.
      for (T item: itemSet) {
        if (itemSet != itemSets_.get(item)) {
          throw new IllegalStateException(
              "DisjointSet is in an inconsistent state. Failed item set validation.");
        }
      }
      validatedSets.add(itemSet);
    }

    // Validate set of item sets. Every element should appear in exactly one item set.
    Set<T> seenItems = Sets.newHashSet();
    for (Set<T> itemSet: uniqueSets_.keySet()) {
      for (T item: itemSet) {
        if (!seenItems.add(item)) {
          throw new IllegalStateException(
              "DisjointSet is in an inconsistent state. Failed unique set validation.");
        }
      }
    }
  }
}
