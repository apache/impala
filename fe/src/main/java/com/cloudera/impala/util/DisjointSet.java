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

package com.cloudera.impala.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
  private final Set<Set<T>> uniqueSets_ = Sets.newHashSet();

  /**
   * Returns the item set corresponding to the given item or null if it
   * doesn't exist.
   */
  public Set<T> get(T item) { return itemSets_.get(item); }

  public Set<Set<T>> getSets() { return uniqueSets_; }

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
    uniqueSets_.add(s);
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
    uniqueSets_.remove(updateItems);
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
    Set<Set<T>> validatedSets = Sets.newHashSet();
    for (Set<T> itemSet: itemSets_.values()) {
      // Avoid checking the same item set multiple times.
      if (validatedSets.contains(itemSet)) continue;
      // Validate that all items in this set are properly mapped to
      // the set itself.
      for (T item: itemSet) {
        if (itemSet != itemSets_.get(item)) {
          throw new IllegalStateException("DisjointSet is in an inconsistent state.");
        }
      }
      validatedSets.add(itemSet);
    }
  }
}
