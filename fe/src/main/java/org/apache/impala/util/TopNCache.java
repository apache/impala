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

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Thread-safe class that represents a TOP-N cache of items. It stores the top-N items of
 * a generic type T based on a user-specified ranking function that returns a numeric
 * value (long).
 *
 * The cache has a maximum capacity (N) of stored items. The implementation allows two
 * policies with respect to the way new items are handled when maximum capacity is
 * reached:
 * a) Always evict policy: A new item will always replace the item with the lowest rank
 * according to the specified ranking function even if the rank of the newly added
 * function is lower than the one to be replaced.
 * b) Rank-based eviction policy: A new item will be added to the cache iff its rank is
 * higher than the smallest rank in the cache and the item with that rank will be evicted.
 *
 * TODO: Replace these two policies with an LFU cache with dynamic aging.
 */
public final class TopNCache<T, R extends Long>  {

  // Function used to rank items stored in this cache.
  private final Function<T, R> function_;
  // Maximum capacity of this cache.
  private final int maxCapacity_;
  // The cache is stored as a priority queue.
  private final PriorityQueue<T> heap_;
  // Determines the eviction policy to apply when the cache reaches maximum capacity.
  // TODO: Convert to enum?
  private final boolean alwaysEvictAtCapacity_;

  /**
   * Compares the ranks of two T objects, returning 0 if they are equal, < 0 if the rank
   * of the first is smaller, or > 0 if the rank of the first object is larger.
   */
  private int compareRanks(T t1, T t2) {
    return function_.apply(t1).compareTo(function_.apply(t2));
  }

  public TopNCache(Function<T, R> f, int maxCapacity, boolean evictAtCapacity) {
    Preconditions.checkNotNull(f);
    Preconditions.checkState(maxCapacity > 0);
    function_ = f;
    maxCapacity_ = maxCapacity;
    heap_ = new PriorityQueue<T>(maxCapacity_,
        new Comparator<T>() {
          @Override
          public int compare(T t1, T t2) { return compareRanks(t1, t2); }
        }
    );
    alwaysEvictAtCapacity_ = evictAtCapacity;
  }

  /**
   * Adds or updates an item in the cache. If the item already exists, its rank position
   * is refreshed by removing and adding the item back to the cache. If the item is not in
   * the cache and maximum capacity hasn't been reached, the item is added to the cache.
   * Otherwise, the eviction policy is applied and the item either replaces the cache item
   * with the lowest rank or it is rejected from the cache if its rank is lower than the
   * lowest rank in the cache.
   */
  public synchronized void putOrUpdate(T item) {
    if (!heap_.remove(item)) {
      if (heap_.size() == maxCapacity_) {
        if (!alwaysEvictAtCapacity_ && compareRanks(item, heap_.peek()) <= 0) {
          return;
        }
        heap_.poll();
      }
    }
    heap_.add(item);
  }

  /**
   * Removes an item from the cache.
   */
  public synchronized void remove(T item) { heap_.remove(item); }

  /**
   * Removes all items from the cache.
   */
  @VisibleForTesting
  public synchronized void removeAll() {
    heap_.clear();
  }

  /**
   * Returns the list of all the items in the cache.
   */
  public synchronized List<T> listEntries() { return ImmutableList.copyOf(heap_); }
}
