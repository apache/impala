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

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Thread-safe implementation of a bi-directional map between an
 * index of type Integer and an object of type T.  The indices are
 * allocated on demand when a reverse lookup occurs for an object
 * not already in the map.
 *
 * The forward mapping is implemented as a List<> so that it can be
 * directly used as a Thrift structure.
 */
public class ListMap<T> {
  // Maps from Integer to T.
  private List<T> list_ = Collections.synchronizedList(new ArrayList<T>());
  // Maps from T to Integer.
  private final ConcurrentHashMap<T, Integer> map_ =
      new ConcurrentHashMap<T, Integer> ();

  public List<T> getList() { return ImmutableList.copyOf(list_); }
  public int size() { return list_.size(); }

  /**
   * Map from Integer index to T object.
   */
  public T getEntry(int index) { return list_.get(index); }

  /**
   * Map from T t to Integer index. If the mapping from t doesn't
   * exist, then create a new mapping from t to a unique index.
   */
  public int getOrAddIndex(T t) {
    Integer index = map_.get(t);
    if (index != null) return index;
    // No match was found, add a new entry.
    synchronized (this) {
      // Another thread may have generated the new index, if yes
      // return that.
      index = map_.get(t);
      if (index !=null) return index;
      list_.add(t);
      index = list_.size() - 1;
      map_.put(t, index);
    }
    return index;
  }

  /**
   * Populate the bi-map from the given list. Does not perform a copy
   * of the list.
   */
  public synchronized void populate(List<T> list) {
    Preconditions.checkState(list_.isEmpty() && map_.isEmpty());
    list_ = Collections.synchronizedList(list);
    for (int i = 0; i < list_.size(); ++i) {
      map_.put(list_.get(i), i);
    }
  }
}
