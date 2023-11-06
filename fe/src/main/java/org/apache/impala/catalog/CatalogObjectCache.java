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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Thread safe cache for storing CatalogObjects. Enforces that updates to existing
 * entries only get applied if the new/updated object has a larger catalog version.
 * add() and remove() functions also update the entries of the global instance of
 * CatalogObjectVersionSet which keeps track of the catalog objects versions.
 */
public class CatalogObjectCache<T extends CatalogObject> implements Iterable<T> {
  private final boolean caseInsensitiveKeys_;

  /**
   * Creates a new instance of the CatalogObjectCache that compares keys as
   * insensitive.
   */
  public CatalogObjectCache() {
    this(true);
  }

  /**
   * Creates a new instance of the CatalogObjectCache that compares keys as case
   * insensitive/sensitive based on whether 'caseInsensitiveKeys' is true/false.
   */
  public CatalogObjectCache(boolean caseInsensitiveKeys) {
    caseInsensitiveKeys_ = caseInsensitiveKeys;
  }

  // Map of lower-case object name to CatalogObject. New entries are added
  // by calling add(). Updates of the cache must be synchronized because adding
  // new entries may require two cache accesses that must be performed atomically.
  // TODO: For simplicity, consider using a (non-concurrent) HashMap and marking
  // all methods as synchronized.
  private final Map<String, T> metadataCache_ = new ConcurrentHashMap<String, T>();

  public int size() { return metadataCache_.size(); }

  /**
   * Adds a new catalogObject to the cache. If a catalogObject with the same name already
   * exists in the cache, the new item will only be added if it has a larger catalog
   * version.
   * Synchronized because add() may require two cache accesses that must be performed
   * atomically.
   * Returns true if this item was added or false if the existing value was preserved.
   */
  public synchronized boolean add(T catalogObject) {
    Preconditions.checkNotNull(catalogObject);
    String key = catalogObject.getName();
    if (caseInsensitiveKeys_) key = key.toLowerCase();
    T existingItem = metadataCache_.putIfAbsent(key, catalogObject);
    if (existingItem == null) {
      CatalogObjectVersionSet.INSTANCE.addVersion(
          catalogObject.getCatalogVersion());
      return true;
    }

    if (existingItem.getCatalogVersion() < catalogObject.getCatalogVersion()) {
      // When existingItem != null it indicates there was already an existing entry
      // associated with the key. Add the updated object iff it has a catalog
      // version greater than the existing entry.
      metadataCache_.put(key, catalogObject);
      CatalogObjectVersionSet.INSTANCE.updateVersions(
          existingItem.getCatalogVersion(), catalogObject.getCatalogVersion());
      return true;
    }
    return false;
  }

  /**
   * Removes an item from the metadata cache and returns the removed item, or null
   * if no item was removed.
   */
  public synchronized T remove(String name) {
    if (caseInsensitiveKeys_) name = name.toLowerCase();
    T removedObject = metadataCache_.remove(name);
    if (removedObject != null) {
      CatalogObjectVersionSet.INSTANCE.removeVersion(
          removedObject.getCatalogVersion());
    }
    return removedObject;
  }

  /**
   * Clears all items in the cache.
   */
  public synchronized void clear() {
    metadataCache_.clear();
  }

  /**
   * Returns the set of all known object names. The returned set is backed by
   * the cache, so updates to the cache will be visible in the returned set
   * and vice-versa. However, updates to the cache should not be done via the
   * returned set, use add()/remove() instead.
   */
  public Set<String> keySet() {
    return metadataCache_.keySet();
  }

  /**
   * Returns all the known object values.
   */
  public List<T> getValues() {
    return Lists.newArrayList(metadataCache_.values());
  }

  /**
   * Returns true if the metadataCache_ contains a key with the given name.
   */
  public boolean contains(String name) {
    if (caseInsensitiveKeys_) name = name.toLowerCase();
    return metadataCache_.containsKey(name);
  }

  /**
   * Returns the catalog object corresponding to the supplied name if it exists in the
   * cache, or null if there is no entry in metadataCache_ associated with this
   * key.
   */
  public T get(String name) {
    if (caseInsensitiveKeys_) name = name.toLowerCase();
    return metadataCache_.get(name);
  }

  /**
   * Returns an iterator for the values in the cache. There are no guarantees
   * about the order in which elements are returned. All items at the time of
   * iterator creation will be visible and new items may or may not be visible.
   * Thread safe (will never throw a ConcurrentModificationException).
   */
  @Override
  public Iterator<T> iterator() {
    return metadataCache_.values().iterator();
  }
}