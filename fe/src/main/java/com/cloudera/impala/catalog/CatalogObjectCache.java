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

package com.cloudera.impala.catalog;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Thread safe cache for storing CatalogObjects. Enforces that updates to existing
 * entries only get applied if the new/updated object has a larger catalog version.
 */
public class CatalogObjectCache<T extends CatalogObject> {
  private static final Logger LOG = Logger.getLogger(CatalogObjectCache.class);

  // Map of lower-case object name to CatalogObject. New entries are added
  // by calling add(). Updates of the cache must be synchronized because adding
  // new entries may require two cache accesses that must be performed atomically.
  // TODO: For simplicity, consider using a (non-concurrent) HashMap and marking
  // all methods as synchronized.
  private final ConcurrentHashMap<String, T> metadataCache_ =
      new ConcurrentHashMap<String, T>();

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
    T existingItem = metadataCache_.putIfAbsent(
        catalogObject.getName().toLowerCase(), catalogObject);
    if (existingItem == null) return true;

    if (existingItem.getCatalogVersion() < catalogObject.getCatalogVersion()) {
      // When existingItem != null it indicates there was already an existing entry
      // associated with the key. Add the updated object iff it has a catalog
      // version greater than the existing entry.
      metadataCache_.put(catalogObject.getName().toLowerCase(), catalogObject);
      return true;
    }
    return false;
  }

  /**
   * Removes an item from the metadata cache and returns the removed item, or null
   * if no item was removed.
   */
  public synchronized T remove(String name) {
    return metadataCache_.remove(name.toLowerCase());
  }

  /**
   * Clears all items in the cache.
   */
  public synchronized void clear() {
    metadataCache_.clear();
  }

  /**
   * Returns all known object names.
   */
  public List<String> getAllNames() {
    return Lists.newArrayList(metadataCache_.keySet());
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
    return metadataCache_.containsKey(name.toLowerCase());
  }

  /**
   * Returns the catalog object corresponding to the supplied name if it exists in the
   * cache, or null if there is no entry in metadataCache_ associated with this
   * key.
   */
  public T get(String name) {
    return metadataCache_.get(name.toLowerCase());
  }
}