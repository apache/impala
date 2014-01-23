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
 * Lazily loads metadata on read (through getOrLoad()) and tracks the set of valid/known
 * object names. This class is thread safe. The CatalogObjectCache is created by passing
 * a custom CacheLoader object which can implement its own load() logic.
 *
 * Items are added to the cache by calling add(T catalogObject). New objects can be
 * added in an initialized or uninitialized state (determined by the isLoaded()
 * property of the CatalogObject). If a catalog object is uninitialized, getOrLoad()
 * will attempt to load its metadata. Otherwise, getOrLoad() will return the cached value.
 * Invalidation of cache entries is done by calling add() and passing in a CatalogObject
 * that has isLoaded() == false. The next access to the object will trigger a
 * metadata load.
 * The catalog cache supports parallel loading/gets of different keys. While a load is in
 * progress, any calls to get the same key will block until the load completes at which
 * point the loaded value will be returned.
 * Metadata can be reloaded. Reloading an object will perform a synchronous incremental
 * refresh the object's metadata. Depending on the load() implementation in the
 * CacheLoader, refresh might reuse some of the existing metadata which could result in
 * a partially stale object but faster load time.
 */
public class CatalogObjectCache<T extends CatalogObject> {
  private static final Logger LOG = Logger.getLogger(CatalogObjectCache.class);

  // Map of lower-case object name to CacheEntry objects. New CacheEntries are created
  // by calling add(). If a CacheEntry does not exist "getOrLoad()" will
  // return null.
  private final ConcurrentHashMap<String, CacheEntry<T>> metadataCache_ =
      new ConcurrentHashMap<String, CacheEntry<T>>();

  // The CacheLoader used to load new items into the cache.
  private final CacheLoader<String, T> cacheLoader_;

  /**
   * Stores and lazily loads CatalogObjects upon read. Ensures the CatalogObject
   * catalog versions are strictly increasing when updated with an initialized
   * (loaded) value. This class is thread safe.
   */
  private static class CacheEntry<T extends CatalogObject> {
    private final CacheLoader<String, T> cacheLoader_;
    private T catalogObject_;

    private CacheEntry(T catalogObject, CacheLoader<String, T> cacheLoader) {
      catalogObject_ = catalogObject;
      cacheLoader_ = cacheLoader;
    }

    /**
     * Replaces the CatalogObject in this CacheEntry if it is newer than the
     * existing value (the catalog version is greater) or if the existing value
     * has not yet uninitialized (has not been loaded) and the new value is
     * loaded.
     * Returns true if the existing value was replaced or false if the existing
     * value was preserved.
     */
    public synchronized boolean replaceIfNewer(T newCatalogObject) {
      Preconditions.checkNotNull(newCatalogObject);
      if (catalogObject_.getCatalogVersion() < newCatalogObject.getCatalogVersion()
          || !catalogObject_.isLoaded() && newCatalogObject.isLoaded()) {
        catalogObject_ = newCatalogObject;
        return true;
      }
      return false;
    }

    /**
     * Returns the current CatalogObject value in this CacheEntry.
     */
    public synchronized T value() { return catalogObject_; }

    /**
     * Gets the current catalog object for this CacheEntry, loading it if needed (if the
     * existing catalog object is uninitialized).
     */
    public T getOrLoad() {
      // Get the catalog version to assign the to the loaded object. It's important to
      // do this before locking, because getNextCatalogVersion() may require taking
      // a top-level lock.
      long targetCatalogVersion = cacheLoader_.getNextCatalogVersion();

      synchronized (this) {
        Preconditions.checkNotNull(catalogObject_);
        if (catalogObject_.isLoaded()) return catalogObject_;
        T loadedObject = cacheLoader_.load(catalogObject_.getName().toLowerCase(), null,
            targetCatalogVersion);
        Preconditions.checkNotNull(loadedObject);
        Preconditions.checkState(loadedObject.isLoaded());
        replaceIfNewer(loadedObject);
        return catalogObject_;
      }
    }

    /**
     * Reloads the value for this cache entry and replaces the existing value. This is
     * similar to load(), but can reuse the existing cached value to speedup loading
     * time. Will block if any existing reloads/loads are in progress, and return the
     * value the value they loaded.
     */
    public T reload() {
      // Get the catalog version to assign the to the reloaded object. It's important to
      // do this before locking, because getNextCatalogVersion() may require taking
      // a top-level lock.
      long targetCatalogVersion = cacheLoader_.getNextCatalogVersion();

      T tmpCatalogObject = catalogObject_;
      synchronized (this) {
        // Only reload if the underlying catalog object has not changed since waiting
        // on the lock OR if the catalog object is uninitialized and needs to be loaded.
        if (tmpCatalogObject == catalogObject_ || !catalogObject_.isLoaded()) {
          T loadedObject = cacheLoader_.load(catalogObject_.getName(), catalogObject_,
              targetCatalogVersion);
          Preconditions.checkNotNull(loadedObject);
          Preconditions.checkState(loadedObject.isLoaded());
          replaceIfNewer(loadedObject);
        }
        return catalogObject_;
      }
    }

    /**
     * Creates a new CacheEntry with the given key and CacheLoader.
     */
    public static <T extends CatalogObject> CacheEntry<T>
        create(T catalogObject, CacheLoader<String, T> cacheLoader) {
      return new CacheEntry<T>(catalogObject, cacheLoader);
    }
  }

  /**
   * Initializes the cache with the given CacheLoader.
   */
  public CatalogObjectCache(CacheLoader<String, T> cacheLoader) {
    cacheLoader_ = cacheLoader;
  }

  /**
   * Adds a new catalogObject to the cache. The result of this add() may be overwritten
   * by the next reload(). If a catalogObject with the same name already exists
   * in the cache, the new item will only be added if it has a larger catalog version.
   * Returns true if this item was added or false if the existing value was preserved.
   */
  public boolean add(T catalogObject) {
    Preconditions.checkNotNull(catalogObject);
    CacheEntry<T> cacheEntry =
        CacheEntry.create(catalogObject, cacheLoader_);
    CacheEntry<T> existingItem = metadataCache_.putIfAbsent(
        catalogObject.getName().toLowerCase(), cacheEntry);

    // When existingItem != null it indicates there was already an existing entry
    // associated with the key, so apply the update to the existing entry.
    // Otherwise, update the new CacheEntry.
    cacheEntry = existingItem != null ? existingItem : cacheEntry;
    return cacheEntry.replaceIfNewer(catalogObject);
  }

  /**
   * Removes an item from the metadata cache and returns the removed item, or null
   * if no item was removed.
   */
  public T remove(String name) {
    CacheEntry<T> removedItem = metadataCache_.remove(name.toLowerCase());
    return removedItem != null ? removedItem.value() : null;
  }

  /**
   * Clears all items in the cache.
   */
  public void clear() {
    metadataCache_.clear();
  }

  /**
   * Reloads the metadata for the given object name (if the object already exists
   * in the cache) or loads the object if it does not exists in the metadata
   * cache. If the reload is successful, it will replace any existing item
   * in the cache if the reloaded item's catalog version is greater.
   * If there is no CacheEntry associated with this key, reload() will return
   * immediately.
   */
  public T reload(String name) {
    CacheEntry<T> cacheEntry = metadataCache_.get(name.toLowerCase());
    if (cacheEntry == null) return null;
    return cacheEntry.reload();
  }

  /**
   * Returns all known object names.
   */
  public List<String> getAllNames() {
    return Lists.newArrayList(metadataCache_.keySet());
  }

  /**
   * Returns true if the metadataCache_ contains a key with the given name.
   */
  public boolean contains(String name) {
    return metadataCache_.containsKey(name.toLowerCase());
  }

  /**
   * Returns the catalog object corresponding to the supplied name.
   * Returns null if there is no CacheEntry in the metadataCache_ associated with this
   * key. May throw an unchecked exception if an error was encountered during
   * loading.
   * It is important getOrLoad() not be synchronized to allow concurrent getOrLoad()
   * requests on different keys.
   */
  public T getOrLoad(String name) {
    CacheEntry<T> cacheEntry = metadataCache_.get(name.toLowerCase());
    if (cacheEntry == null) return null;
    return cacheEntry.getOrLoad();
  }

  /**
   * Returns the catalog object corresponding to the supplied name if it exists in the
   * cache, or null if there is no CacheEntry in the metadataCache_ associated with this
   * key. Will not perform a load(), so may return objects that are not initialized
   * (have isLoaded() == false).
   */
  public T get(String name) {
    CacheEntry<T> cacheEntry = metadataCache_.get(name.toLowerCase());
    if (cacheEntry == null) return null;
    return cacheEntry.value();
  }
}
