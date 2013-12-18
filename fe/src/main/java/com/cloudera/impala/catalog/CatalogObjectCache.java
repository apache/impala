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
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Lazily loads metadata on read (through getOrLoad()) and tracks the set of valid/known
 * object names. This class is thread safe.
 *
 * If a catalog object has not yet been loaded successfully, getOrLoad() will attempt to
 * load its metadata. It is only possible to load metadata for objects that have
 * previously been created with a call to add() or addName(). The catalog cache supports
 * parallel loading/gets of different keys. While a load is in progress, any calls to
 * get the same key will block until the load completes at which point the loaded value
 * will be returned.
 *
 * Metadata can be invalidated or reloaded. The CatalogObjectCache is initialized using
 * a custom CacheLoader object which can implement its own load()/reload() logic, but
 * in general the behavior is:
 *   - reload(name) will perform a synchronous incremental refresh of the object.
 *     Depending on its implementation, refresh might reuse some of the existing metadata
 *     which could result in a partially stale object but faster load time.
 *   - invalidate(name) will mark the item in the metadata cache as invalid
 *     and the next getOrLoad() will trigger a full metadata reload.
 */
public class CatalogObjectCache<T extends CatalogObject> {
  private static final Logger LOG = Logger.getLogger(CatalogObjectCache.class);

  // Map of lower-case object name to CacheEntry objects. New CacheEntries are created
  // by calling add() or addName(). If a CacheEntry does not exist "getOrLoad()" will
  // return null.
  private final ConcurrentHashMap<String, CacheEntry<T>> metadataCache_ =
      new ConcurrentHashMap<String, CacheEntry<T>>();

  // The CacheLoader used to load new items into the cache.
  private final CacheLoader<String, T> cacheLoader_;

  /**
   * Stores and lazily loads CatalogObjects upon read. Ensures the CatalogObject
   * catalog versions are strictly increasing when updated. This class is thread safe.
   */
  private static class CacheEntry<T extends CatalogObject> {
    private final String key_;
    private final CacheLoader<String, T> cacheLoader_;
    private T catalogObject_;

    private CacheEntry(String key, CacheLoader<String, T> cacheLoader) {
      key_ = key;
      cacheLoader_ = cacheLoader;
    }

    /**
     * Replaces the CatalogObject in this CacheEntry if it is newer than the
     * existing value (the catalog version is greater). Returns true if the existing
     * value was replaced or false if the existing value was preserved.
     */
    public synchronized boolean replaceIfNewer(T catalogObject) {
      Preconditions.checkNotNull(catalogObject);
      if (catalogObject_ == null ||
          catalogObject_.getCatalogVersion() < catalogObject.getCatalogVersion()) {
        catalogObject_ = catalogObject;
        return true;
      }
      return false;
    }

    /**
     * Invalidates the current value. The next call to getOrLoad() or reload() will
     * trigger a metadata load.
     */
    public void invalidate() {
      T tmpCatalogObject = catalogObject_;
      synchronized(this) {
        // Only invalidate if the reference hasn't changed. This helps reduce the
        // likely-hood that a newly loaded value gets immediately wiped out
        // by a concurrent invalidate().
        // TODO: Consider investigating a more fair locking scheme.
        if (tmpCatalogObject == catalogObject_)  catalogObject_ = null;
      }
    }

    /**
     * Returns the current CatalogObject value in this CacheEntry.
     */
    public synchronized T value() { return catalogObject_; }

    /**
     * Gets the current catalog object for this CacheEntry, loading it if needed (if the
     * existing catalog object is null). Throws a CatalogException on any error
     * loading the metadata.
     */
    public synchronized T getOrLoad() throws CatalogException {
      if (catalogObject_ != null) return catalogObject_;
      try {
        T loadedObject = cacheLoader_.load(key_.toLowerCase());
        Preconditions.checkNotNull(loadedObject);
        replaceIfNewer(loadedObject);
        return catalogObject_;
      } catch (Exception e) {
        throw new CatalogException("Error loading metadata for: " + key_, e);
      }
    }

    /**
     * Reloads the value for this cache entry and replaces the existing value if the
     * new object's catalog version is greater. All exceptions are logged and
     * swallowed and the existing value will not be modified. This is similar to
     * getOrLoad(), but can reuse the existing cached value to speedup loading time.
     * TODO: Instead of serializing reload() requests, concurrent reload()'s could
     * block until the in-progress reload() completes.
     */
    public synchronized T reload() {
      try {
        ListenableFuture<T> result = cacheLoader_.reload(key_, catalogObject_);
        Preconditions.checkNotNull(result);

        // Wait for the reload to complete.
        T reloadedObject = result.get();
        Preconditions.checkNotNull(reloadedObject);
        replaceIfNewer(reloadedObject);
      } catch (Exception e) {
        LOG.error(e);
      }
      return catalogObject_;
    }

    /**
     * Creates a new CacheEntry with the given key and CacheLoader.
     */
    public static <T extends CatalogObject> CacheEntry<T>
        create(String key, CacheLoader<String, T> cacheLoader) {
      return new CacheEntry<T>(key.toLowerCase(), cacheLoader);
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
        CacheEntry.create(catalogObject.getName(), cacheLoader_);
    CacheEntry<T> existingItem = metadataCache_.putIfAbsent(
        catalogObject.getName().toLowerCase(),
        cacheEntry);

    // When existingItem != null it indicates there was already an existing entry
    // associated with the key, so apply the update to the existing entry.
    // Otherwise, update the new CacheEntry.
    cacheEntry = existingItem != null ? existingItem : cacheEntry;
    return cacheEntry.replaceIfNewer(catalogObject);
  }

  /**
   * Adds a new name to the cache, the next access to the object will trigger a
   * metadata load. If an item with the same name already exists in the cache
   * it will be invalidated.
   * TODO: Should addName() require a catalog version associated with the operation?
   */
  public void addName(String objectName) {
    CacheEntry<T> cacheEntry = CacheEntry.create(objectName, cacheLoader_);
    CacheEntry<T> existingItem = metadataCache_.putIfAbsent(
        objectName.toLowerCase(), cacheEntry);
    cacheEntry = existingItem != null ? existingItem : cacheEntry;
    // This invalidate may be unnecessary if there was an existing item in the cache
    // that didn't need invalidation, but it is still safe to do so.
    cacheEntry.invalidate();
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
   * Invalidates the CacheEntry's value for the given object name. The next access to
   * this object will trigger a metadata load. Note that this does NOT remove the
   * CacheEntry value or the key in the metadataCache_.
   */
  public void invalidate(String name) {
    CacheEntry<T> cacheEntry = metadataCache_.get(name.toLowerCase());
    if (cacheEntry != null) cacheEntry.invalidate();
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
  public T getOrLoad(final String name) {
    CacheEntry<T> cacheEntry = metadataCache_.get(name.toLowerCase());
    if (cacheEntry == null) return null;
    try {
      return cacheEntry.getOrLoad();
    } catch (CatalogException e) {
      // TODO: Consider throwing a CatalogException rather than an unchecked
      // exception. IllegalStateException isn't really the right exception type
      // either.
      throw new IllegalStateException(e.getMessage(), e);
    }
  }
}
