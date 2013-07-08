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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.cloudera.impala.common.ImpalaException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;

/**
 * Lazily loads metadata on read (through get()) and tracks the set of valid/known
 * object names and their last updated catalog versions. This class is thread safe,
 * with the caveat below:
 *
 * NOTE: This caches uses a LoadingCache internally. The LoadingCache javadoc specifies
 * that: "No observable state associated with [the] cache is modified until loading
 * completes". This means a call to invalidate() while a load is in progress is a no-op.
 * In the context of this class, it means that in some cases this cache might contain
 * stale metadata. For example, if an object of the same name is remove()'ed + add()'ed
 * back while a load is in flight the metadata might be stale. TODO: Look into ways to
 * improve this behavior.
 *
 * If a catalog object has not yet been loaded successfully, get() will attempt to load
 * its metadata. It is only possible to load metadata for objects that are in the
 * set of known objects, which is populated by calls to add(). It is important to keep
 * a set of known object names separate from the cache of actual metadata so
 * certain metadata operations (such as SHOW) can be executed without performing a
 * complete metadata load.
 *
 * Metadata can be invalidated or refreshed. The CatalogObjectCache is initialized using
 * a custom CacheLoader object which can implement custom refresh() logic, but
 * in general the behavior is:
 *   - refresh(name) will perform a synchronous incremental refresh of the object.
 *     Depending on its implementation, refresh might reuse some of the existing metadata
 *     which could result in a partially stale object but faster load time.
 *   - invalidate(name) will mark the item in the metadata cache as invalid
 *     and the next get() will trigger a full metadata reload.
 *
 * TODO: This loading cache is not really needed anymore, especially on the impalad side.
 * The CatalogService also doesn't need this because it (generally) doesn't care about
 * lazily loading metadata.
 */
public class CatalogObjectCache<T extends CatalogObject> {
  private static final Logger LOG = Logger.getLogger(CatalogObjectCache.class);
  private final CacheLoader<String, T> cacheLoader_;

  // Cache of catalog metadata with a key of lower-case object name.
  private final LoadingCache<String, T> metadataCache_;

  // Map of known (lower-case) object name to the version of the catalog they were last
  // updated. The purpose of this map is to ensure the catalog version returned by add()
  // is the same version assigned to a CatalogObject when its metadata is loaded (since
  // add() doesn't actually load the metadata). When the metadata is loaded, during the
  // next call to get(), the current version from this map is used to set the object's
  // catalog version.
  private final ConcurrentMap<String, Long> nameVersionMap_ = new MapMaker().makeMap();

  /**
   * Initializes the cache with the given CacheLoader.
   */
  public CatalogObjectCache(CacheLoader<String, T> cacheLoader) {
    metadataCache_ = CacheBuilder.newBuilder().concurrencyLevel(16).build(cacheLoader);
    cacheLoader_ = cacheLoader;
  }

  /**
   * Add the name to the known object set and increment the catalog version. Also
   * invalidate any metadata associated with the object. When the object is loaded
   * on the next call to get(), it will be assigned this catalog version. Returns
   * the catalog version assigned to the object.
   */
  public long add(String name) {
    synchronized (nameVersionMap_) {
      long version = Catalog.incrementAndGetCatalogVersion();
      nameVersionMap_.put(name.toLowerCase(), version);
      metadataCache_.invalidate(name.toLowerCase());
      return version;
    }
  }

  /**
   * Adds a new item to the metadata cache and returns that item's catalog version.
   */
  public long add(T item) {
    synchronized (nameVersionMap_) {
      nameVersionMap_.put(item.getName().toLowerCase(), item.getCatalogVersion());
      metadataCache_.put(item.getName().toLowerCase(), item);
      return item.getCatalogVersion();
    }
  }

  /**
   * Add all the names to the known object set.
   */
  public void add(List<String> names) { for (String name: names) add(name); }

  public void clear() {
    synchronized (nameVersionMap_) {
      nameVersionMap_.clear();
      metadataCache_.invalidateAll();
    }
  }

  /**
   * Removes an item from the metadata cache and returns the catalog version that
   * will reflect this change.
   */
  public long remove(String name) {
    synchronized (nameVersionMap_) {
      Long version = nameVersionMap_.remove(name.toLowerCase());
      metadataCache_.invalidate(name.toLowerCase());
      return version != null ? Catalog.incrementAndGetCatalogVersion() : 0L;
    }
  }

  /**
   * Invalidates the metadata for the given object.
   */
  public long invalidate(String name) {
    synchronized (nameVersionMap_) {
      long version = Catalog.INITIAL_CATALOG_VERSION;
      if (nameVersionMap_.containsKey(name.toLowerCase())) {
        version = Catalog.incrementAndGetCatalogVersion();
        nameVersionMap_.put(name.toLowerCase(), version);
      }
      metadataCache_.invalidate(name.toLowerCase());
      return version;
    }
  }

  /**
   * Refresh the metadata for the given object name (if the object already exists
   * in the cache), or load the object metadata if the object has not yet been loaded.
   * If refreshing the metadata fails, no exception will be thrown and the existing
   * value will not be modified. Returns the new catalog version for the item, or
   * Catalog.INITIAL_CATALOG_VERSION if the refresh() was not successful.
   */
  public long refresh(String name) {
    // If this is not a known object name, skip the refresh. This helps protect
    // against the metadata cache having items added to it which are not in
    // the name set (since refresh can trigger a load).
    if (!contains(name.toLowerCase())) return Catalog.INITIAL_CATALOG_VERSION;

    metadataCache_.refresh(name.toLowerCase());

    synchronized (nameVersionMap_) {
      // Only get the item if it exists in the cache, we don't want this to trigger
      // a metadata load.
      T item = metadataCache_.getIfPresent(name.toLowerCase());

      // The object may have been removed while a refresh/load was in progress. If so,
      // discard any metadata that was loaded as part of this operation. Otherwise,
      // update the version in the name version map and return the object's new
      // catalog version.
      if (item != null && nameVersionMap_.containsKey(name.toLowerCase())) {
        nameVersionMap_.put(item.getName().toLowerCase(), item.getCatalogVersion());
        return item.getCatalogVersion();
      } else {
        metadataCache_.invalidate(name.toLowerCase());
        return Catalog.INITIAL_CATALOG_VERSION;
      }
    }
  }

  /**
   * Returns all known object names.
   */
  public List<String> getAllNames() {
    return Lists.newArrayList(nameVersionMap_.keySet());
  }

  /**
   * Returns true if the name map contains the given object name.
   */
  public boolean contains(String name) {
    return nameVersionMap_.containsKey(name.toLowerCase());
  }

  /**
   * Returns the catalog object corresponding to the supplied name. The object
   * name must exist in the object name set for the metadata load to succeed. Returns
   * null if the object cannot be found or a CatalogException if there are any
   * problems loading the metadata.
   *
   * The exact behavior is:
   * - If the object already exists in the metadata cache, its value will be returned.
   * - If the object is not present in the metadata cache AND the object exists in
   *   the known object set, the metadata will be loaded.
   * - If the object is not present in the name set, null is returned.
   */
  public T get(final String name) throws ImpalaException {
    if (!contains(name)) return null;
    try {
      // If the item does not exist in the cache, load it and atomically assign
      // it the version associated with its key.
      T loadedObject = metadataCache_.get(name.toLowerCase(), new Callable<T>() {
        @Override
        public T call() throws Exception {
          T item = cacheLoader_.load(name.toLowerCase());
          item.setCatalogVersion(nameVersionMap_.get(name.toLowerCase()));
          return item;
        }});

      // The object may have been removed while a load was in progress. If so, discard
      // any metadata that was loaded as part of this operation.
      if (!contains(name)) {
        metadataCache_.invalidate(name.toLowerCase());
        LOG.info("Object removed while load in progress: " + name);
        return null;
      }
      return loadedObject;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Unwrap ImpalaExceptions
      while(cause != null) {
        if (cause instanceof ImpalaException) {
          throw (ImpalaException) cause;
        }
        cause = cause.getCause();
      }
      throw new IllegalStateException(e);
    }
  }
}
