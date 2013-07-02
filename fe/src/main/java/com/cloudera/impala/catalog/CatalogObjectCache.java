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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.cloudera.impala.common.ImpalaException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;

/**
 * Lazily loads metadata on read (through get()) and tracks the set of valid/known
 * object names. This class is thread safe, with the caveat below:
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
 */
public class CatalogObjectCache<T> {
  // Cache of catalog metadata with a key of lower-case object name.
  private final LoadingCache<String, T> metadataCache;

  // Set of known (lower-case) object names. It is only possible to load metadata for
  // objects that already exist in this set.
  private final Set<String> nameSet =
      Collections.synchronizedSet(new HashSet<String>());

  /**
   * Initializes the cache with the given CacheLoader.
   */
  public CatalogObjectCache(CacheLoader<String, T> cacheLoader) {
    metadataCache = CacheBuilder.newBuilder()
        // TODO: Increase concurrency level once HIVE-3521 is resolved.
        .concurrencyLevel(1)
        .build(cacheLoader);
  }

  /**
   * Add the name to the known object set and invalidate any associated
   * metadata.
   */
  public void add(String name) {
    nameSet.add(name.toLowerCase());
    metadataCache.invalidate(name.toLowerCase());
  }

  /**
   * Add all the names to the known object set.
   */
  public void add(List<String> names) {
    for (String name: names) add(name);
  }

  /**
   * Removes an item from the metadata cache.
   */
  public void remove(String name) {
    nameSet.remove(name.toLowerCase());
    metadataCache.invalidate(name.toLowerCase());
  }

  /**
   * Invalidates the metadata for the given object.
   */
  public void invalidate(String name) {
    metadataCache.invalidate(name.toLowerCase());
  }

  /**
   * Refresh the metadata for the given object name (if the object already exists
   * in the cache), or load the object metadata if the object has not yet been loaded.
   * If refreshing the metadata fails, no exception will be thrown and the existing
   * value will not be modified.
   */
  public void refresh(String name) {
    // If this is not a known object name, skip the refresh. This helps protect
    // against the metadata cache having items added to it which are not in
    // the name set (since refresh can trigger a load).
    if (!nameSet.contains(name.toLowerCase())) return;
    metadataCache.refresh(name.toLowerCase());
    // The object may have been removed while a refresh/load was in progress. If so,
    // discard any metadata that was loaded as part of this operation.
    if (!nameSet.contains(name.toLowerCase())) {
      metadataCache.invalidate(name.toLowerCase());
    }
  }

  /**
   * Returns all known object names.
   */
  public Set<String> getAllNames() {
    return Sets.newHashSet(nameSet);
  }

  /**
   * Returns true if the name map contains the given object name.
   */
  public boolean contains(String name) {
    return nameSet.contains(name.toLowerCase());
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
  public T get(String name) throws ImpalaException {
    if (!nameSet.contains(name.toLowerCase())) return null;
    try {
      T loadedObject = metadataCache.get(name.toLowerCase());
      // The object may have been removed while a load was in progress. If so, discard
      // any metadata that was loaded as part of this operation.
      if (!nameSet.contains(name.toLowerCase())) {
        metadataCache.invalidate(name.toLowerCase());
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