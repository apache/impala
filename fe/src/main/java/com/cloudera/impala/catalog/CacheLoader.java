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

/**
 * Interface that defines how new CatalogObjects are loaded into a cache.
 */
public abstract class CacheLoader<K, V extends CatalogObject> {
  /**
   * Loads an element into the cache for the given key name. The cachedValue
   * parameter is an existing cached entry that can be reused to help speed up
   * value loading. If cachedValue is null it will be ignored.
   */
  public abstract V load(K key, V cachedValue, long catalogVersion);

  /**
   * Returns the next catalog version. This value is generally passed to
   * load() so newly loaded items can get assigned a catalog version.
   */
  public abstract long getNextCatalogVersion();
}
