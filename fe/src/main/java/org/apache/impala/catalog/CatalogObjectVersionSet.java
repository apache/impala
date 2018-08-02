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

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.TreeMultiset;

/**
 * Singleton class used to maintain the versions of all the catalog objects stored in a
 * local catalog cache. Simple wrapper around a multiset which stores the catalog
 * object versions. This allows O(lg n) addition or removal of objects as well as
 * O(1) retrieval of the minimum object version currently stored in the cache.
 *
 * Provides a simple API to add, remove and update catalog object
 * versions. Thread-safe.
 *
 * The primary use case of this class is to allow an Impalad catalog cache determine when
 * the result set of an INVALIDATE METADATA operation has been applied locally by keeping
 * track of the minimum catalog object version.
 */
public class CatalogObjectVersionSet {
  // TODO(todd): it's likely that we should only have exactly one object at each
  // version, in which case this should be a set, instead of a multiset. We should
  // add appropriate preconditions checks and make this change.
  private final TreeMultiset<Long> objectVersions_ = TreeMultiset.create();
  /**
   * Cache of the minimum element in objectVersions_. This provides O(1) runtime
   * for the getMinimumVersion() call.
   */
  private long minVersion_ = Catalog.INITIAL_CATALOG_VERSION;

  public static final CatalogObjectVersionSet INSTANCE =
      new CatalogObjectVersionSet();

  @VisibleForTesting
  CatalogObjectVersionSet() {}

  public synchronized void updateVersions(long oldVersion, long newVersion) {
    removeVersion(oldVersion);
    addVersion(newVersion);
  }

  public synchronized void removeVersion(long oldVersion) {
    int oldCount = objectVersions_.remove(oldVersion, 1);
    // If we removed the minimum version, and no other entries exist with
    // the same min version, we need to update it.
    if (oldCount == 1 && oldVersion == minVersion_) {
      Entry<Long> entry = objectVersions_.firstEntry();
      if (entry != null) {
        minVersion_ = entry.getElement();
      } else {
        minVersion_ = Catalog.INITIAL_CATALOG_VERSION;
      }
    }
  }

  public synchronized void addVersion(long newVersion) {
    if (objectVersions_.isEmpty() || newVersion < minVersion_) {
      minVersion_ = newVersion;
    }
    objectVersions_.add(newVersion);
  }

  public synchronized long getMinimumVersion() {
    return minVersion_;
  }

  public void addAll(List<? extends CatalogObject> catalogObjects) {
    for (CatalogObject catalogObject: catalogObjects) {
      addVersion(catalogObject.getCatalogVersion());
    }
  }

  public void removeAll(List<? extends CatalogObject> catalogObjects) {
    for (CatalogObject catalogObject: catalogObjects) {
      removeVersion(catalogObject.getCatalogVersion());
    }
  }

  public synchronized void clear() {
    minVersion_ = Catalog.INITIAL_CATALOG_VERSION;
    objectVersions_.clear();
  }
}
