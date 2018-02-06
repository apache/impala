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
import java.util.PriorityQueue;

import com.google.common.base.Preconditions;

/**
 * Singleton class used to maintain the versions of all the catalog objects stored in a
 * local catalog cache. Simple wrapper around a priority queue which stores the catalog
 * object versions, allowing O(1) retrieval of the minimum object version currently
 * stored in the cache. Provides a simple API to add, remove and update catalog object
 * versions. Not thread-safe.
 *
 * The primary use case of this class is to allow an Impalad catalog cache determine when
 * the result set of an INVALIDATE METADATA operation has been applied locally by keeping
 * track of the minimum catalog object version.
 */
public class CatalogObjectVersionQueue {
  private final PriorityQueue<Long> objectVersions_ = new PriorityQueue<>();

  public static final CatalogObjectVersionQueue INSTANCE =
      new CatalogObjectVersionQueue();

  private CatalogObjectVersionQueue() {}

  public void updateVersions(long oldVersion, long newVersion) {
    removeVersion(oldVersion);
    addVersion(newVersion);
  }

  public void removeVersion(long oldVersion) {
    objectVersions_.remove(oldVersion);
  }

  public void addVersion(long newVersion) {
    objectVersions_.add(newVersion);
  }

  public long getMinimumVersion() {
    Long minVersion = objectVersions_.peek();
    return minVersion != null ? minVersion : 0;
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

  public void clear() { objectVersions_.clear(); }
}
