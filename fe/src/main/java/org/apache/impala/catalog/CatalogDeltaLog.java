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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Represents a log of deleted catalog objects.
 *
 * There are currently two use cases for this log:
 *
 * a) Processing catalog updates in the impalads
 *   The impalad catalog cache can be modified by either a state store update or by a
 *   direct ("fast") update that applies the result of a catalog operation to the cache
 *   out-of-band of a state store update. This thread safe log tracks the divergence
 *   (due to direct updates to the cache) of this impalad's cache from the last state
 *   store update. This log is needed to ensure work is never undone. For example,
 *   consider the following sequence of events:
 *   t1: [Direct Update] - Add item A - (Catalog Version 9)
 *   t2: [Direct Update] - Drop item A - (Catalog Version 10)
 *   t3: [StateStore Update] - (From Catalog Version 9)
 *   This log is used to ensure the state store update in t3 does not undo the drop in t2.
 *   Currently this only tracks objects that were dropped, since the catalog cache can be
 *   queried to check if an object was added. TODO: Also track object additions from async
 *   operations. This could be used to to "replay" the log in the case of a catalog reset
 *   ("invalidate metadata"). Currently, the catalog may briefly go back in time if
 *   "invalidate metadata" is run concurrently with async catalog operations.
 *
 * b) Building catalog topic updates in the catalogd
 *   The catalogd uses this log to identify deleted catalog objects. Deleted
 *   catalog objects are added to this log by the corresponding operations that delete
 *   them (e.g. dropTable()). While constructing a catalog update topic, we use the log to
 *   determine which catalog objects were deleted since the last catalog topic update.
 *   Once the catalog topic update is constructed, the old deleted catalog objects are
 *   garbage collected to prevent the log from growing indefinitely.
 */
public class CatalogDeltaLog {
  // Map of the catalog version an object was removed from the catalog
  // to the catalog object, ordered by catalog version.
  private SortedMap<Long, TCatalogObject> removedCatalogObjects_ =
      new TreeMap<Long, TCatalogObject>();

  /**
   * Adds a new item to the map of removed catalog objects.
   */
  public synchronized void addRemovedObject(TCatalogObject catalogObject) {
    Preconditions.checkNotNull(catalogObject);
    removedCatalogObjects_.put(catalogObject.getCatalog_version(), catalogObject);
  }

  /**
   * Retrieve all the removed catalog objects with version > 'fromVersion'.
   */
  public synchronized List<TCatalogObject> retrieveObjects(long fromVersion) {
    SortedMap<Long, TCatalogObject> objects =
        removedCatalogObjects_.tailMap(fromVersion + 1);
    return ImmutableList.<TCatalogObject>copyOf(objects.values());
  }

  /**
   * Given the current catalog version, removes all items with catalogVersion <
   * currectCatalogVersion. Such objects do not need to be tracked in the delta
   * log anymore because they are consistent with the state store's view of the
   * catalog.
   */
  public synchronized void garbageCollect(long currentCatalogVersion) {
    // Nothing will be garbage collected so avoid creating a new object.
    if (!removedCatalogObjects_.isEmpty() &&
        removedCatalogObjects_.firstKey() < currentCatalogVersion) {
      removedCatalogObjects_ = new TreeMap<Long, TCatalogObject>(
          removedCatalogObjects_.tailMap(currentCatalogVersion));
    }
  }

  /**
   * Checks if a matching catalog object was removed in a catalog version after this
   * object's catalog version. Returns true if there was a matching object that was
   * removed after this object, false otherwise.
   */
  public synchronized boolean wasObjectRemovedAfter(TCatalogObject catalogObject) {
    Preconditions.checkNotNull(catalogObject);
    if (removedCatalogObjects_.isEmpty()) return false;

    // Get all the items that were removed after the catalog version of this object.
    SortedMap<Long, TCatalogObject> candidateObjects =
        removedCatalogObjects_.tailMap(catalogObject.getCatalog_version());
    for (Map.Entry<Long, TCatalogObject> entry: candidateObjects.entrySet()) {
      if (objectNamesMatch(catalogObject, entry.getValue())) return true;
    }
    return false;
  }

  /**
   * Returns true if the two objects have the same object type and key (generated using
   * toCatalogObjectKey()).
   * TODO: Use global object IDs everywhere instead of tracking catalog objects by
   * generated keys.
   */
  private static boolean objectNamesMatch(TCatalogObject first, TCatalogObject second) {
    return toCatalogObjectKey(first).equals(toCatalogObjectKey(second));
  }

  /**
   * Returns a unique string key of a catalog object.
   */
  public static String toCatalogObjectKey(TCatalogObject catalogObject)
      throws IllegalStateException {
    switch (catalogObject.getType()) {
      case DATABASE:
        return "DATABASE:" + catalogObject.getDb().getDb_name().toLowerCase();
      case TABLE:
      case VIEW:
        TTable tbl = catalogObject.getTable();
        return "TABLE:" + tbl.getDb_name().toLowerCase() + "." +
            tbl.getTbl_name().toLowerCase();
      case FUNCTION:
        return "FUNCTION:" + catalogObject.getFn().getName() + "(" +
            catalogObject.getFn().getSignature() + ")";
      case ROLE:
        return "ROLE:" + catalogObject.getRole().getRole_name().toLowerCase();
      case PRIVILEGE:
        return "PRIVILEGE:" +
            catalogObject.getPrivilege().getPrivilege_name().toLowerCase() + "." +
            Integer.toString(catalogObject.getPrivilege().getRole_id());
      case HDFS_CACHE_POOL:
        return "HDFS_CACHE_POOL:" +
            catalogObject.getCache_pool().getPool_name().toLowerCase();
      case DATA_SOURCE:
        return "DATA_SOURCE:" + catalogObject.getData_source().getName().toLowerCase();
      default:
        throw new IllegalStateException(
            "Unsupported catalog object type: " + catalogObject.getType());
    }
  }
}
