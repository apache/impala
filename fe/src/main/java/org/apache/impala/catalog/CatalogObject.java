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

import org.apache.impala.thrift.TCatalogObjectType;

/**
 * Interface that all catalog objects implement.
 */
public interface CatalogObject extends HasName {

  /**
   * Catalog objects are often serialized to Thrift. When doing so, many of the objects
   * have a minimal "descriptor" form used in query execution as well as a more complete
   * "full" form with all information, used in catalog topic updates and DDL responses to
   * coordinators in the legacy catalog mode. When sending incremental update for a hdfs
   * table, its "descriptor" form is used with no partitions. Its incremental partition
   * updates will follow it in the same topic update. "invalidation" form means only
   * collecting information that is enough for local-catalog mode coordinators to
   * invalidate the stale cache item. Usually only names and ids will be included.
   * "none" form means return nothing, i.e. null.
   */
  enum ThriftObjectType {
    FULL,
    DESCRIPTOR_ONLY,
    INVALIDATION,
    NONE
  }

  // Returns the TCatalogObject type of this Catalog object.
  TCatalogObjectType getCatalogObjectType();

  // Returns the unqualified object name.
  @Override
  String getName();

  // Returns the unique name of this catalog object.
  String getUniqueName();

  // Returns the version of this catalog object.
  long getCatalogVersion();

  // Sets the version of this catalog object.
  void setCatalogVersion(long newVersion);

  // Returns true if this CatalogObject has had its metadata loaded, false otherwise.
  boolean isLoaded();

  // Returns the timestamp when the object of the catalog version is loaded.
  long getLastLoadedTimeMs();

  // Sets the timestamp of the above. Only used in ImpaladCatalog.
  void setLastLoadedTimeMs(long timeMs);
}
