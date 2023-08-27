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

package org.apache.impala.catalog.monitor;

import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TTableName;

import java.util.HashMap;
import java.util.Optional;

/**
 * Extends the CatalogOperationCounter to count the TDdlType Catalog operations.
 */
public class CatalogResetMetadataCounter extends CatalogOperationCounter {
  /**
   * Holds the possible reset metadata types, derived from the
   * TResetMetadataRequest.
   */
  enum ResetMetadataType {
    REFRESH, // is_refresh=true;  table_name=available
    INVALIDATE_METADATA, // is_refresh=false; table_name=available
    INVALIDATE_METADATA_GLOBAL; // is_refresh=false; table_name=null
  }

  /**
   * Initialize the super.metric_ with the ResetMetadataType enum values.
   */
  public CatalogResetMetadataCounter() {
    this.counters_ = new HashMap<>();
    for (ResetMetadataType metadataType : ResetMetadataType.values()) {
      counters_.put(metadataType.toString(), new HashMap<>());
    }
  }

  public void incrementOperation(TResetMetadataRequest req) {
    Optional<TTableName> tTableName =
        req.table_name != null ? Optional.of(req.table_name) : Optional.empty();
    incrementCounter(
        getResetMetadataType(req, tTableName).toString(), getTableName(tTableName));
  }

  public void decrementOperation(TResetMetadataRequest req) {
    Optional<TTableName> tTableName =
        req.table_name != null ? Optional.of(req.table_name) : Optional.empty();
    decrementCounter(
        getResetMetadataType(req, tTableName).toString(), getTableName(tTableName));
  }

  public static ResetMetadataType getResetMetadataType(
      TResetMetadataRequest req, Optional<TTableName> tTableName) {
    if (req.is_refresh) {
      return ResetMetadataType.REFRESH;
    } else if (tTableName.isPresent()) {
      return ResetMetadataType.INVALIDATE_METADATA;
    } else {
      return ResetMetadataType.INVALIDATE_METADATA_GLOBAL;
    }
  }
}