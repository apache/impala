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

import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TOperationUsageCounter;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUpdateCatalogRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Adapter class for the catalog operation counters. Currently, it tracks,
 *  - the number of DDL operations in progress
 *  - the number of reset metadata operations in progress
 *  - the number of DML operations in progress
 */
public final class CatalogOperationMetrics {
  public final static CatalogOperationMetrics INSTANCE = new CatalogOperationMetrics();

  // Keeps track of the on-going DDL operations
  CatalogDdlCounter catalogDdlCounter;

  // Keeps track of the on-going reset metadata requests (refresh/invalidate)
  CatalogResetMetadataCounter catalogResetMetadataCounter;

  // Keeps track of the on-going finalize DML requests (insert/CTAS/upgrade)
  CatalogFinalizeDmlCounter catalogFinalizeDmlCounter;

  private CatalogOperationMetrics() {
    catalogDdlCounter = new CatalogDdlCounter();
    catalogResetMetadataCounter = new CatalogResetMetadataCounter();
    catalogFinalizeDmlCounter = new CatalogFinalizeDmlCounter();
  }

  public void increment(TDdlType tDdlType, Optional<TTableName> tTableName) {
    catalogDdlCounter.incrementOperation(tDdlType, tTableName);
  }

  public void decrement(TDdlType tDdlType, Optional<TTableName> tTableName) {
    catalogDdlCounter.decrementOperation(tDdlType, tTableName);
  }

  public void increment(TResetMetadataRequest req) {
    catalogResetMetadataCounter.incrementOperation(req);
  }

  public void decrement(TResetMetadataRequest req) {
    catalogResetMetadataCounter.decrementOperation(req);
  }

  public void increment(TUpdateCatalogRequest request) {
    catalogFinalizeDmlCounter.incrementOperation(request);
  }

  public void decrement(TUpdateCatalogRequest request) {
    catalogFinalizeDmlCounter.decrementOperation(request);
  }

  /**
   * Merges the CatalogOpMetricCounter operation summary metrics into a single
   * list that can be passed to the backend webserver.
   */
  public List<TOperationUsageCounter> getOperationMetrics() {
    List<TOperationUsageCounter> merged = new ArrayList<>();
    merged.addAll(catalogDdlCounter.getOperationUsage());
    merged.addAll(catalogResetMetadataCounter.getOperationUsage());
    merged.addAll(catalogFinalizeDmlCounter.getOperationUsage());
    return merged;
  }
}