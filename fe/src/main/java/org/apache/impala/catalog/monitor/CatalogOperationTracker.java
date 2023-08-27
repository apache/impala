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

import com.google.common.base.Preconditions;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TCatalogOpRecord;
import org.apache.impala.thrift.TGetOperationUsageResponse;
import org.apache.impala.thrift.TOperationUsageCounter;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Adapter class for tracking the catalog operations. Currently, it tracks,
 *  - the number of DDL operations in progress
 *  - the number of reset metadata operations in progress
 *  - the number of DML operations in progress
 *  Each operation has a corresponding record tracked in memory. Historical operations
 *  are also kept in memory and the size is controlled by 'catalog_operation_log_size'.
 */
public final class CatalogOperationTracker {
  public final static CatalogOperationTracker INSTANCE = new CatalogOperationTracker();

  // Keeps track of the on-going DDL operations
  CatalogDdlCounter catalogDdlCounter;

  // Keeps track of the on-going reset metadata requests (refresh/invalidate)
  CatalogResetMetadataCounter catalogResetMetadataCounter;

  // Keeps track of the on-going finalize DML requests (insert/CTAS/upgrade)
  CatalogFinalizeDmlCounter catalogFinalizeDmlCounter;

  private final Map<TUniqueId, TCatalogOpRecord> inFlightOperations =
      new ConcurrentHashMap<>();
  private final Queue<TCatalogOpRecord> finishedOperations =
      new ConcurrentLinkedQueue<>();
  private final int catalogOperationLogSize;

  private CatalogOperationTracker() {
    catalogDdlCounter = new CatalogDdlCounter();
    catalogResetMetadataCounter = new CatalogResetMetadataCounter();
    catalogFinalizeDmlCounter = new CatalogFinalizeDmlCounter();
    catalogOperationLogSize = BackendConfig.INSTANCE.catalogOperationLogSize();
    Preconditions.checkState(catalogOperationLogSize >= 0);
  }

  private void addRecord(TCatalogServiceRequestHeader header,
      String catalogOpName, Optional<TTableName> tTableName, String details) {
    String user = "unknown";
    String clientIp = "unknown";
    String coordinator = "unknown";
    TUniqueId queryId = header.getQuery_id();
    if (header.isSetRequesting_user()) {
      user = header.getRequesting_user();
    }
    if (header.isSetClient_ip()) {
      clientIp = header.getClient_ip();
    }
    if (header.isSetCoordinator_hostname()) {
      coordinator = header.getCoordinator_hostname();
    }
    if (queryId != null) {
      TCatalogOpRecord record = new TCatalogOpRecord(Thread.currentThread().getId(),
          queryId, clientIp, coordinator, catalogOpName,
          catalogDdlCounter.getTableName(tTableName), user,
          System.currentTimeMillis(), -1, "STARTED", details);
      inFlightOperations.put(queryId, record);
    }
  }

  private void archiveRecord(TUniqueId queryId, String errorMsg) {
    if (queryId != null && inFlightOperations.containsKey(queryId)) {
      TCatalogOpRecord record = inFlightOperations.remove(queryId);
      if (catalogOperationLogSize == 0) return;
      record.setFinish_time_ms(System.currentTimeMillis());
      if (errorMsg != null) {
        record.setStatus("FAILED");
        record.setDetails(record.getDetails() + ", error=" + errorMsg);
      } else {
        record.setStatus("FINISHED");
      }
      synchronized (finishedOperations) {
        if (finishedOperations.size() >= catalogOperationLogSize) {
          finishedOperations.poll();
        }
        finishedOperations.add(record);
      }
    }
  }

  private String getDdlType(TDdlExecRequest ddlRequest) {
    if (ddlRequest.ddl_type == TDdlType.ALTER_TABLE) {
      return "ALTER_TABLE_" + ddlRequest.getAlter_table_params().getAlter_type();
    }
    return ddlRequest.ddl_type.name();
  }

  public void increment(TDdlExecRequest ddlRequest, Optional<TTableName> tTableName) {
    if (ddlRequest.isSetHeader()) {
      String details = "query_options=" + ddlRequest.query_options.toString();
      addRecord(ddlRequest.getHeader(), getDdlType(ddlRequest), tTableName, details);
    }
    catalogDdlCounter.incrementOperation(ddlRequest.ddl_type, tTableName);
  }

  public void decrement(TDdlType tDdlType, TUniqueId queryId,
      Optional<TTableName> tTableName, String errorMsg) {
    archiveRecord(queryId, errorMsg);
    catalogDdlCounter.decrementOperation(tDdlType, tTableName);
  }

  public void increment(TResetMetadataRequest req) {
    Optional<TTableName> tTableName =
        req.table_name != null ? Optional.of(req.table_name) : Optional.empty();
    if (req.isSetHeader()) {
      String details = "sync_ddl=" + req.sync_ddl +
          ", want_minimal_response=" + req.getHeader().want_minimal_response +
          ", refresh_updated_hms_partitions=" + req.refresh_updated_hms_partitions;
      if (req.isSetDebug_action() && !req.debug_action.isEmpty()) {
        details += ", debug_action=" + req.debug_action;
      }
      addRecord(req.getHeader(),
          CatalogResetMetadataCounter.getResetMetadataType(req, tTableName).name(),
          tTableName, details);
    }
    catalogResetMetadataCounter.incrementOperation(req);
  }

  public void decrement(TResetMetadataRequest req, String errorMsg) {
    if (req.isSetHeader()) {
      archiveRecord(req.getHeader().getQuery_id(), errorMsg);
    }
    catalogResetMetadataCounter.decrementOperation(req);
  }

  public void increment(TUpdateCatalogRequest req) {
    Optional<TTableName> tTableName =
        Optional.of(new TTableName(req.db_name, req.target_table));
    if (req.isSetHeader()) {
      String details = "sync_ddl=" + req.sync_ddl +
          ", is_overwrite=" + req.is_overwrite +
          ", transaction_id=" + req.transaction_id +
          ", write_id=" + req.write_id +
          ", num_of_updated_partitions=" + req.getUpdated_partitionsSize();
      if (req.isSetIceberg_operation()) {
        details += ", iceberg_operation=" + req.iceberg_operation.operation;
      }
      if (req.isSetDebug_action() && !req.debug_action.isEmpty()) {
        details += ", debug_action=" + req.debug_action;
      }
      addRecord(req.getHeader(),
          CatalogFinalizeDmlCounter.getDmlType(req.getHeader().redacted_sql_stmt).name(),
          tTableName, details);
    }
    catalogFinalizeDmlCounter.incrementOperation(req);
  }

  public void decrement(TUpdateCatalogRequest req, String errorMsg) {
    if (req.isSetHeader()) {
      archiveRecord(req.getHeader().getQuery_id(), errorMsg);
    }
    catalogFinalizeDmlCounter.decrementOperation(req);
  }

  /**
   * Merges the CatalogOpMetricCounter operation summary metrics into a single
   * list that can be passed to the backend webserver.
   */
  public TGetOperationUsageResponse getOperationMetrics() {
    List<TOperationUsageCounter> merged = new ArrayList<>();
    merged.addAll(catalogDdlCounter.getOperationUsage());
    merged.addAll(catalogResetMetadataCounter.getOperationUsage());
    merged.addAll(catalogFinalizeDmlCounter.getOperationUsage());
    TGetOperationUsageResponse res = new TGetOperationUsageResponse(merged);
    for (TCatalogOpRecord record : inFlightOperations.values()) {
      res.addToIn_flight_catalog_operations(record);
    }
    List<TCatalogOpRecord> records = new ArrayList<>(finishedOperations);
    // Reverse the list to show recent operations first.
    Collections.reverse(records);
    res.setFinished_catalog_operations(records);
    return res;
  }
}