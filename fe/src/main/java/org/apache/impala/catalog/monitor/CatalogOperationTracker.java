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
import org.apache.commons.lang3.StringUtils;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TCatalogOpRecord;
import org.apache.impala.thrift.TGetOperationUsageResponse;
import org.apache.impala.thrift.TOperationUsageCounter;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.apache.impala.util.TUniqueIdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG =
      LoggerFactory.getLogger(CatalogOperationTracker.class);
  public final static CatalogOperationTracker INSTANCE = new CatalogOperationTracker();
  private static final TQueryOptions DEFAULT_QUERY_OPTIONS = new TQueryOptions();

  // Keeps track of the on-going DDL operations
  CatalogDdlCounter catalogDdlCounter_;

  // Keeps track of the on-going reset metadata requests (refresh/invalidate)
  CatalogResetMetadataCounter catalogResetMetadataCounter_;

  // Keeps track of the on-going finalize DML requests (insert/CTAS/upgrade)
  CatalogFinalizeDmlCounter catalogFinalizeDmlCounter_;

  /**
   * Key to track in-flight catalog operations. Each operation is triggered by an RPC.
   * Each RPC is identified by the query id and the thrift thread id that handles it.
   * Note that the thread id is important to identify different RPC retries.
   */
  private static class RpcKey {
    private final TUniqueId queryId_;
    private final long threadId_;

    public RpcKey(TUniqueId queryId) {
      queryId_ = queryId;
      threadId_ = Thread.currentThread().getId();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RpcKey)) return false;
      RpcKey key = (RpcKey) o;
      return queryId_.equals(key.queryId_) && threadId_ == key.threadId_;
    }

    @Override
    public int hashCode() {
      return queryId_.hashCode() * 31 + Long.hashCode(threadId_);
    }
  }

  private final Map<RpcKey, TCatalogOpRecord> inFlightOperations_ =
      new ConcurrentHashMap<>();
  private final Queue<TCatalogOpRecord> finishedOperations_ =
      new ConcurrentLinkedQueue<>();
  private final int catalogOperationLogSize_;

  private CatalogOperationTracker() {
    catalogDdlCounter_ = new CatalogDdlCounter();
    catalogResetMetadataCounter_ = new CatalogResetMetadataCounter();
    catalogFinalizeDmlCounter_ = new CatalogFinalizeDmlCounter();
    catalogOperationLogSize_ = BackendConfig.INSTANCE.catalogOperationLogSize();
    Preconditions.checkState(catalogOperationLogSize_ >= 0);
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
          catalogDdlCounter_.getTableName(tTableName), user,
          System.currentTimeMillis(), -1, "STARTED", details);
      inFlightOperations_.put(new RpcKey(queryId), record);
    }
  }

  private void archiveRecord(TUniqueId queryId, String errorMsg) {
    if (queryId == null) return;
    RpcKey key = new RpcKey(queryId);
    TCatalogOpRecord record = inFlightOperations_.remove(key);
    if (record == null) {
      LOG.error("Null record for query {}", TUniqueIdUtil.PrintId(queryId));
      return;
    }
    if (catalogOperationLogSize_ == 0) return;
    record.setFinish_time_ms(System.currentTimeMillis());
    if (errorMsg != null) {
      record.setStatus("FAILED");
      record.setDetails(record.getDetails() + ", error=" + errorMsg);
    } else {
      record.setStatus("FINISHED");
    }
    synchronized (finishedOperations_) {
      if (finishedOperations_.size() >= catalogOperationLogSize_) {
        finishedOperations_.poll();
      }
      finishedOperations_.add(record);
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
      // Only show non-default options in the 'details' field.
      TDdlQueryOptions options = ddlRequest.query_options;
      List<String> nonDefaultOptions = new ArrayList<>();
      if (options.sync_ddl) nonDefaultOptions.add("sync_ddl=true");
      if (StringUtils.isNotEmpty(options.debug_action)) {
        nonDefaultOptions.add("debug_action=" + options.debug_action);
      }
      if (options.lock_max_wait_time_s != DEFAULT_QUERY_OPTIONS.lock_max_wait_time_s) {
        nonDefaultOptions.add("lock_max_wait_time_s=" + options.lock_max_wait_time_s);
      }
      if (options.kudu_table_reserve_seconds
          != DEFAULT_QUERY_OPTIONS.kudu_table_reserve_seconds) {
        nonDefaultOptions.add(
            "kudu_table_reserve_seconds=" + options.kudu_table_reserve_seconds);
      }
      addRecord(ddlRequest.getHeader(), getDdlType(ddlRequest), tTableName,
          StringUtils.join(nonDefaultOptions, ", "));
    }
    catalogDdlCounter_.incrementOperation(ddlRequest.ddl_type, tTableName);
  }

  public void decrement(TDdlType tDdlType, TUniqueId queryId,
      Optional<TTableName> tTableName, String errorMsg) {
    archiveRecord(queryId, errorMsg);
    catalogDdlCounter_.decrementOperation(tDdlType, tTableName);
  }

  public void increment(TResetMetadataRequest req) {
    Optional<TTableName> tTableName =
        req.table_name != null ? Optional.of(req.table_name) : Optional.empty();
    if (req.isSetHeader()) {
      List<String> details = new ArrayList<>();
      if (req.sync_ddl) details.add("sync_ddl=true");
      if (req.header.want_minimal_response) {
        details.add("want_minimal_response=true");
      }
      if (req.refresh_updated_hms_partitions) {
        details.add("refresh_updated_hms_partitions=true");
      }
      if (StringUtils.isNotEmpty(req.debug_action)) {
        details.add("debug_action=" + req.debug_action);
      }
      addRecord(req.getHeader(),
          CatalogResetMetadataCounter.getResetMetadataType(req, tTableName).name(),
          tTableName, StringUtils.join(details, ", "));
    }
    catalogResetMetadataCounter_.incrementOperation(req);
  }

  public void decrement(TResetMetadataRequest req, String errorMsg) {
    if (req.isSetHeader()) {
      archiveRecord(req.getHeader().getQuery_id(), errorMsg);
    }
    catalogResetMetadataCounter_.decrementOperation(req);
  }

  public void increment(TUpdateCatalogRequest req) {
    Optional<TTableName> tTableName =
        Optional.of(new TTableName(req.db_name, req.target_table));
    if (req.isSetHeader()) {
      List<String> details = new ArrayList<>();
      details.add("#partitions=" + req.getUpdated_partitionsSize());
      if (req.sync_ddl) details.add("sync_ddl=true");
      if (req.is_overwrite) details.add("is_overwrite=true");
      if (req.transaction_id > 0) {
        details.add("transaction_id=" + req.transaction_id);
      }
      if (req.write_id > 0) details.add("write_id=" + req.write_id);
      if (req.isSetIceberg_operation()) {
        details.add("iceberg_operation=" + req.iceberg_operation.operation);
      }
      if (StringUtils.isNotEmpty(req.debug_action)) {
        details.add("debug_action=" + req.debug_action);
      }
      addRecord(req.getHeader(),
          CatalogFinalizeDmlCounter.getDmlType(req.getHeader().redacted_sql_stmt).name(),
          tTableName, StringUtils.join(details, ", "));
    }
    catalogFinalizeDmlCounter_.incrementOperation(req);
  }

  public void decrement(TUpdateCatalogRequest req, String errorMsg) {
    if (req.isSetHeader()) {
      archiveRecord(req.getHeader().getQuery_id(), errorMsg);
    }
    catalogFinalizeDmlCounter_.decrementOperation(req);
  }

  /**
   * Merges the CatalogOpMetricCounter operation summary metrics into a single
   * list that can be passed to the backend webserver.
   */
  public TGetOperationUsageResponse getOperationMetrics() {
    List<TOperationUsageCounter> merged = new ArrayList<>();
    merged.addAll(catalogDdlCounter_.getOperationUsage());
    merged.addAll(catalogResetMetadataCounter_.getOperationUsage());
    merged.addAll(catalogFinalizeDmlCounter_.getOperationUsage());
    TGetOperationUsageResponse res = new TGetOperationUsageResponse(merged);
    for (TCatalogOpRecord record : inFlightOperations_.values()) {
      res.addToIn_flight_catalog_operations(record);
    }
    List<TCatalogOpRecord> records = new ArrayList<>(finishedOperations_);
    // Reverse the list to show recent operations first.
    Collections.reverse(records);
    res.setFinished_catalog_operations(records);
    return res;
  }
}