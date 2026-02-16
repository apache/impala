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
import org.apache.impala.thrift.TEventSequence;
import org.apache.impala.thrift.TGetOperationUsageResponse;
import org.apache.impala.thrift.TOperationUsageCounter;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.apache.impala.util.EventSequence;
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

  /**
   * Tracks in-flight operations. Each entry contains both the catalog operation record
   * and its associated EventSequence for real-time timeline tracking.
   *
   * Note: While TCatalogOpRecord also has a timeline field (TEventSequence type), we
   * need to track the original EventSequence instance separately to allow updates
   * during operation execution.
   */
  private static class InFlightOperation {
    final TCatalogOpRecord record;
    EventSequence timeline;

    InFlightOperation(TCatalogOpRecord record, EventSequence timeline) {
      this.record = record;
      this.timeline = timeline;
    }
  }

  private final Map<RpcKey, InFlightOperation> inFlightOperations_ =
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

  /**
   * Adds a new catalog operation record. Should be called from JniCatalog when
   * an RPC arrives, before calling increment(). It creates a minimal operation record
   * with default values for user, client IP, and coordinator initially set to "unknown".
   * These fields will be updated later in updateRecord().
   *
   * @return the created record (which can be used to set response_size_bytes directly)
   */
  public TCatalogOpRecord addRecord(TUniqueId queryId, EventSequence timeline,
      int requestSizeBytes) {
    if (queryId == null) return null;

    long startTimeMs = System.currentTimeMillis();
    TCatalogOpRecord record = new TCatalogOpRecord(Thread.currentThread().getId(),
        queryId, /*clientIp*/"unknown", /*coordinator*/"unknown",
        /*catalogOpName*/"unknown", /*targetName*/"unknown", /*user*/"unknown",
        startTimeMs, -1, "STARTED", "");

    record.setRequest_size_bytes(requestSizeBytes);

    RpcKey key = new RpcKey(queryId);
    InFlightOperation operation = new InFlightOperation(record, timeline);

    inFlightOperations_.put(key, operation);
    return record;
  }

  /**
   * Updates an in-flight catalog operation record with operation details.
   * Called by increment() to populate fields like catalog_op_name, user, etc.
   *
   * @param queryId The query ID of the operation to update
   * @param header The request header containing user, client_ip, coordinator info
   * @param catalogOpName The name of the catalog operation (e.g., CREATE_TABLE)
   * @param tTableName The table name (if applicable) for counter tracking
   * @param details Additional operation details
   */
  private void updateRecordFields(TUniqueId queryId, TCatalogServiceRequestHeader header,
      String catalogOpName, Optional<TTableName> tTableName, String details) {
    if (queryId == null) return;

    final String user = header.isSetRequesting_user() ?
        header.getRequesting_user() : "unknown";
    final String clientIp = header.isSetClient_ip() ? header.getClient_ip() : "unknown";
    final String coordinator = header.isSetCoordinator_hostname() ?
        header.getCoordinator_hostname() : "unknown";
    final String targetName = catalogDdlCounter_.getTableName(tTableName);

    RpcKey key = new RpcKey(queryId);
    InFlightOperation operation = inFlightOperations_.get(key);
    if (operation == null) {
      LOG.warn("updateRecordFields: In-flight operation not found for query_id={}",
          TUniqueIdUtil.PrintId(queryId));
      return;
    }

    // Update record fields
    TCatalogOpRecord record = operation.record;
    record.setCatalog_op_name(catalogOpName);
    record.setTarget_name(targetName);
    record.setUser(user);
    record.setClient_ip(clientIp);
    record.setCoordinator_hostname(coordinator);
    record.setDetails(details);
  }

  private void archiveRecord(TUniqueId queryId, String errorMsg) {
    if (queryId == null) return;
    RpcKey key = new RpcKey(queryId);
    InFlightOperation operation = inFlightOperations_.remove(key);
    if (operation == null) {
      LOG.error("Failed to archive the in-flight operation of query {} since " +
          "it's missing", TUniqueIdUtil.PrintId(queryId));
      return;
    }
    TCatalogOpRecord record = operation.record;
    if (catalogOperationLogSize_ == 0) return;
    record.setFinish_time_ms(System.currentTimeMillis());
    if (errorMsg != null) {
      record.setStatus("FAILED");
      record.setDetails(record.getDetails() + ", error=" + errorMsg);
    } else {
      record.setStatus("FINISHED");
    }
    // Convert EventSequence to TEventSequence and store if available
    if (operation.timeline != null) {
      record.setTimeline(operation.timeline.toThrift());
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
      TUniqueId queryId = ddlRequest.getHeader().getQuery_id();
      if (queryId == null) return;
      TCatalogServiceRequestHeader header = ddlRequest.getHeader();

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

      final String catalogOpName = getDdlType(ddlRequest);
      final String details = StringUtils.join(nonDefaultOptions, ", ");

      updateRecordFields(queryId, header, catalogOpName, tTableName, details);
      catalogDdlCounter_.incrementOperation(ddlRequest.ddl_type, tTableName);
    }
  }

  public void decrement(TDdlType tDdlType, TUniqueId queryId,
      Optional<TTableName> tTableName, String errorMsg) {
    if (queryId == null) return;
    archiveRecord(queryId, errorMsg);
    catalogDdlCounter_.decrementOperation(tDdlType, tTableName);
  }

  public void increment(TResetMetadataRequest req) {
    Optional<TTableName> tTableName =
        req.table_name != null ? Optional.of(req.table_name) : Optional.empty();
    if (req.isSetHeader()) {
      TUniqueId queryId = req.getHeader().getQuery_id();
      if (queryId == null) return;
      TCatalogServiceRequestHeader header = req.getHeader();

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

      final String catalogOpName = CatalogResetMetadataCounter.getResetMetadataType(
          req, tTableName).name();
      final String detailsStr = StringUtils.join(details, ", ");

      updateRecordFields(queryId, header, catalogOpName, tTableName, detailsStr);

      if (queryId != null) {
        catalogResetMetadataCounter_.incrementOperation(req);
      }
    }
  }

  public void decrement(TResetMetadataRequest req, String errorMsg) {
    if (!(req.isSetHeader() && req.getHeader().isSetQuery_id())) return;
    archiveRecord(req.getHeader().getQuery_id(), errorMsg);
    catalogResetMetadataCounter_.decrementOperation(req);
  }

  public void increment(TUpdateCatalogRequest req) {
    Optional<TTableName> tTableName =
        Optional.of(new TTableName(req.db_name, req.target_table));
    if (req.isSetHeader()) {
      TUniqueId queryId = req.getHeader().getQuery_id();
      if (queryId == null) return;
      TCatalogServiceRequestHeader header = req.getHeader();

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

      final String catalogOpName = CatalogFinalizeDmlCounter.getDmlType(
          req.getHeader().redacted_sql_stmt).name();
      final String detailsStr = StringUtils.join(details, ", ");

      updateRecordFields(queryId, header, catalogOpName, tTableName, detailsStr);

      if (queryId != null) {
        catalogFinalizeDmlCounter_.incrementOperation(req);
      }
    }
  }

  public void decrement(TUpdateCatalogRequest req, String errorMsg) {
    if (!(req.isSetHeader() && req.getHeader().isSetQuery_id())) return;
    archiveRecord(req.getHeader().getQuery_id(), errorMsg);
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
    // For in-flight operations, dynamically set timeline from current EventSequence
    for (InFlightOperation operation : inFlightOperations_.values()) {
      // Create a copy of the record so we don't modify the original
      TCatalogOpRecord recordCopy = operation.record.deepCopy();
      // Set current timeline if available
      if (operation.timeline != null && !recordCopy.isSetTimeline()) {
        TEventSequence timelineThrift = operation.timeline.toThrift();
        recordCopy.setTimeline(timelineThrift);
      }
      res.addToIn_flight_catalog_operations(recordCopy);
    }
    List<TCatalogOpRecord> records = new ArrayList<>(finishedOperations_);
    // Reverse the list to show recent operations first.
    Collections.reverse(records);
    res.setFinished_catalog_operations(records);
    return res;
  }
}
