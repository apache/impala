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

#include "catalog/catalog-server.h"

#include <gutil/strings/substitute.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>

#include "catalog/catalog-util.h"
#include "exec/read-write-util.h"
#include "gen-cpp/CatalogInternalService_types.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/CatalogService_types.h"
#include "statestore/statestore-subscriber.h"
#include "util/collection-metrics.h"
#include "util/debug-util.h"
#include "util/event-metrics.h"
#include "util/json-util.h"
#include "util/logging-support.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/thrift-debug-util.h"
#include "util/webserver.h"

#include "common/names.h"

using boost::bind;
using boost::mem_fn;
using namespace apache::thrift;
using namespace rapidjson;
using namespace strings;

DEFINE_int32(catalog_service_port, 26000, "port where the CatalogService is running");
DEFINE_string(catalog_topic_mode, "full",
    "The type of data that the catalog service will publish into the Catalog "
    "StateStore topic. Valid values are 'full', 'mixed', or 'minimal'.\n"
    "\n"
    "In 'full' mode, complete catalog objects are published any time a new "
    "version is available. In 'minimal' mode, only a minimal object is published "
    "when a new version of a catalog object is available. In 'mixed' mode, both types "
    "of topic entries are published.\n"
    "\n"
    "When all impalad coordinators are configured with --use_local_catalog disabled "
    "(the default), 'full' mode should be used. If all impalad coordinators are "
    "configured with --use_local_catalog enabled, 'minimal' mode should be used. "
    "When some impalads are configured with --use_local_catalog disabled and others "
    "configured with it enabled, then 'mixed' mode is required.");

DEFINE_validator(catalog_topic_mode, [](const char* name, const string& val) {
  if (val == "full" || val == "mixed" || val == "minimal") return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be one of "
      << "'full', 'mixed', or 'minimal'";
  return false;
});

DEFINE_int32_hidden(catalog_max_parallel_partial_fetch_rpc, 32, "Maximum number of "
    "partial catalog object fetch RPCs that can run in parallel. Applicable only when "
    "local catalog mode is configured.");

DEFINE_int64_hidden(catalog_partial_fetch_rpc_queue_timeout_s, LLONG_MAX, "Maximum time "
    "(in seconds) a partial catalog object fetch RPC spends in the queue waiting "
    "to run. Must be set to a value greater than zero.");

DEFINE_int32(catalog_max_lock_skipped_topic_updates, 3, "Maximum number of topic "
    "updates skipped for a table due to lock contention in catalogd after which it must"
    "be added to the topic the update log. This limit only applies to distinct lock "
    "operations which block the topic update thread.");

DEFINE_int64(topic_update_tbl_max_wait_time_ms, 120000, "Maximum time "
     "(in milliseconds) catalog's topic update thread will wait to acquire lock on "
     "table. If the topic update thread cannot acquire a table lock it skips the table "
     "from that topic update and processes the table in the next update. However to "
     "prevent starvation it only skips the table catalog_max_lock_skipped_topic_updates "
     "many times. After that limit is hit, topic thread block until it acquires the "
     "table lock. A value of 0 disables the timeout based locking which means topic "
     "update thread will always block until table lock is acquired.");

DEFINE_int32(max_wait_time_for_sync_ddl_s, 0, "Maximum time (in seconds) until "
     "which a sync ddl operation will wait for the updated tables "
     "to be the added to the catalog topic. A value of 0 means sync ddl operation will "
     "wait as long as necessary until the update is propogated to all the coordinators. "
     "This flag only takes effect when topic_update_tbl_max_wait_time_ms is enabled."
     "A value greater than 0 means catalogd will wait until that number of seconds "
     "before throwing an error indicating that not all the "
     "coordinators might have applied the changes caused due to the ddl.");

DECLARE_string(debug_actions);
DEFINE_bool(start_hms_server, false, "When set to true catalog server starts a HMS "
    "server at a port specified by hms_port flag");

DEFINE_int32(hms_port, 5899, "If start_hms_server is set to true, this "
    "configuration specifies the port number at which it is started.");

DEFINE_bool(fallback_to_hms_on_errors, true, "This configuration is only used if "
    "start_hms_server is true. This is used to determine if the Catalog should fallback "
    "to the backing HMS service if there are errors while processing the HMS request");

DEFINE_bool(invalidate_hms_cache_on_ddls, true, "This configuration is used "
    "only if start_hms_server is true. This is used to invalidate catalogd cache "
    "for non transactional tables if alter/create/delete table hms apis are "
     "invoked over catalogd's metastore endpoint");

DEFINE_bool(hms_event_incremental_refresh_transactional_table, true, "When set to true "
    "events processor will refresh transactional tables incrementally for partition "
    "level events. Otherwise, it will always reload the whole table for transactional "
    "tables.");

DEFINE_bool(enable_sync_to_latest_event_on_ddls, false, "This configuration is "
    "used to sync db/table in catalogd cache to latest HMS event id whenever DDL "
    "operations are performed from Impala shell and catalog metastore server "
    "(if enabled). If this config is enabled, then the flag invalidate_hms_cache_on_ddls "
    "should be disabled");

DEFINE_bool(enable_reload_events, false, "This configuration is used to fire a "
    "refresh/invalidate table event to the HMS such that other event processors "
    "(such as other Impala catalogds) that poll HMS notification logs can process "
    "this event. The default value is false, so impala will not fire this "
    "event. If enabled, impala will fire this event and other catalogD will process it."
    "This config only affects the firing of the reload event. Processing of reload "
    "event will always happen");

DEFINE_string(file_metadata_reload_properties, "EXTERNAL, metadata_location,"
    "transactional, transactional_properties, TRANSLATED_TO_EXTERNAL, repl.last.id",
    "This configuration is used to whitelist the table properties that are supposed to"
    "refresh file metadata when these properties are changed. To skip this optimization,"
    "set the value to empty string");

DEFINE_bool(enable_skipping_older_events, false, "This configuration is used to skip any"
    "older events in the event processor based on the lastRefreshEventId on the"
    "database/table/partition in the cache. All the DML queries that change the metadata"
    "in the catalogD's cache will update the lastRefreshEventId i.e.., fetch the latest"
    "available event on HMS and set it on the object. In case the event processor is"
    "lagging, the older events in event processor queue can be skipped by comparing the"
    "current event id to that of lastRefreshEventId. The default is set to false to"
    "disable the optimisation. Set this true to enable skipping the older events and"
    "quickly catch with the events of HMS");

DEFINE_int32(catalog_operation_log_size, 100, "Number of catalog operation log records "
    "to retain in catalogd. If -1, the operation log has unbounded size.");

// The standby catalogd may have stale metadata for some reason, like event processor
// could have hung or could be just behind in processing events. Also the standby
// catalogd doesn't get invalidate requests from coordinators so we should probably
// reset its metadata when it becomes active to avoid stale metadata.
DEFINE_bool_hidden(catalogd_ha_reset_metadata_on_failover, true, "If true, reset all "
    "metadata when the catalogd becomes active.");

DEFINE_int32(topic_update_log_gc_frequency, 1000, "Frequency at which the entries "
    "of the catalog topic update log are garbage collected. An entry may survive "
    "for (2 * TOPIC_UPDATE_LOG_GC_FREQUENCY) - 1 topic updates.");

DEFINE_bool(invalidate_metadata_on_event_processing_failure, true,
    "This configuration is used to invalidate metadata for table(s) upon event process "
    "failure other than HMS connection issues. The default value is true. When enabled, "
    "invalidate metadata is performed automatically upon event process failure. "
    "Otherwise, failure can put metastore event processor in non-active state.");

DEFINE_bool(invalidate_global_metadata_on_event_processing_failure, false,
    "This configuration is used to global invalidate metadata when "
    "invalidate_metadata_on_event_processing_failure cannot invalidate metadata for "
    "table(s). The default value is false. When enabled, global invalidate metadata is "
    "performed automatically. Otherwise, failure can put metastore event processor in "
    "non-active state.");

DEFINE_string_hidden(inject_process_event_failure_event_types, "",
    "This configuration is used to inject event processing failure for an event type "
    "randomly. It is used for debugging purpose. Empty string indicates no failure "
    "injection and it is default behavior. Valid values are comma separated event types "
    "as specified in MetastoreEventType enum. This config is only for testing purpose "
    "and it should not be set in production environments.");

DEFINE_double_hidden(inject_process_event_failure_ratio, 1.0,
    "This configuration is used in conjunction with the config "
    "'inject_process_event_failure_event_types', to define what is the ratio of an"
    "event failure. If the generated random number is lesser than this value, then we"
    "fail the event processor(EP).");

DEFINE_string(default_skipped_hms_event_types,
    "OPEN_TXN,UPDATE_TBL_COL_STAT_EVENT,UPDATE_PART_COL_STAT_EVENT",
    "HMS event types that are not used by Impala. They are skipped by default in "
    "fetching HMS event batches. Only in few places they will be fetched, e.g. fetching "
    "the latest event time in HMS.");
DEFINE_string(common_hms_event_types, "ADD_PARTITION,ALTER_PARTITION,DROP_PARTITION,"
    "ADD_PARTITION,ALTER_PARTITION,DROP_PARTITION,CREATE_TABLE,ALTER_TABLE,DROP_TABLE,"
    "CREATE_DATABASE,ALTER_DATABASE,DROP_DATABASE,INSERT,OPEN_TXN,COMMIT_TXN,ABORT_TXN,"
    "ALLOC_WRITE_ID_EVENT,ACID_WRITE_EVENT,BATCH_ACID_WRITE_EVENT,"
    "UPDATE_TBL_COL_STAT_EVENT,DELETE_TBL_COL_STAT_EVENT,UPDATE_PART_COL_STAT_EVENT,"
    "UPDATE_PART_COL_STAT_EVENT_BATCH,DELETE_PART_COL_STAT_EVENT,COMMIT_COMPACTION_EVENT,"
    "RELOAD",
    "Common HMS event types that will be used in eventTypeSkipList when fetching events "
    "from HMS. The strings come from constants in "
    "org.apache.hadoop.hive.metastore.messaging.MessageBuilder. When bumping Hive "
    "versions, the list might need to be updated accordingly. To avoid bringing too much "
    "computation overhead to HMS's underlying RDBMS in evaluating predicates of "
    "EVENT_TYPE != 'xxx', rare event types are not tracked in this list. They are "
    "CREATE_FUNCTION,DROP_FUNCTION,ADD_PRIMARYKEY,ADD_FOREIGNKEY,ADD_UNIQUECONSTRAINT,"
    "ADD_NOTNULLCONSTRAINT, ADD_DEFAULTCONSTRAINT, ADD_CHECKCONSTRAINT, DROP_CONSTRAINT,"
    "CREATE_ISCHEMA, ALTER_ISCHEMA, DROP_ISCHEMA, ADD_SCHEMA_VERSION,"
    "ALTER_SCHEMA_VERSION, DROP_SCHEMA_VERSION, CREATE_CATALOG, ALTER_CATALOG,"
    "DROP_CATALOG, CREATE_DATACONNECTOR, ALTER_DATACONNECTOR, DROP_DATACONNECTOR.");

DECLARE_string(state_store_host);
DECLARE_int32(state_store_port);
DECLARE_string(state_store_2_host);
DECLARE_int32(state_store_2_port);
DECLARE_int32(state_store_subscriber_port);
DECLARE_string(hostname);
DECLARE_bool(compact_catalog_topic);
DECLARE_bool(enable_catalogd_ha);
DECLARE_bool(force_catalogd_active);

#ifndef NDEBUG
DECLARE_int32(stress_catalog_startup_delay_ms);
#endif

namespace impala {

string CatalogServer::IMPALA_CATALOG_TOPIC = "catalog-update";

const string CATALOG_SERVER_TOPIC_PROCESSING_TIMES =
    "catalog-server.topic-processing-time-s";
const string CATALOG_SERVER_PARTIAL_FETCH_RPC_QUEUE_LEN =
    "catalog.partial-fetch-rpc.queue-len";
const string CATALOG_ACTIVE_STATUS = "catalog-server.active-status";
const string CATALOG_HA_NUM_ACTIVE_STATUS_CHANGE =
    "catalog-server.ha-number-active-status-change";
const string CATALOG_NUM_FILE_METADATA_LOADING_THREADS =
    "catalog-server.metadata.file.num-loading-threads";
const string CATALOG_NUM_FILE_METADATA_LOADING_TASKS =
    "catalog-server.metadata.file.num-loading-tasks";
const string CATALOG_NUM_TABLES_LOADING_FILE_METADATA =
    "catalog-server.metadata.table.num-loading-file-metadata";
const string CATALOG_NUM_TABLES_LOADING_METADATA =
    "catalog-server.metadata.table.num-loading-metadata";
const string CATALOG_NUM_TABLES_ASYNC_LOADING_METADATA =
    "catalog-server.metadata.table.async-loading.num-in-progress";
const string CATALOG_NUM_TABLES_WAITING_FOR_ASYNC_LOADING =
    "catalog-server.metadata.table.async-loading.queue-len";
const string CATALOG_NUM_DBS = "catalog.num-databases";
const string CATALOG_NUM_TABLES = "catalog.num-tables";
const string CATALOG_NUM_FUNCTIONS = "catalog.num-functions";
const string CATALOG_NUM_HMS_CLIENTS_IDLE = "catalog.hms-client-pool.num-idle";
const string CATALOG_NUM_HMS_CLIENTS_IN_USE = "catalog.hms-client-pool.num-in-use";

const string CATALOG_WEB_PAGE = "/catalog";
const string CATALOG_TEMPLATE = "catalog.tmpl";
const string CATALOG_OBJECT_WEB_PAGE = "/catalog_object";
const string CATALOG_OBJECT_TEMPLATE = "catalog_object.tmpl";
const string CATALOG_OPERATIONS_WEB_PAGE = "/operations";
const string CATALOG_OPERATIONS_TEMPLATE = "catalog_operations.tmpl";
const string TABLE_METRICS_WEB_PAGE = "/table_metrics";
const string TABLE_METRICS_TEMPLATE = "table_metrics.tmpl";
const string EVENT_WEB_PAGE = "/events";
const string EVENT_METRICS_TEMPLATE = "events.tmpl";
const string CATALOG_SERVICE_HEALTH_WEB_PAGE = "/healthz";
const string HADOOP_VARZ_TEMPLATE = "hadoop-varz.tmpl";
const string HADOOP_VARZ_WEB_PAGE = "/hadoop-varz";

const int REFRESH_METRICS_INTERVAL_MS = 1000;

// Implementation for the CatalogService thrift interface.
class CatalogServiceThriftIf : public CatalogServiceIf {
 public:
  CatalogServiceThriftIf(CatalogServer* catalog_server)
      : catalog_server_(catalog_server) {
    server_address_ = TNetworkAddressToString(
        MakeNetworkAddress(FLAGS_hostname, FLAGS_catalog_service_port));
  }

  // Executes a TDdlExecRequest and returns details on the result of the operation.
  void ExecDdl(TDdlExecResponse& resp, const TDdlExecRequest& req) override {
    VLOG_RPC << "ExecDdl(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->ExecDdl(req, &resp);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.result.__set_status(thrift_status);
    VLOG_RPC << "ExecDdl(): response=" << ThriftDebugString(resp);
  }

  // Executes a TResetMetadataRequest and returns details on the result of the operation.
  void ResetMetadata(TResetMetadataResponse& resp, const TResetMetadataRequest& req)
      override {
    VLOG_RPC << "ResetMetadata(): request=" << ThriftDebugString(req);
    DebugActionNoFail(FLAGS_debug_actions, "RESET_METADATA_DELAY");
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->ResetMetadata(req, &resp);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.result.__set_status(thrift_status);
    VLOG_RPC << "ResetMetadata(): response=" << ThriftDebugString(resp);
  }

  // Executes a TUpdateCatalogRequest and returns details on the result of the
  // operation.
  void UpdateCatalog(TUpdateCatalogResponse& resp, const TUpdateCatalogRequest& req)
      override {
    VLOG_RPC << "UpdateCatalog(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->UpdateCatalog(req, &resp);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.result.__set_status(thrift_status);
    VLOG_RPC << "UpdateCatalog(): response=" << ThriftDebugString(resp);
  }

  // Gets functions in the Catalog based on the parameters of the
  // TGetFunctionsRequest.
  void GetFunctions(TGetFunctionsResponse& resp, const TGetFunctionsRequest& req)
      override {
    VLOG_RPC << "GetFunctions(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->GetFunctions(req, &resp);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "GetFunctions(): response=" << ThriftDebugString(resp);
  }

  // Gets a TCatalogObject based on the parameters of the TGetCatalogObjectRequest.
  void GetCatalogObject(TGetCatalogObjectResponse& resp,
      const TGetCatalogObjectRequest& req) override {
    VLOG_RPC << "GetCatalogObject(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->GetCatalogObject(
          req.object_desc, &resp.catalog_object);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "GetCatalogObject(): response=" << ThriftDebugStringNoThrow(resp);
  }

  void GetPartialCatalogObject(TGetPartialCatalogObjectResponse& resp,
      const TGetPartialCatalogObjectRequest& req) override {
    // TODO(todd): capture detailed metrics on the types of inbound requests, lock
    // wait times, etc.
    // TODO(todd): add some kind of limit on the number of concurrent requests here
    // to avoid thread exhaustion -- eg perhaps it would be best to use a trylock
    // on the catalog locks, or defer these calls to a separate (bounded) queue,
    // so a heavy query workload against a table undergoing a slow refresh doesn't
    // end up taking down the catalog by creating thousands of threads.
    VLOG_RPC << "GetPartialCatalogObject(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->GetPartialCatalogObject(req, &resp);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "GetPartialCatalogObject(): response=" << ThriftDebugStringNoThrow(resp);
  }

  void GetPartitionStats(TGetPartitionStatsResponse& resp,
      const TGetPartitionStatsRequest& req) override {
    VLOG_RPC << "GetPartitionStats(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->GetPartitionStats(req, &resp);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "GetPartitionStats(): response=" << ThriftDebugStringNoThrow(resp);
  }

  // Prioritizes the loading of metadata for one or more catalog objects. Currently only
  // used for loading tables/views because they are the only type of object that is loaded
  // lazily.
  void PrioritizeLoad(TPrioritizeLoadResponse& resp, const TPrioritizeLoadRequest& req)
      override {
    VLOG_RPC << "PrioritizeLoad(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->PrioritizeLoad(req);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "PrioritizeLoad(): response=" << ThriftDebugString(resp);
  }

  void UpdateTableUsage(TUpdateTableUsageResponse& resp,
      const TUpdateTableUsageRequest& req) override {
    VLOG_RPC << "UpdateTableUsage(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->UpdateTableUsage(req);
    }
    if (!status.ok()) LOG(WARNING) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "UpdateTableUsage(): response.status=" << resp.status;
  }

  void GetNullPartitionName(TGetNullPartitionNameResponse& resp,
      const TGetNullPartitionNameRequest& req) override {
    VLOG_RPC << "GetNullPartitionName(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->GetNullPartitionName(&resp);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "GetNullPartitionName(): response=" << ThriftDebugStringNoThrow(resp);
  }

  void GetLatestCompactions(TGetLatestCompactionsResponse& resp,
      const TGetLatestCompactionsRequest& req) override {
    VLOG_RPC << "GetLatestCompactions(): request=" << ThriftDebugString(req);
    Status status = AcceptRequest(req.protocol_version);
    if (status.ok()) {
      status = catalog_server_->catalog()->GetLatestCompactions(req, &resp);
    }
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "GetLatestCompactions(): response=" << ThriftDebugStringNoThrow(resp);
  }

 private:
  CatalogServer* catalog_server_;
  string server_address_;

  // Check if catalog protocols are compatible between client and catalog server.
  // Return Status::OK() if the protocols are compatible and catalog server is active.
  Status AcceptRequest(CatalogServiceVersion::type client_version) {
    Status status = Status::OK();
    if (client_version < catalog_server_->GetProtocolVersion()) {
      status = Status(TErrorCode::CATALOG_INCOMPATIBLE_PROTOCOL, client_version + 1,
          catalog_server_->GetProtocolVersion() + 1);
    } else if (FLAGS_enable_catalogd_ha && !catalog_server_->IsActive()) {
      status = Status(Substitute("Request for Catalog service is rejected since "
          "catalogd $0 is in standby mode", server_address_));
    }
    return status;
  }
};

CatalogServer::CatalogServer(MetricGroup* metrics)
  : protocol_version_(CatalogServiceVersion::V2),
    thrift_iface_(new CatalogServiceThriftIf(this)),
    thrift_serializer_(FLAGS_compact_catalog_topic), metrics_(metrics),
    is_active_(!FLAGS_enable_catalogd_ha),
    topic_updates_ready_(false), last_sent_catalog_version_(0L),
    catalog_objects_max_version_(0L) {
  topic_processing_time_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      CATALOG_SERVER_TOPIC_PROCESSING_TIMES);
  partial_fetch_rpc_queue_len_metric_ =
      metrics->AddGauge(CATALOG_SERVER_PARTIAL_FETCH_RPC_QUEUE_LEN, 0);
  num_file_metadata_loading_threads_metric_ =
      metrics->AddGauge(CATALOG_NUM_FILE_METADATA_LOADING_THREADS, 0);
  num_file_metadata_loading_tasks_metric_ =
      metrics->AddGauge(CATALOG_NUM_FILE_METADATA_LOADING_TASKS, 0);
  num_tables_loading_file_metadata_metric_ =
      metrics->AddGauge(CATALOG_NUM_TABLES_LOADING_FILE_METADATA, 0);
  num_tables_loading_metadata_metric_ =
      metrics->AddGauge(CATALOG_NUM_TABLES_LOADING_METADATA, 0);
  num_tables_async_loading_metadata_metric_ =
      metrics->AddGauge(CATALOG_NUM_TABLES_ASYNC_LOADING_METADATA, 0);
  num_tables_waiting_for_async_loading_metric_ =
      metrics->AddGauge(CATALOG_NUM_TABLES_WAITING_FOR_ASYNC_LOADING, 0);
  num_dbs_metric_ = metrics->AddGauge(CATALOG_NUM_DBS, 0);
  num_tables_metric_ = metrics->AddGauge(CATALOG_NUM_TABLES, 0);
  num_functions_metric_ = metrics->AddGauge(CATALOG_NUM_FUNCTIONS, 0);
  num_hms_clients_idle_metric_ = metrics->AddGauge(CATALOG_NUM_HMS_CLIENTS_IDLE, 0);
  num_hms_clients_in_use_metric_ = metrics->AddGauge(CATALOG_NUM_HMS_CLIENTS_IN_USE, 0);

  active_status_metric_ =
      metrics->AddProperty(CATALOG_ACTIVE_STATUS, !FLAGS_enable_catalogd_ha);
  num_ha_active_status_change_metric_ =
      metrics->AddCounter(CATALOG_HA_NUM_ACTIVE_STATUS_CHANGE, 0);
}

Status CatalogServer::Start() {
  TNetworkAddress subscriber_address =
      MakeNetworkAddress(FLAGS_hostname, FLAGS_state_store_subscriber_port);
  TNetworkAddress statestore_address =
      MakeNetworkAddress(FLAGS_state_store_host, FLAGS_state_store_port);
  TNetworkAddress statestore2_address =
      MakeNetworkAddress(FLAGS_state_store_2_host, FLAGS_state_store_2_port);
  TNetworkAddress server_address = MakeNetworkAddress(FLAGS_hostname,
      FLAGS_catalog_service_port);

  // This will trigger a full Catalog metadata load.
  catalog_.reset(new Catalog());
#ifndef NDEBUG
  if (FLAGS_stress_catalog_startup_delay_ms > 0) {
    SleepForMs(FLAGS_stress_catalog_startup_delay_ms);
  }
#endif
  RETURN_IF_ERROR(Thread::Create("catalog-server", "catalog-update-gathering-thread",
      &CatalogServer::GatherCatalogUpdatesThread, this,
      &catalog_update_gathering_thread_));
  RETURN_IF_ERROR(Thread::Create("catalog-server", "catalog-metrics-refresh-thread",
      &CatalogServer::RefreshMetrics, this, &catalog_metrics_refresh_thread_));

  active_catalogd_version_checker_.reset(new ActiveCatalogdVersionChecker());
  statestore_subscriber_.reset(new StatestoreSubscriberCatalog(
     Substitute("catalog-server@$0", TNetworkAddressToString(server_address)),
     subscriber_address, statestore_address, statestore2_address, metrics_,
     protocol_version_, server_address));

  StatestoreSubscriber::UpdateCallback cb =
      bind<void>(mem_fn(&CatalogServer::UpdateCatalogTopicCallback), this, _1, _2);
  // The catalogd never needs to read any entries from the topic. It only publishes
  // entries. So, we set a prefix to some random character that we know won't be a
  // prefix of any key. This saves a bit of network communication from the statestore
  // back to the catalog.
  string filter_prefix = "!";
  Status status = statestore_subscriber_->AddTopic(IMPALA_CATALOG_TOPIC,
      /* is_transient=*/ false, /* populate_min_subscriber_topic_version=*/ false,
      filter_prefix, cb);
  if (!status.ok()) {
    status.AddDetail("CatalogService failed to start");
    return status;
  }
  // Add callback to handle notification of updating catalogd from Statestore.
  if (FLAGS_enable_catalogd_ha) {
    StatestoreSubscriber::UpdateCatalogdCallback update_catalogd_cb =
        bind<void>(mem_fn(&CatalogServer::UpdateActiveCatalogd), this, _1, _2, _3);
    statestore_subscriber_->AddUpdateCatalogdTopic(update_catalogd_cb);
  }

  RETURN_IF_ERROR(statestore_subscriber_->Start());
  if (FLAGS_force_catalogd_active && !IsActive()) {
    // If both catalogd are started with 'force_catalogd_active' as true in short time,
    // the second election overwrite the first election. The one which registering with
    // statstore first will be inactive.
    LOG(WARNING) << "Could not start CatalogD as active instance";
  }

  // Notify the thread to start for the first time.
  {
    lock_guard<mutex> l(catalog_lock_);
    if (is_active_) catalog_update_cv_.NotifyOne();
  }
  return Status::OK();
}

void CatalogServer::RegisterWebpages(Webserver* webserver, bool metrics_only) {
  Webserver::RawUrlCallback healthz_callback =
      [this](const auto& req, auto* data, auto* response) {
        return this->HealthzHandler(req, data, response);
      };
  webserver->RegisterUrlCallback(CATALOG_SERVICE_HEALTH_WEB_PAGE, healthz_callback);

  if (metrics_only) return;

  webserver->RegisterUrlCallback(CATALOG_WEB_PAGE, CATALOG_TEMPLATE,
      [this](const auto& args, auto* doc) { this->CatalogUrlCallback(args, doc); }, true);
  webserver->RegisterUrlCallback(CATALOG_OBJECT_WEB_PAGE, CATALOG_OBJECT_TEMPLATE,
      [this](const auto& args, auto* doc) { this->CatalogObjectsUrlCallback(args, doc); },
      false);
  webserver->RegisterUrlCallback(TABLE_METRICS_WEB_PAGE, TABLE_METRICS_TEMPLATE,
      [this](const auto& args, auto* doc) { this->TableMetricsUrlCallback(args, doc); },
      false);
  webserver->RegisterUrlCallback(EVENT_WEB_PAGE, EVENT_METRICS_TEMPLATE,
      [this](const auto& args, auto* doc) { this->EventMetricsUrlCallback(args, doc); },
      true);
  webserver->RegisterUrlCallback(CATALOG_OPERATIONS_WEB_PAGE, CATALOG_OPERATIONS_TEMPLATE,
      [this](const auto& args, auto* doc) { this->OperationUsageUrlCallback(args, doc); },
      true);
  webserver->RegisterUrlCallback(HADOOP_VARZ_WEB_PAGE, HADOOP_VARZ_TEMPLATE,
      [this](const auto& args, auto* doc) { this->HadoopVarzHandler(args, doc); }, true);
  RegisterLogLevelCallbacks(webserver, true);
}

void CatalogServer::UpdateCatalogTopicCallback(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(CatalogServer::IMPALA_CATALOG_TOPIC);
  if (topic == incoming_topic_deltas.end()) return;

  unique_lock<mutex> l(catalog_lock_, std::try_to_lock);
  // Return if unable to acquire the catalog_lock_, or this instance is not active,
  // or if the topic update data is not yet ready for processing. This indicates the
  // catalog_update_gathering_thread_ is still building a topic update.
  if (!l || !is_active_ || !topic_updates_ready_) return;

  const TTopicDelta& delta = topic->second;

  // If the statestore restarts, both from_version and to_version would be 0. If catalog
  // has sent non-empty topic udpate, pending_topic_updates_ won't be from version 0 and
  // it should be re-collected.
  if (delta.from_version == 0 && delta.to_version == 0 &&
      last_sent_catalog_version_ != 0) {
    LOG(INFO) << "Statestore restart detected. Collecting a non-delta catalog update.";
    last_sent_catalog_version_ = 0L;
  } else if (!pending_topic_updates_.empty()) {
    // Process the pending topic update.
    subscriber_topic_updates->emplace_back();
    TTopicDelta& update = subscriber_topic_updates->back();
    update.topic_name = IMPALA_CATALOG_TOPIC;
    update.topic_entries = std::move(pending_topic_updates_);
    // If this is the first update sent to the statestore, instruct the
    // statestore to clear any entries it may already have for the catalog
    // update topic. This is to guarantee that upon catalog restart, the
    // statestore entries of the catalog update topic are in sync with the
    // catalog objects stored in the catalog (see IMPALA-6948).
    update.__set_clear_topic_entries((last_sent_catalog_version_ == 0));

    VLOG(1) << "A catalog update with " << update.topic_entries.size()
            << " entries is assembled. Catalog version: "
            << catalog_objects_max_version_ << " Last sent catalog version: "
            << last_sent_catalog_version_;

    // Update the new catalog version and the set of known catalog objects.
    last_sent_catalog_version_ = catalog_objects_max_version_;
  }

  // Signal the catalog update gathering thread to start.
  topic_updates_ready_ = false;
  catalog_update_cv_.NotifyOne();
}

void CatalogServer::UpdateActiveCatalogd(bool is_registration_reply,
    int64_t active_catalogd_version, const TCatalogRegistration& catalogd_registration) {
  lock_guard<mutex> l(catalog_lock_);
  if (!active_catalogd_version_checker_->CheckActiveCatalogdVersion(
          is_registration_reply, active_catalogd_version)) {
    return;
  }
  if (catalogd_registration.address.hostname.empty()
      || catalogd_registration.address.port == 0) {
    return;
  }
  LOG(INFO) << "Get notification of active catalogd: "
            << TNetworkAddressToString(catalogd_registration.address);
  bool is_matching = (catalogd_registration.address.hostname == FLAGS_hostname
      && catalogd_registration.address.port == FLAGS_catalog_service_port);
  if (is_matching) {
    if (!is_active_) {
      is_active_ = true;
      active_status_metric_->SetValue(true);
      num_ha_active_status_change_metric_->Increment(1);
      // Reset last_sent_catalog_version_ when the catalogd become active. This will
      // lead to non-delta catalog update for next IMPALA_CATALOG_TOPIC which also
      // instruct the statestore to clear all entries for the catalog update topic.
      last_sent_catalog_version_ = 0;
      // Regenerate Catalog Service ID.
      catalog_->RegenerateServiceId();
      // Clear pending topic updates.
      pending_topic_updates_.clear();
      if (FLAGS_catalogd_ha_reset_metadata_on_failover) {
        // Reset all metadata when the catalogd becomes active.
        TResetMetadataRequest req;
        TResetMetadataResponse resp;
        req.__set_header(TCatalogServiceRequestHeader());
        req.header.__set_want_minimal_response(false);
        req.__set_is_refresh(false);
        req.__set_sync_ddl(false);
        Status status = catalog_->ResetMetadata(req, &resp);
        if (!status.ok()) {
          LOG(ERROR) << "Failed to reset metadata triggered by catalogd failover.";
        }
      } else {
        // Refresh DataSource objects when the catalogd becomes active.
        Status status = catalog_->RefreshDataSources();
        if (!status.ok()) {
          LOG(ERROR) << "Failed to refresh data sources triggered by catalogd failover.";
        }
      }
      // Signal the catalog update gathering thread to start.
      topic_updates_ready_ = false;
      catalog_update_cv_.NotifyOne();
      LOG(INFO) << "This catalogd instance is changed to active status";
    }
  } else {
    if (is_active_) {
      is_active_ = false;
      active_status_metric_->SetValue(false);
      num_ha_active_status_change_metric_->Increment(1);
      LOG(INFO) << "This catalogd instance is changed to inactive status. "
                << "Current active catalogd: "
                << TNetworkAddressToString(catalogd_registration.address)
                << ", active_catalogd_version: "
                << active_catalogd_version;
      // Regenerate Catalog Service ID.
      catalog_->RegenerateServiceId();
    }
  }
}

bool CatalogServer::IsActive() {
  lock_guard<mutex> l(catalog_lock_);
  return is_active_;
}

[[noreturn]] void CatalogServer::GatherCatalogUpdatesThread() {
  while (true) {
    unique_lock<mutex> unique_lock(catalog_lock_);
    // Protect against spurious wake-ups by checking the value of topic_updates_ready_.
    // It is only safe to continue on and update the shared pending_topic_updates_
    // when topic_updates_ready_ is false, otherwise we may be in the middle of
    // processing a heartbeat.
    while (topic_updates_ready_) {
      catalog_update_cv_.Wait(unique_lock);
    }

    MonotonicStopWatch sw;
    sw.Start();

    // Clear any pending topic updates. They will have been processed by the heartbeat
    // thread by the time we make it here.
    pending_topic_updates_.clear();

    long current_catalog_version;
    Status status = catalog_->GetCatalogVersion(&current_catalog_version);
    if (!status.ok()) {
      LOG(ERROR) << status.GetDetail();
    } else if (current_catalog_version != last_sent_catalog_version_) {
      // If there has been a change since the last time the catalog was queried,
      // call into the Catalog to find out what has changed.
      TGetCatalogDeltaResponse resp;
      status = catalog_->GetCatalogDelta(this, last_sent_catalog_version_, &resp);
      if (!status.ok()) {
        LOG(ERROR) << status.GetDetail();
      } else {
        catalog_objects_max_version_ = resp.max_catalog_version;
      }
    }

    topic_processing_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    topic_updates_ready_ = true;
  }
}

[[noreturn]] void CatalogServer::RefreshMetrics() {
  while (true) {
    SleepForMs(REFRESH_METRICS_INTERVAL_MS);
    TGetCatalogServerMetricsResponse response;
    Status status = catalog_->GetCatalogServerMetrics(&response);
    if (!status.ok()) {
      LOG(ERROR) << "Error refreshing catalog metrics: " << status.GetDetail();
      continue;
    }
    partial_fetch_rpc_queue_len_metric_->SetValue(
        response.catalog_partial_fetch_rpc_queue_len);
    num_file_metadata_loading_threads_metric_->SetValue(
        response.catalog_num_file_metadata_loading_threads);
    num_file_metadata_loading_tasks_metric_->SetValue(
        response.catalog_num_file_metadata_loading_tasks);
    num_tables_loading_file_metadata_metric_->SetValue(
        response.catalog_num_tables_loading_file_metadata);
    num_tables_loading_metadata_metric_->SetValue(
        response.catalog_num_tables_loading_metadata);
    num_tables_async_loading_metadata_metric_->SetValue(
        response.catalog_num_tables_async_loading_metadata);
    num_tables_waiting_for_async_loading_metric_->SetValue(
        response.catalog_num_tables_waiting_for_async_loading);
    num_dbs_metric_->SetValue(response.catalog_num_dbs);
    num_tables_metric_->SetValue(response.catalog_num_tables);
    num_functions_metric_->SetValue(response.catalog_num_functions);
    num_hms_clients_idle_metric_->SetValue(response.catalog_num_hms_clients_idle);
    num_hms_clients_in_use_metric_->SetValue(response.catalog_num_hms_clients_in_use);
    TEventProcessorMetrics eventProcessorMetrics = response.event_metrics;
    MetastoreEventMetrics::refresh(&eventProcessorMetrics);
  }
}

void CatalogServer::CatalogUrlCallback(const Webserver::WebRequest& req,
    Document* document) {
  GetCatalogUsage(document);
  TGetDbsResult get_dbs_result;
  Status status = catalog_->GetDbs(NULL, &get_dbs_result);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  long current_catalog_version;
  status = catalog_->GetCatalogVersion(&current_catalog_version);
  if (status.ok()) {
    Value version_value;
    version_value.SetInt(current_catalog_version);
    document->AddMember("version", version_value, document->GetAllocator());
  } else {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("versionError", error, document->GetAllocator());
  }
  Value databases(kArrayType);
  for (const TDatabase& db: get_dbs_result.dbs) {
    Value database(kObjectType);
    Value str(db.db_name.c_str(), document->GetAllocator());
    database.AddMember("name", str, document->GetAllocator());

    TGetTablesResult get_table_results;
    Status status = catalog_->GetTableNames(db.db_name, NULL, &get_table_results);
    if (!status.ok()) {
      Value error(status.GetDetail().c_str(), document->GetAllocator());
      database.AddMember("error", error, document->GetAllocator());
      continue;
    }

    Value table_array(kArrayType);
    for (const string& table: get_table_results.tables) {
      Value table_obj(kObjectType);
      Value fq_name(Substitute("$0.$1", db.db_name, table).c_str(),
          document->GetAllocator());
      table_obj.AddMember("fqtn", fq_name, document->GetAllocator());
      Value table_name(table.c_str(), document->GetAllocator());
      table_obj.AddMember("name", table_name, document->GetAllocator());
      Value has_metrics;
      has_metrics.SetBool(true);
      table_obj.AddMember("has_metrics", has_metrics, document->GetAllocator());
      table_array.PushBack(table_obj, document->GetAllocator());
    }
    database.AddMember("num_tables", table_array.Size(), document->GetAllocator());
    database.AddMember("tables", table_array, document->GetAllocator());
    Value has_metrics;
    has_metrics.SetBool(true);
    database.AddMember("has_metrics", has_metrics, document->GetAllocator());
    databases.PushBack(database, document->GetAllocator());
  }
  document->AddMember("databases", databases, document->GetAllocator());
}

void CatalogServer::GetCatalogUsage(Document* document) {
  TGetCatalogUsageResponse catalog_usage_result;
  Status status = catalog_->GetCatalogUsage(&catalog_usage_result);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  // Collect information about the largest tables in terms of memory requirements
  Value large_tables(kArrayType);
  for (const auto & large_table : catalog_usage_result.large_tables) {
    Value tbl_obj(kObjectType);
    Value tbl_name(Substitute("$0.$1", large_table.table_name.db_name,
        large_table.table_name.table_name).c_str(), document->GetAllocator());
    tbl_obj.AddMember("name", tbl_name, document->GetAllocator());
    DCHECK(large_table.__isset.memory_estimate_bytes);
    tbl_obj.AddMember("mem_estimate", large_table.memory_estimate_bytes,
        document->GetAllocator());
    large_tables.PushBack(tbl_obj, document->GetAllocator());
  }
  Value has_large_tables;
  has_large_tables.SetBool(true);
  document->AddMember("has_large_tables", has_large_tables, document->GetAllocator());
  document->AddMember("large_tables", large_tables, document->GetAllocator());
  Value num_large_tables;
  num_large_tables.SetInt(catalog_usage_result.large_tables.size());
  document->AddMember("num_large_tables", num_large_tables, document->GetAllocator());

  // Collect information about the most frequently accessed tables.
  Value frequent_tables(kArrayType);
  for (const auto & frequent_table : catalog_usage_result.frequently_accessed_tables) {
    Value tbl_obj(kObjectType);
    Value tbl_name(Substitute("$0.$1", frequent_table.table_name.db_name,
        frequent_table.table_name.table_name).c_str(), document->GetAllocator());
    tbl_obj.AddMember("name", tbl_name, document->GetAllocator());
    Value num_metadata_operations;
    DCHECK(frequent_table.__isset.num_metadata_operations);
    num_metadata_operations.SetInt64(frequent_table.num_metadata_operations);
    tbl_obj.AddMember("num_metadata_ops", num_metadata_operations,
        document->GetAllocator());
    frequent_tables.PushBack(tbl_obj, document->GetAllocator());
  }
  Value has_frequent_tables;
  has_frequent_tables.SetBool(true);
  document->AddMember("has_frequent_tables", has_frequent_tables,
      document->GetAllocator());
  document->AddMember("frequent_tables", frequent_tables, document->GetAllocator());
  Value num_frequent_tables;
  num_frequent_tables.SetInt(catalog_usage_result.frequently_accessed_tables.size());
  document->AddMember("num_frequent_tables", num_frequent_tables,
      document->GetAllocator());

  // Collect information about the most number of files tables.
  Value high_filecount_tbls(kArrayType);
  for (const auto & high_filecount_tbl : catalog_usage_result.high_file_count_tables) {
    Value tbl_obj(kObjectType);
    Value tbl_name(Substitute("$0.$1", high_filecount_tbl.table_name.db_name,
        high_filecount_tbl.table_name.table_name).c_str(), document->GetAllocator());
    tbl_obj.AddMember("name", tbl_name, document->GetAllocator());
    Value num_files;
    DCHECK(high_filecount_tbl.__isset.num_files);
    num_files.SetInt64(high_filecount_tbl.num_files);
    tbl_obj.AddMember("num_files", num_files,
        document->GetAllocator());
    high_filecount_tbls.PushBack(tbl_obj, document->GetAllocator());
  }
  Value has_high_filecount_tbls;
  has_high_filecount_tbls.SetBool(true);
  document->AddMember("has_high_file_count_tables", has_high_filecount_tbls,
      document->GetAllocator());
  document->AddMember("high_file_count_tables", high_filecount_tbls,
      document->GetAllocator());
  Value num_high_filecount_tbls;
  num_high_filecount_tbls.SetInt(catalog_usage_result.high_file_count_tables.size());
  document->AddMember("num_high_file_count_tables", num_high_filecount_tbls,
      document->GetAllocator());

  // Collect information about the longest metadata loading tables
  Value longest_loading_tables(kArrayType);
  for (const auto & longest_table : catalog_usage_result.long_metadata_loading_tables) {
    Value tbl_obj(kObjectType);
    Value tbl_name(Substitute("$0.$1", longest_table.table_name.db_name,
        longest_table.table_name.table_name).c_str(), document->GetAllocator());
    tbl_obj.AddMember("name", tbl_name, document->GetAllocator());
    Value median_loading_time;
    Value max_loading_time;
    Value p75_loading_time_ns;
    Value p95_loading_time_ns;
    Value p99_loading_time_ns;
    Value table_loading_count;
    DCHECK(longest_table.__isset.median_table_loading_ns);
    DCHECK(longest_table.__isset.max_table_loading_ns);
    DCHECK(longest_table.__isset.p75_loading_time_ns);
    DCHECK(longest_table.__isset.p95_loading_time_ns);
    DCHECK(longest_table.__isset.p99_loading_time_ns);
    DCHECK(longest_table.__isset.num_table_loading);
    median_loading_time.SetInt64(longest_table.median_table_loading_ns);
    max_loading_time.SetInt64(longest_table.max_table_loading_ns);
    p75_loading_time_ns.SetInt64(longest_table.p75_loading_time_ns);
    p95_loading_time_ns.SetInt64(longest_table.p95_loading_time_ns);
    p99_loading_time_ns.SetInt64(longest_table.p99_loading_time_ns);
    table_loading_count.SetInt64(longest_table.num_table_loading);
    tbl_obj.AddMember("median_metadata_loading_time_ns", median_loading_time,
        document->GetAllocator());
    tbl_obj.AddMember("max_metadata_loading_time_ns", max_loading_time,
        document->GetAllocator());
    tbl_obj.AddMember("p75_loading_time_ns", p75_loading_time_ns,
        document->GetAllocator());
    tbl_obj.AddMember("p95_loading_time_ns", p95_loading_time_ns,
        document->GetAllocator());
    tbl_obj.AddMember("p99_loading_time_ns", p99_loading_time_ns,
        document->GetAllocator());
    tbl_obj.AddMember("table_loading_count", table_loading_count,
        document->GetAllocator());
    longest_loading_tables.PushBack(tbl_obj, document->GetAllocator());
  }
  Value has_longest_loading_tables;
  has_longest_loading_tables.SetBool(true);
  document->AddMember("has_longest_loading_tables", has_longest_loading_tables,
      document->GetAllocator());
  document->AddMember("longest_loading_tables", longest_loading_tables,
      document->GetAllocator());
  Value num_longest_loading_tables;
  num_longest_loading_tables.
      SetInt(catalog_usage_result.long_metadata_loading_tables.size());
  document->AddMember("num_longest_loading_tables", num_longest_loading_tables,
      document->GetAllocator());
}

void CatalogServer::EventMetricsUrlCallback(
    const Webserver::WebRequest& req, Document* document) {
  auto& allocator = document->GetAllocator();
  TEventProcessorMetricsSummaryResponse event_processor_summary_response;
  Status status = catalog_->GetEventProcessorSummary(&event_processor_summary_response);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), allocator);
    document->AddMember("error", error, allocator);
    return;
  }

  Value event_processor_summary(
      event_processor_summary_response.summary.c_str(), allocator);
  document->AddMember("event_processor_metrics", event_processor_summary, allocator);
  if (event_processor_summary_response.__isset.error_msg) {
    Value error_msg(event_processor_summary_response.error_msg.c_str(), allocator);
    document->AddMember("event_processor_error_msg", error_msg, allocator);
  }
  const TEventBatchProgressInfo& progress_info =
      event_processor_summary_response.progress;
  JsonObjWrapper progress_info_obj(allocator);
  // Add lag info
  progress_info_obj.AddMember("last_synced_event_id", progress_info.last_synced_event_id);
  progress_info_obj.AddMember("last_synced_event_time_s",
      progress_info.last_synced_event_time_s);
  progress_info_obj.AddMember("latest_event_id", progress_info.latest_event_id);
  progress_info_obj.AddMember("latest_event_time_s", progress_info.latest_event_time_s);
  int64_t lag_time = max(0L,
      progress_info.latest_event_time_s - progress_info.last_synced_event_time_s);
  progress_info_obj.AddMember("lag_time",
      PrettyPrinter::Print(lag_time, TUnit::TIME_S));
  progress_info_obj.AddMember("last_synced_event_time",
      ToStringFromUnix(progress_info.last_synced_event_time_s));
  progress_info_obj.AddMember("latest_event_time",
      ToStringFromUnix(progress_info.latest_event_time_s));
  // Add current batch info
  if (progress_info.num_hms_events > 0) {
    int progress = 0;
    if (progress_info.num_filtered_events > 0) {
      progress =
          100 * progress_info.current_event_index / progress_info.num_filtered_events;
    }
    int64_t now_ms = UnixMillis();
    int64_t elapsed_ms = max(0L, now_ms - progress_info.current_batch_start_time_ms);
    progress_info_obj.AddMember("num_hms_events", progress_info.num_hms_events);
    progress_info_obj.AddMember("num_filtered_events", progress_info.num_filtered_events);
    progress_info_obj.AddMember("num_synced_events", progress_info.current_event_index);
    progress_info_obj.AddMember("synced_percent", progress);
    progress_info_obj.AddMember("min_event_id", progress_info.min_event_id);
    progress_info_obj.AddMember("max_event_id", progress_info.max_event_id);
    progress_info_obj.AddMember("min_event_time",
        ToStringFromUnix(progress_info.min_event_time_s));
    progress_info_obj.AddMember("max_event_time",
        ToStringFromUnix(progress_info.max_event_time_s));
    progress_info_obj.AddMember("start_time",
        ToStringFromUnixMillis(progress_info.current_batch_start_time_ms));
    progress_info_obj.AddMember("elapsed_time",
        PrettyPrinter::Print(elapsed_ms, TUnit::TIME_MS));
    progress_info_obj.AddMember("start_time_of_event",
        ToStringFromUnixMillis(progress_info.current_event_start_time_ms));
    progress_info_obj.AddMember("elapsed_time_current_event",
        PrettyPrinter::Print(max(0L,
            now_ms - progress_info.current_event_start_time_ms), TUnit::TIME_MS));
    if (progress_info.__isset.current_event) {
      JsonObjWrapper current_event(allocator);
      current_event.AddMember("event_id", progress_info.current_event.eventId);
      current_event.AddMember("event_time",
          ToStringFromUnix(progress_info.current_event.eventTime));
      current_event.AddMember("event_type", progress_info.current_event.eventType);
      current_event.AddMember("cat_name", progress_info.current_event.catName);
      current_event.AddMember("db_name", progress_info.current_event.dbName);
      current_event.AddMember("tbl_name", progress_info.current_event.tableName);
      progress_info_obj.value.AddMember("current_event", current_event.value, allocator);
    }
    if (progress_info.current_event_batch_size > 1) {
      progress_info_obj.AddMember("current_event_batch_size",
          progress_info.current_event_batch_size);
    }
  }
  document->AddMember("progress-info", progress_info_obj.value, allocator);
}

void CatalogServer::CatalogObjectsUrlCallback(const Webserver::WebRequest& req,
    Document* document) {
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator object_type_arg = args.find("object_type");
  Webserver::ArgumentMap::const_iterator object_name_arg = args.find("object_name");
  Webserver::ArgumentMap::const_iterator json_arg = args.find("json");
  if (object_type_arg != args.end() && object_name_arg != args.end()) {
    TCatalogObjectType::type object_type =
        TCatalogObjectTypeFromName(object_type_arg->second);

    // Get the object type and name from the topic entry key
    TCatalogObject request;
    Status status =
        TCatalogObjectFromObjectName(object_type, object_name_arg->second, &request);

    if (json_arg != args.end()) {
      // Get the JSON string from FE since Thrift doesn't have a cpp implementation for
      // SimpleJsonProtocol (THRIFT-2476), so we use it's java implementation.
      // TODO: switch to use cpp implementation of SimpleJsonProtocol after THRIFT-2476
      //  is resolved.
      string json_str;
      if (status.ok()) status = catalog_->GetJsonCatalogObject(request, &json_str);
      if (status.ok()) {
        Value debug_string(json_str.c_str(), document->GetAllocator());
        document->AddMember("json_string", debug_string, document->GetAllocator());
      }
    } else {
      // Get the object and dump its contents.
      TCatalogObject result;
      if (status.ok()) status = catalog_->GetCatalogObject(request, &result);
      if(status.ok()) {
        Value debug_string(
            ThriftDebugStringNoThrow(result).c_str(), document->GetAllocator());
        document->AddMember("thrift_string", debug_string, document->GetAllocator());
      }
    }
    if (!status.ok()) {
      Value error(status.GetDetail().c_str(), document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
    }
  } else {
    Value error("Please specify values for the object_type and object_name parameters.",
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}

void CatalogServer::OperationUsageUrlCallback(
    const Webserver::WebRequest& req, Document* document) {
  TGetOperationUsageResponse operation_usage;
  Status status = catalog_->GetOperationUsage(&operation_usage);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  GetCatalogOpSummary(operation_usage, document);
  GetCatalogOpRecords(operation_usage, document);
}

void CatalogServer::GetCatalogOpSummary(const TGetOperationUsageResponse& operation_usage,
    Document* document) {
  // Add the catalog operation counters to the document
  Value catalog_op_list(kArrayType);
  for (const auto& catalog_op : operation_usage.catalog_op_counters) {
    Value catalog_op_obj(kObjectType);
    Value op_name(catalog_op.catalog_op_name.c_str(), document->GetAllocator());
    catalog_op_obj.AddMember("catalog_op_name", op_name, document->GetAllocator());
    Value op_counter;
    op_counter.SetInt64(catalog_op.op_counter);
    catalog_op_obj.AddMember("op_counter", op_counter, document->GetAllocator());
    Value table_name(catalog_op.table_name.c_str(), document->GetAllocator());
    catalog_op_obj.AddMember("table_name", table_name, document->GetAllocator());
    catalog_op_list.PushBack(catalog_op_obj, document->GetAllocator());
  }
  document->AddMember("catalog_op_list", catalog_op_list, document->GetAllocator());

  // Create a summary and add it to the document
  map<string, int> aggregated_operations;
  for (const auto& catalog_op : operation_usage.catalog_op_counters) {
    aggregated_operations[catalog_op.catalog_op_name] += catalog_op.op_counter;
  }
  Value catalog_op_summary(kArrayType);
  for (const auto& catalog_op : aggregated_operations) {
    Value catalog_op_obj(kObjectType);
    Value op_name(catalog_op.first.c_str(), document->GetAllocator());
    catalog_op_obj.AddMember("catalog_op_name", op_name, document->GetAllocator());
    Value op_counter;
    op_counter.SetInt64(catalog_op.second);
    catalog_op_obj.AddMember("op_counter", op_counter, document->GetAllocator());
    catalog_op_summary.PushBack(catalog_op_obj, document->GetAllocator());
  }
  document->AddMember("catalog_op_summary", catalog_op_summary, document->GetAllocator());
}

static void CatalogOpListToJson(const vector<TCatalogOpRecord>& catalog_ops,
    Value* catalog_op_list, Document* document) {
  for (const auto& catalog_op : catalog_ops) {
    Value obj(kObjectType);
    Value op_name(catalog_op.catalog_op_name.c_str(), document->GetAllocator());
    obj.AddMember("catalog_op_name", op_name, document->GetAllocator());

    Value thread_id;
    thread_id.SetInt64(catalog_op.thread_id);
    obj.AddMember("thread_id", thread_id, document->GetAllocator());

    Value query_id(PrintId(catalog_op.query_id).c_str(), document->GetAllocator());
    obj.AddMember("query_id", query_id, document->GetAllocator());

    Value client_ip(catalog_op.client_ip.c_str(), document->GetAllocator());
    obj.AddMember("client_ip", client_ip, document->GetAllocator());

    Value coordinator(catalog_op.coordinator_hostname.c_str(), document->GetAllocator());
    obj.AddMember("coordinator", coordinator, document->GetAllocator());

    Value user(catalog_op.user.c_str(), document->GetAllocator());
    obj.AddMember("user", user, document->GetAllocator());

    Value target_name(catalog_op.target_name.c_str(), document->GetAllocator());
    obj.AddMember("target_name", target_name, document->GetAllocator());

    Value start_time(ToStringFromUnixMillis(catalog_op.start_time_ms,
        TimePrecision::Millisecond).c_str(), document->GetAllocator());
    obj.AddMember("start_time", start_time, document->GetAllocator());

    int64_t end_time_ms;
    if (catalog_op.finish_time_ms > 0) {
      end_time_ms = catalog_op.finish_time_ms;
      Value finish_time(ToStringFromUnixMillis(catalog_op.finish_time_ms,
          TimePrecision::Millisecond).c_str(), document->GetAllocator());
      obj.AddMember("finish_time", finish_time, document->GetAllocator());
    } else {
      end_time_ms = UnixMillis();
    }

    int64_t duration_ms = end_time_ms - catalog_op.start_time_ms;
    const string& printed_duration = PrettyPrinter::Print(duration_ms, TUnit::TIME_MS);
    Value duration(printed_duration.c_str(), document->GetAllocator());
    obj.AddMember("duration", duration, document->GetAllocator());

    Value status(catalog_op.status.c_str(), document->GetAllocator());
    obj.AddMember("status", status, document->GetAllocator());

    Value details(catalog_op.details.c_str(), document->GetAllocator());
    obj.AddMember("details", details, document->GetAllocator());

    catalog_op_list->PushBack(obj, document->GetAllocator());
  }
}

void CatalogServer::GetCatalogOpRecords(const TGetOperationUsageResponse& response,
    Document* document) {
  Value inflight_catalog_ops(kArrayType);
  CatalogOpListToJson(response.in_flight_catalog_operations, &inflight_catalog_ops,
      document);
  document->AddMember("inflight_catalog_operations", inflight_catalog_ops,
      document->GetAllocator());
  Value finished_catalog_ops(kArrayType);
  CatalogOpListToJson(response.finished_catalog_operations, &finished_catalog_ops,
      document);
  document->AddMember("finished_catalog_operations", finished_catalog_ops,
      document->GetAllocator());
}

void CatalogServer::TableMetricsUrlCallback(const Webserver::WebRequest& req,
    Document* document) {
  const auto& args = req.parsed_args;
  // TODO: Enable json view of table metrics
  Webserver::ArgumentMap::const_iterator object_name_arg = args.find("name");
  if (object_name_arg != args.end()) {
    // Parse the object name to extract database and table names
    const string& full_tbl_name = object_name_arg->second;
    int pos = full_tbl_name.find('.');
    if (pos == string::npos || pos >= full_tbl_name.size() - 1) {
      stringstream error_msg;
      error_msg << "Invalid table name: " << full_tbl_name;
      Value error(error_msg.str().c_str(), document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
      return;
    }
    string metrics;
    Status status = catalog_->GetTableMetrics(
        full_tbl_name.substr(0, pos), full_tbl_name.substr(pos + 1), &metrics);
    if (status.ok()) {
      Value metrics_str(metrics.c_str(), document->GetAllocator());
      document->AddMember("table_metrics", metrics_str, document->GetAllocator());
    } else {
      Value error(status.GetDetail().c_str(), document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
    }
  } else {
    Value error("Please specify the value of the name parameter.",
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}

int CatalogServer::AddPendingTopicItem(std::string key, int64_t version,
    const uint8_t* item_data, uint32_t size, bool deleted) {
  pending_topic_updates_.emplace_back();
  TTopicItem& item = pending_topic_updates_.back();
  if (FLAGS_compact_catalog_topic) {
    Status status = CompressCatalogObject(item_data, size, &item.value);
    if (!status.ok()) {
      pending_topic_updates_.pop_back();
      LOG(ERROR) << "Error compressing topic item: " << status.GetDetail();
      return -1;
    }
  } else {
    item.value.assign(reinterpret_cast<const char*>(item_data),
        static_cast<size_t>(size));
  }
  item.key = std::move(key);
  item.deleted = deleted;
  // Skip logging partition items since FE will log their summary (IMPALA-10076).
  if (item.key.find("HDFS_PARTITION") != string::npos) return item.value.size();
  VLOG(1) << "Collected " << (deleted ? "deletion: " : "update: ") << item.key
          << ", version=" << version << ", original size=" << size
          << (FLAGS_compact_catalog_topic ?
              Substitute(", compressed size=$0", item.value.size()) : string());
  return item.value.size();
}

void CatalogServer::MarkServiceAsStarted() { service_started_ = true; }

void CatalogServer::HealthzHandler(
    const Webserver::WebRequest& req, std::stringstream* data, HttpStatusCode* response) {
  if (service_started_) {
    (*data) << "OK";
    *response = HttpStatusCode::Ok;
    return;
  }
  *(data) << "Not Available";
  *response = HttpStatusCode::ServiceUnavailable;
}

void CatalogServer::HadoopVarzHandler(const Webserver::WebRequest& req,
    Document* document) {
  TGetAllHadoopConfigsResponse response;
  Status status  = catalog_->GetAllHadoopConfigs(&response);
  if (!status.ok()) {
    LOG(ERROR) << "Error getting cluster configuration for hadoop-varz: "
               << status.GetDetail();
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  Value configs(kArrayType);
  typedef map<string, string> ConfigMap;
  for (const auto& config: response.configs) {
    Value key(config.first.c_str(), document->GetAllocator());
    Value value(config.second.c_str(), document->GetAllocator());
    Value config_json(kObjectType);
    config_json.AddMember("key", key, document->GetAllocator());
    config_json.AddMember("value", value, document->GetAllocator());
    configs.PushBack(config_json, document->GetAllocator());
  }
  document->AddMember("configs", configs, document->GetAllocator());
}

}
