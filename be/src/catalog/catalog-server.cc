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
#include "util/logging-support.h"
#include "util/metrics.h"
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

DECLARE_string(state_store_host);
DECLARE_int32(state_store_subscriber_port);
DECLARE_int32(state_store_port);
DECLARE_string(hostname);
DECLARE_bool(compact_catalog_topic);

#ifndef NDEBUG
DECLARE_int32(stress_catalog_startup_delay_ms);
#endif

namespace impala {

string CatalogServer::IMPALA_CATALOG_TOPIC = "catalog-update";

const string CATALOG_SERVER_TOPIC_PROCESSING_TIMES =
    "catalog-server.topic-processing-time-s";

const string CATALOG_SERVER_PARTIAL_FETCH_RPC_QUEUE_LEN =
    "catalog.partial-fetch-rpc.queue-len";

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

const int REFRESH_METRICS_INTERVAL_MS = 1000;

// Implementation for the CatalogService thrift interface.
class CatalogServiceThriftIf : public CatalogServiceIf {
 public:
  CatalogServiceThriftIf(CatalogServer* catalog_server)
      : catalog_server_(catalog_server) {
  }

  // Executes a TDdlExecRequest and returns details on the result of the operation.
  void ExecDdl(TDdlExecResponse& resp, const TDdlExecRequest& req) override {
    VLOG_RPC << "ExecDdl(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->ExecDdl(req, &resp);
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
    Status status = catalog_server_->catalog()->ResetMetadata(req, &resp);
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
    Status status = catalog_server_->catalog()->UpdateCatalog(req, &resp);
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
    Status status = catalog_server_->catalog()->GetFunctions(req, &resp);
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
    Status status = catalog_server_->catalog()->GetCatalogObject(req.object_desc,
        &resp.catalog_object);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
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
    Status status = catalog_server_->catalog()->GetPartialCatalogObject(req, &resp);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "GetPartialCatalogObject(): response=" << ThriftDebugStringNoThrow(resp);
  }

  void GetPartitionStats(TGetPartitionStatsResponse& resp,
      const TGetPartitionStatsRequest& req) override {
    VLOG_RPC << "GetPartitionStats(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->GetPartitionStats(req, &resp);
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
    Status status = catalog_server_->catalog()->PrioritizeLoad(req);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "PrioritizeLoad(): response=" << ThriftDebugString(resp);
  }

  void UpdateTableUsage(TUpdateTableUsageResponse& resp,
      const TUpdateTableUsageRequest& req) override {
    VLOG_RPC << "UpdateTableUsage(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->UpdateTableUsage(req);
    if (!status.ok()) LOG(WARNING) << status.GetDetail();
  }

 private:
  CatalogServer* catalog_server_;
};

CatalogServer::CatalogServer(MetricGroup* metrics)
  : thrift_iface_(new CatalogServiceThriftIf(this)),
    thrift_serializer_(FLAGS_compact_catalog_topic), metrics_(metrics),
    topic_updates_ready_(false), last_sent_catalog_version_(0L),
    catalog_objects_max_version_(0L) {
  topic_processing_time_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      CATALOG_SERVER_TOPIC_PROCESSING_TIMES);
  partial_fetch_rpc_queue_len_metric_ =
      metrics->AddGauge(CATALOG_SERVER_PARTIAL_FETCH_RPC_QUEUE_LEN, 0);
}

Status CatalogServer::Start() {
  TNetworkAddress subscriber_address =
      MakeNetworkAddress(FLAGS_hostname, FLAGS_state_store_subscriber_port);
  TNetworkAddress statestore_address =
      MakeNetworkAddress(FLAGS_state_store_host, FLAGS_state_store_port);
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

  statestore_subscriber_.reset(new StatestoreSubscriber(
     Substitute("catalog-server@$0", TNetworkAddressToString(server_address)),
     subscriber_address, statestore_address, metrics_));

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
  RETURN_IF_ERROR(statestore_subscriber_->Start());

  // Notify the thread to start for the first time.
  {
    lock_guard<mutex> l(catalog_lock_);
    catalog_update_cv_.NotifyOne();
  }
  return Status::OK();
}

void CatalogServer::RegisterWebpages(Webserver* webserver) {
  Webserver::RawUrlCallback healthz_callback =
      [this](const auto& req, auto* data, auto* response) {
        return this->HealthzHandler(req, data, response);
      };
  webserver->RegisterUrlCallback(CATALOG_SERVICE_HEALTH_WEB_PAGE, healthz_callback);
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
      false);
  webserver->RegisterUrlCallback(CATALOG_OPERATIONS_WEB_PAGE, CATALOG_OPERATIONS_TEMPLATE,
      [this](const auto& args, auto* doc) { this->OperationUsageUrlCallback(args, doc); },
      true);
  RegisterLogLevelCallbacks(webserver, true);
}

void CatalogServer::UpdateCatalogTopicCallback(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(CatalogServer::IMPALA_CATALOG_TOPIC);
  if (topic == incoming_topic_deltas.end()) return;

  unique_lock<mutex> l(catalog_lock_, std::try_to_lock);
  // Return if unable to acquire the catalog_lock_ or if the topic update data is
  // not yet ready for processing. This indicates the catalog_update_gathering_thread_
  // is still building a topic update.
  if (!l || !topic_updates_ready_) return;

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
  TEventProcessorMetricsSummaryResponse event_processor_summary_response;
  Status status = catalog_->GetEventProcessorSummary(&event_processor_summary_response);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  Value event_processor_summary(
      event_processor_summary_response.summary.c_str(), document->GetAllocator());
  document->AddMember(
      "event_processor_metrics", event_processor_summary, document->GetAllocator());
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
  TGetOperationUsageResponse opeartion_usage;
  Status status = catalog_->GetOperationUsage(&opeartion_usage);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  // Add the catalog operation counters to the document
  Value catalog_op_list(kArrayType);
  for (const auto& catalog_op : opeartion_usage.catalog_op_counters) {
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
  for (const auto& catalog_op : opeartion_usage.catalog_op_counters) {
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

void CatalogServer::TableMetricsUrlCallback(const Webserver::WebRequest& req,
    Document* document) {
  const auto& args = req.parsed_args;
  // TODO: Enable json view of table metrics
  Webserver::ArgumentMap::const_iterator object_name_arg = args.find("name");
  if (object_name_arg != args.end()) {
    // Parse the object name to extract database and table names
    const string& full_tbl_name = object_name_arg->second;
    int pos = full_tbl_name.find(".");
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

}
