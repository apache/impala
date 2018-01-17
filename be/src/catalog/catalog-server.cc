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

#include "catalog/catalog-util.h"
#include "exec/read-write-util.h"
#include "statestore/statestore-subscriber.h"
#include "util/debug-util.h"
#include "util/logging-support.h"
#include "util/webserver.h"
#include "gen-cpp/CatalogInternalService_types.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/CatalogService_types.h"

#include "common/names.h"

using boost::bind;
using boost::mem_fn;
using namespace apache::thrift;
using namespace impala;
using namespace rapidjson;
using namespace strings;

DEFINE_int32(catalog_service_port, 26000, "port where the CatalogService is running");
DECLARE_string(state_store_host);
DECLARE_int32(state_store_subscriber_port);
DECLARE_int32(state_store_port);
DECLARE_string(hostname);
DECLARE_bool(compact_catalog_topic);

string CatalogServer::IMPALA_CATALOG_TOPIC = "catalog-update";

const string CATALOG_SERVER_TOPIC_PROCESSING_TIMES =
    "catalog-server.topic-processing-time-s";

const string CATALOG_WEB_PAGE = "/catalog";
const string CATALOG_TEMPLATE = "catalog.tmpl";
const string CATALOG_OBJECT_WEB_PAGE = "/catalog_object";
const string CATALOG_OBJECT_TEMPLATE = "catalog_object.tmpl";
const string TABLE_METRICS_WEB_PAGE = "/table_metrics";
const string TABLE_METRICS_TEMPLATE = "table_metrics.tmpl";

// Implementation for the CatalogService thrift interface.
class CatalogServiceThriftIf : public CatalogServiceIf {
 public:
  CatalogServiceThriftIf(CatalogServer* catalog_server)
      : catalog_server_(catalog_server) {
  }

  // Executes a TDdlExecRequest and returns details on the result of the operation.
  virtual void ExecDdl(TDdlExecResponse& resp, const TDdlExecRequest& req) {
    VLOG_RPC << "ExecDdl(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->ExecDdl(req, &resp);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.result.__set_status(thrift_status);
    VLOG_RPC << "ExecDdl(): response=" << ThriftDebugString(resp);
  }

  // Executes a TResetMetadataRequest and returns details on the result of the operation.
  virtual void ResetMetadata(TResetMetadataResponse& resp,
      const TResetMetadataRequest& req) {
    VLOG_RPC << "ResetMetadata(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->ResetMetadata(req, &resp);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.result.__set_status(thrift_status);
    VLOG_RPC << "ResetMetadata(): response=" << ThriftDebugString(resp);
  }

  // Executes a TUpdateCatalogRequest and returns details on the result of the
  // operation.
  virtual void UpdateCatalog(TUpdateCatalogResponse& resp,
      const TUpdateCatalogRequest& req) {
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
  virtual void GetFunctions(TGetFunctionsResponse& resp,
      const TGetFunctionsRequest& req) {
    VLOG_RPC << "GetFunctions(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->GetFunctions(req, &resp);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "GetFunctions(): response=" << ThriftDebugString(resp);
  }

  // Gets a TCatalogObject based on the parameters of the TGetCatalogObjectRequest.
  virtual void GetCatalogObject(TGetCatalogObjectResponse& resp,
      const TGetCatalogObjectRequest& req) {
    VLOG_RPC << "GetCatalogObject(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->GetCatalogObject(req.object_desc,
        &resp.catalog_object);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    VLOG_RPC << "GetCatalogObject(): response=" << ThriftDebugString(resp);
  }

  // Prioritizes the loading of metadata for one or more catalog objects. Currently only
  // used for loading tables/views because they are the only type of object that is loaded
  // lazily.
  virtual void PrioritizeLoad(TPrioritizeLoadResponse& resp,
      const TPrioritizeLoadRequest& req) {
    VLOG_RPC << "PrioritizeLoad(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->PrioritizeLoad(req);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "PrioritizeLoad(): response=" << ThriftDebugString(resp);
  }

  virtual void SentryAdminCheck(TSentryAdminCheckResponse& resp,
      const TSentryAdminCheckRequest& req) {
    VLOG_RPC << "SentryAdminCheck(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->SentryAdminCheck(req);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "SentryAdminCheck(): response=" << ThriftDebugString(resp);
  }

 private:
  CatalogServer* catalog_server_;
};

CatalogServer::CatalogServer(MetricGroup* metrics)
  : thrift_iface_(new CatalogServiceThriftIf(this)),
    thrift_serializer_(FLAGS_compact_catalog_topic), metrics_(metrics),
    topic_updates_ready_(false), last_sent_catalog_version_(0L),
    catalog_objects_min_version_(0L), catalog_objects_max_version_(0L) {
  topic_processing_time_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      CATALOG_SERVER_TOPIC_PROCESSING_TIMES);
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
  Status status = Thread::Create("catalog-server", "catalog-update-gathering-thread",
      &CatalogServer::GatherCatalogUpdatesThread, this,
      &catalog_update_gathering_thread_);
  if (!status.ok()) {
    status.AddDetail("CatalogService failed to start");
    return status;
  }

  statestore_subscriber_.reset(new StatestoreSubscriber(
     Substitute("catalog-server@$0", TNetworkAddressToString(server_address)),
     subscriber_address, statestore_address, metrics_));

  StatestoreSubscriber::UpdateCallback cb =
      bind<void>(mem_fn(&CatalogServer::UpdateCatalogTopicCallback), this, _1, _2);
  status = statestore_subscriber_->AddTopic(IMPALA_CATALOG_TOPIC, false, cb);
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
  webserver->RegisterUrlCallback(CATALOG_WEB_PAGE, CATALOG_TEMPLATE,
      [this](const auto& args, auto* doc) { this->CatalogUrlCallback(args, doc); });
  webserver->RegisterUrlCallback(CATALOG_OBJECT_WEB_PAGE, CATALOG_OBJECT_TEMPLATE,
      [this](const auto& args, auto* doc) { this->CatalogObjectsUrlCallback(args, doc); },
      false);
  webserver->RegisterUrlCallback(TABLE_METRICS_WEB_PAGE, TABLE_METRICS_TEMPLATE,
      [this](const auto& args, auto* doc) { this->TableMetricsUrlCallback(args, doc); },
      false);
  RegisterLogLevelCallbacks(webserver, true);
}

void CatalogServer::UpdateCatalogTopicCallback(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(CatalogServer::IMPALA_CATALOG_TOPIC);
  if (topic == incoming_topic_deltas.end()) return;

  try_mutex::scoped_try_lock l(catalog_lock_);
  // Return if unable to acquire the catalog_lock_ or if the topic update data is
  // not yet ready for processing. This indicates the catalog_update_gathering_thread_
  // is still building a topic update.
  if (!l || !topic_updates_ready_) return;

  const TTopicDelta& delta = topic->second;

  // If not generating a delta update and 'pending_topic_updates_' doesn't already contain
  // the full catalog (beginning with version 0), then force GatherCatalogUpdatesThread()
  // to reload the full catalog.
  if (delta.from_version == 0 && catalog_objects_min_version_ != 0) {
    last_sent_catalog_version_ = 0L;
  } else {
    // Process the pending topic update.
    LOG_EVERY_N(INFO, 300) << "Catalog Version: " << catalog_objects_max_version_
                           << " Last Catalog Version: " << last_sent_catalog_version_;

    subscriber_topic_updates->emplace_back();
    TTopicDelta& update = subscriber_topic_updates->back();
    update.topic_name = IMPALA_CATALOG_TOPIC;
    update.topic_entries = std::move(pending_topic_updates_);

    // Update the new catalog version and the set of known catalog objects.
    last_sent_catalog_version_ = catalog_objects_max_version_;
  }

  // Signal the catalog update gathering thread to start.
  topic_updates_ready_ = false;
  catalog_update_cv_.NotifyOne();
}

[[noreturn]] void CatalogServer::GatherCatalogUpdatesThread() {
  while (1) {
    unique_lock<mutex> unique_lock(catalog_lock_);
    // Protect against spurious wakups by checking the value of topic_updates_ready_.
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
        catalog_objects_min_version_ = last_sent_catalog_version_;
        catalog_objects_max_version_ = resp.max_catalog_version;
      }
    }

    topic_processing_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    topic_updates_ready_ = true;
  }
}

void CatalogServer::CatalogUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  GetCatalogUsage(document);
  TGetDbsResult get_dbs_result;
  Status status = catalog_->GetDbs(NULL, &get_dbs_result);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
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
  for (int i = 0; i < catalog_usage_result.large_tables.size(); ++i) {
    Value tbl_obj(kObjectType);
    const auto& large_table = catalog_usage_result.large_tables[i];
    Value tbl_name(Substitute("$0.$1", large_table.table_name.db_name,
        large_table.table_name.table_name).c_str(), document->GetAllocator());
    tbl_obj.AddMember("name", tbl_name, document->GetAllocator());
    DCHECK(large_table.__isset.memory_estimate_bytes);
    Value memory_estimate(PrettyPrinter::Print(large_table.memory_estimate_bytes,
        TUnit::BYTES).c_str(), document->GetAllocator());
    tbl_obj.AddMember("mem_estimate", memory_estimate, document->GetAllocator());
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
  for (int i = 0; i < catalog_usage_result.frequently_accessed_tables.size(); ++i) {
    Value tbl_obj(kObjectType);
    const auto& frequent_table = catalog_usage_result.frequently_accessed_tables[i];
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
}

void CatalogServer::CatalogObjectsUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  Webserver::ArgumentMap::const_iterator object_type_arg = args.find("object_type");
  Webserver::ArgumentMap::const_iterator object_name_arg = args.find("object_name");
  if (object_type_arg != args.end() && object_name_arg != args.end()) {
    TCatalogObjectType::type object_type =
        TCatalogObjectTypeFromName(object_type_arg->second);

    // Get the object type and name from the topic entry key
    TCatalogObject request;
    Status status =
        TCatalogObjectFromObjectName(object_type, object_name_arg->second, &request);

    // Get the object and dump its contents.
    TCatalogObject result;
    if (status.ok()) status = catalog_->GetCatalogObject(request, &result);
    if (status.ok()) {
      Value debug_string(ThriftDebugString(result).c_str(), document->GetAllocator());
      document->AddMember("thrift_string", debug_string, document->GetAllocator());
    } else {
      Value error(status.GetDetail().c_str(), document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
    }
  } else {
    Value error("Please specify values for the object_type and object_name parameters.",
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}

void CatalogServer::TableMetricsUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
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

bool CatalogServer::AddPendingTopicItem(std::string key, const uint8_t* item_data,
    uint32_t size, bool deleted) {
  pending_topic_updates_.emplace_back();
  TTopicItem& item = pending_topic_updates_.back();
  if (FLAGS_compact_catalog_topic) {
    Status status = CompressCatalogObject(item_data, size, &item.value);
    if (!status.ok()) {
      pending_topic_updates_.pop_back();
      LOG(ERROR) << "Error compressing topic item: " << status.GetDetail();
      return false;
    }
  } else {
    item.value.assign(reinterpret_cast<const char*>(item_data),
        static_cast<size_t>(size));
  }
  item.key = std::move(key);
  item.deleted = deleted;
  VLOG(1) << "Publishing " << (deleted ? "deletion: " : "update: ") << item.key <<
      " original size: " << size << (FLAGS_compact_catalog_topic ?
      Substitute(" compressed size: $0", item.value.size()) : string());
  return true;
}
