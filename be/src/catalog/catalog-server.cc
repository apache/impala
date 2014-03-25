// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "catalog/catalog-server.h"

#include <gutil/strings/substitute.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "catalog/catalog-util.h"
#include "statestore/statestore-subscriber.h"
#include "util/debug-util.h"
#include "gen-cpp/CatalogInternalService_types.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/CatalogService_types.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace strings;

DEFINE_int32(catalog_service_port, 26000, "port where the CatalogService is running");
DECLARE_string(state_store_host);
DECLARE_int32(state_store_subscriber_port);
DECLARE_int32(state_store_port);
DECLARE_string(hostname);

string CatalogServer::IMPALA_CATALOG_TOPIC = "catalog-update";

const string CATALOG_SERVER_TOPIC_PROCESSING_TIMES =
    "catalog-server.topic-processing-time-s";

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
    if (!status.ok()) LOG(ERROR) << status.GetErrorMsg();
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
    if (!status.ok()) LOG(ERROR) << status.GetErrorMsg();
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
    if (!status.ok()) LOG(ERROR) << status.GetErrorMsg();
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
    if (!status.ok()) LOG(ERROR) << status.GetErrorMsg();
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
    if (!status.ok()) LOG(ERROR) << status.GetErrorMsg();
    VLOG_RPC << "GetCatalogObject(): response=" << ThriftDebugString(resp);
  }

  // Prioritizes the loading of metadata for one or more catalog objects. Currently only
  // used for loading tables/views because they are the only type of object that is loaded
  // lazily.
  virtual void PrioritizeLoad(TPrioritizeLoadResponse& resp,
      const TPrioritizeLoadRequest& req) {
    VLOG_RPC << "PrioritizeLoad(): request=" << ThriftDebugString(req);
    Status status = catalog_server_->catalog()->PrioritizeLoad(req);
    if (!status.ok()) LOG(ERROR) << status.GetErrorMsg();
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    resp.__set_status(thrift_status);
    VLOG_RPC << "PrioritizeLoad(): response=" << ThriftDebugString(resp);
  }

 private:
  CatalogServer* catalog_server_;
};

CatalogServer::CatalogServer(Metrics* metrics)
  : thrift_iface_(new CatalogServiceThriftIf(this)), thrift_serializer_(false),
    metrics_(metrics), topic_updates_ready_(false), last_sent_catalog_version_(0L),
    catalog_objects_min_version_(0L), catalog_objects_max_version_(0L) {
  topic_processing_time_metric_ = metrics_->RegisterMetric(
      new StatsMetric<double>(CATALOG_SERVER_TOPIC_PROCESSING_TIMES));
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
  catalog_update_gathering_thread_.reset(new Thread("catalog-server",
      "catalog-update-gathering-thread",
      &CatalogServer::GatherCatalogUpdatesThread, this));

  statestore_subscriber_.reset(new StatestoreSubscriber(
     Substitute("catalog-server@$0", TNetworkAddressToString(server_address)),
     subscriber_address, statestore_address, metrics_));

  StatestoreSubscriber::UpdateCallback cb =
      bind<void>(mem_fn(&CatalogServer::UpdateCatalogTopicCallback), this, _1, _2);
  Status status = statestore_subscriber_->AddTopic(IMPALA_CATALOG_TOPIC, false, cb);
  if (!status.ok()) {
    status.AddErrorMsg("CatalogService failed to start");
    return status;
  }
  RETURN_IF_ERROR(statestore_subscriber_->Start());

  // Notify the thread to start for the first time.
  {
    lock_guard<mutex> l(catalog_lock_);
    catalog_update_cv_.notify_one();
  }
  return Status::OK;
}

void CatalogServer::RegisterWebpages(Webserver* webserver) {
  Webserver::PathHandlerCallback catalog_callback =
      bind<void>(mem_fn(&CatalogServer::CatalogPathHandler), this, _1, _2);
  webserver->RegisterPathHandler("/catalog", catalog_callback);

  Webserver::PathHandlerCallback catalog_objects_callback =
      bind<void>(mem_fn(&CatalogServer::CatalogObjectsPathHandler), this, _1, _2);
  webserver->RegisterPathHandler("/catalog_objects",
      catalog_objects_callback, false, false);
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

  // If this is not a delta update, clear all catalog objects and request an update
  // from version 0 from the local catalog. There is an optimization that checks if
  // pending_topic_updates_ was just reloaded from version 0, if they have then skip this
  // step and use that data.
  if (delta.from_version == 0 && delta.to_version == 0 &&
      catalog_objects_min_version_ != 0) {
    catalog_topic_entry_keys_.clear();
    last_sent_catalog_version_ = 0L;
  } else {
    // Process the pending topic update.
    LOG_EVERY_N(INFO, 300) << "Catalog Version: " << catalog_objects_max_version_
                           << " Last Catalog Version: " << last_sent_catalog_version_;

    BOOST_FOREACH(const TTopicItem& catalog_object, pending_topic_updates_) {
      if (subscriber_topic_updates->size() == 0) {
        subscriber_topic_updates->push_back(TTopicDelta());
        subscriber_topic_updates->back().topic_name = IMPALA_CATALOG_TOPIC;
      }
      TTopicDelta& update = subscriber_topic_updates->back();
      update.topic_entries.push_back(catalog_object);
    }

    // Update the new catalog version and the set of known catalog objects.
    last_sent_catalog_version_ = catalog_objects_max_version_;
  }

  // Signal the catalog update gathering thread to start.
  topic_updates_ready_ = false;
  catalog_update_cv_.notify_one();
}

void CatalogServer::GatherCatalogUpdatesThread() {
  while (1) {
    unique_lock<mutex> unique_lock(catalog_lock_);
    // Protect against spurious wakups by checking the value of topic_updates_ready_.
    // It is only safe to continue on and update the shared pending_topic_updates_
    // when topic_updates_ready_ is false, otherwise we may be in the middle of
    // processing a heartbeat.
    while (topic_updates_ready_) {
      catalog_update_cv_.wait(unique_lock);
    }

    MonotonicStopWatch sw;
    sw.Start();

    // Clear any pending topic updates. They will have been processed by the heartbeat
    // thread by the time we make it here.
    pending_topic_updates_.clear();

    long current_catalog_version;
    Status status = catalog_->GetCatalogVersion(&current_catalog_version);
    if (!status.ok()) {
      LOG(ERROR) << status.GetErrorMsg();
    } else if (current_catalog_version != last_sent_catalog_version_) {
      // If there has been a change since the last time the catalog was queried,
      // call into the Catalog to find out what has changed.
      TGetAllCatalogObjectsResponse catalog_objects;
      status = catalog_->GetAllCatalogObjects(last_sent_catalog_version_,
          &catalog_objects);
      if (!status.ok()) {
        LOG(ERROR) << status.GetErrorMsg();
      } else {
        // Use the catalog objects to build a topic update list.
        BuildTopicUpdates(catalog_objects.objects);
        catalog_objects_min_version_ = last_sent_catalog_version_;
        catalog_objects_max_version_ = catalog_objects.max_catalog_version;
      }
    }

    topic_processing_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    topic_updates_ready_ = true;
  }
}

void CatalogServer::BuildTopicUpdates(const vector<TCatalogObject>& catalog_objects) {
  unordered_set<string> current_entry_keys;

  // Add any new/updated catalog objects to the topic.
  BOOST_FOREACH(const TCatalogObject& catalog_object, catalog_objects) {
    const string& entry_key = TCatalogObjectToEntryKey(catalog_object);
    if (entry_key.empty()) {
      LOG_EVERY_N(WARNING, 60) << "Unable to build topic entry key for TCatalogObject: "
                               << ThriftDebugString(catalog_object);
    }

    current_entry_keys.insert(entry_key);
    // Remove this entry from catalog_topic_entry_keys_. At the end of this loop, we will
    // be left with the set of keys that were in the last update, but not in this
    // update, indicating which objects have been removed/dropped.
    catalog_topic_entry_keys_.erase(entry_key);

    // This isn't a new or an updated item, skip it.
    if (catalog_object.catalog_version <= last_sent_catalog_version_) continue;

    VLOG(1) << "Publishing update: " << entry_key << "@"
            << catalog_object.catalog_version;

    pending_topic_updates_.push_back(TTopicItem());
    TTopicItem& item = pending_topic_updates_.back();
    item.key = entry_key;
    Status status = thrift_serializer_.Serialize(&catalog_object, &item.value);
    if (!status.ok()) {
      LOG(ERROR) << "Error serializing topic value: " << status.GetErrorMsg();
      pending_topic_updates_.pop_back();
    }
  }

  // Any remaining items in catalog_topic_entry_keys_ indicate the object was removed
  // since the last update.
  BOOST_FOREACH(const string& key, catalog_topic_entry_keys_) {
    pending_topic_updates_.push_back(TTopicItem());
    TTopicItem& item = pending_topic_updates_.back();
    item.key = key;
    VLOG(1) << "Publishing deletion: " << key;
    // Don't set a value to mark this item as deleted.
  }
  catalog_topic_entry_keys_.swap(current_entry_keys);
}

// TODO: Create utility function for rendering the Catalog handler so it can
// be shared between CatalogServer and ImpalaServer
void CatalogServer::CatalogPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  TGetDbsResult get_dbs_result;
  Status status = catalog_->GetDbNames(NULL, &get_dbs_result);
  if (!status.ok()) {
    (*output) << "Error: " << status.GetErrorMsg();
    return;
  }
  vector<string>& db_names = get_dbs_result.dbs;

  if (args.find("raw") == args.end()) {
    (*output) << "<h2>Catalog</h2>" << endl;

    // Build a navigation string like [ default | tpch | ... ]
    vector<string> links;
    BOOST_FOREACH(const string& db, db_names) {
      stringstream ss;
      ss << "<a href='#" << db << "'>" << db << "</a>";
      links.push_back(ss.str());
    }
    (*output) << "[ " <<  join(links, " | ") << " ] ";

    BOOST_FOREACH(const string& db, db_names) {
      (*output) << Substitute(
          "<a href='catalog_objects?object_type=DATABASE&object_name=$0' id='$0'>"
          "<h3>$0</h3></a>", db);
      TGetTablesResult get_table_results;
      Status status = catalog_->GetTableNames(db, NULL, &get_table_results);
      if (!status.ok()) {
        (*output) << "Error: " << status.GetErrorMsg();
        continue;
      }
      vector<string>& table_names = get_table_results.tables;
      (*output) << "<p>" << db << " contains <b>" << table_names.size()
                << "</b> tables</p>";

      (*output) << "<ul>" << endl;
      BOOST_FOREACH(const string& table, table_names) {
        const string& link_text = Substitute(
            "<a href='catalog_objects?object_type=TABLE&object_name=$0.$1'>$1</a>",
            db, table);
        (*output) << "<li>" << link_text << "</li>" << endl;
      }
      (*output) << "</ul>" << endl;
    }
  } else {
    (*output) << "Catalog" << endl << endl;
    (*output) << "List of databases:" << endl;
    (*output) << join(db_names, "\n") << endl << endl;

    BOOST_FOREACH(const string& db, db_names) {
      TGetTablesResult get_table_results;
      Status status = catalog_->GetTableNames(db, NULL, &get_table_results);
      if (!status.ok()) {
        (*output) << "Error: " << status.GetErrorMsg();
        continue;
      }
      vector<string>& table_names = get_table_results.tables;
      (*output) << db << " contains " << table_names.size()
                << " tables" << endl;
      BOOST_FOREACH(const string& table, table_names) {
        (*output) << "- " << table << endl;
      }
      (*output) << endl << endl;
    }
  }
}

void CatalogServer::CatalogObjectsPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  Webserver::ArgumentMap::const_iterator object_type_arg = args.find("object_type");
  Webserver::ArgumentMap::const_iterator object_name_arg = args.find("object_name");
  if (object_type_arg != args.end() && object_name_arg != args.end()) {
    TCatalogObjectType::type object_type =
        TCatalogObjectTypeFromName(object_type_arg->second);

    // Get the object type and name from the topic entry key
    TCatalogObject request;
    TCatalogObjectFromObjectName(object_type, object_name_arg->second, &request);

    // Get the object and dump its contents.
    TCatalogObject result;
    Status status = catalog_->GetCatalogObject(request, &result);
    if (status.ok()) {
      if (args.find("raw") == args.end()) {
        (*output) << "<pre>" << ThriftDebugString(result) << "</pre>";
      } else {
        (*output) << ThriftDebugString(result);
      }
    } else {
      (*output) << status.GetErrorMsg();
    }
  } else {
    (*output) << "Please specify values for the object_type and object_name parameters.";
  }
}
