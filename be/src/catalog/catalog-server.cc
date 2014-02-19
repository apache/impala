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

#include <google/malloc_extension.h>
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
  : thrift_iface_(new CatalogServiceThriftIf(this)),
    metrics_(metrics),
    catalog_objects_ready_(false),
    last_sent_catalog_version_(0L) {
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

  {
    try_mutex::scoped_try_lock l(catalog_lock_);
    // Return if unable to acquire the catalog_lock_ or if the catalog_objects data is
    // not yet ready for processing. This indicates the catalog_update_gathering_thread_
    // is still gathering results from the JniCatalog.
    if (!l || !catalog_objects_ready_) return;
  }

  // This function determines what items have been added/removed from the catalog
  // since the last heartbeat. To do this, it enumerates all the catalog objects returned
  // from the last call to the JniCatalog (updated by the catalog_update_gathering_thread_
  // thread) looking for the objects that have a catalog version that is > the catalog
  // version sent with the last heartbeat. To determine items that have been deleted,
  // it saves the set of topic entry keys sent with the last update and looks at the
  // difference between it and the current set of topic entry keys.
  // The key for each entry is a string composed of:
  // "TCatalogObjectType:<unique object name>". So for table foo.bar, the key would be
  // "TABLE:foo.bar". By encoding the object type information in the key it helps uniquify
  // the keys as well as help to determine what object type was removed in a state store
  // delta update since the state store only sends key names for deleted items.
  const TTopicDelta& delta = topic->second;

  // If this is not a delta update, clear all catalog objects and request an update
  // from version 0. There is a special check to see if the catalog_objects_ have been
  // reloaded from version 0, if they have then skip this step and use that data.
  if (delta.from_version == 0 && delta.to_version == 0 &&
      catalog_objects_from_version_ != 0) {
    catalog_object_topic_entry_keys_.clear();
    catalog_objects_.reset(new TGetAllCatalogObjectsResponse());
  }
  LOG_EVERY_N(INFO, 300) << "Catalog Version: " << catalog_objects_->max_catalog_version
                         << " Last Catalog Version: " << last_sent_catalog_version_;

  // Add any new/updated catalog objects to the topic.
  set<string> current_entry_keys;
  ThriftSerializer thrift_serializer(false);
  BOOST_FOREACH(const TCatalogObject& catalog_object, catalog_objects_->objects) {
    const string& entry_key = TCatalogObjectToEntryKey(catalog_object);
    if (entry_key.empty()) {
      LOG_EVERY_N(WARNING, 60) << "Unable to build topic entry key for TCatalogObject: "
                               << ThriftDebugString(catalog_object);
    }
    current_entry_keys.insert(entry_key);

    // Check if we knew about this topic entry key in the last update, and if so remove
    // it from the catalog_object_topic_entry_keys_. At the end of this loop, we will
    // be left with the set of keys that were in the last update, but not in this
    // update, indicating which objects have been removed/dropped.
    set<string>::iterator itr = catalog_object_topic_entry_keys_.find(entry_key);
    if (itr != catalog_object_topic_entry_keys_.end()) {
      catalog_object_topic_entry_keys_.erase(itr);
    }

    // This isn't a new or an updated item, skip it.
    if (catalog_object.catalog_version <= last_sent_catalog_version_) continue;

    VLOG(1) << "Adding update: " << entry_key << "@"
            << catalog_object.catalog_version;

    if (subscriber_topic_updates->size() == 0) {
      subscriber_topic_updates->push_back(TTopicDelta());
      subscriber_topic_updates->back().topic_name = IMPALA_CATALOG_TOPIC;
    }
    TTopicDelta& update = subscriber_topic_updates->back();
    update.topic_entries.push_back(TTopicItem());
    TTopicItem& item = update.topic_entries.back();
    item.key = entry_key;

    Status status = thrift_serializer.Serialize(&catalog_object, &item.value);
    if (!status.ok()) {
      LOG(ERROR) << "Error serializing topic value: " << status.GetErrorMsg();
      subscriber_topic_updates->pop_back();
    }
  }

  // Add all deleted items to the topic. Any remaining items in
  // catalog_object_topic_entry_keys_ indicate a difference since the last update
  // (the object was removed), so mark it as deleted.
  BOOST_FOREACH(const string& key, catalog_object_topic_entry_keys_) {
    if (subscriber_topic_updates->size() == 0) {
      subscriber_topic_updates->push_back(TTopicDelta());
      subscriber_topic_updates->back().topic_name = IMPALA_CATALOG_TOPIC;
    }
    TTopicDelta& update = subscriber_topic_updates->back();
    update.topic_entries.push_back(TTopicItem());
    TTopicItem& item = update.topic_entries.back();
    item.key = key;
    VLOG(1) << "Adding deletion: " << key;
    // Don't set a value to mark this item as deleted.
  }

  // Update the new catalog version and the set of known catalog objects.
  catalog_object_topic_entry_keys_.swap(current_entry_keys);
  last_sent_catalog_version_ = catalog_objects_->max_catalog_version;

  // Signal the catalog update gathering thread to start.
  lock_guard<mutex> l(catalog_lock_);
  catalog_objects_ready_ = false;
  catalog_update_cv_.notify_one();
}

void CatalogServer::GatherCatalogUpdatesThread() {
  while (1) {
    unique_lock<mutex> unique_lock(catalog_lock_);
    catalog_update_cv_.wait(unique_lock);

    // Protect against spurious wakups by checking the value of catalog_objects_ready_.
    // It is only safe to continue on and update the shared catalog_objects_ struct
    // when this flag is false, otherwise we may be in the middle processing a heartbeat.
    if (catalog_objects_ready_) continue;

    // Call into the Catalog to get all the catalog objects (as Thrift structs).
    TGetAllCatalogObjectsResponse* resp = new TGetAllCatalogObjectsResponse();
    Status s = catalog_->GetAllCatalogObjects(last_sent_catalog_version_, resp);
    if (!s.ok()) {
      LOG(ERROR) << s.GetErrorMsg();
      delete resp;
    } else {
      catalog_objects_.reset(resp);
    }
    catalog_objects_from_version_ = last_sent_catalog_version_;
    catalog_objects_ready_ = true;

#ifndef ADDRESS_SANITIZER
    // Required to ensure memory gets released back to the OS, even if tcmalloc doesn't do
    // it for us. This is because tcmalloc releases memory based on the
    // TCMALLOC_RELEASE_RATE property, which is not actually a rate but a divisor based
    // on the number of blocks that have been deleted. When tcmalloc does decide to
    // release memory, it removes a single span from the PageHeap. This means there are
    // certain allocation patterns that can lead to OOM due to not enough memory being
    // released by tcmalloc, even when that memory is no longer being used.
    // One example is continually resizing a vector which results in many allocations.
    // Even after the vector goes out of scope, all the memory will not be released
    // unless there are enough other deletions that are occurring in the system.
    // This can eventually lead to OOM/crashes (see IMPALA-818).
    // See: http://google-perftools.googlecode.com/svn/trunk/doc/tcmalloc.html#runtime
    MallocExtension::instance()->ReleaseFreeMemory();
#endif
  }
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
