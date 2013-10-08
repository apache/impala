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

#ifndef IMPALA_CATALOG_CATALOG_SERVER_H
#define IMPALA_CATALOG_CATALOG_SERVER_H

#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "gen-cpp/CatalogService.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"
#include "catalog/catalog.h"
#include "statestore/state-store-subscriber.h"
#include "util/metrics.h"

namespace impala {

class StateStoreSubscriber;
class Catalog;

// The Impala CatalogServer manages the caching and persistence of cluster-wide metadata.
// The CatalogServer aggregates the metadata from the Hive Metastore, the NameNode,
// and potentially additional sources in the future. The CatalogServer uses the
// StateStore to broadcast metadata updates across the cluster.
// The CatalogService directly handles executing metadata update requests
// (DDL requests) from clients via a Thrift interface.
// The CatalogServer has two main components - a C++ daemon that has the StateStore
// integration code, Thrift service implementiation, and exporting of the debug
// webpage/metrics.
// The other main component is written in Java and manages caching and updating of all
// metadata. For each StateStore heartbeat, the C++ Server queries the Java metadata
// cache over JNI to get the current state of the catalog. Any updates are broadcast to
// the rest of the cluster using the StateStore over the IMPALA_CATALOG_TOPIC.
// The CatalogServer must be the only writer to the IMPALA_CATALOG_TOPIC, meaning there
// cannot be multiple CatalogServers running at the same time, as the correctness of delta
// updates relies upon this assumption.
// TODO: In the future the CatalogServer could go into a "standby" mode if it detects
// updates from another writer on the topic. This is a bit tricky because it requires
// some basic form of leader election.
class CatalogServer {
 public:
  static std::string IMPALA_CATALOG_TOPIC;
  CatalogServer(Metrics* metrics);

  // Starts this CatalogService instance.
  // Returns OK unless some error occurred in which case the status is returned.
  Status Start();

  // Returns the Thrift API interface that proxies requests onto the local CatalogService.
  const boost::shared_ptr<CatalogServiceIf>& thrift_iface() const {
    return thrift_iface_;
  }

  void RegisterWebpages(Webserver* webserver);
  Catalog* catalog() const { return catalog_.get(); }

 private:
  // Thrift API implementation which proxies requests onto this CatalogService
  boost::shared_ptr<CatalogServiceIf> thrift_iface_;
  Metrics* metrics_;
  boost::scoped_ptr<Catalog> catalog_;
  boost::scoped_ptr<StateStoreSubscriber> state_store_subscriber_;

  // Tracks the set of catalog objects that exist via their topic entry key.
  std::set<std::string> catalog_object_topic_entry_keys_;

  // The last version of the catalog that was sent over a statestore heartbeat.
  int64_t last_catalog_version_;

  // Called during each StateStore heartbeat and used to update the current set of
  // catalog objects in the IMPALA_CATALOG_TOPIC. Responds to each heartbeat with a
  // delta update containing the set of changes since the last heartbeat.
  // This function first calls into the Catalog to get the current set of catalog objects
  // that exist (along with some metadata on each object) and then checks which objects
  // are new or have been modified since the last heartbeat (by comparing the catalog
  // version of the object with the last_catalog_version_ sent). As a final step, this
  // function determines any deletions of catalog objects by looking at the
  // difference of the last set of topic entry keys that were sent and the current
  // set of topic entry keys. All updates are added to the subscriber_topic_updates list
  // and sent back to the StateStore.
  void UpdateCatalogTopicCallback(
      const StateStoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
      std::vector<TTopicDelta>* subscriber_topic_updates);

  void CatalogPathHandler(const Webserver::ArgumentMap& args,
      std::stringstream* output);

  // Debug webpage handler that is used to dump the internal state of catalog objects.
  // The caller specifies a "object_type" and "object_name" parameters and this function
  // will get the matching TCatalogObject struct, if one exists.
  // For example, to dump table "bar" in database "foo":
  // <host>:25020/catalog_objects?object_type=TABLE&object_name=foo.bar
  void CatalogObjectsPathHandler(const Webserver::ArgumentMap& args,
      std::stringstream* output);
};

}

#endif
