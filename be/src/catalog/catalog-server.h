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

#ifndef IMPALA_CATALOG_CATALOG_SERVER_H
#define IMPALA_CATALOG_CATALOG_SERVER_H

#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_set.hpp>

#include "gen-cpp/CatalogService.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"
#include "catalog/catalog.h"
#include "statestore/statestore-subscriber.h"
#include "util/metrics.h"
#include "rapidjson/rapidjson.h"

namespace impala {

class StatestoreSubscriber;
class Catalog;

/// The Impala CatalogServer manages the caching and persistence of cluster-wide metadata.
/// The CatalogServer aggregates the metadata from the Hive Metastore, the NameNode,
/// and potentially additional sources in the future. The CatalogServer uses the
/// Statestore to broadcast metadata updates across the cluster.
/// The CatalogService directly handles executing metadata update requests
/// (DDL requests) from clients via a Thrift interface.
/// The CatalogServer has two main components - a C++ daemon that has the Statestore
/// integration code, Thrift service implementiation, and exporting of the debug
/// webpage/metrics.
/// The other main component is written in Java and manages caching and updating of all
/// metadata. For each Statestore heartbeat, the C++ Server queries the Java metadata
/// cache over JNI to get the current state of the catalog. Any updates are broadcast to
/// the rest of the cluster using the Statestore over the IMPALA_CATALOG_TOPIC.
/// The CatalogServer must be the only writer to the IMPALA_CATALOG_TOPIC, meaning there
/// cannot be multiple CatalogServers running at the same time, as the correctness of delta
/// updates relies upon this assumption.
/// TODO: In the future the CatalogServer could go into a "standby" mode if it detects
/// updates from another writer on the topic. This is a bit tricky because it requires
/// some basic form of leader election.
class CatalogServer {
 public:
  static std::string IMPALA_CATALOG_TOPIC;
  CatalogServer(MetricGroup* metrics);

  /// Starts this CatalogService instance.
  /// Returns OK unless some error occurred in which case the status is returned.
  Status Start();

  void RegisterWebpages(Webserver* webserver);

  /// Returns the Thrift API interface that proxies requests onto the local CatalogService.
  const boost::shared_ptr<CatalogServiceIf>& thrift_iface() const {
    return thrift_iface_;
  }
  Catalog* catalog() const { return catalog_.get(); }

 private:
  /// Thrift API implementation which proxies requests onto this CatalogService.
  boost::shared_ptr<CatalogServiceIf> thrift_iface_;
  ThriftSerializer thrift_serializer_;
  MetricGroup* metrics_;
  boost::scoped_ptr<Catalog> catalog_;
  boost::scoped_ptr<StatestoreSubscriber> statestore_subscriber_;

  /// Metric that tracks the amount of time taken preparing a catalog update.
  StatsMetric<double>* topic_processing_time_metric_;

  /// Thread that polls the catalog for any updates.
  std::unique_ptr<Thread> catalog_update_gathering_thread_;

  /// Protects catalog_update_cv_, pending_topic_updates_,
  /// catalog_objects_to/from_version_, and last_sent_catalog_version.
  boost::mutex catalog_lock_;

  /// Condition variable used to signal when the catalog_update_gathering_thread_ should
  /// fetch its next set of updates from the JniCatalog. At the end of each statestore
  /// heartbeat, this CV is signaled and the catalog_update_gathering_thread_ starts
  /// querying the JniCatalog for catalog objects. Protected by the catalog_lock_.
  boost::condition_variable catalog_update_cv_;

  /// The latest available set of catalog topic updates (additions/modifications, and
  /// deletions). Set by the catalog_update_gathering_thread_ and protected by
  /// catalog_lock_.
  std::vector<TTopicItem> pending_topic_updates_;

  /// Flag used to indicate when new topic updates are ready for processing by the
  /// heartbeat thread. Set to false at the end of each heartbeat, before signaling
  /// the catalog_update_gathering_thread_. Set to true by the
  /// catalog_update_gathering_thread_ when it is done building the latest set of
  /// pending_topic_updates_.
  bool topic_updates_ready_;

  /// The last version of the catalog that was sent over a statestore heartbeat.
  /// Set in UpdateCatalogTopicCallback() and protected by the catalog_lock_.
  int64_t last_sent_catalog_version_;

  /// The minimum catalog object version in pending_topic_updates_. All items in
  /// pending_topic_updates_ will be greater than this version. Set by the
  /// catalog_update_gathering_thread_ and protected by catalog_lock_.
  int64_t catalog_objects_min_version_;

  /// The max catalog version in pending_topic_updates_. Set by the
  /// catalog_update_gathering_thread_ and protected by catalog_lock_.
  int64_t catalog_objects_max_version_;

  /// Called during each Statestore heartbeat and is responsible for updating the current
  /// set of catalog objects in the IMPALA_CATALOG_TOPIC. Responds to each heartbeat with a
  /// delta update containing the set of changes since the last heartbeat. This function
  /// finds all catalog objects that have a catalog version greater than the last update
  /// sent by calling into the JniCatalog. The topic is updated with any catalog objects
  /// that are new or have been modified since the last heartbeat (by comparing the
  /// catalog version of the object with last_sent_catalog_version_). At the end of
  /// execution it notifies the catalog_update_gathering_thread_ to fetch the next set of
  /// updates from the JniCatalog. All updates are added to the subscriber_topic_updates
  /// list and sent back to the Statestore.
  void UpdateCatalogTopicCallback(
      const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
      std::vector<TTopicDelta>* subscriber_topic_updates);

  /// Executed by the catalog_update_gathering_thread_. Calls into JniCatalog
  /// to get the latest set of catalog objects that exist, along with some metadata on
  /// each object. The results are stored in the shared catalog_objects_ data structure.
  /// Also, explicitly releases free memory back to the OS after each complete iteration.
  [[noreturn]] void GatherCatalogUpdatesThread();

  /// Builds the next topic update to send based on what items
  /// have been added/changed/removed from the catalog since the last hearbeat. To do
  /// this, it enumerates the given catalog objects returned looking for the objects that
  /// have a catalog version that is > the catalog version sent with the last heartbeat.
  /// 'topic_deletions' is true if 'catalog_objects' contain deleted catalog
  /// objects.
  ///
  /// The key for each entry is a string composed of:
  /// "TCatalogObjectType:<unique object name>". So for table foo.bar, the key would be
  /// "TABLE:foo.bar". Encoding the object type information in the key ensures the keys
  /// are unique. Must hold catalog_lock_ when calling this function.
  void BuildTopicUpdates(const std::vector<TCatalogObject>& catalog_objects,
      bool topic_deletions);

  /// Example output:
  /// "databases": [
  ///         {
  ///             "name": "_impala_builtins",
  ///             "num_tables": 0,
  ///             "tables": []
  ///         },
  ///         {
  ///             "name": "default",
  ///             "num_tables": 1,
  ///             "tables": [
  ///                 {
  ///                     "fqtn": "default.test_table",
  ///                     "name": "test_table"
  ///                 }
  ///             ]
  ///         }
  ///     ]
  void CatalogUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Debug webpage handler that is used to dump the internal state of catalog objects.
  /// The caller specifies a "object_type" and "object_name" parameters and this function
  /// will get the matching TCatalogObject struct, if one exists.
  /// For example, to dump table "bar" in database "foo":
  /// <host>:25020/catalog_objects?object_type=TABLE&object_name=foo.bar
  void CatalogObjectsUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);
};

}

#endif
