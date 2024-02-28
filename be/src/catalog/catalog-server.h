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

#pragma once

#include <mutex>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_set.hpp>

#include "catalog/catalog.h"
#include "common/atomic.h"
#include "gen-cpp/CatalogService.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"
#include "kudu/util/web_callback_registry.h"
#include "rapidjson/rapidjson.h"
#include "statestore/statestore-subscriber-catalog.h"
#include "util/condition-variable.h"
#include "util/metrics-fwd.h"

using kudu::HttpStatusCode;

namespace impala {

class ActiveCatalogdVersionChecker;
class Catalog;
class CatalogServiceThriftIf;
class StatestoreSubscriber;

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
/// cannot be multiple CatalogServers running at the same time, as the correctness of
/// delta updates relies upon this assumption.
///
/// Catalog HA:
/// To support catalog HA, we add the preemptive behavior for catalogd. When enabled,
/// the preemptive behavior allows the catalogd with the higher priority to become active
/// and the paired catalogd becomes standby. The active catalogd acts as the source of
/// metadata and provides catalog service for the Impala cluster.
/// By default, preemption is disabled on the catalogd which is running as single catalog
/// instance in an Impala cluster. For deployment with catalog HA, the preemption must
/// be enabled with starting flag "enable_catalogd_ha" on both the catalogd in the HA pair
/// and statestore.
/// The catalogd in an Active-Passive HA pair can be assigned an instance priority value
/// to indicate a preference for which catalogd should assume the active role. The
/// registration ID which is assigned by statestore can be used as instance priority
/// value. The lower numerical value in registration ID corresponds to a higher priority.
/// The catalogd with the higher priority is designated as active, the other catalogd is
/// designated as standby. Only the active catalogd propagates the IMPALA_CATALOG_TOPIC
/// to the cluster. This guarantees only one writer for the IMPALA_CATALOG_TOPIC in a
/// Impala cluster.
/// Statestore only send the IMPALA_CATALOG_TOPIC messages to active catalogd. Also
/// catalogds are registered as writer for IMPALA_CATALOG_TOPIC so the standby catalogd
/// does not receive catalog updates from statestore. When standby catalogd becomes
/// active, its "last_sent_catalog_version_" member variable is reset. This will lead to
/// non-delta catalog update for next IMPALA_CATALOG_TOPIC which also instructs the
/// statestore to clear all entries for the catalog update topic, so that statestore keeps
/// in-sync with new active catalogd for the catalog update topic.
/// statestore which is the registration center of an Impala cluster assigns the roles
/// for the catalogd in the HA pair after both catalogd register to statestore. When
/// statestore detects the active catalogd is not healthy, it fails over catalog service
/// to standby catalogd. When failover occurs, statestore sends notifications with the
/// address of active catalogd to all coordinators and catalogd in the cluster. The event
/// is logged in the statestore and catalogd logs. When the catalogd with the higher
/// priority recovers from a failure, statestore does not resume it as active to avoid
/// flip-flop between the two catalogd.
/// To make a specific catalogd in the HA pair as active instance, the catalogd must be
/// started with starting flag "force_catalogd_active" so that the catalogd will be
/// assigned with active role when it registers to statestore. This allows administrator
/// to manually perform catalog service failover.
class CatalogServer {
 public:
  static std::string IMPALA_CATALOG_TOPIC;
  CatalogServer(MetricGroup* metrics);

  /// Starts this CatalogService instance.
  /// Returns OK unless some error occurred in which case the status is returned.
  Status Start();

  /// Registers webpages for the input webserver. If metrics_only is set then only
  /// '/healthz' page is registered.
  void RegisterWebpages(Webserver* webserver, bool metrics_only);

  /// Returns the Thrift API interface that proxies requests onto the local CatalogService.
  const std::shared_ptr<CatalogServiceIf>& thrift_iface() const {
    return thrift_iface_;
  }
  Catalog* catalog() const { return catalog_.get(); }

  /// Adds a topic item to pending_topic_updates_. Caller must hold catalog_lock_.
  /// Returns the actual size of the item data. Returns a negative value for failures.
  int AddPendingTopicItem(std::string key, int64_t version, const uint8_t* item_data,
      uint32_t size, bool deleted);

  /// Mark service as started. Should be called only after the thrift server hosting this
  /// service has started.
  void MarkServiceAsStarted();

  // Returns the protocol version of catalog service.
  CatalogServiceVersion::type GetProtocolVersion() const {
    return protocol_version_;
  }

 private:
  friend class CatalogServiceThriftIf;

  /// Protocol version of the Catalog Service.
  CatalogServiceVersion::type protocol_version_;

  /// Indicates whether the catalog service is ready.
  std::atomic_bool service_started_{false};

  /// Thrift API implementation which proxies requests onto this CatalogService.
  std::shared_ptr<CatalogServiceIf> thrift_iface_;
  ThriftSerializer thrift_serializer_;
  MetricGroup* metrics_;
  boost::scoped_ptr<Catalog> catalog_;
  boost::scoped_ptr<StatestoreSubscriberCatalog> statestore_subscriber_;

  /// Metric that tracks the amount of time taken preparing a catalog update.
  StatsMetric<double>* topic_processing_time_metric_;

  /// Tracks the partial fetch RPC call queue length on the Catalog server.
  IntGauge* partial_fetch_rpc_queue_len_metric_;

  /// Metric that tracks if this catalogd is active.
  BooleanProperty* active_status_metric_;

  /// Metric to count the number of active status changes.
  IntCounter* num_ha_active_status_change_metric_;

  /// Metric that tracks the total size of all thread pools used in all
  /// ParallelFileMetadataLoader instances.
  IntGauge* num_file_metadata_loading_threads_metric_;

  /// Metric that tracks the total number of unfinished FileMetadataLoader tasks that
  /// are submitted to the pools.
  IntGauge* num_file_metadata_loading_tasks_metric_;

  /// Metric that tracks the total number of tables that are loading file metadata.
  IntGauge* num_tables_loading_file_metadata_metric_;

  /// Metric that tracks the total number of tables that are loading metadata.
  IntGauge* num_tables_loading_metadata_metric_;

  /// Metric that tracks the total number of tables that are loading metadata
  /// asynchronously, e.g. initial metadata loading triggered by first access of a table.
  IntGauge* num_tables_async_loading_metadata_metric_;

  /// Metric that tracks the total number of tables that are waiting for async loading.
  IntGauge* num_tables_waiting_for_async_loading_metric_;

  /// Metrics of the total number of dbs, tables and functions in the catalog cache
  IntGauge* num_dbs_metric_;
  IntGauge* num_tables_metric_;
  IntGauge* num_functions_metric_;

  /// Metrics that track the number of HMS clients
  IntGauge* num_hms_clients_idle_metric_;
  IntGauge* num_hms_clients_in_use_metric_;

  /// Thread that polls the catalog for any updates.
  std::unique_ptr<Thread> catalog_update_gathering_thread_;

  /// Thread that periodically wakes up and refreshes certain Catalog metrics.
  std::unique_ptr<Thread> catalog_metrics_refresh_thread_;

  /// Protects is_active_, active_catalogd_version_checker_,
  /// catalog_update_cv_, pending_topic_updates_, catalog_objects_to/from_version_, and
  /// last_sent_catalog_version.
  std::mutex catalog_lock_;

  /// Set to true if this catalog instance is active.
  bool is_active_;

  /// Object to track the version of received active catalogd.
  boost::scoped_ptr<ActiveCatalogdVersionChecker> active_catalogd_version_checker_;

  /// Condition variable used to signal when the catalog_update_gathering_thread_ should
  /// fetch its next set of updates from the JniCatalog. At the end of each statestore
  /// heartbeat, this CV is signaled and the catalog_update_gathering_thread_ starts
  /// querying the JniCatalog for catalog objects. Protected by the catalog_lock_.
  ConditionVariable catalog_update_cv_;

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

  /// Callback function for receiving notification of new active catalogd.
  /// This function is called when active catalogd is found from registration process,
  /// or UpdateCatalogd RPC is received. The two kinds of RPCs could be received out of
  /// sending order.
  /// Reset 'last_active_catalogd_version_' if 'is_registration_reply' is true and
  /// 'active_catalogd_version' is negative. In this case, 'catalogd_registration' is
  /// invalid and should not be used.
  void UpdateActiveCatalogd(bool is_registration_reply, int64_t active_catalogd_version,
      const TCatalogRegistration& catalogd_registration);

  /// Returns the active status of the catalogd.
  bool IsActive();

  /// Executed by the catalog_update_gathering_thread_. Calls into JniCatalog
  /// to get the latest set of catalog objects that exist, along with some metadata on
  /// each object. The results are stored in the shared catalog_objects_ data structure.
  /// Also, explicitly releases free memory back to the OS after each complete iteration.
  [[noreturn]] void GatherCatalogUpdatesThread();

  /// Executed by the catalog_metrics_refresh_thread_. Refreshes certain catalog metrics.
  [[noreturn]] void RefreshMetrics();

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
  void CatalogUrlCallback(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Debug webpage handler that is used to dump the internal state of catalog objects.
  /// The caller specifies a "object_type" and "object_name" parameters and this function
  /// will get the matching TCatalogObject struct, if one exists.
  /// For example, to dump table "bar" in database "foo":
  /// <host>:25020/catalog_objects?object_type=TABLE&object_name=foo.bar
  void CatalogObjectsUrlCallback(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Retrieves from the FE information about the current catalog usage and populates
  /// the /catalog debug webpage. The catalog usage includes information about
  /// 1. the TOP-N frequently used (in terms of number of metadata operations) tables,
  /// 2. the TOP-N tables with the highest memory requirements
  /// 3. the TOP-N tables with the most number of files.
  /// 4. the TOP-N tables with the longest metadata loading time (nanoseconds)
  ///
  /// Example output:
  /// "large_tables": [
  ///     {
  ///       "name": "functional.alltypesagg",
  ///       "mem_estimate": 212434233
  ///     }
  ///  ]
  ///  "frequent_tables": [
  ///      {
  ///        "name": "functional.alltypestiny",
  ///        "frequency": 10
  ///      }
  ///  ]
  ///  "high_file_count_tables": [
  ///      {
  ///        "name": functional.alltypesagg",
  ///        "num_files": 30
  ///      }
  ///  ]
  ///  "long_metadata_loading_tables": [
  ///      {
  ///        "name": "tpcds.warehouse",
  ///        "median_metadata_loading_time_ns": 12361844,
  ///        "max_metadata_loading_time_ns": 175518387,
  ///        "p75_loading_time_ns": 12361844,
  ///        "p95_loading_time_ns": 175518387,
  ///        "p99_loading_time_ns": 175518387,
  ///        "table_loading_count": 3
  ///      }
  ///  ]
  void GetCatalogUsage(rapidjson::Document* document);

  /// Retrieves the catalog operation metrics from FE.
  void OperationUsageUrlCallback(
      const Webserver::WebRequest& req, rapidjson::Document* document);
  void GetCatalogOpSummary(
      const TGetOperationUsageResponse& response, rapidjson::Document* document);
  void GetCatalogOpRecords(
      const TGetOperationUsageResponse& response, rapidjson::Document* document);

  /// Debug webpage handler that is used to dump all the registered metrics of a
  /// table. The caller specifies the "name" parameter which is the fully
  /// qualified table name and this function retrieves all the metrics of that
  /// table. For example, to get the table metrics of table "bar" in database
  /// "foo":
  /// <host>:25020/table_metrics?name=foo.bar
  void TableMetricsUrlCallback(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  // url handler for the metastore events page. It calls into JniCatalog to get the latest
  // metastore event processor metrics and adds it to the document
  void EventMetricsUrlCallback(
      const Webserver::WebRequest& req, rapidjson::Document* document);

  /// Raw callback to indicate whether the service is ready.
  void HealthzHandler(const Webserver::WebRequest& req, std::stringstream* data,
      HttpStatusCode* response);

  /// Json callback for /hadoop-varz. Produces Json with a list, 'configs', of (key,
  /// value) pairs, one for each Hadoop configuration value.
  void HadoopVarzHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);
};

}
