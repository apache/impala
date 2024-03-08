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

#include <iosfwd>
#include <rapidjson/document.h>
#include "kudu/util/web_callback_registry.h"
#include "util/webserver.h"

#include "service/impala-server.h"

using kudu::HttpStatusCode;

namespace impala {

class AdmissionController;
class ClusterMembershipMgr;

/// Handles all webserver callbacks for an ImpalaServer. This class is a friend of
/// ImpalaServer in order to access the internal state needed to generate the debug
/// webpages.
class ImpalaHttpHandler {
 public:
  static ImpalaHttpHandler* CreateImpaladHandler(ImpalaServer* server,
      AdmissionController* admission_controller,
      ClusterMembershipMgr* cluster_membership_mgr) {
    return new ImpalaHttpHandler(
        server, admission_controller, cluster_membership_mgr, false);
  }

  static ImpalaHttpHandler* CreateAdmissiondHandler(
      AdmissionController* admission_controller,
      ClusterMembershipMgr* cluster_membership_mgr) {
    return new ImpalaHttpHandler(
        nullptr, admission_controller, cluster_membership_mgr, true);
  }

  /// Registers per-Impalad webserver callbacks. If 'metrics_only' is true, only registers
  /// the callbacks needed by the metrics server, i.e. /healthz.
  void RegisterHandlers(Webserver* webserver, bool metrics_only = false);

 private:
  ImpalaHttpHandler(ImpalaServer* server, AdmissionController* admission_controller,
      ClusterMembershipMgr* cluster_membership_mgr, bool is_admissiond);

  ImpalaServer* server_;
  AdmissionController* admission_controller_;
  ClusterMembershipMgr* cluster_membership_mgr_;

  /// If true, this is an admissiond and we'll only expose admission related endpoints,
  /// otherwise its an impalad.
  bool is_admissiond_;

  /// Raw callback to indicate whether the server is ready to accept queries.
  void HealthzHandler(const Webserver::WebRequest& req, std::stringstream* data,
      HttpStatusCode* response);

  /// Json callback for /hadoop-varz. Produces Json with a list, 'configs', of (key,
  /// value) pairs, one for each Hadoop configuration value.
  void HadoopVarzHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Add description strings for the query record table header tooltips.
  void AddQueryRecordTips(rapidjson::Document* document);

  /// Returns two sorted lists of queries, one in-flight and one completed, as well as a
  /// list of active backends and their plan-fragment count.
  //
  /// "in_flight_queries": [],
  /// "num_in_flight_queries": 0,
  /// "completed_queries": [
  ///       {
  ///         "effective_user": "henry",
  ///         "default_db": "default",
  ///         "stmt": "select sleep(10000)",
  ///         "stmt_type": "QUERY",
  ///         "start_time": "2014-08-07 18:37:47.923614000",
  ///         "end_time": "2014-08-07 18:37:58.146494000",
  ///         "progress": "0 / 0 (0%)",
  ///         "state": "FINISHED",
  ///         "rows_fetched": 1,
  ///         "query_id": "7c459a59fb8cefe3:8b7042d55bf19887"
  ///       }
  /// ],
  /// "completed_log_size": 25,
  /// "query_locations": [
  ///     {
  ///       "location": "henry-impala:22000",
  ///        "count": 0
  ///     }
  /// ]
  void QueryStateHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Json callback for /query_profile. Expects query_id as an argument. If a json
  /// profile is requested, the JSON profile is returned in 'document' under
  /// "contents". Otherwise 'document' has 'profile' set to the profile string,
  /// and 'query_id' set to the query ID.
  void QueryProfileHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Webserver callback. Produces a Json structure with query summary information.
  /// Example:
  /// { "summary": <....>,
  ///   "plan": <....>,
  ///   "stmt": "select count(*) from functional.alltypes"
  ///   "id": <...>,
  ///   "state": "FINISHED"
  /// }
  /// If include_plan_json is true, 'plan_json' will be set to a JSON representation of
  /// the query plan. If include_summary is true, 'summary' will be a text rendering of
  /// the query summary.
  void QuerySummaryHandler(bool include_plan_json, bool include_summary,
      const Webserver::WebRequest& req, rapidjson::Document* document);

  /// If 'args' contains a query id, serializes all backend states for that query to
  /// 'document'.
  void QueryBackendsHandler(
      const Webserver::WebRequest& req, rapidjson::Document* document);

  /// If 'args' contains a query id, serializes all fragment instance states for all
  /// backends for that query to 'document'.
  void QueryFInstancesHandler(
      const Webserver::WebRequest& req, rapidjson::Document* document);

  /// Cancels an in-flight query and writes the result to 'contents'.
  void CancelQueryHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Closes an active session with a client.
  void CloseSessionHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Helper method to put query profile in 'document' with required format.
  void QueryProfileHelper(const Webserver::WebRequest& req,
      rapidjson::Document* document, TRuntimeProfileFormat::type format,
      bool internal_profile = false);

  /// Upon return, 'document' will contain the query profile as a base64 encoded object in
  /// 'contents'.
  void QueryProfileEncodedHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Upon return, 'document' will contain the query profile as a utf8 string in
  /// 'contents'.
  void QueryProfileTextHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Upon return, 'document' will contain the query profile as a JSON object in
  /// 'contents'.
  void QueryProfileJsonHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Produces a list of inflight query IDs printed as text in 'contents'.
  void InflightQueryIdsHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Json callback for /sessions, which prints a table of active client sessions.
  /// "sessions": [
  /// {
  ///     "type": "BEESWAX",
  ///     "num_queries": 0,
  ///     "user": "",
  ///     "delegated_user": "",
  ///     "session_id": "6242f69b02e4d609:ac84df1fbb0e16a3",
  ///     "network_address": "127.0.0.1:46484",
  ///     "default_database": "default",
  ///     "start_time": "2014-08-07 22:50:49",
  ///     "last_accessed": "2014-08-07 22:50:49",
  ///     "expired": false,
  ///     "closed": false,
  ///     "ref_count": 0
  ///     }
  /// ],
  /// "num_sessions": 1
  void SessionsHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Returns a list of all known databases and tables
  void CatalogHandler(const Webserver::WebRequest& req, rapidjson::Document* output);

  /// Returns information on objects in the catalog.
  void CatalogObjectsHandler(const Webserver::WebRequest& req,
      rapidjson::Document* output);

  // Returns memory usage for queries in flight.
  void QueryMemoryHandler(const Webserver::WebRequest& req,
      rapidjson::Document* output);

  /// Utility method to print progress something as n/m(xx%).
  std::string ProgressToString(int64_t num_completed, int64_t total);

  /// Helper method to render a single QueryStateRecord as a Json object Used by
  /// QueryStateHandler().
  void QueryStateToJson(const QueryStateRecord& record,
      rapidjson::Value* value, rapidjson::Document* document, bool inflight);

  /// Json callback for /backends, which prints a table of known backends.
  /// "backends" : [
  /// {
  ///   "address": "localhost:21000",
  ///   "is_coordinator": true,
  ///   "is_executor": false
  ///   }
  /// ]
  void BackendsHandler(const Webserver::WebRequest& req, rapidjson::Document* document);

  /// Json callback for /admission_controller, which prints relevant details for all
  /// resource pools.
  ///"resource_pools": [
  ///  {
  ///    "pool_name": "default-pool",
  ///    "agg_num_running": 1,
  ///    "agg_num_queued": 4,
  ///    "agg_mem_reserved": 10382760,
  ///    "local_mem_admitted": 10382760,
  ///    "local_num_admitted_running": 1,
  ///    "local_num_queued": 4,
  ///    "local_backend_mem_reserved": 10382760,
  ///    "local_backend_mem_usage": 16384,
  ///    "pool_max_mem_resources": 10485760,
  ///    "pool_max_requests": 10,
  ///    "pool_max_queued": 10,
  ///    "pool_queue_timeout": 60000,
  ///    "max_query_mem_limit": 0,
  ///    "min_query_mem_limit": 0,
  ///    "clamp_mem_limit_query_option": true,
  ///    "wait_time_ms_EMA": 325.4,
  ///    "histogram": [
  ///      [
  ///        0,
  ///        3
  ///      ],
  ///      .
  ///      .
  ///      [
  ///        127,
  ///        1
  ///      ]
  ///    ],
  ///    "queued_queries": [
  ///      {
  ///        "query_id": "6f49e509bfa5b347:207d8ef900000000",
  ///        "mem_limit": 10382760,
  ///        "mem_limit_to_admit": 10382760,
  ///        "num_backends": 1
  ///      }
  ///    ],
  ///    "head_queued_reason": "<...>",
  ///    "running_queries": [
  ///      {
  ///        "query_id": "b94cf355d6df041c:ba3b91400000000",
  ///        "mem_limit": 10382760,
  ///        "mem_limit_to_admit": 10382760,
  ///        "num_backends": 1
  ///      }
  ///    ]
  ///  }
  ///]
  void AdmissionStateHandler(
      const Webserver::WebRequest& req, rapidjson::Document* document);

  /// Resets resource pool informational statistics. Takes an optional argument:
  /// 'pool_name'. If its not specified, all resource pool's informational statistics are
  /// reset otherwise it resets the statistics for a single pool identified by the
  /// supplied argument. Produces no JSON output.
  void ResetResourcePoolStatsHandler(
      const Webserver::WebRequest& req, rapidjson::Document* document);

  /// Fill the sessions information into the document.
  void FillSessionsInfo(rapidjson::Document* document);

  /// Fill the hs2 users information into the document.
  void FillUsersInfo(rapidjson::Document* document);

  /// Fill the client hosts information into the document.
  void FillClientHostsInfo(rapidjson::Document* document,
      const ThriftServer::ConnectionContextList& connection_contexts);

  /// Fill the connections information into the document.
  void FillConnectionsInfo(rapidjson::Document* document,
      const ThriftServer::ConnectionContextList& connection_contexts);
};
}
