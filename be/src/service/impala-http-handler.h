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

#ifndef IMPALA_SERVICE_IMPALA_HTTP_HANDLER_H
#define IMPALA_SERVICE_IMPALA_HTTP_HANDLER_H

#include <rapidjson/document.h>
#include "util/webserver.h"

#include "service/impala-server.h"

namespace impala {

/// Handles all webserver callbacks for an ImpalaServer. This class is a friend of
/// ImpalaServer in order to access the internal state needed to generate the debug
/// webpages.
class ImpalaHttpHandler {
 public:
  ImpalaHttpHandler(ImpalaServer* server) : server_(server) { }

  /// Registers all the per-Impalad webserver callbacks
  void RegisterHandlers(Webserver* webserver);

 private:
  ImpalaServer* server_;

  /// Json callback for /hadoop-varz. Produces Json with a list, 'configs', of (key,
  /// value) pairs, one for each Hadoop configuration value.
  void HadoopVarzHandler(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

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
  void QueryStateHandler(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Json callback for /query_profile. Expects query_id as an argument, produces Json
  /// with 'profile' set to the profile string, and 'query_id' set to the query ID.
  void QueryProfileHandler(const Webserver::ArgumentMap& args,
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
      const Webserver::ArgumentMap& args, rapidjson::Document* document);

  /// If 'args' contains a query id, serializes all backend states for that query to
  /// 'document'.
  void QueryBackendsHandler(
      const Webserver::ArgumentMap& args, rapidjson::Document* document);

  /// Cancels an in-flight query and writes the result to 'contents'.
  void CancelQueryHandler(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Closes an active session with a client.
  void CloseSessionHandler(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Upon return, 'document' will contain the query profile as a base64 encoded object in
  /// 'contents'.
  void QueryProfileEncodedHandler(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Produces a list of inflight query IDs printed as text in 'contents'.
  void InflightQueryIdsHandler(const Webserver::ArgumentMap& args,
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
  void SessionsHandler(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Returns a list of all known databases and tables
  void CatalogHandler(const Webserver::ArgumentMap& args, rapidjson::Document* output);

  /// Returns information on objects in the catalog.
  void CatalogObjectsHandler(const Webserver::ArgumentMap& args,
      rapidjson::Document* output);

  // Returns memory usage for queries in flight.
  void QueryMemoryHandler(const Webserver::ArgumentMap& args,
      rapidjson::Document* output);

  /// Helper method to render a single QueryStateRecord as a Json object Used by
  /// QueryStateHandler().
  void QueryStateToJson(const ImpalaServer::QueryStateRecord& record,
      rapidjson::Value* value, rapidjson::Document* document);

  /// Json callback for /backends, which prints a table of known backends.
  /// "backends" : [
  /// {
  ///   "address": "localhost:21000",
  ///   "is_coordinator": true,
  ///   "is_executor": false
  ///   }
  /// ]
  void BackendsHandler(const Webserver::ArgumentMap& args, rapidjson::Document* document);
};

}

#endif
