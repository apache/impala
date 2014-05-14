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

#include "service/impala-server.h"

#include <sstream>
#include <boost/thread/mutex.hpp>
#include <gutil/strings/substitute.h>

#include "catalog/catalog-util.h"
#include "service/query-exec-state.h"
#include "util/webserver.h"

#include "gen-cpp/beeswax_types.h"
#include "thrift/protocol/TDebugProtocol.h"
#include "util/url-coding.h"

using namespace apache::thrift;
using namespace boost;
using namespace std;
using namespace impala;
using namespace beeswax;
using namespace strings;

DECLARE_int32(query_log_size);

void ImpalaServer::RegisterWebserverCallbacks(Webserver* webserver) {
  DCHECK(webserver != NULL);

  Webserver::PathHandlerCallback varz_callback =
      bind<void>(mem_fn(&ImpalaServer::RenderHadoopConfigs), this, _1, _2);
  webserver->RegisterPathHandler("/varz", varz_callback);

  Webserver::PathHandlerCallback query_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryStatePathHandler), this, _1, _2);
  webserver->RegisterPathHandler("/queries", query_callback);

  Webserver::PathHandlerCallback sessions_callback =
      bind<void>(mem_fn(&ImpalaServer::SessionPathHandler), this, _1, _2);
  webserver->RegisterPathHandler("/sessions", sessions_callback);

  Webserver::PathHandlerCallback catalog_callback =
      bind<void>(mem_fn(&ImpalaServer::CatalogPathHandler), this, _1, _2);
  webserver->RegisterPathHandler("/catalog", catalog_callback);

  Webserver::PathHandlerCallback catalog_objects_callback =
      bind<void>(mem_fn(&ImpalaServer::CatalogObjectsPathHandler), this, _1, _2);
  webserver->RegisterPathHandler("/catalog_objects", catalog_objects_callback, false,
      false);

  Webserver::PathHandlerCallback profile_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryProfilePathHandler), this, _1, _2);
  webserver-> RegisterPathHandler("/query_profile", profile_callback, true, false);

  Webserver::PathHandlerCallback cancel_callback =
      bind<void>(mem_fn(&ImpalaServer::CancelQueryPathHandler), this, _1, _2);
  webserver-> RegisterPathHandler("/cancel_query", cancel_callback, true, false);

  Webserver::PathHandlerCallback profile_encoded_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryProfileEncodedPathHandler), this, _1, _2);
  webserver->RegisterPathHandler("/query_profile_encoded", profile_encoded_callback,
      false, false);

  Webserver::PathHandlerCallback inflight_query_ids_callback =
      bind<void>(mem_fn(&ImpalaServer::InflightQueryIdsPathHandler), this, _1, _2);
  webserver->RegisterPathHandler("/inflight_query_ids", inflight_query_ids_callback,
      false, false);
}

void ImpalaServer::RenderHadoopConfigs(const Webserver::ArgumentMap& args,
    stringstream* output) {
  exec_env_->frontend()->RenderHadoopConfigs(args.find("raw") != args.end(), output);
}

// We expect the query id to be passed as one parameter, 'query_id'.
// Returns true if the query id was present and valid; false otherwise.
static bool ParseQueryId(const Webserver::ArgumentMap& args, TUniqueId* id) {
  Webserver::ArgumentMap::const_iterator it = args.find("query_id");
  if (it == args.end()) {
    return false;
  } else {
    return ParseId(it->second, id);
  }
}

void ImpalaServer::CancelQueryPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  TUniqueId unique_id;
  if (!ParseQueryId(args, &unique_id)) {
    (*output) << "Invalid query id";
    return;
  }
  Status status("Cancelled from Impala's debug web interface");
  if (UnregisterQuery(unique_id, &status)) {
    (*output) << "Query cancellation successful";
  } else {
    (*output) << "Error cancelling query: " << unique_id << " not found";
  }
}

void ImpalaServer::QueryProfilePathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  TUniqueId unique_id;
  if (!ParseQueryId(args, &unique_id)) {
    (*output) << "Invalid query id";
    return;
  }

  if (args.find("raw") == args.end()) {
    (*output) << "<pre>";
    stringstream ss;
    Status status = GetRuntimeProfileStr(unique_id, false, &ss);
    if (!status.ok()) {
      (*output) << status.GetErrorMsg();
    } else {
      EscapeForHtml(ss.str(), output);
    }
    (*output) << "</pre>";
  } else {
    Status status = GetRuntimeProfileStr(unique_id, false, output);
    if (!status.ok()) {
      (*output) << status.GetErrorMsg();
    }
  }
}

void ImpalaServer::QueryProfileEncodedPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  TUniqueId unique_id;
  if (!ParseQueryId(args, &unique_id)) {
    (*output) << "Invalid query id";
    return;
  }

  Status status = GetRuntimeProfileStr(unique_id, true, output);
  if (!status.ok()) (*output) << status.GetErrorMsg();
}

void ImpalaServer::InflightQueryIdsPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  lock_guard<mutex> l(query_exec_state_map_lock_);
  BOOST_FOREACH(const QueryExecStateMap::value_type& exec_state, query_exec_state_map_) {
    *output << exec_state.second->query_id() << "\n";
  }
}

void ImpalaServer::RenderSingleQueryTableRow(const ImpalaServer::QueryStateRecord& record,
    bool render_end_time, bool render_cancel, stringstream* output) {
  (*output) << "<tr>"
            << "<td>" << record.user << "</td>"
            << "<td>" << record.default_db << "</td>"
            << "<td>" << record.stmt << "</td>"
            << "<td>"
            << _TStmtType_VALUES_TO_NAMES.find(record.stmt_type)->second
            << "</td>";

  // Output start/end times
  (*output) << "<td>" << record.start_time.DebugString() << "</td>";
  if (render_end_time) {
    (*output) << "<td>" << record.end_time.DebugString() << "</td>";
  }

  // Output progress
  (*output) << "<td>";
  if (record.has_coord == false) {
    (*output) << "N/A";
  } else {
    (*output) << record.num_complete_fragments << " / " << record.total_fragments
              << " (" << setw(4);
    if (record.total_fragments == 0) {
      (*output) << " (0%)";
    } else {
      (*output) <<
          (100.0 * record.num_complete_fragments / (1.f * record.total_fragments))
                << "%)";
    }
  }

  // Output state and rows fetched
  (*output) << "</td>"
            << "<td>" << _QueryState_VALUES_TO_NAMES.find(record.query_state)->second
            << "</td><td>" << record.num_rows_fetched << "</td>";

  // Output profile
  (*output) << "<td><a href='/query_profile?query_id=" << record.id
            << "'>Profile</a></td>";
  if (render_cancel) {
    (*output) << "<td><a href='/cancel_query?query_id=" << record.id
              << "'>Cancel</a></td>";
  }
  (*output) << "</tr>" << endl;
}

void ImpalaServer::QueryStatePathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  set<QueryStateRecord, QueryStateRecord> sorted_query_records;
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);
    (*output) << "<h2>Queries</h2>";
    (*output) << "This page lists all running queries, plus any completed queries that "
              << "are archived in memory. The size of that archive is controlled with the"
              << " --query_log_size command line parameter.";
    (*output) << "<h3>" << query_exec_state_map_.size() << " queries in flight</h3>"
              << endl;
    (*output) << "<table class='table table-hover table-border'><tr>"
              << "<th>User</th>" << endl
              << "<th>Default Db</th>" << endl
              << "<th>Statement</th>" << endl
              << "<th>Query Type</th>" << endl
              << "<th>Start Time</th>" << endl
              << "<th>Backend Progress</th>" << endl
              << "<th>State</th>" << endl
              << "<th># rows fetched</th>" << endl
              << "<th>Profile</th>" << endl
              << "<th>Action</th>" << endl
              << "</tr>";
    BOOST_FOREACH(
        const QueryExecStateMap::value_type& exec_state, query_exec_state_map_) {
      // TODO: Do this in the browser so that sorts on other keys are possible.
      sorted_query_records.insert(QueryStateRecord(*exec_state.second));
    }
  }

  BOOST_FOREACH(const QueryStateRecord& record, sorted_query_records) {
    RenderSingleQueryTableRow(record, false, true, output);
  }

  (*output) << "</table>";

  // Print the query location counts.
  (*output) << "<h3>Query Locations</h3>";
  (*output) << "<table class='table table-hover table-bordered'>";
  (*output) << "<tr><th>Location</th><th>Number of Fragments</th></tr>" << endl;
  {
    lock_guard<mutex> l(query_locations_lock_);
    BOOST_FOREACH(const QueryLocations::value_type& location, query_locations_) {
      (*output) << "<tr><td>" << location.first << "<td><b>" << location.second.size()
                << "</b></td></tr>";
    }
  }
  (*output) << "</table>";

  // Print the query log
  if (FLAGS_query_log_size == 0) {
    (*output) << "<h3>No queries archived in memory (--query_log_size is 0)</h3>";
    return;
  }

  if (FLAGS_query_log_size > 0) {
    (*output) << "<h3>Last " << FLAGS_query_log_size << " Completed Queries</h3>";
  } else {
    (*output) << "<h3>All Completed Queries</h3>";
  }
  (*output) << "<table class='table table-hover table-border'><tr>"
            << "<th>User</th>" << endl
            << "<th>Default Db</th>" << endl
            << "<th>Statement</th>" << endl
            << "<th>Query Type</th>" << endl
            << "<th>Start Time</th>" << endl
            << "<th>End Time</th>" << endl
            << "<th>Backend Progress</th>" << endl
            << "<th>State</th>" << endl
            << "<th># rows fetched</th>" << endl
            << "<th>Profile</th>" << endl
            << "</tr>";

  {
    lock_guard<mutex> l(query_log_lock_);
    BOOST_FOREACH(const QueryStateRecord& log_entry, query_log_) {
      RenderSingleQueryTableRow(log_entry, true, false, output);
    }
  }

  (*output) << "</table>";
}

void ImpalaServer::SessionPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  (*output) << "<h2>Sessions</h2>" << endl;
  lock_guard<mutex> l(session_state_map_lock_);
  (*output) << "There are " << session_state_map_.size() << " active sessions." << endl
            << "<table class='table table-bordered table-hover'>"
            << "<tr><th>Session Type</th>"
            << "<th>Open Queries</th>"
            << "<th>User</th>"
            << "<th>Session ID</th>"
            << "<th>Network Address</th>"
            << "<th>Default Database</th>"
            << "<th>Start Time</th>"
            << "<th>Last Accessed</th>"
            << "<th>Expired</th>"
            << "<th>Closed</th>"
            << "<th>Ref count</th>"
            <<"</tr>"
            << endl;
  (*output) << boolalpha;
  BOOST_FOREACH(const SessionStateMap::value_type& session, session_state_map_) {
    (*output) << "<tr>"
              << "<td>" << PrintTSessionType(session.second->session_type) << "</td>"
              << "<td>" << session.second->inflight_queries.size() << "</td>"
              << "<td>" << session.second->connected_user << "</td>"
              << "<td>" << session.first << "</td>"
              << "<td>" << session.second->network_address << "</td>"
              << "<td>" << session.second->database << "</td>"
              << "<td>" << session.second->start_time.DebugString() << "</td>"
              << "<td>" << TimestampValue(
                  session.second->last_accessed_ms / 1000).DebugString() << "</td>"
              << "<td>" << session.second->expired << "</td>"
              << "<td>" << session.second->closed << "</td>"
              << "<td>" << session.second->ref_count << "</td>"
              << "</tr>";
  }
  (*output) << "</table>";
}

void ImpalaServer::CatalogPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  // TODO: This is an almost exact copy of CatalogServer::CatalogPathHandler(). Merge the
  // two and deal with the different ways to get tables and databases.
  TGetDbsResult get_dbs_result;
  Status status = exec_env_->frontend()->GetDbNames(NULL, NULL, &get_dbs_result);
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
      Status status = exec_env_->frontend()->
          GetTableNames(db, NULL, NULL, &get_table_results);
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
      Status status = exec_env_->frontend()->
          GetTableNames(db, NULL, NULL, &get_table_results);
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

void ImpalaServer::CatalogObjectsPathHandler(const Webserver::ArgumentMap& args,
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
    Status status = exec_env_->frontend()->GetCatalogObject(request, &result);
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
