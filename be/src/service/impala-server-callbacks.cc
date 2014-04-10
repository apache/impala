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
using namespace rapidjson;

DECLARE_int32(query_log_size);

void ImpalaServer::RegisterWebserverCallbacks(Webserver* webserver) {
  DCHECK(webserver != NULL);

  Webserver::HtmlUrlCallback varz_callback =
      bind<void>(mem_fn(&ImpalaServer::RenderHadoopConfigs), this, _1, _2);
  webserver->RegisterHtmlUrlCallback("/hadoop-varz", varz_callback);

  Webserver::HtmlUrlCallback query_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryStateUrlCallback), this, _1, _2);
  webserver->RegisterHtmlUrlCallback("/queries", query_callback);

  Webserver::HtmlUrlCallback sessions_callback =
      bind<void>(mem_fn(&ImpalaServer::SessionUrlCallback), this, _1, _2);
  webserver->RegisterHtmlUrlCallback("/sessions", sessions_callback);

  Webserver::JsonUrlCallback catalog_callback =
      bind<void>(mem_fn(&ImpalaServer::CatalogUrlCallback), this, _1, _2);
  webserver->RegisterJsonUrlCallback("/catalog", "catalog.tmpl",
      catalog_callback);

  Webserver::JsonUrlCallback catalog_objects_callback =
      bind<void>(mem_fn(&ImpalaServer::CatalogObjectsUrlCallback), this, _1, _2);
  webserver->RegisterJsonUrlCallback("/catalog_object", "catalog_object.tmpl",
      catalog_objects_callback, true, false);

  Webserver::HtmlUrlCallback profile_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryProfileUrlCallback), this, _1, _2);
  webserver-> RegisterHtmlUrlCallback("/query_profile", profile_callback, true, false);

  Webserver::HtmlUrlCallback cancel_callback =
      bind<void>(mem_fn(&ImpalaServer::CancelQueryUrlCallback), this, _1, _2);
  webserver-> RegisterHtmlUrlCallback("/cancel_query", cancel_callback, true, false);

  Webserver::HtmlUrlCallback profile_encoded_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryProfileEncodedUrlCallback), this, _1, _2);
  webserver->RegisterHtmlUrlCallback("/query_profile_encoded", profile_encoded_callback,
      false, false);

  Webserver::HtmlUrlCallback inflight_query_ids_callback =
      bind<void>(mem_fn(&ImpalaServer::InflightQueryIdsUrlCallback), this, _1, _2);
  webserver->RegisterHtmlUrlCallback("/inflight_query_ids", inflight_query_ids_callback,
      false, false);
}

void ImpalaServer::RenderHadoopConfigs(const Webserver::ArgumentMap& args,
    stringstream* output) {
  exec_env_->frontend()->RenderHadoopConfigs(args.find("raw") == args.end(), output);
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

void ImpalaServer::CancelQueryUrlCallback(const Webserver::ArgumentMap& args,
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

void ImpalaServer::QueryProfileUrlCallback(const Webserver::ArgumentMap& args,
    stringstream* output) {
  TUniqueId unique_id;
  if (!ParseQueryId(args, &unique_id)) {
    (*output) << "Invalid query id";
    return;
  }

  (*output) << "<pre>";
  stringstream ss;
  Status status = GetRuntimeProfileStr(unique_id, false, &ss);
  if (!status.ok()) {
    (*output) << status.GetErrorMsg();
  } else {
    EscapeForHtml(ss.str(), output);
  }
  (*output) << "</pre>";
}

void ImpalaServer::QueryProfileEncodedUrlCallback(const Webserver::ArgumentMap& args,
    stringstream* output) {
  TUniqueId unique_id;
  if (!ParseQueryId(args, &unique_id)) {
    (*output) << "Invalid query id";
    return;
  }

  Status status = GetRuntimeProfileStr(unique_id, true, output);
  if (!status.ok()) (*output) << status.GetErrorMsg();
}

void ImpalaServer::InflightQueryIdsUrlCallback(const Webserver::ArgumentMap& args,
    stringstream* output) {
  lock_guard<mutex> l(query_exec_state_map_lock_);
  BOOST_FOREACH(const QueryExecStateMap::value_type& exec_state, query_exec_state_map_) {
    *output << exec_state.second->query_id() << "\n";
  }
}

void ImpalaServer::RenderSingleQueryTableRow(const ImpalaServer::QueryStateRecord& record,
    bool render_end_time, bool render_cancel, stringstream* output) {
  (*output) << "<tr>"
            << "<td>" << record.effective_user << "</td>"
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

void ImpalaServer::QueryStateUrlCallback(const Webserver::ArgumentMap& args,
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

void ImpalaServer::SessionUrlCallback(const Webserver::ArgumentMap& args,
    stringstream* output) {
  (*output) << "<h2>Sessions</h2>" << endl;
  lock_guard<mutex> l(session_state_map_lock_);
  (*output) << "There are " << session_state_map_.size() << " active sessions." << endl
            << "<table class='table table-bordered table-hover'>"
            << "<tr><th>Session Type</th>"
            << "<th>Open Queries</th>"
            << "<th>User</th>"
            << "<th>Delegated User</th>"
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
              << "<td>" << session.second->do_as_user << "</td>"
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

void ImpalaServer::CatalogUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  TGetDbsResult get_dbs_result;
  Status status = exec_env_->frontend()->GetDbNames(NULL, NULL, &get_dbs_result);
  if (!status.ok()) {
    Value error(status.GetErrorMsg().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  Value databases(kArrayType);
  BOOST_FOREACH(const string& db, get_dbs_result.dbs) {
    Value database(kObjectType);
    Value str(db.c_str(), document->GetAllocator());
    database.AddMember("name", str, document->GetAllocator());

    TGetTablesResult get_table_results;
    Status status =
        exec_env_->frontend()->GetTableNames(db, NULL, NULL, &get_table_results);
    if (!status.ok()) {
      Value error(status.GetErrorMsg().c_str(), document->GetAllocator());
      database.AddMember("error", error, document->GetAllocator());
      continue;
    }

    Value table_array(kArrayType);
    BOOST_FOREACH(const string& table, get_table_results.tables) {
      Value table_obj(kObjectType);
      Value fq_name(Substitute("$0.$1", db, table).c_str(), document->GetAllocator());
      table_obj.AddMember("fqtn", fq_name, document->GetAllocator());
      Value table_name(table.c_str(), document->GetAllocator());
      table_obj.AddMember("name", table_name, document->GetAllocator());
      table_array.PushBack(table_obj, document->GetAllocator());
    }
    database.AddMember("num_tables", table_array.Size(), document->GetAllocator());
    database.AddMember("tables", table_array, document->GetAllocator());
    databases.PushBack(database, document->GetAllocator());
  }
  document->AddMember("databases", databases, document->GetAllocator());
}

void ImpalaServer::CatalogObjectsUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
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
      Value debug_string(ThriftDebugString(result).c_str(), document->GetAllocator());
      document->AddMember("thrift_string", debug_string, document->GetAllocator());
    } else {
      Value error(status.GetErrorMsg().c_str(), document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
    }
  } else {
    Value error("Please specify values for the object_type and object_name parameters.",
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}
