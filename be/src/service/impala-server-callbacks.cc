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
#include "util/redactor.h"
#include "util/summary-util.h"
#include "util/url-coding.h"

#include "common/names.h"

using boost::adopt_lock_t;
using namespace apache::thrift;
using namespace beeswax;
using namespace impala;
using namespace rapidjson;
using namespace strings;

DECLARE_int32(query_log_size);

void ImpalaServer::RegisterWebserverCallbacks(Webserver* webserver) {
  DCHECK(webserver != NULL);

  Webserver::UrlCallback hadoop_varz_callback =
      bind<void>(mem_fn(&ImpalaServer::HadoopVarzUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback("/hadoop-varz", "hadoop-varz.tmpl",
      hadoop_varz_callback);

  Webserver::UrlCallback query_json_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryStateUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback("/queries", "queries.tmpl",
      query_json_callback);

  Webserver::UrlCallback sessions_json_callback =
      bind<void>(mem_fn(&ImpalaServer::SessionsUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback("/sessions", "sessions.tmpl",
      sessions_json_callback);

  Webserver::UrlCallback catalog_callback =
      bind<void>(mem_fn(&ImpalaServer::CatalogUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback("/catalog", "catalog.tmpl",
      catalog_callback);

  Webserver::UrlCallback catalog_objects_callback =
      bind<void>(mem_fn(&ImpalaServer::CatalogObjectsUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback("/catalog_object", "catalog_object.tmpl",
      catalog_objects_callback, false);

  Webserver::UrlCallback profile_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryProfileUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback("/query_profile", "query_profile.tmpl",
      profile_callback, false);

  Webserver::UrlCallback cancel_callback =
      bind<void>(mem_fn(&ImpalaServer::CancelQueryUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback("/cancel_query", "common-pre.tmpl", cancel_callback,
      false);

  Webserver::UrlCallback profile_encoded_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryProfileEncodedUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback("/query_profile_encoded", "raw_text.tmpl",
      profile_encoded_callback, false);

  Webserver::UrlCallback inflight_query_ids_callback =
      bind<void>(mem_fn(&ImpalaServer::InflightQueryIdsUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback("/inflight_query_ids", "raw_text.tmpl",
      inflight_query_ids_callback, false);

  Webserver::UrlCallback query_summary_callback =
      bind<void>(mem_fn(&ImpalaServer::QuerySummaryCallback), this, false, true, _1, _2);
  webserver->RegisterUrlCallback("/query_summary", "query_summary.tmpl",
      query_summary_callback, false);

  Webserver::UrlCallback query_plan_callback =
      bind<void>(mem_fn(&ImpalaServer::QuerySummaryCallback), this, true, true, _1, _2);
  webserver->RegisterUrlCallback("/query_plan", "query_plan.tmpl",
      query_plan_callback, false);

  Webserver::UrlCallback query_plan_text_callback =
      bind<void>(mem_fn(&ImpalaServer::QuerySummaryCallback), this, false, false, _1, _2);
  webserver->RegisterUrlCallback("/query_plan_text", "query_plan_text.tmpl",
      query_plan_text_callback, false);
  webserver->RegisterUrlCallback("/query_stmt", "query_stmt.tmpl",
      query_plan_text_callback, false);
}

void ImpalaServer::HadoopVarzUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  TGetAllHadoopConfigsResponse response;
  Status status  = exec_env_->frontend()->GetAllHadoopConfigs(&response);
  if (!status.ok()) return;

  Value configs(kArrayType);
  typedef map<string, string> ConfigMap;
  BOOST_FOREACH(const ConfigMap::value_type& config, response.configs) {
    Value key(config.first.c_str(), document->GetAllocator());
    Value value(config.second.c_str(), document->GetAllocator());
    Value config_json(kObjectType);
    config_json.AddMember("key", key, document->GetAllocator());
    config_json.AddMember("value", value, document->GetAllocator());
    configs.PushBack(config_json, document->GetAllocator());
  }
  document->AddMember("configs", configs, document->GetAllocator());
}

// We expect the query id to be passed as one parameter, 'query_id'.
// Returns true if the query id was present and valid; false otherwise.
static Status ParseQueryId(const Webserver::ArgumentMap& args, TUniqueId* id) {
  Webserver::ArgumentMap::const_iterator it = args.find("query_id");
  if (it == args.end()) {
    return Status("No 'query_id' argument found");
  } else {
    if (ParseId(it->second, id)) return Status::OK();
    return Status(Substitute("Could not parse 'query_id' argument: $0", it->second));
  }
}

void ImpalaServer::CancelQueryUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  TUniqueId unique_id;
  Status status = ParseQueryId(args, &unique_id);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  Status cause("Cancelled from Impala's debug web interface");
  status = UnregisterQuery(unique_id, true, &cause);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  Value message("Query cancellation successful", document->GetAllocator());
  document->AddMember("contents", message, document->GetAllocator());
}

void ImpalaServer::QueryProfileUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  TUniqueId unique_id;
  Status parse_status = ParseQueryId(args, &unique_id);
  if (!parse_status.ok()) {
    Value error(parse_status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  stringstream ss;
  Status status = GetRuntimeProfileStr(unique_id, false, &ss);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  Value profile(ss.str().c_str(), document->GetAllocator());
  document->AddMember("profile", profile, document->GetAllocator());
  document->AddMember("query_id", args.find("query_id")->second.c_str(),
      document->GetAllocator());
}

void ImpalaServer::QueryProfileEncodedUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  TUniqueId unique_id;
  stringstream ss;
  Status status = ParseQueryId(args, &unique_id);
  if (!status.ok()) {
    ss << status.GetDetail();
  } else {
    Status status = GetRuntimeProfileStr(unique_id, true, &ss);
    if (!status.ok()) {
      ss.str(Substitute("Could not obtain runtime profile: $0", status.GetDetail()));
    }
  }

  document->AddMember(Webserver::ENABLE_RAW_JSON_KEY, true, document->GetAllocator());
  Value profile(ss.str().c_str(), document->GetAllocator());
  document->AddMember("contents", profile, document->GetAllocator());
}

void ImpalaServer::InflightQueryIdsUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  lock_guard<mutex> l(query_exec_state_map_lock_);
  stringstream ss;
  BOOST_FOREACH(const QueryExecStateMap::value_type& exec_state, query_exec_state_map_) {
    ss << exec_state.second->query_id() << "\n";
  }
  document->AddMember(Webserver::ENABLE_RAW_JSON_KEY, true, document->GetAllocator());
  Value query_ids(ss.str().c_str(), document->GetAllocator());
  document->AddMember("contents", query_ids, document->GetAllocator());
}

void ImpalaServer::QueryStateToJson(const ImpalaServer::QueryStateRecord& record,
    Value* value, Document* document) {
  Value user(record.effective_user.c_str(), document->GetAllocator());
  value->AddMember("effective_user", user, document->GetAllocator());

  Value default_db(record.default_db.c_str(), document->GetAllocator());
  value->AddMember("default_db", default_db, document->GetAllocator());

  // Redact the query string
  Value stmt(RedactCopy(record.stmt).c_str(), document->GetAllocator());
  value->AddMember("stmt", stmt, document->GetAllocator());

  Value stmt_type(_TStmtType_VALUES_TO_NAMES.find(record.stmt_type)->second,
      document->GetAllocator());
  value->AddMember("stmt_type", stmt_type, document->GetAllocator());

  Value start_time(record.start_time.DebugString().c_str(), document->GetAllocator());
  value->AddMember("start_time", start_time, document->GetAllocator());

  Value end_time(record.end_time.DebugString().c_str(), document->GetAllocator());
  value->AddMember("end_time", end_time, document->GetAllocator());

  const TimestampValue& end_timestamp =
      record.end_time.HasDate() ? record.end_time : TimestampValue::LocalTime();
  double duration =
      end_timestamp.ToSubsecondUnixTime() - record.start_time.ToSubsecondUnixTime();
  const string& printed_duration = PrettyPrinter::Print(duration, TUnit::TIME_S);
  Value val_duration(printed_duration.c_str(), document->GetAllocator());
  value->AddMember("duration", val_duration, document->GetAllocator());

  string progress = "N/A";
  if (record.has_coord) {
    stringstream ss;
    ss << record.num_complete_fragments << " / " << record.total_fragments
       << " (" << setw(4);
    if (record.total_fragments == 0) {
      ss << "0%)";
    } else {
      ss << (100.0 * record.num_complete_fragments / (1.f * record.total_fragments))
         << "%)";
    }
    progress = ss.str();
  }
  Value progress_json(progress.c_str(), document->GetAllocator());
  value->AddMember("progress", progress_json, document->GetAllocator());

  Value state(_QueryState_VALUES_TO_NAMES.find(record.query_state)->second,
      document->GetAllocator());
  value->AddMember("state", state, document->GetAllocator());

  value->AddMember("rows_fetched", record.num_rows_fetched, document->GetAllocator());

  Value query_id(PrintId(record.id).c_str(), document->GetAllocator());
  value->AddMember("query_id", query_id, document->GetAllocator());

  if (record.event_sequence.labels.size() > 0) {
    Value last_event(record.event_sequence.labels.back().c_str(),
        document->GetAllocator());
    value->AddMember("last_event", last_event, document->GetAllocator());
  }
}

void ImpalaServer::QueryStateUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  set<QueryStateRecord, QueryStateRecord> sorted_query_records;
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);
    BOOST_FOREACH(
        const QueryExecStateMap::value_type& exec_state, query_exec_state_map_) {
      // TODO: Do this in the browser so that sorts on other keys are possible.
      sorted_query_records.insert(QueryStateRecord(*exec_state.second));
    }
  }

  Value in_flight_queries(kArrayType);
  BOOST_FOREACH(const QueryStateRecord& record, sorted_query_records) {
    Value record_json(kObjectType);
    QueryStateToJson(record, &record_json, document);
    in_flight_queries.PushBack(record_json, document->GetAllocator());
  }
  document->AddMember("in_flight_queries", in_flight_queries, document->GetAllocator());
  document->AddMember("num_in_flight_queries",
      static_cast<uint64_t>(sorted_query_records.size()),
      document->GetAllocator());

  Value completed_queries(kArrayType);
  {
    lock_guard<mutex> l(query_log_lock_);
    BOOST_FOREACH(const QueryStateRecord& log_entry, query_log_) {
      Value record_json(kObjectType);
      QueryStateToJson(log_entry, &record_json, document);
      completed_queries.PushBack(record_json, document->GetAllocator());
    }
  }
  document->AddMember("completed_queries", completed_queries, document->GetAllocator());
  document->AddMember("completed_log_size", FLAGS_query_log_size,
      document->GetAllocator());

  Value query_locations(kArrayType);
  {
    lock_guard<mutex> l(query_locations_lock_);
    BOOST_FOREACH(const QueryLocations::value_type& location, query_locations_) {
      Value location_json(kObjectType);
      Value location_name(lexical_cast<string>(location.first).c_str(),
          document->GetAllocator());
      location_json.AddMember("location", location_name, document->GetAllocator());
      location_json.AddMember("count", static_cast<uint64_t>(location.second.size()),
          document->GetAllocator());
      query_locations.PushBack(location_json, document->GetAllocator());
    }
  }
  document->AddMember("query_locations", query_locations, document->GetAllocator());
}


void ImpalaServer::SessionsUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  lock_guard<mutex> l(session_state_map_lock_);
  Value sessions(kArrayType);
  BOOST_FOREACH(const SessionStateMap::value_type& session, session_state_map_) {
    shared_ptr<SessionState> state = session.second;
    Value session_json(kObjectType);
    Value type(PrintTSessionType(state->session_type).c_str(),
        document->GetAllocator());
    session_json.AddMember("type", type, document->GetAllocator());

    session_json.AddMember("inflight_queries",
        static_cast<uint64_t>(state->inflight_queries.size()),
        document->GetAllocator());
    session_json.AddMember("total_queries", state->total_queries,
        document->GetAllocator());

    Value user(state->connected_user.c_str(), document->GetAllocator());
    session_json.AddMember("user", user, document->GetAllocator());

    Value delegated_user(state->do_as_user.c_str(), document->GetAllocator());
    session_json.AddMember("delegated_user", delegated_user, document->GetAllocator());

    Value session_id(PrintId(session.first).c_str(), document->GetAllocator());
    session_json.AddMember("session_id", session_id, document->GetAllocator());

    Value network_address(lexical_cast<string>(state->network_address).c_str(),
        document->GetAllocator());
    session_json.AddMember("network_address", network_address, document->GetAllocator());

    Value default_db(state->database.c_str(), document->GetAllocator());
    session_json.AddMember("default_database", default_db, document->GetAllocator());

    Value start_time(state->start_time.DebugString().c_str(), document->GetAllocator());
    session_json.AddMember("start_time", start_time, document->GetAllocator());

    Value last_accessed(
        TimestampValue(session.second->last_accessed_ms / 1000).DebugString().c_str(),
        document->GetAllocator());
    session_json.AddMember("last_accessed", last_accessed, document->GetAllocator());

    session_json.AddMember("session_timeout", state->session_timeout,
        document->GetAllocator());
    session_json.AddMember("expired", state->expired, document->GetAllocator());
    session_json.AddMember("closed", state->closed, document->GetAllocator());
    session_json.AddMember("ref_count", state->ref_count, document->GetAllocator());
    sessions.PushBack(session_json, document->GetAllocator());
  }

  document->AddMember("sessions", sessions, document->GetAllocator());
  document->AddMember("num_sessions", static_cast<uint64_t>(session_state_map_.size()),
      document->GetAllocator());
}

void ImpalaServer::CatalogUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  TGetDbsResult get_dbs_result;
  Status status = exec_env_->frontend()->GetDbs(NULL, NULL, &get_dbs_result);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  Value databases(kArrayType);
  BOOST_FOREACH(const TDatabase& db, get_dbs_result.dbs) {
    Value database(kObjectType);
    Value str(db.db_name.c_str(), document->GetAllocator());
    database.AddMember("name", str, document->GetAllocator());

    TGetTablesResult get_table_results;
    Status status =
        exec_env_->frontend()->GetTableNames(db.db_name, NULL, NULL, &get_table_results);
    if (!status.ok()) {
      Value error(status.GetDetail().c_str(), document->GetAllocator());
      database.AddMember("error", error, document->GetAllocator());
      continue;
    }

    Value table_array(kArrayType);
    BOOST_FOREACH(const string& table, get_table_results.tables) {
      Value table_obj(kObjectType);
      Value fq_name(Substitute("$0.$1", db.db_name, table).c_str(),
          document->GetAllocator());
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
      Value error(status.GetDetail().c_str(), document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
    }
  } else {
    Value error("Please specify values for the object_type and object_name parameters.",
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}

// Helper for PlanToJson(), processes a single list of plan nodes which are the
// DFS-flattened representation of a single plan fragment. Called recursively, the
// iterator parameter is updated in place so that when a recursive call returns, the
// caller is pointing at the next of its children.
void PlanToJsonHelper(const map<TPlanNodeId, TPlanNodeExecSummary>& summaries,
    const vector<TPlanNode>& nodes,
    vector<TPlanNode>::const_iterator* it, rapidjson::Document* document, Value* value) {
  Value children(kArrayType);
  value->AddMember("label", (*it)->label.c_str(), document->GetAllocator());
  // Node "details" may contain exprs which should be redacted.
  Value label_detail(RedactCopy((*it)->label_detail).c_str(), document->GetAllocator());
  value->AddMember("label_detail", label_detail, document->GetAllocator());

  TPlanNodeId id = (*it)->node_id;
  map<TPlanNodeId, TPlanNodeExecSummary>::const_iterator summary = summaries.find(id);
  if (summary != summaries.end()) {
    int64_t cardinality = 0;
    int64_t max_time = 0L;
    int64_t total_time = 0;
    BOOST_FOREACH(const TExecStats& stat, summary->second.exec_stats) {
      if (summary->second.is_broadcast) {
        // Avoid multiple-counting for recipients of broadcasts.
        cardinality = ::max(cardinality, stat.cardinality);
      } else {
        cardinality += stat.cardinality;
      }
      total_time += stat.latency_ns;
      max_time = ::max(max_time, stat.latency_ns);
    }
    value->AddMember("output_card", cardinality, document->GetAllocator());
    value->AddMember("num_instances",
        static_cast<uint64_t>(summary->second.exec_stats.size()),
        document->GetAllocator());
    if (summary->second.is_broadcast) {
      value->AddMember("is_broadcast", true, document->GetAllocator());
    }

    const string& max_time_str = PrettyPrinter::Print(max_time, TUnit::TIME_NS);
    Value max_time_str_json(max_time_str.c_str(), document->GetAllocator());
    value->AddMember("max_time", max_time_str_json, document->GetAllocator());
    value->AddMember("max_time_val", max_time, document->GetAllocator());

    // Round to the nearest ns, to workaround a bug in pretty-printing a fraction of a
    // ns. See IMPALA-1800.
    const string& avg_time_str = PrettyPrinter::Print(
        // A bug may occasionally cause 1-instance nodes to appear to have 0 instances.
        total_time / ::max(static_cast<int>(summary->second.exec_stats.size()), 1),
        TUnit::TIME_NS);
    Value avg_time_str_json(avg_time_str.c_str(), document->GetAllocator());
    value->AddMember("avg_time", avg_time_str_json, document->GetAllocator());
  }

  int num_children = (*it)->num_children;
  for (int i = 0; i < num_children; ++i) {
    ++(*it);
    Value container(kObjectType);
    PlanToJsonHelper(summaries, nodes, it, document, &container);
    children.PushBack(container, document->GetAllocator());
  }
  value->AddMember("children", children, document->GetAllocator());
}

// Helper method which converts a list of plan fragments into a single JSON document, with
// the following schema:
// "plan_nodes": [
//     {
//       "label": "12:AGGREGATE",
//       "label_detail": "FINALIZE",
//       "output_card": 23456,
//       "num_instances": 34,
//       "max_time": "1m23s",
//       "avg_time": "1.3ms",
//       "children": [
//           {
//             "label": "11:EXCHANGE",
//             "label_detail": "UNPARTITIONED",
//             "children": []
//           }
//       ]
//     },
//     {
//       "label": "07:AGGREGATE",
//       "label_detail": "",
//       "children": [],
//       "data_stream_target": "11:EXCHANGE"
//     }
// ]
void PlanToJson(const vector<TPlanFragment>& fragments, const TExecSummary& summary,
    rapidjson::Document* document, Value* value) {
  // Build a map from id to label so that we can resolve the targets of data-stream sinks
  // and connect plan fragments.
  map<TPlanNodeId, string> label_map;
  BOOST_FOREACH(const TPlanFragment& fragment, fragments) {
    BOOST_FOREACH(const TPlanNode& node, fragment.plan.nodes) {
      label_map[node.node_id] = node.label;
    }
  }

  map<TPlanNodeId, TPlanNodeExecSummary> exec_summaries;
  BOOST_FOREACH(const TPlanNodeExecSummary& s, summary.nodes) {
    exec_summaries[s.node_id] = s;
  }

  Value nodes(kArrayType);
  BOOST_FOREACH(const TPlanFragment& fragment, fragments) {
    Value plan_fragment(kObjectType);
    vector<TPlanNode>::const_iterator it = fragment.plan.nodes.begin();
    PlanToJsonHelper(exec_summaries, fragment.plan.nodes, &it, document, &plan_fragment);
    if (fragment.__isset.output_sink) {
      const TDataSink& sink = fragment.output_sink;
      if (sink.__isset.stream_sink) {
        plan_fragment.AddMember("data_stream_target",
            label_map[sink.stream_sink.dest_node_id].c_str(), document->GetAllocator());
      }
    }
    nodes.PushBack(plan_fragment, document->GetAllocator());
  }
  value->AddMember("plan_nodes", nodes, document->GetAllocator());
}

void ImpalaServer::QuerySummaryCallback(bool include_json_plan, bool include_summary,
    const Webserver::ArgumentMap& args, Document* document) {
  TUniqueId query_id;
  Status status = ParseQueryId(args, &query_id);
  if (!status.ok()) {
    // Redact the error message, it may contain part or all of the query.
    Value json_error(RedactCopy(status.GetDetail()).c_str(), document->GetAllocator());
    document->AddMember("error", json_error, document->GetAllocator());
    return;
  }

  TExecSummary summary;
  string stmt;
  string plan;
  Status query_status;
  bool found = false;
  vector<TPlanFragment> fragments;

  // Search the in-flight queries first, followed by the archived ones.
  {
    shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
    if (exec_state != NULL) {
      found = true;
      lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());
      if (exec_state->coord() == NULL) {
        const string& err = Substitute("Invalid query id: $0", PrintId(query_id));
        Value json_error(err.c_str(), document->GetAllocator());
        document->AddMember("error", json_error, document->GetAllocator());
        return;
      }
      query_status = exec_state->query_status();
      stmt = exec_state->sql_stmt();
      plan = exec_state->exec_request().query_exec_request.query_plan;
      if (include_json_plan || include_summary) {
        lock_guard<SpinLock> lock(exec_state->coord()->GetExecSummaryLock());
        summary = exec_state->coord()->exec_summary();
      }
      if (include_json_plan) {
        fragments = exec_state->exec_request().query_exec_request.fragments;
      }
    }
  }

  if (!found) {
    lock_guard<mutex> l(query_log_lock_);
    QueryLogIndex::const_iterator query_record = query_log_index_.find(query_id);
    if (query_record == query_log_index_.end()) {
      const string& err = Substitute("Unknown query id: $0", PrintId(query_id));
      Value json_error(err.c_str(), document->GetAllocator());
      document->AddMember("error", json_error, document->GetAllocator());
      return;
    }
    if (include_json_plan || include_summary) {
      summary = query_record->second->exec_summary;
    }
    stmt = query_record->second->stmt;
    plan = query_record->second->plan;
    query_status = query_record->second->query_status;
    if (include_json_plan) {
      fragments = query_record->second->fragments;
    }
  }

  if (include_json_plan) {
    Value v(kObjectType);
    PlanToJson(fragments, summary, document, &v);
    document->AddMember("plan_json", v, document->GetAllocator());
  }
  if (include_summary) {
    const string& printed_summary = PrintExecSummary(summary);
    Value json_summary(printed_summary.c_str(), document->GetAllocator());
    document->AddMember("summary", json_summary, document->GetAllocator());
  }
  Value json_stmt(RedactCopy(stmt).c_str(), document->GetAllocator());
  document->AddMember("stmt", json_stmt, document->GetAllocator());
  Value json_plan_text(RedactCopy(plan).c_str(), document->GetAllocator());
  document->AddMember("plan", json_plan_text, document->GetAllocator());

  // Redact the error in case the query is contained in the error message.
  Value json_status(query_status.ok() ? "OK" :
      RedactCopy(query_status.GetDetail()).c_str(), document->GetAllocator());
  document->AddMember("status", json_status, document->GetAllocator());
  Value json_id(PrintId(query_id).c_str(), document->GetAllocator());
  document->AddMember("query_id", json_id, document->GetAllocator());
}
