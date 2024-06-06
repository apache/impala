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

#include "service/impala-http-handler.h"

#include <algorithm>
#include <mutex>
#include <sstream>
#include <boost/unordered_set.hpp>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "catalog/catalog-util.h"
#include "gen-cpp/beeswax_types.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-driver.h"
#include "runtime/query-state.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "scheduling/admission-control-service.h"
#include "scheduling/admission-controller.h"
#include "scheduling/admissiond-env.h"
#include "scheduling/cluster-membership-mgr.h"
#include "service/client-request-state.h"
#include "service/frontend.h"
#include "service/impala-server.h"
#include "service/query-state-record.h"
#include "thrift/protocol/TDebugProtocol.h"
#include "util/debug-util.h"
#include "util/logging-support.h"
#include "util/pretty-printer.h"
#include "util/redactor.h"
#include "util/summary-util.h"
#include "util/time.h"
#include "util/uid-util.h"
#include "util/webserver.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift;
using namespace beeswax;
using namespace impala;
using namespace rapidjson;
using namespace strings;

DECLARE_int32(query_log_size);
DECLARE_int64(query_log_size_in_bytes);
DECLARE_int32(query_stmt_size);
DECLARE_bool(use_local_catalog);
DECLARE_string(admission_service_host);

namespace {

// Helper method to turn a class + a method to invoke into a UrlCallback
template<typename T, class F>
Webserver::UrlCallback MakeCallback(T* caller, const F& fnc) {
  return [caller, fnc](const auto& req, auto* doc) {
    (caller->*fnc)(req, doc);
  };
}

// We expect the id to be passed as one parameter. Eg: 'query_id' or 'session_id'.
// Returns true if the id was present and valid; false otherwise.
static Status ParseIdFromRequest(const Webserver::WebRequest& req, TUniqueId* id,
    const std::string &to_find) {
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator it = args.find(to_find);
  if (it == args.end()) {
    return Status(Substitute("No '$0' argument found.", to_find));
  } else {
    if (ParseId(it->second, id)) return Status::OK();
    return Status(Substitute("Could not parse '$0' argument: $1", to_find, it->second));
  }
}

}

ImpalaHttpHandler::ImpalaHttpHandler(ImpalaServer* server,
    AdmissionController* admission_controller,
    ClusterMembershipMgr* cluster_membership_mgr, bool is_admissiond)
  : server_(server),
    admission_controller_(admission_controller),
    cluster_membership_mgr_(cluster_membership_mgr),
    is_admissiond_(is_admissiond) {}

void ImpalaHttpHandler::RegisterHandlers(Webserver* webserver, bool metrics_only) {
  DCHECK(webserver != NULL);

  Webserver::RawUrlCallback healthz_callback =
    [this](const auto& req, auto* data, auto* response) {
      return this->HealthzHandler(req, data, response);
    };
  webserver->RegisterUrlCallback("/healthz", healthz_callback);

  if (metrics_only) return;

  if (is_admissiond_) {
    // The admissiond only exposes a subset of endpoints that have info relevant to
    // admission control.
    webserver->RegisterUrlCallback("/backends", "backends.tmpl",
        MakeCallback(this, &ImpalaHttpHandler::BackendsHandler), true);

    webserver->RegisterUrlCallback("/admission", "admission_controller.tmpl",
        MakeCallback(this, &ImpalaHttpHandler::AdmissionStateHandler), true);

    webserver->RegisterUrlCallback("/resource_pool_reset", "",
        MakeCallback(this, &ImpalaHttpHandler::ResetResourcePoolStatsHandler), false);
    return;
  }

  webserver->RegisterUrlCallback("/backends", "backends.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::BackendsHandler), true);

  webserver->RegisterUrlCallback("/hadoop-varz", "hadoop-varz.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::HadoopVarzHandler), true);

  webserver->RegisterUrlCallback("/queries", "queries.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::QueryStateHandler), true);

  webserver->RegisterUrlCallback("/sessions", "sessions.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::SessionsHandler), true);

  webserver->RegisterUrlCallback("/catalog", "catalog.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::CatalogHandler), true);

  if(!FLAGS_use_local_catalog) {
      // The /catalog_object endpoint is disabled if local_catalog_mode is used
      // since metadata is partially fetched on demand
      webserver->RegisterUrlCallback("/catalog_object", "catalog_object.tmpl",
          MakeCallback(this, &ImpalaHttpHandler::CatalogObjectsHandler), false);
    }

  webserver->RegisterUrlCallback("/query_profile", "query_profile.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::QueryProfileHandler), false);

  webserver->RegisterUrlCallback("/query_memory", "query_memory.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::QueryMemoryHandler), false);

  webserver->RegisterUrlCallback("/query_backends", "query_backends.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::QueryBackendsHandler), false);

  webserver->RegisterUrlCallback("/query_finstances", "query_finstances.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::QueryFInstancesHandler), false);

  webserver->RegisterUrlCallback("/cancel_query", "common-pre.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::CancelQueryHandler), false);

  webserver->RegisterUrlCallback("/close_session", "common-pre.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::CloseSessionHandler), false);

  webserver->RegisterUrlCallback("/query_profile_encoded", "raw_text.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::QueryProfileEncodedHandler), false);

  webserver->RegisterUrlCallback("/query_profile_plain_text", "raw_text.tmpl",
        MakeCallback(this, &ImpalaHttpHandler::QueryProfileTextHandler), false);

  webserver->RegisterUrlCallback("/query_profile_json", "raw_text.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::QueryProfileJsonHandler), false);

  webserver->RegisterUrlCallback("/inflight_query_ids", "raw_text.tmpl",
      MakeCallback(this, &ImpalaHttpHandler::InflightQueryIdsHandler), false);

  webserver->RegisterUrlCallback("/query_summary", "query_summary.tmpl",
      [this](const auto& req, auto* doc) {
        this->QuerySummaryHandler(false, true, req, doc); }, false);

  webserver->RegisterUrlCallback("/query_plan", "query_plan.tmpl",
      [this](const auto& req, auto* doc) {
        this->QuerySummaryHandler(true, true, req, doc); }, false);

  webserver->RegisterUrlCallback("/query_timeline", "query_timeline.tmpl",
      [this](const auto& req, auto* doc) {
        this->QueryProfileHelper(req, doc, TRuntimeProfileFormat::JSON, true);
        }, false);

  webserver->RegisterUrlCallback("/query_plan_text", "query_plan_text.tmpl",
      [this](const auto& req, auto* doc) {
        this->QuerySummaryHandler(false, false, req, doc); }, false);

  webserver->RegisterUrlCallback("/query_stmt", "query_stmt.tmpl",
      [this](const auto& req, auto* doc) {
        this->QuerySummaryHandler(false, false, req, doc); }, false);

  // Only enable the admission control endpoints for impalads if the admission service is
  // not enabled, otherwise these will be exposed on the admissiond.
  if (!ExecEnv::GetInstance()->AdmissionServiceEnabled()) {
    webserver->RegisterUrlCallback("/admission", "admission_controller.tmpl",
        MakeCallback(this, &ImpalaHttpHandler::AdmissionStateHandler), true);

    webserver->RegisterUrlCallback("/resource_pool_reset", "",
        MakeCallback(this, &ImpalaHttpHandler::ResetResourcePoolStatsHandler), false);
  }

  RegisterLogLevelCallbacks(webserver, true);
}

void ImpalaHttpHandler::HealthzHandler(const Webserver::WebRequest& req,
    std::stringstream* data, HttpStatusCode* response) {
  if ((server_ != nullptr && server_->IsHealthy()) ||
      (is_admissiond_ &&
       AdmissiondEnv::GetInstance()->admission_control_service()->IsHealthy())) {
    (*data) << "OK";
    *response = HttpStatusCode::Ok;
    return;
  }
  *(data) << "Not Available";
  *response = HttpStatusCode::ServiceUnavailable;
}

void ImpalaHttpHandler::HadoopVarzHandler(const Webserver::WebRequest& req,
    Document* document) {
  TGetAllHadoopConfigsResponse response;
  Status status  = server_->exec_env_->frontend()->GetAllHadoopConfigs(&response);
  if (!status.ok()) {
    LOG(ERROR) << "Error getting cluster configuration for hadoop-varz: "
               << status.GetDetail();
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  Value configs(kArrayType);
  typedef map<string, string> ConfigMap;
  for (const auto& config: response.configs) {
    Value key(config.first.c_str(), document->GetAllocator());
    Value value(config.second.c_str(), document->GetAllocator());
    Value config_json(kObjectType);
    config_json.AddMember("key", key, document->GetAllocator());
    config_json.AddMember("value", value, document->GetAllocator());
    configs.PushBack(config_json, document->GetAllocator());
  }
  document->AddMember("configs", configs, document->GetAllocator());
}

void ImpalaHttpHandler::CancelQueryHandler(const Webserver::WebRequest& req,
    Document* document) {
  TUniqueId unique_id;
  Status status = ParseIdFromRequest(req, &unique_id, "query_id");
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  Status cause(Substitute("Cancelled from Impala's debug web interface by user:"
                          " '$0' at $1", req.source_user, req.source_socket));
  // Web UI doesn't have access to secret so we can't validate it. We assume that
  // web UI is allowed to close queries.
  status = server_->UnregisterQuery(unique_id, true, &cause);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  Value message("Query cancellation successful", document->GetAllocator());
  document->AddMember("contents", message, document->GetAllocator());
}

void ImpalaHttpHandler::CloseSessionHandler(const Webserver::WebRequest& req,
    Document* document) {
  TUniqueId unique_id;
  Status status = ParseIdFromRequest(req, &unique_id, "session_id");
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  Status cause(Substitute("Session closed from Impala's debug web interface by user:"
                          " '$0' at $1", req.source_user, req.source_socket));
  // Web UI doesn't have access to secret so we can't validate it. We assume that
  // web UI is allowed to close sessions.
  status = server_->CloseSessionInternal(unique_id,
      ImpalaServer::SecretArg::SkipSecretCheck(), /* ignore_if_absent= */ false);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  stringstream ss;
  ss << "Session " << PrintId(unique_id) << " closed successfully";
  Value message(ss.str().c_str(), document->GetAllocator());
  document->AddMember("contents", message, document->GetAllocator());
}

void ImpalaHttpHandler::QueryProfileHandler(const Webserver::WebRequest& req,
    Document* document) {
  TUniqueId unique_id;
  Status parse_status = ParseIdFromRequest(req, &unique_id, "query_id");
  if (!parse_status.ok()) {
    Value error(parse_status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  Value query_id_val(PrintId(unique_id).c_str(), document->GetAllocator());
  document->AddMember("query_id", query_id_val, document->GetAllocator());

  ImpalaServer::RuntimeProfileOutput runtime_profile;
  stringstream ss;
  runtime_profile.string_output = &ss;
  Status status = server_->GetRuntimeProfileOutput(
      unique_id, "", TRuntimeProfileFormat::STRING, &runtime_profile);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  Value profile(ss.str().c_str(), document->GetAllocator());
  document->AddMember("profile", profile, document->GetAllocator());
}

void ImpalaHttpHandler::QueryProfileHelper(const Webserver::WebRequest& req,
    Document* document, TRuntimeProfileFormat::type format, bool internal_profile) {
  TUniqueId unique_id;
  stringstream ss;
  Status status = ParseIdFromRequest(req, &unique_id, "query_id");
  if (!status.ok()) {
    ss << status.GetDetail();
  } else {
    ImpalaServer::RuntimeProfileOutput runtime_profile;
    if (internal_profile) {
      Value query_id_val(PrintId(unique_id).c_str(), document->GetAllocator());
      document->AddMember("query_id", query_id_val, document->GetAllocator());
      document->AddMember("internal_profile", true, document->GetAllocator());
    }
    if (format != TRuntimeProfileFormat::JSON) {
      runtime_profile.string_output = &ss;
    }
    runtime_profile.json_output = document;
    status = server_->GetRuntimeProfileOutput(unique_id, "", format, &runtime_profile);
    if (!status.ok()) {
      ss.str(Substitute("Could not obtain runtime profile: $0", status.GetDetail()));
    }
  }
  // JSON format contents already been added inside document in GetRuntimeProfileOutput()
  if (format != TRuntimeProfileFormat::JSON){
    Value profile(ss.str().c_str(), document->GetAllocator());
    document->AddMember("contents", profile, document->GetAllocator());
  } else if (internal_profile) {
    if (!status.ok()) {
      Value error(ss.str().c_str(), document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
      return;
    }
    // Add OK Status like other handlers have. These status lines could be
    // eliminated if error was handled uniformly in all handlers.
    Value json_status("OK");
    document->AddMember("status", json_status, document->GetAllocator());
  }
}

void ImpalaHttpHandler::QueryProfileEncodedHandler(const Webserver::WebRequest& req,
    Document* document) {
  QueryProfileHelper(req, document, TRuntimeProfileFormat::BASE64);
  document->AddMember(rapidjson::StringRef(Webserver::ENABLE_RAW_HTML_KEY), true,
      document->GetAllocator());
}

void ImpalaHttpHandler::QueryProfileTextHandler(const Webserver::WebRequest& req,
    Document* document) {
  QueryProfileHelper(req, document, TRuntimeProfileFormat::STRING);
  document->AddMember(rapidjson::StringRef(Webserver::ENABLE_RAW_HTML_KEY), true,
      document->GetAllocator());
}

void ImpalaHttpHandler::QueryProfileJsonHandler(const Webserver::WebRequest& req,
    Document* document) {
  QueryProfileHelper(req, document, TRuntimeProfileFormat::JSON);
  document->AddMember(rapidjson::StringRef(Webserver::ENABLE_PLAIN_JSON_KEY), true,
      document->GetAllocator());
}

void ImpalaHttpHandler::InflightQueryIdsHandler(const Webserver::WebRequest& req,
    Document* document) {
  stringstream ss;
  server_->query_driver_map_.DoFuncForAllEntries(
      [&](const std::shared_ptr<QueryDriver>& query_driver) {
        ss << PrintId(query_driver->GetActiveClientRequestState()->query_id()) << "\n";
      });
  document->AddMember(rapidjson::StringRef(Webserver::ENABLE_RAW_HTML_KEY), true,
      document->GetAllocator());
  Value query_ids(ss.str().c_str(), document->GetAllocator());
  document->AddMember("contents", query_ids, document->GetAllocator());
}

void ImpalaHttpHandler::QueryMemoryHandler(const Webserver::WebRequest& req,
    Document* document) {
  TUniqueId unique_id;
  Status parse_status = ParseIdFromRequest(req, &unique_id, "query_id");
  if (!parse_status.ok()) {
    Value error(parse_status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  QueryState::ScopedRef qs(unique_id);
  string mem_usage_text;
  // Only queries that have started execution have a MemTracker to get usage from.
  if (qs.get() != nullptr) {
    mem_usage_text = qs->query_mem_tracker()->LogUsage(MemTracker::UNLIMITED_DEPTH);
  } else {
    mem_usage_text = "The query has either finished or has not started execution yet, "
                     "current memory consumption is not available.";
  }

  Value mem_usage(mem_usage_text.c_str(), document->GetAllocator());
  document->AddMember("mem_usage", mem_usage, document->GetAllocator());
  const auto& args = req.parsed_args;
  Value query_id(args.find("query_id")->second.c_str(), document->GetAllocator());
  document->AddMember("query_id", query_id, document->GetAllocator());
}

void ImpalaHttpHandler::AddQueryRecordTips(Document* document) {
  document->AddMember("tips_query_id", "Unique ID of the query, click ID to jump to the "
      "query details page to get more detailed information.", document->GetAllocator());

  document->AddMember("tips_action_cancel", "Cancel the query, abort the execution and "
      "release resources.", document->GetAllocator());

  document->AddMember("tips_action_close", "Close the query and release resources.",
      document->GetAllocator());

  document->AddMember("tips_user", "The user who submitted the query.",
      document->GetAllocator());

  document->AddMember("tips_default_db", "The default database for query.",
      document->GetAllocator());

  document->AddMember("tips_query_type", "The type of query.",
      document->GetAllocator());

  document->AddMember("tips_start_time", "The start time of the query, i.e. the time when"
      " the query was submitted.", document->GetAllocator());

  document->AddMember("tips_waiting_time", "The duration of the query in the waiting to "
      "close state.", document->GetAllocator());

  document->AddMember("tips_end_time", "The end time of the query, i.e. the time when the"
      " query is completed, canceled or failed.", document->GetAllocator());

  document->AddMember("tips_first_fetch", "The time taken to fetch the first row."
      " Available in the query profile as \"First row fetched\".",
      document->GetAllocator());

  document->AddMember("tips_client_fetch_duration", "Total time spent returning rows to "
      "the client and other client-side processing. Available in the query profile as "
      "\"ClientFetchWaitTimer\".", document->GetAllocator());

  document->AddMember("tips_duration", "The total duration of the query, including queued"
      " time.", document->GetAllocator());

  document->AddMember("tips_queued_duration", "The duration of the query being queued due"
      " to admission control.", document->GetAllocator());

  document->AddMember("tips_mem_usage", "The sum of memory usage peaks on each executor "
      "during the query execution.", document->GetAllocator());

  document->AddMember("tips_mem_estimate", "The cluster wide estimated memory usage of "
      "this query.", document->GetAllocator());

  document->AddMember("tips_scan_progress", "The scan progress of the query, i.e. the "
      "completed number of scan ranges / the total number of scan ranges.",
      document->GetAllocator());

  document->AddMember("tips_query_progress", "The progress of the query, i.e. the number "
      "of completed fragment instances / the total number of fragment instances.",
      document->GetAllocator());

  document->AddMember("tips_bytes_read", "The total number of bytes read from the data "
      "source during the query execution.", document->GetAllocator());

  document->AddMember("tips_bytes_sent", "The total number of bytes sent by this query in"
      " exchange nodes. Does not include remote reads, data written to disk, or data sent"
      " to the client.", document->GetAllocator());

  document->AddMember("tips_state", "The current state of the query.",
      document->GetAllocator());

  document->AddMember("tips_last_event", "The latest event reported during the execution "
      "of the query.", document->GetAllocator());

  document->AddMember("tips_rows_fetched", "The number of rows already fetched from the "
      "query by the client.", document->GetAllocator());

  document->AddMember("tips_resource_pool", "The resource pool that the query is "
      "submitted to.", document->GetAllocator());

  document->AddMember("tips_statement", "The statement submitted for the query.",
      document->GetAllocator());
}

std::string ImpalaHttpHandler::ProgressToString(int64_t num_completed, int64_t total) {
  stringstream ss;
  ss << num_completed << " / " << total << " (" << setw(4);
  if (num_completed == 0 || total == 0) {
    ss << "0%)";
  } else {
    ss << (100.0 * num_completed / (1.f * total)) << "%)";
  }
  return ss.str();
}

void ImpalaHttpHandler::QueryStateToJson(const QueryStateRecord& record,
    Value* value, Document* document, bool inflight) {
  Value user(record.effective_user.c_str(), document->GetAllocator());
  value->AddMember("effective_user", user, document->GetAllocator());

  Value default_db(record.default_db.c_str(), document->GetAllocator());
  value->AddMember("default_db", default_db, document->GetAllocator());

  // Redact the query string
  std::string tmp_stmt = RedactCopy(record.stmt);
  if(FLAGS_query_stmt_size && tmp_stmt.length() > FLAGS_query_stmt_size) {
    tmp_stmt = tmp_stmt.substr(0, FLAGS_query_stmt_size).append("...");
  }
  Value stmt(tmp_stmt.c_str(), document->GetAllocator());
  value->AddMember("stmt", stmt, document->GetAllocator());

  Value stmt_type(_TStmtType_VALUES_TO_NAMES.find(record.stmt_type)->second,
      document->GetAllocator());
  value->AddMember("stmt_type", stmt_type, document->GetAllocator());

  Value start_time(ToStringFromUnixMicros(record.start_time_us,
      TimePrecision::Millisecond).c_str(), document->GetAllocator());
  value->AddMember("start_time", start_time, document->GetAllocator());

  Value end_time(ToStringFromUnixMicros(record.end_time_us,
      TimePrecision::Millisecond).c_str(), document->GetAllocator());
  value->AddMember("end_time", end_time, document->GetAllocator());

  vector<string>::const_iterator it = std::find(record.event_sequence.labels.begin(),
      record.event_sequence.labels.end(),
      Coordinator::PROFILE_EVENT_LABEL_FIRST_ROW_FETCHED);
  int64_t first_fetch_ns = 0;
  if (it != record.event_sequence.labels.end()) {
    first_fetch_ns = record.event_sequence.timestamps[it
        - record.event_sequence.labels.begin()];
  }
  const string& printed_first_fetch = PrettyPrinter::Print(first_fetch_ns,
      TUnit::TIME_NS);
  Value val_first_fetch(printed_first_fetch.c_str(), document->GetAllocator());
  value->AddMember("first_fetch", val_first_fetch, document->GetAllocator());

  const string& printed_client_fetch_duration = PrettyPrinter::Print(
      record.client_fetch_wait_time_ns, TUnit::TIME_NS);
  Value val_client_fetch_duration(printed_client_fetch_duration.c_str(),
      document->GetAllocator());
  value->AddMember("client_fetch_duration", val_client_fetch_duration,
      document->GetAllocator());

  // record.end_time_us might still be zero if the query is not yet done
  // Use the current Unix time in that case. Note that the duration can be
  // negative if a system clock reset happened after the query was initiated.
  int64_t end_time_us = record.end_time_us > 0LL ? record.end_time_us : UnixMicros();
  int64_t duration_us = end_time_us - record.start_time_us;
  const string& printed_duration = PrettyPrinter::Print(duration_us * NANOS_PER_MICRO,
      TUnit::TIME_NS);
  Value val_duration(printed_duration.c_str(), document->GetAllocator());
  value->AddMember("duration", val_duration, document->GetAllocator());

  const string& printed_queued_duration = PrettyPrinter::Print(record.wait_time_ms,
      TUnit::TIME_MS);
  Value queued_duration(printed_queued_duration.c_str(), document->GetAllocator());
  value->AddMember("queued_duration", queued_duration, document->GetAllocator());

  const string& printed_mem_usage = PrettyPrinter::Print(record.total_peak_mem_usage,
      TUnit::BYTES);
  Value mem_usage(printed_mem_usage.c_str(), document->GetAllocator());
  value->AddMember("mem_usage", mem_usage, document->GetAllocator());

  const string& printed_mem_est = PrettyPrinter::Print(record.cluster_mem_est,
      TUnit::BYTES);
  Value mem_est(printed_mem_est.c_str(), document->GetAllocator());
  value->AddMember("mem_est", mem_est, document->GetAllocator());

  string progress = "N/A";
  string query_progress = "N/A";
  if (record.has_coord) {
    progress =
        ProgressToString(record.num_completed_scan_ranges, record.total_scan_ranges);
    query_progress = ProgressToString(record.num_completed_fragment_instances,
        record.total_fragment_instances);
  }
  Value progress_json(progress.c_str(), document->GetAllocator());
  value->AddMember("progress", progress_json, document->GetAllocator());

  Value query_progress_json(query_progress.c_str(), document->GetAllocator());
  value->AddMember("query_progress", query_progress_json, document->GetAllocator());

  const string& printed_bytes_read = PrettyPrinter::Print(record.bytes_read,
      TUnit::BYTES);
  Value bytes_read(printed_bytes_read.c_str(), document->GetAllocator());
  value->AddMember("bytes_read", bytes_read, document->GetAllocator());

  const string& printed_bytes_sent = PrettyPrinter::Print(record.bytes_sent,
      TUnit::BYTES);
  Value bytes_sent(printed_bytes_sent.c_str(), document->GetAllocator());
  value->AddMember("bytes_sent", bytes_sent, document->GetAllocator());

  Value state(record.query_state.c_str(), document->GetAllocator());
  value->AddMember("state", state, document->GetAllocator());

  value->AddMember("rows_fetched", record.num_rows_fetched, document->GetAllocator());

  Value query_id(PrintId(record.id).c_str(), document->GetAllocator());
  value->AddMember("query_id", query_id, document->GetAllocator());

  if (record.event_sequence.labels.size() > 0) {
    Value last_event(record.event_sequence.labels.back().c_str(),
        document->GetAllocator());
    value->AddMember("last_event", last_event, document->GetAllocator());
  }

  // Waiting to be closed.
  bool waiting = record.beeswax_query_state == beeswax::QueryState::EXCEPTION ||
      record.all_rows_returned;
  value->AddMember("waiting", inflight && waiting, document->GetAllocator());
  value->AddMember("executing", inflight && !waiting, document->GetAllocator());

  int64_t waiting_time = impala::UnixMillis() - record.last_active_time_ms;
  string waiting_time_str = "";
  if (waiting_time > 0) {
    waiting_time_str = PrettyPrinter::Print(waiting_time, TUnit::TIME_MS);
  }
  Value val_waiting_time(waiting_time_str.c_str(), document->GetAllocator());
  value->AddMember("waiting_time", val_waiting_time, document->GetAllocator());

  Value resource_pool(record.resource_pool.c_str(), document->GetAllocator());
  value->AddMember("resource_pool", resource_pool, document->GetAllocator());
}

void ImpalaHttpHandler::QueryStateHandler(const Webserver::WebRequest& req,
    Document* document) {
  AddQueryRecordTips(document);

  set<QueryStateRecord, QueryStateRecord::StartTimeComparator>
      sorted_query_records;

  server_->query_driver_map_.DoFuncForAllEntries(
      [&](const std::shared_ptr<QueryDriver>& query_driver) {
        sorted_query_records.insert(
            QueryStateRecord(*query_driver->GetActiveClientRequestState()));
      });

  unordered_set<TUniqueId> in_flight_query_ids;
  Value in_flight_queries(kArrayType);
  int64_t num_waiting_queries = 0;
  for (const QueryStateRecord& record: sorted_query_records) {
    Value record_json(kObjectType);
    QueryStateToJson(record, &record_json, document, true);

    if (record_json["waiting"].GetBool()) ++num_waiting_queries;

    in_flight_queries.PushBack(record_json, document->GetAllocator());
    in_flight_query_ids.insert(record.id);
  }
  document->AddMember("in_flight_queries", in_flight_queries, document->GetAllocator());
  document->AddMember("num_in_flight_queries",
      static_cast<uint64_t>(sorted_query_records.size()),
      document->GetAllocator());
  document->AddMember("num_executing_queries",
      sorted_query_records.size() - num_waiting_queries,
      document->GetAllocator());
  document->AddMember("num_waiting_queries", num_waiting_queries,
      document->GetAllocator());
  document->AddMember("waiting-tooltip", "These queries are no longer executing, either "
      "because they encountered an error or because they have returned all of their "
      "results, but they are still active so that their results can be inspected. To "
      "free the resources they are using, they must be closed.",
      document->GetAllocator());

  Value completed_queries(kArrayType);
  {
    lock_guard<mutex> l(server_->query_log_lock_);
    for (const shared_ptr<QueryStateRecord>& log_entry :
        server_->query_log_) {
      // Don't show duplicated entries between in-flight and completed queries.
      if (in_flight_query_ids.find(log_entry->id) != in_flight_query_ids.end()) continue;
      Value record_json(kObjectType);
      QueryStateToJson(*log_entry, &record_json, document, false);
      completed_queries.PushBack(record_json, document->GetAllocator());
    }
  }
  document->AddMember("completed_queries", completed_queries, document->GetAllocator());
  document->AddMember("completed_log_size", FLAGS_query_log_size,
      document->GetAllocator());
  document->AddMember("completed_log_size_in_bytes", FLAGS_query_log_size_in_bytes,
      document->GetAllocator());

  Value query_locations(kArrayType);
  {
    lock_guard<mutex> l(server_->query_locations_lock_);
    for (const ImpalaServer::QueryLocations::value_type& location :
        server_->query_locations_) {
      Value location_json(kObjectType);
      Value location_name(NetworkAddressPBToString(location.second.address).c_str(),
          document->GetAllocator());
      location_json.AddMember("location", location_name, document->GetAllocator());
      Value backend_id_str(PrintId(location.first).c_str(), document->GetAllocator());
      location_json.AddMember("backend_id", backend_id_str, document->GetAllocator());
      location_json.AddMember("count",
          static_cast<uint64_t>(location.second.query_ids.size()),
          document->GetAllocator());
      query_locations.PushBack(location_json, document->GetAllocator());
    }
  }
  document->AddMember("query_locations", query_locations, document->GetAllocator());
}

void ImpalaHttpHandler::SessionsHandler(const Webserver::WebRequest& req,
    Document* document) {
  VLOG(1) << "Step1: Fill the sessions information into the document.";
  FillSessionsInfo(document);

  ThriftServer::ConnectionContextList connection_contexts;
  server_->GetAllConnectionContexts(&connection_contexts);
  VLOG(1) << "Step2: Fill the client hosts information into the document.";
  FillClientHostsInfo(document, connection_contexts);

  VLOG(1) << "Step3: Fill the connections information into the document.";
  FillConnectionsInfo(document, connection_contexts);

  VLOG(1) << "Step4: Fill the hs2 users information into the document.";
  FillUsersInfo(document);
}

void ImpalaHttpHandler::FillSessionsInfo(Document* document) {
  lock_guard<mutex> l(server_->session_state_map_lock_);
  Value sessions(kArrayType);
  int num_active = 0;
  for (const ImpalaServer::SessionStateMap::value_type& session :
      server_->session_state_map_) {
    shared_ptr<ImpalaServer::SessionState> state = session.second;
    Value session_json(kObjectType);
    Value type(PrintValue(state->session_type).c_str(), document->GetAllocator());
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

    Value connection_ids(PrintIdSet(state->connections, "\n").c_str(),
        document->GetAllocator());
    session_json.AddMember("connection_ids", connection_ids, document->GetAllocator());

    Value default_db(state->database.c_str(), document->GetAllocator());
    session_json.AddMember("default_database", default_db, document->GetAllocator());

    Value start_time(ToStringFromUnixMillis(session.second->start_time_ms,
        TimePrecision::Second).c_str(), document->GetAllocator());
    session_json.AddMember("start_time", start_time, document->GetAllocator());
    session_json.AddMember(
        "start_time_sort", session.second->start_time_ms, document->GetAllocator());

    Value last_accessed(ToStringFromUnixMillis(session.second->last_accessed_ms,
        TimePrecision::Second).c_str(), document->GetAllocator());
    session_json.AddMember("last_accessed", last_accessed, document->GetAllocator());
    session_json.AddMember(
        "last_accessed_sort", session.second->last_accessed_ms, document->GetAllocator());

    session_json.AddMember("session_timeout", state->session_timeout,
        document->GetAllocator());
    session_json.AddMember("expired", state->expired, document->GetAllocator());
    session_json.AddMember("closed", state->closed, document->GetAllocator());
    if (!state->expired && !state->closed) ++num_active;
    session_json.AddMember("ref_count", state->ref_count, document->GetAllocator());
    sessions.PushBack(session_json, document->GetAllocator());
  }

  document->AddMember("sessions", sessions, document->GetAllocator());
  document->AddMember("num_sessions",
      static_cast<uint64_t>(server_->session_state_map_.size()),
      document->GetAllocator());
  document->AddMember("num_active", num_active, document->GetAllocator());
  document->AddMember("num_inactive", server_->session_state_map_.size() - num_active,
      document->GetAllocator());
}

// Comparer that will sort the users array by the session_count field.
bool SessionCountComparer(const Value& a, const Value& b) {
  return a["session_count"].GetInt64() < b["session_count"].GetInt64();
}

void ImpalaHttpHandler::FillUsersInfo(Document* document) {
  Value users(kArrayType);
  {
    lock_guard<mutex> l(server_->per_user_session_count_lock_);
    for (auto const& it : server_->per_user_session_count_map_) {
      const string& name = it.first;
      const int64& session_count = it.second;
      Value users_json(kObjectType);
      Value user_name(name.c_str(), document->GetAllocator());
      users_json.AddMember("user", user_name, document->GetAllocator());
      users_json.AddMember("session_count", session_count, document->GetAllocator());
      users.PushBack(users_json, document->GetAllocator());
    }
  }
  sort(users.Begin(), users.End(), SessionCountComparer);
  document->AddMember("users", users, document->GetAllocator());
}

void ImpalaHttpHandler::FillClientHostsInfo(
    Document* document, const ThriftServer::ConnectionContextList& connection_contexts) {
  lock_guard<mutex> session_state_map_l(server_->session_state_map_lock_);
  lock_guard<mutex> connection_to_sessions_map_l(
      server_->connection_to_sessions_map_lock_);
  Value client_hosts(kArrayType);
  /// Map from a client hostname to the associated list of connections.
  std::map<std::string, std::set<TUniqueId>> client_hostname_to_connections_map;
  for (const ThriftServer::ConnectionContextList::value_type& connection_context :
      connection_contexts) {
    client_hostname_to_connections_map[connection_context->network_address.hostname]
        .insert(connection_context->connection_id);
  }
  for (const auto& pair : client_hostname_to_connections_map) {
    std::set<TUniqueId> client_hostname_sessions;
    Value client_host_json(kObjectType);
    int64_t total_connections = 0;
    int64_t total_sessions = 0;
    int64_t total_active_sessions = 0;
    int64_t total_inactive_sessions = 0;
    int64_t inflight_queries = 0;
    int64_t total_queries = 0;
    std::set<TUniqueId> connection_ids = pair.second;
    total_connections += connection_ids.size();
    Value hostname(pair.first.c_str(), document->GetAllocator());
    client_host_json.AddMember("hostname", hostname, document->GetAllocator());
    for (const TUniqueId& connection_id : connection_ids) {
      ImpalaServer::ConnectionToSessionMap::iterator it =
          server_->connection_to_sessions_map_.find(connection_id);
      if (it != server_->connection_to_sessions_map_.end()) {
        std::set<TUniqueId> session_ids = it->second;
        for (const TUniqueId& session_id : session_ids) {
          ImpalaServer::SessionStateMap::iterator session_state_map_iterator =
              server_->session_state_map_.find(session_id);
          if (session_state_map_iterator != server_->session_state_map_.end()
              && client_hostname_sessions.find(session_id)
                  == client_hostname_sessions.end()) {
            client_hostname_sessions.insert(session_id);
            ++total_sessions;
            shared_ptr<ImpalaServer::SessionState> session_state =
                session_state_map_iterator->second;
            if (session_state->expired || session_state->closed) {
              ++total_inactive_sessions;
            } else {
              ++total_active_sessions;
            }
            inflight_queries += session_state->inflight_queries.size();
            total_queries += session_state->total_queries;
          }
        }
      }
    }
    client_host_json.AddMember("total_connections", total_connections,
        document->GetAllocator());
    client_host_json.AddMember("total_sessions", total_sessions,
        document->GetAllocator());
    client_host_json.AddMember("total_active_sessions", total_active_sessions,
        document->GetAllocator());
    client_host_json.AddMember("total_inactive_sessions", total_inactive_sessions,
        document->GetAllocator());
    client_host_json.AddMember("inflight_queries", inflight_queries,
        document->GetAllocator());
    client_host_json.AddMember("total_queries", total_queries, document->GetAllocator());
    client_hosts.PushBack(client_host_json, document->GetAllocator());
  }
  document->AddMember("client_hosts", client_hosts, document->GetAllocator());
}

void ImpalaHttpHandler::FillConnectionsInfo(
    Document* document, const ThriftServer::ConnectionContextList& connection_contexts) {
  lock_guard<mutex> session_state_map_l(server_->session_state_map_lock_);
  lock_guard<mutex> connection_to_sessions_map_l(
      server_->connection_to_sessions_map_lock_);
  Value connections(kArrayType);
  int64_t num_beeswax_frontend_connections = 0;
  int64_t num_hiveserver2_frontend_connections = 0;
  int64_t num_hiveserver2_http_frontend_connections = 0;
  int64_t num_external_frontend_connections = 0;
  for (const ThriftServer::ConnectionContextList::value_type& connection_context :
      connection_contexts) {
    if (connection_context.get()) {
      if (connection_context->server_name == ImpalaServer::BEESWAX_SERVER_NAME) {
        ++num_beeswax_frontend_connections;
      } else if (connection_context->server_name == ImpalaServer::HS2_SERVER_NAME) {
        ++num_hiveserver2_frontend_connections;
      } else if (connection_context->server_name == ImpalaServer::HS2_HTTP_SERVER_NAME) {
        ++num_hiveserver2_http_frontend_connections;
      } else if (connection_context->server_name
          == ImpalaServer::EXTERNAL_FRONTEND_SERVER_NAME) {
        ++num_external_frontend_connections;
      }
      Value connection_json(kObjectType);
      Value connection_id(PrintId(connection_context->connection_id).c_str(),
          document->GetAllocator());
      connection_json.AddMember("connection_id", connection_id, document->GetAllocator());

      Value user(connection_context->username.c_str(), document->GetAllocator());
      connection_json.AddMember("user", user, document->GetAllocator());

      Value delegated_user(
          connection_context->do_as_user.c_str(), document->GetAllocator());
      connection_json.AddMember(
          "delegated_user", delegated_user, document->GetAllocator());

      Value network_address(
          TNetworkAddressToString(connection_context->network_address).c_str(),
          document->GetAllocator());
      connection_json.AddMember(
          "network_address", network_address, document->GetAllocator());

      Value server_name(
          connection_context->server_name.c_str(), document->GetAllocator());
      connection_json.AddMember("server_name", server_name, document->GetAllocator());

      std::set<TUniqueId> valid_session_ids;
      ImpalaServer::ConnectionToSessionMap::iterator it =
          server_->connection_to_sessions_map_.find(connection_context->connection_id);
      if (it != server_->connection_to_sessions_map_.end()) {
        // Filter out invalid session
        for (const TUniqueId& session_id : it->second) {
          if (server_->session_state_map_.find(session_id)
              != server_->session_state_map_.end())
            valid_session_ids.insert(session_id);
        }
      }
      Value session_ids_str(PrintIdSet(valid_session_ids, "\n").c_str(),
          document->GetAllocator());
      connection_json.AddMember("session_ids", session_ids_str, document->GetAllocator());
      connections.PushBack(connection_json, document->GetAllocator());
    }
  }
  document->AddMember("connections", connections, document->GetAllocator());
  document->AddMember(
      "num_connections", connection_contexts.size(), document->GetAllocator());
  document->AddMember("num_beeswax_frontend_connections",
      num_beeswax_frontend_connections, document->GetAllocator());
  document->AddMember("num_hiveserver2_frontend_connections",
      num_hiveserver2_frontend_connections, document->GetAllocator());
  document->AddMember("num_hiveserver2_http_frontend_connections",
      num_hiveserver2_http_frontend_connections, document->GetAllocator());
  document->AddMember("num_external_frontend_connections",
      num_external_frontend_connections, document->GetAllocator());
}

void ImpalaHttpHandler::CatalogHandler(const Webserver::WebRequest& req,
    Document* document) {
  TGetDbsResult get_dbs_result;
  Status status = server_->exec_env_->frontend()->GetDbs(NULL, NULL, &get_dbs_result);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }

  Value databases(kArrayType);
  for (const TDatabase& db: get_dbs_result.dbs) {
    Value database(kObjectType);
    Value str(db.db_name.c_str(), document->GetAllocator());
    database.AddMember("name", str, document->GetAllocator());

    TGetTablesResult get_table_results;
    status = server_->exec_env_->frontend()->GetTableNames(
        db.db_name, nullptr, nullptr, &get_table_results);
    if (!status.ok()) {
      Value error(status.GetDetail().c_str(), document->GetAllocator());
      database.AddMember("error", error, document->GetAllocator());
      continue;
    }

    Value table_array(kArrayType);
    for (const string& table: get_table_results.tables) {
      Value table_obj(kObjectType);
      if(!FLAGS_use_local_catalog){
        // Creates hyperlinks for /catalog_object. This is disabled in local catalog mode
        Value fq_name(Substitute("$0.$1", db.db_name, table).c_str(),
            document->GetAllocator());
        table_obj.AddMember("fqtn", fq_name, document->GetAllocator());
      }
      Value table_name(table.c_str(), document->GetAllocator());
      table_obj.AddMember("name", table_name, document->GetAllocator());
      table_array.PushBack(table_obj, document->GetAllocator());
    }
    database.AddMember("num_tables", table_array.Size(), document->GetAllocator());
    database.AddMember("tables", table_array, document->GetAllocator());
    Value use_local_catalog(FLAGS_use_local_catalog);
    database.AddMember("use_local_catalog", use_local_catalog,
        document->GetAllocator());
    databases.PushBack(database, document->GetAllocator());
  }
  document->AddMember("databases", databases, document->GetAllocator());
}

void ImpalaHttpHandler::CatalogObjectsHandler(const Webserver::WebRequest& req,
    Document* document) {
  DCHECK(!FLAGS_use_local_catalog);
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator object_type_arg = args.find("object_type");
  Webserver::ArgumentMap::const_iterator object_name_arg = args.find("object_name");
  if (object_type_arg != args.end() && object_name_arg != args.end()) {
    TCatalogObjectType::type object_type =
        TCatalogObjectTypeFromName(object_type_arg->second);

    // Get the object type and name from the topic entry key
    TCatalogObject request;
    TCatalogObject result;
    Status status = TCatalogObjectFromObjectName(object_type, object_name_arg->second, &request);
    if (status.ok()) {
      // Get the object and dump its contents.
      status = server_->exec_env_->frontend()->GetCatalogObject(request, &result);
    }
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

namespace {

// Helper for PlanToJson(), processes a single list of plan nodes which are the
// DFS-flattened representation of a single plan fragment. Called recursively, the
// iterator parameter is updated in place so that when a recursive call returns, the
// caller is pointing at the next of its children.
void PlanToJsonHelper(const map<TPlanNodeId, TPlanNodeExecSummary>& summaries,
    const vector<TPlanNode>& nodes,
    vector<TPlanNode>::const_iterator* it, rapidjson::Document* document, Value* value) {
  Value children(kArrayType);
  Value label((*it)->label.c_str(), document->GetAllocator());
  value->AddMember("label", label, document->GetAllocator());
  // Node "details" may contain exprs which should be redacted.
  Value label_detail(RedactCopy((*it)->label_detail).c_str(), document->GetAllocator());
  value->AddMember("label_detail", label_detail, document->GetAllocator());

  TPlanNodeId id = (*it)->node_id;
  map<TPlanNodeId, TPlanNodeExecSummary>::const_iterator summary = summaries.find(id);
  if (summary != summaries.end()) {
    int64_t cardinality = 0;
    int64_t max_time = 0L;
    int64_t total_time = 0;
    for (const TExecStats& stat: summary->second.exec_stats) {
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
  for (const TPlanFragment& fragment: fragments) {
    for (const TPlanNode& node: fragment.plan.nodes) {
      label_map[node.node_id] = node.label;
    }
  }

  map<TPlanNodeId, TPlanNodeExecSummary> exec_summaries;
  for (const TPlanNodeExecSummary& s: summary.nodes) {
    exec_summaries[s.node_id] = s;
  }

  Value nodes(kArrayType);
  for (const TPlanFragment& fragment: fragments) {
    Value plan_fragment(kObjectType);
    vector<TPlanNode>::const_iterator it = fragment.plan.nodes.begin();
    PlanToJsonHelper(exec_summaries, fragment.plan.nodes, &it, document, &plan_fragment);
    if (fragment.__isset.output_sink) {
      const TDataSink& sink = fragment.output_sink;
      if (sink.__isset.stream_sink) {
        Value target(label_map[sink.stream_sink.dest_node_id].c_str(),
            document->GetAllocator());
        plan_fragment.AddMember("data_stream_target", target, document->GetAllocator());
      } else if (sink.__isset.join_build_sink) {
        Value target(label_map[sink.join_build_sink.dest_node_id].c_str(),
            document->GetAllocator());
        plan_fragment.AddMember("join_build_target", target, document->GetAllocator());
      }
    }
    nodes.PushBack(plan_fragment, document->GetAllocator());
  }
  value->AddMember("plan_nodes", nodes, document->GetAllocator());
}

}

void ImpalaHttpHandler::QueryBackendsHandler(
    const Webserver::WebRequest& req, Document* document) {
  TUniqueId query_id;
  Status status = ParseIdFromRequest(req, &query_id, "query_id");
  Value query_id_val(PrintId(query_id).c_str(), document->GetAllocator());
  document->AddMember("query_id", query_id_val, document->GetAllocator());
  if (!status.ok()) {
    // Redact the error message, it may contain part or all of the query.
    Value json_error(RedactCopy(status.GetDetail()).c_str(), document->GetAllocator());
    document->AddMember("error", json_error, document->GetAllocator());
    return;
  }

  QueryHandle query_handle;
  status = server_->GetQueryHandle(query_id, &query_handle);
  if (status.ok()) {
    if (query_handle->GetCoordinator() == nullptr) {
      return;
    }
    query_handle->GetCoordinator()->BackendsToJson(document);
  } else {
    VLOG(1) << "Failed to get backends of the query: " << status.GetDetail();
  }
}

void ImpalaHttpHandler::QueryFInstancesHandler(
    const Webserver::WebRequest& req, Document* document) {
  TUniqueId query_id;
  Status status = ParseIdFromRequest(req, &query_id, "query_id");
  Value query_id_val(PrintId(query_id).c_str(), document->GetAllocator());
  document->AddMember("query_id", query_id_val, document->GetAllocator());
  if (!status.ok()) {
    // Redact the error message, it may contain part or all of the query.
    Value json_error(RedactCopy(status.GetDetail()).c_str(), document->GetAllocator());
    document->AddMember("error", json_error, document->GetAllocator());
    return;
  }

  QueryHandle query_handle;
  status = server_->GetQueryHandle(query_id, &query_handle);
  if (status.ok()) {
    if (query_handle->GetCoordinator() == nullptr) {
      return;
    }
    query_handle->GetCoordinator()->FInstanceStatsToJson(document);
  } else {
    VLOG(1) << "Failed to get fragment instances of the query: " << status.GetDetail();
  }
}

void ImpalaHttpHandler::QuerySummaryHandler(bool include_json_plan, bool include_summary,
    const Webserver::WebRequest& req, Document* document) {
  TUniqueId query_id;
  Status status = ParseIdFromRequest(req, &query_id, "query_id");
  Value query_id_val(PrintId(query_id).c_str(), document->GetAllocator());
  document->AddMember("query_id", query_id_val, document->GetAllocator());
  if (!status.ok()) {
    // Redact the error message, it may contain part or all of the query.
    Value json_error(RedactCopy(status.GetDetail()).c_str(), document->GetAllocator());
    document->AddMember("error", json_error, document->GetAllocator());
    return;
  }

  shared_ptr<QueryStateRecord> query_record = nullptr;
  TExecSummary summary;
  string stmt;
  string plan;
  Status query_status;
  bool inflight = false;
  vector<TPlanFragment> fragments;

  // Search the in-flight queries first, followed by the archived ones.
  {
    QueryHandle query_handle;
    status = server_->GetQueryHandle(query_id, &query_handle);
    if (status.ok()) {
      inflight = true;
      query_record = make_shared<QueryStateRecord>(*query_handle);
      // If the query plan isn't generated, avoid waiting for the request
      // state lock to be acquired, since it could potentially be an expensive
      // call, if the table Catalog metadata loading is in progress. Instead
      // update the caller that the plan information is unavailable.
      if (query_handle->exec_state() == ClientRequestState::ExecState::INITIALIZED) {
        document->AddMember(
            "plan_metadata_unavailable", "true", document->GetAllocator());
        return;
      }
      lock_guard<mutex> l(*(*query_handle).lock());
      query_status = query_handle->query_status();
      stmt = query_handle->sql_stmt();
      const TExecRequest& exec_request = query_handle->exec_request();
      plan = exec_request.query_exec_request.query_plan;
      if ((include_json_plan || include_summary)
          && query_handle->GetCoordinator() != nullptr) {
        query_handle->GetCoordinator()->GetTExecSummary(&summary);
      }
      if (include_json_plan) {
        for (const TPlanExecInfo& plan_exec_info:
            exec_request.query_exec_request.plan_exec_info) {
          for (const TPlanFragment& fragment: plan_exec_info.fragments) {
            fragments.push_back(fragment);
          }
        }
      }
    }
  }

  if (!inflight) {
    if (!server_->GetQueryRecord(query_id, &query_record).ok()) {
      const string& err = Substitute("Unknown query id: $0", PrintId(query_id));
      Value json_error(err.c_str(), document->GetAllocator());
      document->AddMember("error", json_error, document->GetAllocator());
      return;
    }
    if (include_json_plan || include_summary) {
      summary = query_record->exec_summary;
    }
    stmt = query_record->stmt;
    plan = query_record->plan;
    query_status = query_record->query_status;
    if (include_json_plan) {
      fragments = query_record->fragments;
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
    Value json_timeline(query_record->timeline.c_str(), document->GetAllocator());
    document->AddMember("timeline", json_timeline, document->GetAllocator());
  }
  Value json_stmt(RedactCopy(stmt).c_str(), document->GetAllocator());
  document->AddMember("stmt", json_stmt, document->GetAllocator());
  Value json_plan_text(RedactCopy(plan).c_str(), document->GetAllocator());
  document->AddMember("plan", json_plan_text, document->GetAllocator());
  Value json_inflight(inflight);
  document->AddMember("inflight", json_inflight, document->GetAllocator());
  Value json_not_inflight(!inflight);
  document->AddMember("not_inflight", json_not_inflight, document->GetAllocator());

  // Redact the error in case the query is contained in the error message.
  Value json_status(query_status.ok() ? "OK" :
      RedactCopy(query_status.GetDetail()).c_str(), document->GetAllocator());
  document->AddMember("status", json_status, document->GetAllocator());

  AddQueryRecordTips(document);

  Value record_json(kObjectType);
  QueryStateToJson(*query_record, &record_json, document, inflight);
  document->AddMember("record_json", record_json, document->GetAllocator());
}

void ImpalaHttpHandler::BackendsHandler(const Webserver::WebRequest& req,
    Document* document) {
  AdmissionController::PerHostStats host_stats;
  DCHECK(admission_controller_ != nullptr);
  admission_controller_->PopulatePerHostMemReservedAndAdmitted(&host_stats);
  Value backends_list(kArrayType);
  int num_active_backends = 0;
  int num_quiescing_backends = 0;
  int num_blacklisted_backends = 0;
  DCHECK(cluster_membership_mgr_ != nullptr);
  ClusterMembershipMgr::SnapshotPtr membership_snapshot =
      cluster_membership_mgr_->GetSnapshot();
  DCHECK(membership_snapshot.get() != nullptr);
  for (const auto& entry : membership_snapshot->current_backends) {
    BackendDescriptorPB backend = entry.second;
    Value backend_obj(kObjectType);
    string address = NetworkAddressPBToString(backend.address());
    Value str(address.c_str(), document->GetAllocator());
    Value krpc_address(NetworkAddressPBToString(backend.krpc_address()).c_str(),
        document->GetAllocator());
    backend_obj.AddMember("address", str, document->GetAllocator());
    backend_obj.AddMember("krpc_address", krpc_address, document->GetAllocator());
    Value backend_id_str(PrintId(backend.backend_id()).c_str(), document->GetAllocator());
    backend_obj.AddMember("backend_id", backend_id_str, document->GetAllocator());
    string webserver_url =
        Substitute("$0://$1", backend.secure_webserver() ? "https" : "http",
            NetworkAddressPBToString(backend.debug_http_address()));
    Value webserver_url_val(webserver_url.c_str(), document->GetAllocator());
    backend_obj.AddMember("webserver_url", webserver_url_val, document->GetAllocator());
    backend_obj.AddMember(
        "is_coordinator", backend.is_coordinator(), document->GetAllocator());
    backend_obj.AddMember("is_executor", backend.is_executor(), document->GetAllocator());
    backend_obj.AddMember(
        "is_quiescing", backend.is_quiescing(), document->GetAllocator());
    Status blacklist_cause;
    int64_t blacklist_time_remaining_ms;
    bool is_blacklisted = membership_snapshot->executor_blacklist.IsBlacklisted(
        backend, &blacklist_cause, &blacklist_time_remaining_ms);
    backend_obj.AddMember("is_blacklisted", is_blacklisted, document->GetAllocator());
    backend_obj.AddMember("is_active", !is_blacklisted && !backend.is_quiescing(),
        document->GetAllocator());
    if (backend.is_quiescing()) {
      // Backends cannot be both blacklisted and quiescing.
      DCHECK(!is_blacklisted);
      ++num_quiescing_backends;
    } else if (is_blacklisted) {
      Value blacklist_cause_value(
          blacklist_cause.GetDetail().c_str(), document->GetAllocator());
      backend_obj.AddMember(
          "blacklist_cause", blacklist_cause_value, document->GetAllocator());
      Value blacklist_time_remaining_str(
          Substitute("$0 s", (blacklist_time_remaining_ms / 1000)).c_str(),
          document->GetAllocator());
      backend_obj.AddMember("blacklist_time_remaining", blacklist_time_remaining_str,
          document->GetAllocator());
      ++num_blacklisted_backends;
    } else {
      ++num_active_backends;
    }
    Value admit_mem_limit(PrettyPrinter::PrintBytes(backend.admit_mem_limit()).c_str(),
        document->GetAllocator());
    backend_obj.AddMember("admit_mem_limit", admit_mem_limit, document->GetAllocator());
    // If the host address does not exist in the 'host_stats', this would ensure that a
    // value of zero is used for those addresses.
    Value mem_reserved(PrettyPrinter::PrintBytes(
        host_stats[address].mem_reserved).c_str(), document->GetAllocator());
    backend_obj.AddMember("mem_reserved", mem_reserved, document->GetAllocator());
    Value mem_admitted(PrettyPrinter::PrintBytes(
        host_stats[address].mem_admitted).c_str(), document->GetAllocator());
    backend_obj.AddMember("mem_admitted", mem_admitted, document->GetAllocator());
    backend_obj.AddMember(
        "admission_slots", backend.admission_slots(), document->GetAllocator());
    backend_obj.AddMember("num_admitted", host_stats[address].num_admitted,
        document->GetAllocator());
    backend_obj.AddMember("admission_slots_in_use", host_stats[address].slots_in_use,
        document->GetAllocator());
    vector<string> group_names;
    for (const auto& group : backend.executor_groups()) {
      group_names.push_back(group.name());
    }
    Value executor_groups(JoinStrings(group_names, ", ").c_str(),
        document->GetAllocator());
    backend_obj.AddMember("executor_groups", executor_groups, document->GetAllocator());
    Value start_time_val(backend.process_start_time().c_str(), document->GetAllocator());
    backend_obj.AddMember("process_start_time", start_time_val, document->GetAllocator());
    Value version_val(backend.version().c_str(), document->GetAllocator());
    backend_obj.AddMember("version", version_val, document->GetAllocator());
    backends_list.PushBack(backend_obj, document->GetAllocator());
  }
  document->AddMember("backends", backends_list, document->GetAllocator());
  document->AddMember(
      "num_active_backends", num_active_backends, document->GetAllocator());
  // Don't add the following fields if they're 0 so that we won't display the
  // corresponding tables if they would be empty.
  if (num_quiescing_backends > 0) {
    document->AddMember(
        "num_quiescing_backends", num_quiescing_backends, document->GetAllocator());
  }
  if (num_blacklisted_backends > 0) {
    document->AddMember(
        "num_blacklisted_backends", num_blacklisted_backends, document->GetAllocator());
  }
}

void ImpalaHttpHandler::AdmissionStateHandler(
    const Webserver::WebRequest& req, Document* document) {
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator pool_name_arg = args.find("pool_name");
  bool get_all_pools = (pool_name_arg == args.end());
  Value resource_pools(kArrayType);
  if (get_all_pools) {
    admission_controller_->AllPoolsToJson(&resource_pools, document);
  } else {
    admission_controller_->PoolToJson(pool_name_arg->second, &resource_pools, document);
  }

  // Now get running queries from CRS map.
  struct QueryInfo {
    TUniqueId query_id;
    int64_t executor_mem_limit;
    int64_t executor_mem_to_admit;
    int64_t coord_mem_limit;
    int64_t coord_mem_to_admit;
    unsigned long num_backends;
  };
  unordered_map<string, vector<QueryInfo>> running_queries;
  if (is_admissiond_) {
    AdmissiondEnv::GetInstance()
        ->admission_control_service()
        ->admission_state_map_.DoFuncForAllEntries(
            [&running_queries](
                const std::shared_ptr<AdmissionControlService::AdmissionState>&
                    query_info) {
              lock_guard<mutex> l(query_info->lock);
              if (query_info->schedule.get() != nullptr) {
                TUniqueId query_id;
                UniqueIdPBToTUniqueId(query_info->query_id, &query_id);
                running_queries[query_info->request_pool].push_back(
                    {query_id, query_info->schedule->per_backend_mem_limit(),
                        query_info->schedule->per_backend_mem_to_admit(),
                        query_info->schedule->coord_backend_mem_limit(),
                        query_info->schedule->coord_backend_mem_to_admit(),
                        static_cast<unsigned long>(
                            query_info->schedule->backend_exec_params().size())});
              };
            });
  } else {
    server_->query_driver_map_.DoFuncForAllEntries(
        [&running_queries](const std::shared_ptr<QueryDriver>& query_driver) {
          // Make sure only queries past admission control are added.
          ClientRequestState* request_state = query_driver->GetActiveClientRequestState();
          auto query_state = request_state->exec_state();
          if (query_state != ClientRequestState::ExecState::INITIALIZED
              && query_state != ClientRequestState::ExecState::PENDING
              && request_state->schedule() != nullptr)
            running_queries[request_state->request_pool()].push_back(
                {request_state->query_id(),
                    request_state->schedule()->per_backend_mem_limit(),
                    request_state->schedule()->per_backend_mem_to_admit(),
                    request_state->schedule()->coord_backend_mem_limit(),
                    request_state->schedule()->coord_backend_mem_to_admit(),
                    static_cast<unsigned long>(
                        request_state->schedule()->backend_exec_params().size())});
        });
  }

  // Add the running queries to the resource_pools json.
  for (int i = 0; i < resource_pools.Size(); i++) {
    DCHECK(resource_pools[i].IsObject());
    Value::MemberIterator it = resource_pools[i].GetObject().FindMember("pool_name");
    DCHECK(it != resource_pools[i].GetObject().MemberEnd());
    DCHECK(it->value.IsString());
    string pool_name(it->value.GetString());
    // Now add running queries to the json.
    auto query_list = running_queries.find(pool_name);
    if (query_list == running_queries.end()) continue;
    vector<QueryInfo>& info_array = query_list->second;
    Value queries_in_pool(rapidjson::kArrayType);
    for (QueryInfo info : info_array) {
      Value query_info(rapidjson::kObjectType);
      Value query_id(PrintId(info.query_id).c_str(), document->GetAllocator());
      query_info.AddMember("query_id", query_id, document->GetAllocator());
      query_info.AddMember(
          "mem_limit", info.executor_mem_limit, document->GetAllocator());
      query_info.AddMember(
          "mem_limit_to_admit", info.executor_mem_to_admit, document->GetAllocator());
      query_info.AddMember(
          "coord_mem_limit", info.coord_mem_limit, document->GetAllocator());
      query_info.AddMember(
          "coord_mem_to_admit", info.coord_mem_to_admit, document->GetAllocator());
      query_info.AddMember("num_backends", info.num_backends, document->GetAllocator());
      queries_in_pool.PushBack(query_info, document->GetAllocator());
    }
    resource_pools[i].GetObject().AddMember(
        "running_queries", queries_in_pool, document->GetAllocator());
  }
  int64_t ms_since_last_statestore_update;
  string staleness_detail =
      admission_controller_->GetStalenessDetail("", &ms_since_last_statestore_update);

  // In order to embed a plain json inside the webpage generated by mustache, we need
  // to stringify it and write it out as a json element. We do not need to pretty-print
  // it, so use the basic writer.
  rapidjson::StringBuffer strbuf;
  Writer<rapidjson::StringBuffer> writer(strbuf);

  resource_pools.Accept(writer);
  Value raw_json(strbuf.GetString(), document->GetAllocator());
  document->AddMember("resource_pools_plain_json", raw_json, document->GetAllocator());
  document->AddMember("resource_pools", resource_pools, document->GetAllocator());
  document->AddMember("statestore_admission_control_time_since_last_update_ms",
      ms_since_last_statestore_update, document->GetAllocator());
  if (!staleness_detail.empty()) {
    Value staleness_detail_json(staleness_detail.c_str(), document->GetAllocator());
    document->AddMember("statestore_update_staleness_detail", staleness_detail_json,
        document->GetAllocator());
  }

  // Indicator that helps render UI elements based on this condition.
  document->AddMember("get_all_pools", get_all_pools, document->GetAllocator());
}

void ImpalaHttpHandler::ResetResourcePoolStatsHandler(
    const Webserver::WebRequest& req, Document* document) {
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator pool_name_arg = args.find("pool_name");
  bool reset_all_pools = (pool_name_arg == args.end());
  if (reset_all_pools) {
    admission_controller_->ResetAllPoolInformationalStats();
  } else {
    admission_controller_->ResetPoolInformationalStats(pool_name_arg->second);
  }
}
