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

#include <algorithm>
#include <exception>
#include <boost/algorithm/string/join.hpp>
#include <boost/filesystem.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/unordered_set.hpp>
#include <thrift/protocol/TDebugProtocol.h>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <google/heap-profiler.h>
#include <google/malloc_extension.h>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/version.h"
#include "exec/ddl-executor.h"
#include "exec/exec-node.h"
#include "exec/hdfs-table-sink.h"
#include "exec/scan-node.h"
#include "exprs/expr.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/client-cache.h"
#include "runtime/descriptors.h"
#include "runtime/data-stream-sender.h"
#include "runtime/row-batch.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/exec-env.h"
#include "runtime/timestamp-value.h"
#include "service/query-exec-state.h"
#include "statestore/simple-scheduler.h"
#include "util/bit-util.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/network-util.h"
#include "util/parse-util.h"
#include "util/string-parser.h"
#include "util/thrift-util.h"
#include "util/thrift-server.h"
#include "util/url-coding.h"
#include "util/webserver.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Frontend_types.h"

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace boost::filesystem;
using namespace boost::uuids;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;
using namespace apache::hive::service::cli::thrift;
using namespace beeswax;
using namespace boost::posix_time;

DECLARE_int32(be_port);
DECLARE_string(nn);
DECLARE_int32(nn_port);
DECLARE_bool(enable_process_lifetime_heap_profiling);
DECLARE_string(heap_profile_dir);

DEFINE_int32(beeswax_port, 21000, "port on which Beeswax client requests are served");
DEFINE_int32(hs2_port, 21050, "port on which HiveServer2 client requests are served");

DEFINE_int32(fe_service_threads, 64,
    "number of threads available to serve client requests");
DEFINE_int32(be_service_threads, 64,
    "(Advanced) number of threads available to serve backend execution requests");
DEFINE_string(default_query_options, "", "key=value pair of default query options for"
    " impalad, separated by ','");
DEFINE_int32(query_log_size, 25, "Number of queries to retain in the query log. If -1, "
                                 "the query log has unbounded size.");
DEFINE_bool(log_query_to_file, true, "if true, logs completed query profiles to file.");

// TODO: this logging should go into a per query log.
DEFINE_int32(log_mem_usage_interval, 0, "If non-zero, impalad will output memory usage "
    "every log_mem_usage_interval'th fragment completion.");

DEFINE_bool(abort_on_config_error, true, "Abort Impala if there are improper configs.");

DEFINE_string(profile_log_dir, "", "The directory in which profile log files are"
    " written. If blank, defaults to <log_file_dir>/profiles");
DEFINE_int32(max_profile_log_file_size, 5000, "The maximum size (in queries) of the "
    "profile log file before a new one is created");

DEFINE_int32(cancellation_thread_pool_size, 5,
    "(Advanced) Size of the thread-pool processing cancellations due to node failure");

namespace impala {

ThreadManager* fe_tm;
ThreadManager* be_tm;

// Prefix of profile log filenames. The version number is
// internal, and does not correspond to an Impala release - it should
// be changed only when the file format changes.
const string PROFILE_LOG_FILE_PREFIX = "impala_profile_log_1.0-";
const ptime EPOCH = time_from_string("1970-01-01 00:00:00.000");

const uint32_t MAX_CANCELLATION_QUEUE_SIZE = 65536;

// Execution state of a single plan fragment.
class ImpalaServer::FragmentExecState {
 public:
  FragmentExecState(const TUniqueId& query_id, int backend_num,
                    const TUniqueId& fragment_instance_id, ExecEnv* exec_env,
                    const TNetworkAddress& coord_hostport)
    : query_id_(query_id),
      backend_num_(backend_num),
      fragment_instance_id_(fragment_instance_id),
      executor_(exec_env,
          bind<void>(mem_fn(&ImpalaServer::FragmentExecState::ReportStatusCb),
                     this, _1, _2, _3)),
      client_cache_(exec_env->client_cache()),
      coord_hostport_(coord_hostport) {
  }

  // Calling the d'tor releases all memory and closes all data streams
  // held by executor_.
  ~FragmentExecState() {
  }

  // Returns current execution status, if there was an error. Otherwise cancels
  // the fragment and returns OK.
  Status Cancel();

  // Call Prepare() and create and initialize data sink.
  Status Prepare(const TExecPlanFragmentParams& exec_params);

  // Main loop of plan fragment execution. Blocks until execution finishes.
  void Exec();

  const TUniqueId& query_id() const { return query_id_; }
  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }

  void set_exec_thread(thread* exec_thread) { exec_thread_.reset(exec_thread); }

 private:
  TUniqueId query_id_;
  int backend_num_;
  TUniqueId fragment_instance_id_;
  PlanFragmentExecutor executor_;
  ImpalaInternalServiceClientCache* client_cache_;
  TExecPlanFragmentParams exec_params_;

  // initiating coordinator to which we occasionally need to report back
  // (it's exported ImpalaInternalService)
  const TNetworkAddress coord_hostport_;

  // the thread executing this plan fragment
  scoped_ptr<thread> exec_thread_;

  // protects exec_status_
  mutex status_lock_;

  // set in ReportStatusCb();
  // if set to anything other than OK, execution has terminated w/ an error
  Status exec_status_;

  // Callback for executor; updates exec_status_ if 'status' indicates an error
  // or if there was a thrift error.
  void ReportStatusCb(const Status& status, RuntimeProfile* profile, bool done);

  // Update exec_status_ w/ status, if the former isn't already an error.
  // Returns current exec_status_.
  Status UpdateStatus(const Status& status);
};

Status ImpalaServer::FragmentExecState::UpdateStatus(const Status& status) {
  lock_guard<mutex> l(status_lock_);
  if (!status.ok() && exec_status_.ok()) exec_status_ = status;
  return exec_status_;
}

Status ImpalaServer::FragmentExecState::Cancel() {
  lock_guard<mutex> l(status_lock_);
  RETURN_IF_ERROR(exec_status_);
  executor_.Cancel();
  return Status::OK;
}

Status ImpalaServer::FragmentExecState::Prepare(
    const TExecPlanFragmentParams& exec_params) {
  exec_params_ = exec_params;
  RETURN_IF_ERROR(executor_.Prepare(exec_params));
  executor_.OptimizeLlvmModule();
  return Status::OK;
}

void ImpalaServer::FragmentExecState::Exec() {
  // Open() does the full execution, because all plan fragments have sinks
  executor_.Open();
  executor_.Close();
}

// There can only be one of these callbacks in-flight at any moment, because
// it is only invoked from the executor's reporting thread.
// Also, the reported status will always reflect the most recent execution status,
// including the final status when execution finishes.
void ImpalaServer::FragmentExecState::ReportStatusCb(
    const Status& status, RuntimeProfile* profile, bool done) {
  DCHECK(status.ok() || done);  // if !status.ok() => done
  Status exec_status = UpdateStatus(status);

  Status coord_status;
  ImpalaInternalServiceConnection coord(client_cache_, coord_hostport_, &coord_status);
  if (!coord_status.ok()) {
    stringstream s;
    s << "couldn't get a client for " << coord_hostport_;
    UpdateStatus(Status(TStatusCode::INTERNAL_ERROR, s.str()));
    return;
  }

  TReportExecStatusParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_query_id(query_id_);
  params.__set_backend_num(backend_num_);
  params.__set_fragment_instance_id(fragment_instance_id_);
  exec_status.SetTStatus(&params);
  params.__set_done(done);
  profile->ToThrift(&params.profile);
  params.__isset.profile = true;

  RuntimeState* runtime_state = executor_.runtime_state();
  DCHECK(runtime_state != NULL);
  // Only send updates to insert status if fragment is finished, the coordinator
  // waits until query execution is done to use them anyhow.
  if (done) {
    TInsertExecStatus insert_status;

    if (runtime_state->hdfs_files_to_move()->size() > 0) {
      insert_status.__set_files_to_move(*runtime_state->hdfs_files_to_move());
    }
    if (executor_.runtime_state()->num_appended_rows()->size() > 0) {
      insert_status.__set_num_appended_rows(
          *executor_.runtime_state()->num_appended_rows());
    }

    params.__set_insert_exec_status(insert_status);
  }

  // Send new errors to coordinator
  runtime_state->GetUnreportedErrors(&(params.error_log));
  params.__isset.error_log = (params.error_log.size() > 0);

  TReportExecStatusResult res;
  Status rpc_status;
  try {
    try {
      coord->ReportExecStatus(res, params);
    } catch (TTransportException& e) {
      VLOG_RPC << "Retrying ReportExecStatus: " << e.what();
      rpc_status = coord.Reopen();
      if (!rpc_status.ok()) {
        // we need to cancel the execution of this fragment
        UpdateStatus(rpc_status);
        executor_.Cancel();
        return;
      }
      coord->ReportExecStatus(res, params);
    }
    rpc_status = Status(res.status);
  } catch (TException& e) {
    stringstream msg;
    msg << "ReportExecStatus() to " << coord_hostport_ << " failed:\n" << e.what();
    VLOG_QUERY << msg.str();
    rpc_status = Status(TStatusCode::INTERNAL_ERROR, msg.str());
  }

  if (!rpc_status.ok()) {
    // we need to cancel the execution of this fragment
    UpdateStatus(rpc_status);
    executor_.Cancel();
  }
}

const char* ImpalaServer::SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION = "42000";
const char* ImpalaServer::SQLSTATE_GENERAL_ERROR = "HY000";
const char* ImpalaServer::SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED = "HYC00";
const int ImpalaServer::ASCII_PRECISION = 16; // print 16 digits for double/float

ImpalaServer::ImpalaServer(ExecEnv* exec_env)
    : profile_log_file_size_(0),
      exec_env_(exec_env) {
  // Initialize default config
  InitializeConfigVariables();

#ifndef ADDRESS_SANITIZER
  // tcmalloc and address sanitizer can not be used together
  if (FLAGS_enable_process_lifetime_heap_profiling) {
    HeapProfilerStart(FLAGS_heap_profile_dir.c_str());
  }
#endif

  frontend_.reset(new Frontend());

  Status status = frontend_->ValidateSettings();
  if (!status.ok()) {
    LOG(ERROR) << status.GetErrorMsg();
    if (FLAGS_abort_on_config_error) {
      LOG(ERROR) << "Impala is aborted due to improper configurations.";
      exit(1);
    }
  }

  if (!InitProfileLogging().ok()) {
    LOG(ERROR) << "Query profile archival is disabled";
    FLAGS_log_query_to_file = false;
  }

  Webserver::PathHandlerCallback varz_callback =
      bind<void>(mem_fn(&ImpalaServer::RenderHadoopConfigs), this, _1, _2);
  exec_env->webserver()->RegisterPathHandler("/varz", varz_callback);

  Webserver::PathHandlerCallback query_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryStatePathHandler), this, _1, _2);
  exec_env->webserver()->RegisterPathHandler("/queries", query_callback);

  Webserver::PathHandlerCallback sessions_callback =
      bind<void>(mem_fn(&ImpalaServer::SessionPathHandler), this, _1, _2);
  exec_env->webserver()->RegisterPathHandler("/sessions", sessions_callback);

  Webserver::PathHandlerCallback catalog_callback =
      bind<void>(mem_fn(&ImpalaServer::CatalogPathHandler), this, _1, _2);
  exec_env->webserver()->RegisterPathHandler("/catalog", catalog_callback);

  Webserver::PathHandlerCallback profile_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryProfilePathHandler), this, _1, _2);
  exec_env->webserver()->
      RegisterPathHandler("/query_profile", profile_callback, true, false);

  Webserver::PathHandlerCallback cancel_callback =
      bind<void>(mem_fn(&ImpalaServer::CancelQueryPathHandler), this, _1, _2);
  exec_env->webserver()->
      RegisterPathHandler("/cancel_query", cancel_callback, true, false);

  Webserver::PathHandlerCallback profile_encoded_callback =
      bind<void>(mem_fn(&ImpalaServer::QueryProfileEncodedPathHandler), this, _1, _2);
  exec_env->webserver()->RegisterPathHandler("/query_profile_encoded",
      profile_encoded_callback, false, false);

  Webserver::PathHandlerCallback inflight_query_ids_callback =
      bind<void>(mem_fn(&ImpalaServer::InflightQueryIdsPathHandler), this, _1, _2);
  exec_env->webserver()->RegisterPathHandler("/inflight_query_ids",
      inflight_query_ids_callback, false, false);

  // Initialize impalad metrics
  ImpaladMetrics::CreateMetrics(exec_env->metrics());
  ImpaladMetrics::IMPALA_SERVER_START_TIME->Update(
      TimestampValue::local_time().DebugString());

  // Register the membership callback if required
  if (exec_env->subscriber() != NULL) {
    StateStoreSubscriber::UpdateCallback cb =
        bind<void>(mem_fn(&ImpalaServer::MembershipCallback), this, _1, _2);
    exec_env->subscriber()->AddTopic(SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC, true, cb);
  }

  EXIT_IF_ERROR(UpdateCatalogMetrics());

  // Initialise the cancellation thread pool with 5 (by default) threads. The max queue
  // size is deliberately set so high that it should never fill; if it does the
  // cancellations will get ignored and retried on the next statestore heartbeat.
  cancellation_thread_pool_.reset(new ThreadPool<TUniqueId>(
      FLAGS_cancellation_thread_pool_size, MAX_CANCELLATION_QUEUE_SIZE,
      bind<void>(&ImpalaServer::CancelFromThreadPool, this, _1, _2)));
}

Status ImpalaServer::InitProfileLogging() {
  if (!FLAGS_log_query_to_file) return Status::OK;

  if (FLAGS_profile_log_dir.empty()) {
    stringstream ss;
    ss << FLAGS_log_dir << "/profiles/";
    FLAGS_profile_log_dir = ss.str();
  }

  LOG(INFO) << "Profile log path: " << FLAGS_profile_log_dir;

  if (!exists(FLAGS_profile_log_dir)) {
    LOG(INFO) << "Profile log directory does not exist, creating: "
              << FLAGS_profile_log_dir;
    try {
      create_directory(FLAGS_profile_log_dir);
    } catch (const std::exception& e) { // Explicit std:: to distinguish from boost::
      LOG(ERROR) << "Could not create profile log directory: "
                 << FLAGS_profile_log_dir << ", " << e.what();
      return Status("Failed to create profile log directory");
    }
  }

  if (!is_directory(FLAGS_profile_log_dir)) {
    LOG(ERROR) << "Profile log path is not a directory ("
               << FLAGS_profile_log_dir << ")";
    return Status("Profile log path is not a directory");
  }

  LOG(INFO) << "Profile log path is a directory ("
            << FLAGS_profile_log_dir << ")";

  Status log_file_status = OpenProfileLogFile(false);
  if (!log_file_status.ok()) {
    LOG(ERROR) << "Could not open query log file for writing: "
               << log_file_status.GetErrorMsg();
    return log_file_status;
  }
  profile_log_file_flush_thread_.reset(
      new thread(&ImpalaServer::LogFileFlushThread, this));

  return Status::OK;
}

void ImpalaServer::RenderHadoopConfigs(const Webserver::ArgumentMap& args,
    stringstream* output) {
  frontend_->RenderHadoopConfigs(args.find("raw") != args.end(), output);
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
  if (UnregisterQuery(unique_id)) {
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
  if (!status.ok()) {
    (*output) << status.GetErrorMsg();
  }
}

void ImpalaServer::InflightQueryIdsPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  lock_guard<mutex> l(query_exec_state_map_lock_);
  BOOST_FOREACH(const QueryExecStateMap::value_type& exec_state, query_exec_state_map_) {
    *output << exec_state.second->query_id() << "\n";
  }
}

Status ImpalaServer::GetRuntimeProfileStr(const TUniqueId& query_id,
    bool base64_encoded, stringstream* output) {
  DCHECK(output != NULL);
  // Search for the query id in the active query map
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);
    QueryExecStateMap::const_iterator exec_state = query_exec_state_map_.find(query_id);
    if (exec_state != query_exec_state_map_.end()) {
      if (base64_encoded) {
        exec_state->second->profile().SerializeToArchiveString(output);
      } else {
        exec_state->second->profile().PrettyPrint(output);
      }
      return Status::OK;
    }
  }

  // The query was not found the active query map, search the query log.
  {
    lock_guard<mutex> l(query_log_lock_);
    QueryLogIndex::const_iterator query_record = query_log_index_.find(query_id);
    if (query_record == query_log_index_.end()) {
      stringstream ss;
      ss << "Query id " << PrintId(query_id) << " not found.";
      return Status(ss.str());
    }
    if (base64_encoded) {
      (*output) << query_record->second->encoded_profile_str;
    } else {
      (*output) << query_record->second->profile_str;
    }
  }
  return Status::OK;
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
  (*output) << "<h2>Queries</h2>";
  lock_guard<mutex> l(query_exec_state_map_lock_);
  (*output) << "This page lists all registered queries, i.e., those that are not closed "
    " nor cancelled.<br/>" << endl;
  (*output) << query_exec_state_map_.size() << " queries in flight" << endl;
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
  BOOST_FOREACH(const QueryExecStateMap::value_type& exec_state, query_exec_state_map_) {
    QueryStateRecord record(*exec_state.second);
    RenderSingleQueryTableRow(record, false, true, output);
  }

  (*output) << "</table>";

  // Print the query location counts.
  (*output) << "<h2>Query Locations</h2>";
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
  (*output) << "<h2>Finished Queries</h2>";
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
            << "<th>User</th>"
            << "<th>Session ID</th>"
            << "<th>Network Address</th>"
            << "<th>Default Database</th>"
            << "<th>Start Time</th></tr>"
            << endl;
  BOOST_FOREACH(const SessionStateMap::value_type& session, session_state_map_) {
    string session_type;
    string session_id;
    if (session.second->session_type == BEESWAX) {
      session_type = "Beeswax";
      session_id = session.first;
    } else {
      session_type = "HiveServer2";
      // Print HiveServer2 session key as TUniqueId
      stringstream result;
      TUniqueId tmp_key;
      memcpy(&(tmp_key.hi), session.first.c_str(), 8);
      memcpy(&(tmp_key.lo), session.first.c_str() + 8, 8);
      result << tmp_key.hi << ":" << tmp_key.lo;
      session_id = result.str();
    }
    (*output) << "<tr>"
              << "<td>" << session_type << "</td>"
              << "<td>" << session.second->user << "</td>"
              << "<td>" << session_id << "</td>"
              << "<td>" << session.second->network_address << "</td>"
              << "<td>" << session.second->database << "</td>"
              << "<td>" << session.second->start_time.DebugString() << "</td>"
              << "</tr>";
  }
  (*output) << "</table>";
}

void ImpalaServer::CatalogPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  TGetDbsResult get_dbs_result;
  Status status = frontend_->GetDbNames(NULL, NULL, &get_dbs_result);
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
      (*output) << "<a id='" << db << "'><h3>" << db << "</h3></a>";
      TGetTablesResult get_table_results;
      Status status = frontend_->GetTableNames(db, NULL, NULL, &get_table_results);
      if (!status.ok()) {
        (*output) << "Error: " << status.GetErrorMsg();
        continue;
      }
      vector<string>& table_names = get_table_results.tables;
      (*output) << "<p>" << db << " contains <b>" << table_names.size()
                << "</b> tables</p>";

      (*output) << "<ul>" << endl;
      BOOST_FOREACH(const string& table, table_names) {
        (*output) << "<li>" << table << "</li>" << endl;
      }
      (*output) << "</ul>" << endl;
    }
  } else {
    (*output) << "Catalog" << endl << endl;
    (*output) << "List of databases:" << endl;
    (*output) << join(db_names, "\n") << endl << endl;

    BOOST_FOREACH(const string& db, db_names) {
      TGetTablesResult get_table_results;
      Status status = frontend_->GetTableNames(db, NULL, NULL, &get_table_results);
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

Status ImpalaServer::OpenProfileLogFile(bool reopen) {
  if (profile_log_file_.is_open()) {
    // flush() alone does not apparently fsync, but we actually want
    // the results to become visible, hence the close / reopen
    profile_log_file_.flush();
    profile_log_file_.close();
  }
  if (!reopen) {
    stringstream ss;
    int64_t ms_since_epoch = (microsec_clock::local_time() - EPOCH).total_milliseconds();
    ss << FLAGS_profile_log_dir << "/" << PROFILE_LOG_FILE_PREFIX
       << ms_since_epoch;
    profile_log_file_name_ = ss.str();
    profile_log_file_size_ = 0;
  }
  profile_log_file_.open(profile_log_file_name_.c_str(), ios_base::app | ios_base::out);
  if (!profile_log_file_.is_open()) return Status("Could not open log file");
  return Status::OK;
}

void ImpalaServer::LogFileFlushThread() {
  while (true) {
    sleep(5);
    {
      lock_guard<mutex> l(profile_log_file_lock_);
      OpenProfileLogFile(true);
    }
  }
}

void ImpalaServer::ArchiveQuery(const QueryExecState& query) {
  string encoded_profile_str = query.profile().SerializeToArchiveString();

  // If there was an error initialising archival (e.g. directory is
  // not writeable), FLAGS_log_query_to_file will have been set to
  // false
  if (FLAGS_log_query_to_file) {
    lock_guard<mutex> l(profile_log_file_lock_);
    if (!profile_log_file_.is_open() ||
        profile_log_file_size_ > FLAGS_max_profile_log_file_size) {
      // Roll the file
      Status status = OpenProfileLogFile(false);
      if (!status.ok()) {
        LOG_EVERY_N(WARNING, 1000) << "Could not open new query log file ("
                                   << google::COUNTER << " attempts failed): "
                                   << status.GetErrorMsg();
        LOG_EVERY_N(WARNING, 1000)
            << "Disable query logging with --log_query_to_file=false";
      }
    }

    if (profile_log_file_.is_open()) {
      int64_t timestamp = (microsec_clock::local_time() - EPOCH).total_milliseconds();
      profile_log_file_ << timestamp << " " << query.query_id() << " "
                      << encoded_profile_str
                      << "\n"; // Not std::endl, since that causes an implicit flush
      ++profile_log_file_size_;
    }
  }

  if (FLAGS_query_log_size == 0) return;
  QueryStateRecord record(query, true, encoded_profile_str);
  {
    lock_guard<mutex> l(query_log_lock_);
    // Add record to the beginning of the log, and to the lookup index.
    query_log_index_[query.query_id()] = query_log_.insert(query_log_.begin(), record);

    if (FLAGS_query_log_size > -1 && FLAGS_query_log_size < query_log_.size()) {
      DCHECK_EQ(query_log_.size() - FLAGS_query_log_size, 1);
      query_log_index_.erase(query_log_.back().id);
      query_log_.pop_back();
    }
  }
}

ImpalaServer::~ImpalaServer() {}

Status ImpalaServer::Execute(const TClientRequest& request,
    shared_ptr<SessionState> session_state,
    const TSessionState& query_session_state,
    shared_ptr<QueryExecState>* exec_state) {
  bool registered_exec_state;
  ImpaladMetrics::IMPALA_SERVER_NUM_QUERIES->Increment(1L);
  Status status = ExecuteInternal(request, session_state, query_session_state,
      &registered_exec_state, exec_state);
  if (!status.ok() && registered_exec_state) {
    UnregisterQuery((*exec_state)->query_id());
  }
  return status;
}

Status ImpalaServer::ExecuteInternal(
    const TClientRequest& request,
    shared_ptr<SessionState> session_state,
    const TSessionState& query_session_state,
    bool* registered_exec_state,
    shared_ptr<QueryExecState>* exec_state) {
  DCHECK(session_state != NULL);

  *registered_exec_state = false;

  exec_state->reset(new QueryExecState(
      exec_env_, frontend_.get(), session_state, query_session_state, request.stmt));

  (*exec_state)->query_events()->MarkEvent("Start execution");

  TExecRequest result;
  {
    // Keep a lock on exec_state so that registration and setting
    // result_metadata are atomic.
    //
    // Note: this acquires the exec_state lock *before* the
    // query_exec_state_map_ lock. This is the opposite of
    // GetQueryExecState(..., true), and therefore looks like a
    // candidate for deadlock. The reason this works here is that
    // GetQueryExecState cannot find exec_state (under the exec state
    // map lock) and take it's lock until RegisterQuery has
    // finished. By that point, the exec state map lock will have been
    // given up, so the classic deadlock interleaving is not possible.
    lock_guard<mutex> l(*(*exec_state)->lock());

    // register exec state as early as possible so that queries that
    // take a long time to plan show up, and to handle incoming status
    // reports before execution starts.
    RETURN_IF_ERROR(RegisterQuery(session_state, *exec_state));
    *registered_exec_state = true;

    RETURN_IF_ERROR((*exec_state)->UpdateQueryStatus(
        frontend_->GetExecRequest(request, &result)));
    (*exec_state)->query_events()->MarkEvent("Planning finished");
    if (result.__isset.result_set_metadata) {
      (*exec_state)->set_result_metadata(result.result_set_metadata);
    }
  }

  // start execution of query; also starts fragment status reports
  RETURN_IF_ERROR((*exec_state)->Exec(&result));
  if (result.stmt_type == TStmtType::DDL) {
    Status status = UpdateCatalogMetrics();
    if (!status.ok()) {
      VLOG_QUERY << "Couldn't update catalog metrics: " << status.GetErrorMsg();
    }
  }

  if ((*exec_state)->coord() != NULL) {
    const unordered_set<TNetworkAddress>& unique_hosts =
        (*exec_state)->coord()->unique_hosts();
    if (!unique_hosts.empty()) {
      lock_guard<mutex> l(query_locations_lock_);
      BOOST_FOREACH(const TNetworkAddress& port, unique_hosts) {
        query_locations_[port].insert((*exec_state)->query_id());
      }
    }
  }

  return Status::OK;
}

Status ImpalaServer::RegisterQuery(shared_ptr<SessionState> session_state,
    const shared_ptr<QueryExecState>& exec_state) {
  lock_guard<mutex> l2(session_state->lock);
  if (session_state->closed) {
    return Status("Session has been closed, ignoring query.");
  }

  lock_guard<mutex> l(query_exec_state_map_lock_);
  const TUniqueId& query_id = exec_state->query_id();
  QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
  if (entry != query_exec_state_map_.end()) {
    // There shouldn't be an active query with that same id.
    // (query_id is globally unique)
    stringstream ss;
    ss << "query id " << PrintId(query_id) << " already exists";
    return Status(TStatusCode::INTERNAL_ERROR, ss.str());
  }

  session_state->inflight_queries.insert(query_id);
  query_exec_state_map_.insert(make_pair(query_id, exec_state));
  return Status::OK;
}

bool ImpalaServer::UnregisterQuery(const TUniqueId& query_id) {
  VLOG_QUERY << "UnregisterQuery(): query_id=" << query_id;

  // Cancel the query if it's still running
  CancelInternal(query_id);

  shared_ptr<QueryExecState> exec_state;
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);
    QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
    if (entry == query_exec_state_map_.end()) {
      VLOG_QUERY << "unknown query id: " << PrintId(query_id);
      return false;
    } else {
      exec_state = entry->second;
    }
    query_exec_state_map_.erase(entry);
  }

  exec_state->Done();

  {
    lock_guard<mutex> l(exec_state->parent_session()->lock);
    exec_state->parent_session()->inflight_queries.erase(query_id);
  }

  ArchiveQuery(*exec_state);

  if (exec_state->coord() != NULL) {
    const unordered_set<TNetworkAddress>& unique_hosts =
        exec_state->coord()->unique_hosts();
    if (!unique_hosts.empty()) {
      lock_guard<mutex> l(query_locations_lock_);
      BOOST_FOREACH(const TNetworkAddress& hostport, unique_hosts) {
        // Query may have been removed already by cancellation path. In
        // particular, if node to fail was last sender to an exchange, the
        // coordinator will realise and fail the query at the same time the
        // failure detection path does the same thing. They will harmlessly race
        // to remove the query from this map.
        QueryLocations::iterator it = query_locations_.find(hostport);
        if (it != query_locations_.end()) {
          it->second.erase(exec_state->query_id());
        }
      }
    }
  }

  return true;
}

void ImpalaServer::Wait(shared_ptr<QueryExecState> exec_state) {
  // block until results are ready
  Status status = exec_state->Wait();
  {
    lock_guard<mutex> l(*(exec_state->lock()));
    if (exec_state->returns_result_set()) {
      exec_state->query_events()->MarkEvent("Rows available");
    } else {
      exec_state->query_events()->MarkEvent("Request finished");
    }

    exec_state->UpdateQueryStatus(status);
  }
  if (status.ok()) {
    exec_state->UpdateQueryState(QueryState::FINISHED);
  }
}

Status ImpalaServer::UpdateCatalogMetrics() {
  TGetDbsResult db_names;
  RETURN_IF_ERROR(frontend_->GetDbNames(NULL, NULL, &db_names));
  ImpaladMetrics::CATALOG_NUM_DBS->Update(db_names.dbs.size());
  ImpaladMetrics::CATALOG_NUM_TABLES->Update(0L);
  BOOST_FOREACH(const string& db, db_names.dbs) {
    TGetTablesResult table_names;
    RETURN_IF_ERROR(frontend_->GetTableNames(db, NULL, NULL, &table_names));
    ImpaladMetrics::CATALOG_NUM_TABLES->Increment(table_names.tables.size());
  }

  return Status::OK;
}

Status ImpalaServer::CancelInternal(const TUniqueId& query_id) {
  VLOG_QUERY << "Cancel(): query_id=" << PrintId(query_id);
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
  if (exec_state == NULL) return Status("Invalid or unknown query handle");

  lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());
  // TODO: can we call Coordinator::Cancel() here while holding lock?
  exec_state->Cancel();
  return Status::OK;
}

Status ImpalaServer::CloseSessionInternal(const ThriftServer::SessionId& session_id) {
  // Find the session_state and remove it from the map.
  shared_ptr<SessionState> session_state;
  {
    lock_guard<mutex> l(session_state_map_lock_);
    SessionStateMap::iterator entry = session_state_map_.find(session_id);
    if (entry == session_state_map_.end()) {
      return Status("Invalid session ID");
    }
    session_state = entry->second;
    session_state_map_.erase(session_id);
  }
  DCHECK(session_state != NULL);

  unordered_set<TUniqueId> inflight_queries;
  {
    lock_guard<mutex> l(session_state->lock);
    DCHECK(!session_state->closed);
    session_state->closed = true;
    // Once closed is set to true, no more queries can be started. The inflight list
    // will not grow.
    inflight_queries.insert(session_state->inflight_queries.begin(),
        session_state->inflight_queries.end());
  }

  // Unregister all open queries from this session.
  BOOST_FOREACH(const TUniqueId& query_id, inflight_queries) {
    UnregisterQuery(query_id);
  }

  return Status::OK;
}


Status ImpalaServer::ParseQueryOptions(const string& options,
    TQueryOptions* query_options) {
  if (options.length() == 0) return Status::OK;
  vector<string> kv_pairs;
  split(kv_pairs, options, is_any_of(","), token_compress_on);
  BOOST_FOREACH(string& kv_string, kv_pairs) {
    trim(kv_string);
    if (kv_string.length() == 0) continue;
    vector<string> key_value;
    split(key_value, kv_string, is_any_of("="), token_compress_on);
    if (key_value.size() != 2) {
      stringstream ss;
      ss << "Ignoring invalid configuration option " << kv_string
         << ": bad format (expected key=value)";
      return Status(ss.str());
    }

    RETURN_IF_ERROR(SetQueryOptions(key_value[0], key_value[1], query_options));
  }
  return Status::OK;
}

Status ImpalaServer::SetQueryOptions(const string& key, const string& value,
    TQueryOptions* query_options) {
  int option = GetQueryOption(key);
  if (option < 0) {
    stringstream ss;
    ss << "Ignoring invalid configuration option: " << key;
    return Status(ss.str());
  } else {
    switch (option) {
      case TImpalaQueryOptions::ABORT_ON_ERROR:
        query_options->__set_abort_on_error(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::MAX_ERRORS:
        query_options->__set_max_errors(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::DISABLE_CODEGEN:
        query_options->__set_disable_codegen(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::BATCH_SIZE:
        query_options->__set_batch_size(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::MEM_LIMIT: {
        // Parse the mem limit spec and validate it.
        bool is_percent;
        int64_t bytes_limit = ParseUtil::ParseMemSpec(value, &is_percent);
        if (bytes_limit < 0) {
          return Status("Failed to parse mem limit from '" + value + "'.");
        }
        if (is_percent) {
          return Status("Invalid query memory limit with percent '" + value + "'.");
        }
        query_options->__set_mem_limit(bytes_limit);
        break;
      }
      case TImpalaQueryOptions::NUM_NODES:
        query_options->__set_num_nodes(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH:
        query_options->__set_max_scan_range_length(atol(value.c_str()));
        break;
      case TImpalaQueryOptions::MAX_IO_BUFFERS:
        query_options->__set_max_io_buffers(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::NUM_SCANNER_THREADS:
        query_options->__set_num_scanner_threads(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::ALLOW_UNSUPPORTED_FORMATS:
        query_options->__set_allow_unsupported_formats(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::DEFAULT_ORDER_BY_LIMIT:
        query_options->__set_default_order_by_limit(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::DEBUG_ACTION:
        query_options->__set_debug_action(value.c_str());
        break;
      case TImpalaQueryOptions::PARQUET_COMPRESSION_CODEC: {
        if (value.empty()) break;
        if (iequals(value, "none")) {
          query_options->__set_parquet_compression_codec(THdfsCompression::NONE);
        } else if (iequals(value, "gzip")) {
          query_options->__set_parquet_compression_codec(THdfsCompression::GZIP);
        } else if (iequals(value, "snappy")) {
          query_options->__set_parquet_compression_codec(THdfsCompression::SNAPPY);
        } else {
          stringstream ss;
          ss << "Invalid parquet compression codec: " << value;
          return Status(ss.str());
        }
        break;
      }
      case TImpalaQueryOptions::ABORT_ON_DEFAULT_LIMIT_EXCEEDED:
        query_options->__set_abort_on_default_limit_exceeded(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::HBASE_CACHING:
        query_options->__set_hbase_caching(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::HBASE_CACHE_BLOCKS:
        query_options->__set_hbase_cache_blocks(
            iequals(value, "true") || iequals(value, "1"));
        break;
      default:
        // We hit this DCHECK(false) if we forgot to add the corresponding entry here
        // when we add a new query option.
        LOG(ERROR) << "Missing exec option implementation: " << key;
        DCHECK(false);
        break;
    }
  }
  return Status::OK;
}

inline shared_ptr<ImpalaServer::FragmentExecState> ImpalaServer::GetFragmentExecState(
    const TUniqueId& fragment_instance_id) {
  lock_guard<mutex> l(fragment_exec_state_map_lock_);
  FragmentExecStateMap::iterator i = fragment_exec_state_map_.find(fragment_instance_id);
  if (i == fragment_exec_state_map_.end()) {
    return shared_ptr<FragmentExecState>();
  } else {
    return i->second;
  }
}

void ImpalaServer::ExecPlanFragment(
    TExecPlanFragmentResult& return_val, const TExecPlanFragmentParams& params) {
  VLOG_QUERY << "ExecPlanFragment() instance_id=" << params.params.fragment_instance_id
             << " coord=" << params.coord << " backend#=" << params.backend_num;
  StartPlanFragmentExecution(params).SetTStatus(&return_val);
}

void ImpalaServer::ReportExecStatus(
    TReportExecStatusResult& return_val, const TReportExecStatusParams& params) {
  VLOG_FILE << "ReportExecStatus() query_id=" << params.query_id
            << " backend#=" << params.backend_num
            << " instance_id=" << params.fragment_instance_id
            << " done=" << (params.done ? "true" : "false");
  // TODO: implement something more efficient here, we're currently
  // acquiring/releasing the map lock and doing a map lookup for
  // every report (assign each query a local int32_t id and use that to index into a
  // vector of QueryExecStates, w/o lookup or locking?)
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(params.query_id, false);
  if (exec_state.get() == NULL) {
    return_val.status.__set_status_code(TStatusCode::INTERNAL_ERROR);
    stringstream str;
    str << "unknown query id: " << params.query_id;
    return_val.status.error_msgs.push_back(str.str());
    LOG(ERROR) << str.str();
    return;
  }
  exec_state->coord()->UpdateFragmentExecStatus(params).SetTStatus(&return_val);
}

void ImpalaServer::CancelPlanFragment(
    TCancelPlanFragmentResult& return_val, const TCancelPlanFragmentParams& params) {
  VLOG_QUERY << "CancelPlanFragment(): instance_id=" << params.fragment_instance_id;
  shared_ptr<FragmentExecState> exec_state =
      GetFragmentExecState(params.fragment_instance_id);
  if (exec_state.get() == NULL) {
    stringstream str;
    str << "unknown fragment id: " << params.fragment_instance_id;
    Status status(TStatusCode::INTERNAL_ERROR, str.str());
    status.SetTStatus(&return_val);
    return;
  }
  // we only initiate cancellation here, the map entry as well as the exec state
  // are removed when fragment execution terminates (which is at present still
  // running in exec_state->exec_thread_)
  exec_state->Cancel().SetTStatus(&return_val);
}

void ImpalaServer::TransmitData(
    TTransmitDataResult& return_val, const TTransmitDataParams& params) {
  VLOG_ROW << "TransmitData(): instance_id=" << params.dest_fragment_instance_id
           << " node_id=" << params.dest_node_id
           << " #rows=" << params.row_batch.num_rows
           << " eos=" << (params.eos ? "true" : "false");
  // TODO: fix Thrift so we can simply take ownership of thrift_batch instead
  // of having to copy its data
  if (params.row_batch.num_rows > 0) {
    Status status = exec_env_->stream_mgr()->AddData(
        params.dest_fragment_instance_id, params.dest_node_id, params.row_batch);
    status.SetTStatus(&return_val);
    if (!status.ok()) {
      // should we close the channel here as well?
      return;
    }
  }

  if (params.eos) {
    exec_env_->stream_mgr()->CloseSender(
        params.dest_fragment_instance_id, params.dest_node_id).SetTStatus(&return_val);
  }
}

Status ImpalaServer::StartPlanFragmentExecution(
    const TExecPlanFragmentParams& exec_params) {
  if (!exec_params.fragment.__isset.output_sink) {
    return Status("missing sink in plan fragment");
  }

  const TPlanFragmentExecParams& params = exec_params.params;
  shared_ptr<FragmentExecState> exec_state(
      new FragmentExecState(
        params.query_id, exec_params.backend_num, params.fragment_instance_id,
        exec_env_, exec_params.coord));
  // Call Prepare() now, before registering the exec state, to avoid calling
  // exec_state->Cancel().
  // We might get an async cancellation, and the executor requires that Cancel() not
  // be called before Prepare() returns.
  RETURN_IF_ERROR(exec_state->Prepare(exec_params));

  {
    lock_guard<mutex> l(fragment_exec_state_map_lock_);
    // register exec_state before starting exec thread
    fragment_exec_state_map_.insert(make_pair(params.fragment_instance_id, exec_state));
  }

  // execute plan fragment in new thread
  // TODO: manage threads via global thread pool
  exec_state->set_exec_thread(
      new thread(&ImpalaServer::RunExecPlanFragment, this, exec_state.get()));
  return Status::OK;
}

void ImpalaServer::RunExecPlanFragment(FragmentExecState* exec_state) {
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->Increment(1L);
  exec_state->Exec();

  // we're done with this plan fragment
  {
    lock_guard<mutex> l(fragment_exec_state_map_lock_);
    FragmentExecStateMap::iterator i =
        fragment_exec_state_map_.find(exec_state->fragment_instance_id());
    if (i != fragment_exec_state_map_.end()) {
      // ends up calling the d'tor, if there are no async cancellations
      fragment_exec_state_map_.erase(i);
    } else {
      LOG(ERROR) << "missing entry in fragment exec state map: instance_id="
                 << exec_state->fragment_instance_id();
    }
  }
#ifndef ADDRESS_SANITIZER
  // tcmalloc and address sanitizer can not be used together
  if (FLAGS_log_mem_usage_interval > 0) {
    uint64_t num_complete = ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->value();
    if (num_complete % FLAGS_log_mem_usage_interval == 0) {
      char buf[2048];
      // This outputs how much memory is currently being used by this impalad
      MallocExtension::instance()->GetStats(buf, 2048);
      LOG(INFO) << buf;
    }
  }
#endif
}

int ImpalaServer::GetQueryOption(const string& key) {
  map<int, const char*>::const_iterator itr =
      _TImpalaQueryOptions_VALUES_TO_NAMES.begin();
  for (; itr != _TImpalaQueryOptions_VALUES_TO_NAMES.end(); ++itr) {
    if (iequals(key, (*itr).second)) {
      return itr->first;
    }
  }
  return -1;
}

void ImpalaServer::InitializeConfigVariables() {
  Status status = ParseQueryOptions(FLAGS_default_query_options, &default_query_options_);
  if (!status.ok()) {
    // Log error and exit if the default query options are invalid.
    LOG(ERROR) << "Invalid default query options. Please check -default_query_options.\n"
               << status.GetErrorMsg();
    exit(1);
  }
  LOG(INFO) << "Default query options:" << ThriftDebugString(default_query_options_);

  map<string, string> string_map;
  TQueryOptionsToMap(default_query_options_, &string_map);
  map<string, string>::const_iterator itr = string_map.begin();
  for (; itr != string_map.end(); ++itr) {
    ConfigVariable option;
    option.__set_key(itr->first);
    option.__set_value(itr->second);
    default_configs_.push_back(option);
  }
  ConfigVariable support_start_over;
  support_start_over.__set_key("support_start_over");
  support_start_over.__set_value("false");
  default_configs_.push_back(support_start_over);
}

void ImpalaServer::TQueryOptionsToMap(const TQueryOptions& query_option,
    map<string, string>* configuration) {
  map<int, const char*>::const_iterator itr =
      _TImpalaQueryOptions_VALUES_TO_NAMES.begin();
  for (; itr != _TImpalaQueryOptions_VALUES_TO_NAMES.end(); ++itr) {
    stringstream val;
    switch (itr->first) {
      case TImpalaQueryOptions::ABORT_ON_ERROR:
        val << query_option.abort_on_error;
        break;
      case TImpalaQueryOptions::MAX_ERRORS:
        val << query_option.max_errors;
        break;
      case TImpalaQueryOptions::DISABLE_CODEGEN:
        val << query_option.disable_codegen;
        break;
      case TImpalaQueryOptions::BATCH_SIZE:
        val << query_option.batch_size;
        break;
      case TImpalaQueryOptions::MEM_LIMIT:
        val << query_option.mem_limit;
        break;
      case TImpalaQueryOptions::NUM_NODES:
        val << query_option.num_nodes;
        break;
      case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH:
        val << query_option.max_scan_range_length;
        break;
      case TImpalaQueryOptions::MAX_IO_BUFFERS:
        val << query_option.max_io_buffers;
        break;
      case TImpalaQueryOptions::NUM_SCANNER_THREADS:
        val << query_option.num_scanner_threads;
        break;
      case TImpalaQueryOptions::ALLOW_UNSUPPORTED_FORMATS:
        val << query_option.allow_unsupported_formats;
        break;
      case TImpalaQueryOptions::DEFAULT_ORDER_BY_LIMIT:
        val << query_option.default_order_by_limit;
        break;
      case TImpalaQueryOptions::DEBUG_ACTION:
        val << query_option.debug_action;
        break;
      case TImpalaQueryOptions::ABORT_ON_DEFAULT_LIMIT_EXCEEDED:
        val << query_option.abort_on_default_limit_exceeded;
        break;
      case TImpalaQueryOptions::PARQUET_COMPRESSION_CODEC:
        val << query_option.parquet_compression_codec;
        break;
      case TImpalaQueryOptions::HBASE_CACHING:
        val << query_option.hbase_caching;
        break;
      case TImpalaQueryOptions::HBASE_CACHE_BLOCKS:
        val << query_option.hbase_cache_blocks;
        break;
      default:
        // We hit this DCHECK(false) if we forgot to add the corresponding entry here
        // when we add a new query option.
        LOG(ERROR) << "Missing exec option implementation: " << itr->second;
        DCHECK(false);
    }
    (*configuration)[itr->second] = val.str();
  }
}

void ImpalaServer::SessionState::ToThrift(const ThriftServer::SessionId& session_id,
    TSessionState* state) {
  lock_guard<mutex> l(lock);
  state->database = database;
  state->user = user;
  state->session_id = session_id;
  state->network_address = network_address;
}

void ImpalaServer::CancelFromThreadPool(uint32_t thread_id, const TUniqueId& query_id) {
  Status status = CancelInternal(query_id);
  if (!status.ok()) {
    VLOG_QUERY << "Query cancellation (" << query_id << ") did not succeed: "
               << status.GetErrorMsg();
  }
}

void ImpalaServer::MembershipCallback(
    const StateStoreSubscriber::TopicDeltaMap& topic_deltas,
    vector<TTopicUpdate>* topic_updates) {
  // TODO: Consider rate-limiting this. In the short term, best to have
  // state-store heartbeat less frequently.
  StateStoreSubscriber::TopicDeltaMap::const_iterator topic =
      topic_deltas.find(SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC);

  if (topic != topic_deltas.end()) {
    const TTopicDelta& delta = topic->second;
    // Although the protocol allows for it, we don't accept true
    // deltas from the state-store, but expect each topic to contains
    // its entire contents.
    if (delta.is_delta) {
      LOG_EVERY_N(WARNING, 60) << "Unexpected topic delta from state-store, ignoring"
                               << "(seen " << google::COUNTER << " deltas)";
      return;
    }
    set<TNetworkAddress> current_membership;

    BOOST_FOREACH(const TTopicItem& item, delta.topic_entries) {
      uint32_t len = item.value.size();
      TBackendDescriptor backend_descriptor;
      Status status = DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &backend_descriptor);
      if (!status.ok()) {
        VLOG(2) << "Error deserializing topic item with key: " << item.key;
        continue;
      }
      current_membership.insert(backend_descriptor.address);
    }

    set<TUniqueId> queries_to_cancel;
    {
      // Build a list of queries that are running on failed hosts (as evidenced by their
      // absence from the membership list).
      // TODO: crash-restart failures can give false negatives for failed Impala demons.
      lock_guard<mutex> l(query_locations_lock_);
      QueryLocations::const_iterator backend = query_locations_.begin();
      while (backend != query_locations_.end()) {
        if (current_membership.find(backend->first) == current_membership.end()) {
          queries_to_cancel.insert(backend->second.begin(), backend->second.end());
          exec_env_->client_cache()->CloseConnections(backend->first);
          // We can remove the location wholesale once we know backend's failed. To do so
          // safely during iteration, we have to be careful not in invalidate the current
          // iterator, so copy the iterator to do the erase(..) and advance the original.
          QueryLocations::const_iterator failed_backend = backend;
          ++backend;
          query_locations_.erase(failed_backend);
        } else {
          ++backend;
        }
      }
    }

    if (cancellation_thread_pool_->GetQueueSize() + queries_to_cancel.size() >
        MAX_CANCELLATION_QUEUE_SIZE) {
      // Ignore the cancellations - we'll be able to process them on the next heartbeat
      // instead.
      LOG_EVERY_N(WARNING, 60) << "Cancellation queue is full";
    } else {
      // Since we are the only producer for this pool, we know that this cannot block
      // indefinitely since the queue is large enough to accept all new cancellation
      // requests.
      BOOST_FOREACH(const TUniqueId& query_id, queries_to_cancel) {
        cancellation_thread_pool_->Offer(query_id);
      }
    }
  }
}

ImpalaServer::QueryStateRecord::QueryStateRecord(const QueryExecState& exec_state,
    bool copy_profile, const string& encoded_profile) {
  id = exec_state.query_id();
  const TExecRequest& request = exec_state.exec_request();

  stmt = exec_state.sql_stmt();
  stmt_type = request.stmt_type;
  user = exec_state.user();
  default_db = exec_state.default_db();
  start_time = exec_state.start_time();
  end_time = exec_state.end_time();
  has_coord = false;

  Coordinator* coord = exec_state.coord();
  if (coord != NULL) {
    num_complete_fragments = coord->progress().num_complete();
    total_fragments = coord->progress().total();
    has_coord = true;
  }
  query_state = exec_state.query_state();
  num_rows_fetched = exec_state.num_rows_fetched();

  if (copy_profile) {
    stringstream ss;
    exec_state.profile().PrettyPrint(&ss);
    profile_str = ss.str();
    if (encoded_profile.empty()) {
      encoded_profile_str = exec_state.profile().SerializeToArchiveString();
    } else {
      encoded_profile_str = encoded_profile;
    }
  }
}

Status CreateImpalaServer(ExecEnv* exec_env, int beeswax_port, int hs2_port,
    int be_port, ThriftServer** beeswax_server, ThriftServer** hs2_server,
    ThriftServer** be_server, ImpalaServer** impala_server) {
  DCHECK((beeswax_port == 0) == (beeswax_server == NULL));
  DCHECK((hs2_port == 0) == (hs2_server == NULL));
  DCHECK((be_port == 0) == (be_server == NULL));

  shared_ptr<ImpalaServer> handler(new ImpalaServer(exec_env));

  // TODO: do we want a BoostThreadFactory?
  // TODO: we want separate thread factories here, so that fe requests can't starve
  // be requests
  shared_ptr<ThreadFactory> thread_factory(new PosixThreadFactory());

  if (beeswax_port != 0 && beeswax_server != NULL) {
    // Beeswax FE must be a TThreadPoolServer because ODBC and Hue only support
    // TThreadPoolServer.
    shared_ptr<TProcessor> beeswax_processor(new ImpalaServiceProcessor(handler));
    *beeswax_server = new ThriftServer("beeswax-frontend", beeswax_processor,
        beeswax_port, exec_env->metrics(), FLAGS_fe_service_threads,
        ThriftServer::ThreadPool);

    (*beeswax_server)->SetSessionHandler(handler.get());

    LOG(INFO) << "Impala Beeswax Service listening on " << beeswax_port;
  }

  if (hs2_port != 0 && hs2_server != NULL) {
    // HiveServer2 JDBC driver does not support non-blocking server.
    shared_ptr<TProcessor> hs2_fe_processor(
        new ImpalaHiveServer2ServiceProcessor(handler));
    *hs2_server = new ThriftServer("hiveServer2-frontend",
        hs2_fe_processor, hs2_port, exec_env->metrics(), FLAGS_fe_service_threads,
        ThriftServer::ThreadPool);

    LOG(INFO) << "Impala HiveServer2 Service listening on " << hs2_port;
  }

  if (be_port != 0 && be_server != NULL) {
    shared_ptr<TProcessor> be_processor(new ImpalaInternalServiceProcessor(handler));
    *be_server = new ThriftServer("backend", be_processor, be_port, exec_env->metrics(),
        FLAGS_be_service_threads);

    LOG(INFO) << "ImpalaInternalService listening on " << be_port;
  }
  if (impala_server != NULL) *impala_server = handler.get();

  return Status::OK;
}

shared_ptr<ImpalaServer::QueryExecState> ImpalaServer::GetQueryExecState(
    const TUniqueId& query_id, bool lock) {
  lock_guard<mutex> l(query_exec_state_map_lock_);
  QueryExecStateMap::iterator i = query_exec_state_map_.find(query_id);
  if (i == query_exec_state_map_.end()) {
    return shared_ptr<QueryExecState>();
  } else {
    if (lock) i->second->lock()->lock();
    return i->second;
  }
}

}
