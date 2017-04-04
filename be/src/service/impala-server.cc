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

#include "service/impala-server.h"

#include <algorithm>
#include <exception>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/filesystem.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/unordered_set.hpp>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <gperftools/malloc_extension.h>
#include <gutil/strings/substitute.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

#include "catalog/catalog-server.h"
#include "catalog/catalog-util.h"
#include "common/logging.h"
#include "common/version.h"
#include "exec/external-data-source-executor.h"
#include "rpc/authentication.h"
#include "rpc/rpc-trace.h"
#include "rpc/thrift-thread.h"
#include "rpc/thrift-util.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/lib-cache.h"
#include "runtime/timestamp-value.h"
#include "runtime/tmp-file-mgr.h"
#include "scheduling/scheduler.h"
#include "service/impala-http-handler.h"
#include "service/impala-internal-service.h"
#include "service/query-exec-state.h"
#include "util/bit-util.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/lineage-util.h"
#include "util/network-util.h"
#include "util/parse-util.h"
#include "util/redactor.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"
#include "util/string-parser.h"
#include "util/summary-util.h"
#include "util/uid-util.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/LineageGraph_types.h"

#include "common/names.h"

using boost::adopt_lock_t;
using boost::algorithm::is_any_of;
using boost::algorithm::istarts_with;
using boost::algorithm::replace_all_copy;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::get_system_time;
using boost::system_time;
using boost::uuids::random_generator;
using boost::uuids::uuid;
using namespace apache::thrift;
using namespace boost::posix_time;
using namespace beeswax;
using namespace strings;

DECLARE_int32(be_port);
DECLARE_string(nn);
DECLARE_int32(nn_port);
DECLARE_string(authorized_proxy_user_config);
DECLARE_string(authorized_proxy_user_config_delimiter);
DECLARE_bool(abort_on_config_error);
DECLARE_bool(disk_spill_encryption);

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

DEFINE_int64(max_result_cache_size, 100000L, "Maximum number of query results a client "
    "may request to be cached on a per-query basis to support restarting fetches. This "
    "option guards against unreasonably large result caches requested by clients. "
    "Requests exceeding this maximum will be rejected.");

DEFINE_int32(max_audit_event_log_file_size, 5000, "The maximum size (in queries) of the "
    "audit event log file before a new one is created (if event logging is enabled)");
DEFINE_string(audit_event_log_dir, "", "The directory in which audit event log files are "
    "written. Setting this flag will enable audit event logging.");
DEFINE_bool(abort_on_failed_audit_event, true, "Shutdown Impala if there is a problem "
    "recording an audit event.");

DEFINE_int32(max_lineage_log_file_size, 5000, "The maximum size (in queries) of "
    "the lineage event log file before a new one is created (if lineage logging is "
    "enabled)");
DEFINE_string(lineage_event_log_dir, "", "The directory in which lineage event log "
    "files are written. Setting this flag with enable lineage logging.");
DEFINE_bool(abort_on_failed_lineage_event, true, "Shutdown Impala if there is a problem "
    "recording a lineage record.");

DEFINE_string(profile_log_dir, "", "The directory in which profile log files are"
    " written. If blank, defaults to <log_file_dir>/profiles");
DEFINE_int32(max_profile_log_file_size, 5000, "The maximum size (in queries) of the "
    "profile log file before a new one is created");
DEFINE_int32(max_profile_log_files, 10, "Maximum number of profile log files to "
    "retain. The most recent log files are retained. If set to 0, all log files "
    "are retained.");

DEFINE_int32(cancellation_thread_pool_size, 5,
    "(Advanced) Size of the thread-pool processing cancellations due to node failure");

DEFINE_string(ssl_server_certificate, "", "The full path to the SSL certificate file used"
    " to authenticate Impala to clients. If set, both Beeswax and HiveServer2 ports will "
    "only accept SSL connections");
DEFINE_string(ssl_private_key, "", "The full path to the private key used as a "
    "counterpart to the public key contained in --ssl_server_certificate. If "
    "--ssl_server_certificate is set, this option must be set as well.");
DEFINE_string(ssl_client_ca_certificate, "", "(Advanced) The full path to a certificate "
    "used by Thrift clients to check the validity of a server certificate. May either be "
    "a certificate for a third-party Certificate Authority, or a copy of the certificate "
    "the client expects to receive from the server.");
DEFINE_string(ssl_private_key_password_cmd, "", "A Unix command whose output returns the "
    "password used to decrypt the certificate private key file specified in "
    "--ssl_private_key. If the .PEM key file is not password-protected, this command "
    "will not be invoked. The output of the command will be truncated to 1024 bytes, and "
    "then all trailing whitespace will be trimmed before it is used to decrypt the "
    "private key");


DEFINE_int32(idle_session_timeout, 0, "The time, in seconds, that a session may be idle"
    " for before it is closed (and all running queries cancelled) by Impala. If 0, idle"
    " sessions are never expired.");
DEFINE_int32(idle_query_timeout, 0, "The time, in seconds, that a query may be idle for"
    " (i.e. no processing work is done and no updates are received from the client) "
    "before it is cancelled. If 0, idle queries are never expired. The query option "
    "QUERY_TIMEOUT_S overrides this setting, but, if set, --idle_query_timeout represents"
    " the maximum allowable timeout.");

DEFINE_bool(is_coordinator, true, "If true, this Impala daemon can accept and coordinate "
    "queries from clients. If false, this daemon will only execute query fragments, and "
    "will refuse client connections.");

// TODO: Remove for Impala 3.0.
DEFINE_string(local_nodemanager_url, "", "Deprecated");

DECLARE_bool(compact_catalog_topic);

namespace impala {

// Prefix of profile, event and lineage log filenames. The version number is
// internal, and does not correspond to an Impala release - it should
// be changed only when the file format changes.
//
// In the 1.0 version of the profile log, the timestamp at the beginning of each entry
// was relative to the local time zone. In log version 1.1, this was changed to be
// relative to UTC. The same time zone change was made for the audit log, but the
// version was kept at 1.0 because there is no known consumer of the timestamp.
const string PROFILE_LOG_FILE_PREFIX = "impala_profile_log_1.1-";
const string ImpalaServer::AUDIT_EVENT_LOG_FILE_PREFIX = "impala_audit_event_log_1.0-";
const string LINEAGE_LOG_FILE_PREFIX = "impala_lineage_log_1.0-";

const uint32_t MAX_CANCELLATION_QUEUE_SIZE = 65536;
// Max size for multiple update in a single split. JNI is not able to write java byte
// array more than 2GB. A single topic update is not restricted by this.
const uint64_t MAX_CATALOG_UPDATE_BATCH_SIZE_BYTES = 500 * 1024 * 1024;

const string BEESWAX_SERVER_NAME = "beeswax-frontend";
const string HS2_SERVER_NAME = "hiveserver2-frontend";

const char* ImpalaServer::SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION = "42000";
const char* ImpalaServer::SQLSTATE_GENERAL_ERROR = "HY000";
const char* ImpalaServer::SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED = "HYC00";

// Work item for ImpalaServer::cancellation_thread_pool_.
class CancellationWork {
 public:
  CancellationWork(const TUniqueId& query_id, const Status& cause, bool unregister)
      : query_id_(query_id), cause_(cause), unregister_(unregister) {
  }

  CancellationWork() {
  }

  const TUniqueId& query_id() const { return query_id_; }
  const Status& cause() const { return cause_; }
  bool unregister() const { return unregister_; }

  bool operator<(const CancellationWork& other) const {
    return query_id_ < other.query_id_;
  }

  bool operator==(const CancellationWork& other) const {
    return query_id_ == other.query_id_;
  }

 private:
  // Id of query to be canceled.
  TUniqueId query_id_;

  // Error status containing a list of failed impalads causing the cancellation.
  Status cause_;

  // If true, unregister the query rather than cancelling it. Calling UnregisterQuery()
  // does call CancelInternal eventually, but also ensures that the query is torn down and
  // archived.
  bool unregister_;
};

ImpalaServer::ImpalaServer(ExecEnv* exec_env)
    : exec_env_(exec_env),
      thrift_serializer_(false) {
  // Initialize default config
  InitializeConfigVariables();

  Status status = exec_env_->frontend()->ValidateSettings();
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    if (FLAGS_abort_on_config_error) {
      CLEAN_EXIT_WITH_ERROR(
          "Aborting Impala Server startup due to improper configuration");
    }
  }

  status = exec_env->tmp_file_mgr()->Init(exec_env->metrics());
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    if (FLAGS_abort_on_config_error) {
      CLEAN_EXIT_WITH_ERROR("Aborting Impala Server startup due to improperly "
           "configured scratch directories.");
    }
  }

  if (!InitProfileLogging().ok()) {
    LOG(ERROR) << "Query profile archival is disabled";
    FLAGS_log_query_to_file = false;
  }

  if (!InitAuditEventLogging().ok()) {
    CLEAN_EXIT_WITH_ERROR("Aborting Impala Server startup due to failure initializing "
        "audit event logging");
  }

  if (!InitLineageLogging().ok()) {
    CLEAN_EXIT_WITH_ERROR("Aborting Impala Server startup due to failure initializing "
        "lineage logging");
  }

  if (!FLAGS_authorized_proxy_user_config.empty()) {
    // Parse the proxy user configuration using the format:
    // <proxy user>=<comma separated list of users they are allowed to delegate>
    // See FLAGS_authorized_proxy_user_config for more details.
    vector<string> proxy_user_config;
    split(proxy_user_config, FLAGS_authorized_proxy_user_config, is_any_of(";"),
        token_compress_on);
    if (proxy_user_config.size() > 0) {
      for (const string& config: proxy_user_config) {
        size_t pos = config.find("=");
        if (pos == string::npos) {
          CLEAN_EXIT_WITH_ERROR(Substitute("Invalid proxy user configuration. No "
              "mapping value specified for the proxy user. For more information review "
              "usage of the --authorized_proxy_user_config flag: $0", config));
        }
        string proxy_user = config.substr(0, pos);
        string config_str = config.substr(pos + 1);
        vector<string> parsed_allowed_users;
        split(parsed_allowed_users, config_str,
            is_any_of(FLAGS_authorized_proxy_user_config_delimiter), token_compress_on);
        unordered_set<string> allowed_users(parsed_allowed_users.begin(),
            parsed_allowed_users.end());
        authorized_proxy_user_config_.insert(make_pair(proxy_user, allowed_users));
      }
    }
  }

  if (FLAGS_disk_spill_encryption) {
    // Initialize OpenSSL for spilling encryption. This is not thread-safe so we
    // initialize it once on startup.
    // TODO: Set OpenSSL callbacks to provide locking to make the library thread-safe.
    OpenSSL_add_all_algorithms();
    ERR_load_crypto_strings();
  }

  http_handler_.reset(new ImpalaHttpHandler(this));
  http_handler_->RegisterHandlers(exec_env->webserver());

  // Initialize impalad metrics
  ImpaladMetrics::CreateMetrics(
      exec_env->metrics()->GetOrCreateChildGroup("impala-server"));
  ImpaladMetrics::IMPALA_SERVER_START_TIME->set_value(
      TimestampValue::LocalTime().DebugString());

  ABORT_IF_ERROR(ExternalDataSourceExecutor::InitJNI(exec_env->metrics()));

  // Register the membership callback if required
  if (exec_env->subscriber() != nullptr) {
    auto cb = [this] (const StatestoreSubscriber::TopicDeltaMap& state,
         vector<TTopicDelta>* topic_updates) {
      this->MembershipCallback(state, topic_updates);
    };
    exec_env->subscriber()->AddTopic(Scheduler::IMPALA_MEMBERSHIP_TOPIC, true, cb);

    if (FLAGS_is_coordinator) {
      auto catalog_cb = [this] (const StatestoreSubscriber::TopicDeltaMap& state,
          vector<TTopicDelta>* topic_updates) {
        this->CatalogUpdateCallback(state, topic_updates);
      };
      exec_env->subscriber()->AddTopic(CatalogServer::IMPALA_CATALOG_TOPIC, true,
          catalog_cb);
    }
  }

  ABORT_IF_ERROR(UpdateCatalogMetrics());

  // Initialise the cancellation thread pool with 5 (by default) threads. The max queue
  // size is deliberately set so high that it should never fill; if it does the
  // cancellations will get ignored and retried on the next statestore heartbeat.
  cancellation_thread_pool_.reset(new ThreadPool<CancellationWork>(
          "impala-server", "cancellation-worker",
      FLAGS_cancellation_thread_pool_size, MAX_CANCELLATION_QUEUE_SIZE,
      bind<void>(&ImpalaServer::CancelFromThreadPool, this, _1, _2)));

  // Initialize a session expiry thread which blocks indefinitely until the first session
  // with non-zero timeout value is opened. Note that a session which doesn't specify any
  // idle session timeout value will use the default value FLAGS_idle_session_timeout.
  session_timeout_thread_.reset(new Thread("impala-server", "session-expirer",
      bind<void>(&ImpalaServer::ExpireSessions, this)));

  query_expiration_thread_.reset(new Thread("impala-server", "query-expirer",
      bind<void>(&ImpalaServer::ExpireQueries, this)));

  is_coordinator_ = FLAGS_is_coordinator;
  exec_env_->SetImpalaServer(this);
}

Status ImpalaServer::LogLineageRecord(const QueryExecState& query_exec_state) {
  const TExecRequest& request = query_exec_state.exec_request();
  if (!request.__isset.query_exec_request && !request.__isset.catalog_op_request) {
    return Status::OK();
  }
  TLineageGraph lineage_graph;
  if (request.__isset.query_exec_request &&
      request.query_exec_request.__isset.lineage_graph) {
    lineage_graph = request.query_exec_request.lineage_graph;
  } else if (request.__isset.catalog_op_request &&
      request.catalog_op_request.__isset.lineage_graph) {
    lineage_graph = request.catalog_op_request.lineage_graph;
  } else {
    return Status::OK();
  }
  // Set the query end time in TLineageGraph. Must use UNIX time directly rather than
  // e.g. converting from query_exec_state.end_time() (IMPALA-4440).
  lineage_graph.__set_ended(UnixMillis() / 1000);
  string lineage_record;
  LineageUtil::TLineageToJSON(lineage_graph, &lineage_record);
  const Status& status = lineage_logger_->AppendEntry(lineage_record);
  if (!status.ok()) {
    LOG(ERROR) << "Unable to record query lineage record: " << status.GetDetail();
    if (FLAGS_abort_on_failed_lineage_event) {
      CLEAN_EXIT_WITH_ERROR("Shutting down Impala Server due to "
          "abort_on_failed_lineage_event=true");
    }
  }
  return status;
}

bool ImpalaServer::IsCoordinator() { return is_coordinator_; }

bool ImpalaServer::IsLineageLoggingEnabled() {
  return !FLAGS_lineage_event_log_dir.empty();
}

Status ImpalaServer::InitLineageLogging() {
  if (!IsLineageLoggingEnabled()) {
    LOG(INFO) << "Lineage logging is disabled";
    return Status::OK();
  }
  lineage_logger_.reset(new SimpleLogger(FLAGS_lineage_event_log_dir,
      LINEAGE_LOG_FILE_PREFIX, FLAGS_max_lineage_log_file_size));
  RETURN_IF_ERROR(lineage_logger_->Init());
  lineage_logger_flush_thread_.reset(new Thread("impala-server",
        "lineage-log-flush", &ImpalaServer::LineageLoggerFlushThread, this));
  return Status::OK();
}

Status ImpalaServer::LogAuditRecord(const ImpalaServer::QueryExecState& exec_state,
    const TExecRequest& request) {
  stringstream ss;
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

  writer.StartObject();
  // Each log entry is a timestamp mapped to a JSON object
  ss << UnixMillis();
  writer.String(ss.str().c_str());
  writer.StartObject();
  writer.String("query_id");
  writer.String(PrintId(exec_state.query_id()).c_str());
  writer.String("session_id");
  writer.String(PrintId(exec_state.session_id()).c_str());
  writer.String("start_time");
  writer.String(exec_state.start_time().DebugString().c_str());
  writer.String("authorization_failure");
  writer.Bool(Frontend::IsAuthorizationError(exec_state.query_status()));
  writer.String("status");
  writer.String(exec_state.query_status().GetDetail().c_str());
  writer.String("user");
  writer.String(exec_state.effective_user().c_str());
  writer.String("impersonator");
  if (exec_state.do_as_user().empty()) {
    // If there is no do_as_user() is empty, the "impersonator" field should be Null.
    writer.Null();
  } else {
    // Otherwise, the delegator is the current connected user.
    writer.String(exec_state.connected_user().c_str());
  }
  writer.String("statement_type");
  if (request.stmt_type == TStmtType::DDL) {
    if (request.catalog_op_request.op_type == TCatalogOpType::DDL) {
      writer.String(
          PrintTDdlType(request.catalog_op_request.ddl_params.ddl_type).c_str());
    } else {
      writer.String(PrintTCatalogOpType(request.catalog_op_request.op_type).c_str());
    }
  } else {
    writer.String(PrintTStmtType(request.stmt_type).c_str());
  }
  writer.String("network_address");
  writer.String(
      lexical_cast<string>(exec_state.session()->network_address).c_str());
  writer.String("sql_statement");
  string stmt = replace_all_copy(exec_state.sql_stmt(), "\n", " ");
  Redact(&stmt);
  writer.String(stmt.c_str());
  writer.String("catalog_objects");
  writer.StartArray();
  for (const TAccessEvent& event: request.access_events) {
    writer.StartObject();
    writer.String("name");
    writer.String(event.name.c_str());
    writer.String("object_type");
    writer.String(PrintTCatalogObjectType(event.object_type).c_str());
    writer.String("privilege");
    writer.String(event.privilege.c_str());
    writer.EndObject();
  }
  writer.EndArray();
  writer.EndObject();
  writer.EndObject();
  Status status = audit_event_logger_->AppendEntry(buffer.GetString());
  if (!status.ok()) {
    LOG(ERROR) << "Unable to record audit event record: " << status.GetDetail();
    if (FLAGS_abort_on_failed_audit_event) {
      CLEAN_EXIT_WITH_ERROR("Shutting down Impala Server due to "
          "abort_on_failed_audit_event=true");
    }
  }
  return status;
}

bool ImpalaServer::IsAuditEventLoggingEnabled() {
  return !FLAGS_audit_event_log_dir.empty();
}

Status ImpalaServer::InitAuditEventLogging() {
  if (!IsAuditEventLoggingEnabled()) {
    LOG(INFO) << "Event logging is disabled";
    return Status::OK();
  }
  audit_event_logger_.reset(new SimpleLogger(FLAGS_audit_event_log_dir,
     AUDIT_EVENT_LOG_FILE_PREFIX, FLAGS_max_audit_event_log_file_size));
  RETURN_IF_ERROR(audit_event_logger_->Init());
  audit_event_logger_flush_thread_.reset(new Thread("impala-server",
        "audit-event-log-flush", &ImpalaServer::AuditEventLoggerFlushThread, this));
  return Status::OK();
}

void ImpalaServer::LogQueryEvents(const QueryExecState& exec_state) {
  Status status = exec_state.query_status();
  bool log_events = true;
  switch (exec_state.stmt_type()) {
    case TStmtType::QUERY: {
      // If the query didn't finish, log audit and lineage events only if the
      // the client issued at least one fetch.
      if (!status.ok() && !exec_state.fetched_rows()) log_events = false;
      break;
    }
    case TStmtType::DML: {
      if (!status.ok()) log_events = false;
      break;
    }
    case TStmtType::DDL: {
      if (exec_state.catalog_op_type() == TCatalogOpType::DDL) {
        // For a DDL operation, log audit and lineage events only if the
        // operation finished.
        if (!status.ok()) log_events = false;
      } else {
        // This case covers local catalog operations such as SHOW and DESCRIBE.
        if (!status.ok() && !exec_state.fetched_rows()) log_events = false;
      }
      break;
    }
    case TStmtType::EXPLAIN:
    case TStmtType::LOAD:
    case TStmtType::SET:
    default:
      break;
  }
  // Log audit events that are due to an AuthorizationException.
  if (IsAuditEventLoggingEnabled() &&
      (Frontend::IsAuthorizationError(exec_state.query_status()) || log_events)) {
    LogAuditRecord(exec_state, exec_state.exec_request());
  }
  if (IsLineageLoggingEnabled() && log_events) {
    LogLineageRecord(exec_state);
  }
}

Status ImpalaServer::InitProfileLogging() {
  if (!FLAGS_log_query_to_file) return Status::OK();

  if (FLAGS_profile_log_dir.empty()) {
    stringstream ss;
    ss << FLAGS_log_dir << "/profiles/";
    FLAGS_profile_log_dir = ss.str();
  }
  profile_logger_.reset(new SimpleLogger(FLAGS_profile_log_dir,
      PROFILE_LOG_FILE_PREFIX, FLAGS_max_profile_log_file_size,
      FLAGS_max_profile_log_files));
  RETURN_IF_ERROR(profile_logger_->Init());
  profile_log_file_flush_thread_.reset(new Thread("impala-server", "log-flush-thread",
      &ImpalaServer::LogFileFlushThread, this));

  return Status::OK();
}

Status ImpalaServer::GetRuntimeProfileStr(const TUniqueId& query_id,
    bool base64_encoded, stringstream* output) {
  DCHECK(output != nullptr);
  // Search for the query id in the active query map
  {
    shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
    if (exec_state.get() != nullptr) {
      lock_guard<mutex> l(*exec_state->lock());
      if (base64_encoded) {
        exec_state->profile().SerializeToArchiveString(output);
      } else {
        exec_state->profile().PrettyPrint(output);
      }
      return Status::OK();
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
  return Status::OK();
}

Status ImpalaServer::GetExecSummary(const TUniqueId& query_id, TExecSummary* result) {
  // Search for the query id in the active query map.
  {
    shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, true);
    if (exec_state != nullptr) {
      lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());
      if (exec_state->coord() != nullptr) {
        TExecProgress progress;
        {
          lock_guard<SpinLock> lock(exec_state->coord()->GetExecSummaryLock());
          *result = exec_state->coord()->exec_summary();

          // Update the current scan range progress for the summary.
          progress.__set_num_completed_scan_ranges(
              exec_state->coord()->progress().num_complete());
          progress.__set_total_scan_ranges(exec_state->coord()->progress().total());
        }
        result->__set_progress(progress);
        return Status::OK();
      }
    }
  }

  // Look for the query in completed query log.
  {
    lock_guard<mutex> l(query_log_lock_);
    QueryLogIndex::const_iterator query_record = query_log_index_.find(query_id);
    if (query_record == query_log_index_.end()) {
      stringstream ss;
      ss << "Query id " << PrintId(query_id) << " not found.";
      return Status(ss.str());
    }
    *result = query_record->second->exec_summary;
  }
  return Status::OK();
}

[[noreturn]] void ImpalaServer::LogFileFlushThread() {
  while (true) {
    sleep(5);
    profile_logger_->Flush();
  }
}

[[noreturn]] void ImpalaServer::AuditEventLoggerFlushThread() {
  while (true) {
    sleep(5);
    Status status = audit_event_logger_->Flush();
    if (!status.ok()) {
      LOG(ERROR) << "Error flushing audit event log: " << status.GetDetail();
      if (FLAGS_abort_on_failed_audit_event) {
        CLEAN_EXIT_WITH_ERROR("Shutting down Impala Server due to "
             "abort_on_failed_audit_event=true");
      }
    }
  }
}

[[noreturn]] void ImpalaServer::LineageLoggerFlushThread() {
  while (true) {
    sleep(5);
    Status status = lineage_logger_->Flush();
    if (!status.ok()) {
      LOG(ERROR) << "Error flushing lineage event log: " << status.GetDetail();
      if (FLAGS_abort_on_failed_lineage_event) {
        CLEAN_EXIT_WITH_ERROR("Shutting down Impala Server due to "
            "abort_on_failed_lineage_event=true");
      }
    }
  }
}

void ImpalaServer::ArchiveQuery(const QueryExecState& query) {
  const string& encoded_profile_str = query.profile().SerializeToArchiveString();

  // If there was an error initialising archival (e.g. directory is not writeable),
  // FLAGS_log_query_to_file will have been set to false
  if (FLAGS_log_query_to_file) {
    stringstream ss;
    ss << UnixMillis() << " " << query.query_id() << " " << encoded_profile_str;
    Status status = profile_logger_->AppendEntry(ss.str());
    if (!status.ok()) {
      LOG_EVERY_N(WARNING, 1000) << "Could not write to profile log file file ("
                                 << google::COUNTER << " attempts failed): "
                                 << status.GetDetail();
      LOG_EVERY_N(WARNING, 1000)
          << "Disable query logging with --log_query_to_file=false";
    }
  }

  if (FLAGS_query_log_size == 0) return;
  QueryStateRecord record(query, true, encoded_profile_str);
  if (query.coord() != nullptr) {
    lock_guard<SpinLock> lock(query.coord()->GetExecSummaryLock());
    record.exec_summary = query.coord()->exec_summary();
  }
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

void ImpalaServer::AddPoolQueryOptions(TQueryCtx* ctx,
    const QueryOptionsMask& override_options_mask) {
  // Errors are not returned and are only logged (at level 2) because some incoming
  // requests are not expected to be mapped to a pool and will not have query options,
  // e.g. 'use [db];'. For requests that do need to be mapped to a pool successfully, the
  // pool is resolved again during scheduling and errors are handled at that point.
  string resolved_pool;
  Status status = exec_env_->request_pool_service()->ResolveRequestPool(*ctx,
      &resolved_pool);
  if (!status.ok()) {
    VLOG_RPC << "Not adding pool query options for query=" << ctx->query_id
             << " ResolveRequestPool status: " << status.GetDetail();
    return;
  }

  TPoolConfig config;
  status = exec_env_->request_pool_service()->GetPoolConfig(resolved_pool, &config);
  if (!status.ok()) {
    VLOG_RPC << "Not adding pool query options for query=" << ctx->query_id
             << " GetConfigPool status: " << status.GetDetail();
    return;
  }

  TQueryOptions pool_options;
  QueryOptionsMask set_pool_options_mask;
  status = ParseQueryOptions(config.default_query_options, &pool_options,
      &set_pool_options_mask);
  if (!status.ok()) {
    VLOG_QUERY << "Ignoring errors while parsing default query options for pool="
               << resolved_pool << ", message: " << status.GetDetail();
  }

  QueryOptionsMask overlay_mask = override_options_mask & set_pool_options_mask;
  VLOG_RPC << "Parsed pool options: " << DebugQueryOptions(pool_options)
           << " override_options_mask=" << override_options_mask.to_string()
           << " set_pool_mask=" << set_pool_options_mask.to_string()
           << " overlay_mask=" << overlay_mask.to_string();
  OverlayQueryOptions(pool_options, overlay_mask, &ctx->client_request.query_options);
}

Status ImpalaServer::Execute(TQueryCtx* query_ctx,
    shared_ptr<SessionState> session_state,
    shared_ptr<QueryExecState>* exec_state) {
  PrepareQueryContext(query_ctx);
  ImpaladMetrics::IMPALA_SERVER_NUM_QUERIES->Increment(1L);

  // Redact the SQL stmt and update the query context
  string stmt = replace_all_copy(query_ctx->client_request.stmt, "\n", " ");
  Redact(&stmt);
  query_ctx->client_request.__set_redacted_stmt((const string) stmt);

  bool registered_exec_state;
  Status status = ExecuteInternal(*query_ctx, session_state, &registered_exec_state,
      exec_state);
  if (!status.ok() && registered_exec_state) {
    UnregisterQuery((*exec_state)->query_id(), false, &status);
  }
  return status;
}

Status ImpalaServer::ExecuteInternal(
    const TQueryCtx& query_ctx,
    shared_ptr<SessionState> session_state,
    bool* registered_exec_state,
    shared_ptr<QueryExecState>* exec_state) {
  DCHECK(session_state != nullptr);
  *registered_exec_state = false;

  exec_state->reset(new QueryExecState(query_ctx, exec_env_, exec_env_->frontend(),
      this, session_state));

  (*exec_state)->query_events()->MarkEvent("Query submitted");

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
        exec_env_->frontend()->GetExecRequest(query_ctx, &result)));
    (*exec_state)->query_events()->MarkEvent("Planning finished");
    (*exec_state)->summary_profile()->AddEventSequence(
        result.timeline.name, result.timeline);
    if (result.__isset.result_set_metadata) {
      (*exec_state)->set_result_metadata(result.result_set_metadata);
    }
  }
  VLOG(2) << "Execution request: " << ThriftDebugString(result);

  // start execution of query; also starts fragment status reports
  RETURN_IF_ERROR((*exec_state)->Exec(&result));
  if (result.stmt_type == TStmtType::DDL) {
    Status status = UpdateCatalogMetrics();
    if (!status.ok()) {
      VLOG_QUERY << "Couldn't update catalog metrics: " << status.GetDetail();
    }
  }

  if ((*exec_state)->coord() != nullptr) {
    const unordered_set<TNetworkAddress>& unique_hosts =
        (*exec_state)->schedule()->unique_hosts();
    if (!unique_hosts.empty()) {
      lock_guard<mutex> l(query_locations_lock_);
      for (const TNetworkAddress& port: unique_hosts) {
        query_locations_[port].insert((*exec_state)->query_id());
      }
    }
  }
  return Status::OK();
}

void ImpalaServer::PrepareQueryContext(TQueryCtx* query_ctx) {
  query_ctx->__set_pid(getpid());
  query_ctx->__set_now_string(TimestampValue::LocalTime().DebugString());
  query_ctx->__set_start_unix_millis(UnixMillis());
  query_ctx->__set_coord_address(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port));

  // Creating a random_generator every time is not free, but
  // benchmarks show it to be slightly cheaper than contending for a
  // single generator under a lock (since random_generator is not
  // thread-safe).
  query_ctx->query_id = UuidToQueryId(random_generator()());
}

Status ImpalaServer::RegisterQuery(shared_ptr<SessionState> session_state,
    const shared_ptr<QueryExecState>& exec_state) {
  lock_guard<mutex> l2(session_state->lock);
  // The session wasn't expired at the time it was checked out and it isn't allowed to
  // expire while checked out, so it must not be expired.
  DCHECK(session_state->ref_count > 0 && !session_state->expired);
  // The session may have been closed after it was checked out.
  if (session_state->closed) return Status("Session has been closed, ignoring query.");
  const TUniqueId& query_id = exec_state->query_id();
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);
    QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
    if (entry != query_exec_state_map_.end()) {
      // There shouldn't be an active query with that same id.
      // (query_id is globally unique)
      stringstream ss;
      ss << "query id " << PrintId(query_id) << " already exists";
      return Status(ErrorMsg(TErrorCode::INTERNAL_ERROR, ss.str()));
    }
    query_exec_state_map_.insert(make_pair(query_id, exec_state));
  }
  return Status::OK();
}

Status ImpalaServer::SetQueryInflight(shared_ptr<SessionState> session_state,
    const shared_ptr<QueryExecState>& exec_state) {
  const TUniqueId& query_id = exec_state->query_id();
  lock_guard<mutex> l(session_state->lock);
  // The session wasn't expired at the time it was checked out and it isn't allowed to
  // expire while checked out, so it must not be expired.
  DCHECK_GT(session_state->ref_count, 0);
  DCHECK(!session_state->expired);
  // The session may have been closed after it was checked out.
  if (session_state->closed) return Status("Session closed");
  // Add query to the set that will be unregistered if sesssion is closed.
  session_state->inflight_queries.insert(query_id);
  ++session_state->total_queries;
  // Set query expiration.
  int32_t timeout_s = exec_state->query_options().query_timeout_s;
  if (FLAGS_idle_query_timeout > 0 && timeout_s > 0) {
    timeout_s = min(FLAGS_idle_query_timeout, timeout_s);
  } else {
    // Use a non-zero timeout, if one exists
    timeout_s = max(FLAGS_idle_query_timeout, timeout_s);
  }
  if (timeout_s > 0) {
    lock_guard<mutex> l2(query_expiration_lock_);
    VLOG_QUERY << "Query " << PrintId(query_id) << " has timeout of "
               << PrettyPrinter::Print(timeout_s * 1000L * 1000L * 1000L,
                     TUnit::TIME_NS);
    queries_by_timestamp_.insert(
        make_pair(UnixMillis() + (1000L * timeout_s), query_id));
  }
  return Status::OK();
}

Status ImpalaServer::UnregisterQuery(const TUniqueId& query_id, bool check_inflight,
    const Status* cause) {
  VLOG_QUERY << "UnregisterQuery(): query_id=" << query_id;

  RETURN_IF_ERROR(CancelInternal(query_id, check_inflight, cause));

  shared_ptr<QueryExecState> exec_state;
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);
    QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
    if (entry == query_exec_state_map_.end()) {
      return Status("Invalid or unknown query handle");
    } else {
      exec_state = entry->second;
    }
    query_exec_state_map_.erase(entry);
  }

  exec_state->Done();

  double ut_end_time, ut_start_time;
  double duration_ms = 0.0;
  if (LIKELY(exec_state->end_time().ToSubsecondUnixTime(&ut_end_time))
      && LIKELY(exec_state->start_time().ToSubsecondUnixTime(&ut_start_time))) {
    duration_ms = 1000 * (ut_end_time - ut_start_time);
  }

  // duration_ms can be negative when the local timezone changes during query execution.
  if (duration_ms >= 0) {
    if (exec_state->stmt_type() == TStmtType::DDL) {
      ImpaladMetrics::DDL_DURATIONS->Update(duration_ms);
    } else {
      ImpaladMetrics::QUERY_DURATIONS->Update(duration_ms);
    }
  }
  LogQueryEvents(*exec_state.get());

  {
    lock_guard<mutex> l(exec_state->session()->lock);
    exec_state->session()->inflight_queries.erase(query_id);
  }

  if (exec_state->coord() != nullptr) {
    string exec_summary;
    {
      lock_guard<SpinLock> lock(exec_state->coord()->GetExecSummaryLock());
      const TExecSummary& summary = exec_state->coord()->exec_summary();
      exec_summary = PrintExecSummary(summary);
    }
    exec_state->summary_profile()->AddInfoString("ExecSummary", exec_summary);
    exec_state->summary_profile()->AddInfoString("Errors",
        exec_state->coord()->GetErrorLog());

    const unordered_set<TNetworkAddress>& unique_hosts =
        exec_state->schedule()->unique_hosts();
    if (!unique_hosts.empty()) {
      lock_guard<mutex> l(query_locations_lock_);
      for (const TNetworkAddress& hostport: unique_hosts) {
        // Query may have been removed already by cancellation path. In particular, if
        // node to fail was last sender to an exchange, the coordinator will realise and
        // fail the query at the same time the failure detection path does the same
        // thing. They will harmlessly race to remove the query from this map.
        QueryLocations::iterator it = query_locations_.find(hostport);
        if (it != query_locations_.end()) {
          it->second.erase(exec_state->query_id());
        }
      }
    }
  }
  ArchiveQuery(*exec_state);
  return Status::OK();
}

Status ImpalaServer::UpdateCatalogMetrics() {
  TGetDbsResult dbs;
  RETURN_IF_ERROR(exec_env_->frontend()->GetDbs(nullptr, nullptr, &dbs));
  ImpaladMetrics::CATALOG_NUM_DBS->set_value(dbs.dbs.size());
  ImpaladMetrics::CATALOG_NUM_TABLES->set_value(0L);
  for (const TDatabase& db: dbs.dbs) {
    TGetTablesResult table_names;
    RETURN_IF_ERROR(exec_env_->frontend()->GetTableNames(db.db_name, nullptr, nullptr,
        &table_names));
    ImpaladMetrics::CATALOG_NUM_TABLES->Increment(table_names.tables.size());
  }

  return Status::OK();
}

Status ImpalaServer::CancelInternal(const TUniqueId& query_id, bool check_inflight,
    const Status* cause) {
  VLOG_QUERY << "Cancel(): query_id=" << PrintId(query_id);
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state == nullptr) return Status("Invalid or unknown query handle");
  exec_state->Cancel(check_inflight, cause);
  return Status::OK();
}

Status ImpalaServer::CloseSessionInternal(const TUniqueId& session_id,
    bool ignore_if_absent) {
  // Find the session_state and remove it from the map.
  shared_ptr<SessionState> session_state;
  {
    lock_guard<mutex> l(session_state_map_lock_);
    SessionStateMap::iterator entry = session_state_map_.find(session_id);
    if (entry == session_state_map_.end()) {
      if (ignore_if_absent) {
        return Status::OK();
      } else {
        return Status("Invalid session ID");
      }
    }
    session_state = entry->second;
    session_state_map_.erase(session_id);
  }
  DCHECK(session_state != nullptr);
  if (session_state->session_type == TSessionType::BEESWAX) {
    ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS->Increment(-1L);
  } else {
    ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS->Increment(-1L);
  }
  unordered_set<TUniqueId> inflight_queries;
  {
    lock_guard<mutex> l(session_state->lock);
    DCHECK(!session_state->closed);
    session_state->closed = true;
    // Since closed is true, no more queries will be added to the inflight list.
    inflight_queries.insert(session_state->inflight_queries.begin(),
        session_state->inflight_queries.end());
  }
  // Unregister all open queries from this session.
  Status status("Session closed");
  for (const TUniqueId& query_id: inflight_queries) {
    UnregisterQuery(query_id, false, &status);
  }
  // Reconfigure the poll period of session_timeout_thread_ if necessary.
  int32_t session_timeout = session_state->session_timeout;
  if (session_timeout > 0) {
    lock_guard<mutex> l(session_timeout_lock_);
    multiset<int32_t>::const_iterator itr = session_timeout_set_.find(session_timeout);
    DCHECK(itr != session_timeout_set_.end());
    session_timeout_set_.erase(itr);
    session_timeout_cv_.notify_one();
  }
  return Status::OK();
}

Status ImpalaServer::GetSessionState(const TUniqueId& session_id,
    shared_ptr<SessionState>* session_state, bool mark_active) {
  lock_guard<mutex> l(session_state_map_lock_);
  SessionStateMap::iterator i = session_state_map_.find(session_id);
  if (i == session_state_map_.end()) {
    *session_state = std::shared_ptr<SessionState>();
    return Status("Invalid session id");
  } else {
    if (mark_active) {
      lock_guard<mutex> session_lock(i->second->lock);
      if (i->second->expired) {
        stringstream ss;
        ss << "Client session expired due to more than " << i->second->session_timeout
           << "s of inactivity (last activity was at: "
           << TimestampValue(i->second->last_accessed_ms / 1000).DebugString() << ").";
        return Status(ss.str());
      }
      if (i->second->closed) return Status("Session is closed");
      ++i->second->ref_count;
    }
    *session_state = i->second;
    return Status::OK();
  }
}

void ImpalaServer::ReportExecStatus(
    TReportExecStatusResult& return_val, const TReportExecStatusParams& params) {
  VLOG_FILE << "ReportExecStatus()"
            << " instance_id=" << PrintId(params.fragment_instance_id)
            << " done=" << (params.done ? "true" : "false");
  // TODO: implement something more efficient here, we're currently
  // acquiring/releasing the map lock and doing a map lookup for
  // every report (assign each query a local int32_t id and use that to index into a
  // vector of QueryExecStates, w/o lookup or locking?)
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(params.query_id, false);
  if (exec_state.get() == nullptr) {
    // This is expected occasionally (since a report RPC might be in flight while
    // cancellation is happening). Return an error to the caller to get it to stop.
    const string& err = Substitute("ReportExecStatus(): Received report for unknown "
        "query ID (probably closed or cancelled). (instance: $0 done: $1)",
        PrintId(params.fragment_instance_id), params.done);
    Status(TErrorCode::INTERNAL_ERROR, err).SetTStatus(&return_val);
    VLOG_QUERY << err;
    return;
  }
  exec_state->coord()->UpdateFragmentExecStatus(params).SetTStatus(&return_val);
}

void ImpalaServer::TransmitData(
    TTransmitDataResult& return_val, const TTransmitDataParams& params) {
  VLOG_ROW << "TransmitData(): instance_id=" << params.dest_fragment_instance_id
           << " node_id=" << params.dest_node_id
           << " #rows=" << params.row_batch.num_rows
           << " sender_id=" << params.sender_id
           << " eos=" << (params.eos ? "true" : "false");
  // TODO: fix Thrift so we can simply take ownership of thrift_batch instead
  // of having to copy its data
  if (params.row_batch.num_rows > 0) {
    Status status = exec_env_->stream_mgr()->AddData(
        params.dest_fragment_instance_id, params.dest_node_id, params.row_batch,
        params.sender_id);
    status.SetTStatus(&return_val);
    if (!status.ok()) {
      // should we close the channel here as well?
      return;
    }
  }

  if (params.eos) {
    exec_env_->stream_mgr()->CloseSender(
        params.dest_fragment_instance_id, params.dest_node_id,
        params.sender_id).SetTStatus(&return_val);
  }
}

void ImpalaServer::InitializeConfigVariables() {
  QueryOptionsMask set_query_options; // unused
  Status status = ParseQueryOptions(FLAGS_default_query_options,
      &default_query_options_, &set_query_options);
  if (!status.ok()) {
    // Log error and exit if the default query options are invalid.
    CLEAN_EXIT_WITH_ERROR(Substitute("Invalid default query options. Please check "
        "-default_query_options.\n $0", status.GetDetail()));
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

void ImpalaServer::SessionState::ToThrift(const TUniqueId& session_id,
    TSessionState* state) {
  lock_guard<mutex> l(lock);
  state->session_id = session_id;
  state->session_type = session_type;
  state->database = database;
  state->connected_user = connected_user;
  // The do_as_user will only be set if delegation is enabled and the
  // proxy user is authorized to delegate as this user.
  if (!do_as_user.empty()) state->__set_delegated_user(do_as_user);
  state->network_address = network_address;
  state->__set_kudu_latest_observed_ts(kudu_latest_observed_ts);
}

void ImpalaServer::CancelFromThreadPool(uint32_t thread_id,
    const CancellationWork& cancellation_work) {
  if (cancellation_work.unregister()) {
    Status status = UnregisterQuery(cancellation_work.query_id(), true,
        &cancellation_work.cause());
    if (!status.ok()) {
      VLOG_QUERY << "Query de-registration (" << cancellation_work.query_id()
                 << ") failed";
    }
  } else {
    Status status = CancelInternal(cancellation_work.query_id(), true,
        &cancellation_work.cause());
    if (!status.ok()) {
      VLOG_QUERY << "Query cancellation (" << cancellation_work.query_id()
                 << ") did not succeed: " << status.GetDetail();
    }
  }
}

Status ImpalaServer::AuthorizeProxyUser(const string& user, const string& do_as_user) {
  if (user.empty()) {
    return Status("Unable to delegate using empty proxy username.");
  } else if (do_as_user.empty()) {
    return Status("Unable to delegate using empty doAs username.");
  }

  stringstream error_msg;
  error_msg << "User '" << user << "' is not authorized to delegate to '"
            << do_as_user << "'.";
  if (authorized_proxy_user_config_.size() == 0) {
    error_msg << " User delegation is disabled.";
    return Status(error_msg.str());
  }

  // Get the short version of the user name (the user name up to the first '/' or '@')
  // from the full principal name.
  size_t end_idx = min(user.find("/"), user.find("@"));
  // If neither are found (or are found at the beginning of the user name),
  // return the username. Otherwise, return the username up to the matching character.
  string short_user(
      end_idx == string::npos || end_idx == 0 ? user : user.substr(0, end_idx));

  // Check if the proxy user exists. If he/she does, then check if they are allowed
  // to delegate to the do_as_user.
  ProxyUserMap::const_iterator proxy_user =
      authorized_proxy_user_config_.find(short_user);
  if (proxy_user != authorized_proxy_user_config_.end()) {
    for (const string& user: proxy_user->second) {
      if (user == "*" || user == do_as_user) return Status::OK();
    }
  }
  return Status(error_msg.str());
}

void ImpalaServer::CatalogUpdateCallback(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(CatalogServer::IMPALA_CATALOG_TOPIC);
  if (topic == incoming_topic_deltas.end()) return;
  const TTopicDelta& delta = topic->second;

  // Update catalog cache in frontend. An update is split into batches of size
  // MAX_CATALOG_UPDATE_BATCH_SIZE_BYTES each for multiple updates. IMPALA-3499
  if (delta.topic_entries.size() != 0 || delta.topic_deletions.size() != 0)  {
    vector<TUpdateCatalogCacheRequest> update_reqs;
    update_reqs.push_back(TUpdateCatalogCacheRequest());
    TUpdateCatalogCacheRequest* incremental_request = &update_reqs.back();
    incremental_request->__set_is_delta(delta.is_delta);
    // Process all Catalog updates (new and modified objects) and determine what the
    // new catalog version will be.
    int64_t new_catalog_version = catalog_update_info_.catalog_version;
    uint64_t batch_size_bytes = 0;
    for (const TTopicItem& item: delta.topic_entries) {
      uint32_t len = item.value.size();
      TCatalogObject catalog_object;
      Status status = DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, FLAGS_compact_catalog_topic, &catalog_object);
      if (!status.ok()) {
        LOG(ERROR) << "Error deserializing item: " << status.GetDetail();
        continue;
      }
      if (len > 100 * 1024 * 1024 /* 100MB */) {
        LOG(INFO) << "Received large catalog update(>100mb): "
                     << item.key << " is "
                     << PrettyPrinter::Print(len, TUnit::BYTES);
      }
      if (catalog_object.type == TCatalogObjectType::CATALOG) {
        incremental_request->__set_catalog_service_id(
            catalog_object.catalog.catalog_service_id);
        new_catalog_version = catalog_object.catalog_version;
      }

      // Refresh the lib cache entries of any added functions and data sources
      // TODO: if frontend returns the list of functions and data sources, we do not
      // need to deserialize these in backend.
      if (catalog_object.type == TCatalogObjectType::FUNCTION) {
        DCHECK(catalog_object.__isset.fn);
        LibCache::instance()->SetNeedsRefresh(catalog_object.fn.hdfs_location);
      }
      if (catalog_object.type == TCatalogObjectType::DATA_SOURCE) {
        DCHECK(catalog_object.__isset.data_source);
        LibCache::instance()->SetNeedsRefresh(catalog_object.data_source.hdfs_location);
      }

      if (batch_size_bytes + len > MAX_CATALOG_UPDATE_BATCH_SIZE_BYTES) {
        update_reqs.push_back(TUpdateCatalogCacheRequest());
        incremental_request = &update_reqs.back();
        batch_size_bytes = 0;
      }
      incremental_request->updated_objects.push_back(catalog_object);
      batch_size_bytes += len;
    }
    update_reqs.push_back(TUpdateCatalogCacheRequest());
    TUpdateCatalogCacheRequest* deletion_request = &update_reqs.back();

    // We need to look up the dropped functions and data sources and remove them
    // from the library cache. The data sent from the catalog service does not
    // contain all the function metadata so we'll ask our local frontend for it. We
    // need to do this before updating the catalog.
    vector<TCatalogObject> dropped_objects;

    // Process all Catalog deletions (dropped objects). We only know the keys (object
    // names) so must parse each key to determine the TCatalogObject.
    for (const string& key: delta.topic_deletions) {
      LOG(INFO) << "Catalog topic entry deletion: " << key;
      TCatalogObject catalog_object;
      Status status = TCatalogObjectFromEntryKey(key, &catalog_object);
      if (!status.ok()) {
        LOG(ERROR) << "Error parsing catalog topic entry deletion key: " << key << " "
                   << "Error: " << status.GetDetail();
        continue;
      }
      deletion_request->removed_objects.push_back(catalog_object);
      if (catalog_object.type == TCatalogObjectType::FUNCTION ||
          catalog_object.type == TCatalogObjectType::DATA_SOURCE) {
        TCatalogObject dropped_object;
        if (exec_env_->frontend()->GetCatalogObject(
                catalog_object, &dropped_object).ok()) {
          // This object may have been dropped and re-created. To avoid removing the
          // re-created object's entry from the cache verify the existing object has a
          // catalog version <= the catalog version included in this statestore heartbeat.
          if (dropped_object.catalog_version <= new_catalog_version) {
            if (catalog_object.type == TCatalogObjectType::FUNCTION ||
                catalog_object.type == TCatalogObjectType::DATA_SOURCE) {
              dropped_objects.push_back(dropped_object);
            }
          }
        }
        // Nothing to do in error case.
      }
    }

    // Call the FE to apply the changes to the Impalad Catalog.
    TUpdateCatalogCacheResponse resp;
    Status s = exec_env_->frontend()->UpdateCatalogCache(update_reqs, &resp);
    if (!s.ok()) {
      LOG(ERROR) << "There was an error processing the impalad catalog update. Requesting"
                 << " a full topic update to recover: " << s.GetDetail();
      subscriber_topic_updates->push_back(TTopicDelta());
      TTopicDelta& update = subscriber_topic_updates->back();
      update.topic_name = CatalogServer::IMPALA_CATALOG_TOPIC;
      update.__set_from_version(0L);
      ImpaladMetrics::CATALOG_READY->set_value(false);
      // Dropped all cached lib files (this behaves as if all functions and data
      // sources are dropped).
      LibCache::instance()->DropCache();
    } else {
      {
        unique_lock<mutex> unique_lock(catalog_version_lock_);
        catalog_update_info_.catalog_version = new_catalog_version;
        catalog_update_info_.catalog_topic_version = delta.to_version;
        catalog_update_info_.catalog_service_id = resp.catalog_service_id;
      }
      ImpaladMetrics::CATALOG_READY->set_value(new_catalog_version > 0);
      UpdateCatalogMetrics();
      // Remove all dropped objects from the library cache.
      // TODO: is this expensive? We'd like to process heartbeats promptly.
      for (TCatalogObject& object: dropped_objects) {
        if (object.type == TCatalogObjectType::FUNCTION) {
          LibCache::instance()->RemoveEntry(object.fn.hdfs_location);
        } else if (object.type == TCatalogObjectType::DATA_SOURCE) {
          LibCache::instance()->RemoveEntry(object.data_source.hdfs_location);
        } else {
          DCHECK(false);
        }
      }
    }
  }

  // Always update the minimum subscriber version for the catalog topic.
  {
    unique_lock<mutex> unique_lock(catalog_version_lock_);
    min_subscriber_catalog_topic_version_ = delta.min_subscriber_topic_version;
  }
  catalog_version_update_cv_.notify_all();
}

Status ImpalaServer::ProcessCatalogUpdateResult(
    const TCatalogUpdateResult& catalog_update_result, bool wait_for_all_subscribers) {
  // If this update result contains catalog objects to add or remove, directly apply the
  // updates to the local impalad's catalog cache. Otherwise, wait for a statestore
  // heartbeat that contains this update version.
  if (catalog_update_result.__isset.updated_catalog_object_DEPRECATED ||
      catalog_update_result.__isset.removed_catalog_object_DEPRECATED ||
      catalog_update_result.__isset.updated_catalog_objects ||
      catalog_update_result.__isset.removed_catalog_objects) {
    TUpdateCatalogCacheRequest update_req;
    update_req.__set_is_delta(true);
    update_req.__set_catalog_service_id(catalog_update_result.catalog_service_id);

    // Check that the response either exclusively uses the single updated/removed field
    // or the corresponding list versions of the fields, but not a mix.
    // The non-list version of the fields are maintained for backwards compatibility,
    // e.g., BDR relies on a stable catalog API.
    if ((catalog_update_result.__isset.updated_catalog_object_DEPRECATED ||
         catalog_update_result.__isset.removed_catalog_object_DEPRECATED)
        &&
        (catalog_update_result.__isset.updated_catalog_objects ||
         catalog_update_result.__isset.removed_catalog_objects)) {
      stringstream err;
      err << "Failed to process malformed catalog update response:\n"
          << "__isset.updated_catalog_object_DEPRECATED="
          << catalog_update_result.__isset.updated_catalog_object_DEPRECATED << "\n"
          << "__isset.removed_catalog_object_DEPRECATED="
          << catalog_update_result.__isset.updated_catalog_object_DEPRECATED << "\n"
          << "__isset.updated_catalog_objects="
          << catalog_update_result.__isset.updated_catalog_objects << "\n"
          << "__isset.removed_catalog_objects="
          << catalog_update_result.__isset.removed_catalog_objects;
      return Status(TErrorCode::INTERNAL_ERROR, err.str());
    }

    if (catalog_update_result.__isset.updated_catalog_object_DEPRECATED) {
      update_req.updated_objects.push_back(
          catalog_update_result.updated_catalog_object_DEPRECATED);
    }
    if (catalog_update_result.__isset.removed_catalog_object_DEPRECATED) {
      update_req.removed_objects.push_back(
          catalog_update_result.removed_catalog_object_DEPRECATED);
    }
    if (catalog_update_result.__isset.updated_catalog_objects) {
      update_req.__set_updated_objects(catalog_update_result.updated_catalog_objects);
    }
    if (catalog_update_result.__isset.removed_catalog_objects) {
      update_req.__set_removed_objects(catalog_update_result.removed_catalog_objects);
    }

     // Apply the changes to the local catalog cache.
    TUpdateCatalogCacheResponse resp;
    Status status = exec_env_->frontend()->UpdateCatalogCache(
        vector<TUpdateCatalogCacheRequest>{update_req}, &resp);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    RETURN_IF_ERROR(status);
    if (!wait_for_all_subscribers) return Status::OK();
  }

  unique_lock<mutex> unique_lock(catalog_version_lock_);
  int64_t min_req_catalog_version = catalog_update_result.version;
  const TUniqueId& catalog_service_id = catalog_update_result.catalog_service_id;

  // Wait for the update to be processed locally.
  // TODO: What about query cancellation?
  VLOG_QUERY << "Waiting for catalog version: " << min_req_catalog_version
             << " current version: " << catalog_update_info_.catalog_version;
  while (catalog_update_info_.catalog_version < min_req_catalog_version &&
         catalog_update_info_.catalog_service_id == catalog_service_id) {
    catalog_version_update_cv_.wait(unique_lock);
  }

  if (!wait_for_all_subscribers) return Status::OK();

  // Now wait for this update to be propagated to all catalog topic subscribers.
  // If we make it here it implies the first condition was met (the update was processed
  // locally or the catalog service id has changed).
  int64_t min_req_subscriber_topic_version = catalog_update_info_.catalog_topic_version;

  VLOG_QUERY << "Waiting for min subscriber topic version: "
             << min_req_subscriber_topic_version << " current version: "
             << min_subscriber_catalog_topic_version_;
  while (min_subscriber_catalog_topic_version_ < min_req_subscriber_topic_version &&
         catalog_update_info_.catalog_service_id == catalog_service_id) {
    catalog_version_update_cv_.wait(unique_lock);
  }
  return Status::OK();
}

void ImpalaServer::MembershipCallback(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  // TODO: Consider rate-limiting this. In the short term, best to have
  // statestore heartbeat less frequently.
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(Scheduler::IMPALA_MEMBERSHIP_TOPIC);

  if (topic != incoming_topic_deltas.end()) {
    const TTopicDelta& delta = topic->second;
    // If this is not a delta, the update should include all entries in the topic so
    // clear the saved mapping of known backends.
    if (!delta.is_delta) known_backends_.clear();

    // Process membership additions.
    for (const TTopicItem& item: delta.topic_entries) {
      uint32_t len = item.value.size();
      TBackendDescriptor backend_descriptor;
      Status status = DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &backend_descriptor);
      if (!status.ok()) {
        VLOG(2) << "Error deserializing topic item with key: " << item.key;
        continue;
      }
      // This is a new item - add it to the map of known backends.
      known_backends_.insert(make_pair(item.key, backend_descriptor));
    }

    // Register the local backend in the statestore and update the list of known backends.
    AddLocalBackendToStatestore(subscriber_topic_updates);

    // Process membership deletions.
    for (const string& backend_id: delta.topic_deletions) {
      known_backends_.erase(backend_id);
    }

    // Create a set of known backend network addresses. Used to test for cluster
    // membership by network address.
    set<TNetworkAddress> current_membership;
    // Also reflect changes to the frontend. Initialized only if any_changes is true.
    TUpdateMembershipRequest update_req;
    bool any_changes = !delta.topic_entries.empty() || !delta.topic_deletions.empty() ||
        !delta.is_delta;
    for (const BackendDescriptorMap::value_type& backend: known_backends_) {
      current_membership.insert(backend.second.address);
      if (any_changes) {
        update_req.hostnames.insert(backend.second.address.hostname);
        update_req.ip_addresses.insert(backend.second.ip_address);
      }
    }
    if (any_changes) {
      update_req.num_nodes = known_backends_.size();
      Status status = exec_env_->frontend()->UpdateMembership(update_req);
      if (!status.ok()) {
        LOG(WARNING) << "Error updating frontend membership snapshot: "
                     << status.GetDetail();
      }
    }

    // Maps from query id (to be cancelled) to a list of failed Impalads that are
    // the cause of the cancellation.
    map<TUniqueId, vector<TNetworkAddress>> queries_to_cancel;
    {
      // Build a list of queries that are running on failed hosts (as evidenced by their
      // absence from the membership list).
      // TODO: crash-restart failures can give false negatives for failed Impala demons.
      lock_guard<mutex> l(query_locations_lock_);
      QueryLocations::const_iterator loc_entry = query_locations_.begin();
      while (loc_entry != query_locations_.end()) {
        if (current_membership.find(loc_entry->first) == current_membership.end()) {
          unordered_set<TUniqueId>::const_iterator query_id = loc_entry->second.begin();
          // Add failed backend locations to all queries that ran on that backend.
          for(; query_id != loc_entry->second.end(); ++query_id) {
            vector<TNetworkAddress>& failed_hosts = queries_to_cancel[*query_id];
            failed_hosts.push_back(loc_entry->first);
          }
          exec_env_->impalad_client_cache()->CloseConnections(loc_entry->first);
          // We can remove the location wholesale once we know backend's failed. To do so
          // safely during iteration, we have to be careful not in invalidate the current
          // iterator, so copy the iterator to do the erase(..) and advance the original.
          QueryLocations::const_iterator failed_backend = loc_entry;
          ++loc_entry;
          query_locations_.erase(failed_backend);
        } else {
          ++loc_entry;
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
      map<TUniqueId, vector<TNetworkAddress>>::iterator cancellation_entry;
      for (cancellation_entry = queries_to_cancel.begin();
          cancellation_entry != queries_to_cancel.end();
          ++cancellation_entry) {
        stringstream cause_msg;
        cause_msg << "Cancelled due to unreachable impalad(s): ";
        for (int i = 0; i < cancellation_entry->second.size(); ++i) {
          cause_msg << cancellation_entry->second[i];
          if (i + 1 != cancellation_entry->second.size()) cause_msg << ", ";
        }
        cancellation_thread_pool_->Offer(
            CancellationWork(cancellation_entry->first, Status(cause_msg.str()), false));
      }
    }
  }
}

void ImpalaServer::AddLocalBackendToStatestore(
    vector<TTopicDelta>* subscriber_topic_updates) {
  const string& local_backend_id = exec_env_->subscriber()->id();
  if (known_backends_.find(local_backend_id) != known_backends_.end()) return;

  TBackendDescriptor local_backend_descriptor;
  local_backend_descriptor.__set_address(
      MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port));
  IpAddr ip;
  const Hostname& hostname = local_backend_descriptor.address.hostname;
  Status status = HostnameToIpAddr(hostname, &ip);
  if (!status.ok()) {
    // TODO: Should we do something about this failure?
    LOG(WARNING) << "Failed to convert hostname " << hostname << " to IP address: "
                 << status.GetDetail();
    return;
  }
  local_backend_descriptor.ip_address = ip;
  subscriber_topic_updates->emplace_back(TTopicDelta());
  TTopicDelta& update = subscriber_topic_updates->back();
  update.topic_name = Scheduler::IMPALA_MEMBERSHIP_TOPIC;
  update.topic_entries.emplace_back(TTopicItem());

  TTopicItem& item = update.topic_entries.back();
  item.key = local_backend_id;
  status = thrift_serializer_.Serialize(&local_backend_descriptor, &item.value);
  if (!status.ok()) {
    LOG(WARNING) << "Failed to serialize Impala backend descriptor for statestore topic:"
                 << " " << status.GetDetail();
    subscriber_topic_updates->pop_back();
  } else {
    known_backends_.insert(make_pair(item.key, local_backend_descriptor));
  }
}

ImpalaServer::QueryStateRecord::QueryStateRecord(const QueryExecState& exec_state,
    bool copy_profile, const string& encoded_profile) {
  id = exec_state.query_id();
  const TExecRequest& request = exec_state.exec_request();

  const string* plan_str = exec_state.summary_profile().GetInfoString("Plan");
  if (plan_str != nullptr) plan = *plan_str;
  stmt = exec_state.sql_stmt();
  stmt_type = request.stmt_type;
  effective_user = exec_state.effective_user();
  default_db = exec_state.default_db();
  start_time = exec_state.start_time();
  end_time = exec_state.end_time();
  has_coord = false;

  Coordinator* coord = exec_state.coord();
  if (coord != nullptr) {
    num_complete_fragments = coord->progress().num_complete();
    total_fragments = coord->progress().total();
    has_coord = true;
  }
  query_state = exec_state.query_state();
  num_rows_fetched = exec_state.num_rows_fetched();
  query_status = exec_state.query_status();

  exec_state.query_events()->ToThrift(&event_sequence);

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

  // Save the query fragments so that the plan can be visualised.
  for (const TPlanExecInfo& plan_exec_info:
      exec_state.exec_request().query_exec_request.plan_exec_info) {
    fragments.insert(fragments.end(),
        plan_exec_info.fragments.begin(), plan_exec_info.fragments.end());
  }
  all_rows_returned = exec_state.eos();
  last_active_time_ms = exec_state.last_active_ms();
  request_pool = exec_state.request_pool();
}

bool ImpalaServer::QueryStateRecordLessThan::operator() (
    const QueryStateRecord& lhs, const QueryStateRecord& rhs) const {
  if (lhs.start_time == rhs.start_time) return lhs.id < rhs.id;
  return lhs.start_time < rhs.start_time;
}

void ImpalaServer::ConnectionStart(
    const ThriftServer::ConnectionContext& connection_context) {
  if (connection_context.server_name == BEESWAX_SERVER_NAME) {
    // Beeswax only allows for one session per connection, so we can share the session ID
    // with the connection ID
    const TUniqueId& session_id = connection_context.connection_id;
    shared_ptr<SessionState> session_state;
    session_state.reset(new SessionState);
    session_state->closed = false;
    session_state->start_time_ms = UnixMillis();
    session_state->last_accessed_ms = UnixMillis();
    session_state->database = "default";
    session_state->session_timeout = FLAGS_idle_session_timeout;
    session_state->session_type = TSessionType::BEESWAX;
    session_state->network_address = connection_context.network_address;
    session_state->default_query_options = default_query_options_;
    session_state->kudu_latest_observed_ts = 0;

    // If the username was set by a lower-level transport, use it.
    if (!connection_context.username.empty()) {
      session_state->connected_user = connection_context.username;
    }
    RegisterSessionTimeout(session_state->session_timeout);

    {
      lock_guard<mutex> l(session_state_map_lock_);
      bool success =
          session_state_map_.insert(make_pair(session_id, session_state)).second;
      // The session should not have already existed.
      DCHECK(success);
    }
    {
      lock_guard<mutex> l(connection_to_sessions_map_lock_);
      connection_to_sessions_map_[connection_context.connection_id].push_back(session_id);
    }
    ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS->Increment(1L);
  }
}

void ImpalaServer::ConnectionEnd(
    const ThriftServer::ConnectionContext& connection_context) {

  vector<TUniqueId> sessions_to_close;
  {
    unique_lock<mutex> l(connection_to_sessions_map_lock_);
    ConnectionToSessionMap::iterator it =
        connection_to_sessions_map_.find(connection_context.connection_id);

    // Not every connection must have an associated session
    if (it == connection_to_sessions_map_.end()) return;

    // We don't expect a large number of sessions per connection, so we copy it, so that
    // we can drop the map lock early.
    sessions_to_close = it->second;
    connection_to_sessions_map_.erase(it);
  }

  LOG(INFO) << "Connection from client " << connection_context.network_address
            << " closed, closing " << sessions_to_close.size() << " associated session(s)";

  for (const TUniqueId& session_id: sessions_to_close) {
    Status status = CloseSessionInternal(session_id, true);
    if (!status.ok()) {
      LOG(WARNING) << "Error closing session " << session_id << ": "
                   << status.GetDetail();
    }
  }
}

void ImpalaServer::RegisterSessionTimeout(int32_t session_timeout) {
  if (session_timeout <= 0) return;
  lock_guard<mutex> l(session_timeout_lock_);
  session_timeout_set_.insert(session_timeout);
  session_timeout_cv_.notify_one();
}

[[noreturn]] void ImpalaServer::ExpireSessions() {
  while (true) {
    {
      unique_lock<mutex> timeout_lock(session_timeout_lock_);
      if (session_timeout_set_.empty()) {
        session_timeout_cv_.wait(timeout_lock);
      } else {
        // Sleep for half the minimum session timeout; the maximum delay between a session
        // expiring and this method picking it up is equal to the size of this sleep.
        int64_t session_timeout_min_ms = *session_timeout_set_.begin() * 1000 / 2;
        system_time deadline = get_system_time() + milliseconds(session_timeout_min_ms);
        session_timeout_cv_.timed_wait(timeout_lock, deadline);
      }
    }

    lock_guard<mutex> map_lock(session_state_map_lock_);
    int64_t now = UnixMillis();
    VLOG(3) << "Session expiration thread waking up";
    // TODO: If holding session_state_map_lock_ for the duration of this loop is too
    // expensive, consider a priority queue.
    for (SessionStateMap::value_type& session_state: session_state_map_) {
      unordered_set<TUniqueId> inflight_queries;
      {
        lock_guard<mutex> state_lock(session_state.second->lock);
        if (session_state.second->ref_count > 0) continue;
        // A session closed by other means is in the process of being removed, and it's
        // best not to interfere.
        if (session_state.second->closed || session_state.second->expired) continue;
        if (session_state.second->session_timeout == 0) continue;

        int64_t last_accessed_ms = session_state.second->last_accessed_ms;
        int64_t session_timeout_ms = session_state.second->session_timeout * 1000;
        if (now - last_accessed_ms <= session_timeout_ms) continue;
        LOG(INFO) << "Expiring session: " << session_state.first << ", user:"
                  << session_state.second->connected_user << ", last active: "
                  << TimestampValue(last_accessed_ms / 1000).DebugString();
        session_state.second->expired = true;
        ImpaladMetrics::NUM_SESSIONS_EXPIRED->Increment(1L);
        // Since expired is true, no more queries will be added to the inflight list.
        inflight_queries.insert(session_state.second->inflight_queries.begin(),
            session_state.second->inflight_queries.end());
      }
      // Unregister all open queries from this session.
      Status status("Session expired due to inactivity");
      for (const TUniqueId& query_id: inflight_queries) {
        cancellation_thread_pool_->Offer(CancellationWork(query_id, status, true));
      }
    }
  }
}

[[noreturn]] void ImpalaServer::ExpireQueries() {
  while (true) {
    // The following block accomplishes three things:
    //
    // 1. Update the ordered list of queries by checking the 'idle_time' parameter in
    // query_exec_state. We are able to avoid doing this for *every* query in flight
    // thanks to the observation that expiry times never move backwards, only
    // forwards. Therefore once we find a query that a) hasn't changed its idle time and
    // b) has not yet expired we can stop moving through the list. If the idle time has
    // changed, we need to re-insert the query in the right place in queries_by_timestamp_
    //
    // 2. Remove any queries that would have expired but have already been closed for any
    // reason.
    //
    // 3. Compute the next time a query *might* expire, so that the sleep at the end of
    // this loop has an accurate duration to wait. If the list of queries is empty, the
    // default sleep duration is half the idle query timeout.
    int64_t now;
    {
      lock_guard<mutex> l(query_expiration_lock_);
      ExpirationQueue::iterator expiration_event = queries_by_timestamp_.begin();
      now = UnixMillis();
      while (expiration_event != queries_by_timestamp_.end()) {
        // If the last-observed expiration time for this query is still in the future, we
        // know that the true expiration time will be at least that far off. So we can
        // break here and sleep.
        if (expiration_event->first > now) break;
        shared_ptr<QueryExecState> query_state =
            GetQueryExecState(expiration_event->second, false);
        if (query_state.get() == nullptr) {
          // Query was deleted some other way.
          queries_by_timestamp_.erase(expiration_event++);
          continue;
        }
        // First, check the actual expiration time in case the query has updated it
        // since the last time we looked.
        int32_t timeout_s = query_state->query_options().query_timeout_s;
        if (FLAGS_idle_query_timeout > 0 && timeout_s > 0) {
          timeout_s = min(FLAGS_idle_query_timeout, timeout_s);
        } else {
          // Use a non-zero timeout, if one exists
          timeout_s = max(FLAGS_idle_query_timeout, timeout_s);
        }
        int64_t expiration = query_state->last_active_ms() + (timeout_s * 1000L);
        if (now < expiration) {
          // If the real expiration date is in the future we may need to re-insert the
          // query's expiration event at its correct location.
          if (expiration == expiration_event->first) {
            // The query hasn't been updated since it was inserted, so we know (by the
            // fact that queries are inserted in-expiration-order initially) that it is
            // still the next query to expire. No need to re-insert it.
            break;
          } else {
            // Erase and re-insert with an updated expiration time.
            TUniqueId query_id = expiration_event->second;
            queries_by_timestamp_.erase(expiration_event++);
            queries_by_timestamp_.insert(make_pair(expiration, query_id));
          }
        } else if (!query_state->is_active()) {
          // Otherwise time to expire this query
          VLOG_QUERY
              << "Expiring query due to client inactivity: " << expiration_event->second
              << ", last activity was at: "
              << TimestampValue(query_state->last_active_ms() / 1000).DebugString();
          const string& err_msg = Substitute(
              "Query $0 expired due to client inactivity (timeout is $1)",
              PrintId(expiration_event->second),
              PrettyPrinter::Print(timeout_s * 1000000000L, TUnit::TIME_NS));

          cancellation_thread_pool_->Offer(
              CancellationWork(expiration_event->second, Status(err_msg), false));
          queries_by_timestamp_.erase(expiration_event++);
          ImpaladMetrics::NUM_QUERIES_EXPIRED->Increment(1L);
        } else {
          // Iterator is moved on in every other branch.
          ++expiration_event;
        }
      }
    }
    // Since we only allow timeouts to be 1s or greater, the earliest that any new query
    // could expire is in 1s time. An existing query may expire sooner, but we are
    // comfortable with a maximum error of 1s as a trade-off for not frequently waking
    // this thread.
    SleepForMs(1000L);
  }
}

Status CreateImpalaServer(ExecEnv* exec_env, int beeswax_port, int hs2_port, int be_port,
    ThriftServer** beeswax_server, ThriftServer** hs2_server, ThriftServer** be_server,
    boost::shared_ptr<ImpalaServer>* impala_server) {
  DCHECK((beeswax_port == 0) == (beeswax_server == nullptr));
  DCHECK((hs2_port == 0) == (hs2_server == nullptr));
  DCHECK((be_port == 0) == (be_server == nullptr));

  impala_server->reset(new ImpalaServer(exec_env));

  if (be_port != 0 && be_server != nullptr) {
    boost::shared_ptr<ImpalaInternalService> thrift_if(new ImpalaInternalService());
    boost::shared_ptr<TProcessor> be_processor(
        new ImpalaInternalServiceProcessor(thrift_if));
    boost::shared_ptr<TProcessorEventHandler> event_handler(
        new RpcEventHandler("backend", exec_env->metrics()));
    be_processor->setEventHandler(event_handler);

    *be_server = new ThriftServer("backend", be_processor, be_port, nullptr,
        exec_env->metrics(), FLAGS_be_service_threads);
    if (EnableInternalSslConnections()) {
      LOG(INFO) << "Enabling SSL for backend";
      RETURN_IF_ERROR((*be_server)->EnableSsl(FLAGS_ssl_server_certificate,
          FLAGS_ssl_private_key, FLAGS_ssl_private_key_password_cmd));
    }

    LOG(INFO) << "ImpalaInternalService listening on " << be_port;
  }
  if (!FLAGS_is_coordinator) {
    LOG(INFO) << "Started worker Impala server on "
              << ExecEnv::GetInstance()->backend_address();
    return Status::OK();
  }

  // Initialize the HS2 and Beeswax services.
  if (beeswax_port != 0 && beeswax_server != nullptr) {
    // Beeswax FE must be a TThreadPoolServer because ODBC and Hue only support
    // TThreadPoolServer.
    boost::shared_ptr<TProcessor> beeswax_processor(
        new ImpalaServiceProcessor(*impala_server));
    boost::shared_ptr<TProcessorEventHandler> event_handler(
        new RpcEventHandler("beeswax", exec_env->metrics()));
    beeswax_processor->setEventHandler(event_handler);
    *beeswax_server = new ThriftServer(BEESWAX_SERVER_NAME, beeswax_processor,
        beeswax_port, AuthManager::GetInstance()->GetExternalAuthProvider(),
        exec_env->metrics(), FLAGS_fe_service_threads, ThriftServer::ThreadPool);

    (*beeswax_server)->SetConnectionHandler(impala_server->get());
    if (!FLAGS_ssl_server_certificate.empty()) {
      LOG(INFO) << "Enabling SSL for Beeswax";
      RETURN_IF_ERROR((*beeswax_server)->EnableSsl(FLAGS_ssl_server_certificate,
          FLAGS_ssl_private_key, FLAGS_ssl_private_key_password_cmd));
    }

    LOG(INFO) << "Impala Beeswax Service listening on " << beeswax_port;
  }

  if (hs2_port != 0 && hs2_server != nullptr) {
    // HiveServer2 JDBC driver does not support non-blocking server.
    boost::shared_ptr<TProcessor> hs2_fe_processor(
        new ImpalaHiveServer2ServiceProcessor(*impala_server));
    boost::shared_ptr<TProcessorEventHandler> event_handler(
        new RpcEventHandler("hs2", exec_env->metrics()));
    hs2_fe_processor->setEventHandler(event_handler);

    *hs2_server = new ThriftServer(HS2_SERVER_NAME, hs2_fe_processor, hs2_port,
        AuthManager::GetInstance()->GetExternalAuthProvider(), exec_env->metrics(),
        FLAGS_fe_service_threads, ThriftServer::ThreadPool);

    (*hs2_server)->SetConnectionHandler(impala_server->get());
    if (!FLAGS_ssl_server_certificate.empty()) {
      LOG(INFO) << "Enabling SSL for HiveServer2";
      RETURN_IF_ERROR((*hs2_server)->EnableSsl(FLAGS_ssl_server_certificate,
          FLAGS_ssl_private_key, FLAGS_ssl_private_key_password_cmd));
    }

    LOG(INFO) << "Impala HiveServer2 Service listening on " << hs2_port;
  }

  LOG(INFO) << "Started coordinator Impala server on "
            << ExecEnv::GetInstance()->backend_address();
  return Status::OK();
}

bool ImpalaServer::GetSessionIdForQuery(const TUniqueId& query_id,
    TUniqueId* session_id) {
  DCHECK(session_id != nullptr);
  lock_guard<mutex> l(query_exec_state_map_lock_);
  QueryExecStateMap::iterator i = query_exec_state_map_.find(query_id);
  if (i == query_exec_state_map_.end()) {
    return false;
  } else {
    *session_id = i->second->session_id();
    return true;
  }
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

void ImpalaServer::UpdateFilter(TUpdateFilterResult& result,
    const TUpdateFilterParams& params) {
  shared_ptr<QueryExecState> query_exec_state = GetQueryExecState(params.query_id, false);
  if (query_exec_state.get() == nullptr) {
    LOG(INFO) << "Could not find query exec state: " << params.query_id;
    return;
  }
  query_exec_state->coord()->UpdateFilter(params);
}

}
