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
#include "runtime/timestamp-value.inline.h"
#include "runtime/tmp-file-mgr.h"
#include "scheduling/scheduler.h"
#include "service/impala-http-handler.h"
#include "service/impala-internal-service.h"
#include "service/client-request-state.h"
#include "util/bit-util.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/lineage-util.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/parse-util.h"
#include "util/redactor.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"
#include "util/string-parser.h"
#include "util/summary-util.h"
#include "util/test-info.h"
#include "util/uid-util.h"
#include "util/time.h"

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
using boost::algorithm::join;
using boost::algorithm::replace_all_copy;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::get_system_time;
using boost::system_time;
using boost::uuids::random_generator;
using boost::uuids::uuid;
using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace boost::posix_time;
using namespace beeswax;
using namespace rapidjson;
using namespace strings;

DECLARE_string(nn);
DECLARE_int32(nn_port);
DECLARE_string(authorized_proxy_user_config);
DECLARE_string(authorized_proxy_user_config_delimiter);
DECLARE_bool(abort_on_config_error);
DECLARE_bool(disk_spill_encryption);
DECLARE_bool(use_krpc);

DEFINE_int32(beeswax_port, 21000, "port on which Beeswax client requests are served");
DEFINE_int32(hs2_port, 21050, "port on which HiveServer2 client requests are served");

DEFINE_int32(fe_service_threads, 64,
    "number of threads available to serve client requests");
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

// TODO: For 3.0 (compatibility-breaking release), set this to a whitelist of ciphers,
// e.g.  https://wiki.mozilla.org/Security/Server_Side_TLS
DEFINE_string(ssl_cipher_list, "",
    "The cipher suite preferences to use for TLS-secured "
    "Thrift RPC connections. Uses the OpenSSL cipher preference list format. See man (1) "
    "ciphers for more information. If empty, the default cipher list for your platform "
    "is used");

const string SSL_MIN_VERSION_HELP = "The minimum SSL/TLS version that Thrift "
    "services should use for both client and server connections. Supported versions are "
    "TLSv1.0, TLSv1.1 and TLSv1.2 (as long as the system OpenSSL library supports them)";
DEFINE_string(ssl_minimum_version, "tlsv1", SSL_MIN_VERSION_HELP.c_str());

DEFINE_int32(idle_session_timeout, 0, "The time, in seconds, that a session may be idle"
    " for before it is closed (and all running queries cancelled) by Impala. If 0, idle"
    " sessions are never expired. It can be overridden by the query option"
    " 'idle_session_timeout' for specific sessions");
DEFINE_int32(idle_query_timeout, 0, "The time, in seconds, that a query may be idle for"
    " (i.e. no processing work is done and no updates are received from the client) "
    "before it is cancelled. If 0, idle queries are never expired. The query option "
    "QUERY_TIMEOUT_S overrides this setting, but, if set, --idle_query_timeout represents"
    " the maximum allowable timeout.");

DEFINE_bool(is_coordinator, true, "If true, this Impala daemon can accept and coordinate "
    "queries from clients. If false, it will refuse client connections.");
DEFINE_bool(is_executor, true, "If true, this Impala daemon will execute query "
    "fragments.");

#ifndef NDEBUG
  DEFINE_int64(stress_metadata_loading_pause_injection_ms, 0, "Simulates metadata loading"
      "for a given query by injecting a sleep equivalent to this configuration in "
      "milliseconds. Only used for testing.");
#endif

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
      thrift_serializer_(false),
      services_started_(false) {
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

  ABORT_IF_ERROR(ExternalDataSourceExecutor::InitJNI(exec_env->metrics()));

  // Register the membership callback if running in a real cluster.
  if (!TestInfo::is_test()) {
    auto cb = [this] (const StatestoreSubscriber::TopicDeltaMap& state,
         vector<TTopicDelta>* topic_updates) {
      this->MembershipCallback(state, topic_updates);
    };
    ABORT_IF_ERROR(
        exec_env->subscriber()->AddTopic(Statestore::IMPALA_MEMBERSHIP_TOPIC, true, cb));

    if (FLAGS_is_coordinator) {
      auto catalog_cb = [this] (const StatestoreSubscriber::TopicDeltaMap& state,
          vector<TTopicDelta>* topic_updates) {
        this->CatalogUpdateCallback(state, topic_updates);
      };
      ABORT_IF_ERROR(exec_env->subscriber()->AddTopic(
            CatalogServer::IMPALA_CATALOG_TOPIC, true, catalog_cb));
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
  ABORT_IF_ERROR(cancellation_thread_pool_->Init());

  // Initialize a session expiry thread which blocks indefinitely until the first session
  // with non-zero timeout value is opened. Note that a session which doesn't specify any
  // idle session timeout value will use the default value FLAGS_idle_session_timeout.
  ABORT_IF_ERROR(Thread::Create("impala-server", "session-expirer",
      bind<void>(&ImpalaServer::ExpireSessions, this), &session_timeout_thread_));

  ABORT_IF_ERROR(Thread::Create("impala-server", "query-expirer",
      bind<void>(&ImpalaServer::ExpireQueries, this), &query_expiration_thread_));

  is_coordinator_ = FLAGS_is_coordinator;
  is_executor_ = FLAGS_is_executor;
  exec_env_->SetImpalaServer(this);
}

Status ImpalaServer::LogLineageRecord(const ClientRequestState& client_request_state) {
  const TExecRequest& request = client_request_state.exec_request();
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
  // e.g. converting from client_request_state.end_time() (IMPALA-4440).
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
bool ImpalaServer::IsExecutor() { return is_executor_; }

const ImpalaServer::BackendDescriptorMap& ImpalaServer::GetKnownBackends() {
  return known_backends_;
}

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
  RETURN_IF_ERROR(Thread::Create("impala-server", "lineage-log-flush",
      &ImpalaServer::LineageLoggerFlushThread, this, &lineage_logger_flush_thread_));
  return Status::OK();
}

Status ImpalaServer::LogAuditRecord(const ClientRequestState& request_state,
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
  writer.String(PrintId(request_state.query_id()).c_str());
  writer.String("session_id");
  writer.String(PrintId(request_state.session_id()).c_str());
  writer.String("start_time");
  writer.String(ToStringFromUnixMicros(request_state.start_time_us()).c_str());
  writer.String("authorization_failure");
  writer.Bool(Frontend::IsAuthorizationError(request_state.query_status()));
  writer.String("status");
  writer.String(request_state.query_status().GetDetail().c_str());
  writer.String("user");
  writer.String(request_state.effective_user().c_str());
  writer.String("impersonator");
  if (request_state.do_as_user().empty()) {
    // If there is no do_as_user() is empty, the "impersonator" field should be Null.
    writer.Null();
  } else {
    // Otherwise, the delegator is the current connected user.
    writer.String(request_state.connected_user().c_str());
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
      lexical_cast<string>(request_state.session()->network_address).c_str());
  writer.String("sql_statement");
  string stmt = replace_all_copy(request_state.sql_stmt(), "\n", " ");
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
  RETURN_IF_ERROR(Thread::Create("impala-server", "audit-event-log-flush",
      &ImpalaServer::AuditEventLoggerFlushThread, this,
      &audit_event_logger_flush_thread_));
  return Status::OK();
}

void ImpalaServer::LogQueryEvents(const ClientRequestState& request_state) {
  Status status = request_state.query_status();
  bool log_events = true;
  switch (request_state.stmt_type()) {
    case TStmtType::QUERY: {
      // If the query didn't finish, log audit and lineage events only if the
      // the client issued at least one fetch.
      if (!status.ok() && !request_state.fetched_rows()) log_events = false;
      break;
    }
    case TStmtType::DML: {
      if (!status.ok()) log_events = false;
      break;
    }
    case TStmtType::DDL: {
      if (request_state.catalog_op_type() == TCatalogOpType::DDL) {
        // For a DDL operation, log audit and lineage events only if the
        // operation finished.
        if (!status.ok()) log_events = false;
      } else {
        // This case covers local catalog operations such as SHOW and DESCRIBE.
        if (!status.ok() && !request_state.fetched_rows()) log_events = false;
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
      (Frontend::IsAuthorizationError(request_state.query_status()) || log_events)) {
    // TODO: deal with an error status
    discard_result(LogAuditRecord(request_state, request_state.exec_request()));
  }
  if (IsLineageLoggingEnabled() && log_events) {
    // TODO: deal with an error status
    discard_result(LogLineageRecord(request_state));
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
  RETURN_IF_ERROR(Thread::Create("impala-server", "log-flush-thread",
      &ImpalaServer::LogFileFlushThread, this, &profile_log_file_flush_thread_));

  return Status::OK();
}

Status ImpalaServer::GetRuntimeProfileStr(const TUniqueId& query_id,
    const string& user, bool base64_encoded, stringstream* output) {
  DCHECK(output != nullptr);
  // Search for the query id in the active query map
  {
    shared_ptr<ClientRequestState> request_state = GetClientRequestState(query_id);
    if (request_state.get() != nullptr) {
      // For queries in CREATED state, the profile information isn't populated yet.
      if (request_state->query_state() == beeswax::QueryState::CREATED) {
        return Status::Expected("Query plan is not ready.");
      }
      lock_guard<mutex> l(*request_state->lock());
      RETURN_IF_ERROR(CheckProfileAccess(user, request_state->effective_user(),
          request_state->user_has_profile_access()));
      if (base64_encoded) {
        RETURN_IF_ERROR(request_state->profile()->SerializeToArchiveString(output));
      } else {
        request_state->profile()->PrettyPrint(output);
      }
      return Status::OK();
    }
  }

  // The query was not found the active query map, search the query log.
  {
    lock_guard<mutex> l(query_log_lock_);
    QueryLogIndex::const_iterator query_record = query_log_index_.find(query_id);
    if (query_record == query_log_index_.end()) {
      // Common error, so logging explicitly and eliding Status's stack trace.
      string err = strings::Substitute("Query id $0 not found.", PrintId(query_id));
      VLOG(1) << err;
      return Status::Expected(err);
    }
    RETURN_IF_ERROR(CheckProfileAccess(user, query_record->second->effective_user,
        query_record->second->user_has_profile_access));
    if (base64_encoded) {
      (*output) << query_record->second->encoded_profile_str;
    } else {
      (*output) << query_record->second->profile_str;
    }
  }
  return Status::OK();
}

Status ImpalaServer::GetExecSummary(const TUniqueId& query_id, const string& user,
    TExecSummary* result) {
  // Search for the query id in the active query map.
  {
    shared_ptr<ClientRequestState> request_state = GetClientRequestState(query_id);
    if (request_state != nullptr) {
      lock_guard<mutex> l(*request_state->lock());
      RETURN_IF_ERROR(CheckProfileAccess(user, request_state->effective_user(),
          request_state->user_has_profile_access()));
      if (request_state->coord() != nullptr) {
        request_state->coord()->GetTExecSummary(result);
        TExecProgress progress;
        progress.__set_num_completed_scan_ranges(
            request_state->coord()->progress().num_complete());
        progress.__set_total_scan_ranges(request_state->coord()->progress().total());
        // TODO: does this not need to be synchronized?
        result->__set_progress(progress);
        return Status::OK();
      }
    }
  }

  // Look for the query in completed query log.
  // IMPALA-5275: Don't create Status while holding query_log_lock_
  {
    string effective_user;
    bool user_has_profile_access = false;
    bool is_query_missing = false;
    TExecSummary exec_summary;
    {
      lock_guard<mutex> l(query_log_lock_);
      QueryLogIndex::const_iterator query_record = query_log_index_.find(query_id);
      is_query_missing = query_record == query_log_index_.end();
      if (!is_query_missing) {
        effective_user = query_record->second->effective_user;
        user_has_profile_access = query_record->second->user_has_profile_access;
        exec_summary = query_record->second->exec_summary;
      }
    }
    if (is_query_missing) {
      // Common error, so logging explicitly and eliding Status's stack trace.
      string err = strings::Substitute("Query id $0 not found.", PrintId(query_id));
      VLOG(1) << err;
      return Status::Expected(err);
    }
    RETURN_IF_ERROR(CheckProfileAccess(user, effective_user, user_has_profile_access));
    *result = exec_summary;
  }
  return Status::OK();
}

[[noreturn]] void ImpalaServer::LogFileFlushThread() {
  while (true) {
    sleep(5);
    const Status status = profile_logger_->Flush();
    if (!status.ok()) {
      LOG(WARNING) << "Error flushing profile log: " << status.GetDetail();
    }
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

void ImpalaServer::ArchiveQuery(const ClientRequestState& query) {
  string encoded_profile_str;
  Status status = query.profile()->SerializeToArchiveString(&encoded_profile_str);
  if (!status.ok()) {
    // Didn't serialize the string. Continue with empty string.
    LOG_EVERY_N(WARNING, 1000) << "Could not serialize profile to archive string "
                               << status.GetDetail();
    return;
  }

  // If there was an error initialising archival (e.g. directory is not writeable),
  // FLAGS_log_query_to_file will have been set to false
  if (FLAGS_log_query_to_file) {
    stringstream ss;
    ss << UnixMillis() << " " << query.query_id() << " " << encoded_profile_str;
    status = profile_logger_->AppendEntry(ss.str());
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
  if (query.coord() != nullptr) query.coord()->GetTExecSummary(&record.exec_summary);
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
    shared_ptr<ClientRequestState>* request_state) {
  PrepareQueryContext(query_ctx);
  ImpaladMetrics::IMPALA_SERVER_NUM_QUERIES->Increment(1L);

  // Redact the SQL stmt and update the query context
  string stmt = replace_all_copy(query_ctx->client_request.stmt, "\n", " ");
  Redact(&stmt);
  query_ctx->client_request.__set_redacted_stmt((const string) stmt);

  bool registered_request_state;
  Status status = ExecuteInternal(*query_ctx, session_state, &registered_request_state,
      request_state);
  if (!status.ok() && registered_request_state) {
    discard_result(UnregisterQuery((*request_state)->query_id(), false, &status));
  }
  return status;
}

Status ImpalaServer::ExecuteInternal(
    const TQueryCtx& query_ctx,
    shared_ptr<SessionState> session_state,
    bool* registered_request_state,
    shared_ptr<ClientRequestState>* request_state) {
  DCHECK(session_state != nullptr);
  *registered_request_state = false;

  request_state->reset(new ClientRequestState(query_ctx, exec_env_, exec_env_->frontend(),
      this, session_state));

  (*request_state)->query_events()->MarkEvent("Query submitted");

  TExecRequest result;
  {
    // Keep a lock on request_state so that registration and setting
    // result_metadata are atomic.
    lock_guard<mutex> l(*(*request_state)->lock());

    // register exec state as early as possible so that queries that
    // take a long time to plan show up, and to handle incoming status
    // reports before execution starts.
    RETURN_IF_ERROR(RegisterQuery(session_state, *request_state));
    *registered_request_state = true;

#ifndef NDEBUG
    // Inject a sleep to simulate metadata loading pauses for tables. This
    // is only used for testing.
    if (FLAGS_stress_metadata_loading_pause_injection_ms > 0) {
      SleepForMs(FLAGS_stress_metadata_loading_pause_injection_ms);
    }
#endif

    RETURN_IF_ERROR((*request_state)->UpdateQueryStatus(
        exec_env_->frontend()->GetExecRequest(query_ctx, &result)));

    (*request_state)->query_events()->MarkEvent("Planning finished");
    (*request_state)->set_user_profile_access(result.user_has_profile_access);
    (*request_state)->summary_profile()->AddEventSequence(
        result.timeline.name, result.timeline);
    if (result.__isset.result_set_metadata) {
      (*request_state)->set_result_metadata(result.result_set_metadata);
    }
  }
  VLOG(2) << "Execution request: " << ThriftDebugString(result);

  // start execution of query; also starts fragment status reports
  RETURN_IF_ERROR((*request_state)->Exec(&result));
  if (result.stmt_type == TStmtType::DDL) {
    Status status = UpdateCatalogMetrics();
    if (!status.ok()) {
      VLOG_QUERY << "Couldn't update catalog metrics: " << status.GetDetail();
    }
  }

  if ((*request_state)->coord() != nullptr) {
    const PerBackendExecParams& per_backend_params =
        (*request_state)->schedule()->per_backend_exec_params();
    if (!per_backend_params.empty()) {
      lock_guard<mutex> l(query_locations_lock_);
      for (const auto& entry : per_backend_params) {
        const TNetworkAddress& host = entry.first;
        query_locations_[host].insert((*request_state)->query_id());
      }
    }
  }
  return Status::OK();
}

void ImpalaServer::PrepareQueryContext(TQueryCtx* query_ctx) {
  query_ctx->__set_pid(getpid());
  int64_t now_us = UnixMicros();
  query_ctx->__set_utc_timestamp_string(ToUtcStringFromUnixMicros(now_us));
  query_ctx->__set_now_string(ToStringFromUnixMicros(now_us));
  query_ctx->__set_start_unix_millis(now_us / MICROS_PER_MILLI);
  query_ctx->__set_coord_address(ExecEnv::GetInstance()->backend_address());

  // Creating a random_generator every time is not free, but
  // benchmarks show it to be slightly cheaper than contending for a
  // single generator under a lock (since random_generator is not
  // thread-safe).
  query_ctx->query_id = UuidToQueryId(random_generator()());

}

Status ImpalaServer::RegisterQuery(shared_ptr<SessionState> session_state,
    const shared_ptr<ClientRequestState>& request_state) {
  lock_guard<mutex> l2(session_state->lock);
  // The session wasn't expired at the time it was checked out and it isn't allowed to
  // expire while checked out, so it must not be expired.
  DCHECK(session_state->ref_count > 0 && !session_state->expired);
  // The session may have been closed after it was checked out.
  if (session_state->closed) {
    VLOG(1) << "RegisterQuery(): session has been closed, ignoring query.";
    return Status::Expected("Session has been closed, ignoring query.");
  }
  const TUniqueId& query_id = request_state->query_id();
  {
    ScopedShardedMapRef<std::shared_ptr<ClientRequestState>> map_ref(query_id,
        &ExecEnv::GetInstance()->impala_server()->client_request_state_map_);
    DCHECK(map_ref.get() != nullptr);

    auto entry = map_ref->find(query_id);
    if (entry != map_ref->end()) {
      // There shouldn't be an active query with that same id.
      // (query_id is globally unique)
      stringstream ss;
      ss << "query id " << PrintId(query_id) << " already exists";
      return Status(ErrorMsg(TErrorCode::INTERNAL_ERROR, ss.str()));
    }
    map_ref->insert(make_pair(query_id, request_state));
  }
  // Metric is decremented in UnregisterQuery().
  ImpaladMetrics::NUM_QUERIES_REGISTERED->Increment(1L);
  return Status::OK();
}

Status ImpalaServer::SetQueryInflight(shared_ptr<SessionState> session_state,
    const shared_ptr<ClientRequestState>& request_state) {
  const TUniqueId& query_id = request_state->query_id();
  lock_guard<mutex> l(session_state->lock);
  // The session wasn't expired at the time it was checked out and it isn't allowed to
  // expire while checked out, so it must not be expired.
  DCHECK_GT(session_state->ref_count, 0);
  DCHECK(!session_state->expired);
  // The session may have been closed after it was checked out.
  if (session_state->closed) {
    VLOG(1) << "Session closed: cannot set " << PrintId(query_id) << " in-flight";
    return Status::Expected("Session closed");
  }
  // Add query to the set that will be unregistered if sesssion is closed.
  session_state->inflight_queries.insert(query_id);
  ++session_state->total_queries;

  // If the query has a timeout or time limit, schedule checks.
  int32_t idle_timeout_s = request_state->query_options().query_timeout_s;
  if (FLAGS_idle_query_timeout > 0 && idle_timeout_s > 0) {
    idle_timeout_s = min(FLAGS_idle_query_timeout, idle_timeout_s);
  } else {
    // Use a non-zero timeout, if one exists
    idle_timeout_s = max(FLAGS_idle_query_timeout, idle_timeout_s);
  }
  int32_t exec_time_limit_s = request_state->query_options().exec_time_limit_s;
  if (idle_timeout_s > 0 || exec_time_limit_s > 0) {
    lock_guard<mutex> l2(query_expiration_lock_);
    int64_t now = UnixMillis();
    if (idle_timeout_s > 0) {
      VLOG_QUERY << "Query " << PrintId(query_id) << " has idle timeout of "
                 << PrettyPrinter::Print(idle_timeout_s, TUnit::TIME_S);
      queries_by_timestamp_.emplace(ExpirationEvent{
          now + (1000L * idle_timeout_s), query_id, ExpirationKind::IDLE_TIMEOUT});
    }
    if (exec_time_limit_s > 0) {
      VLOG_QUERY << "Query " << PrintId(query_id) << " has execution time limit of "
                 << PrettyPrinter::Print(exec_time_limit_s, TUnit::TIME_S);
      queries_by_timestamp_.emplace(ExpirationEvent{
          now + (1000L * exec_time_limit_s), query_id, ExpirationKind::EXEC_TIME_LIMIT});
    }
  }
  return Status::OK();
}

Status ImpalaServer::UnregisterQuery(const TUniqueId& query_id, bool check_inflight,
    const Status* cause) {
  VLOG_QUERY << "UnregisterQuery(): query_id=" << query_id;

  RETURN_IF_ERROR(CancelInternal(query_id, check_inflight, cause));

  shared_ptr<ClientRequestState> request_state;
  {
    ScopedShardedMapRef<std::shared_ptr<ClientRequestState>> map_ref(query_id,
        &ExecEnv::GetInstance()->impala_server()->client_request_state_map_);
    DCHECK(map_ref.get() != nullptr);

    auto entry = map_ref->find(query_id);
    if (entry == map_ref->end()) {
      VLOG(1) << "Invalid or unknown query handle " << PrintId(query_id);
      return Status::Expected("Invalid or unknown query handle");
    } else {
      request_state = entry->second;
    }
    map_ref->erase(entry);
  }

  request_state->Done();

  int64_t duration_us = request_state->end_time_us() - request_state->start_time_us();
  int64_t duration_ms = duration_us / MICROS_PER_MILLI;

  // duration_ms can be negative when the local timezone changes during query execution.
  if (duration_ms >= 0) {
    if (request_state->stmt_type() == TStmtType::DDL) {
      ImpaladMetrics::DDL_DURATIONS->Update(duration_ms);
    } else {
      ImpaladMetrics::QUERY_DURATIONS->Update(duration_ms);
    }
  }
  LogQueryEvents(*request_state.get());

  {
    lock_guard<mutex> l(request_state->session()->lock);
    request_state->session()->inflight_queries.erase(query_id);
  }

  if (request_state->coord() != nullptr) {
    TExecSummary t_exec_summary;
    request_state->coord()->GetTExecSummary(&t_exec_summary);
    string exec_summary = PrintExecSummary(t_exec_summary);
    request_state->summary_profile()->AddInfoStringRedacted("ExecSummary", exec_summary);
    request_state->summary_profile()->AddInfoStringRedacted("Errors",
        request_state->coord()->GetErrorLog());

    const PerBackendExecParams& per_backend_params =
        request_state->schedule()->per_backend_exec_params();
    if (!per_backend_params.empty()) {
      lock_guard<mutex> l(query_locations_lock_);
      for (const auto& entry : per_backend_params) {
        const TNetworkAddress& hostport = entry.first;
        // Query may have been removed already by cancellation path. In particular, if
        // node to fail was last sender to an exchange, the coordinator will realise and
        // fail the query at the same time the failure detection path does the same
        // thing. They will harmlessly race to remove the query from this map.
        QueryLocations::iterator it = query_locations_.find(hostport);
        if (it != query_locations_.end()) {
          it->second.erase(request_state->query_id());
        }
      }
    }
  }
  ArchiveQuery(*request_state);
  ImpaladMetrics::NUM_QUERIES_REGISTERED->Increment(-1L);
  return Status::OK();
}

Status ImpalaServer::UpdateCatalogMetrics() {
  TGetDbsResult dbs;
  RETURN_IF_ERROR(exec_env_->frontend()->GetDbs(nullptr, nullptr, &dbs));
  ImpaladMetrics::CATALOG_NUM_DBS->SetValue(dbs.dbs.size());
  ImpaladMetrics::CATALOG_NUM_TABLES->SetValue(0L);
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
  shared_ptr<ClientRequestState> request_state = GetClientRequestState(query_id);
  if (request_state == nullptr) {
    return Status::Expected("Invalid or unknown query handle");
  }
  RETURN_IF_ERROR(request_state->Cancel(check_inflight, cause));
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
        string err_msg = Substitute("Invalid session id: $0", PrintId(session_id));
        VLOG(1) << "CloseSessionInternal(): " << err_msg;
        return Status::Expected(err_msg);
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
  Status status = Status::Expected("Session closed");
  for (const TUniqueId& query_id: inflight_queries) {
    // TODO: deal with an error status
    discard_result(UnregisterQuery(query_id, false, &status));
  }
  // Reconfigure the poll period of session_timeout_thread_ if necessary.
  UnregisterSessionTimeout(session_state->session_timeout);
  return Status::OK();
}

Status ImpalaServer::GetSessionState(const TUniqueId& session_id,
    shared_ptr<SessionState>* session_state, bool mark_active) {
  lock_guard<mutex> l(session_state_map_lock_);
  SessionStateMap::iterator i = session_state_map_.find(session_id);
  if (i == session_state_map_.end()) {
    *session_state = std::shared_ptr<SessionState>();
    string err_msg = Substitute("Invalid session id: $0", PrintId(session_id));
    VLOG(1) << "GetSessionState(): " << err_msg;
    return Status::Expected(err_msg);
  } else {
    if (mark_active) {
      lock_guard<mutex> session_lock(i->second->lock);
      if (i->second->expired) {
        stringstream ss;
        ss << "Client session expired due to more than " << i->second->session_timeout
           << "s of inactivity (last activity was at: "
           << ToStringFromUnixMillis(i->second->last_accessed_ms) << ").";
        return Status::Expected(ss.str());
      }
      if (i->second->closed) {
        VLOG(1) << "GetSessionState(): session " << PrintId(session_id) << " is closed.";
        return Status::Expected("Session is closed");
      }
      ++i->second->ref_count;
    }
    *session_state = i->second;
    return Status::OK();
  }
}

void ImpalaServer::ReportExecStatus(
    TReportExecStatusResult& return_val, const TReportExecStatusParams& params) {
  VLOG_FILE << "ReportExecStatus() coord_state_idx=" << params.coord_state_idx;
  // TODO: implement something more efficient here, we're currently
  // acquiring/releasing the map lock and doing a map lookup for
  // every report (assign each query a local int32_t id and use that to index into a
  // vector of ClientRequestStates, w/o lookup or locking?)
  shared_ptr<ClientRequestState> request_state =
      GetClientRequestState(params.query_id);
  if (request_state.get() == nullptr) {
    // This is expected occasionally (since a report RPC might be in flight while
    // cancellation is happening). Return an error to the caller to get it to stop.
    const string& err = Substitute("ReportExecStatus(): Received report for unknown "
        "query ID (probably closed or cancelled): $0", PrintId(params.query_id));
    VLOG(1) << err;
    Status::Expected(err).SetTStatus(&return_val);
    return;
  }
  request_state->coord()->UpdateBackendExecStatus(params).SetTStatus(&return_val);
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
    Status status = exec_env_->ThriftStreamMgr()->AddData(
        params.dest_fragment_instance_id, params.dest_node_id, params.row_batch,
        params.sender_id);
    status.SetTStatus(&return_val);
    if (!status.ok()) {
      // should we close the channel here as well?
      return;
    }
  }

  if (params.eos) {
    exec_env_->ThriftStreamMgr()->CloseSender(
        params.dest_fragment_instance_id, params.dest_node_id,
        params.sender_id).SetTStatus(&return_val);
  }
}

void ImpalaServer::InitializeConfigVariables() {
  // Set idle_session_timeout here to let the SET command return the value of
  // the command line option FLAGS_idle_session_timeout
  default_query_options_.__set_idle_session_timeout(FLAGS_idle_session_timeout);
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
  string_map["SUPPORT_START_OVER"] = "false";
  PopulateQueryOptionLevels(&query_option_levels_);
  map<string, string>::const_iterator itr = string_map.begin();
  for (; itr != string_map.end(); ++itr) {
    ConfigVariable option;
    option.__set_key(itr->first);
    option.__set_value(itr->second);
    AddOptionLevelToConfig(&option, itr->first);
    default_configs_.push_back(option);
  }
}

void ImpalaServer::SessionState::UpdateTimeout() {
  DCHECK(impala_server != nullptr);
  int32_t old_timeout = session_timeout;
  if (set_query_options.__isset.idle_session_timeout) {
    session_timeout = set_query_options.idle_session_timeout;
  } else {
    session_timeout = server_default_query_options->idle_session_timeout;
  }
  if (old_timeout != session_timeout) {
    impala_server->UnregisterSessionTimeout(old_timeout);
    impala_server->RegisterSessionTimeout(session_timeout);
  }
}

void ImpalaServer::AddOptionLevelToConfig(ConfigVariable* config,
    const string& option_key) const {
  const auto query_option_level = query_option_levels_.find(option_key);
  DCHECK(query_option_level != query_option_levels_.end());
  config->__set_level(query_option_level->second);
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

TQueryOptions ImpalaServer::SessionState::QueryOptions() {
  TQueryOptions ret = *server_default_query_options;
  OverlayQueryOptions(set_query_options, set_query_options_mask, &ret);
  return ret;
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
    const string err_msg("Unable to delegate using empty proxy username.");
    VLOG(1) << err_msg;
    return Status::Expected(err_msg);
  } else if (do_as_user.empty()) {
    const string err_msg("Unable to delegate using empty doAs username.");
    VLOG(1) << err_msg;
    return Status::Expected(err_msg);
  }

  stringstream error_msg;
  error_msg << "User '" << user << "' is not authorized to delegate to '"
            << do_as_user << "'.";
  if (authorized_proxy_user_config_.size() == 0) {
    error_msg << " User delegation is disabled.";
    VLOG(1) << error_msg;
    return Status::Expected(error_msg.str());
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
  VLOG(1) << error_msg;
  return Status::Expected(error_msg.str());
}

void ImpalaServer::CatalogUpdateVersionInfo::UpdateCatalogVersionMetrics()
{
  ImpaladMetrics::CATALOG_VERSION->SetValue(catalog_version);
  ImpaladMetrics::CATALOG_TOPIC_VERSION->SetValue(catalog_topic_version);
  ImpaladMetrics::CATALOG_SERVICE_ID->SetValue(PrintId(catalog_service_id));
}

void ImpalaServer::CatalogUpdateCallback(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(CatalogServer::IMPALA_CATALOG_TOPIC);
  if (topic == incoming_topic_deltas.end()) return;
  const TTopicDelta& delta = topic->second;
  TopicItemSpanIterator callback_ctx (delta.topic_entries, FLAGS_compact_catalog_topic);

  TUpdateCatalogCacheRequest req;
  req.__set_is_delta(delta.is_delta);
  req.__set_native_iterator_ptr(reinterpret_cast<int64_t>(&callback_ctx));
  TUpdateCatalogCacheResponse resp;
  Status s = exec_env_->frontend()->UpdateCatalogCache(req, &resp);
  if (!s.ok()) {
    LOG(ERROR) << "There was an error processing the impalad catalog update. Requesting"
               << " a full topic update to recover: " << s.GetDetail();
    subscriber_topic_updates->emplace_back();
    TTopicDelta& update = subscriber_topic_updates->back();
    update.topic_name = CatalogServer::IMPALA_CATALOG_TOPIC;
    update.__set_from_version(0L);
    ImpaladMetrics::CATALOG_READY->SetValue(false);
    // Dropped all cached lib files (this behaves as if all functions and data
    // sources are dropped).
    LibCache::instance()->DropCache();
  } else {
    {
      unique_lock<mutex> unique_lock(catalog_version_lock_);
      if (catalog_update_info_.catalog_version != resp.new_catalog_version) {
        LOG(INFO) << "Catalog topic update applied with version: " <<
            resp.new_catalog_version << " new min catalog object version: " <<
            resp.min_catalog_object_version;
      }
      catalog_update_info_.catalog_version = resp.new_catalog_version;
      catalog_update_info_.catalog_topic_version = delta.to_version;
      catalog_update_info_.catalog_service_id = resp.catalog_service_id;
      catalog_update_info_.min_catalog_object_version = resp.min_catalog_object_version;
      catalog_update_info_.UpdateCatalogVersionMetrics();
    }
    ImpaladMetrics::CATALOG_READY->SetValue(resp.new_catalog_version > 0);
    // TODO: deal with an error status
    discard_result(UpdateCatalogMetrics());
  }
  // Always update the minimum subscriber version for the catalog topic.
  {
    unique_lock<mutex> unique_lock(catalog_version_lock_);
    min_subscriber_catalog_topic_version_ = delta.min_subscriber_topic_version;
  }
  catalog_version_update_cv_.NotifyAll();
}

void ImpalaServer::WaitForCatalogUpdate(const int64_t catalog_update_version,
    const TUniqueId& catalog_service_id) {
  unique_lock<mutex> unique_lock(catalog_version_lock_);
  // Wait for the update to be processed locally.
  VLOG_QUERY << "Waiting for catalog version: " << catalog_update_version
             << " current version: " << catalog_update_info_.catalog_version;
  while (catalog_update_info_.catalog_version < catalog_update_version &&
         catalog_update_info_.catalog_service_id == catalog_service_id) {
    catalog_version_update_cv_.Wait(unique_lock);
  }

  if (catalog_update_info_.catalog_service_id != catalog_service_id) {
    VLOG_QUERY << "Detected change in catalog service ID";
  } else {
    VLOG_QUERY << "Received catalog version: " << catalog_update_version;
  }
}

void ImpalaServer::WaitForCatalogUpdateTopicPropagation(
    const TUniqueId& catalog_service_id) {
  unique_lock<mutex> unique_lock(catalog_version_lock_);
  int64_t min_req_subscriber_topic_version =
      catalog_update_info_.catalog_topic_version;
  VLOG_QUERY << "Waiting for min subscriber topic version: "
      << min_req_subscriber_topic_version << " current version: "
      << min_subscriber_catalog_topic_version_;
  while (min_subscriber_catalog_topic_version_ < min_req_subscriber_topic_version &&
         catalog_update_info_.catalog_service_id == catalog_service_id) {
    catalog_version_update_cv_.Wait(unique_lock);
  }

  if (catalog_update_info_.catalog_service_id != catalog_service_id) {
    VLOG_QUERY << "Detected change in catalog service ID";
  } else {
    VLOG_QUERY << "Received min subscriber topic version: "
        << min_req_subscriber_topic_version;
  }
}

void ImpalaServer::WaitForMinCatalogUpdate(const int64_t min_req_catalog_object_version,
    const TUniqueId& catalog_service_id) {
  unique_lock<mutex> unique_lock(catalog_version_lock_);
  int64_t min_catalog_object_version =
      catalog_update_info_.min_catalog_object_version;
  // TODO: Set a timeout to eventually break out of this loop is something goes
  // wrong?
  VLOG_QUERY << "Waiting for minimum catalog object version: "
      << min_req_catalog_object_version << " current version: "
      << min_catalog_object_version;
  while (catalog_update_info_.min_catalog_object_version < min_req_catalog_object_version
      && catalog_update_info_.catalog_service_id == catalog_service_id) {
    catalog_version_update_cv_.Wait(unique_lock);
  }

  if (catalog_update_info_.catalog_service_id != catalog_service_id) {
    VLOG_QUERY << "Detected change in catalog service ID";
  } else {
    VLOG_QUERY << "Updated minimum catalog object version: "
        << min_req_catalog_object_version;
  }
}

Status ImpalaServer::ProcessCatalogUpdateResult(
    const TCatalogUpdateResult& catalog_update_result, bool wait_for_all_subscribers) {
  const TUniqueId& catalog_service_id = catalog_update_result.catalog_service_id;
  if (!catalog_update_result.__isset.updated_catalog_objects &&
      !catalog_update_result.__isset.removed_catalog_objects) {
    // Operation with no result set. Use the version specified in
    // 'catalog_update_result' to determine when the effects of this operation
    // have been applied to the local catalog cache.
    if (catalog_update_result.is_invalidate) {
      WaitForMinCatalogUpdate(catalog_update_result.version, catalog_service_id);
    } else {
      WaitForCatalogUpdate(catalog_update_result.version, catalog_service_id);
    }
    if (wait_for_all_subscribers) {
      // Now wait for this update to be propagated to all catalog topic subscribers.
      // If we make it here it implies the first condition was met (the update was
      // processed locally or the catalog service id has changed).
      WaitForCatalogUpdateTopicPropagation(catalog_service_id);
    }
  } else {
    CatalogUpdateResultIterator callback_ctx(catalog_update_result);
    TUpdateCatalogCacheRequest update_req;
    update_req.__set_is_delta(true);
    update_req.__set_native_iterator_ptr(reinterpret_cast<int64_t>(&callback_ctx));
    // The catalog version is updated in WaitForCatalogUpdate below. So we need a
    // standalone field in the request to update the service ID without touching the
    // catalog version.
    update_req.__set_catalog_service_id(catalog_update_result.catalog_service_id);
    // Apply the changes to the local catalog cache.
    TUpdateCatalogCacheResponse resp;
    Status status = exec_env_->frontend()->UpdateCatalogCache(update_req, &resp);
    if (!status.ok()) LOG(ERROR) << status.GetDetail();
    RETURN_IF_ERROR(status);
    if (!wait_for_all_subscribers) return Status::OK();
    // Wait until we receive and process the catalog update that covers the effects
    // (catalog objects) of this operation.
    WaitForCatalogUpdate(catalog_update_result.version, catalog_service_id);
    // Now wait for this update to be propagated to all catalog topic
    // subscribers.
    WaitForCatalogUpdateTopicPropagation(catalog_service_id);
  }
  return Status::OK();
}

void ImpalaServer::MembershipCallback(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  // TODO: Consider rate-limiting this. In the short term, best to have
  // statestore heartbeat less frequently.
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(Statestore::IMPALA_MEMBERSHIP_TOPIC);

  if (topic != incoming_topic_deltas.end()) {
    const TTopicDelta& delta = topic->second;
    // If this is not a delta, the update should include all entries in the topic so
    // clear the saved mapping of known backends.
    if (!delta.is_delta) known_backends_.clear();

    // Process membership additions.
    for (const TTopicItem& item: delta.topic_entries) {
      if (item.deleted) {
        known_backends_.erase(item.key);
        continue;
      }
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
    // Only register if all ports have been opened and are ready.
    if (services_started_.load()) AddLocalBackendToStatestore(subscriber_topic_updates);

    // Create a set of known backend network addresses. Used to test for cluster
    // membership by network address.
    set<TNetworkAddress> current_membership;
    // Also reflect changes to the frontend. Initialized only if any_changes is true.
    TUpdateMembershipRequest update_req;
    bool any_changes = !delta.topic_entries.empty() || !delta.is_delta;
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
        string cause_str = cause_msg.str();
        LOG(INFO) << "Query " << PrintId(cancellation_entry->first) << ": " << cause_str;
        cancellation_thread_pool_->Offer(CancellationWork(cancellation_entry->first,
            Status::Expected(cause_msg.str()), false));
      }
    }
  }
}

void ImpalaServer::AddLocalBackendToStatestore(
    vector<TTopicDelta>* subscriber_topic_updates) {
  const string& local_backend_id = exec_env_->subscriber()->id();
  if (known_backends_.find(local_backend_id) != known_backends_.end()) return;

  TBackendDescriptor local_backend_descriptor;
  local_backend_descriptor.__set_is_coordinator(FLAGS_is_coordinator);
  local_backend_descriptor.__set_is_executor(FLAGS_is_executor);
  local_backend_descriptor.__set_address(exec_env_->backend_address());
  local_backend_descriptor.ip_address = exec_env_->ip_address();
  if (FLAGS_use_krpc) {
    const TNetworkAddress& krpc_address = exec_env_->krpc_address();
    DCHECK(IsResolvedAddress(krpc_address));
    local_backend_descriptor.__set_krpc_address(krpc_address);
  }
  subscriber_topic_updates->emplace_back(TTopicDelta());
  TTopicDelta& update = subscriber_topic_updates->back();
  update.topic_name = Statestore::IMPALA_MEMBERSHIP_TOPIC;
  update.topic_entries.emplace_back(TTopicItem());

  TTopicItem& item = update.topic_entries.back();
  item.key = local_backend_id;
  Status status = thrift_serializer_.Serialize(&local_backend_descriptor, &item.value);
  if (!status.ok()) {
    LOG(WARNING) << "Failed to serialize Impala backend descriptor for statestore topic:"
                 << " " << status.GetDetail();
    subscriber_topic_updates->pop_back();
  } else {
    known_backends_.insert(make_pair(item.key, local_backend_descriptor));
  }
}

ImpalaServer::QueryStateRecord::QueryStateRecord(const ClientRequestState& request_state,
    bool copy_profile, const string& encoded_profile) {
  id = request_state.query_id();
  const TExecRequest& request = request_state.exec_request();

  const string* plan_str = request_state.summary_profile()->GetInfoString("Plan");
  if (plan_str != nullptr) plan = *plan_str;
  stmt = request_state.sql_stmt();
  stmt_type = request.stmt_type;
  effective_user = request_state.effective_user();
  default_db = request_state.default_db();
  start_time_us = request_state.start_time_us();
  end_time_us = request_state.end_time_us();
  has_coord = false;

  Coordinator* coord = request_state.coord();
  if (coord != nullptr) {
    num_complete_fragments = coord->progress().num_complete();
    total_fragments = coord->progress().total();
    has_coord = true;
  }
  query_state = request_state.query_state();
  num_rows_fetched = request_state.num_rows_fetched();
  query_status = request_state.query_status();

  request_state.query_events()->ToThrift(&event_sequence);

  if (copy_profile) {
    stringstream ss;
    request_state.profile()->PrettyPrint(&ss);
    profile_str = ss.str();
    if (encoded_profile.empty()) {
      Status status =
          request_state.profile()->SerializeToArchiveString(&encoded_profile_str);
      if (!status.ok()) {
        LOG_EVERY_N(WARNING, 1000) << "Could not serialize profile to archive string "
                                   << status.GetDetail();
      }
    } else {
      encoded_profile_str = encoded_profile;
    }
  }

  // Save the query fragments so that the plan can be visualised.
  for (const TPlanExecInfo& plan_exec_info:
      request_state.exec_request().query_exec_request.plan_exec_info) {
    fragments.insert(fragments.end(),
        plan_exec_info.fragments.begin(), plan_exec_info.fragments.end());
  }
  all_rows_returned = request_state.eos();
  last_active_time_ms = request_state.last_active_ms();
  request_pool = request_state.request_pool();
  user_has_profile_access = request_state.user_has_profile_access();
}

bool ImpalaServer::QueryStateRecordLessThan::operator() (
    const QueryStateRecord& lhs, const QueryStateRecord& rhs) const {
  if (lhs.start_time_us == rhs.start_time_us) return lhs.id < rhs.id;
  return lhs.start_time_us < rhs.start_time_us;
}

void ImpalaServer::ConnectionStart(
    const ThriftServer::ConnectionContext& connection_context) {
  if (connection_context.server_name == BEESWAX_SERVER_NAME) {
    // Beeswax only allows for one session per connection, so we can share the session ID
    // with the connection ID
    const TUniqueId& session_id = connection_context.connection_id;
    shared_ptr<SessionState> session_state = make_shared<SessionState>(this);
    session_state->closed = false;
    session_state->start_time_ms = UnixMillis();
    session_state->last_accessed_ms = UnixMillis();
    session_state->database = "default";
    session_state->session_timeout = FLAGS_idle_session_timeout;
    session_state->session_type = TSessionType::BEESWAX;
    session_state->network_address = connection_context.network_address;
    session_state->server_default_query_options = &default_query_options_;
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
  {
    lock_guard<mutex> l(session_timeout_lock_);
    session_timeout_set_.insert(session_timeout);
  }
  session_timeout_cv_.NotifyOne();
}

void ImpalaServer::UnregisterSessionTimeout(int32_t session_timeout) {
  if (session_timeout > 0) {
    lock_guard<mutex> l(session_timeout_lock_);
    auto itr = session_timeout_set_.find(session_timeout);
    DCHECK(itr != session_timeout_set_.end());
    session_timeout_set_.erase(itr);
  }
}

[[noreturn]] void ImpalaServer::ExpireSessions() {
  while (true) {
    {
      unique_lock<mutex> timeout_lock(session_timeout_lock_);
      if (session_timeout_set_.empty()) {
        session_timeout_cv_.Wait(timeout_lock);
      } else {
        // Sleep for a second before checking whether an active session can be expired.
        session_timeout_cv_.WaitFor(timeout_lock, MICROS_PER_SEC);
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
                  << ToStringFromUnixMillis(last_accessed_ms);
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
    // client_request_state. We are able to avoid doing this for *every* query in flight
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
        // 'queries_by_timestamp_' is stored in ascending order of deadline so we can
        // break out of the loop and sleep as soon as we see a deadline in the future.
        if (expiration_event->deadline > now) break;
        shared_ptr<ClientRequestState> query_state =
            GetClientRequestState(expiration_event->query_id);
        if (query_state == nullptr || query_state->is_expired()) {
          // Query was deleted or expired already from a previous expiration event.
          expiration_event = queries_by_timestamp_.erase(expiration_event);
          continue;
        }

        // If the query time limit expired, we must cancel the query.
        if (expiration_event->kind == ExpirationKind::EXEC_TIME_LIMIT) {
          int32_t exec_time_limit_s = query_state->query_options().exec_time_limit_s;
          VLOG_QUERY << "Expiring query " << expiration_event->query_id
                     << " due to execution time limit of " << exec_time_limit_s << "s.";
          const string& err_msg = Substitute(
              "Query $0 expired due to execution time limit of $1",
              PrintId(expiration_event->query_id),
              PrettyPrinter::Print(exec_time_limit_s, TUnit::TIME_S));
          ExpireQuery(query_state.get(), Status::Expected(err_msg));
          expiration_event = queries_by_timestamp_.erase(expiration_event);
          continue;
        }
        DCHECK(expiration_event->kind == ExpirationKind::IDLE_TIMEOUT)
            << static_cast<int>(expiration_event->kind);

        // Now check to see if the idle timeout has expired. We must check the actual
        // expiration time in case the query has updated 'last_active_ms' since the last
        // time we looked.
        int32_t idle_timeout_s = query_state->query_options().query_timeout_s;
        if (FLAGS_idle_query_timeout > 0 && idle_timeout_s > 0) {
          idle_timeout_s = min(FLAGS_idle_query_timeout, idle_timeout_s);
        } else {
          // Use a non-zero timeout, if one exists
          idle_timeout_s = max(FLAGS_idle_query_timeout, idle_timeout_s);
        }
        int64_t expiration = query_state->last_active_ms() + (idle_timeout_s * 1000L);
        if (now < expiration) {
          // If the real expiration date is in the future we may need to re-insert the
          // query's expiration event at its correct location.
          if (expiration == expiration_event->deadline) {
            // The query hasn't been updated since it was inserted, so we know (by the
            // fact that queries are inserted in-expiration-order initially) that it is
            // still the next query to expire. No need to re-insert it.
            break;
          } else {
            // Erase and re-insert with an updated expiration time.
            TUniqueId query_id = expiration_event->query_id;
            expiration_event = queries_by_timestamp_.erase(expiration_event);
            queries_by_timestamp_.emplace(ExpirationEvent{
                expiration, query_id, ExpirationKind::IDLE_TIMEOUT});
          }
        } else if (!query_state->is_active()) {
          // Otherwise time to expire this query
          VLOG_QUERY
              << "Expiring query due to client inactivity: "
              << expiration_event->query_id << ", last activity was at: "
              << ToStringFromUnixMillis(query_state->last_active_ms());
          const string& err_msg = Substitute(
              "Query $0 expired due to client inactivity (timeout is $1)",
              PrintId(expiration_event->query_id),
              PrettyPrinter::Print(idle_timeout_s, TUnit::TIME_S));
          ExpireQuery(query_state.get(), Status::Expected(err_msg));
          expiration_event = queries_by_timestamp_.erase(expiration_event);
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

void ImpalaServer::ExpireQuery(ClientRequestState* crs, const Status& status) {
  DCHECK(!status.ok());
  cancellation_thread_pool_->Offer(CancellationWork(crs->query_id(), status, false));
  ImpaladMetrics::NUM_QUERIES_EXPIRED->Increment(1L);
  crs->set_expired();
}

Status ImpalaServer::Start(int32_t thrift_be_port, int32_t beeswax_port,
   int32_t hs2_port) {
  exec_env_->SetImpalaServer(this);

  if (!FLAGS_is_coordinator && !FLAGS_is_executor) {
    return Status("Impala does not have a valid role configured. "
        "Either --is_coordinator or --is_executor must be set to true.");
  }

  // Subscribe with the statestore. Coordinators need to subscribe to the catalog topic
  // then wait for the initial catalog update.
  RETURN_IF_ERROR(exec_env_->StartStatestoreSubscriberService());

  if (FLAGS_is_coordinator) exec_env_->frontend()->WaitForCatalog();

  SSLProtocol ssl_version = SSLProtocol::TLSv1_0;
  if (IsExternalTlsConfigured() || IsInternalTlsConfigured()) {
    RETURN_IF_ERROR(
        SSLProtoVersions::StringToProtocol(FLAGS_ssl_minimum_version, &ssl_version));
  }

  // Start the internal service.
  if (thrift_be_port > 0) {
    boost::shared_ptr<ImpalaInternalService> thrift_if(new ImpalaInternalService());
    boost::shared_ptr<TProcessor> be_processor(
        new ImpalaInternalServiceProcessor(thrift_if));
    boost::shared_ptr<TProcessorEventHandler> event_handler(
        new RpcEventHandler("backend", exec_env_->metrics()));
    be_processor->setEventHandler(event_handler);

    ThriftServerBuilder be_builder("backend", be_processor, thrift_be_port);

    if (IsInternalTlsConfigured()) {
      LOG(INFO) << "Enabling SSL for backend";
      be_builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
          .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
          .ssl_version(ssl_version)
          .cipher_list(FLAGS_ssl_cipher_list);
    }
    ThriftServer* server;
    RETURN_IF_ERROR(be_builder.metrics(exec_env_->metrics()).Build(&server));
    thrift_be_server_.reset(server);
  }

  if (!FLAGS_is_coordinator) {
    LOG(INFO) << "Initialized executor Impala server on "
              << ExecEnv::GetInstance()->backend_address();
  } else {
    // Initialize the client servers.
    boost::shared_ptr<ImpalaServer> handler = shared_from_this();
    if (beeswax_port > 0) {
      boost::shared_ptr<TProcessor> beeswax_processor(
          new ImpalaServiceProcessor(handler));
      boost::shared_ptr<TProcessorEventHandler> event_handler(
          new RpcEventHandler("beeswax", exec_env_->metrics()));
      beeswax_processor->setEventHandler(event_handler);
      ThriftServerBuilder builder(BEESWAX_SERVER_NAME, beeswax_processor, beeswax_port);

      if (IsExternalTlsConfigured()) {
        LOG(INFO) << "Enabling SSL for Beeswax";
        builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
              .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
              .ssl_version(ssl_version)
              .cipher_list(FLAGS_ssl_cipher_list);
      }

      ThriftServer* server;
      RETURN_IF_ERROR(
          builder.auth_provider(AuthManager::GetInstance()->GetExternalAuthProvider())
          .metrics(exec_env_->metrics())
          .max_concurrent_connections(FLAGS_fe_service_threads)
          .Build(&server));
      beeswax_server_.reset(server);
      beeswax_server_->SetConnectionHandler(this);
    }

    if (hs2_port > 0) {
      boost::shared_ptr<TProcessor> hs2_fe_processor(
          new ImpalaHiveServer2ServiceProcessor(handler));
      boost::shared_ptr<TProcessorEventHandler> event_handler(
          new RpcEventHandler("hs2", exec_env_->metrics()));
      hs2_fe_processor->setEventHandler(event_handler);

      ThriftServerBuilder builder(HS2_SERVER_NAME, hs2_fe_processor, hs2_port);

      if (IsExternalTlsConfigured()) {
        LOG(INFO) << "Enabling SSL for HiveServer2";
        builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
              .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
              .ssl_version(ssl_version)
              .cipher_list(FLAGS_ssl_cipher_list);
      }

      ThriftServer* server;
      RETURN_IF_ERROR(
          builder.auth_provider(AuthManager::GetInstance()->GetExternalAuthProvider())
          .metrics(exec_env_->metrics())
          .max_concurrent_connections(FLAGS_fe_service_threads)
          .Build(&server));
      hs2_server_.reset(server);
      hs2_server_->SetConnectionHandler(this);
    }
  }
  LOG(INFO) << "Initialized coordinator/executor Impala server on "
      << ExecEnv::GetInstance()->backend_address();

  // Start the RPC services.
  RETURN_IF_ERROR(exec_env_->StartKrpcService());
  if (thrift_be_server_.get()) {
    RETURN_IF_ERROR(thrift_be_server_->Start());
    LOG(INFO) << "Impala InternalService listening on " << thrift_be_server_->port();
  }
  if (hs2_server_.get()) {
    RETURN_IF_ERROR(hs2_server_->Start());
    LOG(INFO) << "Impala HiveServer2 Service listening on " << hs2_server_->port();
  }
  if (beeswax_server_.get()) {
    RETURN_IF_ERROR(beeswax_server_->Start());
    LOG(INFO) << "Impala Beeswax Service listening on " << beeswax_server_->port();
  }
  services_started_ = true;
  ImpaladMetrics::IMPALA_SERVER_READY->SetValue(true);
  LOG(INFO) << "Impala has started.";

  return Status::OK();
}

void ImpalaServer::Join() {
  thrift_be_server_->Join();
  thrift_be_server_.reset();

  if (FLAGS_is_coordinator) {
    beeswax_server_->Join();
    hs2_server_->Join();
    beeswax_server_.reset();
    hs2_server_.reset();
  }
  shutdown_promise_.Get();
}

shared_ptr<ClientRequestState> ImpalaServer::GetClientRequestState(
    const TUniqueId& query_id) {
  ScopedShardedMapRef<std::shared_ptr<ClientRequestState>> map_ref(query_id,
      &ExecEnv::GetInstance()->impala_server()->client_request_state_map_);
  DCHECK(map_ref.get() != nullptr);

  auto entry = map_ref->find(query_id);
  if (entry == map_ref->end()) {
    return shared_ptr<ClientRequestState>();
  } else {
    return entry->second;
  }
}

void ImpalaServer::UpdateFilter(TUpdateFilterResult& result,
    const TUpdateFilterParams& params) {
  DCHECK(params.__isset.query_id);
  DCHECK(params.__isset.filter_id);
  shared_ptr<ClientRequestState> client_request_state =
      GetClientRequestState(params.query_id);
  if (client_request_state.get() == nullptr) {
    LOG(INFO) << "Could not find client request state: " << params.query_id;
    return;
  }
  client_request_state->coord()->UpdateFilter(params);
}

}
