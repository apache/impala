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

#include <netdb.h>
#include <unistd.h>
#include <algorithm>
#include <exception>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/unordered_set.hpp>
#include <gperftools/malloc_extension.h>
#include <gutil/strings/numbers.h>
#include <gutil/strings/split.h>
#include <gutil/strings/substitute.h>
#include <gutil/walltime.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <rapidjson/error/en.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "catalog/catalog-server.h"
#include "catalog/catalog-util.h"
#include "common/compiler-util.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "common/thread-debug-info.h"
#include "common/version.h"
#include "exec/external-data-source-executor.h"
#include "exprs/timezone_db.h"
#include "gen-cpp/CatalogService_constants.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/random_util.h"
#include "rpc/authentication.h"
#include "rpc/rpc-trace.h"
#include "rpc/thrift-thread.h"
#include "rpc/thrift-util.h"
#include "runtime/client-cache.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "runtime/lib-cache.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tmp-file-mgr.h"
#include "runtime/query-driver.h"
#include "scheduling/admission-controller.h"
#include "service/cancellation-work.h"
#include "service/client-request-state.h"
#include "service/frontend.h"
#include "service/impala-http-handler.h"
#include "service/impala-internal-service.h"
#include "util/auth-util.h"
#include "util/bit-util.h"
#include "util/coding-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/redactor.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"
#include "util/simple-logger.h"
#include "util/string-parser.h"
#include "util/summary-util.h"
#include "util/test-info.h"
#include "util/time.h"
#include "util/uid-util.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/LineageGraph_types.h"
#include "gen-cpp/Frontend_types.h"

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
using google::protobuf::RepeatedPtrField;
using kudu::GetRandomSeed32;
using kudu::rpc::RpcContext;
using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace beeswax;
using namespace boost::posix_time;
using namespace rapidjson;
using namespace strings;

DECLARE_string(nn);
DECLARE_int32(nn_port);
DECLARE_string(authorized_proxy_user_config);
DECLARE_string(authorized_proxy_user_config_delimiter);
DECLARE_string(authorized_proxy_group_config);
DECLARE_string(authorized_proxy_group_config_delimiter);
DECLARE_string(debug_actions);
DECLARE_bool(abort_on_config_error);
DECLARE_bool(disk_spill_encryption);
DECLARE_bool(enable_ldap_auth);
DECLARE_bool(use_local_catalog);

DEFINE_int32(beeswax_port, 21000, "port on which Beeswax client requests are served."
    "If 0 or less, the Beeswax server is not started.");
DEFINE_int32(hs2_port, 21050, "port on which HiveServer2 client requests are served."
    "If 0 or less, the HiveServer2 server is not started.");
DEFINE_int32(hs2_http_port, 28000, "port on which HiveServer2 HTTP(s) client "
    "requests are served. If 0 or less, the HiveServer2 http server is not started.");

DEFINE_int32(fe_service_threads, 64,
    "number of threads available to serve client requests");
DEFINE_string(default_query_options, "", "key=value pair of default query options for"
    " impalad, separated by ','");
DEFINE_int32(query_log_size, 100, "Number of queries to retain in the query log. If -1, "
    "the query log has unbounded size.");
DEFINE_int32(query_stmt_size, 250, "length of the statements in the query log. If <=0, "
    "the full statement is displayed in the query log without trimming.");
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
DEFINE_int32(max_audit_event_log_files, 0, "Maximum number of audit event log files "
    "to retain. The most recent audit event log files are retained. If set to 0, "
    "all audit event log files are retained.");

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

DEFINE_int32(unregistration_thread_pool_size, 4,
    "(Advanced) Size of the thread-pool for unregistering queries, including "
    "finalizing runtime profiles");
// Limit the number of queries that can be queued for unregistration to avoid holding
// too many queries in memory unnecessary. The default is set fairly low so that if
// queries are finishing faster than they can be unregistered, there will be backpressure
// on query execution before too much memory fills up with queries pending unregistration.
DEFINE_int32(unregistration_thread_pool_queue_depth, 16,
    "(Advanced) Max number of queries that can be queued for unregistration.");

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
DEFINE_int32(disconnected_session_timeout, 15 * 60, "The time, in seconds, that a "
    "hiveserver2 session will be maintained after the last connection that it has been "
    "used over is disconnected.");
DEFINE_int32(idle_client_poll_period_s, 30, "The poll period, in seconds, after "
    "no activity from an Impala client which an Impala service thread (beeswax and HS2) "
    "wakes up to check if the connection should be closed. If --idle_session_timeout is "
    "also set, a client connection will be closed if all the sessions associated with it "
    "have become idle. Set this to 0 to disable the polling behavior and clients' "
    "connection will remain opened until they are explicitly closed.");
DEFINE_int32(status_report_interval_ms, 5000, "(Advanced) Interval between profile "
    "reports in milliseconds. If set to <= 0, periodic reporting is disabled and only "
    "the final report is sent.");
DEFINE_int32(status_report_max_retry_s, 600, "(Advanced) Max amount of time in seconds "
    "for a backend to attempt to send a status report before cancelling. This must be > "
    "--status_report_interval_ms. Effective only if --status_report_interval_ms > 0.");
DEFINE_int32(status_report_cancellation_padding, 20, "(Advanced) The coordinator will "
    "wait --status_report_max_retry_s * (1 + --status_report_cancellation_padding / 100) "
    "without receiving a status report before deciding that a backend is unresponsive "
    "and the query should be cancelled. This must be > 0.");

DEFINE_bool(is_coordinator, true, "If true, this Impala daemon can accept and coordinate "
    "queries from clients. If false, it will refuse client connections.");
DEFINE_bool(is_executor, true, "If true, this Impala daemon will execute query "
    "fragments.");
DEFINE_string(executor_groups, "",
    "List of executor groups, separated by comma. Each executor group specification can "
    "optionally contain a minimum size, separated by a ':', e.g. --executor_groups "
    "default-pool-1:3. Default minimum size is 1. Only when the cluster membership "
    "contains at least that number of executors for the group will it be considered "
    "healthy for admission. Currently only a single group may be specified.");

DEFINE_int32(num_expected_executors, 20, "The number of executors that are expected to "
    "be available for the execution of a single query. This value is used during "
    "planning if no executors have started yet. Once a healthy executor group has "
    "started, its size is used instead.");

// TODO: can we automatically choose a startup grace period based on the max admission
// control queue timeout + some margin for error?
DEFINE_int64(shutdown_grace_period_s, 120, "Shutdown startup grace period in seconds. "
    "When the shutdown process is started for this daemon, it will wait for at least the "
    "startup grace period before shutting down. This gives time for updated cluster "
    "membership information to propagate to all coordinators and for fragment instances "
    "that were scheduled based on old cluster membership to start executing (and "
    "therefore be reflected in the metrics used to detect quiescence).");

DEFINE_int64(shutdown_deadline_s, 60 * 60, "Default time limit in seconds for the shut "
    "down process. If this duration elapses after the shut down process is started, "
    "the daemon shuts down regardless of any running queries.");

#ifndef NDEBUG
  DEFINE_int64(stress_metadata_loading_pause_injection_ms, 0, "Simulates metadata loading"
      "for a given query by injecting a sleep equivalent to this configuration in "
      "milliseconds. Only used for testing.");
#endif

DEFINE_int64(accepted_client_cnxn_timeout, 300000,
    "(Advanced) The amount of time in milliseconds an accepted connection will wait in "
    "the post-accept, pre-setup connection queue before it is timed out and the "
    "connection request is rejected. A value of 0 means there is no timeout.");

DEFINE_string(query_event_hook_classes, "", "Comma-separated list of java QueryEventHook "
    "implementation classes to load and register at Impala startup. Class names should "
    "be fully-qualified and on the classpath. Whitespace acceptable around delimiters.");

DEFINE_int32(query_event_hook_nthreads, 1, "Number of threads to use for "
    "QueryEventHook execution. If this number is >1 then hooks will execute "
    "concurrently.");

DECLARE_bool(compact_catalog_topic);

namespace {
using namespace impala;

void SetExecutorGroups(const string& flag, BackendDescriptorPB* be_desc) {
  vector<StringPiece> groups;
  groups = Split(flag, ",", SkipEmpty());
  if (groups.empty()) groups.push_back(ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME);
  DCHECK_EQ(1, groups.size());
  // Name and optional minimum group size are separated by ':'.
  for (const StringPiece& group : groups) {
    int colon_idx = group.find_first_of(':');
    ExecutorGroupDescPB* group_desc = be_desc->add_executor_groups();
    group_desc->set_name(group.substr(0, colon_idx).as_string());
    if (colon_idx != StringPiece::npos) {
      StringParser::ParseResult result;
      group_desc->set_min_size(StringParser::StringToInt<int64_t>(
          group.data() + colon_idx + 1, group.length() - colon_idx - 1, &result));
      if (result != StringParser::PARSE_SUCCESS) {
        LOG(FATAL) << "Failed to parse minimum executor group size from group: "
                     << group.ToString();
      }
    } else {
      group_desc->set_min_size(1);
    }
  }
}
} // end anonymous namespace

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
const string AUDIT_EVENT_LOG_FILE_PREFIX = "impala_audit_event_log_1.0-";
const string LINEAGE_LOG_FILE_PREFIX = "impala_lineage_log_1.0-";

const uint32_t MAX_CANCELLATION_QUEUE_SIZE = 65536;

const string BEESWAX_SERVER_NAME = "beeswax-frontend";
const string HS2_SERVER_NAME = "hiveserver2-frontend";
const string HS2_HTTP_SERVER_NAME = "hiveserver2-http-frontend";

const string ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME = "default";

const char* ImpalaServer::SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION = "42000";
const char* ImpalaServer::SQLSTATE_GENERAL_ERROR = "HY000";
const char* ImpalaServer::SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED = "HYC00";

const char* ImpalaServer::GET_LOG_QUERY_RETRY_INFO_FORMAT =
    "Original query failed:\n$0\nQuery has been retried using query id: $1\n";

// Interval between checks for query expiration.
const int64_t EXPIRATION_CHECK_INTERVAL_MS = 1000L;

// Template to return error messages for client requests that could not be found, belonged
// to the wrong session, or had a mismatched secret. We need to use this particular string
// in some places because the shell has a regex for it.
// TODO: Make consistent "Invalid or unknown query handle: $0" template used elsewhere.
// TODO: this should be turned into a proper error code and used throughout ImpalaServer.
static const char* LEGACY_INVALID_QUERY_HANDLE_TEMPLATE = "Query id $0 not found.";

ThreadSafeRandom ImpalaServer::rng_(GetRandomSeed32());

ImpalaServer::ImpalaServer(ExecEnv* exec_env)
    : exec_env_(exec_env),
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

  status = exec_env_->tmp_file_mgr()->Init(exec_env_->metrics());
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
    Status status = PopulateAuthorizedProxyConfig(FLAGS_authorized_proxy_user_config,
        FLAGS_authorized_proxy_user_config_delimiter, &authorized_proxy_user_config_);
    if (!status.ok()) {
      CLEAN_EXIT_WITH_ERROR(Substitute("Invalid proxy user configuration."
          "No mapping value specified for the proxy user. For more information review "
          "usage of the --authorized_proxy_user_config flag: $0", status.GetDetail()));
    }
  }

  if (!FLAGS_authorized_proxy_group_config.empty()) {
    Status status = PopulateAuthorizedProxyConfig(FLAGS_authorized_proxy_group_config,
        FLAGS_authorized_proxy_group_config_delimiter, &authorized_proxy_group_config_);
    if (!status.ok()) {
      CLEAN_EXIT_WITH_ERROR(Substitute("Invalid proxy group configuration. "
          "No mapping value specified for the proxy group. For more information review "
          "usage of the --authorized_proxy_group_config flag: $0", status.GetDetail()));
    }
  }

  if (FLAGS_disk_spill_encryption) {
    // Initialize OpenSSL for spilling encryption. This is not thread-safe so we
    // initialize it once on startup.
    // TODO: Set OpenSSL callbacks to provide locking to make the library thread-safe.
    OpenSSL_add_all_algorithms();
    ERR_load_crypto_strings();
  }

  ABORT_IF_ERROR(ExternalDataSourceExecutor::InitJNI(exec_env_->metrics()));

  // Register the catalog update callback if running in a real cluster as a coordinator.
  if (!TestInfo::is_test() && FLAGS_is_coordinator) {
    auto catalog_cb = [this] (const StatestoreSubscriber::TopicDeltaMap& state,
        vector<TTopicDelta>* topic_updates) {
      this->CatalogUpdateCallback(state, topic_updates);
    };
    // The 'local-catalog' implementation only needs minimal metadata to
    // trigger cache invalidations.
    // The legacy implementation needs full metadata objects.
    string filter_prefix = FLAGS_use_local_catalog ?
        g_CatalogService_constants.CATALOG_TOPIC_V2_PREFIX :
        g_CatalogService_constants.CATALOG_TOPIC_V1_PREFIX;
    ABORT_IF_ERROR(exec_env->subscriber()->AddTopic(
        CatalogServer::IMPALA_CATALOG_TOPIC, /* is_transient=*/ true,
        /* populate_min_subscriber_topic_version=*/ true,
        filter_prefix, catalog_cb));
  }

  // Initialise the cancellation thread pool with 5 (by default) threads. The max queue
  // size is deliberately set so high that it should never fill; if it does the
  // cancellations will get ignored and retried on the next statestore heartbeat.
  cancellation_thread_pool_.reset(new ThreadPool<CancellationWork>(
          "impala-server", "cancellation-worker",
      FLAGS_cancellation_thread_pool_size, MAX_CANCELLATION_QUEUE_SIZE,
      bind<void>(&ImpalaServer::CancelFromThreadPool, this, _2)));
  ABORT_IF_ERROR(cancellation_thread_pool_->Init());

  unreg_thread_pool_.reset(new ThreadPool<QueryHandle>("impala-server",
      "unregistration-worker", FLAGS_unregistration_thread_pool_size,
      FLAGS_unregistration_thread_pool_queue_depth,
      bind<void>(&ImpalaServer::FinishUnregisterQuery, this, _2)));
  ABORT_IF_ERROR(unreg_thread_pool_->Init());

  // Initialize a session expiry thread which blocks indefinitely until the first session
  // with non-zero timeout value is opened. Note that a session which doesn't specify any
  // idle session timeout value will use the default value FLAGS_idle_session_timeout.
  ABORT_IF_ERROR(Thread::Create("impala-server", "session-maintenance",
      bind<void>(&ImpalaServer::SessionMaintenance, this), &session_maintenance_thread_));

  ABORT_IF_ERROR(Thread::Create("impala-server", "query-expirer",
      bind<void>(&ImpalaServer::ExpireQueries, this), &query_expiration_thread_));

  // Only enable the unresponsive backend thread if periodic status reporting is enabled.
  if (FLAGS_status_report_interval_ms > 0) {
    if (FLAGS_status_report_max_retry_s * 1000 <= FLAGS_status_report_interval_ms) {
      const string& err = "Since --status_report_max_retry_s <= "
          "--status_report_interval_ms, most queries will likely be cancelled.";
      LOG(ERROR) << err;
      if (FLAGS_abort_on_config_error) {
        CLEAN_EXIT_WITH_ERROR(Substitute("Aborting Impala Server startup: $0", err));
      }
    }
    if (FLAGS_status_report_cancellation_padding <= 0) {
      const string& err = "--status_report_cancellationn_padding should be > 0.";
      LOG(ERROR) << err;
      if (FLAGS_abort_on_config_error) {
        CLEAN_EXIT_WITH_ERROR(Substitute("Aborting Impala Server startup: $0", err));
      }
    }
    ABORT_IF_ERROR(Thread::Create("impala-server", "unresponsive-backend-thread",
        bind<void>(&ImpalaServer::UnresponsiveBackendThread, this),
        &unresponsive_backend_thread_));
  }

  is_coordinator_ = FLAGS_is_coordinator;
  is_executor_ = FLAGS_is_executor;
}

Status ImpalaServer::PopulateAuthorizedProxyConfig(
    const string& authorized_proxy_config,
    const string& authorized_proxy_config_delimiter,
    AuthorizedProxyMap* authorized_proxy_config_map) {
  // Parse the proxy user configuration using the format:
  // <proxy user>=<comma separated list of users/groups they are allowed to delegate>
  // See FLAGS_authorized_proxy_user_config or FLAGS_authorized_proxy_group_config
  // for more details.
  vector<string> proxy_config;
  split(proxy_config, authorized_proxy_config, is_any_of(";"),
      token_compress_on);
  if (proxy_config.size() > 0) {
    for (const string& config: proxy_config) {
      size_t pos = config.find("=");
      if (pos == string::npos) {
        return Status(config);
      }
      string proxy_user = config.substr(0, pos);
      boost::trim(proxy_user);
      string config_str = config.substr(pos + 1);
      boost::trim(config_str);
      vector<string> parsed_allowed_users_or_groups;
      split(parsed_allowed_users_or_groups, config_str,
          is_any_of(authorized_proxy_config_delimiter), token_compress_on);
      unordered_set<string> allowed_users_or_groups(
          parsed_allowed_users_or_groups.begin(), parsed_allowed_users_or_groups.end());
      authorized_proxy_config_map->insert({proxy_user, allowed_users_or_groups});
    }
  }
  return Status::OK();
}

bool ImpalaServer::IsCoordinator() { return is_coordinator_; }

bool ImpalaServer::IsExecutor() { return is_executor_; }

bool ImpalaServer::IsHealthy() { return services_started_.load(); }

int ImpalaServer::GetThriftBackendPort() {
  DCHECK(thrift_be_server_ != nullptr);
  return thrift_be_server_->port();
}

TNetworkAddress ImpalaServer::GetThriftBackendAddress() {
  return MakeNetworkAddress(FLAGS_hostname, GetThriftBackendPort());
}

int ImpalaServer::GetBeeswaxPort() {
  DCHECK(beeswax_server_ != nullptr);
  return beeswax_server_->port();
}

int ImpalaServer::GetHS2Port() {
  DCHECK(hs2_server_ != nullptr);
  return hs2_server_->port();
}

bool ImpalaServer::IsLineageLoggingEnabled() {
  return !FLAGS_lineage_event_log_dir.empty();
}

bool ImpalaServer::AreQueryHooksEnabled() {
  return !FLAGS_query_event_hook_classes.empty();
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

bool ImpalaServer::IsAuditEventLoggingEnabled() {
  return !FLAGS_audit_event_log_dir.empty();
}

Status ImpalaServer::InitAuditEventLogging() {
  if (!IsAuditEventLoggingEnabled()) {
    LOG(INFO) << "Event logging is disabled";
    return Status::OK();
  }
  audit_event_logger_.reset(new SimpleLogger(FLAGS_audit_event_log_dir,
      AUDIT_EVENT_LOG_FILE_PREFIX, FLAGS_max_audit_event_log_file_size,
      FLAGS_max_audit_event_log_files));
  RETURN_IF_ERROR(audit_event_logger_->Init());
  RETURN_IF_ERROR(Thread::Create("impala-server", "audit-event-log-flush",
      &ImpalaServer::AuditEventLoggerFlushThread, this,
      &audit_event_logger_flush_thread_));
  return Status::OK();
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

Status ImpalaServer::AppendAuditEntry(const string& entry ) {
  DCHECK(IsAuditEventLoggingEnabled());
  return audit_event_logger_->AppendEntry(entry);
}

Status ImpalaServer::AppendLineageEntry(const string& entry ) {
  DCHECK(IsLineageLoggingEnabled());
  return lineage_logger_->AppendEntry(entry);
}

Status ImpalaServer::GetRuntimeProfileOutput(const TUniqueId& query_id,
    const string& user, TRuntimeProfileFormat::type format, stringstream* output,
    TRuntimeProfileTree* thrift_output, Document* json_output) {
  DCHECK(output != nullptr);
  // Search for the query id in the active query map
  {
    QueryHandle query_handle;
    Status status = GetQueryHandle(query_id, &query_handle,
        /*return_unregistered=*/ true);
    if (status.ok()) {
      // For queries in INITIALIZED state, the profile information isn't populated yet.
      if (query_handle->exec_state() == ClientRequestState::ExecState::INITIALIZED) {
        return Status::Expected("Query plan is not ready.");
      }
      lock_guard<mutex> l(*query_handle->lock());
      RETURN_IF_ERROR(CheckProfileAccess(user, query_handle->effective_user(),
          query_handle->user_has_profile_access()));
      if (query_handle->GetCoordinator() != nullptr) {
        UpdateExecSummary(query_handle);
      }
      if (format == TRuntimeProfileFormat::BASE64) {
        RETURN_IF_ERROR(query_handle->profile()->SerializeToArchiveString(output));
      } else if (format == TRuntimeProfileFormat::THRIFT) {
        query_handle->profile()->ToThrift(thrift_output);
      } else if (format == TRuntimeProfileFormat::JSON) {
        query_handle->profile()->ToJson(json_output);
      } else {
        DCHECK_EQ(format, TRuntimeProfileFormat::STRING);
        query_handle->profile()->PrettyPrint(output);
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
      string err =
          strings::Substitute(LEGACY_INVALID_QUERY_HANDLE_TEMPLATE, PrintId(query_id));
      VLOG(1) << err;
      return Status::Expected(err);
    }
    RETURN_IF_ERROR(CheckProfileAccess(user, query_record->second->effective_user,
        query_record->second->user_has_profile_access));
    if (format == TRuntimeProfileFormat::BASE64) {
      Base64Encode(query_record->second->compressed_profile, output);
    } else if (format == TRuntimeProfileFormat::THRIFT) {
      RETURN_IF_ERROR(RuntimeProfile::DecompressToThrift(
          query_record->second->compressed_profile, thrift_output));
    } else if (format == TRuntimeProfileFormat::JSON) {
      ObjectPool tmp_pool;
      RuntimeProfile* tmp_profile;
      RETURN_IF_ERROR(RuntimeProfile::DecompressToProfile(
          query_record->second->compressed_profile, &tmp_pool, &tmp_profile));
      tmp_profile->ToJson(json_output);
    } else {
      DCHECK_EQ(format, TRuntimeProfileFormat::STRING);
      ObjectPool tmp_pool;
      RuntimeProfile* tmp_profile;
      RETURN_IF_ERROR(RuntimeProfile::DecompressToProfile(
          query_record->second->compressed_profile, &tmp_pool, &tmp_profile));
      tmp_profile->PrettyPrint(output);
    }
  }
  return Status::OK();
}

Status ImpalaServer::GetExecSummary(const TUniqueId& query_id, const string& user,
    TExecSummary* result) {
  // Search for the query id in the active query map.
  {
    // QueryHandle of the current query.
    QueryHandle query_handle;
    // QueryHandle or the original query if the query is retried.
    QueryHandle original_query_handle;
    Status status = GetAllQueryHandles(query_id, &query_handle, &original_query_handle,
        /*return_unregistered=*/ true);
    if (status.ok()) {
      lock_guard<mutex> l(*query_handle->lock());
      RETURN_IF_ERROR(CheckProfileAccess(user, query_handle->effective_user(),
          query_handle->user_has_profile_access()));
      if (query_handle->exec_state() == ClientRequestState::ExecState::PENDING) {
        const string* admission_result = query_handle->summary_profile()->GetInfoString(
            AdmissionController::PROFILE_INFO_KEY_ADMISSION_RESULT);
        if (admission_result != nullptr) {
          if (*admission_result == AdmissionController::PROFILE_INFO_VAL_QUEUED) {
            result->__set_is_queued(true);
            const string* queued_reason = query_handle->summary_profile()->GetInfoString(
                AdmissionController::PROFILE_INFO_KEY_LAST_QUEUED_REASON);
            if (queued_reason != nullptr) {
              result->__set_queued_reason(*queued_reason);
            }
          }
        }
      } else if (query_handle->GetCoordinator() != nullptr) {
        query_handle->GetCoordinator()->GetTExecSummary(result);
        TExecProgress progress;
        progress.__set_num_completed_scan_ranges(
            query_handle->GetCoordinator()->progress().num_complete());
        progress.__set_total_scan_ranges(
            query_handle->GetCoordinator()->progress().total());
        // TODO: does this not need to be synchronized?
        result->__set_progress(progress);
      } else {
        *result = TExecSummary();
      }
      if (query_handle->IsRetriedQuery()) {
        // Don't need to acquire lock on original_query_handle since the query is
        // finished. There are no concurrent updates on its status.
        result->error_logs.push_back(original_query_handle->query_status().GetDetail());
        result->error_logs.push_back(Substitute("Retrying query using query id: $0",
            PrintId(query_handle->query_id())));
        result->__isset.error_logs = true;
      }
      return Status::OK();
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
      string err =
          strings::Substitute(LEGACY_INVALID_QUERY_HANDLE_TEMPLATE, PrintId(query_id));
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

void ImpalaServer::ArchiveQuery(const QueryHandle& query_handle) {
  vector<uint8_t> compressed_profile;
  Status status = query_handle->profile()->Compress(&compressed_profile);
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
    ss << UnixMillis() << " " << PrintId(query_handle->query_id()) << " ";
    Base64Encode(compressed_profile, &ss);
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
  // 'fetch_rows_lock()' protects several fields in ClientReqestState that are read
  // during QueryStateRecord creation. There should be no contention on this lock because
  // the query has already been closed (e.g. no more results can be fetched).
  unique_ptr<QueryStateRecord> record = nullptr;
  {
    lock_guard<mutex> l(*query_handle->fetch_rows_lock());
    record = make_unique<QueryStateRecord>(*query_handle, move(compressed_profile));
  }
  if (query_handle->GetCoordinator() != nullptr) {
    query_handle->GetCoordinator()->GetTExecSummary(&record->exec_summary);
  }
  {
    lock_guard<mutex> l(query_log_lock_);
    // Add record to the beginning of the log, and to the lookup index.
    query_log_index_[query_handle->query_id()] = record.get();
    query_log_.insert(query_log_.begin(), move(record));

    if (FLAGS_query_log_size > -1 && FLAGS_query_log_size < query_log_.size()) {
      DCHECK_EQ(query_log_.size() - FLAGS_query_log_size, 1);
      query_log_index_.erase(query_log_.back()->id);
      query_log_.pop_back();
    }
  }
}

ImpalaServer::~ImpalaServer() {}

void ImpalaServer::AddPoolConfiguration(TQueryCtx* ctx,
    const QueryOptionsMask& override_options_mask) {
  // Errors are not returned and are only logged (at level 2) because some incoming
  // requests are not expected to be mapped to a pool and will not have query options,
  // e.g. 'use [db];'. For requests that do need to be mapped to a pool successfully, the
  // pool is resolved again during scheduling and errors are handled at that point.
  string resolved_pool;
  Status status = exec_env_->request_pool_service()->ResolveRequestPool(*ctx,
      &resolved_pool);
  if (!status.ok()) {
    VLOG_RPC << "Not adding pool query options for query=" << PrintId(ctx->query_id)
             << " ResolveRequestPool status: " << status.GetDetail();
    return;
  }
  ctx->__set_request_pool(resolved_pool);

  TPoolConfig config;
  status = exec_env_->request_pool_service()->GetPoolConfig(resolved_pool, &config);
  if (!status.ok()) {
    VLOG_RPC << "Not adding pool query options for query=" << PrintId(ctx->query_id)
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

  // Enforce the max mt_dop after the defaults and overlays have already been done.
  EnforceMaxMtDop(ctx, config.max_mt_dop);

  status = ValidateQueryOptions(&pool_options);
  if (!status.ok()) {
    VLOG_QUERY << "Ignoring errors while validating default query options for pool="
               << resolved_pool << ", message: " << status.GetDetail();
  }
}

void ImpalaServer::EnforceMaxMtDop(TQueryCtx* query_ctx, int64_t max_mt_dop) {
  TQueryOptions& query_options = query_ctx->client_request.query_options;
  // The mt_dop is overridden if all three conditions are met:
  // 1. There is a nonnegative max mt_dop setting
  // 2. The mt_dop query option is set
  // 3. The specified mt_dop is larger than the max mt_dop setting
  if (max_mt_dop >= 0 && query_options.__isset.mt_dop &&
      max_mt_dop < query_options.mt_dop) {
    query_ctx->__set_overridden_mt_dop_value(query_options.mt_dop);
    query_options.__set_mt_dop(max_mt_dop);
  }
}

Status ImpalaServer::Execute(TQueryCtx* query_ctx, shared_ptr<SessionState> session_state,
    QueryHandle* query_handle) {
  PrepareQueryContext(query_ctx);
  ScopedThreadContext debug_ctx(GetThreadDebugInfo(), query_ctx->query_id);
  ImpaladMetrics::IMPALA_SERVER_NUM_QUERIES->Increment(1L);

  // Redact the SQL stmt and update the query context
  string stmt = replace_all_copy(query_ctx->client_request.stmt, "\n", " ");
  Redact(&stmt);
  query_ctx->client_request.__set_redacted_stmt((const string) stmt);

  bool registered_query = false;
  Status status = ExecuteInternal(*query_ctx, session_state, &registered_query,
      query_handle);
  if (!status.ok() && registered_query) {
    UnregisterQueryDiscardResult((*query_handle)->query_id(), false, &status);
  }
  return status;
}

Status ImpalaServer::ExecuteInternal(const TQueryCtx& query_ctx,
    shared_ptr<SessionState> session_state, bool* registered_query,
    QueryHandle* query_handle) {
  DCHECK(session_state != nullptr);
  DCHECK(query_handle != nullptr);
  DCHECK(registered_query != nullptr);
  *registered_query = false;

  // Create the QueryDriver for this query. CreateNewDriver creates the associated
  // ClientRequestState as well.
  QueryDriver::CreateNewDriver(this, query_handle, query_ctx, session_state);

  (*query_handle)->query_events()->MarkEvent("Query submitted");

  {
    // Keep a lock on query_handle so that registration and setting
    // result_metadata are atomic.
    lock_guard<mutex> l(*(*query_handle)->lock());

    // register exec state as early as possible so that queries that
    // take a long time to plan show up, and to handle incoming status
    // reports before execution starts.
    RETURN_IF_ERROR(RegisterQuery(query_ctx.query_id, session_state, query_handle));
    *registered_query = true;

#ifndef NDEBUG
    // Inject a sleep to simulate metadata loading pauses for tables. This
    // is only used for testing.
    if (FLAGS_stress_metadata_loading_pause_injection_ms > 0) {
      SleepForMs(FLAGS_stress_metadata_loading_pause_injection_ms);
    }
#endif

    size_t statement_length = query_ctx.client_request.stmt.length();
    int32_t max_statement_length =
        query_ctx.client_request.query_options.max_statement_length_bytes;
    if (max_statement_length > 0 && statement_length > max_statement_length) {
      return Status(ErrorMsg(TErrorCode::MAX_STATEMENT_LENGTH_EXCEEDED,
          statement_length, max_statement_length));
    }

    // Takes the TQueryCtx and calls into the frontend to initialize the TExecRequest for
    // this query.
    RETURN_IF_ERROR(query_handle->query_driver()->RunFrontendPlanner(query_ctx));

    const TExecRequest& result = (*query_handle)->exec_request();
    (*query_handle)->query_events()->MarkEvent("Planning finished");
    (*query_handle)->set_user_profile_access(result.user_has_profile_access);
    (*query_handle)->summary_profile()->AddEventSequence(
        result.timeline.name, result.timeline);
    (*query_handle)->SetFrontendProfile(result.profile);
    if (result.__isset.result_set_metadata) {
      (*query_handle)->set_result_metadata(result.result_set_metadata);
    }
  }
  VLOG(2) << "Execution request: "
          << ThriftDebugString((*query_handle)->exec_request());

  // start execution of query; also starts fragment status reports
  RETURN_IF_ERROR((*query_handle)->Exec());
  Status status = UpdateCatalogMetrics();
  if (!status.ok()) {
    VLOG_QUERY << "Couldn't update catalog metrics: " << status.GetDetail();
  }
  return Status::OK();
}

void ImpalaServer::PrepareQueryContext(TQueryCtx* query_ctx) {
  PrepareQueryContext(GetThriftBackendAddress(),
      ExecEnv::GetInstance()->krpc_address(), query_ctx);
}

void ImpalaServer::PrepareQueryContext(const TNetworkAddress& backend_addr,
    const TNetworkAddress& krpc_addr, TQueryCtx* query_ctx) {
  query_ctx->__set_pid(getpid());
  int64_t now_us = UnixMicros();
  const Timezone& utc_tz = TimezoneDatabase::GetUtcTimezone();
  // Fill in query options with default timezone so it is visible in "SET" command,
  // profiles, etc.
  if (query_ctx->client_request.query_options.timezone.empty()) {
    query_ctx->client_request.query_options.timezone = TimezoneDatabase::LocalZoneName();
  }
  string local_tz_name = query_ctx->client_request.query_options.timezone;
  const Timezone* local_tz = TimezoneDatabase::FindTimezone(local_tz_name);
  if (local_tz != nullptr) {
    LOG(INFO) << "Found local timezone \"" << local_tz_name << "\".";
  } else {
    LOG(ERROR) << "Failed to find local timezone \"" << local_tz_name
        << "\". Falling back to UTC";
    local_tz_name = "UTC";
    local_tz = &utc_tz;
  }
  query_ctx->__set_utc_timestamp_string(ToStringFromUnixMicros(now_us, utc_tz));
  if (query_ctx->client_request.query_options.now_string.empty()) {
    query_ctx->__set_now_string(ToStringFromUnixMicros(now_us, *local_tz));
  } else {
    // For testing purposes
    query_ctx->__set_now_string(query_ctx->client_request.query_options.now_string);
  }
  query_ctx->__set_start_unix_millis(now_us / MICROS_PER_MILLI);
  query_ctx->__set_coord_address(backend_addr);
  query_ctx->__set_coord_krpc_address(krpc_addr);
  TUniqueId backend_id;
  UniqueIdPBToTUniqueId(ExecEnv::GetInstance()->backend_id(), &backend_id);
  query_ctx->__set_coord_backend_id(backend_id);
  query_ctx->__set_local_time_zone(local_tz_name);
  query_ctx->__set_status_report_interval_ms(FLAGS_status_report_interval_ms);
  query_ctx->__set_status_report_max_retry_s(FLAGS_status_report_max_retry_s);

  // Creating a random_generator every time is not free, but
  // benchmarks show it to be slightly cheaper than contending for a
  // single generator under a lock (since random_generator is not
  // thread-safe).
  // TODO: as cleanup we should consolidate this with uuid_generator_ - there's no reason
  // to have two different methods to achieve the same end. To address the scalability
  // concern we could shard the RNG or similar.
  query_ctx->query_id = UuidToQueryId(random_generator()());
  GetThreadDebugInfo()->SetQueryId(query_ctx->query_id);

  const double trace_ratio = query_ctx->client_request.query_options.resource_trace_ratio;
  if (trace_ratio > 0 && rng_.NextDoubleFraction() < trace_ratio) {
    query_ctx->__set_trace_resource_usage(true);
  }
}

Status ImpalaServer::RegisterQuery(const TUniqueId& query_id,
    shared_ptr<SessionState> session_state, QueryHandle* query_handle) {
  lock_guard<mutex> l2(session_state->lock);
  // The session wasn't expired at the time it was checked out and it isn't allowed to
  // expire while checked out, so it must not be expired.
  DCHECK_GT(session_state->ref_count, 0);
  DCHECK(!session_state->expired);
  // The session may have been closed after it was checked out.
  if (session_state->closed) {
    VLOG(1) << "RegisterQuery(): session has been closed, ignoring query.";
    return Status::Expected("Session has been closed, ignoring query.");
  }
  DCHECK_EQ(this, ExecEnv::GetInstance()->impala_server());
  RETURN_IF_ERROR(
      query_driver_map_.AddQueryDriver(query_id, query_handle->query_driver()));
  // Metric is decremented in UnregisterQuery().
  ImpaladMetrics::NUM_QUERIES_REGISTERED->Increment(1L);
  VLOG_QUERY << "Registered query query_id=" << PrintId(query_id) << " session_id="
             << PrintId(session_state->session_id);
  return Status::OK();
}

Status ImpalaServer::SetQueryInflight(
    shared_ptr<SessionState> session_state, const QueryHandle& query_handle) {
  const TUniqueId& query_id = query_handle->query_id();
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
  int32_t idle_timeout_s = query_handle->query_options().query_timeout_s;
  if (FLAGS_idle_query_timeout > 0 && idle_timeout_s > 0) {
    idle_timeout_s = min(FLAGS_idle_query_timeout, idle_timeout_s);
  } else {
    // Use a non-zero timeout, if one exists
    idle_timeout_s = max(FLAGS_idle_query_timeout, idle_timeout_s);
  }
  int32_t exec_time_limit_s = query_handle->query_options().exec_time_limit_s;
  int64_t cpu_limit_s = query_handle->query_options().cpu_limit_s;
  int64_t scan_bytes_limit = query_handle->query_options().scan_bytes_limit;
  if (idle_timeout_s > 0 || exec_time_limit_s > 0 ||
        cpu_limit_s > 0 || scan_bytes_limit > 0) {
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
    if (cpu_limit_s > 0 || scan_bytes_limit > 0) {
      if (cpu_limit_s > 0) {
        VLOG_QUERY << "Query " << PrintId(query_id) << " has CPU limit of "
                   << PrettyPrinter::Print(cpu_limit_s, TUnit::TIME_S);
      }
      if (scan_bytes_limit > 0) {
        VLOG_QUERY << "Query " << PrintId(query_id) << " has scan bytes limit of "
                   << PrettyPrinter::Print(scan_bytes_limit, TUnit::BYTES);
      }
      queries_by_timestamp_.emplace(ExpirationEvent{
          now + EXPIRATION_CHECK_INTERVAL_MS, query_id, ExpirationKind::RESOURCE_LIMIT});
    }
  }
  return Status::OK();
}

void ImpalaServer::UpdateExecSummary(const QueryHandle& query_handle) const {
  DCHECK(query_handle->GetCoordinator() != nullptr);
  TExecSummary t_exec_summary;
  query_handle->GetCoordinator()->GetTExecSummary(&t_exec_summary);
  query_handle->summary_profile()->SetTExecSummary(t_exec_summary);
  string exec_summary = PrintExecSummary(t_exec_summary);
  query_handle->summary_profile()->AddInfoStringRedacted("ExecSummary", exec_summary);
  query_handle->summary_profile()->AddInfoStringRedacted("Errors",
      query_handle->GetCoordinator()->GetErrorLog());
}

Status ImpalaServer::UnregisterQuery(const TUniqueId& query_id, bool check_inflight,
    const Status* cause) {
  VLOG_QUERY << "UnregisterQuery(): query_id=" << PrintId(query_id);

  QueryHandle query_handle;
  RETURN_IF_ERROR(GetActiveQueryHandle(query_id, &query_handle));

  // Do the work of unregistration that needs to be done synchronously. Once
  // Finalize() returns, the query is considered unregistered from the client's point of
  // view. If Finalize() returns OK, this thread is responsible for doing the
  // unregistration work. Finalize() succeeds for the first thread to call it to avoid
  // multiple threads unregistering.
  RETURN_IF_ERROR(
      query_handle.query_driver()->Finalize(&query_handle, check_inflight, cause));

  // Do the rest of the unregistration work in the background so that the client does
  // not need to wait for profile serialization, etc.
  unreg_thread_pool_->Offer(move(query_handle));
  return Status::OK();
}

void ImpalaServer::FinishUnregisterQuery(const QueryHandle& query_handle) {
  DCHECK_EQ(this, ExecEnv::GetInstance()->impala_server());
  // Do all the finalization before removing the QueryDriver from the map so that
  // concurrent operations, e.g. GetRuntimeProfile() can find the query.
  CloseClientRequestState(query_handle);
  // Make the QueryDriver inaccessible. There is a time window where the query is
  // both in 'query_driver_map_' and 'query_locations_'.
  Status status = query_handle.query_driver()->Unregister(&query_driver_map_);
  string err_msg = "QueryDriver can only be deleted once: " + status.GetDetail();
  DCHECK(status.ok()) << err_msg;
  if (UNLIKELY(!status.ok())) {
    LOG(ERROR) << status.GetDetail();
  } else {
    VLOG_QUERY << "Query successfully unregistered: query_id="
               << PrintId(query_handle->query_id());
  }
}

void ImpalaServer::UnregisterQueryDiscardResult(
    const TUniqueId& query_id, bool check_inflight, const Status* cause) {
  Status status = UnregisterQuery(query_id, check_inflight, cause);
  if (!status.ok()) {
    LOG(ERROR) << Substitute("Query de-registration for query_id={0} failed: {1}",
        PrintId(query_id, cause->GetDetail()));
  }
}

void ImpalaServer::CloseClientRequestState(const QueryHandle& query_handle) {
  int64_t duration_us = query_handle->end_time_us() - query_handle->start_time_us();
  int64_t duration_ms = duration_us / MICROS_PER_MILLI;

  // duration_ms can be negative when the local timezone changes during query execution.
  if (duration_ms >= 0) {
    if (query_handle->stmt_type() == TStmtType::DDL) {
      ImpaladMetrics::DDL_DURATIONS->Update(duration_ms);
    } else {
      ImpaladMetrics::QUERY_DURATIONS->Update(duration_ms);
    }
  }
  {
    lock_guard<mutex> l(query_handle->session()->lock);
    query_handle->session()->inflight_queries.erase(query_handle->query_id());
  }

  if (query_handle->GetCoordinator() != nullptr) {
    UpdateExecSummary(query_handle);
  }

  if (query_handle->schedule() != nullptr) {
    const RepeatedPtrField<BackendExecParamsPB>& backend_exec_params =
        query_handle->schedule()->backend_exec_params();
    if (!backend_exec_params.empty()) {
      lock_guard<mutex> l(query_locations_lock_);
      for (const BackendExecParamsPB& param : backend_exec_params) {
        // Query may have been removed already by cancellation path. In particular, if
        // node to fail was last sender to an exchange, the coordinator will realise and
        // fail the query at the same time the failure detection path does the same
        // thing. They will harmlessly race to remove the query from this map.
        auto it = query_locations_.find(param.backend_id());
        if (it != query_locations_.end()) {
          it->second.query_ids.erase(query_handle->query_id());
        }
      }
    }
  }
  ArchiveQuery(query_handle);
  ImpaladMetrics::NUM_QUERIES_REGISTERED->Increment(-1L);
}

Status ImpalaServer::UpdateCatalogMetrics() {
  TGetCatalogMetricsResult metrics;
  RETURN_IF_ERROR(exec_env_->frontend()->GetCatalogMetrics(&metrics));
  ImpaladMetrics::CATALOG_NUM_DBS->SetValue(metrics.num_dbs);
  ImpaladMetrics::CATALOG_NUM_TABLES->SetValue(metrics.num_tables);
  if (!FLAGS_use_local_catalog) return Status::OK();
  DCHECK(metrics.__isset.cache_eviction_count);
  DCHECK(metrics.__isset.cache_hit_count);
  DCHECK(metrics.__isset.cache_load_count);
  DCHECK(metrics.__isset.cache_load_exception_count);
  DCHECK(metrics.__isset.cache_load_success_count);
  DCHECK(metrics.__isset.cache_miss_count);
  DCHECK(metrics.__isset.cache_request_count);
  DCHECK(metrics.__isset.cache_total_load_time);
  DCHECK(metrics.__isset.cache_avg_load_time);
  DCHECK(metrics.__isset.cache_hit_rate);
  DCHECK(metrics.__isset.cache_load_exception_rate);
  DCHECK(metrics.__isset.cache_miss_rate);
  ImpaladMetrics::CATALOG_CACHE_EVICTION_COUNT->SetValue(metrics.cache_eviction_count);
  ImpaladMetrics::CATALOG_CACHE_HIT_COUNT->SetValue(metrics.cache_hit_count);
  ImpaladMetrics::CATALOG_CACHE_LOAD_COUNT->SetValue(metrics.cache_load_count);
  ImpaladMetrics::CATALOG_CACHE_LOAD_EXCEPTION_COUNT->SetValue(
      metrics.cache_load_exception_count);
  ImpaladMetrics::CATALOG_CACHE_LOAD_SUCCESS_COUNT->SetValue(
      metrics.cache_load_success_count);
  ImpaladMetrics::CATALOG_CACHE_MISS_COUNT->SetValue(metrics.cache_miss_count);
  ImpaladMetrics::CATALOG_CACHE_REQUEST_COUNT->SetValue(metrics.cache_request_count);
  ImpaladMetrics::CATALOG_CACHE_TOTAL_LOAD_TIME->SetValue(metrics.cache_total_load_time);
  ImpaladMetrics::CATALOG_CACHE_AVG_LOAD_TIME->SetValue(metrics.cache_avg_load_time);
  ImpaladMetrics::CATALOG_CACHE_HIT_RATE->SetValue(metrics.cache_hit_rate);
  ImpaladMetrics::CATALOG_CACHE_LOAD_EXCEPTION_RATE->SetValue(
      metrics.cache_load_exception_rate);
  ImpaladMetrics::CATALOG_CACHE_MISS_RATE->SetValue(metrics.cache_miss_rate);
  return Status::OK();

}

shared_ptr<QueryDriver> ImpalaServer::GetQueryDriver(
    const TUniqueId& query_id, bool return_unregistered) {
  DCHECK_EQ(this, ExecEnv::GetInstance()->impala_server());
  ScopedShardedMapRef<std::shared_ptr<QueryDriver>> map_ref(query_id, &query_driver_map_);
  DCHECK(map_ref.get() != nullptr);

  auto entry = map_ref->find(query_id);
  if (entry == map_ref->end()) return shared_ptr<QueryDriver>();

  // This started_unregister() check can race with unregistration. It cannot prevent
  // unregistration starting immediately after the value is loaded. This check, however,
  // is sufficient to ensure that after a client operation has unregistered the request,
  // subsequent operations won't spuriously find the request.
  if (!return_unregistered && entry->second->finalized()) {
    return shared_ptr<QueryDriver>();
  }
  return entry->second;
}

Status ImpalaServer::GetActiveQueryHandle(
    const TUniqueId& query_id, QueryHandle* query_handle) {
  DCHECK(query_handle != nullptr);
  shared_ptr<QueryDriver> query_driver = GetQueryDriver(query_id);
  if (UNLIKELY(query_driver == nullptr)) {
    Status err = Status::Expected(TErrorCode::INVALID_QUERY_HANDLE, PrintId(query_id));
    VLOG(1) << err.GetDetail();
    return err;
  }
  query_handle->SetHandle(query_driver, query_driver->GetActiveClientRequestState());
  return Status::OK();
}

Status ImpalaServer::GetQueryHandle(
    const TUniqueId& query_id, QueryHandle* query_handle, bool return_unregistered) {
  DCHECK(query_handle != nullptr);
  shared_ptr<QueryDriver> query_driver = GetQueryDriver(query_id, return_unregistered);
  if (UNLIKELY(query_driver == nullptr)) {
    Status err = Status::Expected(TErrorCode::INVALID_QUERY_HANDLE, PrintId(query_id));
    VLOG(1) << err.GetDetail();
    return err;
  }
  query_handle->SetHandle(query_driver, query_driver->GetClientRequestState(query_id));
  return Status::OK();
}

Status ImpalaServer::GetAllQueryHandles(const TUniqueId& query_id,
    QueryHandle* active_query_handle, QueryHandle* original_query_handle,
    bool return_unregistered) {
  DCHECK(active_query_handle != nullptr);
  DCHECK(original_query_handle != nullptr);
  shared_ptr<QueryDriver> query_driver = GetQueryDriver(query_id, return_unregistered);
  if (UNLIKELY(query_driver == nullptr)) {
    Status err = Status::Expected(TErrorCode::INVALID_QUERY_HANDLE, PrintId(query_id));
    VLOG(1) << err.GetDetail();
    return err;
  }
  active_query_handle->SetHandle(query_driver,
      query_driver->GetActiveClientRequestState());
  original_query_handle->SetHandle(query_driver,
      query_driver->GetClientRequestState(query_id));
  return Status::OK();
}

Status ImpalaServer::CancelInternal(const TUniqueId& query_id) {
  VLOG_QUERY << "Cancel(): query_id=" << PrintId(query_id);
  QueryHandle query_handle;
  RETURN_IF_ERROR(GetActiveQueryHandle(query_id, &query_handle));
  RETURN_IF_ERROR(
      query_handle->Cancel(/*check_inflight=*/ true, /*cause=*/ nullptr));
  return Status::OK();
}

Status ImpalaServer::CloseSessionInternal(const TUniqueId& session_id,
    const SecretArg& secret, bool ignore_if_absent) {
  DCHECK(secret.is_session_secret());
  VLOG_QUERY << "Closing session: " << PrintId(session_id);

  // Find the session_state and remove it from the map.
  shared_ptr<SessionState> session_state;
  {
    lock_guard<mutex> l(session_state_map_lock_);
    SessionStateMap::iterator entry = session_state_map_.find(session_id);
    if (entry == session_state_map_.end() || !secret.Validate(entry->second->secret)) {
      if (ignore_if_absent) {
        return Status::OK();
      } else {
        if (entry != session_state_map_.end()) {
          // Log invalid attempts to connect. Be careful not to log secret.
          VLOG(1) << "Client tried to connect to session " << PrintId(session_id)
                  << " with invalid secret.";
        }
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
    UnregisterQueryDiscardResult(query_id, false, &status);
  }
  // Reconfigure the poll period of session_maintenance_thread_ if necessary.
  UnregisterSessionTimeout(session_state->session_timeout);
  VLOG_QUERY << "Closed session: " << PrintId(session_id);
  return Status::OK();
}

Status ImpalaServer::GetSessionState(const TUniqueId& session_id, const SecretArg& secret,
    shared_ptr<SessionState>* session_state, bool mark_active) {
  lock_guard<mutex> l(session_state_map_lock_);
  SessionStateMap::iterator i = session_state_map_.find(session_id);
  // TODO: consider factoring out the lookup and secret validation into a separate method.
  // This would require rethinking the locking protocol for 'session_state_map_lock_' -
  // it probably doesn't not need to be held for the full duration of this function.
  if (i == session_state_map_.end() || !secret.Validate(i->second->secret)) {
    if (i != session_state_map_.end()) {
      // Log invalid attempts to connect. Be careful not to log secret.
      VLOG(1) << "Client tried to connect to session " << PrintId(session_id)
              << " with invalid "
              << (secret.is_session_secret() ? "session" : "operation") << " secret.";
    }
    *session_state = std::shared_ptr<SessionState>();
    string err_msg = secret.is_session_secret() ?
        Substitute("Invalid session id: $0", PrintId(session_id)) :
        Substitute(LEGACY_INVALID_QUERY_HANDLE_TEMPLATE, PrintId(secret.query_id()));
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

void ImpalaServer::InitializeConfigVariables() {
  // Set idle_session_timeout here to let the SET command return the value of
  // the command line option FLAGS_idle_session_timeout
  default_query_options_.__set_idle_session_timeout(FLAGS_idle_session_timeout);
  QueryOptionsMask set_query_options; // unused
  Status status = ParseQueryOptions(FLAGS_default_query_options,
      &default_query_options_, &set_query_options);
  status.MergeStatus(ValidateQueryOptions(&default_query_options_));
  if (!status.ok()) {
    // Log error and exit if the default query options are invalid.
    CLEAN_EXIT_WITH_ERROR(Substitute("Invalid default query options. Please check "
        "-default_query_options.\n $0", status.GetDetail()));
  }
  LOG(INFO) << "Default query options:" << ThriftDebugString(default_query_options_);

  map<string, string> string_map;
  TQueryOptionsToMap(default_query_options_, &string_map);
  string_map["SUPPORT_START_OVER"] = "false";
  string_map["TIMEZONE"] = TimezoneDatabase::LocalZoneName();
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

Status ImpalaServer::WaitForResults(const TUniqueId& query_id,
    QueryHandle* query_handle, int64_t* block_on_wait_time_us,
    bool* timed_out) {
  // Make sure ClientRequestState::Wait() has completed before fetching rows. Wait()
  // ensures that rows are ready to be fetched (e.g., Wait() opens
  // ClientRequestState::output_exprs_, which are evaluated in
  // ClientRequestState::FetchRows() below).
  RETURN_IF_ERROR(GetActiveQueryHandle(query_id, query_handle));
  BlockOnWait(*query_handle, timed_out, block_on_wait_time_us);

  // After BlockOnWait returns, it is possible that the query did not time out, and that
  // it was instead retried. In that case, wait until the query has been successfully
  // retried and then call GetActiveQueryHandle, which should now return the
  // ClientRequestState for the new query.
  ClientRequestState::RetryState retry_state;
  retry_state = (*query_handle)->retry_state();
  if (retry_state == ClientRequestState::RetryState::RETRYING
      || retry_state == ClientRequestState::RetryState::RETRIED) {
    (*query_handle)->WaitUntilRetried();
    RETURN_IF_ERROR(GetActiveQueryHandle(query_id, query_handle));
    // Call BlockOnWait and then DCHECK that the state is not RETRYING or RETRIED
    BlockOnWait(*query_handle, timed_out, block_on_wait_time_us);
    retry_state = (*query_handle)->retry_state();
    DCHECK(retry_state != ClientRequestState::RetryState::RETRYING
        && retry_state != ClientRequestState::RetryState::RETRIED)
        << "Unexpected state: " << (*query_handle)->RetryStateToString(retry_state);
  }
  return Status::OK();
}

void ImpalaServer::BlockOnWait(QueryHandle& query_handle,
    bool* timed_out, int64_t* block_on_wait_time_us) {
  int64_t fetch_rows_timeout_us = query_handle->fetch_rows_timeout_us();
  *timed_out =
      !query_handle->BlockOnWait(fetch_rows_timeout_us, block_on_wait_time_us);
}

void ImpalaServer::CancelFromThreadPool(const CancellationWork& cancellation_work) {
  const TUniqueId& query_id = cancellation_work.query_id();
  QueryHandle query_handle;
  Status status = GetQueryHandle(query_id, &query_handle);
  // Query was already unregistered.
  if (!status.ok()) {
    VLOG_QUERY << "CancelFromThreadPool(): query " << PrintId(query_id)
               << " already unregistered.";
    return;
  }

  DebugActionNoFail(query_handle->query_options(), "QUERY_CANCELLATION_THREAD");
  Status error;
  switch (cancellation_work.cause()) {
    case CancellationWorkCause::TERMINATED_BY_SERVER:
      error = cancellation_work.error();
      break;
    case CancellationWorkCause::BACKEND_FAILED: {
      // We only want to proceed with cancellation if the backends are still in use for
      // the query.
      vector<NetworkAddressPB> active_backends;
      Coordinator* coord = query_handle->GetCoordinator();
      if (coord == nullptr) {
        // Query hasn't started yet - it still will run on all backends.
        active_backends = cancellation_work.failed_backends();
      } else {
        active_backends = coord->GetActiveBackends(cancellation_work.failed_backends());
      }
      if (active_backends.empty()) {
        VLOG_QUERY << "CancelFromThreadPool(): all failed backends already completed for "
                   << "query " << PrintId(query_id);
        return;
      }
      stringstream msg;
      for (int i = 0; i < active_backends.size(); ++i) {
        msg << active_backends[i];
        if (i + 1 != active_backends.size()) msg << ", ";
      }
      error = Status::Expected(TErrorCode::UNREACHABLE_IMPALADS, msg.str());
      break;
    }
    default:
      DCHECK(false) << static_cast<int>(cancellation_work.cause());
  }

  if (cancellation_work.unregister()) {
    UnregisterQueryDiscardResult(cancellation_work.query_id(), true, &error);
  } else {
    // Retry queries that would otherwise be cancelled due to an impalad leaving the
    // cluster. CancellationWorkCause::BACKEND_FAILED indicates that a backend running
    // the query was removed from the cluster membership due to a statestore heartbeat
    // timeout. Historically, this would cause the Coordinator to cancel all queries
    // running on that backend. Now, Impala attempts to retry the queries instead of
    // cancelling them.
    bool was_retried = false;
    if (cancellation_work.cause() == CancellationWorkCause::BACKEND_FAILED) {
      query_handle.query_driver()->TryQueryRetry(&*query_handle, &error, &was_retried);
    }
    // If the query could not be retried, then cancel the query.
    if (!was_retried) {
      VLOG_QUERY << "CancelFromThreadPool(): cancelling query_id=" << PrintId(query_id);
      Status status = query_handle->Cancel(true, &error);
      if (!status.ok()) {
        VLOG_QUERY << "Query cancellation (" << PrintId(cancellation_work.query_id())
                   << ") did not succeed: " << status.GetDetail();
      }
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
  if (authorized_proxy_user_config_.size() == 0 &&
      authorized_proxy_group_config_.size() == 0) {
    error_msg << " User/group delegation is disabled.";
    string error_msg_str = error_msg.str();
    VLOG(1) << error_msg_str;
    return Status::Expected(error_msg_str);
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
  AuthorizedProxyMap::const_iterator proxy_user =
      authorized_proxy_user_config_.find(short_user);
  if (proxy_user != authorized_proxy_user_config_.end()) {
    boost::unordered_set<string> users = proxy_user->second;
    if (users.find("*") != users.end() ||
        users.find(do_as_user) != users.end()) {
      return Status::OK();
    }
  }

  if (authorized_proxy_group_config_.size() > 0) {
    // Check if the groups of do_as_user are in the authorized proxy groups.
    AuthorizedProxyMap::const_iterator proxy_group =
        authorized_proxy_group_config_.find(short_user);
    if (proxy_group != authorized_proxy_group_config_.end()) {
      boost::unordered_set<string> groups = proxy_group->second;
      if (groups.find("*") != groups.end()) return Status::OK();

      TGetHadoopGroupsRequest req;
      req.__set_user(do_as_user);
      TGetHadoopGroupsResponse res;
      int64_t start = MonotonicMillis();
      Status status = exec_env_->frontend()->GetHadoopGroups(req, &res);
      VLOG_QUERY << "Getting Hadoop groups for user: " << short_user << " took " <<
          (PrettyPrinter::Print(MonotonicMillis() - start, TUnit::TIME_MS));
      if (!status.ok()) {
        LOG(ERROR) << "Error getting Hadoop groups for user: " << short_user << ": "
            << status.GetDetail();
        return status;
      }

      for (const string& do_as_group : res.groups) {
        if (groups.find(do_as_group) != groups.end()) {
          return Status::OK();
        }
      }
    }
  }

  string error_msg_str = error_msg.str();
  VLOG(1) << error_msg_str;
  return Status::Expected(error_msg_str);
}

bool ImpalaServer::IsAuthorizedProxyUser(const string& user) {
  if (user.empty()) return false;

  // Get the short version of the user name (the user name up to the first '/' or '@')
  // from the full principal name.
  size_t end_idx = min(user.find("/"), user.find("@"));
  // If neither are found (or are found at the beginning of the user name),
  // return the username. Otherwise, return the username up to the matching character.
  string short_user(
      end_idx == string::npos || end_idx == 0 ? user : user.substr(0, end_idx));

  return authorized_proxy_user_config_.find(short_user)
      != authorized_proxy_user_config_.end()
      || authorized_proxy_group_config_.find(short_user)
      != authorized_proxy_group_config_.end();
}

void ImpalaServer::CatalogUpdateVersionInfo::UpdateCatalogVersionMetrics()
{
  ImpaladMetrics::CATALOG_VERSION->SetValue(catalog_version);
  ImpaladMetrics::CATALOG_OBJECT_VERSION_LOWER_BOUND->SetValue(
      catalog_object_version_lower_bound);
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
            resp.catalog_object_version_lower_bound;
      }
      catalog_update_info_.catalog_version = resp.new_catalog_version;
      catalog_update_info_.catalog_topic_version = delta.to_version;
      catalog_update_info_.catalog_service_id = resp.catalog_service_id;
      catalog_update_info_.catalog_object_version_lower_bound =
          resp.catalog_object_version_lower_bound;
      catalog_update_info_.UpdateCatalogVersionMetrics();
    }
    ImpaladMetrics::CATALOG_READY->SetValue(resp.new_catalog_version > 0);
    // TODO: deal with an error status
    discard_result(UpdateCatalogMetrics());
  }
  // Always update the minimum subscriber version for the catalog topic.
  {
    unique_lock<mutex> unique_lock(catalog_version_lock_);
    DCHECK(delta.__isset.min_subscriber_topic_version);
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
  int64_t catalog_object_version_lower_bound =
      catalog_update_info_.catalog_object_version_lower_bound;
  // TODO: Set a timeout to eventually break out of this loop if something goes
  //  wrong?
  VLOG_QUERY << "Waiting for local minimum catalog object version to be > "
      << min_req_catalog_object_version << ", current lower bound of local versions: "
      << catalog_object_version_lower_bound;
  while (catalog_update_info_.catalog_service_id == catalog_service_id
      && catalog_update_info_.catalog_object_version_lower_bound <=
          min_req_catalog_object_version) {
    catalog_version_update_cv_.Wait(unique_lock);
  }

  if (catalog_update_info_.catalog_service_id != catalog_service_id) {
    VLOG_QUERY << "Detected change in catalog service ID";
  } else {
    VLOG_QUERY << "Updated catalog object version lower bound: "
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

void ImpalaServer::RegisterQueryLocations(
    const RepeatedPtrField<BackendExecParamsPB>& backend_params,
    const TUniqueId& query_id) {
  VLOG_QUERY << "Registering query locations";
  if (!backend_params.empty()) {
    lock_guard<mutex> l(query_locations_lock_);
    for (const BackendExecParamsPB& param : backend_params) {
      const BackendIdPB& backend_id = param.backend_id();
      auto it = query_locations_.find(backend_id);
      if (it == query_locations_.end()) {
        query_locations_.emplace(
            backend_id, QueryLocationInfo(param.address(), query_id));
      } else {
        it->second.query_ids.insert(query_id);
      }
    }
  }
}

void ImpalaServer::CancelQueriesOnFailedBackends(
    const std::unordered_set<BackendIdPB>& current_membership) {
  // Maps from query id (to be cancelled) to a list of failed Impalads that are
  // the cause of the cancellation. Note that we don't need to use TBackendIds as a single
  // query can't be scheduled on two backends with the same TNetworkAddress so there's no
  // ambiguity, and passing the TNetworkAddresses into the CancellationWork makes them
  // available for generating a user-friendly error message.
  map<TUniqueId, vector<NetworkAddressPB>> queries_to_cancel;
  {
    // Build a list of queries that are running on failed hosts (as evidenced by their
    // absence from the membership list).
    lock_guard<mutex> l(query_locations_lock_);
    QueryLocations::const_iterator loc_entry = query_locations_.begin();
    while (loc_entry != query_locations_.end()) {
      if (current_membership.find(loc_entry->first) == current_membership.end()) {
        // Add failed backend locations to all queries that ran on that backend.
        for (const auto& query_id : loc_entry->second.query_ids) {
          queries_to_cancel[query_id].push_back(loc_entry->second.address);
        }
        loc_entry = query_locations_.erase(loc_entry);
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
    for (const auto& cancellation_entry : queries_to_cancel) {
      stringstream backends_ss;
      for (int i = 0; i < cancellation_entry.second.size(); ++i) {
        backends_ss << cancellation_entry.second[i];
        if (i + 1 != cancellation_entry.second.size()) backends_ss << ", ";
      }
      VLOG_QUERY << "Backends failed for query " << PrintId(cancellation_entry.first)
                 << ", adding to queue to check for cancellation: " << backends_ss.str();
      cancellation_thread_pool_->Offer(CancellationWork::BackendFailure(
          cancellation_entry.first, cancellation_entry.second));
    }
  }
}

std::shared_ptr<const BackendDescriptorPB> ImpalaServer::GetLocalBackendDescriptor() {
  if (!services_started_.load()) return nullptr;

  lock_guard<mutex> l(local_backend_descriptor_lock_);
  // Check if the current backend descriptor needs to be initialized.
  if (local_backend_descriptor_.get() == nullptr) {
    std::shared_ptr<BackendDescriptorPB> new_be_desc =
        std::make_shared<BackendDescriptorPB>();
    BuildLocalBackendDescriptorInternal(new_be_desc.get());
    local_backend_descriptor_ = new_be_desc;
  }

  // Check to see if it needs to be updated.
  if (IsShuttingDown() != local_backend_descriptor_->is_quiescing()) {
    std::shared_ptr<BackendDescriptorPB> new_be_desc =
        std::make_shared<BackendDescriptorPB>(*local_backend_descriptor_);
    new_be_desc->set_is_quiescing(IsShuttingDown());
    local_backend_descriptor_ = new_be_desc;
  }

  return local_backend_descriptor_;
}

void ImpalaServer::BuildLocalBackendDescriptorInternal(BackendDescriptorPB* be_desc) {
  DCHECK(services_started_.load());
  bool is_quiescing = shutting_down_.Load() != 0;

  *be_desc->mutable_backend_id() = exec_env_->backend_id();
  *be_desc->mutable_address() = FromTNetworkAddress(exec_env_->GetThriftBackendAddress());
  be_desc->set_ip_address(exec_env_->ip_address());
  be_desc->set_is_coordinator(FLAGS_is_coordinator);
  be_desc->set_is_executor(FLAGS_is_executor);

  Webserver* webserver = ExecEnv::GetInstance()->webserver();
  if (webserver != nullptr) {
    *be_desc->mutable_debug_http_address() =
        MakeNetworkAddressPB(webserver->hostname(), webserver->port());
    be_desc->set_secure_webserver(webserver->IsSecure());
  }

  const TNetworkAddress& krpc_address = exec_env_->krpc_address();
  DCHECK(IsResolvedAddress(krpc_address));
  *be_desc->mutable_krpc_address() = FromTNetworkAddress(krpc_address);

  be_desc->set_admit_mem_limit(exec_env_->admit_mem_limit());
  be_desc->set_admission_slots(exec_env_->admission_slots());
  be_desc->set_is_quiescing(is_quiescing);
  SetExecutorGroups(FLAGS_executor_groups, be_desc);
}

ImpalaServer::QueryStateRecord::QueryStateRecord(
    const ClientRequestState& query_handle, vector<uint8_t>&& compressed_profile)
  : compressed_profile(compressed_profile) {
  Init(query_handle);
}

ImpalaServer::QueryStateRecord::QueryStateRecord(const ClientRequestState& query_handle)
  : compressed_profile() {
  Init(query_handle);
}

void ImpalaServer::QueryStateRecord::Init(const ClientRequestState& query_handle) {
  id = query_handle.query_id();
  const TExecRequest& request = query_handle.exec_request();

  const string* plan_str = query_handle.summary_profile()->GetInfoString("Plan");
  if (plan_str != nullptr) plan = *plan_str;
  stmt = query_handle.sql_stmt();
  stmt_type = request.stmt_type;
  effective_user = query_handle.effective_user();
  default_db = query_handle.default_db();
  start_time_us = query_handle.start_time_us();
  end_time_us = query_handle.end_time_us();
  has_coord = false;

  Coordinator* coord = query_handle.GetCoordinator();
  if (coord != nullptr) {
    num_complete_fragments = coord->progress().num_complete();
    total_fragments = coord->progress().total();
    has_coord = true;
  }
  beeswax_query_state = query_handle.BeeswaxQueryState();
  ClientRequestState::RetryState retry_state = query_handle.retry_state();
  if (retry_state == ClientRequestState::RetryState::NOT_RETRIED) {
    query_state = _QueryState_VALUES_TO_NAMES.find(beeswax_query_state)->second;
  } else {
    query_state = query_handle.RetryStateToString(retry_state);
  }
  num_rows_fetched = query_handle.num_rows_fetched();
  query_status = query_handle.query_status();

  query_handle.query_events()->ToThrift(&event_sequence);

  // Save the query fragments so that the plan can be visualised.
  for (const TPlanExecInfo& plan_exec_info:
      query_handle.exec_request().query_exec_request.plan_exec_info) {
    fragments.insert(fragments.end(),
        plan_exec_info.fragments.begin(), plan_exec_info.fragments.end());
  }
  all_rows_returned = query_handle.eos();
  last_active_time_ms = query_handle.last_active_ms();
  // For statement types other than QUERY/DML, show an empty string for resource pool
  // to indicate that they are not subjected to admission control.
  if (stmt_type == TStmtType::QUERY || stmt_type == TStmtType::DML) {
    resource_pool = query_handle.request_pool();
  }
  user_has_profile_access = query_handle.user_has_profile_access();
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
    // Generate a secret per Beeswax session so that the HS2 secret validation mechanism
    // prevent accessing of Beeswax sessions from HS2.
    uuid secret_uuid;
    {
      lock_guard<mutex> l(uuid_lock_);
      secret_uuid = crypto_uuid_generator_();
    }
    TUniqueId secret;
    UUIDToTUniqueId(secret_uuid, &secret);
    shared_ptr<SessionState> session_state =
        std::make_shared<SessionState>(this, session_id, secret);
    session_state->closed = false;
    session_state->start_time_ms = UnixMillis();
    session_state->last_accessed_ms = UnixMillis();
    session_state->database = "default";
    session_state->session_timeout = FLAGS_idle_session_timeout;
    session_state->session_type = TSessionType::BEESWAX;
    session_state->network_address = connection_context.network_address;
    session_state->server_default_query_options = &default_query_options_;
    session_state->kudu_latest_observed_ts = 0;
    session_state->connections.insert(connection_context.connection_id);

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
      connection_to_sessions_map_[connection_context.connection_id].insert(session_id);
    }
    ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_BEESWAX_SESSIONS->Increment(1L);
  }
}

void ImpalaServer::ConnectionEnd(
    const ThriftServer::ConnectionContext& connection_context) {
  set<TUniqueId> disconnected_sessions;
  {
    unique_lock<mutex> l(connection_to_sessions_map_lock_);
    ConnectionToSessionMap::iterator it =
        connection_to_sessions_map_.find(connection_context.connection_id);

    // Not every connection must have an associated session
    if (it == connection_to_sessions_map_.end()) return;

    // Sessions are not removed from the map even after they are closed and an entry
    // won't be added to the map unless a session is established.
    DCHECK(!it->second.empty());

    // We don't expect a large number of sessions per connection, so we copy it, so that
    // we can drop the map lock early.
    disconnected_sessions = std::move(it->second);
    connection_to_sessions_map_.erase(it);
  }

  const string connection_id = PrintId(connection_context.connection_id);
  LOG(INFO) << "Connection " << connection_id << " from client "
            << TNetworkAddressToString(connection_context.network_address)
            << " to server " << connection_context.server_name << " closed."
            << " The connection had " << disconnected_sessions.size()
            << " associated session(s).";

  bool close = connection_context.server_name == BEESWAX_SERVER_NAME
      || FLAGS_disconnected_session_timeout <= 0;
  if (close) {
    for (const TUniqueId& session_id : disconnected_sessions) {
      Status status = CloseSessionInternal(session_id, SecretArg::SkipSecretCheck(),
          /* ignore_if_absent= */ true);
      if (!status.ok()) {
        LOG(WARNING) << "Error closing session " << PrintId(session_id) << ": "
                     << status.GetDetail();
      }
    }
  } else {
    DCHECK(connection_context.server_name ==  HS2_SERVER_NAME
        || connection_context.server_name == HS2_HTTP_SERVER_NAME);
    for (const TUniqueId& session_id : disconnected_sessions) {
      shared_ptr<SessionState> state;
      Status status = GetSessionState(session_id, SecretArg::SkipSecretCheck(), &state);
      // The session may not exist if it was explicitly closed.
      if (!status.ok()) continue;
      lock_guard<mutex> state_lock(state->lock);
      state->connections.erase(connection_context.connection_id);
      if (state->connections.empty()) {
        state->disconnected_ms = UnixMillis();
        RegisterSessionTimeout(FLAGS_disconnected_session_timeout);
      }
    }
  }
}

bool ImpalaServer::IsIdleConnection(
    const ThriftServer::ConnectionContext& connection_context) {
  // The set of sessions associated with this connection.
  std::set<TUniqueId> session_ids;
  {
    TUniqueId connection_id = connection_context.connection_id;
    unique_lock<mutex> l(connection_to_sessions_map_lock_);
    ConnectionToSessionMap::iterator it = connection_to_sessions_map_.find(connection_id);

    // Not every connection must have an associated session
    if (it == connection_to_sessions_map_.end()) return false;

    session_ids = it->second;

    // Sessions are not removed from the map even after they are closed and an entry
    // won't be added to the map unless a session is established. The code below relies
    // on this invariant to not mark a connection with no session yet as idle.
    DCHECK(!session_ids.empty());
  }

  // Check if all the sessions associated with the connection are idle.
  {
    lock_guard<mutex> map_lock(session_state_map_lock_);
    for (const TUniqueId& session_id : session_ids) {
      const auto it = session_state_map_.find(session_id);
      if (it == session_state_map_.end()) continue;

      // If any session associated with this connection is not idle,
      // the connection is not idle.
      lock_guard<mutex> state_lock(it->second->lock);
      if (!it->second->expired) return false;
    }
  }
  return true;
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

[[noreturn]] void ImpalaServer::SessionMaintenance() {
  while (true) {
    {
      unique_lock<mutex> timeout_lock(session_timeout_lock_);
      if (session_timeout_set_.empty()) {
        session_timeout_cv_.Wait(timeout_lock);
      } else {
        // Sleep for a second before doing maintenance.
        session_timeout_cv_.WaitFor(timeout_lock, MICROS_PER_SEC);
      }
    }

    int64_t now = UnixMillis();
    int expired_cnt = 0;
    VLOG(3) << "Session maintenance thread waking up";
    {
      // TODO: If holding session_state_map_lock_ for the duration of this loop is too
      // expensive, consider a priority queue.
      lock_guard<mutex> map_lock(session_state_map_lock_);
      vector<TUniqueId> sessions_to_remove;
      for (SessionStateMap::value_type& map_entry : session_state_map_) {
        const TUniqueId& session_id = map_entry.first;
        std::shared_ptr<SessionState> session_state = map_entry.second;
        unordered_set<TUniqueId> inflight_queries;
        Status query_cancel_status;
        {
          lock_guard<mutex> state_lock(session_state->lock);
          if (session_state->ref_count > 0) continue;
          // A session closed by other means is in the process of being removed, and it's
          // best not to interfere.
          if (session_state->closed) continue;

          if (session_state->connections.size() == 0
              && (now - session_state->disconnected_ms)
                  >= FLAGS_disconnected_session_timeout * 1000L) {
            // This session has no active connections and is past the disconnected session
            // timeout, so close it.
            DCHECK_ENUM_EQ(session_state->session_type, TSessionType::HIVESERVER2);
            LOG(INFO) << "Closing session: " << PrintId(session_id)
                      << ", user: " << session_state->connected_user
                      << ", because it no longer  has any open connections. The last "
                      << "connection was closed at: "
                      << ToStringFromUnixMillis(session_state->disconnected_ms);
            session_state->closed = true;
            sessions_to_remove.push_back(session_id);
            ImpaladMetrics::IMPALA_SERVER_NUM_OPEN_HS2_SESSIONS->Increment(-1L);
            UnregisterSessionTimeout(FLAGS_disconnected_session_timeout);
            query_cancel_status =
                Status::Expected(TErrorCode::DISCONNECTED_SESSION_CLOSED);
          } else {
            // Check if the session should be expired.
            if (session_state->expired || session_state->session_timeout == 0) {
              continue;
            }

            int64_t last_accessed_ms = session_state->last_accessed_ms;
            int64_t session_timeout_ms = session_state->session_timeout * 1000;
            if (now - last_accessed_ms <= session_timeout_ms) continue;
            LOG(INFO) << "Expiring session: " << PrintId(session_id)
                      << ", user: " << session_state->connected_user
                      << ", last active: " << ToStringFromUnixMillis(last_accessed_ms);
            session_state->expired = true;
            ++expired_cnt;
            ImpaladMetrics::NUM_SESSIONS_EXPIRED->Increment(1L);
            query_cancel_status = Status::Expected(TErrorCode::INACTIVE_SESSION_EXPIRED);
          }

          // Since either expired or closed is true no more queries will be added to the
          // inflight list.
          inflight_queries.insert(session_state->inflight_queries.begin(),
              session_state->inflight_queries.end());
        }
        // Unregister all open queries from this session.
        for (const TUniqueId& query_id : inflight_queries) {
          cancellation_thread_pool_->Offer(
              CancellationWork::TerminatedByServer(query_id, query_cancel_status, true));
        }
      }
      // Remove any sessions that were closed from the map.
      for (const TUniqueId& session_id : sessions_to_remove) {
        session_state_map_.erase(session_id);
      }
    }
    LOG_IF(INFO, expired_cnt > 0) << "Expired sessions. Count: " << expired_cnt;
  }
}

[[noreturn]] void ImpalaServer::ExpireQueries() {
  while (true) {
    // The following block accomplishes four things:
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
    //
    // 4. Cancel queries with CPU and scan bytes constraints if limit is exceeded
    int64_t now;
    {
      lock_guard<mutex> l(query_expiration_lock_);
      ExpirationQueue::iterator expiration_event = queries_by_timestamp_.begin();
      now = UnixMillis();
      while (expiration_event != queries_by_timestamp_.end()) {
        // 'queries_by_timestamp_' is stored in ascending order of deadline so we can
        // break out of the loop and sleep as soon as we see a deadline in the future.
        if (expiration_event->deadline > now) break;
        shared_ptr<QueryDriver> query_driver = GetQueryDriver(expiration_event->query_id);
        if (query_driver == nullptr) {
          // Query was deleted already from a previous expiration event
          expiration_event = queries_by_timestamp_.erase(expiration_event);
          continue;
        }
        ClientRequestState* crs = query_driver->GetActiveClientRequestState();
        if (crs->is_expired()) {
          // Query was expired already from a previous expiration event.
          expiration_event = queries_by_timestamp_.erase(expiration_event);
          continue;
        }

        // Check for CPU and scanned bytes limits
        if (expiration_event->kind == ExpirationKind::RESOURCE_LIMIT) {
          Status resource_status = CheckResourceLimits(crs);
          if (resource_status.ok()) {
            queries_by_timestamp_.emplace(
                ExpirationEvent{now + EXPIRATION_CHECK_INTERVAL_MS,
                    expiration_event->query_id, ExpirationKind::RESOURCE_LIMIT});
          } else {
            ExpireQuery(crs, resource_status);
          }
          expiration_event = queries_by_timestamp_.erase(expiration_event);
          continue;
        }

        // If the query time limit expired, we must cancel the query.
        if (expiration_event->kind == ExpirationKind::EXEC_TIME_LIMIT) {
          int32_t exec_time_limit_s = crs->query_options().exec_time_limit_s;
          VLOG_QUERY << "Expiring query " << PrintId(expiration_event->query_id)
                     << " due to execution time limit of " << exec_time_limit_s << "s.";
          ExpireQuery(crs,
              Status::Expected(TErrorCode::EXEC_TIME_LIMIT_EXCEEDED,
                  PrintId(expiration_event->query_id),
                  PrettyPrinter::Print(exec_time_limit_s, TUnit::TIME_S)));
          expiration_event = queries_by_timestamp_.erase(expiration_event);
          continue;
        }
        DCHECK(expiration_event->kind == ExpirationKind::IDLE_TIMEOUT)
            << static_cast<int>(expiration_event->kind);

        // Now check to see if the idle timeout has expired. We must check the actual
        // expiration time in case the query has updated 'last_active_ms' since the last
        // time we looked.
        int32_t idle_timeout_s = crs->query_options().query_timeout_s;
        if (FLAGS_idle_query_timeout > 0 && idle_timeout_s > 0) {
          idle_timeout_s = min(FLAGS_idle_query_timeout, idle_timeout_s);
        } else {
          // Use a non-zero timeout, if one exists
          idle_timeout_s = max(FLAGS_idle_query_timeout, idle_timeout_s);
        }
        int64_t expiration = crs->last_active_ms() + (idle_timeout_s * 1000L);
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
        } else if (!crs->is_active()) {
          // Otherwise time to expire this query
          VLOG_QUERY << "Expiring query due to client inactivity: "
                     << PrintId(expiration_event->query_id) << ", last activity was at: "
                     << ToStringFromUnixMillis(crs->last_active_ms());
          ExpireQuery(crs,
              Status::Expected(TErrorCode::INACTIVE_QUERY_EXPIRED,
                  PrintId(expiration_event->query_id),
                  PrettyPrinter::Print(idle_timeout_s, TUnit::TIME_S)));
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
    SleepForMs(EXPIRATION_CHECK_INTERVAL_MS);
  }
}

[[noreturn]] void ImpalaServer::UnresponsiveBackendThread() {
  int64_t max_lag_ms = FLAGS_status_report_max_retry_s * 1000
      * (1 + FLAGS_status_report_cancellation_padding / 100.0);
  DCHECK_GT(max_lag_ms, 0);
  VLOG(1) << "Queries will be cancelled if a backend has not reported its status in "
          << "more than " << max_lag_ms << "ms.";
  while (true) {
    vector<CancellationWork> to_cancel;
    query_driver_map_.DoFuncForAllEntries(
        [&](const std::shared_ptr<QueryDriver>& query_driver) {
          ClientRequestState* request_state = query_driver->GetActiveClientRequestState();
          Coordinator* coord = request_state->GetCoordinator();
          if (coord != nullptr) {
            NetworkAddressPB address;
            int64_t lag_time_ms = coord->GetMaxBackendStateLagMs(&address);
            if (lag_time_ms > max_lag_ms) {
              to_cancel.push_back(
                  CancellationWork::TerminatedByServer(request_state->query_id(),
                      Status(TErrorCode::UNRESPONSIVE_BACKEND,
                          PrintId(request_state->query_id()),
                          NetworkAddressPBToString(address), lag_time_ms, max_lag_ms),
                      false /* unregister */));
            }
          }
        });

    // We call Offer() outside of DoFuncForAllEntries() to ensure that if the
    // cancellation_thread_pool_ queue is full, we're not blocked while holding one of the
    // 'query_driver_map_' shard locks.
    for (auto cancellation_work : to_cancel) {
      cancellation_thread_pool_->Offer(cancellation_work);
    }
    SleepForMs(max_lag_ms * 0.1);
  }
}

Status ImpalaServer::CheckResourceLimits(ClientRequestState* crs) {
  Coordinator* coord = crs->GetCoordinator();
  // Coordinator may be null if query has not started executing, check again later.
  if (coord == nullptr) return Status::OK();
  Coordinator::ResourceUtilization utilization = coord->ComputeQueryResourceUtilization();

  // CPU time consumed by the query so far
  int64_t cpu_time_ns = utilization.cpu_sys_ns + utilization.cpu_user_ns;
  int64_t cpu_limit_s = crs->query_options().cpu_limit_s;
  int64_t cpu_limit_ns = cpu_limit_s * 1000'000'000L;
  if (cpu_limit_ns > 0 && cpu_time_ns > cpu_limit_ns) {
    Status err = Status::Expected(TErrorCode::CPU_LIMIT_EXCEEDED,
        PrintId(crs->query_id()), PrettyPrinter::Print(cpu_limit_s, TUnit::TIME_S));
    VLOG_QUERY << err.msg().msg();
    return err;
  }

  int64_t scan_bytes = utilization.bytes_read;
  int64_t scan_bytes_limit = crs->query_options().scan_bytes_limit;
  if (scan_bytes_limit > 0 && scan_bytes > scan_bytes_limit) {
    Status err = Status::Expected(TErrorCode::SCAN_BYTES_LIMIT_EXCEEDED,
        PrintId(crs->query_id()), PrettyPrinter::Print(scan_bytes_limit, TUnit::BYTES));
    VLOG_QUERY << err.msg().msg();
    return err;
  }
  // Query is within the resource limits, check again later.
  return Status::OK();
}

void ImpalaServer::ExpireQuery(ClientRequestState* crs, const Status& status) {
  DCHECK(!status.ok());
  cancellation_thread_pool_->Offer(
      CancellationWork::TerminatedByServer(crs->query_id(), status, false));
  ImpaladMetrics::NUM_QUERIES_EXPIRED->Increment(1L);
  crs->set_expired();
}

Status ImpalaServer::Start(int32_t thrift_be_port, int32_t beeswax_port, int32_t hs2_port,
    int32_t hs2_http_port) {
  exec_env_->SetImpalaServer(this);

  // We must register the HTTP handlers after registering the ImpalaServer with the
  // ExecEnv. Otherwise the HTTP handlers will try to resolve the ImpalaServer through the
  // ExecEnv singleton and will receive a nullptr.
  http_handler_.reset(new ImpalaHttpHandler(this));
  http_handler_->RegisterHandlers(exec_env_->webserver());
  if (exec_env_->metrics_webserver() != nullptr) {
    http_handler_->RegisterHandlers(
        exec_env_->metrics_webserver(), /* metrics_only */ true);
  }

  if (!FLAGS_is_coordinator && !FLAGS_is_executor) {
    return Status("Impala does not have a valid role configured. "
        "Either --is_coordinator or --is_executor must be set to true.");
  }

  // Subscribe with the statestore. Coordinators need to subscribe to the catalog topic
  // then wait for the initial catalog update.
  RETURN_IF_ERROR(exec_env_->StartStatestoreSubscriberService());

  if (FLAGS_is_coordinator) {
    exec_env_->frontend()->WaitForCatalog();
    ABORT_IF_ERROR(UpdateCatalogMetrics());
  }

  SSLProtocol ssl_version = SSLProtocol::TLSv1_0;
  if (IsExternalTlsConfigured() || IsInternalTlsConfigured()) {
    RETURN_IF_ERROR(
        SSLProtoVersions::StringToProtocol(FLAGS_ssl_minimum_version, &ssl_version));
  }

  // Start the internal service.
  if (thrift_be_port > 0 || (TestInfo::is_test() && thrift_be_port == 0)) {
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
              << TNetworkAddressToString(GetThriftBackendAddress());
  } else {
    // Initialize the client servers.
    boost::shared_ptr<ImpalaServer> handler = shared_from_this();
    if (beeswax_port > 0 || (TestInfo::is_test() && beeswax_port == 0)) {
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
          .queue_timeout_ms(FLAGS_accepted_client_cnxn_timeout)
          .idle_poll_period_ms(FLAGS_idle_client_poll_period_s * MILLIS_PER_SEC)
          .Build(&server));
      beeswax_server_.reset(server);
      beeswax_server_->SetConnectionHandler(this);
    }

    if (hs2_port > 0 || (TestInfo::is_test() && hs2_port == 0)) {
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
          .queue_timeout_ms(FLAGS_accepted_client_cnxn_timeout)
          .idle_poll_period_ms(FLAGS_idle_client_poll_period_s * MILLIS_PER_SEC)
          .Build(&server));
      hs2_server_.reset(server);
      hs2_server_->SetConnectionHandler(this);
    }

    if (hs2_http_port > 0 || (TestInfo::is_test() && hs2_http_port == 0)) {
      boost::shared_ptr<TProcessor> hs2_http_processor(
          new ImpalaHiveServer2ServiceProcessor(handler));
      boost::shared_ptr<TProcessorEventHandler> event_handler(
          new RpcEventHandler("hs2_http", exec_env_->metrics()));
      hs2_http_processor->setEventHandler(event_handler);

      ThriftServer* http_server;
      ThriftServerBuilder http_builder(
          HS2_HTTP_SERVER_NAME, hs2_http_processor, hs2_http_port);
      if (IsExternalTlsConfigured()) {
        LOG(INFO) << "Enabling SSL for HiveServer2 HTTP endpoint.";
        http_builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
            .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
            .ssl_version(ssl_version)
            .cipher_list(FLAGS_ssl_cipher_list);
      }

      RETURN_IF_ERROR(
          http_builder
              .auth_provider(AuthManager::GetInstance()->GetExternalAuthProvider())
              .transport_type(ThriftServer::TransportType::HTTP)
              .metrics(exec_env_->metrics())
              .max_concurrent_connections(FLAGS_fe_service_threads)
              .queue_timeout_ms(FLAGS_accepted_client_cnxn_timeout)
              .idle_poll_period_ms(FLAGS_idle_client_poll_period_s * MILLIS_PER_SEC)
              .Build(&http_server));
      hs2_http_server_.reset(http_server);
      hs2_http_server_->SetConnectionHandler(this);
    }
  }
  LOG(INFO) << "Initialized coordinator/executor Impala server on "
      << TNetworkAddressToString(GetThriftBackendAddress());

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
  if (hs2_http_server_.get()) {
    RETURN_IF_ERROR(hs2_http_server_->Start());
    LOG(INFO) << "Impala HiveServer2 Service (HTTP) listening on "
              << hs2_http_server_->port();
  }
  if (beeswax_server_.get()) {
    RETURN_IF_ERROR(beeswax_server_->Start());
    LOG(INFO) << "Impala Beeswax Service listening on " << beeswax_server_->port();
  }
  RETURN_IF_ERROR(DebugAction(FLAGS_debug_actions, "IMPALA_SERVER_END_OF_START"));
  services_started_ = true;
  ImpaladMetrics::IMPALA_SERVER_READY->SetValue(true);
  LOG(INFO) << "Impala has started.";

  return Status::OK();
}

void ImpalaServer::Join() {
  // The server shuts down by exiting the process, so just block here until the process
  // exits.
  thrift_be_server_->Join();
  thrift_be_server_.reset();

  if (FLAGS_is_coordinator) {
    beeswax_server_->Join();
    hs2_server_->Join();
    beeswax_server_.reset();
    hs2_server_.reset();
  }
}

Status ImpalaServer::CheckClientRequestSession(
    SessionState* session, const std::string& client_request_effective_user,
    const TUniqueId& query_id) {
  const string& session_user = GetEffectiveUser(*session);
  // Empty session users only occur for unauthenticated sessions where no user was
  // specified by the client, e.g. unauthenticated beeswax sessions. Skip the
  // check in this case because no security is enabled. Some tests rely on
  // this behaviour, e.g. to poll query status from a new beeswax connection.
  if (!session_user.empty() && session_user != client_request_effective_user) {
    Status err = Status::Expected(
        Substitute(LEGACY_INVALID_QUERY_HANDLE_TEMPLATE, PrintId(query_id)));
    VLOG(1) << err << " caused by user mismatch: '" << session_user << "' vs '"
            << client_request_effective_user << "'";
    return err;
  }
  return Status::OK();
}

void ImpalaServer::UpdateFilter(UpdateFilterResultPB* result,
    const UpdateFilterParamsPB& params, RpcContext* context) {
  DCHECK(params.has_query_id());
  DCHECK(params.has_filter_id());
  QueryHandle query_handle;
  Status status = GetQueryHandle(ProtoToQueryId(params.query_id()), &query_handle);
  if (!status.ok()) {
    LOG(INFO) << "Could not find query handle for query id: "
              << PrintId(ProtoToQueryId(params.query_id()));
    return;
  }
  ClientRequestState::RetryState retry_state = query_handle->retry_state();
  if (retry_state != ClientRequestState::RetryState::RETRYING
      && retry_state != ClientRequestState::RetryState::RETRIED) {
    query_handle->UpdateFilter(params, context);
  }
}

Status ImpalaServer::CheckNotShuttingDown() const {
  if (!IsShuttingDown()) return Status::OK();
  return Status::Expected(ErrorMsg(
      TErrorCode::SERVER_SHUTTING_DOWN, ShutdownStatusToString(GetShutdownStatus())));
}

ShutdownStatusPB ImpalaServer::GetShutdownStatus() const {
  ShutdownStatusPB result;
  int64_t shutdown_time = shutting_down_.Load();
  DCHECK_GT(shutdown_time, 0);
  int64_t shutdown_deadline = shutdown_deadline_.Load();
  DCHECK_GT(shutdown_time, 0);
  int64_t now = MonotonicMillis();
  int64_t elapsed_ms = now - shutdown_time;
  result.set_grace_remaining_ms(
      max<int64_t>(0, FLAGS_shutdown_grace_period_s * 1000 - elapsed_ms));
  result.set_deadline_remaining_ms(max<int64_t>(0, shutdown_deadline - now));
  result.set_finstances_executing(
      ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->GetValue());
  result.set_client_requests_registered(
      ImpaladMetrics::NUM_QUERIES_REGISTERED->GetValue());
  result.set_backend_queries_executing(
      ImpaladMetrics::BACKEND_NUM_QUERIES_EXECUTING->GetValue());
  return result;
}

string ImpalaServer::ShutdownStatusToString(const ShutdownStatusPB& shutdown_status) {
  return Substitute("shutdown grace period left: $0, deadline left: $1, "
                    "queries registered on coordinator: $2, queries executing: $3, "
                    "fragment instances: $4",
      PrettyPrinter::Print(shutdown_status.grace_remaining_ms(), TUnit::TIME_MS),
      PrettyPrinter::Print(shutdown_status.deadline_remaining_ms(), TUnit::TIME_MS),
      shutdown_status.client_requests_registered(),
      shutdown_status.backend_queries_executing(),
      shutdown_status.finstances_executing());
}

Status ImpalaServer::StartShutdown(
    int64_t relative_deadline_s, ShutdownStatusPB* shutdown_status) {
  DCHECK_GE(relative_deadline_s, -1);
  if (relative_deadline_s == -1) relative_deadline_s = FLAGS_shutdown_deadline_s;
  int64_t now = MonotonicMillis();
  int64_t new_deadline = now + relative_deadline_s * 1000L;

  bool set_deadline = false;
  bool set_grace = false;
  int64_t curr_deadline = shutdown_deadline_.Load();
  while (curr_deadline == 0 || curr_deadline > new_deadline) {
    // Set the deadline - it was either unset or later than the new one.
    if (shutdown_deadline_.CompareAndSwap(curr_deadline, new_deadline)) {
      set_deadline = true;
      break;
    }
    curr_deadline = shutdown_deadline_.Load();
  }

  while (shutting_down_.Load() == 0) {
    if (!shutting_down_.CompareAndSwap(0, now)) continue;
    unique_ptr<Thread> t;
    Status status =
        Thread::Create("shutdown", "shutdown", [this] { ShutdownThread(); }, &t, false);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to create shutdown thread: " << status.GetDetail();
      return status;
    }
    set_grace = true;
    break;
  }
  *shutdown_status = GetShutdownStatus();
  // Show the full grace/limit times to avoid showing confusing intermediate values
  // to the person running the statement.
  if (set_grace) {
    shutdown_status->set_grace_remaining_ms(FLAGS_shutdown_grace_period_s * 1000L);
  }
  if (set_deadline) {
    shutdown_status->set_deadline_remaining_ms(relative_deadline_s * 1000L);
  }
  return Status::OK();
}

[[noreturn]] void ImpalaServer::ShutdownThread() {
  while (true) {
    SleepForMs(1000);
    const ShutdownStatusPB& shutdown_status = GetShutdownStatus();
    LOG(INFO) << "Shutdown status: " << ShutdownStatusToString(shutdown_status);
    if (shutdown_status.grace_remaining_ms() <= 0
        && shutdown_status.backend_queries_executing() == 0
        && shutdown_status.client_requests_registered() == 0) {
      break;
    } else if (shutdown_status.deadline_remaining_ms() <= 0) {
      break;
    }
  }
  LOG(INFO) << "Shutdown complete, going down.";
  // Use _exit here instead since exit() does cleanup which interferes with the shutdown
  // signal handler thread causing a data race.
  ShutdownLogging();
  _exit(0);
}

// This should never be inlined to prevent it potentially being optimized, e.g.
// by short-circuiting the comparisons.
__attribute__((noinline)) int ImpalaServer::SecretArg::ConstantTimeCompare(
    const TUniqueId& other) const {
  // Compiles to two integer comparisons and an addition with no branches.
  // TODO: consider replacing with CRYPTO_memcmp() once our minimum supported OpenSSL
  // version has it.
  return (secret_.hi != other.hi) + (secret_.lo != other.lo);
}
}
