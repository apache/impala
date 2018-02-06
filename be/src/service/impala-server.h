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

#ifndef IMPALA_SERVICE_IMPALA_SERVER_H
#define IMPALA_SERVICE_IMPALA_SERVER_H

#include <atomic>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <unordered_map>

#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaHiveServer2Service.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Frontend_types.h"
#include "rpc/thrift-server.h"
#include "common/status.h"
#include "service/frontend.h"
#include "service/query-options.h"
#include "util/condition-variable.h"
#include "util/metrics.h"
#include "util/runtime-profile.h"
#include "util/sharded-query-map-util.h"
#include "util/simple-logger.h"
#include "util/thread-pool.h"
#include "util/time.h"
#include "runtime/coordinator.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"
#include "statestore/statestore-subscriber.h"

namespace impala {

class ExecEnv;
class DataSink;
class CancellationWork;
class ImpalaHttpHandler;
class RowDescriptor;
class TCatalogUpdate;
class TPlanExecRequest;
class TPlanExecParams;
class TInsertResult;
class TReportExecStatusArgs;
class TReportExecStatusResult;
class TTransmitDataArgs;
class TTransmitDataResult;
class TNetworkAddress;
class TClientRequest;
class TExecRequest;
class TSessionState;
class TQueryOptions;
class TGetExecSummaryResp;
class TGetExecSummaryReq;
class ClientRequestState;

/// An ImpalaServer contains both frontend and backend functionality;
/// it implements ImpalaService (Beeswax), ImpalaHiveServer2Service (HiveServer2)
/// and ImpalaInternalService APIs.
/// ImpalaServer can be started in 1 of 3 roles: executor, coordinator, or both executor
/// and coordinator. All roles start ImpalaInternalService API's. The
/// coordinator role additionally starts client API's (Beeswax and HiveServer2).
///
/// Startup Sequence
/// ----------------
/// The startup sequence opens and starts all services so that they are ready to be used
/// by clients at the same time. The Impala server is considered 'ready' only when it can
/// process requests with all of its specified roles. Avoiding states where some roles are
/// ready and some are not makes it easier to reason about the state of the server.
///
/// Main thread (caller code), after instantiating the server, must call Start().
/// Start() does the following:
///    - Start internal services
///    - Wait (indefinitely) for local catalog to be initialized from statestore
///      (if coordinator)
///    - Open ImpalaInternalService ports
///    - Open client ports (if coordinator)
///    - Start ImpalaInternalService API
///    - Start client service API's (if coordinator)
///    - Set services_started_ flag
///
/// Internally, the Membership callback thread also participates in startup:
///    - If services_started_, then register to the statestore as an executor.
///
/// Locking
/// -------
/// This class is partially thread-safe. To ensure freedom from deadlock, if multiple
/// locks are acquired, lower-numbered locks must be acquired before higher-numbered
/// locks:
/// 1. session_state_map_lock_
/// 2. SessionState::lock
/// 3. query_expiration_lock_
/// 4. ClientRequestState::fetch_rows_lock
/// 5. ClientRequestState::lock
/// 6. ClientRequestState::expiration_data_lock_
/// 7. Coordinator::exec_summary_lock
///
/// Coordinator::lock_ should not be acquired at the same time as the
/// ImpalaServer/SessionState/ClientRequestState locks. Aside from
/// Coordinator::exec_summary_lock_ the Coordinator's lock ordering is independent of
/// the above lock ordering.
///
/// The following locks are not held in conjunction with other locks:
/// * query_log_lock_
/// * session_timeout_lock_
/// * query_locations_lock_
/// * uuid_lock_
/// * catalog_version_lock_
/// * connection_to_sessions_map_lock_
///
/// TODO: The state of a running query is currently not cleaned up if the
/// query doesn't experience any errors at runtime and close() doesn't get called.
/// The solution is to have a separate thread that cleans up orphaned
/// query execution states after a timeout period.
/// TODO: The same doesn't apply to the execution state of an individual plan
/// fragment: the originating coordinator might die, but we can get notified of
/// that via the statestore. This still needs to be implemented.
class ImpalaServer : public ImpalaServiceIf,
                     public ImpalaHiveServer2ServiceIf,
                     public ThriftServer::ConnectionHandlerIf,
                     public boost::enable_shared_from_this<ImpalaServer>,
                     public CacheLineAligned {
 public:

  ImpalaServer(ExecEnv* exec_env);
  ~ImpalaServer();

  /// Initializes and starts RPC services and other subsystems (like audit logging).
  /// Returns an error if starting any services failed. If the port is <= 0, their
  ///respective service will not be started.
  Status Start(int32_t thrift_be_port, int32_t beeswax_port, int32_t hs2_port);

  /// Blocks until the server shuts down (by calling Shutdown()).
  void Join();

  /// Triggers service shutdown, by unblocking Join().
  void Shutdown() { shutdown_promise_.Set(true); }

  /// ImpalaService rpcs: Beeswax API (implemented in impala-beeswax-server.cc)
  virtual void query(beeswax::QueryHandle& query_handle, const beeswax::Query& query);
  virtual void executeAndWait(beeswax::QueryHandle& query_handle,
      const beeswax::Query& query, const beeswax::LogContextId& client_ctx);
  virtual void explain(beeswax::QueryExplanation& query_explanation,
      const beeswax::Query& query);
  virtual void fetch(beeswax::Results& query_results,
      const beeswax::QueryHandle& query_handle, const bool start_over,
      const int32_t fetch_size);
  virtual void get_results_metadata(beeswax::ResultsMetadata& results_metadata,
      const beeswax::QueryHandle& handle);
  virtual void close(const beeswax::QueryHandle& handle);
  virtual beeswax::QueryState::type get_state(const beeswax::QueryHandle& handle);
  virtual void echo(std::string& echo_string, const std::string& input_string);
  virtual void clean(const beeswax::LogContextId& log_context);
  virtual void get_log(std::string& log, const beeswax::LogContextId& context);

  /// Return ImpalaQueryOptions default values and "support_start_over/false" to indicate
  /// that Impala does not support start over in the fetch call. Hue relies on this not to
  /// issue a "start_over" fetch call.
  /// "include_hadoop" is not applicable.
  virtual void get_default_configuration(
      std::vector<beeswax::ConfigVariable>& configurations, const bool include_hadoop);

  /// ImpalaService rpcs: unimplemented parts of Beeswax API.
  /// These APIs will not be implemented because ODBC driver does not use them.
  virtual void dump_config(std::string& config);

  /// ImpalaService rpcs: extensions over Beeswax (implemented in
  /// impala-beeswax-server.cc)
  virtual void Cancel(impala::TStatus& status, const beeswax::QueryHandle& query_id);
  virtual void CloseInsert(impala::TInsertResult& insert_result,
      const beeswax::QueryHandle& query_handle);

  /// Pings the Impala service and gets the server version string.
  virtual void PingImpalaService(TPingImpalaServiceResp& return_val);

  virtual void GetRuntimeProfile(std::string& profile_output,
      const beeswax::QueryHandle& query_id);

  virtual void GetExecSummary(impala::TExecSummary& result,
      const beeswax::QueryHandle& query_id);

  /// Performs a full catalog metadata reset, invalidating all table and database
  /// metadata.
  virtual void ResetCatalog(impala::TStatus& status);

  /// Resets the specified table's catalog metadata, forcing a reload on the next access.
  /// Returns an error if the table or database was not found in the catalog.
  virtual void ResetTable(impala::TStatus& status, const TResetTableReq& request);

  /// ImpalaHiveServer2Service rpcs: HiveServer2 API (implemented in impala-hs2-server.cc)
  /// TODO: Migrate existing extra ImpalaServer RPCs to ImpalaHiveServer2Service.
  virtual void OpenSession(
      apache::hive::service::cli::thrift::TOpenSessionResp& return_val,
      const apache::hive::service::cli::thrift::TOpenSessionReq& request);
  virtual void CloseSession(
      apache::hive::service::cli::thrift::TCloseSessionResp& return_val,
      const apache::hive::service::cli::thrift::TCloseSessionReq& request);
  virtual void GetInfo(
      apache::hive::service::cli::thrift::TGetInfoResp& return_val,
      const apache::hive::service::cli::thrift::TGetInfoReq& request);
  virtual void ExecuteStatement(
      apache::hive::service::cli::thrift::TExecuteStatementResp& return_val,
      const apache::hive::service::cli::thrift::TExecuteStatementReq& request);
  virtual void GetTypeInfo(
      apache::hive::service::cli::thrift::TGetTypeInfoResp& return_val,
      const apache::hive::service::cli::thrift::TGetTypeInfoReq& request);
  virtual void GetCatalogs(
      apache::hive::service::cli::thrift::TGetCatalogsResp& return_val,
      const apache::hive::service::cli::thrift::TGetCatalogsReq& request);
  virtual void GetSchemas(
      apache::hive::service::cli::thrift::TGetSchemasResp& return_val,
      const apache::hive::service::cli::thrift::TGetSchemasReq& request);
  virtual void GetTables(
      apache::hive::service::cli::thrift::TGetTablesResp& return_val,
      const apache::hive::service::cli::thrift::TGetTablesReq& request);
  virtual void GetTableTypes(
      apache::hive::service::cli::thrift::TGetTableTypesResp& return_val,
      const apache::hive::service::cli::thrift::TGetTableTypesReq& request);
  virtual void GetColumns(
      apache::hive::service::cli::thrift::TGetColumnsResp& return_val,
      const apache::hive::service::cli::thrift::TGetColumnsReq& request);
  virtual void GetFunctions(
      apache::hive::service::cli::thrift::TGetFunctionsResp& return_val,
      const apache::hive::service::cli::thrift::TGetFunctionsReq& request);
  virtual void GetOperationStatus(
      apache::hive::service::cli::thrift::TGetOperationStatusResp& return_val,
      const apache::hive::service::cli::thrift::TGetOperationStatusReq& request);
  virtual void CancelOperation(
      apache::hive::service::cli::thrift::TCancelOperationResp& return_val,
      const apache::hive::service::cli::thrift::TCancelOperationReq& request);
  virtual void CloseOperation(
      apache::hive::service::cli::thrift::TCloseOperationResp& return_val,
      const apache::hive::service::cli::thrift::TCloseOperationReq& request);
  virtual void GetResultSetMetadata(
      apache::hive::service::cli::thrift::TGetResultSetMetadataResp& return_val,
      const apache::hive::service::cli::thrift::TGetResultSetMetadataReq& request);
  virtual void FetchResults(
      apache::hive::service::cli::thrift::TFetchResultsResp& return_val,
      const apache::hive::service::cli::thrift::TFetchResultsReq& request);
  virtual void GetLog(apache::hive::service::cli::thrift::TGetLogResp& return_val,
      const apache::hive::service::cli::thrift::TGetLogReq& request);
  virtual void GetExecSummary(TGetExecSummaryResp& return_val,
      const TGetExecSummaryReq& request);
  virtual void GetRuntimeProfile(TGetRuntimeProfileResp& return_val,
      const TGetRuntimeProfileReq& request);
  virtual void GetDelegationToken(
      apache::hive::service::cli::thrift::TGetDelegationTokenResp& return_val,
      const apache::hive::service::cli::thrift::TGetDelegationTokenReq& req);
  virtual void CancelDelegationToken(
      apache::hive::service::cli::thrift::TCancelDelegationTokenResp& return_val,
      const apache::hive::service::cli::thrift::TCancelDelegationTokenReq& req);
  virtual void RenewDelegationToken(
      apache::hive::service::cli::thrift::TRenewDelegationTokenResp& return_val,
      const apache::hive::service::cli::thrift::TRenewDelegationTokenReq& req);

  /// ImpalaInternalService rpcs
  void ReportExecStatus(TReportExecStatusResult& return_val,
      const TReportExecStatusParams& params);
  void TransmitData(TTransmitDataResult& return_val,
      const TTransmitDataParams& params);
  void UpdateFilter(TUpdateFilterResult& return_val,
      const TUpdateFilterParams& params);

  /// Generates a unique id for this query and sets it in the given query context.
  /// Prepares the given query context by populating fields required for evaluating
  /// certain expressions, such as now(), pid(), etc. Should be called before handing
  /// the query context to the frontend for query compilation.
  static void PrepareQueryContext(TQueryCtx* query_ctx);

  /// SessionHandlerIf methods

  /// Called when a Beeswax or HS2 connection starts. For Beeswax, registers a new
  /// SessionState associated with the new connection. For HS2, this is a no-op (HS2
  /// has an explicit CreateSession RPC).
  virtual void ConnectionStart(const ThriftServer::ConnectionContext& session_context);

  /// Called when a Beeswax or HS2 connection terminates. Unregisters all sessions
  /// associated with the closed connection.
  virtual void ConnectionEnd(const ThriftServer::ConnectionContext& session_context);

  /// Called when a membership update is received from the statestore. Looks for
  /// active nodes that have failed, and cancels any queries running on them.
  ///  - incoming_topic_deltas: all changes to registered statestore topics
  ///  - subscriber_topic_updates: output parameter to publish any topic updates to.
  ///                              Currently unused.
  void MembershipCallback(
      const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
      std::vector<TTopicDelta>* subscriber_topic_updates);

  void CatalogUpdateCallback(const StatestoreSubscriber::TopicDeltaMap& topic_deltas,
      std::vector<TTopicDelta>* topic_updates);

  /// Processes a TCatalogUpdateResult returned from the CatalogServer and ensures
  /// the update has been applied to the local impalad's catalog cache. Called from
  /// ClientRequestState after executing any statement that modifies the catalog.
  ///
  /// If TCatalogUpdateResult contains TCatalogObject(s) to add and/or remove, this
  /// function will update the local cache by directly calling UpdateCatalog() with the
  /// TCatalogObject results.
  ///
  /// If TCatalogUpdateResult does not contain any TCatalogObjects and this is
  /// the result of an INVALIDATE METADATA operation, it waits until the minimum
  /// catalog version in the local cache is greater than or equal to the catalog
  /// version specified in TCatalogUpdateResult. If it is not an INVALIDATE
  /// METADATA operation, it waits until the local impalad's catalog cache has
  /// been updated from a statestore heartbeat that includes this catalog
  /// update's version.
  ///
  /// If wait_for_all_subscribers is true, this function also
  /// waits for all other catalog topic subscribers to process this update by checking the
  /// current min_subscriber_topic_version included in each state store heartbeat.
  Status ProcessCatalogUpdateResult(const TCatalogUpdateResult& catalog_update_result,
      bool wait_for_all_subscribers) WARN_UNUSED_RESULT;

  /// Wait until the catalog update with version 'catalog_update_version' is
  /// received and applied in the local catalog cache or until the catalog
  /// service id has changed.
  void WaitForCatalogUpdate(const int64_t catalog_update_version,
      const TUniqueId& catalog_service_id);

  /// Wait until the minimum catalog object version in the local cache is
  /// greater than or equal to 'min_catalog_update_version' or until the catalog
  /// service id has changed.
  void WaitForMinCatalogUpdate(const int64_t min_catalog_update_version,
      const TUniqueId& catalog_service_id);

  /// Wait until the last applied catalog update has been broadcast to
  /// all coordinators or until the catalog service id has changed.
  void WaitForCatalogUpdateTopicPropagation(const TUniqueId& catalog_service_id);

  /// Returns true if lineage logging is enabled, false otherwise.
  bool IsLineageLoggingEnabled();

  /// Retuns true if this is a coordinator, false otherwise.
  bool IsCoordinator();

  /// Returns true if this is an executor, false otherwise.
  bool IsExecutor();

  typedef boost::unordered_map<std::string, TBackendDescriptor> BackendDescriptorMap;
  const BackendDescriptorMap& GetKnownBackends();

  // Mapping between query option names and levels
  QueryOptionLevels query_option_levels_;

  /// The prefix of audit event log filename.
  static const string AUDIT_EVENT_LOG_FILE_PREFIX;

  /// Per-session state.  This object is reference counted using shared_ptrs.  There
  /// is one ref count in the SessionStateMap for as long as the session is active.
  /// All queries running from this session also have a reference.
  struct SessionState {
    /// The default hs2_version must be V1 so that child queries (which use HS2, but may
    /// run as children of Beeswax sessions) get results back in the expected format -
    /// child queries inherit the HS2 version from their parents, and a Beeswax session
    /// will never update the HS2 version from the default.
    SessionState(ImpalaServer* impala_server) : impala_server(impala_server),
        closed(false), expired(false), hs2_version(apache::hive::service::cli::thrift::
        TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V1), total_queries(0), ref_count(0) {
      DCHECK(this->impala_server != nullptr);
    }

    /// Pointer to the Impala server of this session
    ImpalaServer* impala_server;

    TSessionType::type session_type;

    /// Time the session was created, in ms since epoch (UTC).
    int64_t start_time_ms;

    /// Connected user for this session, i.e. the user which originated this session.
    std::string connected_user;

    /// The user to delegate to. Empty for no delegation.
    std::string do_as_user;

    /// Client network address.
    TNetworkAddress network_address;

    /// Protects all fields below. See "Locking" in the class comment for lock
    /// acquisition order.
    boost::mutex lock;

    /// If true, the session has been closed.
    bool closed;

    /// If true, the session was idle for too long and has been expired. Only set when
    /// ref_count == 0, after which point ref_count should never become non-zero (since
    /// clients will use ScopedSessionState to access the session, which will prevent use
    /// after expiration).
    bool expired;

    /// The default database (changed as a result of 'use' query execution).
    std::string database;

    /// Reference to the ImpalaServer's query options
    TQueryOptions* server_default_query_options;

    /// Query options that have been explicitly set in this session.
    TQueryOptions set_query_options;

    /// BitSet indicating which query options in set_query_options have been
    /// explicitly set in the session. Updated when a query option is specified using a
    /// SET command: the bit corresponding to the TImpalaQueryOptions enum is set.
    /// If the option is subsequently reset via a SET with an empty value, the bit
    /// is cleared.
    QueryOptionsMask set_query_options_mask;

    /// For HS2 only, the protocol version this session is expecting.
    apache::hive::service::cli::thrift::TProtocolVersion::type hs2_version;

    /// Inflight queries belonging to this session
    boost::unordered_set<TUniqueId> inflight_queries;

    /// Total number of queries run as part of this session.
    int64_t total_queries;

    /// Time the session was last accessed, in ms since epoch (UTC).
    int64_t last_accessed_ms;

    /// The latest Kudu timestamp observed after DML operations executed within this
    /// session.
    uint64_t kudu_latest_observed_ts;

    /// Number of RPCs concurrently accessing this session state. Used to detect when a
    /// session may be correctly expired after a timeout (when ref_count == 0). Typically
    /// at most one RPC will be issued against a session at a time, but clients may do
    /// something unexpected and, for example, poll in one thread and fetch in another.
    uint32_t ref_count;

    /// Per-session idle timeout in seconds. Default value is FLAGS_idle_session_timeout.
    /// It can be overridden with the query option "idle_session_timeout" when opening a
    /// HS2 session, or using the SET command.
    int32_t session_timeout = 0;

    /// Updates the session timeout based on the query option idle_session_timeout.
    /// It registers/unregisters the session timeout to the Impala server.
    /// The lock must be owned by the caller of this function.
    void UpdateTimeout();

    /// Builds a Thrift representation of this SessionState for serialisation to
    /// the frontend.
    void ToThrift(const TUniqueId& session_id, TSessionState* session_state);

    /// Builds the overlay of the default server query options and the options
    /// explicitly set in this session.
    TQueryOptions QueryOptions();
  };

 private:
  struct ExpirationEvent;
  friend class ChildQuery;
  friend class ImpalaHttpHandler;
  friend struct SessionState;

  boost::scoped_ptr<ImpalaHttpHandler> http_handler_;

  /// Relevant ODBC SQL State code; for more info,
  /// goto http://msdn.microsoft.com/en-us/library/ms714687.aspx
  static const char* SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION;
  static const char* SQLSTATE_GENERAL_ERROR;
  static const char* SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED;

  /// Return exec state for given query_id, or NULL if not found.
  std::shared_ptr<ClientRequestState> GetClientRequestState(
      const TUniqueId& query_id);

  /// Updates the number of databases / tables metrics from the FE catalog
  Status UpdateCatalogMetrics() WARN_UNUSED_RESULT;

  /// Starts asynchronous execution of query. Creates ClientRequestState (returned
  /// in exec_state), registers it and calls Coordinator::Execute().
  /// If it returns with an error status, exec_state will be NULL and nothing
  /// will have been registered in client_request_state_map_.
  /// session_state is a ptr to the session running this query and must have
  /// been checked out.
  /// query_session_state is a snapshot of session state that changes when the
  /// query was run. (e.g. default database).
  Status Execute(TQueryCtx* query_ctx,
      std::shared_ptr<SessionState> session_state,
      std::shared_ptr<ClientRequestState>* exec_state) WARN_UNUSED_RESULT;

  /// Implements Execute() logic, but doesn't unregister query on error.
  Status ExecuteInternal(const TQueryCtx& query_ctx,
      std::shared_ptr<SessionState> session_state, bool* registered_exec_state,
      std::shared_ptr<ClientRequestState>* exec_state) WARN_UNUSED_RESULT;

  /// Registers the query exec state with client_request_state_map_ using the
  /// globally unique query_id and add the query id to session state's open query list.
  /// The caller must have checked out the session state.
  Status RegisterQuery(std::shared_ptr<SessionState> session_state,
      const std::shared_ptr<ClientRequestState>& exec_state) WARN_UNUSED_RESULT;

  /// Adds the query to the set of in-flight queries for the session. The query remains
  /// in-flight until the query is unregistered.  Until a query is in-flight, an attempt
  /// to cancel or close the query by the user will return an error status.  If the
  /// session is closed before a query is in-flight, then the query cancellation is
  /// deferred until after the issuing path has completed initializing the query.  Once
  /// a query is in-flight, it can be cancelled/closed asynchronously by the user
  /// (e.g. via an RPC) and the session close path can close (cancel and unregister) it.
  /// The query must have already been registered using RegisterQuery().  The caller
  /// must have checked out the session state.
  Status SetQueryInflight(std::shared_ptr<SessionState> session_state,
      const std::shared_ptr<ClientRequestState>& exec_state) WARN_UNUSED_RESULT;

  /// Unregister the query by cancelling it, removing exec_state from
  /// client_request_state_map_, and removing the query id from session state's
  /// in-flight query list.  If check_inflight is true, then return an error if the query
  /// is not yet in-flight.  Otherwise, proceed even if the query isn't yet in-flight (for
  /// cleaning up after an error on the query issuing path).
  Status UnregisterQuery(const TUniqueId& query_id, bool check_inflight,
      const Status* cause = NULL) WARN_UNUSED_RESULT;

  /// Initiates query cancellation reporting the given cause as the query status.
  /// Assumes deliberate cancellation by the user if the cause is NULL.  Returns an
  /// error if query_id is not found.  If check_inflight is true, then return an error
  /// if the query is not yet in-flight.  Otherwise, returns OK.  Queries still need to
  /// be unregistered, after cancellation.  Caller should not hold any locks when
  /// calling this function.
  Status CancelInternal(const TUniqueId& query_id, bool check_inflight,
      const Status* cause = NULL) WARN_UNUSED_RESULT;

  /// Close the session and release all resource used by this session.
  /// Caller should not hold any locks when calling this function.
  /// If ignore_if_absent is true, returns OK even if a session with the supplied ID does
  /// not exist.
  Status CloseSessionInternal(const TUniqueId& session_id, bool ignore_if_absent)
      WARN_UNUSED_RESULT;

  /// Gets the runtime profile string for a given query_id and writes it to the output
  /// stream. First searches for the query id in the map of in-flight queries. If no
  /// match is found there, the query log is searched. Returns OK if the profile was
  /// found, otherwise a Status object with an error message will be returned. The
  /// output stream will not be modified on error.
  /// If base64_encoded, outputs the base64 encoded profile output, otherwise the human
  /// readable string.
  /// If the user asking for this profile is the same user that runs the query
  /// and that user has access to the runtime profile, the profile is written to
  /// the output. Otherwise, nothing is written to output and an error code is
  /// returned to indicate an authorization error.
  Status GetRuntimeProfileStr(const TUniqueId& query_id, const std::string& user,
      bool base64_encoded, std::stringstream* output) WARN_UNUSED_RESULT;

  /// Returns the exec summary for this query if the user asking for the exec
  /// summary is the same user that run the query and that user has access to the full
  /// query profile. Otherwise, an error status is returned to indicate an
  /// authorization error.
  Status GetExecSummary(const TUniqueId& query_id, const std::string& user,
      TExecSummary* result) WARN_UNUSED_RESULT;

  /// Initialize "default_configs_" to show the default values for ImpalaQueryOptions and
  /// "support_start_over/false" to indicate that Impala does not support start over
  /// in the fetch call.
  void InitializeConfigVariables();

  /// Sets the option level for parameter 'option' based on the mapping stored in
  /// 'query_option_levels_'. The option level is used by the Impala shell when it
  /// displays the options. 'option_key' is the key for the 'query_option_levels_'
  /// to get the level of the query option.
  void AddOptionLevelToConfig(beeswax::ConfigVariable* option,
      const string& option_key) const;

  /// Checks settings for profile logging, including whether the output
  /// directory exists and is writeable, and initialises the first log file.
  /// Returns OK unless there is some problem preventing profile log files
  /// from being written. If an error is returned, the constructor will disable
  /// profile logging.
  Status InitProfileLogging() WARN_UNUSED_RESULT;

  /// Checks settings for audit event logging, including whether the output
  /// directory exists and is writeable, and initialises the first log file.
  /// Returns OK unless there is some problem preventing audit event log files
  /// from being written. If an error is returned, impalad startup will be aborted.
  Status InitAuditEventLogging() WARN_UNUSED_RESULT;

  /// Checks settings for lineage logging, including whether the output
  /// directory exists and is writeable, and initialises the first log file.
  /// Returns OK unless there is some problem preventing lineage log files
  /// from being written. If an error is returned, impalad startup will be aborted.
  Status InitLineageLogging() WARN_UNUSED_RESULT;

  /// Initializes a logging directory, creating the directory if it does not already
  /// exist. If there is any error creating the directory an error will be returned.
  static Status InitLoggingDir(const std::string& log_dir) WARN_UNUSED_RESULT;

  /// Returns true if audit event logging is enabled, false otherwise.
  bool IsAuditEventLoggingEnabled();

  Status LogAuditRecord(
      const ClientRequestState& exec_state, const TExecRequest& request)
      WARN_UNUSED_RESULT;

  Status LogLineageRecord(const ClientRequestState& exec_state) WARN_UNUSED_RESULT;

  /// Log audit and column lineage events
  void LogQueryEvents(const ClientRequestState& exec_state);

  /// Runs once every 5s to flush the profile log file to disk.
  [[noreturn]] void LogFileFlushThread();

  /// Runs once every 5s to flush the audit log file to disk.
  [[noreturn]] void AuditEventLoggerFlushThread();

  /// Runs once every 5s to flush the lineage log file to disk.
  [[noreturn]] void LineageLoggerFlushThread();

  /// Copies a query's state into the query log. Called immediately prior to a
  /// ClientRequestState's deletion. Also writes the query profile to the profile log
  /// on disk.
  void ArchiveQuery(const ClientRequestState& query);

  /// Checks whether the given user is allowed to delegate as the specified do_as_user.
  /// Returns OK if the authorization suceeds, otherwise returns an status with details
  /// on why the failure occurred.
  Status AuthorizeProxyUser(const std::string& user, const std::string& do_as_user)
      WARN_UNUSED_RESULT;

  // Check if the local backend descriptor is in the list of known backends. If not, add
  // it to the list of known backends and add it to the 'topic_updates'.
  void AddLocalBackendToStatestore(std::vector<TTopicDelta>* topic_updates);

  /// Snapshot of a query's state, archived in the query log.
  struct QueryStateRecord {
    /// Pretty-printed runtime profile. TODO: Copy actual profile object
    std::string profile_str;

    /// Base64 encoded runtime profile
    std::string encoded_profile_str;

    /// Query id
    TUniqueId id;

    /// Queries are run and authorized on behalf of the effective_user.
    /// If there is no delegated user, this will be the connected user. Otherwise, it
    /// will be set to the delegated user.
    std::string effective_user;

    /// If true, effective_user has access to the runtime profile and execution
    /// summary.
    bool user_has_profile_access;

    /// default db for this query
    std::string default_db;

    /// SQL statement text
    std::string stmt;

    /// Text representation of plan
    std::string plan;

    /// DDL, DML etc.
    TStmtType::type stmt_type;

    /// True if the query required a coordinator fragment
    bool has_coord;

    /// The number of fragments that have completed
    int64_t num_complete_fragments;

    /// The total number of fragments
    int64_t total_fragments;

    /// The number of rows fetched by the client
    int64_t num_rows_fetched;

    /// The state of the query as of this snapshot
    beeswax::QueryState::type query_state;

    /// Start and end time of the query, in Unix microseconds.
    /// A query whose end_time_us is 0 indicates that it is an in-flight query.
    /// These two variables are initialized with the corresponding values from
    /// ClientRequestState.
    int64_t start_time_us, end_time_us;

    /// Summary of execution for this query.
    TExecSummary exec_summary;

    Status query_status;

    /// Timeline of important query events
    TEventSequence event_sequence;

    /// Save the query plan fragments so that the plan tree can be rendered on the debug
    /// webpages.
    vector<TPlanFragment> fragments;

    // If true, this query has no more rows to return
    bool all_rows_returned;

    // The most recent time this query was actively being processed, in Unix milliseconds.
    int64_t last_active_time_ms;

    /// Request pool to which the request was submitted for admission, or an empty string
    /// if this request doesn't have a pool.
    std::string request_pool;

    /// Initialise from an exec_state. If copy_profile is true, print the query
    /// profile to a string and copy that into this.profile (which is expensive),
    /// otherwise leave this.profile empty.
    /// If encoded_str is non-empty, it is the base64 encoded string for
    /// exec_state->profile.
    QueryStateRecord(const ClientRequestState& exec_state, bool copy_profile = false,
        const std::string& encoded_str = "");

    /// Default constructor used only when participating in collections
    QueryStateRecord() { }
  };

  struct QueryStateRecordLessThan {
    /// Comparator that sorts by start time.
    bool operator() (const QueryStateRecord& lhs, const QueryStateRecord& rhs) const;
  };

  /// Beeswax private methods

  /// Helper functions to translate between Beeswax and Impala structs
  Status QueryToTQueryContext(const beeswax::Query& query, TQueryCtx* query_ctx)
      WARN_UNUSED_RESULT;
  void TUniqueIdToQueryHandle(const TUniqueId& query_id, beeswax::QueryHandle* handle);
  void QueryHandleToTUniqueId(const beeswax::QueryHandle& handle, TUniqueId* query_id);

  /// Helper function to raise BeeswaxException
  [[noreturn]] void RaiseBeeswaxException(const std::string& msg, const char* sql_state);

  /// Executes the fetch logic. Doesn't clean up the exec state if an error occurs.
  Status FetchInternal(const TUniqueId& query_id, bool start_over,
      int32_t fetch_size, beeswax::Results* query_results) WARN_UNUSED_RESULT;

  /// Populate insert_result and clean up exec state. If the query
  /// status is an error, insert_result is not populated and the status is returned.
  Status CloseInsertInternal(const TUniqueId& query_id, TInsertResult* insert_result)
      WARN_UNUSED_RESULT;

  /// HiveServer2 private methods (implemented in impala-hs2-server.cc)

  /// Starts the synchronous execution of a HiverServer2 metadata operation.
  /// If the execution succeeds, an ClientRequestState will be created and registered in
  /// client_request_state_map_. Otherwise, nothing will be registered in
  /// client_request_state_map_ and an error status will be returned. As part of this
  /// call, the TMetadataOpRequest struct will be populated with the requesting user's
  /// session state.
  /// Returns a TOperationHandle and TStatus.
  void ExecuteMetadataOp(
      const apache::hive::service::cli::thrift::THandleIdentifier& session_handle,
      TMetadataOpRequest* request,
      apache::hive::service::cli::thrift::TOperationHandle* handle,
      apache::hive::service::cli::thrift::TStatus* status);

  /// Executes the fetch logic for HiveServer2 FetchResults. If fetch_first is true, then
  /// the query's state should be reset to fetch from the beginning of the result set.
  /// Doesn't clean up the exec state if an error occurs.
  Status FetchInternal(const TUniqueId& query_id, int32_t fetch_size, bool fetch_first,
      apache::hive::service::cli::thrift::TFetchResultsResp* fetch_results)
      WARN_UNUSED_RESULT;

  /// Helper functions to translate between HiveServer2 and Impala structs

  /// Returns !ok() if handle.guid.size() or handle.secret.size() != 16
  static Status THandleIdentifierToTUniqueId(
      const apache::hive::service::cli::thrift::THandleIdentifier& handle,
      TUniqueId* unique_id, TUniqueId* secret) WARN_UNUSED_RESULT;
  static void TUniqueIdToTHandleIdentifier(
      const TUniqueId& unique_id, const TUniqueId& secret,
      apache::hive::service::cli::thrift::THandleIdentifier* handle);
  Status TExecuteStatementReqToTQueryContext(
      const apache::hive::service::cli::thrift::TExecuteStatementReq execute_request,
      TQueryCtx* query_ctx) WARN_UNUSED_RESULT;

  /// Helper method to process cancellations that result from failed backends, called from
  /// the cancellation thread pool. The cancellation_work contains the query id to cancel
  /// and a cause listing the failed backends that led to cancellation. Calls
  /// CancelInternal directly, but has a signature compatible with the thread pool.
  void CancelFromThreadPool(uint32_t thread_id,
      const CancellationWork& cancellation_work);

  /// Helper method to add any pool query options to the query_ctx. Must be called before
  /// ExecuteInternal() at which point the TQueryCtx is const and cannot be mutated.
  /// override_options_mask indicates which query options can be overridden by the pool
  /// default query options.
  void AddPoolQueryOptions(TQueryCtx* query_ctx,
      const QueryOptionsMask& override_options_mask);

  /// Register timeout value upon opening a new session. This will wake up
  /// session_timeout_thread_.
  void RegisterSessionTimeout(int32_t timeout);

  /// Unregister timeout value.
  void UnregisterSessionTimeout(int32_t timeout);

  /// To be run in a thread which wakes up every second. This function checks all
  /// sessions for their last-idle times. Those that have been idle for longer than
  /// their configured timeout values are 'expired': they will no longer accept queries
  /// and any running queries associated with those sessions are unregistered.
  [[noreturn]] void ExpireSessions();

  /// Runs forever, walking queries_by_timestamp_ and expiring any queries that have been
  /// idle (i.e. no client input and no time spent processing locally) for
  /// FLAGS_idle_query_timeout seconds.
  [[noreturn]] void ExpireQueries();

  /// Expire 'crs' and cancel it with status 'status'.
  void ExpireQuery(ClientRequestState* crs, const Status& status);

  /// Guards query_log_ and query_log_index_
  boost::mutex query_log_lock_;

  /// FIFO list of query records, which are written after the query finishes executing
  typedef std::list<QueryStateRecord> QueryLog;
  QueryLog query_log_;

  /// Index that allows lookup via TUniqueId into the query log
  typedef boost::unordered_map<TUniqueId, QueryLog::iterator> QueryLogIndex;
  QueryLogIndex query_log_index_;

  /// Logger for writing encoded query profiles, one per line with the following format:
  /// <ms-since-epoch> <query-id> <thrift query profile URL encoded and gzipped>
  boost::scoped_ptr<SimpleLogger> profile_logger_;

  /// Logger for writing audit events, one per line with the format:
  /// "<current timestamp>" : { JSON object }
  boost::scoped_ptr<SimpleLogger> audit_event_logger_;

  /// Logger for writing lineage events, one per line with the format:
  /// { JSON object }
  boost::scoped_ptr<SimpleLogger> lineage_logger_;

  /// If profile logging is enabled, wakes once every 5s to flush query profiles to disk
  std::unique_ptr<Thread> profile_log_file_flush_thread_;

  /// If audit event logging is enabled, wakes once every 5s to flush audit events to disk
  std::unique_ptr<Thread> audit_event_logger_flush_thread_;

  /// If lineage logging is enabled, wakes once every 5s to flush lineage events to disk
  std::unique_ptr<Thread> lineage_logger_flush_thread_;

  /// global, per-server state
  ExecEnv* exec_env_;  // not owned

  /// Thread pool to process cancellation requests that come from failed Impala demons to
  /// avoid blocking the statestore callback.
  boost::scoped_ptr<ThreadPool<CancellationWork>> cancellation_thread_pool_;

  /// Thread that runs ExpireSessions. It will wake up periodically to check for sessions
  /// which are idle for more their timeout values.
  std::unique_ptr<Thread> session_timeout_thread_;

  /// Contains all the non-zero idle session timeout values.
  std::multiset<int32_t> session_timeout_set_;

  /// The lock for protecting the session_timeout_set_.
  boost::mutex session_timeout_lock_;

  /// session_timeout_thread_ relies on the following conditional variable to wake up
  /// when there are sessions that have a timeout.
  ConditionVariable session_timeout_cv_;

  /// maps from query id to exec state; ClientRequestState is owned by us and referenced
  /// as a shared_ptr to allow asynchronous deletion
  typedef class ShardedQueryMap<std::shared_ptr<ClientRequestState>>
      ClientRequestStateMap;
  ClientRequestStateMap client_request_state_map_;

  /// Default query options in the form of TQueryOptions and beeswax::ConfigVariable
  TQueryOptions default_query_options_;
  std::vector<beeswax::ConfigVariable> default_configs_;

  /// Class that allows users of SessionState to mark a session as in-use, and therefore
  /// immune to expiration. The marking is done in WithSession() and undone in the
  /// destructor, so this class can be used to 'check-out' a session for the duration of a
  /// scope.
  class ScopedSessionState {
   public:
    ScopedSessionState(ImpalaServer* impala) : impala_(impala) { }

    /// Marks a session as in-use, and saves it so that it can be unmarked when this
    /// object goes out of scope. Returns OK unless there is an error in GetSessionState.
    /// Must only be called once per ScopedSessionState.
    Status WithSession(const TUniqueId& session_id,
        std::shared_ptr<SessionState>* session = NULL) WARN_UNUSED_RESULT {
      DCHECK(session_.get() == NULL);
      RETURN_IF_ERROR(impala_->GetSessionState(session_id, &session_, true));
      if (session != NULL) (*session) = session_;
      return Status::OK();
    }

    /// Decrements the reference count so the session can be expired correctly.
    ~ScopedSessionState() {
      if (session_.get() != NULL) {
        impala_->MarkSessionInactive(session_);
      }
    }

   private:
    /// Reference-counted pointer to the session state object.
    std::shared_ptr<SessionState> session_;

    /// Saved so that we can access ImpalaServer methods to get / return session state.
    ImpalaServer* impala_;
  };

  /// For access to GetSessionState() / MarkSessionInactive()
  friend class ScopedSessionState;

  /// Protects session_state_map_. See "Locking" in the class comment for lock
  /// acquisition order.
  boost::mutex session_state_map_lock_;

  /// A map from session identifier to a structure containing per-session information
  typedef boost::unordered_map<TUniqueId, std::shared_ptr<SessionState>> SessionStateMap;
  SessionStateMap session_state_map_;

  /// Protects connection_to_sessions_map_. See "Locking" in the class comment for lock
  /// acquisition order.
  boost::mutex connection_to_sessions_map_lock_;

  /// Map from a connection ID to the associated list of sessions so that all can be
  /// closed when the connection ends. HS2 allows for multiplexing several sessions across
  /// a single connection. If a session has already been closed (only possible via HS2) it
  /// is not removed from this map to avoid the cost of looking it up.
  typedef boost::unordered_map<TUniqueId, std::vector<TUniqueId>>
    ConnectionToSessionMap;
  ConnectionToSessionMap connection_to_sessions_map_;

  /// Returns session state for given session_id.
  /// If not found, session_state will be NULL and an error status will be returned.
  /// If mark_active is true, also checks if the session is expired or closed and
  /// increments the session's reference counter if it is still alive.
  Status GetSessionState(const TUniqueId& session_id,
      std::shared_ptr<SessionState>* session_state, bool mark_active = false)
      WARN_UNUSED_RESULT;

  /// Decrement the session's reference counter and mark last_accessed_ms so that state
  /// expiration can proceed.
  inline void MarkSessionInactive(std::shared_ptr<SessionState> session) {
    boost::lock_guard<boost::mutex> l(session->lock);
    DCHECK_GT(session->ref_count, 0);
    --session->ref_count;
    session->last_accessed_ms = UnixMillis();
  }

  /// Protects query_locations_. Not held in conjunction with other locks.
  boost::mutex query_locations_lock_;

  /// A map from backend to the list of queries currently running there.
  typedef boost::unordered_map<TNetworkAddress, boost::unordered_set<TUniqueId>>
      QueryLocations;
  QueryLocations query_locations_;

  /// A map from unique backend ID to the corresponding TBackendDescriptor of that
  /// backend. Used to track membership updates from the statestore so queries can be
  /// cancelled when a backend is removed. It's not enough to just cancel fragments that
  /// are running based on the deletions mentioned in the most recent statestore
  /// heartbeat; sometimes cancellations are skipped and the statestore, at its
  /// discretion, may send only a delta of the current membership so we need to compute
  /// any deletions.
  /// TODO: Currently there are multiple locations where cluster membership is tracked,
  /// here and in the scheduler. This should be consolidated so there is a single
  /// component (the scheduler?) that tracks this information and calls other interested
  /// components.
  BackendDescriptorMap known_backends_;

  /// Generate unique session id for HiveServer2 session
  boost::uuids::random_generator uuid_generator_;

  /// Lock to protect uuid_generator
  boost::mutex uuid_lock_;

  /// Lock for catalog_update_version_, min_subscriber_catalog_topic_version_,
  /// and catalog_version_update_cv_
  boost::mutex catalog_version_lock_;

  /// Variable to signal when the catalog version has been modified
  ConditionVariable catalog_version_update_cv_;

  /// Contains details on the version information of a catalog update.
  struct CatalogUpdateVersionInfo {
    CatalogUpdateVersionInfo() :
      catalog_version(0L),
      catalog_topic_version(0L),
      min_catalog_object_version(0L) {
    }
    /// Update the metrics to store the current version of catalog, current topic and
    /// current service id used by impalad.
    void  UpdateCatalogVersionMetrics();
    /// The last catalog version returned from UpdateCatalog()
    int64_t catalog_version;
    /// The CatalogService ID that this catalog version is from.
    TUniqueId catalog_service_id;
    /// The statestore catalog topic version this update was received in.
    int64_t catalog_topic_version;
    /// Minimum catalog object version after a call to UpdateCatalog()
    int64_t min_catalog_object_version;
  };

  /// The version information from the last successfull call to UpdateCatalog().
  CatalogUpdateVersionInfo catalog_update_info_;

  /// The current minimum topic version processed across all subscribers of the catalog
  /// topic. Used to determine when other nodes have successfully processed a catalog
  /// update. Updated with each catalog topic heartbeat from the statestore.
  int64_t min_subscriber_catalog_topic_version_;

  /// Map of short usernames of authorized proxy users to the set of user(s) they are
  /// allowed to delegate to. Populated by parsing the --authorized_proxy_users_config
  /// flag.
  typedef boost::unordered_map<std::string, boost::unordered_set<std::string>>
      ProxyUserMap;
  ProxyUserMap authorized_proxy_user_config_;

  /// Guards queries_by_timestamp_. See "Locking" in the class comment for lock
  /// acquisition order.
  boost::mutex query_expiration_lock_;

  enum class ExpirationKind {
    // The query is cancelled if the query has been inactive this long. The event may
    // cancel the query after checking the last active time.
    IDLE_TIMEOUT,
    // A hard time limit on query execution. The query is cancelled if this event occurs
    // before the query finishes.
    EXEC_TIME_LIMIT
  };

  // Describes a query expiration event where the query identified by 'query_id' is
  // checked for expiration when UnixMillis() exceeds 'deadline'.
  struct ExpirationEvent {
    int64_t deadline;
    TUniqueId query_id;
    ExpirationKind kind;
  };

  /// Comparator that breaks ties when two queries have identical expiration deadlines.
  struct ExpirationEventComparator {
    bool operator()(const ExpirationEvent& t1, const ExpirationEvent& t2) {
      if (t1.deadline < t2.deadline) return true;
      if (t2.deadline < t1.deadline) return false;
      if (t1.query_id < t2.query_id) return true;
      if (t2.query_id < t1.query_id) return false;
      return t1.kind < t2.kind;
    }
  };

  /// Ordered set of (expiration_time, query_id) pairs. This queue is updated either by
  /// RegisterQuery(), which adds a new query to the set, or by ExpireQueries(), which
  /// updates the entries as it iterates over the set. Therefore, it is not directly
  /// accessed during normal query execution and the benefit of that is there is no
  /// competition for locks when ExpireQueries() walks this list to find expired queries.
  //
  /// In order to make the query expiration algorithm work, the following condition always
  /// holds:
  //
  /// * For any pair (t, q) in the set, t is always a lower bound on the true expiration
  /// time of the query q (in q->idle_time()). Therefore we can bound the expiration error
  /// by sleeping to no more than t + d for some small d (in our implementation, d is 1s).
  //
  /// The reason that the expiration time saved in each entry here may not exactly match
  /// the true expiration time of a query is because we wish to avoid accessing (and
  /// therefore locking) this structure on every query activity. Instead, queries maintain
  /// an accurate expiration time, and this structure guarantees that we will always
  /// (modulo scheduling delays out of our control) read the expiration time before it has
  /// passed.
  typedef std::set<ExpirationEvent, ExpirationEventComparator> ExpirationQueue;
  ExpirationQueue queries_by_timestamp_;

  /// Container for a thread that runs ExpireQueries() if FLAGS_idle_query_timeout is set.
  std::unique_ptr<Thread> query_expiration_thread_;

  /// Serializes TBackendDescriptors when creating topic updates
  ThriftSerializer thrift_serializer_;

  /// True if this ImpalaServer can accept client connections and coordinate
  /// queries.
  bool is_coordinator_;

  /// True if this ImpalaServer can execute query fragments.
  bool is_executor_;

  /// Containers for client and internal services. May not be set if the ports passed to
  /// Init() were <= 0.
  /// Note that these hold a shared pointer to 'this', and so need to be reset()
  /// explicitly.
  boost::scoped_ptr<ThriftServer> beeswax_server_;
  boost::scoped_ptr<ThriftServer> hs2_server_;
  boost::scoped_ptr<ThriftServer> thrift_be_server_;

  /// Flag that records if backend and/or client services have been started. The flag is
  /// set after all services required for the server have been started.
  std::atomic_bool services_started_;

  /// Set to true when this ImpalaServer should shut down.
  Promise<bool> shutdown_promise_;
};


}

#endif
