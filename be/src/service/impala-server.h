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

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <boost/random/random_device.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "common/status.h"
#include "gen-cpp/BackendGflags_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ImpalaHiveServer2Service.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/control_service.pb.h"
#include "gen-cpp/Query_types.h"
#include "kudu/util/random.h"
#include "rpc/thrift-server.h"
#include "runtime/types.h"
#include "service/internal-server.h"
#include "service/query-options.h"
#include "service/query-state-record.h"
#include "service/workload-management.h"
#include "statestore/statestore-subscriber.h"
#include "util/condition-variable.h"
#include "util/container-util.h"
#include "util/runtime-profile.h"
#include "util/sharded-query-map-util.h"
#include "util/simple-logger.h"
#include "util/thread-pool.h"
#include "util/ticker.h"
#include "util/time.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {
using kudu::ThreadSafeRandom;

class BackendDescriptorPB;
class BackendExecParamsPB;
class ExecEnv;
class DataSink;
class CancellationWork;
class ImpalaHttpHandler;
class RowDescriptor;
class TDmlResult;
class TExecutePlannedStatementReq;
class TNetworkAddress;
class TClientRequest;
class TExecRequest;
class TSessionState;
class TQueryOptions;
class TGetExecSummaryResp;
class TGetExecSummaryReq;
class ClientRequestState;
class QueryDriver;
struct QueryHandle;
class QueryScanner;
class SimpleLogger;
class UpdateFilterParamsPB;
class UpdateFilterResultPB;
class TQueryExecRequest;

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
///    - Registers the ImpalaServer instance with the ExecEnv. This also registers it with
///      the ClusterMembershipMgr, which will register it with the statestore as soon as
///      the local backend becomes available through GetLocalBackendDescriptor(), which is
///      in turn gated by the services_started_.
///    - Start internal services
///    - Wait (indefinitely) for local catalog to be initialized from statestore
///      (if coordinator)
///    - Open ImpalaInternalService ports
///    - Open client ports (if coordinator)
///    - Start ImpalaInternalService API
///    - Start client service API's (if coordinator)
///    - Set services_started_ flag
///
///
/// Shutdown
/// --------
/// Impala Server shutdown can be initiated by a remote shutdown command from another
/// Impala daemon or by a local shutdown command from a user session. The shutdown
/// sequence aims to quiesce the Impalad (i.e. drain it of any running finstances or
/// client requests) then exit the process cleanly. The shutdown sequence is as follows:
///
/// 1. StartShutdown() is called to initiate the process.
/// 2. The startup grace period starts, during which:
///   - no new client requests are accepted. Clients can still interact with registered
///     requests and sessions as normal.
///   - the local backend of the Impala daemon is marked in the statestore as quiescing,
///     so coordinators will not schedule new fragments on it (once the statestore update
///     propagates through the ClusterMembershipMgr).
///   - the Impala daemon continues to start executing any new fragments sent to it by
///     coordinators. This is required because the query may have been submitted before
///     the coordinator learned that the executor was quiescing. Delays occur for several
///     reasons:
///     -> Latency of membership propagation through the statestore
///     -> Latency of query startup work including scheduling, admission control and
///        fragment startup.
///     -> Queuing delay in the admission controller (which may be unbounded).
/// 3. The startup grace period elapses.
/// 4. The background shutdown thread periodically checks to see if the Impala daemon is
///    quiesced (i.e. no client requests are registered and no queries are executing on
///    the backend). If it is quiesced then it cleanly shuts down by exiting the process.
///    The statestore will detect that the process is not responding to heartbeats and
///    remove any entries.
/// 5. The shutdown deadline elapses. The Impala daemon exits regardless of whether
///    it was successfully quiesced or not.
///
/// If shutdown is initiated again during this process, it does not cancel the existing
/// shutdown but can decrease the deadline, e.g. if an administrator starts shutdown
/// with a deadline of 1 hour, but then wants to shut down the cluster sooner, they can
/// run the shutdown function again to set a shorter deadline. The deadline can't be
/// increased after shutdown is started.
///
/// Secrets
/// -------
/// The HS2 protocol has a concept of a 'secret' associated with each session and
/// client request that is returned to the client, then must be passed back along with
/// each RPC that interacts with the session or operation. RPCs with non-matching secrets
/// should fail. Beeswax does not have this concept - rather sessions are bound to a
/// connection, and that session and associated requests should (generally) only be
/// accessed from the session's connection.
///
/// All RPC handler functions are responsible for validating secrets when looking up
/// client sessions or requests, with some exceptions:
/// * Beewax sessions are tied to a connection, so it is always valid to access the
///   session from the original connection.
/// * Cancel() and close() Beeswax operations can cancel any query if the ID is known.
///   Existing tools (impala-shell and administrative tools) depend on this behaviour.
///
/// HS2 RPC handlers should pass the client-provided secret to WithSession() before
/// taking any action on behalf of the client. Beeswax RPC handlers should call
/// CheckClientRequestSession() to ensure that the client request is accessible from
/// the current session. All other functions assume that the validation was already done.
///
/// HTTP handlers do not have access to client secrets, so do not validate them.
///
/// Locking
/// -------
/// This class is partially thread-safe. To ensure freedom from deadlock, if multiple
/// locks are acquired, lower-numbered locks must be acquired before higher-numbered
/// locks:
/// 1. session_state_map_lock_
/// 2. SessionState::lock
/// 3. query_expiration_lock_
/// 4. idle_query_statuses_lock_
/// 5. ClientRequestState::fetch_rows_lock
/// 6. ClientRequestState::lock
/// 7. ClientRequestState::expiration_data_lock_
/// 8. Coordinator::exec_summary_lock
///
/// The following locks are not held in conjunction with other locks:
/// * query_log_lock_
/// * session_timeout_lock_
/// * query_locations_lock_
/// * uuid_lock_
/// * catalog_version_lock_
/// * connection_to_sessions_map_lock_
/// * per_user_session_count_lock_
///
/// TODO: The same doesn't apply to the execution state of an individual plan
/// fragment: the originating coordinator might die, but we can get notified of
/// that via the statestore. This still needs to be implemented.
class ImpalaServer : public ImpalaServiceIf,
                     public ImpalaHiveServer2ServiceIf,
                     public ThriftServer::ConnectionHandlerIf,
                     public InternalServer,
                     public std::enable_shared_from_this<ImpalaServer>,
                     public CacheLineAligned {
 public:

  ImpalaServer(ExecEnv* exec_env);
  ~ImpalaServer();

  /// Initializes and starts RPC services and other subsystems (like audit logging).
  /// Returns an error if starting any services failed.
  ///
  /// Different port values have special behaviour. A port > 0 explicitly specifies
  /// the port the server run on. A port value of 0 means to choose an arbitrary
  /// ephemeral port in tests and to not start the service in a daemon. A port < 0
  /// always means to not start the service. The port values can be obtained after
  /// Start() by calling GetBeeswaxPort() or GetHS2Port().
  Status Start(int32_t beeswax_port, int32_t hs2_port, int32_t hs2_http_port,
      int32_t external_fe_port);

  /// Blocks until the server shuts down.
  void Join();

  /// ImpalaService rpcs: Beeswax API (implemented in impala-beeswax-server.cc)
  virtual void query(beeswax::QueryHandle& beeswax_handle, const beeswax::Query& query);
  virtual void executeAndWait(beeswax::QueryHandle& beeswax_handle,
      const beeswax::Query& query, const beeswax::LogContextId& client_ctx);
  virtual void explain(beeswax::QueryExplanation& query_explanation,
      const beeswax::Query& query);
  virtual void fetch(beeswax::Results& query_results,
      const beeswax::QueryHandle& beeswax_handle, const bool start_over,
      const int32_t fetch_size);
  virtual void get_results_metadata(beeswax::ResultsMetadata& results_metadata,
      const beeswax::QueryHandle& beeswax_handle);
  virtual void close(const beeswax::QueryHandle& beeswax_handle);
  virtual beeswax::QueryState::type get_state(const beeswax::QueryHandle& beeswax_handle);
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
  virtual void Cancel(
      impala::TStatus& status, const beeswax::QueryHandle& beeswax_handle);
  virtual void CloseInsert(
      impala::TDmlResult& dml_result, const beeswax::QueryHandle& beeswax_handle);

  /// Pings the Impala service and gets the server version string.
  virtual void PingImpalaService(TPingImpalaServiceResp& return_val);

  virtual void GetRuntimeProfile(std::string& profile_output,
      const beeswax::QueryHandle& beeswax_handle);

  virtual void GetExecSummary(impala::TExecSummary& result,
      const beeswax::QueryHandle& beeswax_handle);

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
  virtual void GetPrimaryKeys(
      apache::hive::service::cli::thrift::TGetPrimaryKeysResp& return_val,
      const apache::hive::service::cli::thrift::TGetPrimaryKeysReq& request);
  virtual void GetCrossReference(
      apache::hive::service::cli::thrift::TGetCrossReferenceResp& return_val,
      const apache::hive::service::cli::thrift::TGetCrossReferenceReq& request);
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

  // Extensions to HS2 implemented by ImpalaHiveServer2Service.

  /// Pings the Impala service and gets the server version string.
  virtual void PingImpalaHS2Service(TPingImpalaHS2ServiceResp& return_val,
      const TPingImpalaHS2ServiceReq& req);

  // Initialize a query context for external frontend
  virtual void InitQueryContext(TInitQueryContextResp& return_val);

  // Execute the provided Thrift statement/plan
  virtual void ExecutePlannedStatement(
      apache::hive::service::cli::thrift::TExecuteStatementResp& return_val,
      const TExecutePlannedStatementReq& req);

  // Retrieves the current BackendConfig
  virtual void GetBackendConfig(TGetBackendConfigResp& return_val,
      const TGetBackendConfigReq& request);

  // Retrieves the current ExecutorMembership
  virtual void GetExecutorMembership(
      TGetExecutorMembershipResp& return_val, const TGetExecutorMembershipReq& request);

  /// Closes an Impala operation and returns additional information about the closed
  /// operation.
  virtual void CloseImpalaOperation(
      TCloseImpalaOperationResp& return_val, const TCloseImpalaOperationReq& request);

  void UpdateFilter(UpdateFilterResultPB* return_val, const UpdateFilterParamsPB& params,
      kudu::rpc::RpcContext* context);

  /// Generates a unique id for this query and sets it in the given query context.
  /// Prepares the given query context by populating fields required for evaluating
  /// certain expressions, such as now(), pid(), etc. Should be called before handing
  /// the query context to the frontend for query compilation.
  void PrepareQueryContext(TQueryCtx* query_ctx);

  /// Static helper for PrepareQueryContext() that is used from expr-benchmark.
  static void PrepareQueryContext(const std::string& hostname,
      const NetworkAddressPB& krpc_addr, TQueryCtx* query_ctx);

  /// ThriftServer::ConnectionHandlerIf methods

  /// Called when a Beeswax or HS2 connection starts. For Beeswax, registers a new
  /// SessionState associated with the new connection. For HS2, this is a no-op (HS2
  /// has an explicit CreateSession RPC).
  virtual void ConnectionStart(const ThriftServer::ConnectionContext& session_context);

  /// Called when a Beeswax or HS2 connection terminates. Unregisters all sessions
  /// associated with the closed connection.
  virtual void ConnectionEnd(const ThriftServer::ConnectionContext& session_context);

  /// Returns true if the connection is considered idle. A connection is considered
  /// idle if all the sessions associated with it have expired due to idle timeout.
  /// Called when a client has been inactive for --idle_client_poll_period_s seconds.
  virtual bool IsIdleConnection(const ThriftServer::ConnectionContext& session_context);

  /// InternalServer methods, see internal-server.h for details
  virtual Status OpenSession(const std::string& user_name, TUniqueId& new_session_id,
      const QueryOptionMap& query_opts = {});
  virtual bool CloseSession(const impala::TUniqueId& session_id);
  virtual Status ExecuteIgnoreResults(const std::string& user_name,
      const std::string& sql, const QueryOptionMap& query_opts = {},
      const bool persist_in_db = true, TUniqueId* query_id = nullptr);
  virtual Status ExecuteAndFetchAllText(const std::string& user_name,
      const std::string& sql, query_results& results, results_columns* columns = nullptr,
      TUniqueId* query_id = nullptr);
  virtual Status SubmitAndWait(const std::string& user_name, const std::string& sql,
      TUniqueId& new_session_id, TUniqueId& new_query_id,
      const QueryOptionMap& query_opts = {}, const bool persist_in_db = true);
  virtual Status WaitForResults(TUniqueId& query_id);
  virtual Status SubmitQuery(const std::string& sql, const impala::TUniqueId& session_id,
      TUniqueId& new_query_id, const bool persist_in_db = true);
  virtual Status FetchAllRows(const TUniqueId& query_id, query_results& results,
      results_columns* columns = nullptr);
  virtual void CloseQuery(const TUniqueId& query_id);
  virtual void GetConnectionContextList(
      ThriftServer::ConnectionContextList* connection_contexts);
  /// end of InternalServer methods

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
  /// If 'wait_for_all_subscribers' is true, this function also waits for all other
  /// catalog topic subscribers to process this update by checking the current
  /// min_subscriber_topic_version included in each state store heartbeat.
  ///
  /// 'query_options' is used for running debug actions.
  Status ProcessCatalogUpdateResult(const TCatalogUpdateResult& catalog_update_result,
      bool wait_for_all_subscribers, const TQueryOptions& query_options,
      RuntimeProfile::EventSequence* timeline)
      WARN_UNUSED_RESULT;

  /// Wait until the catalog update with version 'catalog_update_version' is
  /// received and applied in the local catalog cache or until the catalog
  /// service id has changed.
  void WaitForCatalogUpdate(const int64_t catalog_update_version,
      const TUniqueId& catalog_service_id, RuntimeProfile::EventSequence* timeline);

  /// Wait until the minimum catalog object version in the local cache is
  /// greater than 'min_catalog_update_version' or until the catalog
  /// service id has changed.
  void WaitForMinCatalogUpdate(const int64_t min_catalog_update_version,
      const TUniqueId& catalog_service_id, RuntimeProfile::EventSequence* timeline);

  /// Wait until the last applied catalog update has been broadcast to
  /// all coordinators or until the catalog service id has changed.
  void WaitForCatalogUpdateTopicPropagation(const TUniqueId& catalog_service_id,
      RuntimeProfile::EventSequence* timeline);

  /// Returns true if lineage logging is enabled, false otherwise.
  ///
  /// DEPRECATED: lineage file logging has been deprecated in favor of
  ///             query execution hooks (FE)
  bool IsLineageLoggingEnabled();

  /// Returns true if query execution (FE) hooks are enabled, false otherwise.
  bool AreQueryHooksEnabled();

  /// Returns true if audit event logging is enabled, false otherwise.
  bool IsAuditEventLoggingEnabled();

  /// Retuns true if this is a coordinator, false otherwise.
  bool IsCoordinator();

  /// Returns true if this is an executor, false otherwise.
  bool IsExecutor();

  /// Returns whether this backend is healthy, i.e. able to accept queries.
  bool IsHealthy();

  /// Returns the port that the Beeswax server is listening on. Valid to call after
  /// the server has started successfully.
  int GetBeeswaxPort();

  /// Returns the port that the Beeswax server is listening on. Valid to call after
  /// the server has started successfully.
  int GetHS2Port();

  /// Return the number of live queries managed by this server. Acquires
  /// completed_queries_lock_ to check for completed queries that have not been written.
  /// (implemented in workload-management.cc)
  size_t NumLiveQueries();

  /// Returns a current snapshot of the local backend descriptor.
  std::shared_ptr<const BackendDescriptorPB> GetLocalBackendDescriptor();

  /// Adds the query_id to the map from backend to the list of queries running or expected
  /// to run there (query_locations_). After calling this function, the server will cancel
  /// a query with an error if one of its backends fail.
  void RegisterQueryLocations(
      const google::protobuf::RepeatedPtrField<BackendExecParamsPB>& per_backend_params,
      const TUniqueId& query_id);

  /// Takes a set of backend ids of active backends and cancels all the queries running on
  /// failed ones (that is, ids not in the active set).
  void CancelQueriesOnFailedBackends(
      const std::unordered_set<BackendIdPB>& current_membership);

  /// Returns true if all services required for the server have been started and the
  /// registration with statestore is completed.
  bool AreServicesReady() const;

  /// Start the shutdown process. Return an error if it could not be started. Otherwise,
  /// if it was successfully started by this or a previous call, return OK along with
  /// information about the pending shutdown in 'shutdown_status'. 'relative_deadline_s'
  /// is the deadline value in seconds to use, or -1 if we should use the default
  /// deadline. See Shutdown class comment for explanation of the shutdown sequence.
  Status StartShutdown(int64_t relative_deadline_s, ShutdownStatusPB* shutdown_status);

  /// Returns true if a shut down is in progress.
  bool IsShuttingDown() const { return shutting_down_.Load() != 0; }

  /// Returns an informational error about why a new operation could not be started
  /// if the server is shutting down. Must be called before starting execution of a
  /// new operation (e.g. a query).
  Status CheckNotShuttingDown() const;

  /// Return information about the status of a shutdown. Only valid to call if a shutdown
  /// is in progress (i.e. IsShuttingDown() is true).
  ShutdownStatusPB GetShutdownStatus() const;

  /// Convert the shutdown status to a human-readable string.
  static std::string ShutdownStatusToString(const ShutdownStatusPB& shutdown_status);

  /// Appends the audit_entry to audit_event_logger_.
  Status AppendAuditEntry(const std::string& audit_entry);

  /// Appends the lineage_entry to lineage_logger_.
  Status AppendLineageEntry(const std::string& lineage_entry);

  /// Returns true if 'user' was configured as an authorized proxy user.
  bool IsAuthorizedProxyUser(const std::string& user) WARN_UNUSED_RESULT;

  /// Gets connection contexts for all types of thrift servers.
  void GetAllConnectionContexts(
      ThriftServer::ConnectionContextList* connection_contexts);

  // Mapping between query option names and levels
  QueryOptionLevels query_option_levels_;

  /// The default executor group name for executors that do not explicitly belong to a
  /// specific executor group.
  static const std::string DEFAULT_EXECUTOR_GROUP_NAME;

  /// Per-session state.  This object is reference counted using shared_ptrs.  There
  /// is one ref count in the SessionStateMap for as long as the session is active.
  /// All queries running from this session also have a reference, because query
  /// unregistration may complete asynchronously after the session is unregistered.
  struct SessionState {
    /// The default hs2_version must be V1 so that child queries (which use HS2, but may
    /// run as children of Beeswax sessions) get results back in the expected format -
    /// child queries inherit the HS2 version from their parents, and a Beeswax session
    /// will never update the HS2 version from the default.
    SessionState(ImpalaServer* impala_server, TUniqueId session_id, TUniqueId secret)
      : impala_server(impala_server), session_id(std::move(session_id)),
        secret(std::move(secret)),
        closed(false), expired(false), hs2_version(apache::hive::service::cli::thrift::
        TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V1), total_queries(0), ref_count(0) {
      DCHECK(this->impala_server != nullptr);
    }

    /// Pointer to the Impala server of this session
    ImpalaServer* impala_server;

    /// The unique session ID. This is also the key for session_state_map_, but
    /// we redundantly store it here for convenience.
    const TUniqueId session_id;

    /// The session secret that client RPCs must pass back in to access the session.
    /// This must not be printed to logs or exposed in any other way.
    const TUniqueId secret;

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
    std::mutex lock;

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

    /// Inflight queries belonging to this session. It represents the set of queries that
    /// are either queued or have started executing. Used primarily to identify queries
    /// that need to be closed if the session closes or expires.
    boost::unordered_set<TUniqueId> inflight_queries;

    /// Queries are added to inflight_queries via SetQueryInflight. Execution is async
    /// and can start before SetQueryInflight is called. If a query is retried before
    /// SetQueryInflight is called, the original may be cleaned up before it is added to
    /// inflight_queries. In that case we add it to prestopped_queries instead.
    std::set<TUniqueId> prestopped_queries;

    /// Unregistered queries we need to clear from idle_query_statuses_ on closure.
    std::vector<TUniqueId> idled_queries;

    /// Total number of queries run as part of this session.
    int64_t total_queries;

    /// Time the session was last accessed, in ms since epoch (UTC).
    int64_t last_accessed_ms;

    /// If this session has no open connections, this is the time in UTC when the last
    /// connection was closed.
    int64_t disconnected_ms;

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

    /// The connection ids of any connections that this session has been used over.
    std::set<TUniqueId> connections;

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

  /// Helper function that decrements the value associated with the given key.
  /// Removes the entry from the map if the value becomes zero.
  static void DecrementCount(std::map<std::string, int64>& loads, const std::string& key);

 private:
  struct ExpirationEvent;
  class SecretArg;
  friend class ChildQuery;
  friend class ControlService;
  friend class ImpalaHttpHandler;
  friend struct SessionState;
  friend class ImpalaServerTest;
  friend class QueryDriver;
  friend class QueryScanner;

  static const string BEESWAX_SERVER_NAME;
  static const string HS2_SERVER_NAME;
  static const string HS2_HTTP_SERVER_NAME;
  static const string INTERNAL_SERVER_NAME;
  // Used to identify external frontend RPC calls
  static const string EXTERNAL_FRONTEND_SERVER_NAME;

  boost::scoped_ptr<ImpalaHttpHandler> http_handler_;

  /// Relevant ODBC SQL State code; for more info,
  /// goto http://msdn.microsoft.com/en-us/library/ms714687.aspx
  static const char* SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION;
  static const char* SQLSTATE_GENERAL_ERROR;
  static const char* SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED;
  /// String format of retry information returned in GetLog() RPCs.
  static const char* GET_LOG_QUERY_RETRY_INFO_FORMAT;

  /// Used in situations where the client provides a session ID and a query ID and the
  /// caller needs to validate that the query can be accessed from the session. The two
  /// arguments are the session obtained by looking up the session ID provided by the
  /// RPC client and the effective user from the query. If the username doesn't match,
  /// return an error indicating that the query was not found (since we prefer not to
  /// leak information about the existence of queries that the user doesn't have
  /// access to).
  Status CheckClientRequestSession(SessionState* session,
      const std::string& client_request_effective_user, const TUniqueId& query_id);

  /// Updates a set of Impalad catalog metrics including number tables/databases and
  /// some cache metrics applicable to local catalog mode (if configured). Called at
  /// the end of statestore update callback (to account for metadata object changes)
  /// and at the end of query planning to update local catalog cache access metrics.
  Status UpdateCatalogMetrics() WARN_UNUSED_RESULT;

  /// Depending on the query type, this either submits the query to the admission
  /// controller for performing async admission control or starts asynchronous execution
  /// of query. Creates ClientRequestState (returned in query_handle), registers it and
  /// calls ClientRequestState::Execute(). If it returns with an error status,
  /// query_driver->request_state will be NULL and nothing will have been registered in
  /// query_driver_map_. session_state is a ptr to the session running this query and must
  /// have been checked out.
  /// external_exec_request is a statement that was prepared by an external frontend using
  /// Impala PlanNodes or null if the external frontend isn't being used.
  Status Execute(TQueryCtx* query_ctx, std::shared_ptr<SessionState> session_state,
      QueryHandle* query_handle, const TExecRequest* external_exec_request,
      const bool include_in_query_log = true) WARN_UNUSED_RESULT;

  /// Implements Execute() logic, but doesn't unregister query on error.
  Status ExecuteInternal(const TQueryCtx& query_ctx,
      const TExecRequest* external_exec_request,
      std::shared_ptr<SessionState> session_state, bool* registered_query,
      QueryHandle* query_handle);

  /// Common execution logic factored out from Execute to be shared with
  /// ExecutePlannedStatement. Optional TExecRequest is the provided Thrift
  /// request to execute instead of parsing and executing the SQL statement.
  void ExecuteStatementCommon(
      apache::hive::service::cli::thrift::TExecuteStatementResp& return_val,
      const apache::hive::service::cli::thrift::TExecuteStatementReq& request,
      const TExecRequest* external_exec_request = nullptr);

  /// Registers the query with query_driver_map_ using the globally unique query_id. The
  /// caller must have checked out the session state.
  Status RegisterQuery(const TUniqueId& query_id,
      std::shared_ptr<SessionState> session_state,
      QueryHandle* query_handle) WARN_UNUSED_RESULT;

  /// Adds the query to the set of in-flight queries for the session. The query remains
  /// in-flight until the query is unregistered. Until a query is in-flight, an attempt
  /// to cancel or close the query by the user will return an error status. A query that
  /// retries may close the original query state before SetQueryInflight is called; in
  /// that case it adds the query to prestopped_queries, which will bypass unnecessary
  /// setup when SetQueryInflight is later called on it.
  ///
  /// If the session is closed before a query is in-flight, then the query cancellation
  /// is deferred until after the issuing path has completed initializing the query. Once
  /// a query is in-flight, it can be cancelled/closed asynchronously by the user
  /// (e.g. via an RPC) and the session close path can close (cancel and unregister) it.
  ///
  /// The query must have already been registered using RegisterQuery(). The caller
  /// must have checked out the session state.
  Status SetQueryInflight(std::shared_ptr<SessionState> session_state,
      const QueryHandle& query_handle) WARN_UNUSED_RESULT;

  /// Starts the process of unregistering the query. The query is cancelled on the
  /// current thread, then asynchronously the query's entry is removed from
  /// query_driver_map_ and the session state's in-flight query list.
  /// If check_inflight is true, then return an error if the query
  /// is not yet in-flight. Otherwise, proceed even if the query isn't yet in-flight (for
  /// cleaning up after an error on the query issuing path).
  Status UnregisterQuery(const TUniqueId& query_id, bool check_inflight,
      const Status* cause = NULL) WARN_UNUSED_RESULT;

  /// Delegates to UnregisterQuery. If UnregisterQuery returns an error Status, the
  /// status is logged and then discarded.
  void UnregisterQueryDiscardResult(
      const TUniqueId& query_id, bool check_inflight, const Status* cause = NULL);

  /// Unregisters the provided query, does all required finalization and removes it from
  /// 'query_driver_map_'.
  void FinishUnregisterQuery(const QueryHandle& query_handle);

  /// Performs finalization of 'request_state' before the request state is removed from
  /// the server and deleted. Runs asynchronously after the request is reported done
  /// to the client. Removes the query from the inflight queries list, updates
  /// query_locations_, and archives the query.
  void CloseClientRequestState(const QueryHandle& query_handle);

  /// Initiates query cancellation triggered by the user (i.e. deliberate cancellation).
  /// Returns an error if query_id is not found or if the query is not yet in flight.
  /// Otherwise, returns OK. Queries still need to be unregistered after cancellation.
  /// Caller should not hold any locks when calling this function.
  Status CancelInternal(const TUniqueId& query_id);

  /// Close the session and release all resource used by this session.
  /// Caller should not hold any locks when calling this function.
  /// If ignore_if_absent is true, returns OK even if a session with the supplied ID does
  /// not exist.
  Status CloseSessionInternal(const TUniqueId& session_id, const SecretArg& secret,
      bool ignore_if_absent) WARN_UNUSED_RESULT;

  /// The output of a runtime profile. The output of a profile can be in one of three
  /// formats: string, thrift, or json. The format is specified by TRuntimeProfileFormat.
  /// The struct is a union of all output profiles types. The struct is similar to a
  /// union because only one field can be set.
  struct RuntimeProfileOutput {
    std::stringstream* string_output = nullptr;
    TRuntimeProfileTree* thrift_output = nullptr;
    rapidjson::Document* json_output = nullptr;
  };

  /// Gets the runtime profile string for a given query_id and writes it to the output
  /// stream. First searches for the query id in the map of in-flight queries. If no
  /// match is found there, the query log is searched. Returns OK if the profile was
  /// found, otherwise a Status object with an error message will be returned. The
  /// output stream will not be modified on error.
  /// On success, if 'format' is BASE64 or STRING then 'profile.string_output' will be
  /// set, or if 'format' is THRIFT then 'profile.thrift_output' will be set. If 'format'
  /// is JSON then 'profile.json_output' will be set.
  /// If the user asking for this profile is the same user that runs the query
  /// and that user has access to the runtime profile, the profile is written to
  /// the output. Otherwise, nothing is written to output and an error code is
  /// returned to indicate an authorization error.
  Status GetRuntimeProfileOutput(const TUniqueId& query_id, const string& user,
      TRuntimeProfileFormat::type format, RuntimeProfileOutput* profile);

  /// Similar to GetRuntimeProfileOutput above, unless the query was retried then it sets
  /// the runtime profile for both query attempts. 'original_profile' is set to the
  /// profile of the original query attempt (the one that failed), and 'retried_profile'
  /// is set to the profile of the most recent query attempt (the retried query attempt).
  /// If the query was not retried, 'retried_profile' is not set and 'original_profile'
  /// is set to the normal runtime profile. Both 'retried_profile' and 'original_profile'
  /// cannot be nullptr.
  Status GetRuntimeProfileOutput(const TUniqueId& query_id, const string& user,
      TRuntimeProfileFormat::type format, RuntimeProfileOutput* original_profile,
      RuntimeProfileOutput* retried_profile, bool* was_retried) WARN_UNUSED_RESULT;

  /// Helper method for GetRuntimeProfileOutput that fetches the runtime profile for the
  /// given QueryHandle.
  Status GetRuntimeProfileOutput(const string& user, const QueryHandle& query_handle,
      TRuntimeProfileFormat::type format, RuntimeProfileOutput* profile);

  /// Set the profile (or thrift_profile) field for the given TRuntimeProfileFormat
  /// using the profile from the given RuntimeProfileOutput. If 'set_failed_profile'
  /// is true, then the profile is added to the 'failed_profile' field of
  /// TGetRuntimeProfileResp, otherwise it is added to the normal profile field.
  void SetProfile(TGetRuntimeProfileResp& get_profile_resp,
      TRuntimeProfileFormat::type profile_format, const RuntimeProfileOutput& profile,
      bool set_failed_profile = false);

  /// Returns the exec summary for this query if the user asking for the exec
  /// summary is the same user that run the query and that user has access to the full
  /// query profile. Otherwise, an error status is returned to indicate an
  /// authorization error.
  /// If 'original_result' and 'was_retried' are not null pointers, returns whether the
  /// query is retried in '*was_retried'. If the query is retried, returns the exec
  /// summary of the original query in '*original_result'. '*original_result' won't be
  /// set if the query is not retried. 'original_result' and 'was_retried' should be both
  /// valid pointers when any of them is used.
  Status GetExecSummary(const TUniqueId& query_id, const std::string& user,
      TExecSummary* result, TExecSummary* original_result = nullptr,
      bool* was_retried = nullptr) WARN_UNUSED_RESULT;

  /// Collect ExecSummary and update it to the profile in request_state
  void UpdateExecSummary(const QueryHandle& query_handle) const;

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

  /// Runs once every 5s to flush the profile log file to disk.
  [[noreturn]] void LogFileFlushThread();

  /// Runs once every 5s to flush the audit log file to disk.
  [[noreturn]] void AuditEventLoggerFlushThread();

  /// Runs once every 5s to flush the lineage log file to disk.
  [[noreturn]] void LineageLoggerFlushThread();

  /// Copies a query's state into the query log. Called immediately prior to a
  /// ClientRequestState's deletion. Also writes the query profile to the profile log
  /// on disk.
  void ArchiveQuery(const QueryHandle& query_handle);

  /// Checks whether the given user is allowed to delegate as the specified do_as_user.
  /// Returns OK if the authorization suceeds, otherwise returns an status with details
  /// on why the failure occurred.
  Status AuthorizeProxyUser(const std::string& user, const std::string& do_as_user)
      WARN_UNUSED_RESULT;

  /// Initializes the backend descriptor in 'be_desc' with the local backend information.
  void BuildLocalBackendDescriptorInternal(BackendDescriptorPB* be_desc);

  /// Converts a type to a string. The only complex types supported are array and map,
  /// which are returned as strings (see IMPALA-11041).
  std::string ColumnTypeToBeeswaxTypeString(const TColumnType& type);

  /// Places a completed query into the in-memory queue of completed queries.
  /// (implemented in workload-management.cc)
  void EnqueueCompletedQuery(const QueryHandle& query_handle,
      const std::shared_ptr<QueryStateRecord> qs_rec);

  /// Returns the active QueryHandle for this query id. The QueryHandle contains the
  /// active ClientRequestState. Returns an error Status if the query id cannot be found.
  /// If caller is an RPC thread, RPC context will be registered for time tracking
  /// See QueryDriver for a description of active ClientRequestStates.
  Status GetActiveQueryHandle(
      const TUniqueId& query_id, QueryHandle* query_handle);

  /// Similar to 'GetActiveQueryHandle' except it does not return the active handle, it
  /// returns the handle directly associated with the given query id. Returns an error
  /// Status if the query id cannot be found. See QueryDriver for a description of active
  /// ClientRequestStates. See 'GetQueryDriver' for a description of the
  /// 'return_unregistered' parameter.
  Status GetQueryHandle(const TUniqueId& query_id, QueryHandle* query_handle,
      bool return_unregistered = false);

  /// Returns both the active QueryHandle and the original QueryHandle for this query id.
  /// In scenarios that require both QueryHandles, calling 'GetActiveQueryHandle' and
  /// 'GetQueryHandle' one by one may have race conditions with QueryDriver deletion
  /// causing QueryDriver not found in the second call. Use this method in such cases.
  /// See 'GetQueryDriver' for a description of the 'return_unregistered' parameter.
  Status GetAllQueryHandles(const TUniqueId& query_id, QueryHandle* active_query_handle,
      QueryHandle* original_query_handle, bool return_unregistered = false);

  /// Returns the QueryDriver for the given query_id, or nullptr if not found. If
  /// 'return_unregistered' is true, queries that have started unregistration
  /// may be returned. Otherwise queries that have started unregistration will
  /// not be returned.
  std::shared_ptr<QueryDriver> GetQueryDriver(
      const TUniqueId& query_id, bool return_unregistered = false);

  /// Beeswax private methods

  /// Helper functions to translate between Beeswax and Impala structs
  Status QueryToTQueryContext(
      const beeswax::Query& query, TQueryCtx* query_ctx) WARN_UNUSED_RESULT;
  void TUniqueIdToBeeswaxHandle(
      const TUniqueId& query_id, beeswax::QueryHandle* beeswax_handle);
  void BeeswaxHandleToTUniqueId(
      const beeswax::QueryHandle& beeswax_handle, TUniqueId* query_id);

  /// Helper function to raise BeeswaxException
  [[noreturn]] void RaiseBeeswaxException(const std::string& msg, const char* sql_state);

  /// Executes the fetch logic. Doesn't clean up the exec state if an error occurs.
  Status FetchInternal(TUniqueId query_id, bool start_over,
      int32_t fetch_size, beeswax::Results* query_results) WARN_UNUSED_RESULT;

  /// Populate dml_result and clean up exec state. If the query
  /// status is an error, dml_result is not populated and the status is returned.
  /// 'session' is RPC client's session, used to check whether the DML can
  /// be closed via that session.
  Status CloseInsertInternal(SessionState* session, const TUniqueId& query_id,
      TDmlResult* dml_result) WARN_UNUSED_RESULT;

  /// HiveServer2 private methods (implemented in impala-hs2-server.cc)

  /// Starts the synchronous execution of a HiverServer2 metadata operation.
  /// If the execution succeeds, a QueryDriver and ClientRequestState will be created
  /// and registered in query_driver_map_. Otherwise, nothing will be registered in
  /// query_driver_map_ and an error status will be returned. As part of this
  /// call, the TMetadataOpRequest struct will be populated with the requesting user's
  /// session state.
  /// Returns a TOperationHandle and TStatus.
  void ExecuteMetadataOp(
      const apache::hive::service::cli::thrift::THandleIdentifier& session_handle,
      TMetadataOpRequest* request,
      apache::hive::service::cli::thrift::TOperationHandle* handle,
      apache::hive::service::cli::thrift::TStatus* status);

  /// Executes the fetch logic for HiveServer2 FetchResults and stores result size in
  /// 'num_results'. If fetch_first is true, then the query's state should be reset to
  /// fetch from the beginning of the result set. Doesn't clean up exec state if an
  /// error occurs.
  Status FetchInternal(TUniqueId query_id, SessionState* session, int32_t fetch_size,
      bool fetch_first,
      apache::hive::service::cli::thrift::TFetchResultsResp* fetch_results,
      int32_t* num_results) WARN_UNUSED_RESULT;

  /// Setup the results cache. The results cache saves the results for a query so that
  /// clients can re-read rows they have already fetched. The cache is used to support
  /// TFetchOrientation::FETCH_FIRST option in TFetchResultsReq. Results cacheing only
  /// saves results for as long as the query is open; results are not shared between
  /// queries. This feature is useful for clients such as Hue, which allow users to
  /// dynamically scroll through the results, but also allow users to download a file
  /// containing all the results for a query.
  Status SetupResultsCacheing(const QueryHandle& query_handle,
      std::shared_ptr<SessionState> session, int64_t cache_num_rows) WARN_UNUSED_RESULT;

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

  /// Blocks until results are available. Handles any query retries that might occur
  /// while waiting for results to be produced. It uses 'BlockOnWait' to wait for
  /// results to be available. It is possible for a query to be retried while BlockOnWait
  /// is running. If that happens, this method will first check if the query has been
  /// retried. If it has been retried, it waits until the retry has started and then calls
  /// BlockOnWait again, but with the QueryHandle of the retried query.
  Status WaitForResults(const TUniqueId& query_id, QueryHandle* query_handle,
      int64_t* block_on_wait_time_us, bool* timed_out);

  /// Blocks until results from the given QueryHandle are ready to be fetched, or
  /// until the given timeout is hit. 'timed_out' is set to true if the timeout
  /// 'block_on_wait_time_us' was exceeded, false otherwise.
  void BlockOnWait(
      QueryHandle& query_handle, bool* timed_out, int64_t* block_on_wait_time_us);

  /// Helper method to process cancellations that result from failed backends, called from
  /// the cancellation thread pool. The cancellation_work contains the query id to cancel
  /// and a cause listing the failed backends that led to cancellation. Calls
  /// CancelInternal directly, but has a signature compatible with the thread pool.
  void CancelFromThreadPool(const CancellationWork& cancellation_work);

  /// Helper method to add the pool name and query options to the query_ctx. Must be
  /// called before ExecuteInternal() at which point the TQueryCtx is const and cannot
  /// be mutated. override_options_mask indicates which query options can be overridden
  /// by the pool default query options.
  void AddPoolConfiguration(TQueryCtx* query_ctx,
      const QueryOptionsMask& override_options_mask);

  /// Helper method to enforce a pool's max mt_dop setting. If the provided maximum is
  /// nonnegative and mt_dop is set higher than the maximum, the mt_dop is reduced to the
  /// maximum. Otherwise, the mt_dop value is not modified.
  void EnforceMaxMtDop(TQueryCtx* query_ctx, int64_t max_mt_dop);

  /// Register timeout value upon opening a new session. This will wake up
  /// session_timeout_thread_.
  void RegisterSessionTimeout(int32_t timeout);

  /// Unregister timeout value.
  void UnregisterSessionTimeout(int32_t timeout);

  /// To be run in a thread which wakes up every second if there are registered sesions
  /// timeouts. This function checks all sessions for:
  /// - Last-idle times. Those that have been idle for longer than their configured
  ///   timeout values are 'expired': they will no longer accept queries.
  /// - Disconnected times. Those that have had no active connections for longer than
  ///   FLAGS_disconnected_session_timeout are closed: they are removed from the session
  ///   state map and can no longer be accessed by clients.
  /// For either case any running queries associated with those sessions are unregistered.
  [[noreturn]] void SessionMaintenance();

  /// Runs forever, walking queries_by_timestamp_ and expiring any queries that have been
  /// idle (i.e. no client input and no time spent processing locally) for
  /// FLAGS_idle_query_timeout seconds.
  [[noreturn]] void ExpireQueries();

  /// Periodically iterates over all queries and cancels any where a backend hasn't sent a
  /// status report in greater than GetMaxReportRetryMs().
  [[noreturn]] void UnresponsiveBackendThread();

  /// If the admission control service is enabled, periodically sends a list of all
  /// current query ids to the admissiond.
  [[noreturn]] void AdmissionHeartbeatThread();

  /// If workload management is enabled, starts workload management threads.
  /// (implemented in workload-management.cc)
  Status InitWorkloadManagement();

  /// Blocks until running workload management threads are shut down.
  /// (implemented in workload-management.cc)
  void ShutdownWorkloadManagement();

  /// Periodically writes out completed queries (if configured)
  /// (implemented in workload-management.cc)
  void CompletedQueriesThread();

  /// Returns a list of completed queries that have not yet been written to storage.
  /// Acquires completed_queries_lock_ to make a copy of completed_queries_ state.
  /// (implemented in workload-management.cc)
  std::vector<std::shared_ptr<QueryStateExpanded>> GetCompletedQueries();

  /// Called from ExpireQueries() to check query resource limits for 'crs'. If the query
  /// exceeded a resource limit, returns a non-OK status with information about what
  /// limit was exceeded. Returns OK if the query will continue running and expiration
  /// check should be rescheduled for a later time.
  Status CheckResourceLimits(ClientRequestState* crs);

  /// Expire 'crs' and cancel it with status 'status'. Optionally unregisters the query.
  void ExpireQuery(ClientRequestState* crs, const Status& status,
      bool unregister = false);

  typedef boost::unordered_map<std::string, boost::unordered_set<std::string>>
      AuthorizedProxyMap;
  /// Populates authorized proxy config into the given map.
  /// For example:
  /// - authorized_proxy_config: foo=abc,def;bar=ghi
  /// - authorized_proxy_config_delimiter: ,
  /// - authorized_proxy_map: {foo:[abc, def], bar=s[ghi]}
  static Status PopulateAuthorizedProxyConfig(
      const std::string& authorized_proxy_config,
      const std::string& authorized_proxy_config_delimiter,
      AuthorizedProxyMap* authorized_proxy_map);

  /// Background thread that does the shutdown.
  [[noreturn]] void ShutdownThread();

  /// Random number generator for use in this class, thread safe.
  static ThreadSafeRandom rng_;

  /// Guards query_log_, query_log_index_, and query_log_est_sizes_.
  std::mutex query_log_lock_;

  /// FIFO list of query records, which are written after the query finishes executing.
  /// Queries may briefly have entries in 'query_log_' and 'query_driver_map_'
  /// while the query is being unregistered. To ensure that the records provided by
  /// GetQueryRecord function are always valid, this list uses shared_ptr to hold
  /// QueryStateRecord, preventing its lifecycle from ending prematurely due to removal
  /// from the list.
  typedef std::list<std::shared_ptr<QueryStateRecord>> QueryLog;
  QueryLog query_log_;

  /// Index that allows lookup via TUniqueId into the query log. The QueryStateRecord
  /// pointer is owned by 'query_log_' so the entry in this index must be removed when
  /// it is removed from 'query_log_'.
  typedef boost::unordered_map<TUniqueId, std::shared_ptr<QueryStateRecord>*>
      QueryLogIndex;
  QueryLogIndex query_log_index_;

  /// Estimated individual size of records in query_log_ in bytes.
  std::list<int64_t> query_log_est_sizes_;

  /// Sets the given query_record (and retried_query_record too if given) for the given
  /// query_id. Returns an error Status if the given query_id cannot be found in the
  /// QueryLogIndex.
  Status GetQueryRecord(
      const TUniqueId& query_id, std::shared_ptr<QueryStateRecord>* query_record,
      std::shared_ptr<QueryStateRecord>* retried_query_record = nullptr);

  /// Decompresses the profile in the given QueryStateRecord into the specified format.
  /// The decompressed profile is added to the given RuntimeProfileOutput.
  Status DecompressToProfile(TRuntimeProfileFormat::type format,
      std::shared_ptr<QueryStateRecord> query_record, RuntimeProfileOutput* profile);

  void WaitForNewCatalogServiceId(TUniqueId cur_service_id,
      std::unique_lock<std::mutex>* ver_lock);

  /// Random `impala::TUniqueID` generator. Use wherever a new `TUniqueId` is needed.
  TUniqueId RandomUniqueID();

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

  /// Thread pool to unregister queries asynchronously from RPCs. FinishUnregisterQuery()
  /// is called on all QueryHandles added to this pool.
  boost::scoped_ptr<ThreadPool<QueryHandle>> unreg_thread_pool_;

  /// Thread that runs SessionMaintenance. It will wake up periodically to check for
  /// sessions which are idle for more their timeout values.
  std::unique_ptr<Thread> session_maintenance_thread_;

  /// Contains all the non-zero idle or disconnected session timeout values.
  std::multiset<int32_t> session_timeout_set_;

  /// The lock for protecting the session_timeout_set_.
  std::mutex session_timeout_lock_;

  /// session_timeout_thread_ relies on the following conditional variable to wake up
  /// when there are sessions that have a timeout.
  ConditionVariable session_timeout_cv_;

  /// Thread that runs UnresponsiveBackendThread().
  std::unique_ptr<Thread> unresponsive_backend_thread_;

  /// Thread that runs AdmissionHeartbeatThread().
  std::unique_ptr<Thread> admission_heartbeat_thread_;

  /// The QueryDriverMap maps query ids to QueryDrivers. The QueryDrivers are owned by the
  /// ImpalaServer and QueryDriverMap references them using shared_ptr to allow
  /// asynchronous deletion.
  ///
  /// QueryDrivers are unregistered from the server as follows:
  /// 1. UnregisterQuery() is called, which calls ClientRequestState::Finalize() to cancel
  ///    query execution and start the unregistration process. At this point the query is
  ///    considered unregistered from the client's point of view.
  /// 2. The ClientRequestState is enqueued in 'unreg_thread_pool_' to complete
  ///    unregistration asynchronously.
  /// 3. Additional cleanup work is done by CloseClientRequestState(), and an entry
  ///    is added to 'query_log_' for this query.
  /// 4. The QueryDriver is removed from this map.
  typedef ShardedQueryMap<std::shared_ptr<QueryDriver>> QueryDriverMap;
  QueryDriverMap query_driver_map_;

  /// Default query options in the form of TQueryOptions and beeswax::ConfigVariable
  TQueryOptions default_query_options_;
  std::vector<beeswax::ConfigVariable> default_configs_;

  // Container for a secret passed into functions for validation.
  class SecretArg {
   public:
    // This should only be used if the client has not provided a secret but the caller
    // knows that the client in fact is allowed to access the session or operation that
    // they are accessing.
    static SecretArg SkipSecretCheck() {
      return SecretArg(true, true, TUniqueId(), TUniqueId());
    }

    /// Pass a session secret for validation.
    static SecretArg Session(const TUniqueId& secret) {
      return SecretArg(false, true, secret, TUniqueId());
    }

    /// Pass a query secret for validation. The error message will include the 'query_id',
    /// so that must also be passed.
    static SecretArg Operation(const TUniqueId& secret, const TUniqueId& query_id) {
      return SecretArg(false, false, secret, query_id);
    }

    /// Return true iff the check should be skipped or the secret matches 'other'.
    /// All validation should be done via this function to avoid subtle errors.
    bool Validate(const TUniqueId& other) const {
      return skip_validation_ || ConstantTimeCompare(other) == 0;
    }

    bool is_session_secret() const { return is_session_secret_; }
    TUniqueId query_id() const {
      DCHECK(!is_session_secret_);
      return query_id_;
    }

   private:
    SecretArg(bool skip_validation, bool is_session_secret, TUniqueId secret,
        TUniqueId query_id)
      : skip_validation_(skip_validation),
        is_session_secret_(is_session_secret),
        secret_(std::move(secret)),
        query_id_(std::move(query_id)) {}

    /// A comparison function for unique IDs that executes in an amount of time unrelated
    /// to the input values. Returns the number of words that differ between id1 and id2.
    int ConstantTimeCompare(const TUniqueId& other) const;

    /// True if the caller wants to skip validation of the session.
    const bool skip_validation_;

    /// True if this is a session secret, false if this is an operation secret.
    const bool is_session_secret_;

    /// The secret to validate.
    const TUniqueId secret_;

    /// The query id, only provided if this is an operation secret.
    const TUniqueId query_id_;
  };

  /// Class that allows users of SessionState to mark a session as in-use, and therefore
  /// immune to expiration. The marking is done in WithSession() and undone in the
  /// destructor, so this class can be used to 'check-out' a session for the duration of a
  /// scope.
  class ScopedSessionState {
   public:
    ScopedSessionState(ImpalaServer* impala) : impala_(impala) { }

    /// Marks a session as in-use, and saves it so that it can be unmarked when this
    /// object goes out of scope. Returns OK unless there is an error in GetSessionState.
    /// 'secret' must be provided and is validated against the stored secret for the
    /// session. Must only be called once per ScopedSessionState.
    Status WithSession(const TUniqueId& session_id, const SecretArg& secret,
        std::shared_ptr<SessionState>* session = NULL) WARN_UNUSED_RESULT {
      DCHECK(session_.get() == NULL);
      RETURN_IF_ERROR(impala_->GetSessionState(
            session_id, secret, &session_, /* mark_active= */ true));
      if (session != NULL) (*session) = session_;

      // We won't have a connection context in the case of ChildQuery, which calls into
      // hiveserver2 functions directly without going through the Thrift stack.
      if (ThriftServer::HasThreadConnectionContext()) {
        // Check that the session user matches the user authenticated on the connection.
        const ThriftServer::Username& connection_username =
            ThriftServer::GetThreadConnectionContext()->username;
        if (!connection_username.empty()
            && session_->connected_user != connection_username) {
          return Status::Expected(TErrorCode::UNAUTHORIZED_SESSION_USER,
              connection_username, session_->connected_user);
        }

        // Try adding the session id to the connection's set of sessions in case this is
        // the first time this session has been used on this connection.
        impala_->AddSessionToConnection(session_id, session_.get());
      }
      return Status::OK();
    }

    /// Same as WithSession(), except:
    /// * It should only be called from beeswax with a 'connection_id' obtained from
    ///   ThriftServer::GetThreadConnectionId().
    /// * It does not update the session/connection mapping, as beeswax sessions can
    ///   only be used over a single connection.
    Status WithBeeswaxSession(const TUniqueId& connection_id,
        std::shared_ptr<SessionState>* session = NULL) {
      DCHECK(session_.get() == NULL);
      RETURN_IF_ERROR(
          impala_->GetSessionState(connection_id, SecretArg::SkipSecretCheck(), &session_,
              /* mark_active= */ true));
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
  std::mutex session_state_map_lock_;

  /// A map from session identifier to a structure containing per-session information
  typedef boost::unordered_map<TUniqueId, std::shared_ptr<SessionState>> SessionStateMap;
  SessionStateMap session_state_map_;

  /// Protects idle_query_statuses_;
  std::mutex idle_query_statuses_lock_;

  /// A map of queries that were stopped due to idle timeout and the status they had when
  /// unregistered. Used to return a more useful error when looking up unregistered IDs.
  std::map<TUniqueId, Status> idle_query_statuses_;

  /// Protects connection_to_sessions_map_. See "Locking" in the class comment for lock
  /// acquisition order.
  std::mutex connection_to_sessions_map_lock_;

  /// A map from user to a count of sessions created by the user.
  typedef std::map<std::string, int64> SessionCounts;
  SessionCounts per_user_session_count_map_;

  /// Protects per_user_session_count_map_. See "Locking" in the class comment for lock
  /// acquisition order.
  std::mutex per_user_session_count_lock_;

  /// Increment the count of HS2 sessions used by the user.
  /// If max_hs2_sessions_per_user is greater than zero, and the count of HS2 sessions
  /// used by the user would be above that value, then an error status is returned.
  Status IncrementAndCheckSessionCount(const string& user_name);
  /// Decrement the count of HS2 sessions used by the user.
  void DecrementSessionCount(const string& user_name);

  /// Map from a connection ID to the associated list of sessions so that all can be
  /// closed when the connection ends. HS2 allows for multiplexing several sessions across
  /// a single connection. If a session has already been closed (only possible via HS2) it
  /// is not removed from this map to avoid the cost of looking it up.
  typedef boost::unordered_map<TUniqueId, std::set<TUniqueId>> ConnectionToSessionMap;
  ConnectionToSessionMap connection_to_sessions_map_;

  /// Map storing connections opened by the InternalServer functions. Key is the session
  /// id, value is a shared pointer holding the connection's ConnectionContext. Use the
  /// `internal_server_connections_lock_` mutex whenever accessing this map
  typedef std::map<TUniqueId, std::shared_ptr<ThriftServer::ConnectionContext>>
      SessionToConnectionContext;
  SessionToConnectionContext internal_server_connections_;
  std::mutex internal_server_connections_lock_;

  /// Returns session state for given session_id.
  /// If not found or validation of 'secret' against the stored secret in the
  /// SessionState fails, session_state will be NULL and an error status will be returned.
  /// If mark_active is true, also checks if the session is expired or closed and
  /// increments the session's reference counter if it is still alive.
  Status GetSessionState(const TUniqueId& session_id, const SecretArg& secret,
      std::shared_ptr<SessionState>* session_state, bool mark_active = false)
      WARN_UNUSED_RESULT;

  /// Decrement the session's reference counter and mark last_accessed_ms so that state
  /// expiration can proceed.
  inline void MarkSessionInactive(std::shared_ptr<SessionState> session) {
    std::lock_guard<std::mutex> l(session->lock);
    DCHECK_GT(session->ref_count, 0);
    --session->ref_count;
    session->last_accessed_ms = UnixMillis();
  }

  /// Increment the session's reference counter.
  inline void MarkSessionActive(std::shared_ptr<SessionState> session) {
    std::lock_guard<std::mutex> l(session->lock);
    ++session->ref_count;
  }

  /// Associate the current connection context with the given session in
  /// 'connection_to_sessions_map_' and 'SessionState::connections'.
  void AddSessionToConnection(const TUniqueId& session_id, SessionState* session);

  /// Protects query_locations_. Not held in conjunction with other locks.
  std::mutex query_locations_lock_;

  /// Entries in the 'query_locations' map.
  struct QueryLocationInfo {
    QueryLocationInfo(NetworkAddressPB address, TUniqueId query_id) : address(address) {
      query_ids.insert(query_id);
    }

    /// Used for logging and error messages so that users don't have to translate between
    /// the BackendIdPB and a hostname themselves.
    NetworkAddressPB address;

    /// Queries currently running or expected to run at this location.
    std::unordered_set<TUniqueId> query_ids;
  };

  /// Contains info about what queries are running on each backend, so that they can be
  /// cancelled if the backend goes down.
  typedef std::unordered_map<BackendIdPB, QueryLocationInfo> QueryLocations;
  QueryLocations query_locations_;

  /// The local backend descriptor. Updated in GetLocalBackendDescriptor() and protected
  /// by 'local_backend_descriptor_lock_';
  std::shared_ptr<const BackendDescriptorPB> local_backend_descriptor_;
  std::mutex local_backend_descriptor_lock_;

  /// UUID generator for session IDs and secrets. Uses system random device to get
  /// cryptographically secure random numbers.
  boost::uuids::basic_random_generator<boost::random_device> crypto_uuid_generator_;

  /// Lock to protect uuid_generator
  std::mutex uuid_lock_;

  /// Lock for catalog_update_version_, min_subscriber_catalog_topic_version_,
  /// and catalog_version_update_cv_
  std::mutex catalog_version_lock_;

  /// Variable to signal when the catalog version has been modified
  ConditionVariable catalog_version_update_cv_;

  /// Contains details on the version information of a catalog update.
  struct CatalogUpdateVersionInfo {
    CatalogUpdateVersionInfo() :
      catalog_version(0L),
      catalog_topic_version(0L),
      catalog_object_version_lower_bound(0L) {
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
    /// Lower bound of catalog object versions after a call to UpdateCatalog()
    int64_t catalog_object_version_lower_bound;
  };

  /// The version information from the last successfull call to UpdateCatalog().
  CatalogUpdateVersionInfo catalog_update_info_;

  /// The current minimum topic version processed across all subscribers of the catalog
  /// topic. Used to determine when other nodes have successfully processed a catalog
  /// update. Updated with each catalog topic heartbeat from the statestore.
  int64_t min_subscriber_catalog_topic_version_;

  /// Map of short usernames of authorized proxy users to the set of users they are
  /// allowed to delegate to. Populated by parsing the --authorized_proxy_users_config
  /// flag.
  AuthorizedProxyMap authorized_proxy_user_config_;
  /// Map of short usernames of authorized proxy users to the set of groups they are
  /// allowed to delegate to. Populated by parsing the --authorized_proxy_groups_config
  /// flag.
  AuthorizedProxyMap authorized_proxy_group_config_;

  /// Guards queries_by_timestamp_. See "Locking" in the class comment for lock
  /// acquisition order.
  std::mutex query_expiration_lock_;

  enum class ExpirationKind {
    // The query is cancelled if the query has been inactive this long. The event may
    // cancel the query after checking the last active time.
    IDLE_TIMEOUT,
    // A hard time limit on query execution. The query is cancelled if this event occurs
    // before the query finishes.
    EXEC_TIME_LIMIT,
    // A hard limit on cpu and scanned bytes. The query is cancelled if this event occurs
    // before the query finishes.
    RESOURCE_LIMIT,
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
    bool operator()(const ExpirationEvent& t1, const ExpirationEvent& t2) const {
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
  boost::scoped_ptr<ThriftServer> hs2_http_server_;
  boost::scoped_ptr<ThriftServer> external_fe_server_;
  std::shared_ptr<InternalServer> internal_server_;

  /// Flag that records if backend and/or client services have been started. The flag is
  /// set after all services required for the server have been started.
  std::atomic_bool services_started_;

  /// Whether the Impala server shutdown process started. If 0, shutdown was not started.
  /// Otherwise, this is the MonotonicMillis() value when the shut down was started.
  AtomicInt64 shutting_down_{0};

  /// The MonotonicMillis() value after we should shut down regardless of registered
  /// client requests and running finstances. Set before 'shutting_down_' and updated
  /// atomically if a new shutdown command with a shorter deadline comes in.
  AtomicInt64 shutdown_deadline_{0};

  /// Stores the last version number for the admission heartbeat that was sent.
  /// Incremented every time a new admission heartbeat is sent.
  int64_t admission_heartbeat_version_ = 0;

  /// Workload Management Related Declarations.
  /// Coordinate periodic execution of the completed queries queue processing thread.
  std::condition_variable completed_queries_cv_;

  /// Coordinate shutdown of the completed queries queue processing thread.
  std::condition_variable completed_queries_shutdown_cv_;

  /// Tracks the state of the thread that drains the completed queries queue to the table.
  /// The associated lock must be held before reading/modifying this variable.
  impala::workload_management::ThreadState completed_queries_thread_state_ =
      impala::workload_management::NOT_STARTED;
  std::mutex completed_queries_threadstate_mu_;

  /// Thread that runs CompletedQueriesThread().
  std::unique_ptr<Thread> completed_queries_thread_;

  /// Ticker that wakes up the completed_queried_thread at set intervals to process the
  /// queued completed queries. Uses the completed_queries_lock_ to synchonize access to
  /// the completed_queries_ list.
  std::unique_ptr<TickerSecondsBool> completed_queries_ticker_;

  /// Queue of completed queries and the lock to synchronize access to it.
  std::list<impala::workload_management::CompletedQuery> completed_queries_;
  std::mutex completed_queries_lock_;

};
}
