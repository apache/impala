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

#ifndef IMPALA_SERVICE_IMPALA_SERVER_H
#define IMPALA_SERVICE_IMPALA_SERVER_H

#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaHiveServer2Service.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Frontend_types.h"
#include "rpc/thrift-server.h"
#include "common/status.h"
#include "service/frontend.h"
#include "util/metrics.h"
#include "util/runtime-profile.h"
#include "util/simple-logger.h"
#include "util/thread-pool.h"
#include "util/time.h"
#include "util/uid-util.h"
#include "runtime/coordinator.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"
#include "rapidjson/rapidjson.h"

namespace impala {

class ExecEnv;
class DataSink;
class CancellationWork;
class Coordinator;
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

/// An ImpalaServer contains both frontend and backend functionality;
/// it implements ImpalaService (Beeswax), ImpalaHiveServer2Service (HiveServer2)
/// and ImpalaInternalService APIs.
/// This class is partially thread-safe. To ensure freedom from deadlock,
/// locks on the maps are obtained before locks on the items contained in the maps.
//
/// TODO: The state of a running query is currently not cleaned up if the
/// query doesn't experience any errors at runtime and close() doesn't get called.
/// The solution is to have a separate thread that cleans up orphaned
/// query execution states after a timeout period.
/// TODO: The same doesn't apply to the execution state of an individual plan
/// fragment: the originating coordinator might die, but we can get notified of
/// that via the statestore. This still needs to be implemented.
class ImpalaServer : public ImpalaServiceIf, public ImpalaHiveServer2ServiceIf,
                     public ThriftServer::ConnectionHandlerIf {
 public:
  ImpalaServer(ExecEnv* exec_env);
  ~ImpalaServer();

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

  /// ImpalaService rpcs: extensions over Beeswax (implemented in impala-beeswax-server.cc)
  virtual void Cancel(impala::TStatus& status, const beeswax::QueryHandle& query_id);
  virtual void CloseInsert(impala::TInsertResult& insert_result,
      const beeswax::QueryHandle& query_handle);

  /// Pings the Impala service and gets the server version string.
  virtual void PingImpalaService(TPingImpalaServiceResp& return_val);

  virtual void GetRuntimeProfile(std::string& profile_output,
      const beeswax::QueryHandle& query_id);

  virtual void GetExecSummary(impala::TExecSummary& result,
      const beeswax::QueryHandle& query_id);

  /// Performs a full catalog metadata reset, invalidating all table and database metadata.
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

  /// ImpalaService common extensions (implemented in impala-server.cc)
  /// ImpalaInternalService rpcs
  void ReportExecStatus(TReportExecStatusResult& return_val,
      const TReportExecStatusParams& params);
  void TransmitData(TTransmitDataResult& return_val,
      const TTransmitDataParams& params);

  /// Generates a unique id for this query and sets it in the given query context.
  /// Prepares the given query context by populating fields required for evaluating
  /// certain expressions, such as now(), pid(), etc. Should be called before handing
  /// the query context to the frontend for query compilation.
  static void PrepareQueryContext(TQueryCtx* query_ctx);

  /// SessionHandlerIf methods

  /// Called when a Beeswax or HS2 connection starts. For Beeswax, registers a new
  /// SessionState associated with the new connection. For HS2, this is a no-op (HS2 has an
  /// explicit CreateSession RPC).
  virtual void ConnectionStart(const ThriftServer::ConnectionContext& session_context);

  /// Called when a Beeswax or HS2 connection terminates. Unregisters all sessions
  /// associated with the closed connection.
  virtual void ConnectionEnd(const ThriftServer::ConnectionContext& session_context);

  /// Called when a membership update is received from the statestore. Looks for
  /// active nodes that have failed, and cancels any queries running on them.
  ///  - incoming_topic_deltas: all changes to registered statestore topics
  ///  - subscriber_topic_updates: output parameter to publish any topic updates to.
  ///                              Currently unused.
  void MembershipCallback(const StatestoreSubscriber::TopicDeltaMap&
      incoming_topic_deltas, std::vector<TTopicDelta>* subscriber_topic_updates);

  void CatalogUpdateCallback(const StatestoreSubscriber::TopicDeltaMap& topic_deltas,
      std::vector<TTopicDelta>* topic_updates);

  /// Returns true if Impala is offline (and not accepting queries), false otherwise.
  bool IsOffline() {
    boost::lock_guard<boost::mutex> l(is_offline_lock_);
    return is_offline_;
  }

  /// Returns true if lineage logging is enabled, false otherwise.
  bool IsLineageLoggingEnabled();

 private:
  friend class ChildQuery;

  /// Query result set stores converted rows returned by QueryExecState.fetchRows(). It
  /// provides an interface to convert Impala rows to external API rows.
  /// It is an abstract class. Subclass must implement AddOneRow().
  class QueryResultSet {
   public:
    QueryResultSet() {}
    virtual ~QueryResultSet() {}

    /// Add the row (list of expr value) from a select query to this result set. When a row
    /// comes from a select query, the row is in the form of expr values (void*). 'scales'
    /// contains the values' scales (# of digits after decimal), with -1 indicating no
    /// scale specified.
    virtual Status AddOneRow(
        const std::vector<void*>& row, const std::vector<int>& scales) = 0;

    /// Add the TResultRow to this result set. When a row comes from a DDL/metadata
    /// operation, the row in the form of TResultRow.
    virtual Status AddOneRow(const TResultRow& row) = 0;

    /// Copies rows in the range [start_idx, start_idx + num_rows) from the other result
    /// set into this result set. Returns the number of rows added to this result set.
    /// Returns 0 if the given range is out of bounds of the other result set.
    virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows) = 0;

    /// Returns the approximate size of this result set in bytes.
    int64_t ByteSize() { return ByteSize(0, size()); }

    /// Returns the approximate size of the given range of rows in bytes.
    virtual int64_t ByteSize(int start_idx, int num_rows) = 0;

    /// Returns the size of this result set in number of rows.
    virtual size_t size() = 0;
  };

  /// Result set implementations for Beeswax and HS2
  class AsciiQueryResultSet;
  class HS2RowOrientedResultSet;
  class HS2ColumnarResultSet;

  struct SessionState;

  /// Execution state of a query.
  class QueryExecState;

  /// Relevant ODBC SQL State code; for more info,
  /// goto http://msdn.microsoft.com/en-us/library/ms714687.aspx
  static const char* SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION;
  static const char* SQLSTATE_GENERAL_ERROR;
  static const char* SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED;

  /// Ascii output precision for double/float
  static const int ASCII_PRECISION;

  QueryResultSet* CreateHS2ResultSet(
      apache::hive::service::cli::thrift::TProtocolVersion::type version,
      const TResultSetMetadata& metadata,
      apache::hive::service::cli::thrift::TRowSet* rowset = NULL);

  /// Return exec state for given query_id, or NULL if not found.
  /// If 'lock' is true, the returned exec state's lock() will be acquired before
  /// the query_exec_state_map_lock_ is released.
  boost::shared_ptr<QueryExecState> GetQueryExecState(
      const TUniqueId& query_id, bool lock);

  /// Writes the session id, if found, for the given query to the output parameter. Returns
  /// false if no query with the given ID is found.
  bool GetSessionIdForQuery(const TUniqueId& query_id, TUniqueId* session_id);

  /// Updates the number of databases / tables metrics from the FE catalog
  Status UpdateCatalogMetrics();

  /// Starts asynchronous execution of query. Creates QueryExecState (returned
  /// in exec_state), registers it and calls Coordinator::Execute().
  /// If it returns with an error status, exec_state will be NULL and nothing
  /// will have been registered in query_exec_state_map_.
  /// session_state is a ptr to the session running this query and must have
  /// been checked out.
  /// query_session_state is a snapshot of session state that changes when the
  /// query was run. (e.g. default database).
  Status Execute(TQueryCtx* query_ctx,
                 boost::shared_ptr<SessionState> session_state,
                 boost::shared_ptr<QueryExecState>* exec_state);

  /// Implements Execute() logic, but doesn't unregister query on error.
  Status ExecuteInternal(const TQueryCtx& query_ctx,
                         boost::shared_ptr<SessionState> session_state,
                         bool* registered_exec_state,
                         boost::shared_ptr<QueryExecState>* exec_state);

  /// Registers the query exec state with query_exec_state_map_ using the globally
  /// unique query_id and add the query id to session state's open query list.
  /// The caller must have checked out the session state.
  Status RegisterQuery(boost::shared_ptr<SessionState> session_state,
      const boost::shared_ptr<QueryExecState>& exec_state);

  /// Adds the query to the set of in-flight queries for the session. The query remains
  /// in-flight until the query is unregistered.  Until a query is in-flight, an attempt
  /// to cancel or close the query by the user will return an error status.  If the
  /// session is closed before a query is in-flight, then the query cancellation is
  /// deferred until after the issuing path has completed initializing the query.  Once
  /// a query is in-flight, it can be cancelled/closed asynchronously by the user
  /// (e.g. via an RPC) and the session close path can close (cancel and unregister) it.
  /// The query must have already been registered using RegisterQuery().  The caller
  /// must have checked out the session state.
  Status SetQueryInflight(boost::shared_ptr<SessionState> session_state,
      const boost::shared_ptr<QueryExecState>& exec_state);

  /// Unregister the query by cancelling it, removing exec_state from
  /// query_exec_state_map_, and removing the query id from session state's in-flight
  /// query list.  If check_inflight is true, then return an error if the query is not
  /// yet in-flight.  Otherwise, proceed even if the query isn't yet in-flight (for
  /// cleaning up after an error on the query issuing path).
  Status UnregisterQuery(const TUniqueId& query_id, bool check_inflight,
      const Status* cause = NULL);

  /// Initiates query cancellation reporting the given cause as the query status.
  /// Assumes deliberate cancellation by the user if the cause is NULL.  Returns an
  /// error if query_id is not found.  If check_inflight is true, then return an error
  /// if the query is not yet in-flight.  Otherwise, returns OK.  Queries still need to
  /// be unregistered, after cancellation.  Caller should not hold any locks when
  /// calling this function.
  Status CancelInternal(const TUniqueId& query_id, bool check_inflight,
      const Status* cause = NULL);

  /// Close the session and release all resource used by this session.
  /// Caller should not hold any locks when calling this function.
  /// If ignore_if_absent is true, returns OK even if a session with the supplied ID does
  /// not exist.
  Status CloseSessionInternal(const TUniqueId& session_id, bool ignore_if_absent);

  /// Gets the runtime profile string for a given query_id and writes it to the output
  /// stream. First searches for the query id in the map of in-flight queries. If no
  /// match is found there, the query log is searched. Returns OK if the profile was
  /// found, otherwise a Status object with an error message will be returned. The
  /// output stream will not be modified on error.
  /// If base64_encoded, outputs the base64 encoded profile output, otherwise the human
  /// readable string.
  Status GetRuntimeProfileStr(const TUniqueId& query_id, bool base64_encoded,
      std::stringstream* output);

  /// Returns the exec summary for this query.
  Status GetExecSummary(const TUniqueId& query_id, TExecSummary* result);

  /// Json callback for /hadoop-varz. Produces Json with a list, 'configs', of (key, value)
  /// pairs, one for each Hadoop configuration value.
  void HadoopVarzUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Webserver callback. Returns two sorted lists of queries, one in-flight and one
  /// completed, as well as a list of active backends and their plan-fragment count.
  //
  /// "in_flight_queries": [],
  /// "num_in_flight_queries": 0,
  /// "completed_queries": [
  ///       {
  ///         "effective_user": "henry",
  ///         "default_db": "default",
  ///         "stmt": "select sleep(10000)",
  ///         "stmt_type": "QUERY",
  ///         "start_time": "2014-08-07 18:37:47.923614000",
  ///         "end_time": "2014-08-07 18:37:58.146494000",
  ///         "progress": "0 / 0 (0%)",
  ///         "state": "FINISHED",
  ///         "rows_fetched": 1,
  ///         "query_id": "7c459a59fb8cefe3:8b7042d55bf19887"
  ///       }
  /// ],
  /// "completed_log_size": 25,
  /// "query_locations": [
  ///     {
  ///       "location": "henry-impala:22000",
  ///        "count": 0
  ///     }
  /// ]
  void QueryStateUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Json callback for /query_profile. Expects query_id as an argument, produces Json with
  /// 'profile' set to the profile string, and 'query_id' set to the query ID.
  void QueryProfileUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Webserver callback. Produces a Json structure with query summary information.
  /// Example:
  /// { "summary": <....>,
  ///   "plan": <....>,
  ///   "stmt": "select count(*) from functional.alltypes"
  ///   "id": <...>,
  ///   "state": "FINISHED"
  /// }
  /// If include_plan_json is true, 'plan_json' will be set to a JSON representation of the
  /// query plan. If include_summary is true, 'summary' will be a text rendering of the
  /// query summary.
  void QuerySummaryCallback(bool include_plan_json, bool include_summary,
      const Webserver::ArgumentMap& args, rapidjson::Document* document);

  /// Webserver callback. Cancels an in-flight query and writes the result to 'contents'.
  void CancelQueryUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Webserver callback.  Upon return, 'document' will contain the query profile as a
  /// base64 encoded object in 'contents'.
  void QueryProfileEncodedUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Webserver callback. Produces a list of inflight query IDs printed as text in
  /// 'contents'.
  void InflightQueryIdsUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Json callback for /sessions, which prints a table of active client sessions.
  /// "sessions": [
  /// {
  ///     "type": "BEESWAX",
  ///     "num_queries": 0,
  ///     "user": "",
  ///     "delegated_user": "",
  ///     "session_id": "6242f69b02e4d609:ac84df1fbb0e16a3",
  ///     "network_address": "127.0.0.1:46484",
  ///     "default_database": "default",
  ///     "start_time": "2014-08-07 22:50:49",
  ///     "last_accessed": "2014-08-07 22:50:49",
  ///     "expired": false,
  ///     "closed": false,
  ///     "ref_count": 0
  ///     }
  /// ],
  /// "num_sessions": 1
  void SessionsUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Webserver callback that prints a list of all known databases and tables
  void CatalogUrlCallback(const Webserver::ArgumentMap& args, rapidjson::Document* output);

  /// Webserver callback that allows for dumping information on objects in the catalog.
  void CatalogObjectsUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* output);

  /// Initialize "default_configs_" to show the default values for ImpalaQueryOptions and
  /// "support_start_over/false" to indicate that Impala does not support start over
  /// in the fetch call.
  void InitializeConfigVariables();

  /// Registers all the per-Impalad webserver callbacks
  void RegisterWebserverCallbacks(Webserver* webserver);

  /// Checks settings for profile logging, including whether the output
  /// directory exists and is writeable, and initialises the first log file.
  /// Returns OK unless there is some problem preventing profile log files
  /// from being written. If an error is returned, the constructor will disable
  /// profile logging.
  Status InitProfileLogging();

  /// Checks settings for audit event logging, including whether the output
  /// directory exists and is writeable, and initialises the first log file.
  /// Returns OK unless there is some problem preventing audit event log files
  /// from being written. If an error is returned, impalad startup will be aborted.
  Status InitAuditEventLogging();

  /// Checks settings for lineage logging, including whether the output
  /// directory exists and is writeable, and initialises the first log file.
  /// Returns OK unless there is some problem preventing lineage log files
  /// from being written. If an error is returned, impalad startup will be aborted.
  Status InitLineageLogging();

  /// Initializes a logging directory, creating the directory if it does not already
  /// exist. If there is any error creating the directory an error will be returned.
  static Status InitLoggingDir(const std::string& log_dir);

  /// Returns true if audit event logging is enabled, false otherwise.
  bool IsAuditEventLoggingEnabled();

  Status LogAuditRecord(const QueryExecState& exec_state, const TExecRequest& request);

  Status LogLineageRecord(const QueryExecState& exec_state);

  /// Log audit and column lineage events
  void LogQueryEvents(const QueryExecState& exec_state);

  /// Runs once every 5s to flush the profile log file to disk.
  void LogFileFlushThread();

  /// Runs once every 5s to flush the audit log file to disk.
  void AuditEventLoggerFlushThread();

  /// Runs once every 5s to flush the lineage log file to disk.
  void LineageLoggerFlushThread();

  /// Copies a query's state into the query log. Called immediately prior to a
  /// QueryExecState's deletion. Also writes the query profile to the profile log on disk.
  /// Must be called with query_exec_state_map_lock_ held
  void ArchiveQuery(const QueryExecState& query);

  /// Checks whether the given user is allowed to delegate as the specified do_as_user.
  /// Returns OK if the authorization suceeds, otherwise returns an status with details
  /// on why the failure occurred.
  Status AuthorizeProxyUser(const std::string& user, const std::string& do_as_user);

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

    /// Start and end time of the query
    TimestampValue start_time, end_time;

    /// Summary of execution for this query.
    TExecSummary exec_summary;

    Status query_status;

    /// Timeline of important query events
    TEventSequence event_sequence;

    /// Save the query plan fragments so that the plan tree can be rendered on the debug
    /// webpages.
    vector<TPlanFragment> fragments;

    /// Initialise from an exec_state. If copy_profile is true, print the query
    /// profile to a string and copy that into this.profile (which is expensive),
    /// otherwise leave this.profile empty.
    /// If encoded_str is non-empty, it is the base64 encoded string for
    /// exec_state->profile.
    QueryStateRecord(const QueryExecState& exec_state, bool copy_profile = false,
        const std::string& encoded_str = "");

    /// Default constructor used only when participating in collections
    QueryStateRecord() { }

    /// Comparator that sorts by start time.
    bool operator() (const QueryStateRecord& lhs, const QueryStateRecord& rhs) const;
  };

  /// Helper method to render a single QueryStateRecord as a Json object
  /// Used by QueryStateUrlCallback().
  void QueryStateToJson(const ImpalaServer::QueryStateRecord& record,
      rapidjson::Value* value, rapidjson::Document* document);

  /// Beeswax private methods

  /// Helper functions to translate between Beeswax and Impala structs
  Status QueryToTQueryContext(const beeswax::Query& query, TQueryCtx* query_ctx);
  void TUniqueIdToQueryHandle(const TUniqueId& query_id, beeswax::QueryHandle* handle);
  void QueryHandleToTUniqueId(const beeswax::QueryHandle& handle, TUniqueId* query_id);

  /// Helper function to raise BeeswaxException
  void RaiseBeeswaxException(const std::string& msg, const char* sql_state);

  /// Executes the fetch logic. Doesn't clean up the exec state if an error occurs.
  Status FetchInternal(const TUniqueId& query_id, bool start_over,
      int32_t fetch_size, beeswax::Results* query_results);

  /// Populate insert_result and clean up exec state. If the query
  /// status is an error, insert_result is not populated and the status is returned.
  Status CloseInsertInternal(const TUniqueId& query_id, TInsertResult* insert_result);

  /// HiveServer2 private methods (implemented in impala-hs2-server.cc)

  /// Starts the synchronous execution of a HiverServer2 metadata operation.
  /// If the execution succeeds, an QueryExecState will be created and registered in
  /// query_exec_state_map_. Otherwise, nothing will be registered in query_exec_state_map_
  /// and an error status will be returned. As part of this call, the TMetadataOpRequest
  /// struct will be populated with the requesting user's session state.
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
      apache::hive::service::cli::thrift::TFetchResultsResp* fetch_results);

  /// Helper functions to translate between HiveServer2 and Impala structs

  /// Returns !ok() if handle.guid.size() or handle.secret.size() != 16
  static Status THandleIdentifierToTUniqueId(
      const apache::hive::service::cli::thrift::THandleIdentifier& handle,
      TUniqueId* unique_id, TUniqueId* secret);
  static void TUniqueIdToTHandleIdentifier(
      const TUniqueId& unique_id, const TUniqueId& secret,
      apache::hive::service::cli::thrift::THandleIdentifier* handle);
  Status TExecuteStatementReqToTQueryContext(
      const apache::hive::service::cli::thrift::TExecuteStatementReq execute_request,
      TQueryCtx* query_ctx);

  /// Helper method to process cancellations that result from failed backends, called from
  /// the cancellation thread pool. The cancellation_work contains the query id to cancel
  /// and a cause listing the failed backends that led to cancellation. Calls
  /// CancelInternal directly, but has a signature compatible with the thread pool.
  void CancelFromThreadPool(uint32_t thread_id,
      const CancellationWork& cancellation_work);

  /// Processes a CatalogUpdateResult returned from the CatalogServer and ensures
  /// the update has been applied to the local impalad's catalog cache. If
  /// wait_for_all_subscribers is true, this function will also wait until all
  /// catalog topic subscribers have processed the update. Called from QueryExecState
  /// after executing any statement that modifies the catalog.
  /// If wait_for_all_subscribers is false AND if the TCatalogUpdateResult contains
  /// TCatalogObject(s) to add and/or remove, this function will update the local cache
  /// by directly calling UpdateCatalog() with the TCatalogObject results.
  /// Otherwise this function will wait until the local impalad's catalog cache has been
  /// updated from a statestore heartbeat that includes this catalog update's catalog
  /// version. If wait_for_all_subscribers is true, this function also wait all other
  /// catalog topic subscribers to process this update by checking the current
  /// min_subscriber_topic_version included in each state store heartbeat.
  Status ProcessCatalogUpdateResult(const TCatalogUpdateResult& catalog_update_result,
      bool wait_for_all_subscribers);

  /// Register timeout value upon opening a new session. This will wake up
  /// session_timeout_thread_ to update its poll period.
  void RegisterSessionTimeout(int32_t timeout);

  /// To be run in a thread which wakes up every x / 2 seconds in which x is the minimum
  /// non-zero idle session timeout value of all sessions. This function checks all
  /// sessions for their last-idle times. Those that have been idle for longer than
  /// their configured timeout values are 'expired': they will no longer accept queries
  /// and any running queries associated with those sessions are unregistered.
  void ExpireSessions();

  /// Runs forever, walking queries_by_timestamp_ and expiring any queries that have been
  /// idle (i.e. no client input and no time spent processing locally) for
  /// FLAGS_idle_query_timeout seconds.
  void ExpireQueries();

  /// Periodically opens a socket to FLAGS_local_nodemanager_url to check if the Yarn Node
  /// Manager is running. If not, this method calls SetOffline(true), and when the NM
  /// recovers, calls SetOffline(false). Only called (in nm_failure_detection_thread_) if
  /// FLAGS_enable_rm is true.
  void DetectNmFailures();

  /// Set is_offline_ to the argument's value.
  void SetOffline(bool offline);

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
  boost::scoped_ptr<Thread> profile_log_file_flush_thread_;

  /// If audit event logging is enabled, wakes once every 5s to flush audit events to disk
  boost::scoped_ptr<Thread> audit_event_logger_flush_thread_;

  /// If lineage logging is enabled, wakes once every 5s to flush lineage events to disk
  boost::scoped_ptr<Thread> lineage_logger_flush_thread_;

  /// global, per-server state
  ExecEnv* exec_env_;  // not owned

  /// Thread pool to process cancellation requests that come from failed Impala demons to
  /// avoid blocking the statestore callback.
  boost::scoped_ptr<ThreadPool<CancellationWork> > cancellation_thread_pool_;

  /// Thread that runs ExpireSessions. It will wake up periodically to check for sessions
  /// which are idle for more their timeout values.
  boost::scoped_ptr<Thread> session_timeout_thread_;

  /// Contains all the non-zero idle session timeout values.
  std::multiset<int32_t> session_timeout_set_;

  /// The lock for protecting the session_timeout_set_.
  boost::mutex session_timeout_lock_;

  /// session_timeout_thread_ relies on the following conditional variable to wake up
  /// on every poll period expiration or when the poll period changes.
  boost::condition_variable session_timeout_cv_;

  /// map from query id to exec state; QueryExecState is owned by us and referenced
  /// as a shared_ptr to allow asynchronous deletion
  typedef boost::unordered_map<TUniqueId, boost::shared_ptr<QueryExecState> >
      QueryExecStateMap;
  QueryExecStateMap query_exec_state_map_;
  boost::mutex query_exec_state_map_lock_;  // protects query_exec_state_map_

  /// Default query options in the form of TQueryOptions and beeswax::ConfigVariable
  TQueryOptions default_query_options_;
  std::vector<beeswax::ConfigVariable> default_configs_;

  /// Per-session state.  This object is reference counted using shared_ptrs.  There
  /// is one ref count in the SessionStateMap for as long as the session is active.
  /// All queries running from this session also have a reference.
  struct SessionState {
    /// The default hs2_version must be V1 so that child queries (which use HS2, but may
    /// run as children of Beeswax sessions) get results back in the expected format -
    /// child queries inherit the HS2 version from their parents, and a Beeswax session
    /// will never update the HS2 version from the default.
    SessionState() : closed(false), expired(false), hs2_version(
        apache::hive::service::cli::thrift::
        TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V1), ref_count(0) { }

    TSessionType::type session_type;

    /// Time the session was created
    TimestampValue start_time;

    /// Connected user for this session, i.e. the user which originated this session.
    std::string connected_user;

    /// The user to delegate to. Empty for no delegation.
    std::string do_as_user;

    /// Client network address
    TNetworkAddress network_address;

    /// Protects all fields below
    /// If this lock has to be taken with query_exec_state_map_lock, take this lock first.
    boost::mutex lock;

    /// If true, the session has been closed.
    bool closed;

    /// If true, the session was idle for too long and has been expired. Only set when
    /// ref_count == 0, after which point ref_count should never become non-zero (since
    /// clients will use ScopedSessionState to access the session, which will prevent use
    /// after expiration).
    bool expired;

    /// The default database (changed as a result of 'use' query execution)
    std::string database;

    /// The default query options of this session
    TQueryOptions default_query_options;

    /// For HS2 only, the protocol version this session is expecting
    apache::hive::service::cli::thrift::TProtocolVersion::type hs2_version;

    /// Inflight queries belonging to this session
    boost::unordered_set<TUniqueId> inflight_queries;

    /// Time the session was last accessed.
    int64_t last_accessed_ms;

    /// Number of RPCs concurrently accessing this session state. Used to detect when a
    /// session may be correctly expired after a timeout (when ref_count == 0). Typically
    /// at most one RPC will be issued against a session at a time, but clients may do
    /// something unexpected and, for example, poll in one thread and fetch in another.
    uint32_t ref_count;

    /// Per-session idle timeout in seconds. Default value is FLAGS_idle_session_timeout.
    /// It can be overridden with a smaller value via the option "idle_session_timeout"
    /// when opening a HS2 session.
    int32_t session_timeout;

    /// Builds a Thrift representation of this SessionState for serialisation to
    /// the frontend.
    void ToThrift(const TUniqueId& session_id, TSessionState* session_state);
  };

  /// Class that allows users of SessionState to mark a session as in-use, and therefore
  /// immune to expiration. The marking is done in WithSession() and undone in the
  /// destructor, so this class can be used to 'check-out' a session for the duration of a
  /// scope.
  class ScopedSessionState {
   public:
    ScopedSessionState(ImpalaServer* impala) : impala_(impala) { }

    /// Marks a session as in-use, and saves it so that it can be unmarked when this object
    /// goes out of scope. Returns OK unless there is an error in GetSessionState.
    /// Must only be called once per ScopedSessionState.
    Status WithSession(const TUniqueId& session_id,
        boost::shared_ptr<SessionState>* session = NULL) {
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
    boost::shared_ptr<SessionState> session_;

    /// Saved so that we can access ImpalaServer methods to get / return session state.
    ImpalaServer* impala_;
  };

  /// For access to GetSessionState() / MarkSessionInactive()
  friend class ScopedSessionState;

  /// Protects session_state_map_. Should be taken before any query exec-state locks,
  /// including query_exec_state_map_lock_. Should be taken before individual session-state
  /// locks.
  boost::mutex session_state_map_lock_;

  /// A map from session identifier to a structure containing per-session information
  typedef boost::unordered_map<TUniqueId, boost::shared_ptr<SessionState> >
    SessionStateMap;
  SessionStateMap session_state_map_;

  /// Protects connection_to_sessions_map_. May be taken before session_state_map_lock_.
  boost::mutex connection_to_sessions_map_lock_;

  /// Map from a connection ID to the associated list of sessions so that all can be closed
  /// when the connection ends. HS2 allows for multiplexing several sessions across a
  /// single connection. If a session has already been closed (only possible via HS2) it is
  /// not removed from this map to avoid the cost of looking it up.
  typedef boost::unordered_map<TUniqueId, std::vector<TUniqueId> >
    ConnectionToSessionMap;
  ConnectionToSessionMap connection_to_sessions_map_;

  /// Returns session state for given session_id.
  /// If not found, session_state will be NULL and an error status will be returned.
  /// If mark_active is true, also checks if the session is expired or closed and
  /// increments the session's reference counter if it is still alive.
  Status GetSessionState(const TUniqueId& session_id,
      boost::shared_ptr<SessionState>* session_state, bool mark_active = false);

  /// Decrement the session's reference counter and mark last_accessed_ms so that state
  /// expiration can proceed.
  inline void MarkSessionInactive(boost::shared_ptr<SessionState> session) {
    boost::lock_guard<boost::mutex> l(session->lock);
    DCHECK_GT(session->ref_count, 0);
    --session->ref_count;
    session->last_accessed_ms = UnixMillis();
  }

  /// protects query_locations_. Must always be taken after
  /// query_exec_state_map_lock_ if both are required.
  boost::mutex query_locations_lock_;

  /// A map from backend to the list of queries currently running there.
  typedef boost::unordered_map<TNetworkAddress, boost::unordered_set<TUniqueId> >
      QueryLocations;
  QueryLocations query_locations_;

  /// A map from unique backend ID to the corresponding TBackendDescriptor of that backend.
  /// Used to track membership updates from the statestore so queries can be cancelled
  /// when a backend is removed. It's not enough to just cancel fragments that are running
  /// based on the deletions mentioned in the most recent statestore heartbeat; sometimes
  /// cancellations are skipped and the statestore, at its discretion, may send only
  /// a delta of the current membership so we need to compute any deletions.
  /// TODO: Currently there are multiple locations where cluster membership is tracked,
  /// here and in the scheduler. This should be consolidated so there is a single component
  /// (the scheduler?) that tracks this information and calls other interested components.
  typedef boost::unordered_map<std::string, TBackendDescriptor> BackendDescriptorMap;
  BackendDescriptorMap known_backends_;

  /// Generate unique session id for HiveServer2 session
  boost::uuids::random_generator uuid_generator_;

  /// Lock to protect uuid_generator
  boost::mutex uuid_lock_;

  /// Lock for catalog_update_version_info_, min_subscriber_catalog_topic_version_,
  /// and catalog_version_update_cv_
  boost::mutex catalog_version_lock_;

  /// Variable to signal when the catalog version has been modified
  boost::condition_variable catalog_version_update_cv_;

  /// Contains details on the version information of a catalog update.
  struct CatalogUpdateVersionInfo {
    CatalogUpdateVersionInfo() :
      catalog_version(0L),
      catalog_topic_version(0L) {
    }

    /// The last catalog version returned from UpdateCatalog()
    int64_t catalog_version;
    /// The CatalogService ID that this catalog version is from.
    TUniqueId catalog_service_id;
    /// The statestore catalog topic version this update was received in.
    int64_t catalog_topic_version;
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
  typedef boost::unordered_map<std::string, boost::unordered_set<std::string> >
      ProxyUserMap;
  ProxyUserMap authorized_proxy_user_config_;

  /// Guards queries_by_timestamp_.  Must not be acquired before a session state lock.
  boost::mutex query_expiration_lock_;

  /// Describes a query expiration event (t, q) where t is the expiration deadline in
  /// seconds, and q is the query ID.
  typedef std::pair<int64_t, TUniqueId> ExpirationEvent;

  /// Comparator that breaks ties when two queries have identical expiration deadlines.
  struct ExpirationEventComparator {
    bool operator()(const ExpirationEvent& t1, const ExpirationEvent& t2) {
      if (t1.first < t2.first) return true;
      if (t2.first < t1.first) return false;
      return t1.second < t2.second;
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
  boost::scoped_ptr<Thread> query_expiration_thread_;

  /// Container thread for DetectNmFailures().
  boost::scoped_ptr<Thread> nm_failure_detection_thread_;

  /// Protects is_offline_
  boost::mutex is_offline_lock_;

  /// True if Impala server is offline, false otherwise.
  bool is_offline_;
};

/// Create an ImpalaServer and Thrift servers.
/// If beeswax_port != 0 (and fe_server != NULL), creates a ThriftServer exporting
/// ImpalaService (Beeswax) on beeswax_port (returned via beeswax_server).
/// If hs2_port != 0 (and hs2_server != NULL), creates a ThriftServer exporting
/// ImpalaHiveServer2Service on hs2_port (returned via hs2_server).
/// If be_port != 0 (and be_server != NULL), create a ThriftServer exporting
/// ImpalaInternalService on be_port (returned via be_server).
/// Returns created ImpalaServer. The caller owns fe_server and be_server.
/// The returned ImpalaServer is referenced by both of these via shared_ptrs and will be
/// deleted automatically.
/// Returns OK unless there was some error creating the servers, in
/// which case none of the output parameters can be assumed to be valid.
Status CreateImpalaServer(ExecEnv* exec_env, int beeswax_port, int hs2_port,
    int be_port, ThriftServer** beeswax_server, ThriftServer** hs2_server,
    ThriftServer** be_server, ImpalaServer** impala_server);

}

#endif
