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
#include "exec/ddl-executor.h"
#include "util/metrics.h"
#include "util/runtime-profile.h"
#include "util/simple-logger.h"
#include "util/thread-pool.h"
#include "util/uid-util.h"
#include "runtime/coordinator.h"
#include "runtime/primitive-type.h"
#include "runtime/timestamp-value.h"
#include "runtime/runtime-state.h"

namespace impala {

class ExecEnv;
class DataSink;
class CancellationWork;
class Coordinator;
class RowDescriptor;
class TCatalogUpdate;
class TPlanExecRequest;
class TPlanExecParams;
class TExecPlanFragmentParams;
class TExecPlanFragmentResult;
class TInsertResult;
class TReportExecStatusArgs;
class TReportExecStatusResult;
class TCancelPlanFragmentArgs;
class TCancelPlanFragmentResult;
class TTransmitDataArgs;
class TTransmitDataResult;
class TNetworkAddress;
class TClientRequest;
class TExecRequest;
class TSessionState;
class TQueryOptions;

class ThriftServer;

// An ImpalaServer contains both frontend and backend functionality;
// it implements ImpalaService (Beeswax), ImpalaHiveServer2Service (HiveServer2)
// and ImpalaInternalService APIs.
// This class is partially thread-safe. To ensure freedom from deadlock,
// locks on the maps are obtained before locks on the items contained in the maps.
//
// TODO: The state of a running query is currently not cleaned up if the
// query doesn't experience any errors at runtime and close() doesn't get called.
// The solution is to have a separate thread that cleans up orphaned
// query execution states after a timeout period.
// TODO: The same doesn't apply to the execution state of an individual plan
// fragment: the originating coordinator might die, but we can get notified of
// that via the statestore. This still needs to be implemented.
class ImpalaServer : public ImpalaServiceIf, public ImpalaHiveServer2ServiceIf,
                     public ImpalaInternalServiceIf,
                     public ThriftServer::ConnectionHandlerIf {
 public:
  ImpalaServer(ExecEnv* exec_env);
  ~ImpalaServer();

  Frontend* frontend() { return frontend_.get(); }

  // ImpalaService rpcs: Beeswax API (implemented in impala-beeswax-server.cc)
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

  // Return ImpalaQueryOptions default values and "support_start_over/false" to indicate
  // that Impala does not support start over in the fetch call. Hue relies on this not to
  // issue a "start_over" fetch call.
  // "include_hadoop" is not applicable.
  virtual void get_default_configuration(
      std::vector<beeswax::ConfigVariable>& configurations, const bool include_hadoop);

  // ImpalaService rpcs: unimplemented parts of Beeswax API.
  // These APIs will not be implemented because ODBC driver does not use them.
  virtual void dump_config(std::string& config);

  // ImpalaService rpcs: extensions over Beeswax (implemented in impala-beeswax-server.cc)
  virtual void Cancel(impala::TStatus& status, const beeswax::QueryHandle& query_id);
  virtual void CloseInsert(impala::TInsertResult& insert_result,
      const beeswax::QueryHandle& query_handle);

  // Pings the Impala service and gets the server version string.
  virtual void PingImpalaService(TPingImpalaServiceResp& return_val);

  // TODO: Need to implement HiveServer2 version of GetRuntimeProfile
  virtual void GetRuntimeProfile(std::string& profile_output,
      const beeswax::QueryHandle& query_id);

  // Performs a full catalog metadata reset, invalidating all table and database metadata.
  virtual void ResetCatalog(impala::TStatus& status);

  // Resets the specified table's catalog metadata, forcing a reload on the next access.
  // Returns an error if the table or database was not found in the catalog.
  virtual void ResetTable(impala::TStatus& status, const TResetTableReq& request);

  // ImpalaHiveServer2Service rpcs: HiveServer2 API (implemented in impala-hs2-server.cc)
  // TODO: Migrate existing extra ImpalaServer RPCs to ImpalaHiveServer2Service.
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

  // ImpalaService common extensions (implemented in impala-server.cc)
  // ImpalaInternalService rpcs
  virtual void ExecPlanFragment(
      TExecPlanFragmentResult& return_val, const TExecPlanFragmentParams& params);
  virtual void ReportExecStatus(
      TReportExecStatusResult& return_val, const TReportExecStatusParams& params);
  virtual void CancelPlanFragment(
      TCancelPlanFragmentResult& return_val, const TCancelPlanFragmentParams& params);
  virtual void TransmitData(
      TTransmitDataResult& return_val, const TTransmitDataParams& params);

  // Returns the ImpalaQueryOptions enum for the given "key". Input is case in-sensitive.
  // Return -1 if the input is an invalid option.
  static int GetQueryOption(const std::string& key);

  // Parse a "," separated key=value pair of query options and set it in TQueryOptions.
  // If the same query option is specified more than once, the last one wins.
  // Return an error if the input is invalid (bad format or invalid query option).
  static Status ParseQueryOptions(const std::string& options,
      TQueryOptions* query_options);

  // Set the key/value pair in TQueryOptions. It will override existing setting in
  // query_options.
  static Status SetQueryOptions(const std::string& key, const std::string& value,
      TQueryOptions* query_options);

  // SessionHandlerIf methods

  // Called when a Beeswax or HS2 connection starts. For Beeswax, registers a new
  // SessionState associated with the new connection. For HS2, this is a no-op (HS2 has an
  // explicit CreateSession RPC).
  virtual void ConnectionStart(const ThriftServer::ConnectionContext& session_context);

  // Called when a Beeswax or HS2 connection terminates. Unregisters all sessions
  // associated with the closed connection.
  virtual void ConnectionEnd(const ThriftServer::ConnectionContext& session_context);

  // Called when a membership update is received from the state-store. Looks for
  // active nodes that have failed, and cancels any queries running on them.
  //  - incoming_topic_deltas: all changes to registered state-store topics
  //  - subscriber_topic_updates: output parameter to publish any topic updates to.
  //                              Currently unused.
  void MembershipCallback(const StateStoreSubscriber::TopicDeltaMap&
      incoming_topic_deltas, std::vector<TTopicDelta>* subscriber_topic_updates);

 private:
  class FragmentExecState;

  // Query result set stores converted rows returned by QueryExecState.fetchRows(). It
  // provides an interface to convert Impala rows to external API rows.
  // It is an abstract class. Subclass must implement AddOneRow().
  class QueryResultSet {
   public:
    QueryResultSet() {}
    virtual ~QueryResultSet() {}

    // Add the row (list of expr value) from a select query to this result set. When a row
    // comes from a select query, the row is in the form of expr values (void*). 'scales'
    // contains the values' scales (# of digits after decimal), with -1 indicating no
    // scale specified.
    virtual Status AddOneRow(
        const std::vector<void*>& row, const std::vector<int>& scales) = 0;

    // Add the TResultRow to this result set. When a row comes from a DDL/metadata
    // operation, the row in the form of TResultRow.
    virtual Status AddOneRow(const TResultRow& row) = 0;
  };

  class AsciiQueryResultSet; // extends QueryResultSet
  class TRowQueryResultSet; // extends QueryResultSet

  struct SessionState;

  // Execution state of a query.
  class QueryExecState;

  // Relevant ODBC SQL State code; for more info,
  // goto http://msdn.microsoft.com/en-us/library/ms714687.aspx
  static const char* SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION;
  static const char* SQLSTATE_GENERAL_ERROR;
  static const char* SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED;

  // Ascii output precision for double/float
  static const int ASCII_PRECISION;

  // Initiate execution of plan fragment in newly created thread.
  // Creates new FragmentExecState and registers it in fragment_exec_state_map_.
  Status StartPlanFragmentExecution(const TExecPlanFragmentParams& exec_params);

  // Top-level loop for synchronously executing plan fragment, which runs in
  // exec_state's thread. Repeatedly calls GetNext() on the executor
  // and feeds the result into the data sink.
  // Returns exec status.
  Status ExecPlanFragment(FragmentExecState* exec_state);

  // Call ExecPlanFragment() and report status to coord.
  void RunExecPlanFragment(FragmentExecState* exec_state);

  // Report status of fragment execution to initiating coord.
  Status ReportStatus(FragmentExecState* exec_state);

  Status CreateDataSink(
      const TPlanExecRequest& request, const TPlanExecParams& params,
      const RowDescriptor& row_desc, DataSink** sink);

  // Return exec state for given query_id, or NULL if not found.
  // If 'lock' is true, the returned exec state's lock() will be acquired before
  // the query_exec_state_map_lock_ is released.
  boost::shared_ptr<QueryExecState> GetQueryExecState(
      const TUniqueId& query_id, bool lock);

  // Return exec state for given fragment_instance_id, or NULL if not found.
  boost::shared_ptr<FragmentExecState> GetFragmentExecState(
      const TUniqueId& fragment_instance_id);

  // Updates the number of databases / tables metrics from the FE catalog
  Status UpdateCatalogMetrics();

  // Starts asynchronous execution of query. Creates QueryExecState (returned
  // in exec_state), registers it and calls Coordinator::Execute().
  // If it returns with an error status, exec_state will be NULL and nothing
  // will have been registered in query_exec_state_map_.
  // session_state is a ptr to the session running this query.
  // query_session_state is a snapshot of session state that changes when the
  // query was run. (e.g. default database).
  Status Execute(const TClientRequest& request,
                 boost::shared_ptr<SessionState> session_state,
                 const TSessionState& query_session_state,
                 boost::shared_ptr<QueryExecState>* exec_state);

  // Implements Execute() logic, but doesn't unregister query on error.
  Status ExecuteInternal(const TClientRequest& request,
                         boost::shared_ptr<SessionState> session_state,
                         const TSessionState& query_session_state,
                         bool* registered_exec_state,
                         boost::shared_ptr<QueryExecState>* exec_state);

  // Registers the query exec state with query_exec_state_map_ using the globally
  // unique query_id and add the query id to session state's open query list.
  Status RegisterQuery(boost::shared_ptr<SessionState> session_state,
      const boost::shared_ptr<QueryExecState>& exec_state);

  // Cancel the query execution if the query is still running. Removes exec_state from
  // query_exec_state_map_, and removes the query id from session state's open query list
  // Returns true if it found a registered exec_state, otherwise false.
  bool UnregisterQuery(const TUniqueId& query_id);

  // Initiates query cancellation reporting the given cause as the query status.
  // Assumes deliberate cancellation by the user if the cause is NULL.
  // Returns OK unless query_id is not found.
  // Queries still need to be unregistered, usually via Close, after cancellation.
  // Caller should not hold any locks when calling this function.
  Status CancelInternal(const TUniqueId& query_id, const Status* cause = NULL);

  // Close the session and release all resource used by this session.
  // Caller should not hold any locks when calling this function.
  // If ignore_if_absent is true, returns OK even if a session with the supplied ID does
  // not exist.
  Status CloseSessionInternal(const TUniqueId& session_id, bool ignore_if_absent);

  // Gets the runtime profile string for a given query_id and writes it to the output
  // stream. First searches for the query id in the map of in-flight queries. If no
  // match is found there, the query log is searched. Returns OK if the profile was
  // found, otherwise a Status object with an error message will be returned. The
  // output stream will not be modified on error.
  // If base64_encoded, outputs the base64 encoded profile output, otherwise the human
  // readable string.
  Status GetRuntimeProfileStr(const TUniqueId& query_id, bool base64_encoded,
      std::stringstream* output);

  // Webserver callback. Retrieves Hadoop confs from frontend and writes them to output
  void RenderHadoopConfigs(const Webserver::ArgumentMap& args, std::stringstream* output);

  // Webserver callback. Prints a sorted table of current queries, including their states,
  // types and IDs.
  void QueryStatePathHandler(const Webserver::ArgumentMap& args,
      std::stringstream* output);

  // Webserver callback.  Prints the query profile (via PrettyPrint)
  void QueryProfilePathHandler(const Webserver::ArgumentMap& args,
      std::stringstream* output);

  // Webserver callback.  Cancels an in-flight query.
  void CancelQueryPathHandler(const Webserver::ArgumentMap& args,
      std::stringstream* output);

  // Webserver callback.  Prints the query profile as a base64 encoded object.
  void QueryProfileEncodedPathHandler(const Webserver::ArgumentMap& args,
      std::stringstream* output);

  // Webserver callback.  Prints the inflight query ids.
  void InflightQueryIdsPathHandler(const Webserver::ArgumentMap& args,
      std::stringstream* output);

  // Webserver callback that prints a table of active sessions.
  void SessionPathHandler(const Webserver::ArgumentMap& args, std::stringstream* output);

  // Webserver callback that prints a list of all known databases and tables
  void CatalogPathHandler(const Webserver::ArgumentMap& args, std::stringstream* output);

  // Wrapper around Coordinator::Wait(); suitable for execution inside thread.
  // Must not be called with exec_state->lock() already taken.
  void Wait(boost::shared_ptr<QueryExecState> exec_state);

  // Initialize "default_configs_" to show the default values for ImpalaQueryOptions and
  // "support_start_over/false" to indicate that Impala does not support start over
  // in the fetch call.
  void InitializeConfigVariables();

  // Checks settings for profile logging, including whether the output
  // directory exists and is writeable, and initialises the first log file.
  // Returns OK unless there is some problem preventing profile log files
  // from being written. If an error is returned, the constructor will disable
  // profile logging.
  Status InitProfileLogging();

  // Checks settings for audit event logging, including whether the output
  // directory exists and is writeable, and initialises the first log file.
  // Returns OK unless there is some problem preventing audit event log files
  // from being written. If an error is returned, impalad startup will be aborted.
  Status InitAuditEventLogging();

  // Initializes a logging directory, creating the directory if it does not already
  // exist. If there is any error creating the directory an error will be returned.
  static Status InitLoggingDir(const std::string& log_dir);

  // Returns true if audit event logging is enabled, false otherwise.
  bool IsAuditEventLoggingEnabled();

  // Runs once every 5s to flush the profile log file to disk.
  void LogFileFlushThread();

  // Runs once every 5s to flush the audit log file to disk.
  void AuditEventLoggerFlushThread();

  Status LogAuditRecord(const QueryExecState& exec_state, const TExecRequest& request);

  // Copies a query's state into the query log. Called immediately prior to a
  // QueryExecState's deletion. Also writes the query profile to the profile log on disk.
  // Must be called with query_exec_state_map_lock_ held
  void ArchiveQuery(const QueryExecState& query);

  // Snapshot of a query's state, archived in the query log.
  struct QueryStateRecord {
    // Pretty-printed runtime profile. TODO: Copy actual profile object
    std::string profile_str;

    // Base64 encoded runtime profile
    std::string encoded_profile_str;

    // Query id
    TUniqueId id;

    // User that ran the query
    std::string user;

    // default db for this query
    std::string default_db;

    // SQL statement text
    std::string stmt;

    // DDL, DML etc.
    TStmtType::type stmt_type;

    // True if the query required a coordinator fragment
    bool has_coord;

    // The number of fragments that have completed
    int64_t num_complete_fragments;

    // The total number of fragments
    int64_t total_fragments;

    // The number of rows fetched by the client
    int64_t num_rows_fetched;

    // The state of the query as of this snapshot
    beeswax::QueryState::type query_state;

    // Start and end time of the query
    TimestampValue start_time, end_time;

    // Initialise from an exec_state. If copy_profile is true, print the query
    // profile to a string and copy that into this.profile (which is expensive),
    // otherwise leave this.profile empty.
    // If encoded_str is non-empty, it is the base64 encoded string for
    // exec_state->profile.
    QueryStateRecord(const QueryExecState& exec_state, bool copy_profile = false,
        const std::string& encoded_str = "");

    // Default constructor used only when participating in collections
    QueryStateRecord() { }

    // Comparator that sorts by start time.
    bool operator() (const QueryStateRecord& lhs, const QueryStateRecord& rhs) const;
  };

  // Helper method to render a single QueryStateRecord as an HTML table
  // row. Used by QueryStatePathHandler.
  void RenderSingleQueryTableRow(const QueryStateRecord& record, bool render_end_time,
      bool render_cancel, std::stringstream* output);

  // Beeswax private methods

  // Helper functions to translate between Beeswax and Impala structs
  Status QueryToTClientRequest(const beeswax::Query& query, TClientRequest* request);
  void TUniqueIdToQueryHandle(const TUniqueId& query_id, beeswax::QueryHandle* handle);
  void QueryHandleToTUniqueId(const beeswax::QueryHandle& handle, TUniqueId* query_id);

  // Helper function to raise BeeswaxException
  void RaiseBeeswaxException(const std::string& msg, const char* sql_state);

  // Executes the fetch logic. Doesn't clean up the exec state if an error occurs.
  Status FetchInternal(const TUniqueId& query_id, bool start_over,
      int32_t fetch_size, beeswax::Results* query_results);

  // Populate insert_result and clean up exec state. If the query
  // status is an error, insert_result is not populated and the status is returned.
  Status CloseInsertInternal(const TUniqueId& query_id, TInsertResult* insert_result);

  // HiveServer2 private methods (implemented in impala-hs2-server.cc)

  // Starts the synchronous execution of a HiverServer2 metadata operation.
  // If the execution succeeds, an QueryExecState will be created and registered in
  // query_exec_state_map_. Otherwise, nothing will be registered in query_exec_state_map_
  // and an error status will be returned. As part of this call, the TMetadataOpRequest
  // struct will be populated with the requesting user's session state.
  // Returns a TOperationHandle and TStatus.
  void ExecuteMetadataOp(
      const apache::hive::service::cli::thrift::THandleIdentifier& session_handle,
      TMetadataOpRequest* request,
      apache::hive::service::cli::thrift::TOperationHandle* handle,
      apache::hive::service::cli::thrift::TStatus* status);

  // Executes the fetch logic for HiveServer2 FetchResults.
  // Doesn't clean up the exec state if an error occurs.
  Status FetchInternal(const TUniqueId& query_id, int32_t fetch_size,
      apache::hive::service::cli::thrift::TFetchResultsResp* fetch_results);

  // Helper functions to translate between HiveServer2 and Impala structs

  // Returns !ok() if handle.guid.size() or handle.secret.size() != 16
  static Status THandleIdentifierToTUniqueId(
      const apache::hive::service::cli::thrift::THandleIdentifier& handle,
      TUniqueId* unique_id, TUniqueId* secret);
  static void TUniqueIdToTHandleIdentifier(
      const TUniqueId& unique_id, const TUniqueId& secret,
      apache::hive::service::cli::thrift::THandleIdentifier* handle);
  Status TExecuteStatementReqToTClientRequest(
      const apache::hive::service::cli::thrift::TExecuteStatementReq execute_request,
      TClientRequest* client_request);
  static void TColumnValueToHiveServer2TColumnValue(const TColumnValue& value,
      const TPrimitiveType::type& type,
      apache::hive::service::cli::thrift::TColumnValue* hs2_col_val);
  static void TQueryOptionsToMap(const TQueryOptions& query_option,
      std::map<std::string, std::string>* configuration);

  // Convert an expr value to HiveServer2 TColumnValue
  static void ExprValueToHiveServer2TColumnValue(const void* value,
      const TPrimitiveType::type& type,
      apache::hive::service::cli::thrift::TColumnValue* hs2_col_val);

  // Helper function to translate between Beeswax and HiveServer2 type
  static apache::hive::service::cli::thrift::TOperationState::type
      QueryStateToTOperationState(const beeswax::QueryState::type& query_state);

  // Helper method to process cancellations that result from failed backends, called from
  // the cancellation thread pool. The cancellation_work contains the query id to cancel
  // and a cause listing the failed backends that led to cancellation. Calls
  // CancelInternal directly, but has a signature compatible with the thread pool.
  void CancelFromThreadPool(uint32_t thread_id,
      const CancellationWork& cancellation_work);

  // For access to GetTableNames and DescribeTable
  friend class DdlExecutor;

  // Guards query_log_ and query_log_index_
  boost::mutex query_log_lock_;

  // FIFO list of query records, which are written after the query finishes executing
  typedef std::list<QueryStateRecord> QueryLog;
  QueryLog query_log_;

  // Index that allows lookup via TUniqueId into the query log
  typedef boost::unordered_map<TUniqueId, QueryLog::iterator> QueryLogIndex;
  QueryLogIndex query_log_index_;

  // Logger for writing encoded query profiles, one per line with the following format:
  // <ms-since-epoch> <query-id> <thrift query profile URL encoded and gzipped>
  boost::scoped_ptr<SimpleLogger> profile_logger_;

  // Logger for writing audit events, one per line with the format:
  // "<current timestamp>" : { JSON object }
  boost::scoped_ptr<SimpleLogger> audit_event_logger_;

  // If profile logging is enabled, wakes once every 5s to flush query profiles to disk
  boost::scoped_ptr<Thread> profile_log_file_flush_thread_;

  // If audit event logging is enabled, wakes once every 5s to flush audit events to disk
  boost::scoped_ptr<Thread> audit_event_logger_flush_thread_;

  // A Frontend proxy, used both for planning and for catalog requests.
  boost::scoped_ptr<Frontend> frontend_;

  // global, per-server state
  ExecEnv* exec_env_;  // not owned

  // Thread pool to process cancellation requests that come from failed Impala demons to
  // avoid blocking the statestore callback.
  boost::scoped_ptr<ThreadPool<CancellationWork> > cancellation_thread_pool_;

  // map from query id to exec state; QueryExecState is owned by us and referenced
  // as a shared_ptr to allow asynchronous deletion
  typedef boost::unordered_map<TUniqueId, boost::shared_ptr<QueryExecState> >
      QueryExecStateMap;
  QueryExecStateMap query_exec_state_map_;
  boost::mutex query_exec_state_map_lock_;  // protects query_exec_state_map_

  // map from fragment id to exec state; FragmentExecState is owned by us and
  // referenced as a shared_ptr to allow asynchronous calls to CancelPlanFragment()
  typedef boost::unordered_map<TUniqueId, boost::shared_ptr<FragmentExecState> >
      FragmentExecStateMap;
  FragmentExecStateMap fragment_exec_state_map_;
  boost::mutex fragment_exec_state_map_lock_;  // protects fragment_exec_state_map_

  // Default query options in the form of TQueryOptions and beeswax::ConfigVariable
  TQueryOptions default_query_options_;
  std::vector<beeswax::ConfigVariable> default_configs_;

  // Per-session state.  This object is reference counted using shared_ptrs.  There
  // is one ref count in the SessionStateMap for as long as the session is active.
  // All queries running from this session also have a reference.
  struct SessionState {
    TSessionType::type session_type;

    // Time the session was created
    TimestampValue start_time;

    // User for this session
    std::string user;

    // Client network address
    TNetworkAddress network_address;

    // Protects all fields below
    // If this lock has to be taken with query_exec_state_map_lock, take this lock first.
    boost::mutex lock;

    // If true, the session has been closed.
    bool closed;

    // The default database (changed as a result of 'use' query execution)
    std::string database;

    // The default query options of this session
    TQueryOptions default_query_options;

    // Inflight queries belonging to this session
    boost::unordered_set<TUniqueId> inflight_queries;

    // Builds a Thrift representation of this SessionState for serialisation to
    // the frontend.
    void ToThrift(const TUniqueId& session_id, TSessionState* session_state);
  };

  // Protects session_state_map_
  boost::mutex session_state_map_lock_;

  // A map from session identifier to a structure containing per-session information
  typedef boost::unordered_map<TUniqueId, boost::shared_ptr<SessionState> >
    SessionStateMap;
  SessionStateMap session_state_map_;

  // Protects connection_to_sessions_map_. May be taken before session_state_map_lock_.
  boost::mutex connection_to_sessions_map_lock_;

  // Map from a connection ID to the associated list of sessions so that all can be closed
  // when the connection ends. HS2 allows for multiplexing several sessions across a
  // single connection. If a session has already been closed (only possible via HS2) it is
  // not removed from this map to avoid the cost of looking it up.
  typedef boost::unordered_map<TUniqueId, std::vector<TUniqueId> >
    ConnectionToSessionMap;
  ConnectionToSessionMap connection_to_sessions_map_;

  // Return session state for given session_id.
  // If not found, session_state will be NULL and an error status will be returned.
  inline Status GetSessionState(const TUniqueId& session_id,
      boost::shared_ptr<SessionState>* session_state) {
    boost::lock_guard<boost::mutex> l(session_state_map_lock_);
    SessionStateMap::iterator i = session_state_map_.find(session_id);
    if (i == session_state_map_.end()) {
      *session_state = boost::shared_ptr<SessionState>();
      return Status("Invalid session id");
    } else {
      *session_state = i->second;
      return Status::OK;
    }
  }

  // protects query_locations_. Must always be taken after
  // query_exec_state_map_lock_ if both are required.
  boost::mutex query_locations_lock_;

  // A map from backend to the list of queries currently running there.
  typedef boost::unordered_map<TNetworkAddress, boost::unordered_set<TUniqueId> >
      QueryLocations;
  QueryLocations query_locations_;

  // A map from unique backend ID to the corresponding TNetworkAddress of that backend.
  // Used to track membership updates from the statestore so queries can be cancelled
  // when a backend is removed. It's not enough to just cancel fragments that are running
  // based on the deletions mentioned in the most recent statestore heartbeat; sometimes
  // cancellations are skipped and the statestore, at its discretion, may send only
  // a delta of the current membership so we need to compute any deletions.
  // TODO: Currently there are multiple locations where cluster membership is tracked,
  // here and in the scheduler. This should be consolidated so there is a single component
  // (the scheduler?) that tracks this information and calls other interested components.
  typedef boost::unordered_map<std::string, TNetworkAddress> BackendAddressMap;
  BackendAddressMap known_backends_;

  // Generate unique session id for HiveServer2 session
  boost::uuids::random_generator uuid_generator_;

  // Lock to protect uuid_generator
  boost::mutex uuid_lock_;
};

// Create an ImpalaServer and Thrift servers.
// If beeswax_port != 0 (and fe_server != NULL), creates a ThriftServer exporting
// ImpalaService (Beeswax) on beeswax_port (returned via beeswax_server).
// If hs2_port != 0 (and hs2_server != NULL), creates a ThriftServer exporting
// ImpalaHiveServer2Service on hs2_port (returned via hs2_server).
// If be_port != 0 (and be_server != NULL), create a ThriftServer exporting
// ImpalaInternalService on be_port (returned via be_server).
// Returns created ImpalaServer. The caller owns fe_server and be_server.
// The returned ImpalaServer is referenced by both of these via shared_ptrs and will be
// deleted automatically.
// Returns OK unless there was some error creating the servers, in
// which case none of the output parameters can be assumed to be valid.
Status CreateImpalaServer(ExecEnv* exec_env, int beeswax_port, int hs2_port,
    int be_port, ThriftServer** beeswax_server, ThriftServer** hs2_server,
    ThriftServer** be_server, ImpalaServer** impala_server);

}

#endif
