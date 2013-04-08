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

#include <jni.h>

#include "util/uid-util.h"  // for some reason needed right here for hash<TUniqueId>
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
#include "util/thrift-server.h"
#include "common/status.h"
#include "exec/ddl-executor.h"
#include "util/metrics.h"
#include "util/runtime-profile.h"
#include "runtime/coordinator.h"
#include "runtime/primitive-type.h"
#include "runtime/timestamp-value.h"
#include "runtime/runtime-state.h"

namespace impala {

class ExecEnv;
class DataSink;
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
class ImpalaPlanServiceClient;

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
                     public ThriftServer::SessionHandlerIf {
 public:
  ImpalaServer(ExecEnv* exec_env);
  ~ImpalaServer();

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
  virtual void PingImpalaService();
  // TODO: Need to implement HiveServer2 version of GetRuntimeProfile
  virtual void GetRuntimeProfile(std::string& profile_output,
      const beeswax::QueryHandle& query_id);

  // ImpalaHiveServer2Service rpcs: HiveServer2 API (implemented in impala-hs2-server.cc)
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

  // ImpalaService common extensions (implemented in impala-server.cc)
  virtual void ResetCatalog(impala::TStatus& status);

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
  // Called when a Beeswax session starts. Registers a new SessionState with the provided
  // key.
  virtual void SessionStart(const ThriftServer::SessionKey& session_key);

  // Called when a Beeswax session terminates. Unregisters the SessionState associated
  // with the provided key.
  virtual void SessionEnd(const ThriftServer::SessionKey& session_key);

  // Called when a membership update is received from the state-store. Looks for
  // active nodes that have failed, and cancels any queries running on them.
  //  - topic_deltas: all changes to registered state-store topics
  //  - topic_updates: output parameter to publish any topic updates to. Unused.
  void MembershipCallback(const StateStoreSubscriber::TopicDeltaMap& topic_deltas,
      vector<TTopicUpdate>* topic_updates);

  // Reads a configuration value from Hadoop's configuration in the
  // front-end. If the configuration key is not found, returns the
  // empty string.
  // Returns Status::OK unless there is a JNI error.
  Status GetHadoopConfigValue(const std::string& key, std::string* output);

 private:
  class FragmentExecState;

  // Query result set stores converted rows returned by QueryExecState.fetchRows(). It
  // provides an interface to convert Impala rows to external API rows.
  // It is an abstract class. Subclass must implement AddOneRow().
  class QueryResultSet {
   public:
    QueryResultSet() {}
    virtual ~QueryResultSet() {}

    // Add the row (list of expr value) from a select query to this result set. When a
    // row comes from a select query, the row is in the form of expr values (void*).
    virtual Status AddOneRow(const vector<void*>& row) = 0;

    // Add the TResultRow to this result set. When a row comes from a DDL/metadata
    // operation, the row in the form of TResultRow.
    virtual Status AddOneRow(const TResultRow& row) = 0;
  };

  class AsciiQueryResultSet; // extends QueryResultSet
  class TRowQueryResultSet; // extends QueryResultSet

  struct SessionState;

  // Execution state of a query. This captures everything necessary
  // to convert row batches received by the coordinator into results
  // we can return to the client. It also captures all state required for
  // servicing query-related requests from the client.
  // Thread safety: this class is generally not thread-safe, callers need to
  // synchronize access explicitly via lock().
  // To avoid deadlocks, the caller must *not* acquire query_exec_state_map_lock_
  // while holding the exec state's lock.
  // TODO: Consider renaming to RequestExecState for consistency.
  class QueryExecState {
   public:
    QueryExecState(ExecEnv* exec_env, ImpalaServer* server,
        boost::shared_ptr<SessionState> session,
        const TSessionState& query_session_state)
      : exec_env_(exec_env),
        parent_session_(session),
        query_session_state_(query_session_state),
        coord_(NULL),
        profile_(&profile_pool_, "Query"),  // assign name w/ id after planning
        summary_info_(&profile_pool_, "Summary"),
        eos_(false),
        query_state_(beeswax::QueryState::CREATED),
        current_batch_(NULL),
        current_batch_row_(0),
        num_rows_fetched_(0),
        impala_server_(server),
        start_time_(TimestampValue::local_time()) {
      planner_timer_ = ADD_TIMER(&profile_, "PlanningTime");
    }

    ~QueryExecState() {
    }

    // Initiates execution of plan fragments, if there are any, and sets
    // up the output exprs for subsequent calls to FetchRows().
    // Also sets up profile and pre-execution counters.
    // Non-blocking.
    Status Exec(TExecRequest* exec_request);

    // Execute a HiveServer2 metadata operation
    Status Exec(const TMetadataOpRequest& exec_request);

    // Call this to ensure that rows are ready when calling FetchRows().
    // Must be preceded by call to Exec().
    Status Wait();

    // Return at most max_rows from the current batch. If the entire current batch has
    // been returned, fetch another batch first.
    // Caller should verify that EOS has not be reached before calling.
    // Always calls coord()->Wait() prior to getting a batch.
    // Also updates query_state_/status_ in case of error.
    Status FetchRows(const int32_t max_rows, QueryResultSet* fetched_rows);

    // Update query state if the requested state isn't already obsolete.
    void UpdateQueryState(beeswax::QueryState::type query_state);

    void SetErrorStatus(const Status& status);

    // Sets state to EXCEPTION and cancels coordinator.
    // Caller needs to hold lock().
    // Does nothing if the query has reached EOS.
    void Cancel();

    // This is called when the query is done (finished, cancelled, or failed).
    void Done();

    SessionState* parent_session() { return parent_session_.get(); }
    const std::string user() const { return parent_session_->user; }
    const std::string default_db() const { return query_session_state_.database; }
    bool eos() { return eos_; }
    Coordinator* coord() const { return coord_.get(); }
    int num_rows_fetched() const { return num_rows_fetched_; }
    const TResultSetMetadata* result_metadata() { return &result_metadata_; }
    const TUniqueId& query_id() const { return query_id_; }
    const TExecRequest& exec_request() const { return exec_request_; }
    TStmtType::type stmt_type() const { return exec_request_.stmt_type; }
    boost::mutex* lock() { return &lock_; }
    const beeswax::QueryState::type query_state() const { return query_state_; }
    void set_query_state(beeswax::QueryState::type state) { query_state_ = state; }
    const Status& query_status() const { return query_status_; }
    RuntimeProfile::Counter* planner_timer() { return planner_timer_; }
    void set_result_metadata(const TResultSetMetadata& md) { result_metadata_ = md; }
    const RuntimeProfile& profile() const { return profile_; }
    const TimestampValue& start_time() const { return start_time_; }
    const TimestampValue& end_time() const { return end_time_; }

   private:
    TUniqueId query_id_;
    boost::mutex lock_;  // protects all following fields
    ExecEnv* exec_env_;

    // Session that this query is from
    boost::shared_ptr<SessionState> parent_session_;

    // Snapshot of state in session_ that is not constant (and can change from
    // QueryExecState to QueryExecState).
    const TSessionState query_session_state_;

    // not set for queries w/o FROM, ddl queries, or short-circuited (i.e. queries with
    // "limit 0")
    boost::scoped_ptr<Coordinator> coord_;

    boost::scoped_ptr<DdlExecutor> ddl_executor_; // Runs DDL queries, instead of coord_
    // local runtime_state_ in case we don't have a coord_
    boost::scoped_ptr<RuntimeState> local_runtime_state_;
    ObjectPool profile_pool_;
    RuntimeProfile profile_;
    RuntimeProfile summary_info_;
    RuntimeProfile::Counter* planner_timer_;
    vector<Expr*> output_exprs_;
    bool eos_;  // if true, there are no more rows to return
    beeswax::QueryState::type query_state_;
    Status query_status_;
    TExecRequest exec_request_;

    TResultSetMetadata result_metadata_; // metadata for select query
    RowBatch* current_batch_; // the current row batch; only applicable if coord is set
    int current_batch_row_; // number of rows fetched within the current batch
    int num_rows_fetched_; // number of rows fetched by client for the entire query

    // To get access to UpdateMetastore
    ImpalaServer* impala_server_;

    // Start/end time of the query
    TimestampValue start_time_, end_time_;

    // Core logic of FetchRows(). Does not update query_state_/status_.
    Status FetchRowsInternal(const int32_t max_rows, QueryResultSet* fetched_rows);

    // Fetch the next row batch and store the results in current_batch_. Only
    // called for non-DDL / DML queries.
    Status FetchNextBatch();

    // Evaluates output_exprs_ against row and output the evaluated row in result.
    // result must have been resized to the number of columns before call.
    Status GetRowValue(TupleRow* row, vector<void*>* result);

    // Set output_exprs_, based on exprs.
    Status PrepareSelectListExprs(RuntimeState* runtime_state,
        const vector<TExpr>& exprs, const RowDescriptor& row_desc);

    // Gather and publish all required updates to the metastore
    Status UpdateMetastore();
  };

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
  inline boost::shared_ptr<QueryExecState> GetQueryExecState(
      const TUniqueId& query_id, bool lock) {
    boost::lock_guard<boost::mutex> l(query_exec_state_map_lock_);
    QueryExecStateMap::iterator i = query_exec_state_map_.find(query_id);
    if (i == query_exec_state_map_.end()) {
      return boost::shared_ptr<QueryExecState>();
    } else {
      if (lock) i->second->lock()->lock();
      return i->second;
    }
  }

  // Return exec state for given fragment_instance_id, or NULL if not found.
  boost::shared_ptr<FragmentExecState> GetFragmentExecState(
      const TUniqueId& fragment_instance_id);

  // Call FE to get TClientRequestResult.
  Status GetExecRequest(const TClientRequest& request, TExecRequest* result);

  // Make any changes required to the metastore as a result of an
  // INSERT query, e.g. newly created partitions.
  Status UpdateMetastore(const TCatalogUpdate& catalog_update);

  // Call FE to get explain plan
  Status GetExplainPlan(const TClientRequest& query_request, std::string* explain_string);

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
      const TUniqueId& query_id, const boost::shared_ptr<QueryExecState>& exec_state);

  // Cancel the query execution if the query is still running. Removes exec_state from
  // query_exec_state_map_, and removes the query id from session state's open query list
  // Returns true if it found a registered exec_state, otherwise false.
  bool UnregisterQuery(const TUniqueId& query_id);

  // Non-thrift callable version of ResetCatalog
  Status ResetCatalogInternal();

  // Initiates query cancellation. Returns OK unless query_id is not found.
  // Caller should not hold any locks when calling this function.
  Status CancelInternal(const TUniqueId& query_id);

  // Close the session and release all resource used by this session.
  // Caller should not hold any locks when calling this function.
  Status CloseSessionInternal(const ThriftServer::SessionKey& session_key);

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

  // Webserver callback. Prints a table of current queries, including their
  // states, types and IDs.
  void QueryStatePathHandler(const Webserver::ArgumentMap& args,
      std::stringstream* output);

  // Webserver callback.  Prints the query profile (via PrettyPrint)
  void QueryProfilePathHandler(const Webserver::ArgumentMap& args,
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

  // Webserver callback that prints a list of known backends
  void BackendsPathHandler(const Webserver::ArgumentMap& args, std::stringstream* output);

  // Wrapper around Coordinator::Wait(); suitable for execution inside thread.
  void Wait(boost::shared_ptr<QueryExecState> exec_state);

  // Initialize "default_configs_" to show the default values for ImpalaQueryOptions and
  // "support_start_over/false" to indicate that Impala does not support start over
  // in the fetch call.
  void InitializeConfigVariables();

  // Validate Hadoop config; requires FE
  Status ValidateSettings();

  // Returns all matching table names, per Hive's "SHOW TABLES <pattern>". Each
  // table name returned is unqualified.
  // If db is NULL, match table names from all databases, otherwise restrict the
  // search to the named database.
  // If pattern is NULL, match all tables otherwise match only those tables that
  // match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  // and each pN may contain wildcards denoted by '*' which match all strings.
  Status GetTableNames(const std::string* db, const std::string* pattern,
      TGetTablesResult* table_names);

  // Return all databases matching the optional argument 'pattern'.
  // If pattern is NULL, match all databases otherwise match only those databases that
  // match the pattern string. Patterns are "p1|p2|p3" where | denotes choice,
  // and each pN may contain wildcards denoted by '*' which match all strings.
  Status GetDbNames(const std::string* pattern, TGetDbsResult* table_names);

  // Returns (in the output parameter) a list of columns for the specified table
  // in the specified database.
  Status DescribeTable(const std::string& db, const std::string& table,
      TDescribeTableResult* columns);

  // Modifies an existing table's metastore metadata. The specific type of operation is
  // defined by the TAlterTableType field in TAlterTableParams. Some supported operations
  // include renaming tables, adding/dropping columns/partitions from tables, and changing
  // a table's file format. Returns OK if the operation was successfull, otherwise a
  // Status object with information on the error will be returned.
  Status AlterTable(const TAlterTableParams& alter_table_params);

  // Creates a new database in the metastore with the specified name. Returns OK if the
  // database was successfully created, otherwise CANCELLED is returned with details on
  // the specific error. Common errors include creating a database that already exists
  // and metastore connectivity problems.
  Status CreateDatabase(const TCreateDbParams& create_db_params);

  // Creates a new table in the metastore with the specified name. Returns OK if the
  // table was successfully created, otherwise CANCELLED is returned. Common errors
  // include creating a table that already exists, creating a table in a database that
  // does not exist, and metastore connectivity problems.
  Status CreateTable(const TCreateTableParams& create_table_params);

  // Creates a new table in the metastore that is a based on the table definition of a
  // given source table. This is a metadata only operation - no data is copied.
  Status CreateTableLike(const TCreateTableLikeParams& create_table_like_params);

  // Drops the specified database from the metastore. Returns OK if the database
  // drop was successful, otherwise CANCELLED is returned.
  Status DropDatabase(const TDropDbParams& drop_db_params);

  // Drops the specified table from the metastore. Returns OK if the table drop was
  // successful, otherwise CANCELLED is returned.
  Status DropTable(const TDropTableParams& drop_table_params);

  // Copies a query's state into the query log. Called immediately prior to a
  // QueryExecState's deletion.
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
    QueryStateRecord(const QueryExecState& exec_state, bool copy_profile=true);
  };

  // Helper method to render a single QueryStateRecord as an HTML table
  // row. Used by QueryStatePathHandler.
  void RenderSingleQueryTableRow(const QueryStateRecord& record, bool render_end_time,
      std::stringstream* output);

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

  // Populate insert_result and clean up exec state
  Status CloseInsertInternal(const TUniqueId& query_id, TInsertResult* insert_result);

  // HiveServer2 private methods (implemented in impala-hs2-server.cc)

  // Starts the synchronous execution of a HiverServer2 metadata operation.
  // If the execution succeeds, an QueryExecState will be created and registered in
  // query_exec_state_map_. Otherwise, nothing will be registered in query_exec_state_map_
  // and an error status will be returned.
  // Returns a TOperationHandle and TStatus.
  void ExecuteMetadataOp(const ThriftServer::SessionKey& session_key,
      const TMetadataOpRequest& request,
      apache::hive::service::cli::thrift::TOperationHandle* handle,
      apache::hive::service::cli::thrift::TStatus* status);

  // Calls FE to execute HiveServer2 metadata operation.
  Status ExecHiveServer2MetadataOp(const TMetadataOpRequest& request,
      TMetadataOpResponse* result);

  // Executes the fetch logic for HiveServer2 FetchResults.
  // Doesn't clean up the exec state if an error occurs.
  Status FetchInternal(const TUniqueId& query_id, int32_t fetch_size,
      apache::hive::service::cli::thrift::TFetchResultsResp* fetch_results);

  // Helper functions to translate between HiveServer2 and Impala structs
  static void THandleIdentifierToTUniqueId(
      const apache::hive::service::cli::thrift::THandleIdentifier &handle,
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

  // global, per-server state
  jobject fe_;  // instance of com.cloudera.impala.service.JniFrontend
  jmethodID create_exec_request_id_;  // JniFrontend.createExecRequest()
  jmethodID get_explain_plan_id_;  // JniFrontend.getExplainPlan()
  jmethodID get_hadoop_config_id_;  // JniFrontend.getHadoopConfigAsHtml()
  jmethodID get_hadoop_config_value_id_; // JniFrontend.getHadoopConfigValue
  jmethodID check_hadoop_config_id_; // JniFrontend.checkHadoopConfig()
  jmethodID reset_catalog_id_; // JniFrontend.resetCatalog()
  jmethodID update_metastore_id_; // JniFrontend.updateMetastore()
  jmethodID get_table_names_id_; // JniFrontend.getTableNames
  jmethodID describe_table_id_; // JniFrontend.describeTable
  jmethodID get_db_names_id_; // JniFrontend.getDbNames
  jmethodID exec_hs2_metadata_op_id_; // JniFrontend.execHiveServer2MetadataOp
  jmethodID alter_table_id_; // JniFrontend.alterTable
  jmethodID create_database_id_; // JniFrontend.createDatabase
  jmethodID create_table_id_; // JniFrontend.createTable
  jmethodID create_table_like_id_; // JniFrontend.createTableLike
  jmethodID drop_database_id_; // JniFrontend.dropDatabase
  jmethodID drop_table_id_; // JniFrontend.dropTable
  ExecEnv* exec_env_;  // not owned

  // plan service-related - impalad optionally uses a standalone
  // plan service (see FLAGS_use_planservice etc)
  boost::shared_ptr<apache::thrift::transport::TTransport> planservice_socket_;
  boost::shared_ptr<apache::thrift::transport::TTransport> planservice_transport_;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> planservice_protocol_;
  boost::scoped_ptr<ImpalaPlanServiceClient> planservice_client_;

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

  // Impala has two types of sessions: Beeswax and HiveServer2
  enum SessionType {
    BEESWAX,
    HIVESERVER2
  };

  // Per-session state.  This object is reference counted using shared_ptrs.  There
  // is one ref count in the SessionStateMap for as long as the session is active.
  // All queries running from this session also have a reference.
  struct SessionState {
    SessionType session_type;

    // Time the session was created
    TimestampValue start_time;

    // User for this session
    std::string user;

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

    // Builds a Thrift representation of the default database for serialisation to
    // the frontend.
    void ToThrift(TSessionState* session_state);
  };

  // Protects session_state_map_
  boost::mutex session_state_map_lock_;

  // A map from session identifier to a structure containing per-session information
  typedef boost::unordered_map<ThriftServer::SessionKey, boost::shared_ptr<SessionState> >
    SessionStateMap;
  SessionStateMap session_state_map_;

  // Return session state for given session_id.
  // If not found, session_state will be NULL and an error status will be returned.
  inline Status GetSessionState(const ThriftServer::SessionKey& session_id,
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

  // The set of backends last reported by the state-store, used for failure detection.
  std::vector<TNetworkAddress> last_membership_;

  // Generate unique session id for HiveServer2 session
  boost::uuids::random_generator uuid_generator_;
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
