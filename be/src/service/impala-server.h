// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SERVICE_IMPALA_SERVER_H
#define IMPALA_SERVICE_IMPALA_SERVER_H

#include <jni.h>

#include "util/uid-util.h"  // for some reasoon needed right here for hash<TUniqueId>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "common/status.h"

namespace impala {

class ExecEnv;
class DataSink;
class RowDescriptor;

class TCatalogUpdate;
class TPlanExecRequest;
class TPlanExecParams;
class TExecPlanFragmentArgs;
class TExecPlanFragmentResult;
class TInsertResult;
class TReportExecStatusArgs;
class TReportExecStatusResult;
class TCancelPlanFragmentArgs;
class TCancelPlanFragmentResult;
class TTransmitDataArgs;
class TTransmitDataResult;
class THostPort;
class TClientRequest;
class TCreateExecRequestResult;
class ImpalaPlanServiceClient;

class ThriftServer;

// An ImpalaServer contains both frontend and backend functionality;
// it implements both the ImpalaService and ImpalaInternalService APIs.
// This class is partially thread-safe. To ensure freedom from deadlock,
// locks on the maps are obtained before locks on the items contained in the maps.
//
// TODO: The state of a running query is currently not cleaned up if the
// query doesn't experience any errors at runtime and close() doesn't get called.
// The solution is to have a separate thread that cleans up orphaned
// query execution states after a timeout period.
// TODO: The same doesn't apply to the execution state of an individual plan
// fragment: the originating coordinator might die, but we can get notified of
// that via sparrow. This still needs to be implemented.
class ImpalaServer : public ImpalaServiceIf, public ImpalaInternalServiceIf {
 public:
  ImpalaServer(ExecEnv* exec_env);
  ~ImpalaServer();

  // ImpalaService rpcs: Beeswax API
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

  // Return ImpalaQueryOptions default values and "support_start_over/false" to indicate
  // that Impala does not support start over in the fetch call. Hue relies on this not to
  // issue a "start_over" fetch call.
  // "include_hadoop" is not applicable.
  virtual void get_default_configuration(
      std::vector<beeswax::ConfigVariable>& configurations, const bool include_hadoop);

  // ImpalaService rpcs: unimplemented parts of Beeswax API.
  // These APIs will not be implemented because ODBC driver does not use them.
  virtual void dump_config(std::string& config);
  virtual void get_log(std::string& log, const beeswax::LogContextId& context);

  // ImpalaService rpcs: extensions over Beeswax
  virtual void Cancel(impala::TStatus& status, const beeswax::QueryHandle& query_id);
  virtual void ResetCatalog(impala::TStatus& status);
  virtual void CloseInsert(impala::TInsertResult& insert_result,
      const beeswax::QueryHandle& query_handle);

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

 private:
  class QueryExecState;
  class FragmentExecState;

  // Relevant ODBC SQL State code; for more info,
  // goto http://msdn.microsoft.com/en-us/library/ms714687.aspx
  static const char* SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION;
  static const char* SQLSTATE_GENERAL_ERROR;
  static const char* SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED;

  // Ascii output precision for double/float
  static const int ASCII_PRECISION;

  // Initiate execution of plan fragment in newly created thread.
  // Creates new FragmentExecState and registers it in corresp. map.
  Status StartPlanFragmentExecution(
      const TPlanExecRequest& request, const TPlanExecParams& params,
      const THostPort& coord_hostport, int backend_num);

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
  // The returned exec state's lock() will be held.
  boost::shared_ptr<QueryExecState> GetQueryExecState(const TUniqueId& query_id);

  // Return exec state for given fragment_id, or NULL if not found.
  FragmentExecState* GetFragmentExecState(const TUniqueId& fragment_id);

  // Call FE to get TClientRequestResult.
  Status GetQueryExecRequest(const TClientRequest& request,
      TCreateExecRequestResult* result);

  // Make any changes required to the metastore as a result of an
  // INSERT query, e.g. newly created partitions.
  Status UpdateMetastore(const TCatalogUpdate& catalog_update);

  // Call FE to get explain plan
  Status GetExplainPlan(const TClientRequest& query_request, std::string* explain_string);

  // Helper function to translate between Beeswax and Impala thrift
  void QueryToTClientRequest(const beeswax::Query& query, TClientRequest* request);
  void TUniqueIdToQueryHandle(const TUniqueId& query_id, beeswax::QueryHandle* handle);
  void QueryHandleToTUniqueId(const beeswax::QueryHandle& handle, TUniqueId* query_id);

  // Helper function to raise BeeswaxException
  void RaiseBeeswaxException(const std::string& msg, const char* sql_state);

  // Starts asynchronous execution of query. Creates QueryExecState (returned
  // in exec_state), registers it and calls Coordinator::Execute().
  // If it returns with an error status, exec_state will be NULL and nothing
  // will have been registered in query_exec_state_map_.
  Status Execute(const TClientRequest& request,
                 boost::shared_ptr<QueryExecState>* exec_state);

  // Remove exec_state from query_exec_state_map_. Returns true if it found
  // a registered exec_state, otherwise false.
  bool UnregisterQuery(const TUniqueId& query_id);

  // Executes the fetch logic. Doesn't clean up the exec state if an error occurs.
  Status FetchInternal(const TUniqueId& query_id, bool start_over,
      int32_t fetch_size, beeswax::Results* query_results);

  // Populate insert_result and clean up exec state
  Status CloseInsertInternal(const TUniqueId& query_id, TInsertResult* insert_result);

  // Non-thrift callable version of ResetCatalog
  Status ResetCatalogInternal();

  // Webserver callback. Retrieves Hadoop confs from frontend and writes them to output
  void RenderHadoopConfigs(std::stringstream* output);

  // Wrapper around Coordinator::Wait(); suitable for execution inside thread.
  void Wait(boost::shared_ptr<QueryExecState> exec_state);

  // Initialize "default_configs_" to show the default values for ImpalaQueryOptions and
  // "support_start_over/false" to indicate that Impala does not support start over
  // in the fetch call.
  void InitializeConfigVariables();

  // global, per-server state
  jobject fe_;  // instance of com.cloudera.impala.service.JniFrontend
  jmethodID create_exec_request_id_;  // JniFrontend.createExecRequest()
  jmethodID get_explain_plan_id_;  // JniFrontend.getExplainPlan()
  jmethodID get_hadoop_config_id_;  // JniFrontend.getHadoopConfigAsHtml()
  jmethodID reset_catalog_id_; // JniFrontend.resetCatalog()
  jmethodID update_metastore_id_; // JniFrontend.updateMetastore()
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

  // map from fragment id to exec state; FragmentExecState is owned by us
  typedef boost::unordered_map<TUniqueId, FragmentExecState*> FragmentExecStateMap;
  FragmentExecStateMap fragment_exec_state_map_;
  boost::mutex fragment_exec_state_map_lock_;  // protects fragment_exec_state_map_

  // Default configurations
  std::vector<beeswax::ConfigVariable> default_configs_;
};

// Create an ImpalaServer and Thrift servers.
// If fe_port != 0 (and fe_server != NULL), creates a ThriftServer exporting ImpalaService
// on fe_port (returned via fe_server).
// If be_port != 0 (and be_server != NULL), create a ThriftServer exporting
// ImpalaInternalService on be_port (returned via be_server).
// Returns created ImpalaServer. The caller owns fe_server and be_server.
// The returned ImpalaServer is referenced by both of these via shared_ptrs and will be
// deleted automatically.
ImpalaServer* CreateImpalaServer(ExecEnv* exec_env, int fe_port, int be_port,
    ThriftServer** fe_server, ThriftServer** be_server);

}

#endif
