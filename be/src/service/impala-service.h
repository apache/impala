// (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services BeeswaxService.

#include <vector>
#include <jni.h>
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common/status.h"
#include "codegen/llvm-codegen.h"
#include "exec/exec-stats.h"
#include "runtime/runtime-state.h"
#include "util/uid-util.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class RowBatch;
class TQueryResult;
class Expr;
class RowDescriptor;
class ExecEnv;
class Coordinator;
class TExpr;
class TQueryRequest;
class TQueryExecRequest;
class ImpalaPlanServiceClient;

// TODO: Beeswax implementation is only partial.
// An Impala implementation of the Beeswax Service that only implements API used by the
// Beeswax+ ODBC driver.
// An impalad server process needs to create a single object of this
// class in order to handle incoming rpcs.
class ImpalaService : public ImpalaServiceIf {
 public:
  ImpalaService(ExecEnv* exec_env, int port);
  virtual ~ImpalaService();

  // Initialize state. Terminates process on error.
  void Init(JNIEnv* env);

    // Beeswax API
  // TODO: only partially implemented (or not implemented at all)
  virtual void executeAndWait(beeswax::QueryHandle& query_handle,
      const beeswax::Query& query, const beeswax::LogContextId& client_ctx);
  virtual void explain(beeswax::QueryExplanation& query_explanation,
      const beeswax::Query& query);
  virtual void fetch(beeswax::Results& query_results,
      const beeswax::QueryHandle& query_id, const bool start_over,
      const int32_t fetch_size);
  virtual void get_results_metadata(beeswax::ResultsMetadata& results_metadata,
      const beeswax::QueryHandle& handle);
  virtual void close(const beeswax::QueryHandle& handle);

  // These APIs are fully implemented.
  virtual beeswax::QueryState::type get_state(const beeswax::QueryHandle& handle);
  virtual void echo(std::string& echo_string, const std::string& input_string);
  virtual void clean(const beeswax::LogContextId& log_context);

  // These APIs will not be implemented because ODBC driver does not use them.
  virtual void query(beeswax::QueryHandle& query_handle, const beeswax::Query& query);
  virtual void dump_config(std::string& config);
  virtual void get_log(std::string& log, const beeswax::LogContextId& context);
  virtual void get_default_configuration(
      std::vector<beeswax::ConfigVariable> & configurations,
      const bool include_hadoop);

  // Impala service extension API
  virtual void Cancel(impala::TStatus& status, const beeswax::QueryHandle& query_id);
  virtual void ResetCatalog(impala::TStatus& status);

 private:
  int port_;

  // global, per-server state
  jobject fe_;  // instance of com.cloudera.impala.service.Frontend
  jmethodID get_exec_request_id_;  // FrontEnd.GetExecRequest()
  jmethodID get_explain_plan_id_;  // FrontEnd.GetExplainPlan()
  jmethodID reset_catalog_id_; // FrontEnd.resetCatalog
  ExecEnv* exec_env_;  // not owned

  // plan service-related - impalad optionally uses a standalone
  // plan service (see FLAGS_use_planservice etc)
  boost::shared_ptr<apache::thrift::transport::TTransport> planservice_socket_;
  boost::shared_ptr<apache::thrift::transport::TTransport> planservice_transport_;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> planservice_protocol_;
  boost::scoped_ptr<ImpalaPlanServiceClient> planservice_client_;

  // execution state of a single query
  // TODO: keep cache of pre-formatted ExecStates
  class ExecState {
   public:
    ExecState(const TQueryRequest& request, ExecEnv* exec_env)
      : request_(request), exec_env_(exec_env), coord_(NULL), eos_(false),
        current_batch_(NULL), current_batch_row_(0), current_row_(0) {}

    // Set output_exprs_ and col_types, based on exprs.
    Status PrepareSelectListExprs(RuntimeState* runtime_state,
        const std::vector<TExpr>& exprs, const RowDescriptor& row_desc);

    // Reset to a new Coordinator
    void ResetCoordinator();

    // Return at most max_rows from the current batch. If the entire current batch has
    // been returned, fetch another batch first.
    // Returns the number of converted rows.
    // Caller should verify that EOS has not be reached before calling.
    void FetchRowsAsAscii(const int32_t max_rows, std::vector<std::string>* fetched_rows);

    bool eos() { return eos_; }
    Coordinator* coord() const { return coord_.get(); }
    int current_row() const { return current_row_; }
    RuntimeState* local_runtime_state() { return &local_runtime_state_; }

   private:
    TQueryRequest request_;  // the original request
    ExecStats exec_stats_;
    ExecEnv* exec_env_;
    boost::scoped_ptr<Coordinator> coord_;  // not set for queries w/o FROM
    // local runtime_state_ in case we don't have a coord_
    RuntimeState local_runtime_state_;
    std::vector<Expr*> output_exprs_;
    bool eos_;  // if true, there are no more rows to return

    std::vector<PrimitiveType> col_types_; // column types of the query
    RowBatch* current_batch_; // the current row batch; only applicable if coord is set
    int current_batch_row_; // num of rows fetched within the current batch
    int current_row_; // num of rows that has been fetched for the entire query

    // Fetch the next row batch and store the results in current_batch_
    void FetchNextBatch();

    // Evaluates output_exprs_ against at most max_rows in the current_batch_ starting
    // from current_batch_row_ and output the evaluated rows in Ascii form in
    // fetched_rows.
    void ConvertRowBatchToAscii(const int32_t max_rows,
        std::vector<std::string>* fetched_rows);

    // Creates single result row in query_result by evaluating output_exprs_ without
    // a row (ie, the expressions are constants) and put it in query_results_;
    void CreateConstantRowAsAscii(std::vector<std::string>* fetched_rows);

  };

  // map from query id to ExecState for that query
  typedef boost::unordered_map<TUniqueId, ExecState*> ExecStateMap;
  ExecStateMap exec_state_map_;
  boost::mutex exec_state_map_lock_;  // protects exec_state_map_;
  ExecState* GetExecState(const TUniqueId& unique_id); // return null if not found

  // Call FE to get TQueryExecRequest.
  Status GetExecRequest(const TQueryRequest& query_request,
      TQueryExecRequest* exec_request);

  // Call FE to get explain plan
  Status GetExplainPlan(const TQueryRequest& query_request, std::string* explain_string);

  // Helper function to translate between Beeswax and Impala thrift
  // TODO: remove these helper once we've merged the old Imapala service API into
  //       Beeswax or HiveServer2.
  void QueryToTQueryRequest(const beeswax::Query& query, TQueryRequest* request);
  void TUniqueIdToQueryHandle(const TUniqueId& query_id, beeswax::QueryHandle* handle);
  void QueryHandleToTUniqueId(const beeswax::QueryHandle& handle, TUniqueId* query_id);

  Status executeAndWaitInternal(const TQueryRequest& request, TUniqueId* query_id);
};

}
