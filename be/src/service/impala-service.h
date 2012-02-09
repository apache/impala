// (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services ImpalaService and ImpalaBackendService.

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

// Thread-safe implementation of ImpalaService thrift service.
// An impalad server process needs to create a single object of this
// class in order to handle incoming rpcs.
class ImpalaService : public ImpalaServiceIf {
 public:
  ImpalaService(int port);
  virtual ~ImpalaService() {}

  // Initialize state. Terminates process on error.
  void Init(JNIEnv* env);

  virtual void RunQuery(TRunQueryResult& result, const TQueryRequest& request);

  // Running multiple FetchResults() rpcs for the same query concurrently
  // is *not* supported.
  virtual void FetchResults(TFetchResultsResult& result, const TUniqueId& query_id);

  virtual void CancelQuery(TStatus& result, const TUniqueId& query_id);

 private:
  int port_;

  // global, per-server state
  jobject fe_;  // instance of com.cloudera.impala.service.Frontend
  jmethodID get_exec_request_id_;  // FrontEnd.GetExecRequest()
  boost::scoped_ptr<ExecEnv> exec_env_;

  // execution state of a single query
  // TODO: keep cache of pre-formatted ExecStates
  class ExecState {
   public:
    ExecState(const TQueryRequest& request, ExecEnv* exec_env)
      : request_(request), coord_(NULL), eos_(false) {}

    // Set output_exprs_ and col_types, based on exprs.
    Status PrepareSelectListExprs(
        RuntimeState* runtime_state, const std::vector<TExpr>& exprs,
        const RowDescriptor& row_desc, std::vector<TPrimitiveType::type>* col_types);

    // Evaluates output_exprs_ against all rows in batch and stores
    // results in query_result.
    void ConvertRowBatch(RowBatch* batch, TQueryResult* query_result);

    // Creates single result row in query_result by evaluating output_exprs_ without
    // a row (ie, the expressions are constants).
    void CreateConstantRow(TQueryResult* query_result);

    const TQueryRequest& request() const { return request_; }
    Coordinator* coord() const { return coord_.get(); }
    RuntimeState* local_runtime_state() { return &local_runtime_state_; }

   private:
    friend class ImpalaService;

    TQueryRequest request_;  // the original request
    ExecStats exec_stats_;
    boost::scoped_ptr<Coordinator> coord_;  // not set for queries w/o FROM
    // local runtime_state_ in case we don't have a coord_
    RuntimeState local_runtime_state_;
    std::vector<Expr*> output_exprs_;
    bool eos_;  // if true, there are no more rows to return
  };

  // map from query id to ExecState for that query
  typedef boost::unordered_map<TUniqueId, ExecState*> ExecStateMap;
  ExecStateMap exec_state_map_;
  boost::mutex exec_state_map_lock_;  // protects exec_state_map_;

  // Call FE to get TQueryExecRequest.
  Status GetExecRequest(
      const TQueryRequest& query_request, TQueryExecRequest* exec_request);
};

}
