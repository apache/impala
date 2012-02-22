// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_COORDINATOR_H
#define IMPALA_RUNTIME_COORDINATOR_H

#include <vector>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "common/status.h"
#include "util/runtime-profile.h"

namespace impala {

class DataStreamMgr;
class RowBatch;
class RowDescriptor;
class PlanExecutor;
class ObjectPool;
class RuntimeState;
class ImpalaBackendServiceClient;
class Expr;
class TestEnv;
class TQueryExecRequest;
class TRowBatch;
class TPlanExecRequest;
class TPlanExecParams;

// Query coordinator: handles execution of plan fragments on remote nodes, given
// a TQueryExecRequest. The coordinator fragment is executed locally in the current
// thread, all other fragments are sent to remote nodes. The coordinator also monitors
// the execution status of the remote fragments and aborts the entire query if an error
// occurs.
// Coordinator is *not* thread-safe; the only exception is Coordinator::Cancel(),
// which can be invoked asynchronously to Exec()/GetNext().
class Coordinator {
 public:
  // host/port are where the node we're executing on is exporting its
  // ImpalaBackendServiceIf; stream_mgr is this node's DataStreamMgr.
  // TODO: get rid of TestEnv here and implement a ClientCache interface
  // (that TestEnv implements)
  Coordinator(const std::string& host, int port, DataStreamMgr* stream_mgr,
              TestEnv* test_env);
  ~Coordinator();

  // Initiate execution of query. Blocks until result rows can be retrieved
  // from the coordinator fragment.
  // 'Request' must contain at least a coordinator plan fragment (ie, can't
  // be for a query like 'SELECT 1').
  Status Exec(const TQueryExecRequest& request);

  // Returns results from the coordinator fragment. Results are valid until
  // the next GetNext() call. '*batch' == NULL implies that subsequent calls
  // will not return any more rows.
  // '*batch' is owned by the underlying PlanExecutor and must not be deleted.
  Status GetNext(RowBatch** batch);

  // Cancel execution of query. Also cancels the execution of all plan fragments
  // on remote nodes.
  void Cancel();

  RuntimeState* runtime_state();
  const RowDescriptor& row_desc() const;

  RuntimeProfile* query_profile() { return query_profile_.get(); }

 private:
  std::string host_;
  int port_;
  DataStreamMgr* stream_mgr_;
  TestEnv* test_env_;

  // execution state of coordinator fragment
  boost::scoped_ptr<PlanExecutor> executor_;

  // owned by plan root, which resides in runtime_state_'s pool
  const RowDescriptor* row_desc_;

  std::vector<ImpalaBackendServiceClient*> clients_;

  // execution status of ExecPlanFragment() rpc; one per thread, indexed
  // by thread number that's handed out in Exec()
  std::vector<Status> remote_exec_status_;

  // group for exec threads
  boost::thread_group exec_thread_group_;

  // Aggregate counters for the entire query.
  boost::scoped_ptr<RuntimeProfile> query_profile_;

  // Lock to synchronize state when fragments complete.  This lock protects access
  // to the ObjectPool returned by obj_pool() and the query_profile_.
  boost::mutex fragment_complete_lock_;

  // Runtime profiles for fragments.  Profiles stored in profile_pool_
  std::vector<RuntimeProfile*> fragment_profiles_;

  // Wrapper for ExecPlanFragment() rpc.
  void ExecRemoteFragment(
      int thread_num, ImpalaBackendServiceClient* client,
      const TPlanExecRequest& request, const TPlanExecParams& params);

  ObjectPool* obj_pool();
};

}

#endif
