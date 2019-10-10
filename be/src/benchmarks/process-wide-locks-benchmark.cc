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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "runtime/query-exec-mgr.h"
#include "runtime/query-state.h"
#include "runtime/test-env.h"
#include "scheduling/request-pool-service.h"
#include "service/fe-support.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/stopwatch.h"
#include "util/thread.h"
#include "util/uid-util.h"
#include "util/unique-id-hash.h"

#include "common/init.h"
#include "common/names.h"

/// This tests the performance of the following process wide locks:
//
/// 1. qs_map_lock_ (Sharded)
/// TODO: query_driver_map_lock_ (Sharded)
//
/// A reasonable amount of queries are created and accessed multiple times via the
/// QueryExecMgr's APIs to benchmark the time taken to acquire the lock and retrieve
/// the QueryState.
//
/// ------------------Benchmark 1: Create and access Query States.
/// Total Time (#Queries: 5 #Accesses: 100) : 2202.44K clock cycles
/// Total Time (#Queries: 50 #Accesses: 100) : 4ms
/// Total Time (#Queries: 50 #Accesses: 1000) : 16ms
/// Total Time (#Queries: 500 #Accesses: 100) : 46ms
/// Total Time (#Queries: 500 #Accesses: 1000) : 129ms
/// Total Time (#Queries: 500 #Accesses: 5000) : 518ms
/// Total Time (#Queries: 1000 #Accesses: 1000) : 246ms
/// Total Time (#Queries: 1000 #Accesses: 5000) : 1s018ms
//
/// This was created to test improvements for IMPALA-4456.

using boost::uuids::random_generator;

using namespace impala;

boost::scoped_ptr<TestEnv> test_env_;
vector<TUniqueId> query_ids;

// This function creates a QueryState and accesses it 'num_accesses' times, via the
// QueryExecMgr APIs.
// TODO: Add a similar funciton for ClientRequestStates.
void CreateAndAccessQueryStates(const TUniqueId& query_id, int num_accesses) {
  TQueryCtx query_ctx;
  query_ctx.query_id = query_id;

  string resolved_pool;
  Status s = ExecEnv::GetInstance()->request_pool_service()->ResolveRequestPool(
      query_ctx, &resolved_pool);

  query_ctx.__set_request_pool(resolved_pool);

  QueryState *query_state;
  query_state = ExecEnv::GetInstance()->query_exec_mgr()->CreateQueryState(query_ctx, -1);
  DCHECK(query_state != nullptr);
  query_state->AcquireBackendResourceRefcount();

  for (int i=0; i < num_accesses ; ++i) {
    QueryState* qs;
    qs = ExecEnv::GetInstance()->query_exec_mgr()->GetQueryState(query_id);
    DCHECK(qs != nullptr);
    ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(qs);
  }

  query_state->ReleaseBackendResourceRefcount();
  // This should drop the last reference count to the QueryState and destroy it.
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(query_state);
  // Make sure that the query doesn't exist in the map any longer.
  DCHECK(ExecEnv::GetInstance()->query_exec_mgr()->GetQueryState(query_id) == nullptr);

}

// Runs 'num_threads' Impala Threads and have each of them execute func().
void ImpalaThreadStarter(void (*func) (const TUniqueId&, int), int num_threads,
    int func_arg) {
  vector<unique_ptr<Thread>> threads;
  threads.reserve(num_threads);

  for (int i=0; i < num_threads; ++i) {
    unique_ptr<Thread> thread;
    function<void ()> f =
        bind(func, query_ids[i], func_arg);
    Status s =
        Thread::Create("mythreadgroup", "thread", f, &thread);
    DCHECK(s.ok());
    threads.push_back(move(thread));
  }
  for (unique_ptr<Thread>& thread: threads) {
    thread->Join();
  }
}

void RunBenchmark(int num_queries, int num_accesses) {
  StopWatch total_time;
  total_time.Start();
  ImpalaThreadStarter(CreateAndAccessQueryStates, num_queries, num_accesses);
  total_time.Stop();

  cout << "Total Time " << "(#Queries: " << num_queries << " #Accesses: "
       << num_accesses << ") : "
       << PrettyPrinter::Print(total_time.ElapsedTime(), TUnit::CPU_TICKS) << endl;
}

// Create and store 'num_queries' Query IDs into 'query_ids'.
void CreateQueryIds(int num_queries) {
  for (int i=0; i < num_queries; ++i) {
    query_ids[i] = UuidToQueryId(random_generator()());
  }
}

int main(int argc, char **argv) {
  // Though we don't use the JVM or require FeSupport, the TestEnv class requires it,
  // so we start them up.
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();

  const int MAX_QUERIES = 1000;

  query_ids.reserve(MAX_QUERIES);

  test_env_.reset(new TestEnv());
  ABORT_IF_ERROR(test_env_->Init());

  CreateQueryIds(MAX_QUERIES);

  cout << "------------------Benchmark 1: Create and access Query States." << endl;
  RunBenchmark(5, 100);
  RunBenchmark(50, 100);
  RunBenchmark(50, 1000);
  RunBenchmark(500, 100);
  RunBenchmark(500, 1000);
  RunBenchmark(500, 5000);
  RunBenchmark(1000, 1000);
  RunBenchmark(1000, 5000);

  cout << endl;

  // TODO: Benchmark lock of ClientRequestStates too.

  return 0;
}
