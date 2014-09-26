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

#include "runtime/parallel-executor.h"

#include <boost/thread/thread.hpp>

#include "util/stopwatch.h"
#include "util/thread.h"

using namespace boost;
using namespace impala;
using namespace std;

Status ParallelExecutor::Exec(Function function, void** args, int num_args,
    StatsMetric<double>* latencies) {
  Status status;
  ThreadGroup worker_threads;
  mutex lock;

  for (int i = 0; i < num_args; ++i) {
    stringstream ss;
    ss << "worker-thread(" << i << ")";
    worker_threads.AddThread(new Thread("parallel-executor", ss.str(),
        &ParallelExecutor::Worker, function, args[i], &lock, &status, latencies));
  }
  worker_threads.JoinAll();

  return status;
}

void ParallelExecutor::Worker(Function function, void* arg, mutex* lock, Status* status,
    StatsMetric<double>* latencies) {
  MonotonicStopWatch sw;
  if (latencies != NULL) sw.Start();
  Status local_status = function(arg);
  if (!local_status.ok()) {
    unique_lock<mutex> l(*lock);
    if (status->ok()) *status = local_status;
  }

  if (latencies != NULL) {
    latencies->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  }
}
