// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/parallel-executor.h"

#include <boost/thread/thread.hpp>

using namespace boost;
using namespace impala;
using namespace std;

Status ParallelExecutor::Exec(Function function, void** args, int num_args) {
  Status status;
  thread_group worker_threads;
  mutex lock;

  for (int i = 0; i < num_args; ++i) {
    worker_threads.add_thread(new thread(&ParallelExecutor::Worker, function, 
            args[i], &lock, &status));
  }
  worker_threads.join_all();

  return status;
}

void ParallelExecutor::Worker(Function function, void* arg, mutex* lock, Status* status) {
  Status local_status = function(arg);
  if (!local_status.ok()) {
    unique_lock<mutex> l(*lock);
    if (status->ok()) *status = local_status;
  }
}


