// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_PARALLEL_EXECUTOR_H
#define IMPALA_RUNTIME_PARALLEL_EXECUTOR_H

#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>
#include "common/status.h"

namespace impala {

// This is a class that executes multiple functions in parallel with different arguments 
// using a thread pool.  
// TODO: look into an API for this.  Boost has one that is in review but not yet official.
// TODO: use a shared pool?  Thread creation is pretty cheap so this might not be 
// worth it
class ParallelExecutor {
 public:
  // Typedef for the underlying function for the work.
  // The function must be thread safe.  
  // The function must return a Status indicating if it was successful or not.
  // An example of how this function should be defined would be:
  //    static Status Foo::IssueRpc(void* arg);
  // TODO: there might some magical template way to do this with boost that is more
  // type safe.
  typedef boost::function<Status (void* arg)> Function;

  // Calls function(args[i]) num_args times in parallel using num_args threads. 
  // If any of the work item fails, returns the Status of the first failed work item.
  // Otherwise, returns Status::OK when all work items have been executed.
  static Status Exec(Function function, void** args, int num_args);

 private:
  // Worker thread function which calls function(arg).  This function updates
  // *status taking *lock to synchronize results from different threads.
  static void Worker(Function function, void* arg, boost::mutex* lock, Status* status);
};

}

#endif
