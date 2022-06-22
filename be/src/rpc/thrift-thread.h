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

#ifndef IMPALA_RPC_THRIFT_THREAD_H
#define IMPALA_RPC_THRIFT_THREAD_H

#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/ThreadFactory.h>

#include "common/logging.h"
#include "util/thread.h"
#include "util/promise.h"

/// This file adds an implementation of Thrift's ThreadFactory and Thread that uses
/// Impala's Thread wrapper class, so that all threads are accounted for by Impala's
/// ThreadManager.
namespace impala {

/// The ThriftThreadFactory is responsible for creating new ThriftThreads to run server
/// tasks on.
class ThriftThreadFactory : public apache::thrift::concurrency::ThreadFactory {
 public:
  /// Group is the thread group for new threads to be assigned to, and prefix is the
  /// per-thread prefix (threads are named "prefix-<count_>-<tid>").
  ThriftThreadFactory(const std::string& group, const std::string& prefix)
      : ThreadFactory(/*detached=*/false), group_(group), prefix_(prefix), count_(-1) { }

  /// (From ThreadFactory) - creates a new ThriftThread to run the supplied Runnable.
  virtual std::shared_ptr<apache::thrift::concurrency::Thread> newThread(
      std::shared_ptr<apache::thrift::concurrency::Runnable> runnable) const;

  /// (From ThreadFactory) - returns the *current* thread ID, i.e. the ID of the executing
  /// thread (which may not have been created by this factory).
  virtual apache::thrift::concurrency::Thread::id_t getCurrentThreadId() const;

 private:
  /// Group name for the Impala ThreadManager
  std::string group_;

  /// Thread name prefix for the Impala ThreadManager
  std::string prefix_;

  /// Marked mutable because we want to increment it inside newThread, which for some
  /// reason is const.
  mutable AtomicInt64 count_;
};

/// A ThriftThread is a Thrift-compatible wrapper for Impala's Thread class, so that all
/// server threads are registered with the global ThreadManager.
class ThriftThread : public apache::thrift::concurrency::Thread {
 public:
  ThriftThread(const std::string& group, const std::string& name, bool detached,
      std::shared_ptr<apache::thrift::concurrency::Runnable> runnable);

  /// (From Thread) - starts execution of the runnable in a separate thread, returning once
  /// execution has begun.
  virtual void start();

  /// Joins the separate thread
  virtual void join();

  /// Returns the Thrift thread ID of the execution thread
  virtual id_t getId();

  virtual ~ThriftThread() { }

 private:
  /// Method executed on impala_thread_. Runs the Runnable once promise has been set to the
  /// current thread ID. The runnable parameter is a shared_ptr so that is always valid
  /// even after the ThriftThread may have been terminated.
  void RunRunnable(std::shared_ptr<apache::thrift::concurrency::Runnable> runnable,
      Promise<apache::thrift::concurrency::Thread::id_t>* promise);

  /// Impala thread that runs the runnable and registers itself with the global
  /// ThreadManager.
  std::unique_ptr<impala::Thread> impala_thread_;

  /// Thrift thread ID, set by RunRunnable.
  apache::thrift::concurrency::Thread::id_t tid_;

  /// Group name for the Impala ThreadManager
  std::string group_;

  /// Individual thread name for the Impala ThreadManager
  std::string name_;
};

}

#endif
