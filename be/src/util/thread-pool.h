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

#ifndef IMPALA_UTIL_THREAD_POOL_H
#define IMPALA_UTIL_THREAD_POOL_H

#include "util/blocking-queue.h"

#include <boost/thread/mutex.hpp>
#include <boost/bind/mem_fn.hpp>

#include "util/thread.h"

namespace impala {

/// Simple threadpool which processes items (of type T) in parallel which were placed on a
/// blocking queue by Offer(). Each item is processed by a single user-supplied method.
template <typename T>
class ThreadPool {
 public:
  /// Signature of a work-processing function. Takes the integer id of the thread which is
  /// calling it (ids run from 0 to num_threads - 1) and a reference to the item to
  /// process.
  typedef boost::function<void (int thread_id, const T& workitem)> WorkFunction;

  /// Creates a new thread pool and start num_threads threads.
  ///  -- num_threads: how many threads are part of this pool
  ///  -- queue_size: the maximum size of the queue on which work items are offered. If the
  ///     queue exceeds this size, subsequent calls to Offer will block until there is
  ///     capacity available.
  ///  -- work_function: the function to run every time an item is consumed from the queue
  ThreadPool(const std::string& group, const std::string& thread_prefix,
      uint32_t num_threads, uint32_t queue_size, const WorkFunction& work_function)
    : work_function_(work_function),
      work_queue_(queue_size),
      shutdown_(false) {
    for (int i = 0; i < num_threads; ++i) {
      std::stringstream threadname;
      threadname << thread_prefix << "(" << i + 1 << ":" << num_threads << ")";
      threads_.AddThread(new Thread(group, threadname.str(),
          boost::bind<void>(boost::mem_fn(&ThreadPool<T>::WorkerThread), this, i)));
    }
  }

  /// Destructor ensures that all threads are terminated before this object is freed
  /// (otherwise they may continue to run and reference member variables)
  virtual ~ThreadPool() {
    Shutdown();
    Join();
  }

  /// Blocking operation that puts a work item on the queue. If the queue is full, blocks
  /// until there is capacity available.
  //
  /// 'work' is copied into the work queue, but may be referenced at any time in the
  /// future. Therefore the caller needs to ensure that any data referenced by work (if T
  /// is, e.g., a pointer type) remains valid until work has been processed, and it's up to
  /// the caller to provide their own signalling mechanism to detect this (or to wait until
  /// after DrainAndShutdown returns).
  //
  /// Returns true if the work item was successfully added to the queue, false otherwise
  /// (which typically means that the thread pool has already been shut down).
  bool Offer(const T& work) {
    return work_queue_.BlockingPut(work);
  }

  /// Shuts the thread pool down, causing the work queue to cease accepting offered work
  /// and the worker threads to terminate once they have processed their current work item.
  /// Returns once the shutdown flag has been set, does not wait for the threads to
  /// terminate.
  void Shutdown() {
    {
      boost::lock_guard<boost::mutex> l(lock_);
      shutdown_ = true;
    }
    work_queue_.Shutdown();
  }

  /// Blocks until all threads are finished. Shutdown does not need to have been called,
  /// since it may be called on a separate thread.
  void Join() {
    threads_.JoinAll();
  }

  uint32_t GetQueueSize() const {
    return work_queue_.GetSize();
  }

  /// Blocks until the work queue is empty, and then calls Shutdown to stop the worker
  /// threads and Join to wait until they are finished.
  /// Any work Offer()'ed during DrainAndShutdown may or may not be processed.
  void DrainAndShutdown() {
    {
      boost::unique_lock<boost::mutex> l(lock_);
      while (work_queue_.GetSize() != 0) {
        empty_cv_.wait(l);
      }
    }
    Shutdown();
    Join();
  }

  Status AssignToCgroup(const std::string& cgroup) {
    return threads_.SetCgroup(cgroup);
  }

 private:
  /// Driver method for each thread in the pool. Continues to read work from the queue
  /// until the pool is shutdown.
  void WorkerThread(int thread_id) {
    while (!IsShutdown()) {
      T workitem;
      if (work_queue_.BlockingGet(&workitem)) {
        work_function_(thread_id, workitem);
      }
      if (work_queue_.GetSize() == 0) {
        /// Take lock to ensure that DrainAndShutdown() cannot be between checking
        /// GetSize() and wait()'ing when the condition variable is notified.
        /// (It will hang if we notify right before calling wait().)
        boost::unique_lock<boost::mutex> l(lock_);
        empty_cv_.notify_all();
      }
    }
  }

  /// Returns value of shutdown_ under a lock, forcing visibility to threads in the pool.
  bool IsShutdown() {
    boost::lock_guard<boost::mutex> l(lock_);
    return shutdown_;
  }

  /// User-supplied method to call to process each work item.
  WorkFunction work_function_;

  /// Queue on which work items are held until a thread is available to process them in
  /// FIFO order.
  BlockingQueue<T> work_queue_;

  /// Collection of worker threads that process work from the queue.
  ThreadGroup threads_;

  /// Guards shutdown_ and empty_cv_
  boost::mutex lock_;

  /// Set to true when threads should stop doing work and terminate.
  bool shutdown_;

  /// Signalled when the queue becomes empty
  boost::condition_variable empty_cv_;
};

/// Utility thread-pool that accepts callable work items, and simply invokes them.
class CallableThreadPool : public ThreadPool<boost::function<void()> > {
 public:
  CallableThreadPool(const std::string& group, const std::string& thread_prefix,
      uint32_t num_threads, uint32_t queue_size) :
      ThreadPool<boost::function<void()> >(
          group, thread_prefix, num_threads, queue_size, &CallableThreadPool::Worker) {
  }

 private:
  static void Worker(int thread_id, const boost::function<void()>& f) {
    f();
  }
};

}

#endif
