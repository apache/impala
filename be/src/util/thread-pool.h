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

#pragma once

#include <mutex>

#include <boost/bind/mem_fn.hpp>

#include "util/aligned-new.h"
#include "util/blocking-queue.h"
#include "util/condition-variable.h"
#include "util/thread.h"

namespace impala {

/// Simple threadpool which processes items (of type T) in parallel which were placed on a
/// blocking queue by Offer(). Each item is processed by a single user-supplied method.
template <typename T>
class ThreadPool : public CacheLineAligned {
 public:
  /// Signature of a work-processing function. Takes the integer id of the thread which is
  /// calling it (ids run from 0 to num_threads - 1) and a reference to the item to
  /// process.
  typedef boost::function<void (int thread_id, const T& workitem)> WorkFunction;

  /// Creates a new thread pool without starting any threads. Code must call
  /// Init() on this thread pool before any calls to Offer().
  ///  -- num_threads: how many threads are part of this pool
  ///  -- queue_size: the maximum size of the queue on which work items are offered. If the
  ///     queue exceeds this size, subsequent calls to Offer will block until there is
  ///     capacity available.
  ///  -- work_function: the function to run every time an item is consumed from the queue
  ///  -- fault_injection_eligible - If set to true, allow fault injection at this
  ///     callsite (see thread_creation_fault_injection). If set to false, fault
  ///     injection is diabled at this callsite. Thread creation sites that crash
  ///     Impala or abort startup must have this set to false.
  ThreadPool(const std::string& group, const std::string& thread_prefix,
      uint32_t num_threads, uint32_t queue_size, const WorkFunction& work_function,
      bool fault_injection_eligible = false)
    : group_(group), thread_prefix_(thread_prefix), num_threads_(num_threads),
      work_function_(work_function), work_queue_(queue_size),
      fault_injection_eligible_(fault_injection_eligible) {}

  /// Destructor ensures that all threads are terminated before this object is freed
  /// (otherwise they may continue to run and reference member variables)
  virtual ~ThreadPool() {
    Shutdown();
    Join();
  }

  /// Create the threads needed for this ThreadPool. Returns an error on any
  /// error spawning the threads.
  Status Init() {
    for (int i = 0; i < num_threads_; ++i) {
      std::stringstream threadname;
      threadname << thread_prefix_ << "(" << i + 1 << ":" << num_threads_ << ")";
      std::unique_ptr<Thread> t;
      Status status = Thread::Create(group_, threadname.str(),
          boost::bind<void>(boost::mem_fn(&ThreadPool<T>::WorkerThread), this, i), &t,
          fault_injection_eligible_);
      if (!status.ok()) {
        // The thread pool initialization failed. Shutdown any threads that were
        // spawned. Note: Shutdown() and Join() are safe to call multiple times.
        Shutdown();
        Join();
        return status;
      }
      threads_.AddThread(std::move(t));
    }
    initialized_ = true;
    return Status::OK();
  }

  /// Blocking operation that puts a work item on the queue. If the queue is full, blocks
  /// until there is capacity available. The ThreadPool must be initialized before
  /// calling this method.
  //
  /// 'work' is copied into the work queue, but may be referenced at any time in the
  /// future. Therefore the caller needs to ensure that any data referenced by work (if T
  /// is, e.g., a pointer type) remains valid until work has been processed, and it's up to
  /// the caller to provide their own signalling mechanism to detect this (or to wait until
  /// after DrainAndShutdown returns).
  //
  /// Returns true if the work item was successfully added to the queue, false otherwise
  /// (which typically means that the thread pool has already been shut down).
  template <typename V>
  bool Offer(V&& work) {
    DCHECK(initialized_);
    return work_queue_.BlockingPut(std::forward<V>(work));
  }

  /// Blocks until the work item is placed on the queue or the timeout expires. The
  /// ThreadPool must be initialized before calling this method. The same requirements
  /// about the lifetime of 'work' applies as in Offer() above. If the operation times
  /// out, the work item can be safely freed.
  ///
  /// Returns true if the work item was successfully added to the queue, false otherwise
  /// (which means the operation timed out or the thread pool has already been shut down).
  template <typename V>
  bool Offer(V&& work, int64_t timeout_millis) {
    DCHECK(initialized_);
    int64_t timeout_micros = timeout_millis * MICROS_PER_MILLI;
    return work_queue_.BlockingPutWithTimeout(std::forward<V>(work), timeout_micros);
  }

  /// Shuts the thread pool down, causing the work queue to cease accepting offered work
  /// and the worker threads to terminate once they have processed their current work item.
  /// Returns once the shutdown flag has been set, does not wait for the threads to
  /// terminate.
  void Shutdown() {
    {
      std::lock_guard<std::mutex> l(lock_);
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
    return work_queue_.Size();
  }

  /// Blocks until the work queue is empty, and then calls Shutdown to stop the worker
  /// threads and Join to wait until they are finished.
  /// Any work Offer()'ed during DrainAndShutdown may or may not be processed.
  void DrainAndShutdown() {
    {
      std::unique_lock<std::mutex> l(lock_);
      // If the ThreadPool is not initialized, then the queue must be empty.
      DCHECK(initialized_ || work_queue_.Size() == 0);
      while (work_queue_.Size() != 0) {
        empty_cv_.Wait(l);
      }
    }
    Shutdown();
    Join();
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
      if (work_queue_.Size() == 0) {
        /// Take lock to ensure that DrainAndShutdown() cannot be between checking
        /// GetSize() and wait()'ing when the condition variable is notified.
        /// (It will hang if we notify right before calling wait().)
        std::unique_lock<std::mutex> l(lock_);
        empty_cv_.NotifyAll();
      }
    }
  }

  /// Returns value of shutdown_ under a lock, forcing visibility to threads in the pool.
  bool IsShutdown() {
    std::lock_guard<std::mutex> l(lock_);
    return shutdown_;
  }

  /// Group string to tag threads for this pool
  const std::string group_;

  /// Thread name prefix
  const std::string thread_prefix_;

  /// The number of threads to start in this pool
  uint32_t num_threads_;

  /// User-supplied method to call to process each work item.
  WorkFunction work_function_;

  /// Queue on which work items are held until a thread is available to process them in
  /// FIFO order.
  BlockingQueue<T> work_queue_;

  /// Whether this ThreadPool will tolerate failure by aborting a query. This means
  /// it is safe to inject errors for Init().
  bool fault_injection_eligible_;

  /// Collection of worker threads that process work from the queue.
  ThreadGroup threads_;

  /// Guards shutdown_ and empty_cv_
  std::mutex lock_;

  /// Set to true when Init() has finished spawning the threads.
  bool initialized_ = false;

  /// Set to true when threads should stop doing work and terminate.
  bool shutdown_ = false;

  /// Signalled when the queue becomes empty
  ConditionVariable empty_cv_;
};

/// Utility thread-pool that accepts callable work items, and simply invokes them.
class CallableThreadPool : public ThreadPool<boost::function<void()>> {
 public:
  CallableThreadPool(const std::string& group, const std::string& thread_prefix,
      uint32_t num_threads, uint32_t queue_size) :
      ThreadPool<boost::function<void()>>(
          group, thread_prefix, num_threads, queue_size, &CallableThreadPool::Worker) {
  }

 private:
  static void Worker(int thread_id, const boost::function<void()>& f) {
    f();
  }
};

/// Parent class for all synchronous work items
///
/// Important note for all subclasses:
/// All fields need to have a lifetime that matches this operation's lifetime.
/// In particular, caller-provided arguments need to be moved, copied, or need to have a
/// lifetime independent of the caller. The monitored operation must survive the caller
/// timing out and potentially deallocating memory.
class SynchronousWorkItem {
 public:
  virtual ~SynchronousWorkItem() {}

  /// Customized implementation for each operation. Subclasses must override this.
  /// The status is conveyed back to the original caller.
  virtual Status Execute() {
    DCHECK(false) << "Execute() must be implemented";
    return Status("Execute() must be implemented");
  }

  virtual std::string GetDescription() {
    DCHECK(false) << "GetDescription() must be implemented";
    return "";
  }

 private:
  friend class SynchronousThreadPool;

  /// This is called by the worker thread and handles the mechanics of notifying
  /// the caller and conveying status when Execute() completes.
  void WorkerExecute() {
    Status status = Execute();
    DCHECK(!done_promise_.IsSet());
    discard_result(done_promise_.Set(status));
  }

  /// Wait for the operation to complete or time out with the specified limit
  /// 'timeout_millis' given that the caller has already waited 'time_used_millis'.
  /// If the operation times out, it returns TErrorCode::THREAD_POOL_TASK_TIMED_OUT.
  /// Otherwise, it returns the status returned by Execute().
  Status Wait(int64_t timeout_millis, int64_t time_used_millis) {
    // If the time used has already exceeded the timeout, go directly to returning
    // THREAD_POOL_TASK_TIMED_OUT.
    bool timed_out = (time_used_millis >= timeout_millis);
    Status status;
    if (!timed_out) {
      int64_t timeout_left_millis = timeout_millis - time_used_millis;
      status = done_promise_.Get(timeout_left_millis, &timed_out);
    }
    if (timed_out) {
      // IMPALA-7946: Always throw an error using the original timeout, not the
      // timeout remaining.
      Status timeout_status = Status(TErrorCode::THREAD_POOL_TASK_TIMED_OUT,
          GetDescription(), timeout_millis / MILLIS_PER_SEC);
      LOG(WARNING) << timeout_status.GetDetail();
      return timeout_status;
    }
    return status;
  }

  // Set to the return status of ExecuteImpl() upon completion
  Promise<Status> done_promise_;
};

/// Synchronous thread pool can run any subclass of SynchronousWorkItem
/// Ownership is shared between the caller side and the worker side:
/// 1. The caller accesses the operation to wait for its completion (or timeout) and
///    retrieve any result.
/// 2. The ThreadPool worker calls Execute() on the operation and needs to maintain
///    ownership until the operation completes. The blocking queue inside the ThreadPool
///    also needs to maintain ownership until a thread removes the operation for
///    processing.
/// This is an awkward circumstance to have exclusive ownership. The caller can time
/// out and leave while the worker is processing. When the worker completes and could
/// release ownership, the caller might still need to retrieve the result or the caller
/// might be gone. shared_ptr does what we want: the HdfsMonitorOp will survive until
/// neither thread needs it anymore.
class SynchronousThreadPool : public ThreadPool<std::shared_ptr<SynchronousWorkItem>> {
 public:
  SynchronousThreadPool(const std::string& group, const std::string& thread_prefix,
      uint32_t num_threads, uint32_t queue_size) :
    ThreadPool<std::shared_ptr<SynchronousWorkItem>>(group, thread_prefix, num_threads,
        queue_size, &SynchronousThreadPool::Worker) {}

  /// Run the provided work item and wait up to 'timeout_milliseconds' for the
  /// operation to complete. If it completes, return the status from the work
  /// item's Execute() function. Otherwise, return an error status:
  ///  - THREAD_POOL_TASK_TIMED_OUT if the individual task timed out
  ///  - THREAD_POOL_SUBMIT_FAILED if all the threads are busy and the task did not
  ///    even start
  Status SynchronousOffer(std::shared_ptr<SynchronousWorkItem> work,
      int64_t timeout_milliseconds) {
    MonotonicStopWatch offer_timer;
    offer_timer.Start();
    bool offer_success = Offer(work, timeout_milliseconds);
    offer_timer.Stop();
    if (!offer_success) {
      // This scenario only happens when all threads are occupied and the queue
      // is full. This means the system is in a catastrophic state. Log to ERROR.
      Status failed_to_submit_status =
        Status(TErrorCode::THREAD_POOL_SUBMIT_FAILED, work->GetDescription(),
               timeout_milliseconds / MILLIS_PER_SEC);
      LOG(ERROR) << failed_to_submit_status.GetDetail();
      return failed_to_submit_status;
    }

    int64_t time_used_millis =
        offer_timer.ElapsedTime() / (NANOS_PER_MICRO * MICROS_PER_MILLI);
    return work->Wait(timeout_milliseconds, time_used_millis);
  }

 private:
  static void Worker(int thread_id, const std::shared_ptr<SynchronousWorkItem>& work) {
    work->WorkerExecute();
  }
};
}
