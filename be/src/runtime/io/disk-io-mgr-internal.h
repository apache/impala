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

#ifndef IMPALA_RUNTIME_DISK_IO_MGR_INTERNAL_H
#define IMPALA_RUNTIME_DISK_IO_MGR_INTERNAL_H

#include <unistd.h>
#include <mutex>
#include <queue>

#include "common/logging.h"
#include "runtime/io/request-context.h"
#include "runtime/io/disk-io-mgr.h"
#include "util/condition-variable.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/runtime-profile-counters.h"

/// This file contains internal structures shared between submodules of the IoMgr. Users
/// of the IoMgr do not need to include this file.

DECLARE_uint64(max_cached_file_handles);

// Macros to work around counters sometimes not being provided.
// TODO: fix things so that counters are always non-NULL.
#define COUNTER_ADD_IF_NOT_NULL(c, v) \
  do { \
    ::impala::RuntimeProfile::Counter* __ctr__ = (c); \
    if (__ctr__ != nullptr) __ctr__->Add(v); \
 } while (false);

#define COUNTER_BITOR_IF_NOT_NULL(c, v) \
  do { \
    ::impala::RuntimeProfile::Counter* __ctr__ = (c); \
    if (__ctr__ != nullptr) __ctr__->BitOr(v); \
 } while (false);

namespace impala {
class HistogramMetric;

namespace io {

// Indicates if file handle caching should be used
static inline bool is_file_handle_caching_enabled() {
  return FLAGS_max_cached_file_handles > 0;
}

/// Global queue of requests for a disk. One or more disk threads pull requests off
/// a given queue. RequestContexts are scheduled in round-robin order to provide some
/// level of fairness between RequestContexts.
class DiskQueue {
 public:
  DiskQueue(int disk_id) : disk_id_(disk_id) {}
  // Destructor is only run in backend tests - in a daemon the singleton DiskIoMgr
  // is not destroyed.
  ~DiskQueue();

  /// Disk worker thread loop. This function retrieves the next range to process on
  /// the disk queue and invokes ScanRange::DoRead() or Write() depending on the type
  /// of Range. There can be multiple threads per disk running this loop.
  void DiskThreadLoop(DiskIoMgr* io_mgr);

  /// Enqueue the request context to the disk queue.
  void EnqueueContext(RequestContext* worker) {
    {
      std::unique_lock<std::mutex> disk_lock(lock_);
      // Check that the reader is not already on the queue
      DCHECK(find(request_contexts_.begin(), request_contexts_.end(), worker) ==
          request_contexts_.end());
      request_contexts_.push_back(worker);
    }
    work_available_.NotifyAll();
  }

  /// Signals that disk threads for this queue should stop processing new work and
  /// terminate once done.
  void ShutDown();

  /// Append debug string to 'ss'. Acquires the DiskQueue lock.
  void DebugString(std::stringstream* ss);

  void set_read_latency(HistogramMetric* read_latency) {
    DCHECK(read_latency_ == nullptr);
    read_latency_ = read_latency;
  }
  void set_read_size(HistogramMetric* read_size) {
    DCHECK(read_size_ == nullptr);
    read_size_ = read_size;
  }
  void set_write_latency(HistogramMetric* write_latency) {
    DCHECK(write_latency_ == nullptr);
    write_latency_ = write_latency;
  }
  void set_write_size(HistogramMetric* write_size) {
    DCHECK(write_size_ == nullptr);
    write_size_ = write_size;
  }
  void set_write_io_err(IntCounter* write_io_err) {
    DCHECK(write_io_err_ == nullptr);
    write_io_err_ = write_io_err;
  }

  HistogramMetric* read_latency() const { return read_latency_; }
  HistogramMetric* read_size() const { return read_size_; }
  HistogramMetric* write_latency() const { return write_latency_; }
  HistogramMetric* write_size() const { return write_size_; }
  IntCounter* write_io_err() const { return write_io_err_; }

 private:
  /// Called from the disk thread to get the next range to process. Wait until a scan
  /// is available to process, a write range is available, or 'shut_down_' is set to
  /// true. Returns the range to process and the RequestContext that the range belongs
  /// to. Only returns NULL if the disk thread should be shut down.
  RequestRange* GetNextRequestRange(RequestContext** request_context);

  /// Disk id (0-based)
  const int disk_id_;

  /// Metric that tracks read latency for this queue.
  HistogramMetric* read_latency_ = nullptr;

  /// Metric that tracks read size for this queue.
  HistogramMetric* read_size_ = nullptr;

  /// Metric that tracks write latency for this queue.
  HistogramMetric* write_latency_ = nullptr;

  /// Metric that tracks write size for this queue.
  HistogramMetric* write_size_ = nullptr;

  /// Metric that tracks write io errors for this queue.
  IntCounter* write_io_err_ = nullptr;

  /// Lock that protects below members.
  std::mutex lock_;

  /// Condition variable to signal the disk threads that there is work to do or the
  /// thread should shut down.  A disk thread will be woken up when there is a reader
  /// added to the queue. A reader is only on the queue when it has at least one
  /// scan range that is not blocked on available buffers.
  ConditionVariable work_available_;

  /// list of all request contexts that have work queued on this disk
  std::list<RequestContext*> request_contexts_;

  /// True if the IoMgr should be torn down. Worker threads check this when dequeueing
  /// from 'request_contexts_' and terminate themselves once it is true. Only used in
  /// backend tests - in a daemon the singleton DiskIOMgr is never shut down.
  bool shut_down_ = false;
};
}
}

#endif
