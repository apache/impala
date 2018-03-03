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
#include <queue>
#include <boost/thread/locks.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "runtime/io/request-context.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/mem-tracker.h"
#include "runtime/thread-resource-mgr.h"
#include "util/condition-variable.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"

/// This file contains internal structures shared between submodules of the IoMgr. Users
/// of the IoMgr do not need to include this file.
namespace impala {
namespace io {

/// Per disk state
struct DiskIoMgr::DiskQueue {
  /// Disk id (0-based)
  int disk_id;

  /// Lock that protects access to 'request_contexts' and 'work_available'
  boost::mutex lock;

  /// Condition variable to signal the disk threads that there is work to do or the
  /// thread should shut down.  A disk thread will be woken up when there is a reader
  /// added to the queue. A reader is only on the queue when it has at least one
  /// scan range that is not blocked on available buffers.
  ConditionVariable work_available;

  /// list of all request contexts that have work queued on this disk
  std::list<RequestContext*> request_contexts;

  /// Enqueue the request context to the disk queue.  The DiskQueue lock must not be taken.
  inline void EnqueueContext(RequestContext* worker) {
    {
      boost::unique_lock<boost::mutex> disk_lock(lock);
      /// Check that the reader is not already on the queue
      DCHECK(find(request_contexts.begin(), request_contexts.end(), worker) ==
          request_contexts.end());
      request_contexts.push_back(worker);
    }
    work_available.NotifyAll();
  }

  DiskQueue(int id) : disk_id(id) {}
};
}
}

#endif
