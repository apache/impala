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
#include <queue>
#include <vector>

#include "runtime/io/request-ranges.h"
#include "util/priority-queue.h"

namespace impala {

/// Queue of all scan ranges that need to be read. Shared by all instances of a
/// fragment. Only used for MT scans where the scan ranges are dynamically assigned
/// to the fragment instances using this queue. The scan ranges in this queue are
/// ordered from largest to smallest, i.e. implementing a Longest-Processing Time
/// scheduling. Exceptions are scan ranges that use HDFS caching, they are prioritized
/// earlier.
/// It's also possible to add high prio scan ranges, they are scheduled before
/// the non-high prio scan ranges.
class ScanRangeQueueMt {
 public:
  ScanRangeQueueMt() = default;

  /// Adds all scan ranges to the queue. If 'high_prio' is true we add it to a separate
  /// list of prioritized items.
  void EnqueueRanges(const std::vector<io::ScanRange*>& ranges, bool high_prio) {
    std::lock_guard<std::mutex> lock(scan_range_queue_lock_);
    if (LIKELY(!high_prio)) {
      for (io::ScanRange* scan_range : ranges) {
        scan_range_queue_.Push(scan_range);
      }
    } else {
      for (io::ScanRange* scan_range : ranges) {
        high_prio_scan_ranges_.push(scan_range);
      }
    }
  }

  /// Returns the next scan range from the queue. Returns nullptr if the queue is empty.
  io::ScanRange* Dequeue() {
    io::ScanRange* ret = nullptr;
    std::lock_guard<std::mutex> lock(scan_range_queue_lock_);
    if (UNLIKELY(!high_prio_scan_ranges_.empty())) {
      ret = high_prio_scan_ranges_.front();
      high_prio_scan_ranges_.pop();
    } else if (!scan_range_queue_.Empty()) {
      ret = scan_range_queue_.Pop();
    }
    return ret;
  }

  /// Returns true if the scan range queue is empty.
  bool Empty() {
    std::lock_guard<std::mutex> lock(scan_range_queue_lock_);
    return high_prio_scan_ranges_.empty() && scan_range_queue_.Empty();
  }

  /// Reserves capacity for the queue. This doesn't affect the queue of the high prio
  /// items.
  void Reserve(int64_t capacity) {
    std::lock_guard<std::mutex> lock(scan_range_queue_lock_);
    scan_range_queue_.Reserve(capacity);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(ScanRangeQueueMt);

  struct ScanRangeComparator {
  public:
    bool Less(const io::ScanRange* lhs, const io::ScanRange* rhs) const {
      DCHECK(lhs != nullptr);
      DCHECK(rhs != nullptr);
      if (!lhs->UseHdfsCache() && rhs->UseHdfsCache()) return true;
      if (lhs->UseHdfsCache() && !rhs->UseHdfsCache()) return false;
      return lhs->bytes_to_read() < rhs->bytes_to_read();
    }
  };

  std::mutex scan_range_queue_lock_;
  std::queue<io::ScanRange*> high_prio_scan_ranges_;
  ScanRangeComparator scan_range_compare_;
  PriorityQueue<io::ScanRange*, ScanRangeComparator> scan_range_queue_{
      scan_range_compare_};
};

}
