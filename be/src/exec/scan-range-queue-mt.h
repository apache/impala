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
/// earlier. The size of a scan range is the amount of work it stands for, see
/// RangeWeight().
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
        scan_range_queue_.Push({scan_range, RangeWeight(scan_range)});
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
      ret = scan_range_queue_.Pop().range;
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

  /// Returns the number of bytes that the instance that processes 'range' will read.
  /// This is the length of the range, except for the footer ranges of Parquet and ORC
  /// files: such a footer range stands for the split it was issued for, so the length of
  /// that split is returned. Defined in hdfs-scan-node-base.cc because it needs
  /// ScanRangeMetadata.
  static int64_t RangeWeight(const io::ScanRange* range);

 private:
  DISALLOW_COPY_AND_ASSIGN(ScanRangeQueueMt);

  struct Entry {
    io::ScanRange* range;
    /// RangeWeight() of 'range', computed once when the range is enqueued.
    int64_t weight;
  };

  struct ScanRangeComparator {
  public:
    bool Less(const Entry& lhs, const Entry& rhs) const {
      DCHECK(lhs.range != nullptr);
      DCHECK(rhs.range != nullptr);
      if (!lhs.range->UseHdfsCache() && rhs.range->UseHdfsCache()) return true;
      if (lhs.range->UseHdfsCache() && !rhs.range->UseHdfsCache()) return false;
      return lhs.weight < rhs.weight;
    }
  };

  std::mutex scan_range_queue_lock_;
  std::queue<io::ScanRange*> high_prio_scan_ranges_;
  ScanRangeComparator scan_range_compare_;
  PriorityQueue<Entry, ScanRangeComparator> scan_range_queue_{scan_range_compare_};
};

}
