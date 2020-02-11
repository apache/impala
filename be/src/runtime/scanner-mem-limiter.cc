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

#include "runtime/scanner-mem-limiter.h"

#include <mutex>

#include "exec/scan-node.h"
#include "runtime/mem-tracker.h"

#include "common/names.h"

namespace impala {
struct ScannerMemLimiter::RegisteredScan {
  RegisteredScan(int64_t estimated_initial_thread_mem)
    : estimated_mem(estimated_initial_thread_mem), num_threads(1) {}

  /// The estimated amount of memory to run all threads that have already been started.
  /// Updated by ClaimMemoryForScannerThread().
  AtomicInt64 estimated_mem;

  /// The number of threads active in the scan node. Updated by
  /// ClaimMemoryForScannerThread().
  AtomicInt64 num_threads;
};

ScannerMemLimiter::ScannerMemLimiter() {}

ScannerMemLimiter::~ScannerMemLimiter() {
  for (const auto& element : registered_scans_) {
    const unique_ptr<RegisteredScan>& scan = element.second;
    DCHECK_EQ(0, scan->estimated_mem.Load());
    DCHECK_EQ(0, scan->num_threads.Load());
  }
}

void ScannerMemLimiter::RegisterScan(
    ScanNode* node, int64_t estimated_initial_thread_mem) {
  unique_ptr<RegisteredScan> scan(new RegisteredScan(estimated_initial_thread_mem));
  lock_guard<shared_mutex> write_lock(registered_scans_lock_);
  bool added = registered_scans_.emplace(node, move(scan)).second;
  DCHECK(added) << node->DebugString();
}

bool ScannerMemLimiter::ClaimMemoryForScannerThread(
    ScanNode* node, int64_t estimated_thread_mem) {
  shared_lock<shared_mutex> read_lock(registered_scans_lock_);
  RegisteredScan* found_scan = nullptr;
  // Calculate the memory consumption in excess of the current consumption that we expect
  // from already-started threads plus the new thread. We need to compute the global
  // total across all scans because multiple scans can compete for the same memory.
  int64_t addtl_consumption = 0;
  for (const auto& element : registered_scans_) {
    const unique_ptr<RegisteredScan>& scan = element.second;
    int64_t consumption = element.first->mem_tracker()->consumption();
    int64_t num_threads = scan->num_threads.Load();
    int64_t estimated_mem = scan->estimated_mem.Load();
    if (consumption > estimated_mem) {
      // Memory exceeded our estimate. Use a crude heuristic of guessing that the scan
      // will use up to 50% more memory. This is carried over from old versions of the
      // code pre-IMPALA-4835, which were initially added in the commit titled
      // "Dynamically scale down mem usage in scanners and io mgr."
      if (node == element.first) {
        // Add consumption for the new thread.
        addtl_consumption += static_cast<int64_t>((consumption * 1.5) / num_threads);
      }
      // We guess that consumption of existing threads will grow up to 50% above the
      // current consumption.
      addtl_consumption += static_cast<int64_t>(consumption * 0.5);
    } else {
      // The scan hasn't used all the estimated memory yet - make sure that that is
      // accounted for.
      addtl_consumption += estimated_mem - consumption;
      if (node == element.first) addtl_consumption += estimated_thread_mem;
    }
    if (node == element.first) found_scan = scan.get();
  }
  DCHECK(found_scan != nullptr) << "Increase mem on unregistered scan";
  // Check if we have capacity for the expected increase in consumption.
  if (addtl_consumption >= node->mem_tracker()->SpareCapacity(MemLimit::SOFT)) {
    return false;
  }
  // There is enough memory - update the estimated memory with the estimate.
  found_scan->estimated_mem.Add(estimated_thread_mem);
  found_scan->num_threads.Add(1);
  return true;
}

void ScannerMemLimiter::ReleaseMemoryForScannerThread(
    ScanNode* node, int64_t estimated_thread_mem) {
  shared_lock<shared_mutex> read_lock(registered_scans_lock_);
  auto it = registered_scans_.find(node);
  DCHECK(it != registered_scans_.end()) << node->id() << " not found.";
  RegisteredScan* scan = it->second.get();
  int64_t mem = scan->estimated_mem.Add(-estimated_thread_mem);
  DCHECK_GE(mem, 0);
  int64_t num_threads = scan->num_threads.Add(-1);
  DCHECK_GE(num_threads, 0);
}
}
