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

#include <memory>
#include <unordered_map>
#include <vector>

#include <boost/thread/pthread/shared_mutex.hpp>

namespace impala {
class MemTracker;
class ScanNode;

/// Class to keep track of scanner threads for a query on the current backend.
/// Tracks the number of threads and their expected memory consumption
/// for the purpose of limiting the aggregate memory consumption of scanner threads.
/// This allows us to implement some heuristics that help prevent us going over the
/// query memory limit by creating too many scanner threads.
/// TODO: this can be removed once all scan nodes are switched to the MT versions.
class ScannerMemLimiter {
 public:
  ScannerMemLimiter();
  ~ScannerMemLimiter();

  /// Register a scan and track the estimated memory 'estimated_initial_thread_mem'
  /// for the initial threads. 'node' must live as long as this object is in use
  /// (i.e. as long as the below methods are being called). Each 'node' can only
  /// be registered once.
  void RegisterScan(ScanNode* node, int64_t estimated_initial_thread_mem);

  /// Returns true if there is enough memory available to create a scanner thread
  /// for node 'node_id' that is estimated to consume 'estimated_thread_mem' bytes
  /// of memory. Updates the state of the limiter to reflect the increased memory.
  /// The caller must call ReleaseMemoryForScannerThread() for every successful
  /// call to this function.
  bool ClaimMemoryForScannerThread(ScanNode* node, int64_t estimated_thread_mem);

  /// Must be called when a scanner thread exits. Releases the memory accounted for
  /// by ClaimMemoryForScannerThread().
  void ReleaseMemoryForScannerThread(ScanNode* node, int64_t estimated_thread_mem);

 private:
  struct RegisteredScan;

  /// Protects below data structures.
  boost::shared_mutex registered_scans_lock_;

  /// All of this query's scan nodes. The scan nodes register themselves in their
  /// Prepare() phase. Threads must hold 'registered_scans_lock_' in shared mode
  /// when reading or in exclusive mode when modifying. The total memory currently
  /// consumed by the scans is calculated by iterating over this map, which is
  /// O(n) in the number of scans. With an unlimited number of thread tokens,
  /// this would mean we do O(n^2) work per query, but in practice if there are a large
  /// number of concurrent scans there will be no available thread tokens and
  /// ClaimMemoryForScannerThread() will not be called.
  std::unordered_map<ScanNode*, std::unique_ptr<RegisteredScan>> registered_scans_;
};
}
