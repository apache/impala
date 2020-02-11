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

#include <cstdint>
#include <string>

#include "common/atomic.h"

namespace impala {

/// Utility class to update progress. This is split out so a different
/// logging level can be set for these updates (GLOG_module)
/// This class is thread safe after Init() is called.
/// Example usage:
///   ProgressUpdater updater;
///   updater.Init("Task", 100, 10);  // 100 items, print every 10%
///   updater.Update(15);  // 15 done, prints 15%
///   updater.Update(3);   // 18 done, doesn't print
///   update.Update(5);    // 23 done, prints 23%
class ProgressUpdater {
 public:

  ProgressUpdater();

  /// Initialize this ProgressUpdater with:
  /// 'label' - label that is printed with each update.
  /// 'total' - maximum number of work items
  /// 'update_period' - how often the progress is printed expressed as a percentage
  void Init(const std::string& label, int64_t total, int update_period = 1);

  /// Sets the GLOG level for this progress updater. By default, this will use 2 but
  /// objects can override it.
  void set_logging_level(int level) { logging_level_ = level; }

  /// 'delta' more of the work has been complete. Will potentially output to
  /// VLOG_PROGRESS. Init() must be called before Update().
  void Update(int64_t delta);

  /// Returns true if all tasks are done.
  bool done() const { return num_complete() >= total_; }

  int64_t total() const { return total_; }
  int64_t num_complete() const { return num_complete_.Load(); }
  int64_t remaining() const { return total() - num_complete(); }

  /// Returns a string representation of the current progress
  std::string ToString() const;

 private:
  /// Label printed with each update.
  std::string label_;

  /// GLOG level for updates.
  int logging_level_;

  /// Total number of work items. -1 before Init().
  int64_t total_;

  /// Number of percentage points between outputs.
  int update_period_;

  /// Number of completed work items.
  AtomicInt64 num_complete_;

  /// Percentage when the last output was generated.
  AtomicInt32 last_output_percentage_;
};
}
