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


#ifndef IMPALA_UTIL_PROGRESS_UPDATER_H
#define IMPALA_UTIL_PROGRESS_UPDATER_H

#include <string>
#include <boost/cstdint.hpp>

#include "common/atomic.h"

namespace impala {

// Utility class to update progress.  This is split out so a different
// logging level can be set for these updates (GLOG_module)
// This class is thread safe.
// Example usage:
//   ProgressUpdater updater("Task", 100, 10);  // 100 items, print every 10%
//   updater.Update(15);  // 15 done, prints 15%
//   updater.Update(3);   // 18 done, doesn't print
//   update.Update(5);    // 23 done, prints 23%
class ProgressUpdater {
 public:
  // label - label that is printed with each update.
  // max - maximum number of work items
  // update_period - how often the progress is spewed expressed as a percentage
  ProgressUpdater(const std::string& label, int64_t max, int update_period = 1);

  ProgressUpdater();

  // Sets the GLOG level for this progress updater.  By default, this will use
  // 2 but objects can override it.
  void set_logging_level(int level) { logging_level_ = level; }

  // 'delta' more of the work has been complete.  Will potentially output to
  // VLOG_PROGRESS
  void Update(int64_t delta);

  // Returns if all tasks are done.
  bool done() const { return num_complete_ >= total_; }

  int64_t total() const { return total_; }
  int64_t num_complete() const { return num_complete_; }
  int64_t remaining() const { return total() - num_complete(); }

  // Returns a string representation of the current progress
  std::string ToString() const;

 private:
  std::string label_;
  int logging_level_;
  int64_t total_;
  int update_period_;

  AtomicInt<int64_t> num_complete_;
  int last_output_percentage_;
};

}

#endif
