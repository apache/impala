// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_PROGRESS_UPDATER_H
#define IMPALA_UTIL_PROGRESS_UPDATER_H

#include <string>
#include <boost/cstdint.hpp>

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

 private:
  std::string label_;
  int logging_level_;
  int64_t total_;
  int update_period_;
  int64_t num_complete_;
  int last_output_percentage_;
};

}

#endif
