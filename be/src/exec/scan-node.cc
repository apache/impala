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

#include "exec/scan-node.h"

#include <boost/bind.hpp>

using namespace std;
using namespace boost;

namespace impala {

const string ScanNode::BYTES_READ_COUNTER = "BytesRead";
const string ScanNode::ROWS_READ_COUNTER = "RowsRead";
const string ScanNode::TOTAL_READ_TIMER = "TotalRawHdfsReadTime";
const string ScanNode::TOTAL_THROUGHPUT_COUNTER = "TotalReadThroughput";
const string ScanNode::MATERIALIZE_TUPLE_TIMER = "MaterializeTupleTime";
const string ScanNode::PER_READ_THREAD_THROUGHPUT_COUNTER =
    "PerReadThreadRawHdfsThroughput";
const string ScanNode::NUM_DISKS_ACCESSED_COUNTER = "NumDisksAccessed";
const string ScanNode::SCAN_RANGES_COMPLETE_COUNTER = "ScanRangesComplete";
const string ScanNode::SCANNER_THREAD_COUNTERS_PREFIX = "ScannerThreads";
const string ScanNode::SCANNER_THREAD_TOTAL_WALLCLOCK_TIME =
    "ScannerThreadsTotalWallClockTime";
const string ScanNode::AVERAGE_SCANNER_THREAD_CONCURRENCY =
    "AverageScannerThreadConcurrency";
const string ScanNode::AVERAGE_IO_MGR_QUEUE_CAPACITY =
    "AverageIoMgrQueueCapcity";
const string ScanNode::AVERAGE_IO_MGR_QUEUE_SIZE =
    "AverageIoMgrQueueSize";
const string ScanNode::AVERAGE_HDFS_READ_THREAD_CONCURRENCY =
    "AverageHdfsReadThreadConcurrency";
const string ScanNode::HDFS_READ_THREAD_CONCURRENCY_BUCKET =
    "HdfsReadThreadConcurrencyCountPercentage";

Status ScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  scanner_thread_counters_ =
      ADD_THREAD_COUNTERS(runtime_profile(), SCANNER_THREAD_COUNTERS_PREFIX);
  bytes_read_counter_ =
      ADD_COUNTER(runtime_profile(), BYTES_READ_COUNTER, TCounterType::BYTES);
  rows_read_counter_ =
      ADD_COUNTER(runtime_profile(), ROWS_READ_COUNTER, TCounterType::UNIT);
  read_timer_ = ADD_TIMER(runtime_profile(), TOTAL_READ_TIMER);
  total_throughput_counter_ = runtime_profile()->AddRateCounter(
      TOTAL_THROUGHPUT_COUNTER, bytes_read_counter_);
  materialize_tuple_timer_ = ADD_CHILD_TIMER(runtime_profile(), MATERIALIZE_TUPLE_TIMER,
      SCANNER_THREAD_TOTAL_WALLCLOCK_TIME);
  per_read_thread_throughput_counter_ = runtime_profile()->AddDerivedCounter(
       PER_READ_THREAD_THROUGHPUT_COUNTER, TCounterType::BYTES_PER_SECOND,
       bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_read_counter_, read_timer_));
  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TCounterType::UNIT);
  num_disks_accessed_counter_ =
      ADD_COUNTER(runtime_profile(), NUM_DISKS_ACCESSED_COUNTER, TCounterType::UNIT);

  return Status::OK;
}

}

