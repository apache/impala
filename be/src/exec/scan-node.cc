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
const string ScanNode::READ_TIMER = "ScannerThreadsReadTime";
const string ScanNode::TOTAL_THROUGHPUT_COUNTER = "TotalReadThroughput";
const string ScanNode::MATERIALIZE_TUPLE_TIMER = "MaterializeTupleTime";
const string ScanNode::PER_THREAD_THROUGHPUT_COUNTER = "PerDiskReadThroughput";
const string ScanNode::SCAN_RANGES_COMPLETE_COUNTER = "ScanRangesComplete";

Status ScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  
  bytes_read_counter_ =
      ADD_COUNTER(runtime_profile(), BYTES_READ_COUNTER, TCounterType::BYTES);
  read_timer_ =
      ADD_COUNTER(runtime_profile(), READ_TIMER, TCounterType::CPU_TICKS);
  total_throughput_counter_ = runtime_profile()->AddRateCounter(
      TOTAL_THROUGHPUT_COUNTER, bytes_read_counter_);
  materialize_tuple_timer_ =
      ADD_COUNTER(runtime_profile(), MATERIALIZE_TUPLE_TIMER, TCounterType::CPU_TICKS);
  per_thread_throughput_counter_ = runtime_profile()->AddDerivedCounter(
       PER_THREAD_THROUGHPUT_COUNTER, TCounterType::BYTES_PER_SECOND,
       bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_read_counter_, read_timer_));
  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TCounterType::UNIT);

  return Status::OK;
}

}

