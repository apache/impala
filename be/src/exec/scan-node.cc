// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/scan-node.h"

#include <boost/bind.hpp>

using namespace std;
using namespace boost;

namespace impala {

const string ScanNode::BYTES_READ_COUNTER = "BytesRead";
const string ScanNode::READ_TIMER = "ReadTime";
const string ScanNode::THROUGHPUT_COUNTER = "ReadThroughput";
const string ScanNode::MATERIALIZE_TUPLE_TIMER = "MaterializeTupleTime";

Status ScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  bytes_read_counter_ =
      ADD_COUNTER(runtime_profile(), BYTES_READ_COUNTER, TCounterType::BYTES);
  read_timer_ =
      ADD_COUNTER(runtime_profile(), READ_TIMER, TCounterType::CPU_TICKS);
  throughput_counter_ = runtime_profile()->AddDerivedCounter(
      THROUGHPUT_COUNTER, TCounterType::BYTES_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::BytesPerSecond,
                           bytes_read_counter_, read_timer_));
  materialize_tuple_timer_ =
      ADD_COUNTER(runtime_profile(), MATERIALIZE_TUPLE_TIMER, TCounterType::CPU_TICKS);
  return Status::OK;
}

}

