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

#ifndef IMPALA_RUNTIME_BUFFER_POOL_COUNTERS_H
#define IMPALA_RUNTIME_BUFFER_POOL_COUNTERS_H

#include "util/runtime-profile.h"

namespace impala {

/// A set of counters for each buffer pool client.
struct BufferPoolClientCounters {
 public:
  /// Amount of time spent trying to get a buffer.
  RuntimeProfile::Counter* get_buffer_time;

  /// Amount of time spent waiting for reads from disk to complete.
  RuntimeProfile::Counter* read_wait_time;

  /// Amount of time spent waiting for writes to disk to complete.
  RuntimeProfile::Counter* write_wait_time;

  /// The peak total size of unpinned buffers.
  RuntimeProfile::HighWaterMarkCounter* peak_unpinned_bytes;

  /// The total bytes of data unpinned. Every time a page's pin count goes from 1 to 0,
  /// this counter is incremented by the page size.
  RuntimeProfile::Counter* total_unpinned_bytes;
};

}

#endif
