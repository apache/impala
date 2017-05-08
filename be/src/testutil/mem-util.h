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

#ifndef IMPALA_TESTUTIL_MEM_UTIL_H_
#define IMPALA_TESTUTIL_MEM_UTIL_H_

#include <cstdint>
#include <cstdlib>

#include <glog/logging.h>

#include "gutil/macros.h"

namespace impala {

/// Allocate 64-byte (an x86-64 cache line) aligned memory so it does not straddle cache
/// line boundaries. This is sufficient to meet alignment requirements for all SIMD
/// instructions, at least up to AVX-512.
/// Exits process if allocation fails so should be used for tests and benchmarks only.
inline uint8_t* AllocateAligned(size_t size) {
  void* ptr;
  if (posix_memalign(&ptr, 64, size) != 0) {
    LOG(FATAL) << "Failed to allocate " << size;
  }
  DCHECK(ptr != nullptr);
  return reinterpret_cast<uint8_t*>(ptr);
}

/// Scoped allocation with 64-bit alignment.
/// Exits process if allocation fails so should be used for tests and benchmarks only.
class AlignedAllocation {
 public:
  AlignedAllocation(size_t bytes) : data_(AllocateAligned(bytes)) {}
  ~AlignedAllocation() { free(data_); }

  uint8_t* data() { return data_; }
 private:
  DISALLOW_COPY_AND_ASSIGN(AlignedAllocation);

  uint8_t* data_;
};

}

#endif

