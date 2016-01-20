// Copyright 2016 Cloudera Inc.
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

#include "util/bloom-filter.h"

#include <stdlib.h>

#include <algorithm>

#include "common/logging.h"
#include "runtime/runtime-state.h"
#include "util/hash-util.h"

namespace impala {

BloomFilter::BloomFilter(const int log_heap_space, RuntimeState* state,
    BufferedBlockMgr::Client* client)
    :  // Since log_heap_space is in bytes, we need to convert it to cache lines. There
       // are 64 = 2^6 bytes in a cache line.
      log_num_buckets_(std::max(1, log_heap_space - 6)),
      // Don't use log_num_buckets_ if it will lead to undefined behavior by a shift
      // that is too large.
      directory_mask_((1ull << std::min(63, log_num_buckets_)) - 1),
      directory_(NULL),
      state_(state),
      client_(client) {
  // Since we use 32 bits in the arguments of Insert() and Find(), log_num_buckets_
  // must be limited.
  DCHECK(log_num_buckets_ <= 32)
      << "Bloom filter too large. log_heap_space: " << log_heap_space;
  DCHECK_EQ(client_ == NULL, state_ == NULL);
  // Each bucket has 64 = 2^6 bytes:
  const size_t alloc_size = 1ull << (log_num_buckets_ + 6);
  if (state_) {
    const bool consume_success = state_->block_mgr()->ConsumeMemory(client_, alloc_size);
    DCHECK(consume_success) << "ConsumeMemory failed. log_heap_space: "
                            << log_heap_space
                            << " log_num_buckets_: " << log_num_buckets_
                            << " alloc_size: " << alloc_size;
  }
  const int malloc_failed =
      posix_memalign(reinterpret_cast<void**>(&directory_), 64, alloc_size);
  DCHECK_EQ(malloc_failed, 0) << "Malloc failed. log_heap_space: " << log_heap_space
                              << " log_num_buckets_: " << log_num_buckets_
                              << " alloc_size: " << alloc_size;
  memset(directory_, 0, alloc_size);
}

BloomFilter::~BloomFilter() {
  if (directory_) {
    if (state_) {
      state_->block_mgr()->ReleaseMemory(client_, 1ll << (log_num_buckets_ + 6));
    }
    free(directory_);
    directory_ = NULL;
  }
}

// The following three methods are derived from
//
// fpp = (1 - exp(-BUCKET_WORDS * ndv/space))^BUCKET_WORDS
//
// where space is in bits.

size_t BloomFilter::MaxNdv(const int log_heap_space, const double fpp) {
  DCHECK(log_heap_space < 61);
  DCHECK(0 < fpp && fpp < 1);
  static const double ik = 1.0 / BUCKET_WORDS;
  return -1 * ik * (1ull << (log_heap_space + 3)) * log(1 - pow(fpp, ik));
}

int BloomFilter::MinLogSpace(const size_t ndv, const double fpp) {
  static const double k = BUCKET_WORDS;
  if (0 == ndv) return 0;
  // m is the number of bits we would need to get the fpp specified
  const double m = -k * ndv/ log(1 - pow(fpp, 1.0 / k));
  return ceil(log2(m/8));
}

double BloomFilter::FalsePositiveProb(const size_t ndv, const int log_heap_space) {
  return pow(
      1 - exp((-1.0 * static_cast<double>(BUCKET_WORDS) * static_cast<double>(ndv)) /
              static_cast<double>(1ull << (log_heap_space + 3))),
      BUCKET_WORDS);
}

}  // namespace impala
