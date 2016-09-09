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

#include <emmintrin.h>
#include <stdlib.h>

#include <glog/logging.h>

#include "tuple-types.h"
#include "common/compiler-util.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/stopwatch.h"

#define STREAMING true

using namespace impala;

namespace impala {

// Tests the throughput of simply partitioning tuples from one stream into many
// with no other processing.
class PartitioningThroughputTest  {
 public:
  // There will be 2^FANOUT_BITS buffers
  static const uint64_t FANOUT_BITS = 6;
  static const uint64_t NUM_BUFFERS = 1<<FANOUT_BITS;
  // How many bytes of data to partition
  static const uint64_t DATA_BYTES = 1<<30; // 2GB
  static const uint64_t DATA_TUPLES = DATA_BYTES / sizeof(ProbeTuple);
  // How many bytes each buffer will hold
  // Twice as much as needed if randomness is perfect.
  static const uint64_t BUFFER_BYTES = DATA_BYTES / NUM_BUFFERS * 2;
  static const uint64_t BUFFER_TUPLES = BUFFER_BYTES / sizeof(ProbeTuple);

  static const int STREAMING_BUFFER_TUPLES = 8 * 4; // 4 cache linesp

  struct Buffer {
    ProbeTuple tuples[BUFFER_TUPLES];
    uint64_t count;
    // offset by 7 cache lines
    uint8_t offset[7 * 64 - sizeof(uint64_t)];

    Buffer() {
      count = 0;
    }
  } __attribute__((__packed__)) __attribute__((aligned(64)));

  struct BufferBuffer {
    ProbeTuple tuples[STREAMING_BUFFER_TUPLES];
    int count;
    uint8_t padding[64 - sizeof(int)];

    BufferBuffer() {
      count = 0;
    }
  } __attribute__((__packed__)) __attribute__((aligned(64)));

#if STREAMING
  inline void BufferTuple(const ProbeTuple* tuple, Buffer* buffer) {
    BufferBuffer* buffer_buffer = &buffer_buffers_[tuple->id];
    DCHECK_LT(buffer_buffer->count, STREAMING_BUFFER_TUPLES);
    buffer_buffer->tuples[buffer_buffer->count++] = *tuple;
    if (UNLIKELY(buffer_buffer->count == STREAMING_BUFFER_TUPLES)) {
      DCHECK_LE(buffer->count + buffer_buffer->count, BUFFER_TUPLES);
      // Do a streaming write of streaming_tuples
      __m128i* buffer_write_ptr = (__m128i*)&buffer->tuples[buffer->count];
      // TODO code very dependent on size of ProbeTuple.
      DCHECK_EQ(buffer_buffer->count % 2, 0);
      for (int i = 0; i < buffer_buffer->count; i += 2) {
        __m128i content = _mm_set_epi64x(*(long long*) (buffer_buffer->tuples + i),
                                         *(long long*) (buffer_buffer->tuples + i + 1));
        _mm_stream_si128(buffer_write_ptr + i/2, content);
      }
      buffer->count += buffer_buffer->count;
      buffer_buffer->count = 0;
    }
  }
#endif

  void TestThroughput() {
    // align allocations.
    bool fail = posix_memalign((void**)&buffers_, __alignof(*buffers_), sizeof(*buffers_) * NUM_BUFFERS);
    CHECK(!fail);
    fail = posix_memalign((void**)&buffer_buffers_, __alignof(*buffer_buffers_), sizeof(*buffers_) * NUM_BUFFERS);
    CHECK(!fail);
    CHECK_EQ(((long)buffers_) % 64, 0);
    for (int i = 0; i < NUM_BUFFERS; ++i) {
      new (buffers_ + i) Buffer();
      new (buffer_buffers_ + i) BufferBuffer();
    }
    ProbeTuple* tuples = GenTuples(DATA_TUPLES, NUM_BUFFERS);
    StopWatch watch;
    watch.Start();
    for (uint64_t i = 0; i < DATA_TUPLES; ++i) {
      const ProbeTuple* tuple = &tuples[i];
      Buffer* buffer = &buffers_[tuple->id];
#if STREAMING
      BufferTuple(tuple, buffer);
#else
      buffer->tuples[buffer->count++] = *tuple;
      DCHECK_LT(buffer->count, BUFFER_TUPLES);
#endif
    }
    watch.Stop();
    LOG(ERROR) << PrettyPrinter::Print(watch.Ticks(), TUnit::CPU_TICKS);;
    free(tuples);
    // Note: destructors not called.
    free(buffers_);
    free(buffer_buffers_);
  }

  void TestRawThroughput() {
    const int NUM_RECORDS = 1<<27;
    int64_t* buffer = (int64_t*) malloc(sizeof(long) * NUM_RECORDS);
    int64_t constant = 0xFA57;
    StopWatch watch;
    watch.Start();
    for (int64_t i = 0; i < NUM_RECORDS; ++i) {
      buffer[i] = constant;
    }
    watch.Stop();
    LOG(ERROR) << PrettyPrinter::Print(watch.Ticks(), TUnit::CPU_TICKS);;
    free(buffer);
  }

  
  Buffer* buffers_;
  BufferBuffer* buffer_buffers_;
};

}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  CpuInfo::Init();
  PartitioningThroughputTest test;
  test.TestRawThroughput();
  //test.TestThroughput();
  return 0;
}
