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

#include <iostream>
#include <vector>
#include <random>

#include "gutil/strings/substitute.h"
#include "util/benchmark.h"
#include "util/rle-encoding.h"
#include "util/cpu-info.h"

#include "common/names.h"

// Benchmark to measure the speed of Parquet RLE decoding for various bit widths and
// run lengths. Currently compares RleBatchDecoder used by Impala with an older version
// that used memset.

// Machine Info: Intel(R) Core(TM) i5-6600 CPU @ 3.30GHz
// RLE decoding bit_width 1:  Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//
//            for loop / run length: 1              0.821    0.821    0.822         1X         1X         1X
//              memset / run length: 1              0.824    0.836    0.836         1X      1.02X      1.02X
//           for loop / run length: 10                0.4      0.4      0.4     0.487X     0.487X     0.486X
//             memset / run length: 10              0.396      0.4      0.4     0.482X     0.487X     0.486X
//          for loop / run length: 100               1.06     1.06     1.06      1.29X      1.29X      1.29X
//            memset / run length: 100               1.04     1.04     1.04      1.26X      1.26X      1.26X
//         for loop / run length: 1000               5.83     5.87     5.94       7.1X      7.14X      7.23X
//           memset / run length: 1000               5.87     5.87     5.87      7.14X      7.14X      7.13X
//        for loop / run length: 10000               9.53     9.54     9.54      11.6X      11.6X      11.6X
//          memset / run length: 10000               9.54     9.54     9.54      11.6X      11.6X      11.6X

using std::min;
using std::uniform_int_distribution;
using std::minstd_rand;

using namespace impala;

constexpr int MAX_BIT_WIDTH = 8;
constexpr int NUM_OUT_VALUES = 1024 * 1024;

uint8_t out_buffer[NUM_OUT_VALUES];

/// RLE encodes NUM_OUT_VALUES number of bytes into the buffer.
/// The length of runs are pseudo random between 1 and max_run_length.
int FillWithRle(vector<uint8_t>* buffer, int bit_width, int max_run_length) {
  RleEncoder encoder(buffer->data(), buffer->size(), bit_width);

  uniform_int_distribution<int> uniform_dist(1, max_run_length);
  minstd_rand rand_engine;
  uint8_t val = 0;
  int run_length = 0;
  for (int i = 0; i < NUM_OUT_VALUES; ++i) {
    if (!encoder.Put(val)) {
      LOG(ERROR) << Substitute(
          "Error during RLE encoding. bit_widths: $0 max_run_length: $1",
          bit_width, max_run_length);
    }
    if (run_length == 0) {
      run_length = uniform_dist(rand_engine);
      val = (val + 1) % (1 << bit_width);
    }
    --run_length;
  }
  return encoder.Flush();
}

struct BenchmarkParams {
  int bit_width;
  int max_run_length;
  vector<uint8_t> input_buffer;
  int input_size;

  BenchmarkParams(int bit_width, int max_run_length)
      : bit_width(bit_width),
        max_run_length(max_run_length),
        // Add some extra space for the possible overhead of RLE.
        input_buffer(3 * NUM_OUT_VALUES * bit_width / 8) {
    input_size = FillWithRle(&input_buffer, bit_width, max_run_length);
  }
};


/// Copy of the old version of RleBatchDecoder<uint8_t>::GetValues() modified to get the
/// RleBatchDecoder as argument.
template <typename T>
inline int32_t GetValuesMemset(int32_t num_values_to_consume, T* values,
    RleBatchDecoder<T>* decoder) {
  int32_t num_consumed = 0;
  while (num_consumed < num_values_to_consume) {
    // Add RLE encoded values by repeating the current value this number of times.
    uint32_t num_repeats = decoder->NextNumRepeats();
    if (num_repeats > 0) {
       uint32_t num_repeats_to_set =
           min<uint32_t>(num_repeats, num_values_to_consume - num_consumed);
       T repeated_value = decoder->GetRepeatedValue(num_repeats_to_set);
       memset(values + num_consumed, repeated_value, num_repeats_to_set);
       num_consumed += num_repeats_to_set;
       continue;
    }

    // Add remaining literal values, if any.
    uint32_t num_literals = decoder->NextNumLiterals();
    if (num_literals == 0) break;
    uint32_t num_literals_to_set =
        min<uint32_t>(num_literals, num_values_to_consume - num_consumed);
    if (!decoder->GetLiteralValues(num_literals_to_set, values + num_consumed)) {
      DCHECK(false);
      return 0;
    }
    num_consumed += num_literals_to_set;
  }
  return num_consumed;
}

/// Benchmark calling RleBatchDecoder<uint8_t>::GetValues().
void RleBenchmark(int batch_size, void* data) {
  for (int i = 0; i < batch_size; ++i) {
    BenchmarkParams* p = reinterpret_cast<BenchmarkParams*>(data);
    RleBatchDecoder<uint8_t> decoder(p->input_buffer.data(), p->input_size, p->bit_width);
    int result = decoder.GetValues(NUM_OUT_VALUES, out_buffer);
    if (result != NUM_OUT_VALUES) {
      LOG(ERROR) << Substitute(
          "Error in GetValues(). bit_width: $0 max_run_length: $1 "
          "expected number of values: $2 decoded number of values: $3",
          p->bit_width, p->max_run_length, NUM_OUT_VALUES, result);
      exit(1);
    }
  }
}

/// Benchmark calling the old version of RleBatchDecoder<uint8_t>::GetValues() that used
/// memset() for setting repeated values.
void RleBenchmarkMemset(int batch_size, void* data) {
  for (int i = 0; i < batch_size; ++i) {
    BenchmarkParams* p = reinterpret_cast<BenchmarkParams*>(data);
    RleBatchDecoder<uint8_t> decoder(p->input_buffer.data(), p->input_size, p->bit_width);
    int result = GetValuesMemset(NUM_OUT_VALUES, out_buffer, &decoder);
    if (result != NUM_OUT_VALUES) {
      LOG(ERROR) << Substitute(
          "Error in GetValuesMemset(). bit_width: $0 max_run_length: $1 "
          "expected number of values: $2 decoded number of values: $3",
          p->bit_width, p->max_run_length, NUM_OUT_VALUES, result);
      exit(1);
    }
  }
}

struct RleBenchmarks {
  BenchmarkParams params;

  RleBenchmarks(Benchmark* suite, int bit_width, int run_length)
      : params(bit_width, run_length) {
    suite->AddBenchmark(
        Substitute("for loop / max run length: $0", run_length),
        RleBenchmark, &params);
    suite->AddBenchmark(
        Substitute("memset / max run length: $0", run_length),
        RleBenchmarkMemset, &params);
  }
};

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << endl << Benchmark::GetMachineInfo() << endl;

  for (int bit_width = 1; bit_width <= MAX_BIT_WIDTH; ++bit_width) {
    Benchmark suite(Substitute("RLE decoding bit_width $0", bit_width));

    RleBenchmarks b1(&suite, bit_width, 1);
    RleBenchmarks b10(&suite, bit_width, 10);
    RleBenchmarks b100(&suite, bit_width, 100);
    RleBenchmarks b1000(&suite, bit_width, 1000);
    RleBenchmarks b10000(&suite, bit_width, 10000);

    cout << suite.Measure() << endl;
  }
  return 0;
}
