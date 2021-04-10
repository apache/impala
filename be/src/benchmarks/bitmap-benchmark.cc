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

#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/bitmap.h"

#include "common/names.h"

using namespace std;
using namespace impala;

// Tests Bitmap performance on three tasks:
//
// 1. Construct/destruct pairs
// 2. Set an index to true with modulo on Bitmap size. In other words,
//    bm.Set(_ % bm.num_bits(), true)
// 3. Get(_ % bm.num_bits())
//
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
// initialize:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                size       1000           5.188e+04                  1X
//                size      10000           2.834e+04             0.5463X
//                size     100000                4795            0.09242X
//                size    1000000               340.3           0.006558X
//                size   10000000               24.63          0.0004746X
//                size  100000000               1.157          2.229e-05X
//                size 1000000000             0.08224          1.585e-06X
//
// set:                  Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                size       1000           1.144e+05                  1X
//                size      10000           1.155e+05               1.01X
//                size     100000           1.168e+05              1.021X
//                size    1000000           1.142e+05             0.9981X
//                size   10000000           9.789e+04             0.8559X
//                size  100000000           3.479e+04             0.3042X
//                size 1000000000           2.985e+04              0.261X
//
// get:                  Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                size       1000           1.231e+05                  1X
//                size      10000           1.245e+05              1.011X
//                size     100000           1.251e+05              1.016X
//                size    1000000           1.249e+05              1.014X
//                size   10000000           1.068e+05              0.867X
//                size  100000000           3.598e+04             0.2922X
//                size 1000000000           3.072e+04             0.2495X

// Make a random non-negative int64_t, avoiding the absent high bit and the low-entropy
// low bits produced by rand().
int64_t MakeNonNegativeRand() {
  int64_t result = (rand() >> 8) & 0x7fff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  result <<= 15;
  result |= (rand() >> 8) & 0xffff;
  return result;
}

// Just benchmark the constructor and destructor cost
namespace bitmapinitialize {

void Benchmark(int batch_size, void* data) {
  int64_t * d = reinterpret_cast<int64_t*>(data);
  for (int i = 0; i < batch_size; ++i) {
    Bitmap bm(*d);
  }
}

}  // namespace initialize

// Benchmark insert
namespace bitmapset {

struct TestData {
  explicit TestData(int64_t size) : bm(size), data(1ull << 20) {
    for (size_t i = 0; i < data.size(); ++i) {
      data[i] = MakeNonNegativeRand();
    }
  }

  Bitmap bm;
  vector<int64_t> data;
};

void Benchmark(int batch_size, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  for (int i = 0; i < batch_size; ++i) {
    d->bm.Set(d->data[i & (d->data.size() - 1)] % d->bm.num_bits(), true);
  }
}

}  // namespace bitmapset

namespace bitmapget {

struct TestData {
  TestData(int64_t size)
    : bm(size), data (1ull << 20) {
    for (size_t i = 0; i < size/2; ++i) {
      bm.Set(MakeNonNegativeRand() % size, true);
    }
    for (size_t i = 0; i < data.size(); ++i) {
      data[i] = MakeNonNegativeRand();
    }
  }

  Bitmap bm;
  vector<int64_t> data;
  // Used only to avoid the compiler optimizing out the results of Bitmap::Get()
  size_t result;
};

void Benchmark(int batch_size, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  for (int i = 0; i < batch_size; ++i) {
    d->result += d->bm.Get(d->data[i & (d->data.size() - 1)] % d->bm.num_bits());
  }
}

}  // namespace bitmapget

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << endl << Benchmark::GetMachineInfo() << endl;

  char name[120];

  {
    Benchmark suite("initialize");
    for (int64_t size = 1000; size <= 1000 * 1000 * 1000; size *= 10) {
      int64_t* d = new int64_t(size);
      snprintf(name, sizeof(name), "size %10ld", size);
      suite.AddBenchmark(name, bitmapinitialize::Benchmark, d);
    }
    cout << suite.Measure() << endl;
  }

  {
    Benchmark suite("set");
    for (int64_t size = 1000; size <= 1000 * 1000 * 1000; size *= 10) {
      bitmapset::TestData* d = new bitmapset::TestData(size);
      snprintf(name, sizeof(name), "size %10ld", size);
      suite.AddBenchmark(name, bitmapset::Benchmark, d);
    }
    cout << suite.Measure() << endl;
  }

  {
    Benchmark suite("get");
    for (int64_t size = 1000; size <= 1000 * 1000 * 1000; size *= 10) {
      bitmapget::TestData* d = new bitmapget::TestData(size);
      snprintf(name, sizeof(name), "size %10ld", size);
      suite.AddBenchmark(name, bitmapget::Benchmark, d);
    }
    cout << suite.Measure() << endl;
  }

  return 0;
}
