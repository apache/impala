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

#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/bloom-filter.h"

#include "common/names.h"

using namespace std;
using namespace impala;

// Tests Bloom filter performance on four tasks:
//
// 1. Construct/destruct pairs
// 2. Inserts
// 3. Lookups when the item is present
// 4. Lookups when the item is absent (this is theoretically faster than when the item is
//    present in some Bloom filter variants)
//
// As in bloom-filter.h, ndv refers to the number of unique items inserted into a filter
// and fpp is the probability of false positives.
//
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
// initialize:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//       ndv      10k fpp   10.0%                6628                  1X
//       ndv      10k fpp    1.0%                3655             0.5515X
//       ndv      10k fpp    0.1%                1293              0.195X
//       ndv    1000k fpp   10.0%               28.92           0.004363X
//       ndv    1000k fpp    1.0%                14.5           0.002188X
//       ndv    1000k fpp    0.1%               14.51            0.00219X
//       ndv  100000k fpp   10.0%             0.05863          8.846e-06X
//       ndv  100000k fpp    1.0%             0.05776          8.714e-06X
//       ndv  100000k fpp    0.1%             0.02849          4.298e-06X
//
// insert:               Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//       ndv      10k fpp   10.0%           1.109e+05                  1X
//       ndv      10k fpp    1.0%           1.083e+05             0.9771X
//       ndv      10k fpp    0.1%           1.088e+05             0.9808X
//       ndv    1000k fpp   10.0%           8.975e+04             0.8094X
//       ndv    1000k fpp    1.0%             8.8e+04             0.7937X
//       ndv    1000k fpp    0.1%           9.353e+04             0.8435X
//       ndv  100000k fpp   10.0%           2.322e+04             0.2094X
//       ndv  100000k fpp    1.0%           2.314e+04             0.2087X
//       ndv  100000k fpp    0.1%           1.953e+04             0.1762X
//
// find:                 Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
// present ndv      10k fpp   10.0%           2.044e+05                  1X
// absent  ndv      10k fpp   10.0%           1.019e+05             0.4984X
// present ndv      10k fpp    1.0%           2.039e+05             0.9976X
// absent  ndv      10k fpp    1.0%           1.234e+05             0.6037X
// present ndv      10k fpp    0.1%           1.928e+05             0.9431X
// absent  ndv      10k fpp    0.1%           1.998e+05             0.9774X
// present ndv    1000k fpp   10.0%           1.367e+05             0.6686X
// absent  ndv    1000k fpp   10.0%           7.115e+04              0.348X
// present ndv    1000k fpp    1.0%           1.164e+05             0.5694X
// absent  ndv    1000k fpp    1.0%           9.859e+04             0.4822X
// present ndv    1000k fpp    0.1%           1.153e+05             0.5638X
// absent  ndv    1000k fpp    0.1%           9.787e+04             0.4787X
// present ndv  100000k fpp   10.0%           2.869e+04             0.1403X
// absent  ndv  100000k fpp   10.0%           3.222e+04             0.1576X
// present ndv  100000k fpp    1.0%           2.868e+04             0.1403X
// absent  ndv  100000k fpp    1.0%           3.212e+04             0.1571X
// present ndv  100000k fpp    0.1%           2.793e+04             0.1366X
// absent  ndv  100000k fpp    0.1%           3.948e+04             0.1931X

// Make a random uint32_t, avoiding the absent high bit and the low-entropy low bits
// produced by rand().
uint32_t MakeRand() {
  uint32_t result = (rand() >> 8) & 0xffff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  return result;
}

// Just benchmark the constructor and destructor cost
namespace initialize {

void Benchmark(int batch_size, void* data) {
  int * d = reinterpret_cast<int*>(data);
  for (int i = 0; i < batch_size; ++i) {
    BloomFilter bf(*d, nullptr, nullptr);
  }
}

}  // namespace initialize

// Benchmark insert
namespace insert {

struct TestData {
  explicit TestData(int log_heap_size)
      : bf(log_heap_size, nullptr, nullptr), data(1ull << 20) {
    for (size_t i = 0; i < data.size(); ++i) {
      data[i] = MakeRand();
    }
  }

  BloomFilter bf;
  vector<uint32_t> data;
};

void Benchmark(int batch_size, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  for (int i = 0; i < batch_size; ++i) {
    d->bf.Insert(d->data[i & (d->data.size() - 1)]);
  }
}

}  // namespace insert

// Benchmark find in a bloom filter. There are separate benchmarks for finding items that
// are present and items that are absent, as the often have different performance
// characteristics in Bloom filters.

namespace find {

struct TestData {
  TestData(int log_heap_size, size_t size)
      : bf(log_heap_size, nullptr, nullptr),
        vec_mask((1ull << static_cast<int>(floor(log2(size))))-1),
        present(size),
        absent(size),
        result(0) {
    for (size_t i = 0; i < size; ++i) {
      present[i] = MakeRand();
      absent[i] = MakeRand();
      bf.Insert(present[i]);
    }
  }

  BloomFilter bf;
  // A mask value such that i & vec_mask < present.size() (and absent.size()). This is
  // used in the benchmark functions to loop through present and absent, because
  // i % present.size() invokes
  size_t vec_mask;
  vector<uint32_t> present, absent;
  // Used only to avoid the compiler optimizing out the results of BloomFilter::Find()
  size_t result;
};

void Present(int batch_size, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  for (int i = 0; i < batch_size; ++i) {
    d->result += d->bf.Find(d->present[i & (d->vec_mask)]);
  }
}

void Absent(int batch_size, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  for (int i = 0; i < batch_size; ++i) {
    d->result += d->bf.Find(d->absent[i & (d->vec_mask)]);
  }
}

}  // namespace find

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << endl << Benchmark::GetMachineInfo() << endl;

  char name[120];

  {
    Benchmark suite("initialize");
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (double fpp = 0.1; fpp >= 0.001; fpp /= 10) {
        int* d = new int(BloomFilter::MinLogSpace(ndv, fpp));
        snprintf(name, sizeof(name), "ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, initialize::Benchmark, d);
      }
    }
    cout << suite.Measure() << endl;
  }

  {
    Benchmark suite("insert");
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (double fpp = 0.1; fpp >= 0.001; fpp /= 10) {
        insert::TestData* d =
            new insert::TestData(BloomFilter::MinLogSpace(ndv, fpp));
        snprintf(name, sizeof(name), "ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, insert::Benchmark, d);
      }
    }
    cout << suite.Measure() << endl;
  }

  {
    Benchmark suite("find");
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (double fpp = 0.1; fpp >= 0.001; fpp /= 10) {
        find::TestData* d = new find::TestData(BloomFilter::MinLogSpace(ndv, fpp), ndv);

        snprintf(name, sizeof(name), "present ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, find::Present, d);

        snprintf(name, sizeof(name), "absent  ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, find::Absent, d);
      }
    }
    cout << suite.Measure() << endl;
  }

  return 0;
}
