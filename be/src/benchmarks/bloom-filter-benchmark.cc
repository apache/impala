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
//
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
//
// With AVX2:
//
// initialize:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//       ndv      10k fpp   10.0%                6607                  1X
//       ndv      10k fpp    1.0%                3427             0.5187X
//       ndv      10k fpp    0.1%                1203              0.182X
//       ndv    1000k fpp   10.0%               5.273          0.0007982X
//       ndv    1000k fpp    1.0%               3.297           0.000499X
//       ndv    1000k fpp    0.1%                3.31           0.000501X
//       ndv  100000k fpp   10.0%             0.08597          1.301e-05X
//       ndv  100000k fpp    1.0%              0.0846           1.28e-05X
//       ndv  100000k fpp    0.1%             0.04349          6.582e-06X
//
// insert:               Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//       ndv      10k fpp   10.0%           1.858e+05                  1X
//       ndv      10k fpp    1.0%           1.801e+05             0.9693X
//       ndv      10k fpp    0.1%           1.869e+05              1.006X
//       ndv    1000k fpp   10.0%           1.686e+05             0.9076X
//       ndv    1000k fpp    1.0%           1.627e+05             0.8756X
//       ndv    1000k fpp    0.1%            1.53e+05             0.8234X
//       ndv  100000k fpp   10.0%           4.262e+04             0.2294X
//       ndv  100000k fpp    1.0%           4.326e+04             0.2329X
//       ndv  100000k fpp    0.1%           4.185e+04             0.2253X
//
// find:                 Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
// present ndv      10k fpp   10.0%           2.277e+05                  1X
// absent  ndv      10k fpp   10.0%           2.258e+05             0.9914X
// present ndv      10k fpp    1.0%           2.277e+05                  1X
// absent  ndv      10k fpp    1.0%           2.295e+05              1.008X
// present ndv      10k fpp    0.1%           2.258e+05             0.9916X
// absent  ndv      10k fpp    0.1%           2.283e+05              1.003X
// present ndv    1000k fpp   10.0%           1.799e+05             0.7901X
// absent  ndv    1000k fpp   10.0%           1.777e+05             0.7803X
// present ndv    1000k fpp    1.0%            1.52e+05             0.6674X
// absent  ndv    1000k fpp    1.0%           1.625e+05             0.7134X
// present ndv    1000k fpp    0.1%           1.825e+05             0.8013X
// absent  ndv    1000k fpp    0.1%           1.836e+05              0.806X
// present ndv  100000k fpp   10.0%           4.125e+04             0.1811X
// absent  ndv  100000k fpp   10.0%           4.147e+04             0.1821X
// present ndv  100000k fpp    1.0%           4.203e+04             0.1845X
// absent  ndv  100000k fpp    1.0%           4.189e+04             0.1839X
// present ndv  100000k fpp    0.1%           3.506e+04             0.1539X
// absent  ndv  100000k fpp    0.1%           3.507e+04              0.154X
//
//
// Without AVX2:
//
// initialize:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//       ndv      10k fpp   10.0%                6453                  1X
//       ndv      10k fpp    1.0%                3271             0.5068X
//       ndv      10k fpp    0.1%                1280             0.1984X
//       ndv    1000k fpp   10.0%               5.213          0.0008078X
//       ndv    1000k fpp    1.0%               2.574          0.0003989X
//       ndv    1000k fpp    0.1%               2.584          0.0004005X
//       ndv  100000k fpp   10.0%             0.03276          5.076e-06X
//       ndv  100000k fpp    1.0%             0.03224          4.996e-06X
//       ndv  100000k fpp    0.1%              0.0161          2.494e-06X
//
// insert:               Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//       ndv      10k fpp   10.0%           1.128e+05                  1X
//       ndv      10k fpp    1.0%           1.162e+05               1.03X
//       ndv      10k fpp    0.1%           1.145e+05              1.015X
//       ndv    1000k fpp   10.0%           1.086e+05             0.9626X
//       ndv    1000k fpp    1.0%           8.377e+04             0.7427X
//       ndv    1000k fpp    0.1%           8.902e+04             0.7892X
//       ndv  100000k fpp   10.0%           2.548e+04             0.2259X
//       ndv  100000k fpp    1.0%            2.37e+04             0.2101X
//       ndv  100000k fpp    0.1%           2.256e+04                0.2X
//
// find:                 Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
// present ndv      10k fpp   10.0%           1.676e+05                  1X
// absent  ndv      10k fpp   10.0%           1.067e+05             0.6366X
// present ndv      10k fpp    1.0%           1.683e+05              1.004X
// absent  ndv      10k fpp    1.0%           1.291e+05             0.7705X
// present ndv      10k fpp    0.1%           1.662e+05             0.9917X
// absent  ndv      10k fpp    0.1%           2.238e+05              1.336X
// present ndv    1000k fpp   10.0%           1.231e+05             0.7344X
// absent  ndv    1000k fpp   10.0%           6.903e+04             0.4119X
// present ndv    1000k fpp    1.0%           1.215e+05              0.725X
// absent  ndv    1000k fpp    1.0%           1.124e+05             0.6707X
// present ndv    1000k fpp    0.1%           1.095e+05             0.6532X
// absent  ndv    1000k fpp    0.1%           1.034e+05             0.6171X
// present ndv  100000k fpp   10.0%           2.733e+04             0.1631X
// absent  ndv  100000k fpp   10.0%           3.447e+04             0.2057X
// present ndv  100000k fpp    1.0%           2.779e+04             0.1658X
// absent  ndv  100000k fpp    1.0%            3.36e+04             0.2005X
// present ndv  100000k fpp    0.1%           2.725e+04             0.1626X
// absent  ndv  100000k fpp    0.1%           4.342e+04             0.2591X

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
    BloomFilter bf(*d);
  }
}

}  // namespace initialize

// Benchmark insert
namespace insert {

struct TestData {
  explicit TestData(int log_heap_size) : bf(log_heap_size), data(1ull << 20) {
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
      : bf(log_heap_size),
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

void RunBenchmarks() {

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
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << endl << Benchmark::GetMachineInfo() << endl << endl
       << "With AVX2:" << endl << endl;
  RunBenchmarks();
  cout << endl << "Without AVX2:" << endl << endl;
  CpuInfo::TempDisable t(CpuInfo::AVX2);
  RunBenchmarks();
}
