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
#include "util/bloom-filter.h"

#include "common/names.h"

using namespace std;
using namespace impala;

// Tests Bloom filter performance on:
//
// 1. Construct/destruct pairs
// 2. Inserts
// 3. Lookups when the item is present
// 4. Lookups when the item is absent (this is theoretically faster than when the item is
//    present in some Bloom filter variants)
// 5. Unions
//
// As in bloom-filter.h, ndv refers to the number of unique items inserted into a filter
// and fpp is the probability of false positives.
//
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
//
// initialize:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           5.89e+03 5.98e+03 6.03e+03         1X         1X         1X
//            ndv      10k fpp    1.0%           3.22e+03 3.25e+03 3.27e+03     0.546X     0.543X     0.542X
//            ndv      10k fpp    0.1%           1.13e+03 1.17e+03 1.18e+03     0.191X     0.195X     0.195X
//            ndv    1000k fpp   10.0%               3.85     3.93     3.93  0.000654X  0.000657X  0.000652X
//            ndv    1000k fpp    1.0%               2.04     2.12     2.12  0.000346X  0.000354X  0.000351X
//            ndv    1000k fpp    0.1%               2.04     2.12     2.12  0.000346X  0.000354X  0.000351X
//            ndv  100000k fpp   10.0%             0.0281    0.029   0.0294  4.77e-06X  4.85e-06X  4.87e-06X
//            ndv  100000k fpp    1.0%             0.0284    0.029   0.0298  4.82e-06X  4.85e-06X  4.93e-06X
//            ndv  100000k fpp    0.1%             0.0144   0.0147   0.0149  2.44e-06X  2.47e-06X  2.47e-06X
//
// With AVX2:
//
// insert:                    Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           1.17e+05 1.23e+05 1.25e+05         1X         1X         1X
//            ndv      10k fpp    1.0%           1.17e+05 1.24e+05 1.25e+05         1X         1X         1X
//            ndv      10k fpp    0.1%            1.2e+05 1.23e+05 1.24e+05      1.02X     0.996X     0.991X
//            ndv    1000k fpp   10.0%            1.1e+05 1.18e+05  1.2e+05     0.944X     0.959X      0.96X
//            ndv    1000k fpp    1.0%           1.11e+05 1.16e+05 1.17e+05     0.954X     0.938X     0.934X
//            ndv    1000k fpp    0.1%           9.73e+04 1.16e+05 1.17e+05     0.834X     0.937X     0.936X
//            ndv  100000k fpp   10.0%           2.96e+04 4.19e+04 5.44e+04     0.254X      0.34X     0.436X
//            ndv  100000k fpp    1.0%           2.92e+04 3.81e+04 4.89e+04      0.25X     0.308X     0.391X
//            ndv  100000k fpp    0.1%           2.44e+04 3.28e+04 4.31e+04     0.209X     0.266X     0.345X
//
// find:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//    present ndv      10k fpp   10.0%           1.16e+05 1.17e+05 1.18e+05         1X         1X         1X
//    absent  ndv      10k fpp   10.0%           1.16e+05 1.17e+05 1.18e+05         1X         1X     0.998X
//    present ndv      10k fpp    1.0%           1.15e+05 1.17e+05 1.18e+05     0.988X         1X     0.999X
//    absent  ndv      10k fpp    1.0%           1.14e+05 1.17e+05 1.19e+05     0.978X         1X         1X
//    present ndv      10k fpp    0.1%           1.09e+05 1.17e+05 1.18e+05     0.939X         1X         1X
//    absent  ndv      10k fpp    0.1%           1.13e+05 1.17e+05 1.18e+05      0.97X         1X         1X
//    present ndv    1000k fpp   10.0%           1.09e+05 1.13e+05 1.15e+05     0.942X     0.968X      0.97X
//    absent  ndv    1000k fpp   10.0%           1.09e+05 1.15e+05 1.16e+05     0.937X     0.982X     0.982X
//    present ndv    1000k fpp    1.0%           9.44e+04 1.12e+05 1.13e+05     0.814X     0.952X     0.951X
//    absent  ndv    1000k fpp    1.0%           1.02e+05 1.14e+05 1.15e+05     0.877X     0.973X     0.972X
//    present ndv    1000k fpp    0.1%           1.01e+05 1.11e+05 1.12e+05     0.868X     0.951X     0.949X
//    absent  ndv    1000k fpp    0.1%           1.08e+05 1.14e+05 1.15e+05     0.927X     0.975X     0.975X
//    present ndv  100000k fpp   10.0%           3.18e+04 3.94e+04 5.18e+04     0.274X     0.336X     0.437X
//    absent  ndv  100000k fpp   10.0%           2.74e+04 3.07e+04 3.49e+04     0.236X     0.262X     0.294X
//    present ndv  100000k fpp    1.0%           3.07e+04 4.29e+04 5.51e+04     0.265X     0.366X     0.465X
//    absent  ndv  100000k fpp    1.0%           2.67e+04  2.9e+04 3.25e+04      0.23X     0.248X     0.274X
//    present ndv  100000k fpp    0.1%           2.78e+04 3.88e+04  4.9e+04      0.24X     0.331X     0.413X
//    absent  ndv  100000k fpp    0.1%           2.44e+04 2.84e+04 3.02e+04     0.211X     0.242X     0.255X
//
// union:                     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           1.81e+04 1.84e+04 1.86e+04         1X         1X         1X
//            ndv      10k fpp    1.0%           8.25e+03 8.39e+03 8.47e+03     0.455X     0.455X     0.455X
//            ndv      10k fpp    0.1%           4.02e+03 4.31e+03 4.35e+03     0.222X     0.234X     0.234X
//            ndv    1000k fpp   10.0%                105      111      112   0.00577X   0.00603X   0.00602X
//            ndv    1000k fpp    1.0%               45.9     46.4     46.9   0.00253X   0.00252X   0.00252X
//            ndv    1000k fpp    0.1%               46.2     46.6     46.9   0.00255X   0.00253X   0.00252X
//            ndv  100000k fpp   10.0%                0.2      0.2      0.2   1.1e-05X  1.08e-05X  1.07e-05X
//            ndv  100000k fpp    1.0%                0.2      0.2      0.2   1.1e-05X  1.08e-05X  1.07e-05X
//            ndv  100000k fpp    0.1%              0.133    0.143    0.145  7.35e-06X  7.75e-06X  7.79e-06X
//
//
// Without AVX or AVX2:
//
// insert:                    Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           9.27e+04 9.33e+04  9.4e+04         1X         1X         1X
//            ndv      10k fpp    1.0%           9.43e+04 9.49e+04 9.61e+04      1.02X      1.02X      1.02X
//            ndv      10k fpp    0.1%           9.36e+04  9.5e+04 9.58e+04      1.01X      1.02X      1.02X
//            ndv    1000k fpp   10.0%            8.4e+04 9.49e+04 9.61e+04     0.906X      1.02X      1.02X
//            ndv    1000k fpp    1.0%           7.64e+04 9.34e+04 9.45e+04     0.824X         1X      1.01X
//            ndv    1000k fpp    0.1%           8.24e+04 9.34e+04 9.44e+04     0.888X         1X         1X
//            ndv  100000k fpp   10.0%           3.22e+04    4e+04 5.03e+04     0.347X     0.429X     0.535X
//            ndv  100000k fpp    1.0%           2.77e+04  3.6e+04  4.8e+04     0.298X     0.386X      0.51X
//            ndv  100000k fpp    0.1%           2.54e+04 2.93e+04 4.32e+04     0.274X     0.314X      0.46X
//
// find:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//    present ndv      10k fpp   10.0%            1.3e+05 1.31e+05 1.33e+05         1X         1X         1X
//    absent  ndv      10k fpp   10.0%           8.74e+04 8.83e+04 8.92e+04     0.674X     0.673X     0.671X
//    present ndv      10k fpp    1.0%           1.25e+05  1.3e+05 1.31e+05      0.96X     0.991X     0.988X
//    absent  ndv      10k fpp    1.0%           1.04e+05 1.06e+05 1.07e+05     0.805X     0.809X     0.807X
//    present ndv      10k fpp    0.1%           1.28e+05  1.3e+05 1.31e+05     0.986X     0.988X     0.984X
//    absent  ndv      10k fpp    0.1%           1.69e+05 1.72e+05 1.74e+05       1.3X      1.31X      1.31X
//    present ndv    1000k fpp   10.0%           9.33e+04  9.6e+04 9.69e+04     0.719X     0.732X     0.729X
//    absent  ndv    1000k fpp   10.0%           5.99e+04 6.07e+04 6.12e+04     0.462X     0.462X     0.461X
//    present ndv    1000k fpp    1.0%           9.48e+04 1.01e+05 1.02e+05     0.731X     0.768X     0.768X
//    absent  ndv    1000k fpp    1.0%           9.49e+04 9.67e+04 9.74e+04     0.731X     0.737X     0.734X
//    present ndv    1000k fpp    0.1%           8.46e+04  9.3e+04 9.41e+04     0.652X     0.709X     0.709X
//    absent  ndv    1000k fpp    0.1%           9.05e+04 9.18e+04 9.28e+04     0.697X       0.7X     0.699X
//    present ndv  100000k fpp   10.0%            2.6e+04 2.88e+04 3.11e+04     0.201X      0.22X     0.235X
//    absent  ndv  100000k fpp   10.0%           2.88e+04 2.99e+04 3.08e+04     0.222X     0.228X     0.232X
//    present ndv  100000k fpp    1.0%           2.34e+04 2.76e+04 2.91e+04      0.18X      0.21X     0.219X
//    absent  ndv  100000k fpp    1.0%           2.86e+04 2.97e+04 3.03e+04      0.22X     0.227X     0.228X
//    present ndv  100000k fpp    0.1%           2.34e+04 2.65e+04 2.81e+04      0.18X     0.202X     0.211X
//    absent  ndv  100000k fpp    0.1%           3.73e+04 3.85e+04 3.91e+04     0.287X     0.293X     0.295X
//
// union:                     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           3.06e+03  3.1e+03 3.12e+03         1X         1X         1X
//            ndv      10k fpp    1.0%           1.51e+03 1.55e+03 1.57e+03     0.493X     0.502X     0.503X
//            ndv      10k fpp    0.1%                748      775      782     0.244X      0.25X     0.251X
//            ndv    1000k fpp   10.0%               19.6       20     20.2    0.0064X   0.00646X   0.00647X
//            ndv    1000k fpp    1.0%               9.41       10     10.1   0.00307X   0.00324X   0.00323X
//            ndv    1000k fpp    0.1%                9.9       10     10.1   0.00323X   0.00324X   0.00323X
//            ndv  100000k fpp   10.0%             0.0671   0.0714   0.0725  2.19e-05X   2.3e-05X  2.32e-05X
//            ndv  100000k fpp    1.0%             0.0676   0.0709   0.0719  2.21e-05X  2.29e-05X  2.31e-05X
//            ndv  100000k fpp    0.1%             0.0338    0.035   0.0356   1.1e-05X  1.13e-05X  1.14e-05X

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
  int* d = reinterpret_cast<int*>(data);
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

// Benchmark or
namespace either {

struct TestData {
  explicit TestData(int log_heap_size) {
    BloomFilter bf(log_heap_size);
    BloomFilter::ToThrift(&bf, &tbf);
  }

  TBloomFilter tbf;
};

void Benchmark(int batch_size, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  for (int i = 0; i < batch_size; ++i) {
    BloomFilter::Or(d->tbf, &d->tbf);
  }
}

} // namespace either

void RunBenchmarks() {

  char name[120];

  {
    Benchmark suite("insert");
    vector<unique_ptr<insert::TestData> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (double fpp = 0.1; fpp >= 0.001; fpp /= 10) {
        testdata.emplace_back(new insert::TestData(BloomFilter::MinLogSpace(ndv, fpp)));
        snprintf(name, sizeof(name), "ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, insert::Benchmark, testdata.back().get());
      }
    }
    cout << suite.Measure() << endl;
  }

  {
    Benchmark suite("find");
    vector<unique_ptr<find::TestData> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (double fpp = 0.1; fpp >= 0.001; fpp /= 10) {
        testdata.emplace_back(
            new find::TestData(BloomFilter::MinLogSpace(ndv, fpp), ndv));
        snprintf(name, sizeof(name), "present ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, find::Present, testdata.back().get());

        snprintf(name, sizeof(name), "absent  ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, find::Absent, testdata.back().get());
      }
    }
    cout << suite.Measure() << endl;
  }

  {
    Benchmark suite("union");
    vector<unique_ptr<either::TestData> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (double fpp = 0.1; fpp >= 0.001; fpp /= 10) {
        testdata.emplace_back(
            new either::TestData(BloomFilter::MinLogSpace(ndv, fpp)));
        snprintf(name, sizeof(name), "ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, either::Benchmark, testdata.back().get());
      }
    }
    cout << suite.Measure() << endl;
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();

  cout << endl << Benchmark::GetMachineInfo() << endl << endl;

  {
    char name[120];
    Benchmark suite("initialize");
    vector<unique_ptr<int> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (double fpp = 0.1; fpp >= 0.001; fpp /= 10) {
        testdata.emplace_back(new int(BloomFilter::MinLogSpace(ndv, fpp)));
        snprintf(name, sizeof(name), "ndv %7dk fpp %6.1f%%", ndv / 1000, fpp * 100);
        suite.AddBenchmark(name, initialize::Benchmark, testdata.back().get());
      }
    }
    cout << suite.Measure() << endl;
  }

  cout << "With AVX2:" << endl << endl;
  RunBenchmarks();
  cout << endl << "Without AVX or AVX2:" << endl << endl;
  CpuInfo::TempDisable t1(CpuInfo::AVX);
  CpuInfo::TempDisable t2(CpuInfo::AVX2);
  RunBenchmarks();
}
