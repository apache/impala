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
//
// initialize:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           7.05e+03 7.27e+03 7.34e+03         1X         1X         1X
//            ndv      10k fpp    1.0%           3.79e+03 3.93e+03 3.96e+03     0.538X     0.541X      0.54X
//            ndv      10k fpp    0.1%           1.39e+03 1.42e+03 1.44e+03     0.198X     0.196X     0.196X
//            ndv    1000k fpp   10.0%               4.62     4.78     4.81  0.000655X  0.000658X  0.000655X
//            ndv    1000k fpp    1.0%               2.49     2.55      2.6  0.000354X  0.000351X  0.000354X
//            ndv    1000k fpp    0.1%               2.45     2.55      2.6  0.000347X  0.000351X  0.000354X
//            ndv  100000k fpp   10.0%              0.035   0.0358    0.037  4.96e-06X  4.93e-06X  5.04e-06X
//            ndv  100000k fpp    1.0%             0.0347   0.0361   0.0372  4.93e-06X  4.96e-06X  5.06e-06X
//            ndv  100000k fpp    0.1%             0.0176   0.0181   0.0186   2.5e-06X  2.49e-06X  2.53e-06X
//
// With AVX2:
//
// insert:                    Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           2.03e+05 2.05e+05 2.08e+05         1X         1X         1X
//            ndv      10k fpp    1.0%           2.03e+05 2.06e+05 2.08e+05     0.997X         1X         1X
//            ndv      10k fpp    0.1%           2.03e+05 2.05e+05 2.07e+05     0.997X     0.998X     0.997X
//            ndv    1000k fpp   10.0%           1.82e+05 1.87e+05 1.89e+05     0.896X      0.91X     0.907X
//            ndv    1000k fpp    1.0%           1.49e+05 1.53e+05 1.56e+05     0.731X     0.747X      0.75X
//            ndv    1000k fpp    0.1%           1.79e+05 1.82e+05 1.83e+05     0.881X     0.886X     0.882X
//            ndv  100000k fpp   10.0%           4.08e+04 4.49e+04 5.44e+04     0.201X     0.219X     0.262X
//            ndv  100000k fpp    1.0%           3.94e+04  4.4e+04 5.04e+04     0.194X     0.214X     0.242X
//            ndv  100000k fpp    0.1%           4.08e+04 4.48e+04 5.68e+04     0.201X     0.218X     0.273X
//
// find:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//    present ndv      10k fpp   10.0%           2.48e+05 2.51e+05 2.53e+05         1X         1X         1X
//    absent  ndv      10k fpp   10.0%           2.47e+05 2.52e+05 2.55e+05     0.995X         1X      1.01X
//    present ndv      10k fpp    1.0%           2.49e+05 2.52e+05 2.55e+05         1X      1.01X      1.01X
//    absent  ndv      10k fpp    1.0%           2.47e+05 2.53e+05 2.56e+05     0.997X      1.01X      1.01X
//    present ndv      10k fpp    0.1%           2.49e+05 2.53e+05 2.54e+05         1X      1.01X      1.01X
//    absent  ndv      10k fpp    0.1%           2.47e+05 2.53e+05 2.56e+05     0.997X      1.01X      1.01X
//    present ndv    1000k fpp   10.0%           1.98e+05 2.04e+05 2.06e+05       0.8X     0.814X     0.812X
//    absent  ndv    1000k fpp   10.0%           2.01e+05 2.07e+05  2.1e+05     0.808X     0.826X     0.829X
//    present ndv    1000k fpp    1.0%           1.83e+05 1.95e+05 2.02e+05     0.737X      0.78X     0.798X
//    absent  ndv    1000k fpp    1.0%           2.01e+05 2.04e+05 2.08e+05     0.808X     0.815X      0.82X
//    present ndv    1000k fpp    0.1%           1.96e+05 2.01e+05 2.03e+05     0.788X       0.8X     0.801X
//    absent  ndv    1000k fpp    0.1%              2e+05 2.05e+05 2.07e+05     0.808X     0.817X     0.818X
//    present ndv  100000k fpp   10.0%            4.6e+04 5.09e+04 6.08e+04     0.185X     0.203X      0.24X
//    absent  ndv  100000k fpp   10.0%           4.11e+04 4.36e+04 4.53e+04     0.166X     0.174X     0.179X
//    present ndv  100000k fpp    1.0%           4.55e+04 4.96e+04 6.19e+04     0.184X     0.198X     0.245X
//    absent  ndv  100000k fpp    1.0%           3.83e+04 4.15e+04 4.69e+04     0.154X     0.166X     0.186X
//    present ndv  100000k fpp    0.1%           4.73e+04 5.43e+04 6.58e+04     0.191X     0.217X      0.26X
//    absent  ndv  100000k fpp    0.1%           3.77e+04 4.07e+04 4.37e+04     0.152X     0.163X     0.173X
//
// Without AVX2:
//
// insert:                    Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           1.25e+05 1.27e+05 1.28e+05         1X         1X         1X
//            ndv      10k fpp    1.0%           1.27e+05 1.29e+05  1.3e+05      1.01X      1.02X      1.02X
//            ndv      10k fpp    0.1%           1.26e+05 1.28e+05  1.3e+05         1X      1.01X      1.01X
//            ndv    1000k fpp   10.0%           1.23e+05 1.25e+05 1.26e+05     0.977X     0.981X     0.985X
//            ndv    1000k fpp    1.0%           1.16e+05 1.22e+05 1.23e+05     0.925X     0.958X     0.958X
//            ndv    1000k fpp    0.1%           1.16e+05 1.22e+05 1.23e+05     0.928X     0.958X     0.957X
//            ndv  100000k fpp   10.0%           3.77e+04 4.06e+04 5.62e+04     0.301X     0.319X     0.438X
//            ndv  100000k fpp    1.0%           3.71e+04 4.06e+04 5.45e+04     0.296X      0.32X     0.425X
//            ndv  100000k fpp    0.1%           3.37e+04 3.68e+04 5.15e+04     0.269X      0.29X     0.401X
//
// find:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//    present ndv      10k fpp   10.0%            1.6e+05 1.64e+05 1.66e+05         1X         1X         1X
//    absent  ndv      10k fpp   10.0%           1.11e+05 1.14e+05 1.15e+05     0.696X     0.697X     0.695X
//    present ndv      10k fpp    1.0%           1.57e+05 1.63e+05 1.64e+05     0.982X     0.994X     0.989X
//    absent  ndv      10k fpp    1.0%            1.3e+05 1.33e+05 1.35e+05     0.814X     0.813X     0.812X
//    present ndv      10k fpp    0.1%           1.55e+05 1.58e+05 1.61e+05     0.967X     0.968X     0.969X
//    absent  ndv      10k fpp    0.1%           2.26e+05 2.29e+05 2.31e+05      1.41X       1.4X       1.4X
//    present ndv    1000k fpp   10.0%           1.21e+05 1.23e+05 1.25e+05     0.758X     0.753X     0.756X
//    absent  ndv    1000k fpp   10.0%            7.6e+04 7.72e+04 7.81e+04     0.475X     0.472X     0.471X
//    present ndv    1000k fpp    1.0%           1.23e+05 1.27e+05 1.28e+05     0.771X     0.773X      0.77X
//    absent  ndv    1000k fpp    1.0%           1.19e+05 1.21e+05 1.22e+05     0.744X     0.739X     0.738X
//    present ndv    1000k fpp    0.1%           1.17e+05 1.18e+05  1.2e+05     0.731X     0.724X     0.723X
//    absent  ndv    1000k fpp    0.1%           1.13e+05 1.16e+05 1.17e+05     0.707X     0.706X     0.705X
//    present ndv  100000k fpp   10.0%           3.42e+04 3.63e+04  3.9e+04     0.214X     0.222X     0.235X
//    absent  ndv  100000k fpp   10.0%            3.6e+04 3.77e+04 3.82e+04     0.225X      0.23X      0.23X
//    present ndv  100000k fpp    1.0%           3.18e+04 3.42e+04 3.57e+04     0.199X     0.209X     0.216X
//    absent  ndv  100000k fpp    1.0%           3.63e+04 3.73e+04 3.79e+04     0.227X     0.228X     0.229X
//    present ndv  100000k fpp    0.1%           2.89e+04  3.2e+04 3.33e+04      0.18X     0.196X     0.201X
//    absent  ndv  100000k fpp    0.1%           4.56e+04 4.78e+04 4.86e+04     0.285X     0.292X     0.293X

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
  cout << endl << "Without AVX2:" << endl << endl;
  CpuInfo::TempDisable t(CpuInfo::AVX2);
  RunBenchmarks();
}
