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
//            ndv      10k fpp   10.0%           5.92e+03 5.98e+03 6.03e+03         1X         1X         1X
//            ndv      10k fpp    1.0%           3.17e+03 3.24e+03 3.26e+03     0.535X     0.542X     0.541X
//            ndv      10k fpp    0.1%           1.16e+03 1.17e+03 1.18e+03     0.195X     0.195X     0.195X
//            ndv    1000k fpp   10.0%               3.85     3.93     3.93  0.000651X  0.000657X  0.000652X
//            ndv    1000k fpp    1.0%               2.08     2.12     2.12  0.000351X  0.000354X  0.000351X
//            ndv    1000k fpp    0.1%               2.08     2.12     2.12  0.000351X  0.000354X  0.000351X
//            ndv  100000k fpp   10.0%             0.0299   0.0304    0.031  5.06e-06X  5.09e-06X  5.14e-06X
//            ndv  100000k fpp    1.0%             0.0295   0.0306   0.0311  4.98e-06X  5.12e-06X  5.15e-06X
//            ndv  100000k fpp    0.1%             0.0151   0.0153   0.0154  2.55e-06X  2.55e-06X  2.55e-06X
//
// With AVX2:
//
// insert:                    Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           1.22e+05 1.23e+05 1.24e+05         1X         1X         1X
//            ndv      10k fpp    1.0%           1.22e+05 1.23e+05 1.24e+05     0.998X         1X         1X
//            ndv      10k fpp    0.1%           1.22e+05 1.23e+05 1.24e+05         1X         1X         1X
//            ndv    1000k fpp   10.0%           1.16e+05 1.18e+05  1.2e+05      0.95X     0.964X     0.965X
//            ndv    1000k fpp    1.0%           1.14e+05 1.15e+05 1.16e+05     0.935X     0.941X     0.939X
//            ndv    1000k fpp    0.1%           1.14e+05 1.16e+05 1.17e+05     0.939X     0.945X     0.943X
//            ndv  100000k fpp   10.0%           3.35e+04 4.22e+04  5.3e+04     0.275X     0.344X     0.428X
//            ndv  100000k fpp    1.0%           3.16e+04 4.77e+04 5.78e+04      0.26X     0.388X     0.466X
//            ndv  100000k fpp    0.1%              3e+04  3.7e+04 4.66e+04     0.246X     0.301X     0.376X
//
// find:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//    present ndv      10k fpp   10.0%           1.16e+05 1.17e+05 1.18e+05         1X         1X         1X
//    absent  ndv      10k fpp   10.0%           1.15e+05 1.17e+05 1.18e+05     0.996X     0.998X         1X
//    present ndv      10k fpp    1.0%           1.16e+05 1.17e+05 1.18e+05     0.999X     0.996X         1X
//    absent  ndv      10k fpp    1.0%           1.16e+05 1.17e+05 1.18e+05         1X     0.998X     0.999X
//    present ndv      10k fpp    0.1%           1.16e+05 1.17e+05 1.18e+05     0.999X     0.997X     0.997X
//    absent  ndv      10k fpp    0.1%           1.16e+05 1.17e+05 1.18e+05         1X     0.996X     0.998X
//    present ndv    1000k fpp   10.0%           1.09e+05 1.12e+05 1.14e+05     0.936X     0.958X     0.964X
//    absent  ndv    1000k fpp   10.0%           1.07e+05 1.14e+05 1.15e+05     0.921X     0.976X     0.976X
//    present ndv    1000k fpp    1.0%           1.05e+05  1.1e+05 1.12e+05     0.906X     0.943X     0.946X
//    absent  ndv    1000k fpp    1.0%           1.11e+05 1.13e+05 1.14e+05     0.961X     0.966X     0.969X
//    present ndv    1000k fpp    0.1%           9.78e+04 1.11e+05 1.12e+05     0.844X     0.944X     0.946X
//    absent  ndv    1000k fpp    0.1%           1.08e+05 1.13e+05 1.14e+05      0.93X     0.967X      0.97X
//    present ndv  100000k fpp   10.0%           3.85e+04 4.53e+04 6.12e+04     0.332X     0.387X     0.518X
//    absent  ndv  100000k fpp   10.0%           2.54e+04 3.01e+04 3.26e+04     0.219X     0.257X     0.276X
//    present ndv  100000k fpp    1.0%            3.3e+04  4.5e+04 6.06e+04     0.284X     0.384X     0.514X
//    absent  ndv  100000k fpp    1.0%           2.67e+04 3.01e+04  3.2e+04      0.23X     0.257X     0.271X
//    present ndv  100000k fpp    0.1%           3.12e+04 4.25e+04 5.15e+04     0.269X     0.362X     0.436X
//    absent  ndv  100000k fpp    0.1%           2.39e+04 2.69e+04 2.84e+04     0.206X     0.229X      0.24X
//
// union:                     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           5.43e+03 5.63e+03 5.67e+03         1X         1X         1X
//            ndv      10k fpp    1.0%           2.82e+03 2.84e+03 2.87e+03      0.52X     0.505X     0.507X
//            ndv      10k fpp    0.1%                780      803      812     0.144X     0.143X     0.143X
//            ndv    1000k fpp   10.0%               16.2     16.5     16.7   0.00298X   0.00292X   0.00294X
//            ndv    1000k fpp    1.0%               7.75     8.04     8.11   0.00143X   0.00143X   0.00143X
//            ndv    1000k fpp    0.1%               7.96     8.11     8.11   0.00147X   0.00144X   0.00143X
//            ndv  100000k fpp   10.0%              0.045   0.0472   0.0478  8.29e-06X  8.38e-06X  8.44e-06X
//            ndv  100000k fpp    1.0%              0.045   0.0474   0.0478  8.29e-06X  8.42e-06X  8.44e-06X
//            ndv  100000k fpp    0.1%              0.023   0.0235   0.0238  4.23e-06X  4.17e-06X   4.2e-06X
//
//
// Without AVX or AVX2:
//
// insert:                    Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           9.47e+04 9.52e+04  9.6e+04         1X         1X         1X
//            ndv      10k fpp    1.0%           9.45e+04 9.53e+04 9.59e+04     0.998X         1X     0.998X
//            ndv      10k fpp    0.1%            9.2e+04 9.56e+04 9.64e+04     0.972X         1X         1X
//            ndv    1000k fpp   10.0%            9.2e+04 9.46e+04 9.57e+04     0.972X     0.993X     0.997X
//            ndv    1000k fpp    1.0%           8.49e+04 9.32e+04 9.45e+04     0.896X     0.979X     0.984X
//            ndv    1000k fpp    0.1%           8.37e+04 9.35e+04 9.47e+04     0.884X     0.981X     0.986X
//            ndv  100000k fpp   10.0%           4.03e+04  5.1e+04 5.83e+04     0.425X     0.536X     0.607X
//            ndv  100000k fpp    1.0%            3.2e+04 3.95e+04 5.11e+04     0.337X     0.415X     0.532X
//            ndv  100000k fpp    0.1%           3.82e+04 4.52e+04 5.19e+04     0.404X     0.474X      0.54X
//
// find:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//    present ndv      10k fpp   10.0%           1.25e+05  1.3e+05 1.31e+05         1X         1X         1X
//    absent  ndv      10k fpp   10.0%           7.91e+04 7.99e+04 8.06e+04     0.633X     0.614X     0.613X
//    present ndv      10k fpp    1.0%           1.26e+05 1.32e+05 1.33e+05      1.01X      1.01X      1.01X
//    absent  ndv      10k fpp    1.0%           9.99e+04 1.01e+05 1.02e+05     0.799X     0.779X     0.777X
//    present ndv      10k fpp    0.1%           1.25e+05 1.29e+05 1.29e+05     0.999X     0.989X     0.985X
//    absent  ndv      10k fpp    0.1%           1.52e+05 1.66e+05 1.68e+05      1.21X      1.28X      1.28X
//    present ndv    1000k fpp   10.0%           9.23e+04 9.61e+04 9.71e+04     0.739X     0.739X     0.739X
//    absent  ndv    1000k fpp   10.0%           5.77e+04 5.84e+04 5.88e+04     0.462X     0.449X     0.448X
//    present ndv    1000k fpp    1.0%           7.25e+04 9.08e+04 9.33e+04     0.581X     0.698X      0.71X
//    absent  ndv    1000k fpp    1.0%            7.6e+04 8.97e+04 9.08e+04     0.608X      0.69X     0.691X
//    present ndv    1000k fpp    0.1%           8.65e+04 9.35e+04 9.43e+04     0.692X     0.719X     0.717X
//    absent  ndv    1000k fpp    0.1%           8.33e+04 8.98e+04 9.07e+04     0.667X      0.69X      0.69X
//    present ndv  100000k fpp   10.0%           2.74e+04 3.06e+04 3.37e+04     0.219X     0.236X     0.256X
//    absent  ndv  100000k fpp   10.0%           2.88e+04 2.98e+04 3.03e+04     0.231X     0.229X     0.231X
//    present ndv  100000k fpp    1.0%           2.29e+04 2.82e+04 2.95e+04     0.184X     0.217X     0.224X
//    absent  ndv  100000k fpp    1.0%           2.84e+04 2.94e+04 3.01e+04     0.227X     0.226X     0.229X
//    present ndv  100000k fpp    0.1%           2.34e+04 2.72e+04 3.09e+04     0.187X     0.209X     0.235X
//    absent  ndv  100000k fpp    0.1%            3.3e+04 3.84e+04 3.96e+04     0.264X     0.295X     0.301X
//
// union:                     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%            3.9e+03 3.96e+03 3.99e+03         1X         1X         1X
//            ndv      10k fpp    1.0%            1.9e+03 1.95e+03 1.96e+03     0.487X     0.492X     0.491X
//            ndv      10k fpp    0.1%                630      638      643     0.161X     0.161X     0.161X
//            ndv    1000k fpp   10.0%               15.5     15.8     15.9   0.00397X   0.00399X   0.00399X
//            ndv    1000k fpp    1.0%               7.52     7.74     7.88   0.00193X   0.00196X   0.00197X
//            ndv    1000k fpp    0.1%               7.46     7.88     7.89   0.00191X   0.00199X   0.00198X
//            ndv  100000k fpp   10.0%             0.0452   0.0474   0.0478  1.16e-05X   1.2e-05X   1.2e-05X
//            ndv  100000k fpp    1.0%             0.0452   0.0474   0.0478  1.16e-05X   1.2e-05X   1.2e-05X
//            ndv  100000k fpp    0.1%             0.0231   0.0235   0.0239  5.92e-06X  5.93e-06X  5.98e-06X

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
    BloomFilter::ToThrift(&bf, &tbf1);
    BloomFilter::ToThrift(&bf, &tbf2);
  }

  TBloomFilter tbf1, tbf2;
};

void Benchmark(int batch_size, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  for (int i = 0; i < batch_size; ++i) {
    BloomFilter::Or(d->tbf1, &d->tbf2);
  }
}

} // namespace either

void RunBenchmarks() {

  char name[120];

  {
    Benchmark suite("insert");
    vector<unique_ptr<insert::TestData> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (int log10fpp = -1; log10fpp >= -3; --log10fpp) {
        const double fpp = pow(10, log10fpp);
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
      for (int log10fpp = -1; log10fpp >= -3; --log10fpp) {
        const double fpp = pow(10, log10fpp);
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
    Benchmark suite("union", false /* micro_heuristics */);
    vector<unique_ptr<either::TestData> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (int log10fpp = -1; log10fpp >= -3; --log10fpp) {
        const double fpp = pow(10, log10fpp);
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
    Benchmark suite("initialize", false /* micro_heuristics */);
    vector<unique_ptr<int> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (int log10fpp = -1; log10fpp >= -3; --log10fpp) {
        const double fpp = pow(10, log10fpp);
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
