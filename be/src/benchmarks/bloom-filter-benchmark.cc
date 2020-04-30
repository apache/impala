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

#include "gen-cpp/data_stream_service.pb.h"
#include "kudu/rpc/rpc_controller.h"
#include "runtime/bufferpool/buffer-allocator.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "util/benchmark.h"
#include "util/bloom-filter.h"
#include "common/init.h"

#include "common/names.h"

// kudu::BlockBloomFilter::kCpu is a static variable and is initialized once.
// To temporarily disable AVX2 for Bloom Filter in runtime testing, set flag
// disable_blockbloomfilter_avx2 as true. See kudu::BlockBloomFilter::has_avx2().
// This flag has no effect if the target CPU doesn't support AVX2.
DECLARE_bool(disable_blockbloomfilter_avx2);

using namespace std;
using namespace impala;

using kudu::rpc::RpcController;

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
// This benchmark must be executed only in RELEASE mode. Since it executes some codepath
// which would not occur in Impala's execution, it crashes due to a DCHECK in DEBUG mode.
//
// Machine Info: Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz
//
// initialize:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           5.77e+03 5.81e+03 5.84e+03         1X         1X         1X
//            ndv      10k fpp    1.0%           3.08e+03  3.1e+03 3.13e+03     0.534X     0.534X     0.536X
//            ndv      10k fpp    0.1%           1.24e+03 1.25e+03 1.27e+03     0.216X     0.216X     0.217X
//            ndv    1000k fpp   10.0%               4.71     4.71     4.71  0.000816X  0.000811X  0.000805X
//            ndv    1000k fpp    1.0%               2.31     2.35     2.35    0.0004X  0.000405X  0.000403X
//            ndv    1000k fpp    0.1%               2.35     2.35     2.35  0.000408X  0.000405X  0.000403X
//            ndv  100000k fpp   10.0%             0.0926   0.0935   0.0935  1.61e-05X  1.61e-05X   1.6e-05X
//            ndv  100000k fpp    1.0%             0.0926   0.0935   0.0935  1.61e-05X  1.61e-05X   1.6e-05X
//            ndv  100000k fpp    0.1%             0.0481   0.0481   0.0481  8.33e-06X  8.28e-06X  8.23e-06X
//
// With AVX2:
//
// insert:                    Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%            2.1e+05 2.11e+05 2.13e+05         1X         1X         1X
//            ndv      10k fpp    1.0%           2.16e+05 2.18e+05 2.19e+05      1.03X      1.03X      1.03X
//            ndv      10k fpp    0.1%           2.12e+05 2.14e+05 2.16e+05      1.01X      1.01X      1.01X
//            ndv    1000k fpp   10.0%           1.98e+05 1.99e+05 2.01e+05     0.943X     0.942X     0.945X
//            ndv    1000k fpp    1.0%           1.96e+05 1.98e+05 1.99e+05     0.935X     0.936X     0.937X
//            ndv    1000k fpp    0.1%           1.96e+05 1.97e+05 1.99e+05     0.935X     0.934X     0.936X
//            ndv  100000k fpp   10.0%           5.63e+04  5.8e+04 6.18e+04     0.269X     0.274X     0.291X
//            ndv  100000k fpp    1.0%           5.64e+04 5.84e+04 6.24e+04     0.269X     0.276X     0.293X
//            ndv  100000k fpp    0.1%           5.56e+04 5.75e+04 5.86e+04     0.265X     0.272X     0.275X
//
// find:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//    present ndv      10k fpp   10.0%           1.97e+05 1.98e+05 1.99e+05         1X         1X         1X
//    absent  ndv      10k fpp   10.0%           1.99e+05 2.01e+05 2.03e+05      1.01X      1.01X      1.02X
//    present ndv      10k fpp    1.0%           1.97e+05 1.98e+05    2e+05         1X         1X         1X
//    absent  ndv      10k fpp    1.0%              2e+05 2.01e+05 2.03e+05      1.02X      1.02X      1.02X
//    present ndv      10k fpp    0.1%           1.97e+05 1.99e+05    2e+05         1X         1X         1X
//    absent  ndv      10k fpp    0.1%              2e+05 2.02e+05 2.03e+05      1.02X      1.02X      1.02X
//    present ndv    1000k fpp   10.0%           1.75e+05 1.77e+05 1.78e+05     0.891X     0.893X     0.893X
//    absent  ndv    1000k fpp   10.0%           1.78e+05  1.8e+05 1.81e+05     0.907X     0.907X     0.907X
//    present ndv    1000k fpp    1.0%            1.8e+05 1.82e+05 1.83e+05     0.917X     0.917X     0.919X
//    absent  ndv    1000k fpp    1.0%           1.84e+05 1.86e+05 1.88e+05     0.937X     0.939X     0.941X
//    present ndv    1000k fpp    0.1%           1.69e+05  1.7e+05 1.71e+05     0.857X     0.859X     0.858X
//    absent  ndv    1000k fpp    0.1%            1.7e+05 1.72e+05 1.74e+05     0.866X      0.87X     0.871X
//    present ndv  100000k fpp   10.0%           5.34e+04 5.53e+04 7.21e+04     0.271X     0.279X     0.362X
//    absent  ndv  100000k fpp   10.0%           5.05e+04 5.28e+04 5.52e+04     0.257X     0.267X     0.277X
//    present ndv  100000k fpp    1.0%           5.43e+04 5.74e+04 8.65e+04     0.276X      0.29X     0.434X
//    absent  ndv  100000k fpp    1.0%           5.09e+04 5.42e+04 5.73e+04     0.259X     0.274X     0.288X
//    present ndv  100000k fpp    0.1%           5.11e+04 5.24e+04 6.69e+04      0.26X     0.265X     0.336X
//    absent  ndv  100000k fpp    0.1%           4.93e+04 5.02e+04  5.1e+04     0.251X     0.254X     0.256X
//
// union:                     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           6.76e+05  6.8e+05 6.88e+05         1X         1X         1X
//            ndv      10k fpp    1.0%           6.77e+05 6.81e+05 6.87e+05         1X         1X     0.998X
//            ndv      10k fpp    0.1%           6.78e+05 6.82e+05 6.86e+05         1X         1X     0.996X
//            ndv    1000k fpp   10.0%           6.78e+05 6.82e+05 6.88e+05         1X         1X         1X
//            ndv    1000k fpp    1.0%           6.78e+05 6.83e+05 6.89e+05         1X         1X         1X
//            ndv    1000k fpp    0.1%           6.77e+05  6.8e+05 6.89e+05         1X         1X         1X
//            ndv  100000k fpp   10.0%           6.77e+05 6.81e+05 6.88e+05         1X         1X     0.999X
//            ndv  100000k fpp    1.0%           6.77e+05 6.85e+05 6.89e+05         1X      1.01X         1X
//            ndv  100000k fpp    0.1%           6.76e+05  6.8e+05 6.88e+05         1X         1X         1X
//
//
// Without AVX or AVX2:
//
// insert:                    Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           9.07e+04 9.12e+04 9.22e+04         1X         1X         1X
//            ndv      10k fpp    1.0%           9.08e+04 9.13e+04 9.21e+04         1X         1X     0.999X
//            ndv      10k fpp    0.1%           9.04e+04 9.08e+04 9.15e+04     0.997X     0.996X     0.993X
//            ndv    1000k fpp   10.0%           8.85e+04 8.92e+04    9e+04     0.976X     0.978X     0.976X
//            ndv    1000k fpp    1.0%            8.8e+04 8.89e+04 8.94e+04     0.971X     0.975X      0.97X
//            ndv    1000k fpp    0.1%           8.79e+04 8.83e+04 8.92e+04      0.97X     0.968X     0.968X
//            ndv  100000k fpp   10.0%           3.64e+04 3.82e+04 4.26e+04     0.401X     0.419X     0.462X
//            ndv  100000k fpp    1.0%           3.67e+04 3.94e+04 4.52e+04     0.405X     0.432X     0.491X
//            ndv  100000k fpp    0.1%           3.58e+04 3.75e+04 4.58e+04     0.395X     0.411X     0.497X
//
// find:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//    present ndv      10k fpp   10.0%           1.34e+05 1.35e+05 1.36e+05         1X         1X         1X
//    absent  ndv      10k fpp   10.0%           7.83e+04 7.87e+04 7.94e+04     0.584X     0.583X     0.584X
//    present ndv      10k fpp    1.0%           1.35e+05 1.36e+05 1.37e+05      1.01X         1X      1.01X
//    absent  ndv      10k fpp    1.0%           8.79e+04 8.84e+04 8.93e+04     0.656X     0.655X     0.657X
//    present ndv      10k fpp    0.1%           1.34e+05 1.35e+05 1.36e+05         1X         1X         1X
//    absent  ndv      10k fpp    0.1%           1.38e+05 1.39e+05  1.4e+05      1.03X      1.03X      1.03X
//    present ndv    1000k fpp   10.0%            9.6e+04 9.66e+04 9.77e+04     0.716X     0.716X     0.719X
//    absent  ndv    1000k fpp   10.0%           5.43e+04 5.47e+04 5.51e+04     0.405X     0.405X     0.406X
//    present ndv    1000k fpp    1.0%           9.48e+04 9.56e+04 9.65e+04     0.707X     0.709X     0.711X
//    absent  ndv    1000k fpp    1.0%           7.95e+04 8.01e+04 8.06e+04     0.593X     0.593X     0.594X
//    present ndv    1000k fpp    0.1%           9.47e+04 9.55e+04 9.64e+04     0.707X     0.708X      0.71X
//    absent  ndv    1000k fpp    0.1%           7.93e+04 7.98e+04 8.05e+04     0.592X     0.592X     0.592X
//    present ndv  100000k fpp   10.0%           3.34e+04 3.46e+04 3.81e+04     0.249X     0.257X      0.28X
//    absent  ndv  100000k fpp   10.0%           3.61e+04 3.81e+04 4.04e+04     0.269X     0.282X     0.298X
//    present ndv  100000k fpp    1.0%           3.86e+04 4.19e+04 4.69e+04     0.288X     0.311X     0.346X
//    absent  ndv  100000k fpp    1.0%            3.6e+04 3.73e+04 4.12e+04     0.268X     0.276X     0.304X
//    present ndv  100000k fpp    0.1%           3.59e+04 3.74e+04 3.97e+04     0.268X     0.277X     0.292X
//    absent  ndv  100000k fpp    0.1%           4.82e+04 4.92e+04 5.11e+04      0.36X     0.365X     0.376X
//
// union:                     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//            ndv      10k fpp   10.0%           6.77e+05 6.81e+05 6.89e+05         1X         1X         1X
//            ndv      10k fpp    1.0%           6.77e+05 6.82e+05 6.87e+05         1X         1X     0.998X
//            ndv      10k fpp    0.1%           6.77e+05 6.82e+05 6.89e+05         1X         1X         1X
//            ndv    1000k fpp   10.0%           6.77e+05  6.8e+05 6.89e+05     0.999X     0.999X         1X
//            ndv    1000k fpp    1.0%           6.77e+05  6.8e+05 6.88e+05     0.999X     0.999X     0.998X
//            ndv    1000k fpp    0.1%           6.78e+05 6.82e+05 6.87e+05         1X         1X     0.997X
//            ndv  100000k fpp   10.0%           6.78e+05 6.82e+05 6.87e+05         1X         1X     0.998X
//            ndv  100000k fpp    1.0%           6.77e+05  6.8e+05 6.87e+05     0.999X     0.998X     0.998X
//            ndv  100000k fpp    0.1%           6.77e+05 6.82e+05 6.87e+05     0.999X         1X     0.997X
//
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
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "");
  ExecEnv* env = ExecEnv::GetInstance();
  BufferPool::ClientHandle client;
  CHECK(env->buffer_pool()
            ->RegisterClient("", nullptr, env->buffer_reservation(), nullptr,
                numeric_limits<int64_t>::max(), profile, &client).ok());
  int* d = reinterpret_cast<int*>(data);
  CHECK(client.IncreaseReservation(BloomFilter::GetExpectedMemoryUsed(*d)));
  for (int i = 0; i < batch_size; ++i) {
    BloomFilter bf(&client);
    CHECK(bf.Init(*d, 0).ok());
    bf.Close();
  }
  env->buffer_pool()->DeregisterClient(&client);
  pool.Clear();
}

}  // namespace initialize

// Benchmark insert
namespace insert {

struct TestData {
  explicit TestData(int log_bufferpool_size, BufferPool::ClientHandle* client)
    : bf(client), data(1ull << 20) {
    CHECK(bf.Init(log_bufferpool_size, 0).ok());
    for (size_t i = 0; i < data.size(); ++i) {
      data[i] = MakeRand();
    }
  }

  ~TestData() { bf.Close(); }

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
  TestData(int log_bufferpool_size, BufferPool::ClientHandle* client, size_t size)
    : bf(client),
      vec_mask((1ull << static_cast<int>(floor(log2(size)))) - 1),
      present(size),
      absent(size),
      result(0) {
    CHECK(bf.Init(log_bufferpool_size, 0).ok());
    for (size_t i = 0; i < size; ++i) {
      present[i] = MakeRand();
      absent[i] = MakeRand();
      bf.Insert(present[i]);
    }
  }

  ~TestData() { bf.Close(); }

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
  explicit TestData(int log_bufferpool_size, BufferPool::ClientHandle* client) {
    BloomFilter bf(client);
    CHECK(bf.Init(log_bufferpool_size, 0).ok());

    RpcController controller1;
    RpcController controller2;
    BloomFilter::ToProtobuf(&bf, &controller1, &pbf1);
    BloomFilter::ToProtobuf(&bf, &controller2, &pbf2);

    // Need to set 'always_false_' of pbf2 to false because
    // (i) 'always_false_' of a BloomFilter is set to true when the Bloom filter
    // hasn't had any elements inserted (since nothing is inserted to the
    /// BloomFilter bf),
    // (ii) ToProtobuf() will set 'always_false_' of a BloomFilterPB
    // to true, and
    // (iii) Or() will check 'always_false_' of the output BloomFilterPB is not true
    /// before performing the corresponding bit operations.
    /// The field 'always_false_' was added by IMPALA-5789, which aims to allow
    /// an HdfsScanner to early terminate the scan at file and split granularities.
    pbf2.set_always_false(false);

    int64_t directory_size = BloomFilter::GetExpectedMemoryUsed(log_bufferpool_size);
    string d1(reinterpret_cast<const char*>(bf.GetBlockBloomFilter()->directory().data()),
        directory_size);
    string d2(reinterpret_cast<const char*>(bf.GetBlockBloomFilter()->directory().data()),
        directory_size);

    directory1 = d1;
    directory2 = d2;

    bf.Close();
  }

  BloomFilterPB pbf1, pbf2;
  string directory1, directory2;
};

void Benchmark(int batch_size, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  for (int i = 0; i < batch_size; ++i) {
    BloomFilter::Or(d->pbf1, reinterpret_cast<const uint8_t*>((d->directory1).data()),
        &(d->pbf2), reinterpret_cast<uint8_t*>(const_cast<char*>((d->directory2).data())),
        d->directory1.size());
  }
}

} // namespace either

void RunBenchmarks() {
  ObjectPool pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&pool, "");
  ExecEnv* env = ExecEnv::GetInstance();
  BufferPool::ClientHandle client;
  CHECK(env->buffer_pool()
            ->RegisterClient("", nullptr, env->buffer_reservation(), nullptr,
                numeric_limits<int64_t>::max(), profile, &client).ok());
  char name[120];

  {
    Benchmark suite("insert");
    vector<unique_ptr<insert::TestData> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (int log10fpp = -1; log10fpp >= -3; --log10fpp) {
        const double fpp = pow(10, log10fpp);
        int log_required_size = BloomFilter::MinLogSpace(ndv, fpp);
        CHECK(client.IncreaseReservation(
            BloomFilter::GetExpectedMemoryUsed(log_required_size)));
        testdata.emplace_back(
            new insert::TestData(log_required_size, &client));
        snprintf(name, sizeof(name), "ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, insert::Benchmark, testdata.back().get());
      }
    }
    cout << suite.Measure() << endl;
  }
  CHECK(client.DecreaseReservationTo(numeric_limits<int64_t>::max(), 0).ok());

  {
    Benchmark suite("find");
    vector<unique_ptr<find::TestData> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (int log10fpp = -1; log10fpp >= -3; --log10fpp) {
        const double fpp = pow(10, log10fpp);
        int log_required_size = BloomFilter::MinLogSpace(ndv, fpp);
        CHECK(client.IncreaseReservation(
            BloomFilter::GetExpectedMemoryUsed(log_required_size)));
        testdata.emplace_back(
            new find::TestData(BloomFilter::MinLogSpace(ndv, fpp), &client, ndv));
        snprintf(name, sizeof(name), "present ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, find::Present, testdata.back().get());

        snprintf(name, sizeof(name), "absent  ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, find::Absent, testdata.back().get());
      }
    }
    cout << suite.Measure() << endl;
  }
  CHECK(client.DecreaseReservationTo(numeric_limits<int64_t>::max(), 0).ok());

  {
    Benchmark suite("union", false /* micro_heuristics */);
    vector<unique_ptr<either::TestData> > testdata;
    for (int ndv = 10000; ndv <= 100 * 1000 * 1000; ndv *= 100) {
      for (int log10fpp = -1; log10fpp >= -3; --log10fpp) {
        const double fpp = pow(10, log10fpp);
        int log_required_size = BloomFilter::MinLogSpace(ndv, fpp);
        CHECK(client.IncreaseReservation(
            BloomFilter::GetExpectedMemoryUsed(log_required_size)));
        testdata.emplace_back(new either::TestData(
            BloomFilter::MinLogSpace(ndv, fpp), &client));
        snprintf(name, sizeof(name), "ndv %7dk fpp %6.1f%%", ndv/1000, fpp*100);
        suite.AddBenchmark(name, either::Benchmark, testdata.back().get());
      }
    }
    cout << suite.Measure() << endl;
  }

  CHECK(client.DecreaseReservationTo(numeric_limits<int64_t>::max(), 0).ok());
  env->buffer_pool()->DeregisterClient(&client);
  pool.Clear();
}

int main(int argc, char **argv) {
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  TestEnv test_env;
  int64_t min_page_size = 8;
  int64_t buffer_bytes_limit = 4L * 1024 * 1024 * 1024;
  test_env.SetBufferPoolArgs(min_page_size, buffer_bytes_limit);
  CHECK(test_env.Init().ok());

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
  FLAGS_disable_blockbloomfilter_avx2 = false;
  RunBenchmarks();
  cout << endl << "Without AVX or AVX2:" << endl << endl;
  FLAGS_disable_blockbloomfilter_avx2 = true;
  RunBenchmarks();
}
