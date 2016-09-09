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

#include <gtest/gtest.h>

#include "hash-store.h"
#include "standard-hash-table.h"
#include "tuple-types.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/stopwatch.h"

using namespace impala;

namespace impala {

// Tests performance of hash aggregation with HashStore, which grows to contain multiple
// hash tables as needed. Allows exploring how buffer size and size of the keyspace
// affect performance.
class GrowingTest : public testing::Test {
 public:
  static const uint64_t NUM_PROBE_TUPLES = 100000000; // 10^8

  template <int buffer_size>
  inline static uint64_t AggregateTest(uint64_t num_probe_tuples, int max_id,
                                       int num_tables) {
    ProbeTuple* tuples = GenTuples(num_probe_tuples, max_id);
    HashStore<buffer_size> hs;
    StopWatch watch;
    watch.Start();
    hs.Aggregate(tuples, num_probe_tuples);
    watch.Stop();
    SanityCheck<buffer_size>(hs, num_probe_tuples, max_id, num_tables);
    free(tuples);
    return watch.Ticks();
  }

  // Confirm that hs appears to be the correct result of aggregating num_probe_tuples,
  // with a keyspace of size max_id, with a fanout into num_tables tables.
  template <int buffer_size>
  static void SanityCheck(const HashStore<buffer_size> & hs,
                          uint64_t num_probe_tuples, int max_id, int num_tables) {
    uint64_t total_count = 0;
    for (int i = 0; i < hs.num_tables_; ++i) {
      StandardHashTable& ht = hs.tables_[i];
      for (StandardHashTable::Iterator it = ht.Begin(); it.HasNext(); it.Next()) {
        total_count += it.GetRow()->count;
      }
    }
    ASSERT_EQ(num_probe_tuples, total_count);
    ASSERT_EQ(num_tables, hs.num_tables_)
      << "You do not have the number of tables that you hoped.\n"
      << "This could mean that there weren't enough probe tuples to fill the keyspace, "
      << "skewed hashing lead a hash table that we expected not to overflow to overflow, "
      << "or a genuine bug.";
    if (buffer_size > 0) {
      ASSERT_EQ(buffer_size, sizeof(typename HashStore<buffer_size>::Buffer));
    }
  }

  template <int buffer_size>
  inline static uint64_t NextTestCase(int build_tuples, uint64_t prev, int num_tables) {
    uint64_t time = AggregateTest<buffer_size>(NUM_PROBE_TUPLES, build_tuples, num_tables);
    int64_t delta;
    if (prev == 0) {
      // just print 0 for the delta the first time.
      delta = 0;
    }
    else {
      delta = static_cast<int64_t>(time) - static_cast<int64_t>(prev);
    }
    LOG(ERROR) << build_tuples << "\t"
               << PrettyPrinter::Print(time, TUnit::CPU_TICKS)
               << "\t" << PrettyPrinter::Print(delta, TUnit::CPU_TICKS);
    return time;
  }

  // Run a test aggregation with buffers of size buffer_size bytes.
  // Try the full range of working set size (and thus fanout) that we can do with
  // single-level fanout.
  template <int buffer_size>
  inline static void Test() {
    LOG(ERROR) << "Buffer size " << HashStore<buffer_size>::BUFFER_SIZE << " tuples ("
               << PrettyPrinter::Print(buffer_size, TUnit::BYTES)
               << "):";
    LOG(ERROR) << "#BuildTuples\tTime\tdTime";
    uint64_t prev = 0;
    // only go up to 2^12 hashtables because after that, there are at least as many
    // buffers as L2 cache lines. We won't let this happen; we'll do multi-level fanout.
    // Note that we should probably allow 2 cache lines per buffer (since a write might
    // span a line break), plus we need some memory aside from the buffers, so we'll
    // actually stop at 2^11, or maybe 2^12. And we might want to keep them in L1,
    // in which case we'll stop at 2^7 or so.
    for (int num_tables = 1; num_tables <= 1<<12; num_tables *= 2) {
      // how many total build tuples we'll use to fill num_tables tables
      // Needs to be comfortably less than the total capacity of num_tables tables
      // (the below expression without the constant factor multiplied by it)
      // because the hashes will not be spread perfectly.
      // But should be more than 1/2 of that expression because otherwise if the hashes
      // spread out perfectly, it will fit in num_tables / 2 and not give us the fanout
      // we expect. (A test will fail in this case so that we know.)
      int build_tuples = (StandardHashTable::NODES * num_tables / 10) * 8;
      prev = NextTestCase<buffer_size>(build_tuples, prev, num_tables);
    }
  }
};

}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  CpuInfo::Init();
  LOG(ERROR) << "Testing with " << GrowingTest::NUM_PROBE_TUPLES << " tuples.";
  return RUN_ALL_TESTS();
}

TEST(GrowingTest, All) {
  // test without buffers
  GrowingTest::Test<0>();
  // test with one of the best buffer sizes
  // Make it slightly more than a power of 2 cache lines so that they are offset.
  GrowingTest::Test< (1 << 13) + 3 * 64 >();
}
