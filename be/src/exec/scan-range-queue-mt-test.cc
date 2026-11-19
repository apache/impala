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

#include <vector>

#include "common/object-pool.h"
#include "exec/hdfs-scan-node-base.h"
#include "exec/scan-range-queue-mt.h"
#include "runtime/io/request-ranges.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

using namespace impala::io;

namespace impala {

static const int64_t FOOTER_SIZE = 100 * 1024;

class ScanRangeQueueMtTest : public testing::Test {
 protected:
  /// Returns a range of 'len' bytes at 'offset' of 'filename'. Doesn't use
  /// ScanRange::AllocateScanRange() because that needs the DiskIoMgr of the ExecEnv.
  ScanRange* MakeRange(const char* filename, int64_t len, int64_t offset,
      const ScanRange* original_split) {
    ScanRangeMetadata* metadata = pool_.Add(new ScanRangeMetadata(0, original_split));
    ScanRange* range = pool_.Add(new ScanRange);
    range->Reset(ScanRange::FileInfo{filename}, len, offset, 0, false,
        BufferOpts(BufferOpts::NO_CACHING), metadata);
    return range;
  }

  /// Returns a split of 'len' bytes of the file 'filename'.
  ScanRange* MakeSplit(const char* filename, int64_t len) {
    return MakeRange(filename, len, 0, nullptr);
  }

  /// Returns a footer range for 'split' like HdfsScanner::IssueFooterRanges() does.
  ScanRange* MakeFooterRange(const ScanRange* split) {
    int64_t footer_size = min(FOOTER_SIZE, split->len());
    return MakeRange(split->file(), footer_size, split->len() - footer_size, split);
  }

  /// Dequeues all ranges from 'queue' and returns them in order.
  static vector<ScanRange*> DequeueAll(ScanRangeQueueMt* queue) {
    vector<ScanRange*> result;
    while (ScanRange* range = queue->Dequeue()) result.push_back(range);
    EXPECT_TRUE(queue->Empty());
    return result;
  }

  ObjectPool pool_;
};

// The footer ranges of Parquet/ORC files all have about the same length. They must be
// ordered by the lengths of the splits they stand for (IMPALA-11539 LPT scheduling).
TEST_F(ScanRangeQueueMtTest, FooterRangesOrderedBySplitLength) {
  ScanRange* small_split = MakeSplit("small", 10L * 1024 * 1024);
  ScanRange* large_split = MakeSplit("large", 300L * 1024 * 1024);
  ScanRange* medium_split = MakeSplit("medium", 50L * 1024 * 1024);
  ScanRange* tiny_split = MakeSplit("tiny", 1024);
  ScanRange* small_footer = MakeFooterRange(small_split);
  ScanRange* large_footer = MakeFooterRange(large_split);
  ScanRange* medium_footer = MakeFooterRange(medium_split);
  ScanRange* tiny_footer = MakeFooterRange(tiny_split);
  EXPECT_EQ(FOOTER_SIZE, small_footer->bytes_to_read());
  EXPECT_EQ(FOOTER_SIZE, large_footer->bytes_to_read());
  EXPECT_EQ(300L * 1024 * 1024, ScanRangeQueueMt::RangeWeight(large_footer));
  EXPECT_EQ(1024, ScanRangeQueueMt::RangeWeight(tiny_footer));

  ScanRangeQueueMt queue;
  queue.EnqueueRanges({small_footer, tiny_footer, large_footer}, false);
  queue.EnqueueRanges({medium_footer}, false);
  vector<ScanRange*> expected = {large_footer, medium_footer, small_footer, tiny_footer};
  EXPECT_EQ(expected, DequeueAll(&queue));
}

// Ranges without 'original_split' (e.g. text splits) are ordered by their own length,
// also when they are mixed with footer ranges.
TEST_F(ScanRangeQueueMtTest, MixedRanges) {
  ScanRange* text_split = MakeSplit("text", 20L * 1024 * 1024);
  ScanRange* parquet_split = MakeSplit("parquet", 30L * 1024 * 1024);
  ScanRange* parquet_footer = MakeFooterRange(parquet_split);
  ScanRange* high_prio = MakeSplit("high_prio", 1024);
  EXPECT_EQ(20L * 1024 * 1024, ScanRangeQueueMt::RangeWeight(text_split));

  ScanRangeQueueMt queue;
  queue.EnqueueRanges({text_split, parquet_footer}, false);
  queue.EnqueueRanges({high_prio}, true);
  vector<ScanRange*> expected = {high_prio, parquet_footer, text_split};
  EXPECT_EQ(expected, DequeueAll(&queue));
}

}
