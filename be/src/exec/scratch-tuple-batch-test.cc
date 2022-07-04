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

#include <cstdlib>
#include <cstdio>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/init.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/tuple-row.h"
#include "service/fe-support.h"
#include "service/frontend.h"
#include "scratch-tuple-batch.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

using namespace impala;

namespace impala {

scoped_ptr<Frontend> fe;

class ScratchTupleBatchTest : public testing::Test {
 public:
  ScratchTupleBatchTest() {}

  static void VerifyMicroBatches(const boost::scoped_array<bool>& selected_rows,
    ScratchMicroBatch* micro_batches, int num_batches, int gap, int batch_size) {
    EXPECT_TRUE(num_batches > 0);
    // All elements upto first micro batch should be False.
    for (int idx = 0; idx < micro_batches[0].start; idx++) {
      EXPECT_FALSE(selected_rows[idx]);
    }
    // All elements after last micro batch should be False
    for (int idx = micro_batches[num_batches - 1].end + 1; idx < batch_size; idx++) {
      EXPECT_FALSE(selected_rows[idx]);
    }
    // Verify every batch
    for (int i = 0; i < num_batches; i++) {
      const ScratchMicroBatch& batch = micro_batches[i];
      EXPECT_TRUE(batch.start <= batch.end);
      EXPECT_TRUE(batch.length == batch.end - batch.start + 1);
      EXPECT_TRUE(selected_rows[batch.start]);
      EXPECT_TRUE(selected_rows[batch.end]);
      int last_true_idx = batch.start;
      for (int j = batch.start + 1; j < batch.end; j++) {
        if (selected_rows[j]) {
          EXPECT_LE(j - last_true_idx, gap);
          last_true_idx = j;
        }
      }
    }
    // Verify any two consecutive batches i and i+1
    for (int i = 0; i < num_batches - 1; i++) {
      const ScratchMicroBatch& batch = micro_batches[i];
      const ScratchMicroBatch& nbatch = micro_batches[i + 1];
      EXPECT_TRUE(batch.end < nbatch.start);
      EXPECT_TRUE(nbatch.start - batch.end >= gap);
      // Any row in betweeen the two batches should not be selected
      for (int j = batch.end + 1; j < nbatch.start; j++) {
        EXPECT_FALSE(selected_rows[j]);
      }
    }
  }

 protected:
  MemTracker tracker_;
  ObjectPool pool_;
  RowDescriptor* desc_;

  virtual void SetUp() {
    DescriptorTblBuilder builder(fe.get(), &pool_);
    builder.DeclareTuple() << TYPE_INT;
    DescriptorTbl* desc_tbl = builder.Build();
    vector<bool> nullable_tuples(1, false);
    vector<TTupleId> tuple_id(1, (TTupleId) 0);
    desc_ = pool_.Add(new RowDescriptor(*desc_tbl, tuple_id, nullable_tuples));
  }
};

// This tests checks conversion of 'selected_rows' with interleaved
// 'true' values to 'ScratchMicroBatch';
TEST_F(ScratchTupleBatchTest, TestInterleavedMicroBatches) {
  const int BATCH_SIZE = 1024;
  scoped_ptr<ScratchTupleBatch> scratch_batch(
      new ScratchTupleBatch(*desc_, BATCH_SIZE, &tracker_));
  scratch_batch->num_tuples = BATCH_SIZE;
  // Interleaving gap
  vector<int> gaps = {2, 4, 8, 16, 32};
  for (auto n : gaps) {
    // Set every nth row as selected.
    for (int batch_idx = 0; batch_idx < 1024; ++batch_idx) {
      scratch_batch->selected_rows[batch_idx] = (batch_idx + 1) % n == 0 ? true : false;
    }
    ScratchMicroBatch micro_batches[BATCH_SIZE];
    int num_batches = scratch_batch->GetMicroBatches(10 /*Skip Length*/, micro_batches);
    ScratchTupleBatchTest::VerifyMicroBatches(
        scratch_batch->selected_rows, micro_batches, num_batches, 10, BATCH_SIZE);
  }
}

// This tests checks conversion of 'selected_rows' with clustered
// 'true' values to 'ScratchMicroBatch';
TEST_F(ScratchTupleBatchTest, TestClusteredMicroBatches) {
  const int BATCH_SIZE = 1024;
  scoped_ptr<ScratchTupleBatch> scratch_batch(
      new ScratchTupleBatch(*desc_, BATCH_SIZE, &tracker_));
  scratch_batch->num_tuples = BATCH_SIZE;
  // clustered size
  vector<int> cluster_sizes = {32, 64, 128, 256};
  for (auto n : cluster_sizes) {
    int batch_idx = 0;
    bool selected = false;
    // Set cluster of 'true' and 'false' values
    while (batch_idx < 1024) {
      int last_row = batch_idx + n;
      while (batch_idx < last_row && batch_idx < 1024) {
        scratch_batch->selected_rows[batch_idx++] = selected;
      }
      selected = !selected;
    }
    ScratchMicroBatch micro_batches[BATCH_SIZE];
    EXPECT_EQ(scratch_batch->GetMicroBatches(
        10 /*Skip Length*/, micro_batches), 1024/(n * 2));
    ScratchTupleBatchTest::VerifyMicroBatches(
        scratch_batch->selected_rows, micro_batches, 1024/(n * 2), 10, BATCH_SIZE);
  }
}
}

TEST_F(ScratchTupleBatchTest, TestRandomGeneratedMicroBatches) {
  const int BATCH_SIZE = 1024;
  scoped_ptr<ScratchTupleBatch> scratch_batch(
      new ScratchTupleBatch(*desc_, BATCH_SIZE, &tracker_));
  scratch_batch->num_tuples = BATCH_SIZE;
  // gaps to try
  vector<int> gaps = {5, 16, 29, 37, 1025};
  vector<float> selected_ratios = {0.5, 0.75, 0.1, 1.0, 0.44};
  for (int g = 0; g < gaps.size(); g++) {
    int n = gaps[g];
    // Set random locations as selected.
    srand(time(NULL));
    bool atleast_one_true = false;
    for (int batch_idx = 0; batch_idx < BATCH_SIZE; ++batch_idx) {
      scratch_batch->selected_rows[batch_idx] =
          (rand() % BATCH_SIZE) < (BATCH_SIZE * selected_ratios[g]);
      if (scratch_batch->selected_rows[batch_idx]) {
        atleast_one_true = true;
      }
    }
    // Ensure atleast one value is true when invoking 'GetMicroBatches'
    if (!atleast_one_true) {
      // Set one of the values randomly as true.
      scratch_batch->selected_rows[rand() % BATCH_SIZE] = true;
    }
    ScratchMicroBatch micro_batches[BATCH_SIZE];
    int num_batches = scratch_batch->GetMicroBatches(n, micro_batches);
    ScratchTupleBatchTest::VerifyMicroBatches(
        scratch_batch->selected_rows, micro_batches, num_batches, n, BATCH_SIZE);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  InitFeSupport();
  fe.reset(new Frontend());
  return RUN_ALL_TESTS();
}
