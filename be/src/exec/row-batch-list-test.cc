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
#include "exec/row-batch-list.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "service/fe-support.h"
#include "service/frontend.h"
#include "util/runtime-profile-counters.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

using namespace impala;

namespace impala {

// For computing tuple mem layouts.
scoped_ptr<Frontend> fe;

class RowBatchListTest : public testing::Test {
 public:
  RowBatchListTest() {}

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

  RowBatch* CreateRowBatch(int start, int end) {
    int num_rows = end - start + 1;
    RowBatch* batch = pool_.Add(new RowBatch(desc_, num_rows, &tracker_));
    int32_t* tuple_mem = reinterpret_cast<int32_t*>(
        batch->tuple_data_pool()->Allocate(sizeof(int32_t) * num_rows));

    for (int i = start; i <= end; ++i) {
      int idx = batch->AddRow();
      TupleRow* row = batch->GetRow(idx);
      *tuple_mem = i;
      row->SetTuple(0, reinterpret_cast<Tuple*>(tuple_mem));

      batch->CommitLastRow();
      tuple_mem++;
    }
    return batch;
  }

  // Validate that row contains the expected value
  void ValidateMatch(TupleRow* row, int32_t expected) {
    EXPECT_EQ(expected, *reinterpret_cast<int32_t*>(row->GetTuple(0)));
  }

  void FullScan(RowBatchList* list, int start, int end) {
    EXPECT_LT(start, end);
    RowBatchList::TupleRowIterator it = list->Iterator();
    int i = start;
    while (!it.AtEnd()) {
      EXPECT_TRUE(it.GetRow() != NULL);
      ValidateMatch(it.GetRow(), i);
      it.Next();
      ++i;
    }
    EXPECT_EQ(end, i - 1);
  }
};

// This tests inserts the rows [0->5] to list. It validates that they are all there.
TEST_F(RowBatchListTest, BasicTest) {
  RowBatchList row_list;
  RowBatch* batch = CreateRowBatch(0, 5);
  row_list.AddRowBatch(batch);
  EXPECT_EQ(row_list.total_num_rows(), 6);

  // Do a full table scan and validate returned pointers
  FullScan(&row_list, 0, 5);
}

// This tests an empty batch is handled correctly.
TEST_F(RowBatchListTest, EmptyBatchTest) {
  const int ALLOC_SIZE = 128;
  RowBatchList row_list;
  RowBatch* batch1 = pool_.Add(new RowBatch(desc_, 1, &tracker_));
  batch1->tuple_data_pool()->Allocate(ALLOC_SIZE);
  DCHECK_EQ(ALLOC_SIZE, batch1->tuple_data_pool()->total_allocated_bytes());

  row_list.AddRowBatch(batch1);
  EXPECT_EQ(row_list.total_num_rows(), 0);
  RowBatchList::TupleRowIterator it = row_list.Iterator();
  EXPECT_TRUE(it.AtEnd());

  // IMPALA-4049: list should transfer resources attached to empty batch.
  RowBatch* batch2 = pool_.Add(new RowBatch(desc_, 1, &tracker_));
  DCHECK_EQ(0, batch2->tuple_data_pool()->total_allocated_bytes());
  row_list.TransferResourceOwnership(batch2);
  DCHECK_EQ(0, batch1->tuple_data_pool()->total_allocated_bytes());
  DCHECK_EQ(ALLOC_SIZE, batch2->tuple_data_pool()->total_allocated_bytes());
}

// This tests inserts 100 row batches of 1024 rows each to list.  It validates that they
// are all there.
TEST_F(RowBatchListTest, MultipleRowBatchesTest) {
  RowBatchList row_list;

  int BATCH_SIZE = 1024;
  for (int batch_idx = 0; batch_idx < 100; ++batch_idx) {
    int batch_start = batch_idx * BATCH_SIZE;
    int batch_end = batch_start + BATCH_SIZE - 1;
    RowBatch* batch = CreateRowBatch(batch_start, batch_end);
    row_list.AddRowBatch(batch);
    EXPECT_EQ(row_list.total_num_rows(), batch_end + 1);
    FullScan(&row_list, 0, batch_end);
  }
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  InitFeSupport();
  fe.reset(new Frontend());
  return RUN_ALL_TESTS();
}
