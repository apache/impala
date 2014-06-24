// Copyright 2012 Cloudera Inc.
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

#include <boost/scoped_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <gtest/gtest.h>

#include <string>

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/row-batch.h"
#include "runtime/tmp-file-mgr.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace boost;
using namespace std;

const int BATCH_SIZE = 250;

namespace impala {

class BufferedTupleStreamTest : public testing::Test {
 protected:
  virtual void SetUp() {
    block_mgr_ = NULL;
    exec_env_.reset(new ExecEnv);
    exec_env_->disk_io_mgr()->Init(&tracker_);
    runtime_state_.reset(
        new RuntimeState(TPlanFragmentInstanceCtx(), "", exec_env_.get()));

    DescriptorTblBuilder builder(&pool_);
    builder.DeclareTuple() << TYPE_INT;
    DescriptorTbl* tbl = builder.Build();
    vector<bool> nullable_tuples(1, false);
    vector<TTupleId> tuple_ids(1, static_cast<TTupleId>(0));
    desc_ = pool_.Add(new RowDescriptor(*tbl, tuple_ids, nullable_tuples));
  }

  virtual void TearDown() {
    if (block_mgr_ != NULL) block_mgr_->Close();
    block_mgr_parent_tracker_.reset();
    runtime_state_.reset();
    exec_env_.reset();
    delete block_mgr_;
  }

  void CreateMgr(int64_t limit, int block_size) {
    Status status = BufferedBlockMgr::Create(runtime_state_.get(),
        &tracker_, runtime_state_->runtime_profile(), limit, block_size, &block_mgr_);
    EXPECT_TRUE(status.ok());
    status = block_mgr_->RegisterClient(0, NULL, &client_);
    EXPECT_TRUE(status.ok());
  }

  RowBatch* CreateIntBatch(int start_val, int num_rows) {
    RowBatch* batch = pool_.Add(new RowBatch(*desc_, num_rows, &tracker_));
    int32_t* tuple_mem = reinterpret_cast<int32_t*>(
        batch->tuple_data_pool()->Allocate(sizeof(int32_t) * num_rows));
    for (int i = 0; i < num_rows; ++i) {
      int idx = batch->AddRow();
      TupleRow* row = batch->GetRow(idx);
      tuple_mem[i] = i + start_val;
      row->SetTuple(0, reinterpret_cast<Tuple*>(&tuple_mem[i]));
      batch->CommitLastRow();
    }
    return batch;
  }

  // Test adding num_batches of ints to the stream and reading them back.
  void TestIntValues(int num_batches) {
    BufferedTupleStream stream(runtime_state_.get(), *desc_, block_mgr_, client_);
    Status status = stream.Init();
    ASSERT_TRUE(status.ok());
    status = stream.Unpin();
    ASSERT_TRUE(status.ok());

    // Add rows to the stream
    for (int i = 0; i < num_batches; ++i) {
      RowBatch* batch = CreateIntBatch(i * BATCH_SIZE, BATCH_SIZE);
      for (int j = 0; j < batch->num_rows(); ++j) {
        bool b = stream.AddRow(batch->GetRow(j));
        ASSERT_TRUE(b);
      }
      // Reset the batch to make sure the stream handles the memory correctly.
      batch->Reset();
    }

    status = stream.PrepareForRead();
    ASSERT_TRUE(status.ok());

    // Read all the rows back
    vector<int32_t> results;
    bool eos = false;
    RowBatch batch(*desc_, BATCH_SIZE, &tracker_);
    do {
      batch.Reset();
      status = stream.GetNext(&batch, &eos);
      EXPECT_TRUE(status.ok());
      for (int i = 0; i < batch.num_rows(); ++i) {
        TupleRow* row = batch.GetRow(i);
        Tuple* tuple = row->GetTuple(0);
        int32_t v = *reinterpret_cast<int32_t*>(tuple);
        results.push_back(v);
      }
    } while (!eos);

    // Verify result
    EXPECT_EQ(results.size(), BATCH_SIZE * num_batches);
    sort(results.begin(), results.end());
    for (int i = 0; i < results.size(); ++i) {
      ASSERT_EQ(results[i], i);
    }

    stream.Close();
  }

  scoped_ptr<ExecEnv> exec_env_;
  scoped_ptr<RuntimeState> runtime_state_;
  scoped_ptr<MemTracker> block_mgr_parent_tracker_;

  BufferedBlockMgr* block_mgr_;
  BufferedBlockMgr::Client* client_;

  MemTracker tracker_;
  ObjectPool pool_;
  RowDescriptor* desc_;
};

// Basic API test. No data should be going to disk.
TEST_F(BufferedTupleStreamTest, Basic) {
  CreateMgr(-1, 8 * 1024 * 1024);
  TestIntValues(1);
  TestIntValues(10);
  TestIntValues(100);
}

// Test with only 1 buffer.
TEST_F(BufferedTupleStreamTest, OneBufferSpill) {
  // Each buffer can only hold 100 ints, so this spills quite often.
  int buffer_size = 100 * sizeof(int32_t);
  CreateMgr(buffer_size, buffer_size);
  TestIntValues(1);
  TestIntValues(10);
}

// Test with a few buffers.
TEST_F(BufferedTupleStreamTest, ManyBufferSpill) {
  int buffer_size = 100 * sizeof(int32_t);
  CreateMgr(10 * buffer_size, buffer_size);
  TestIntValues(1);
  TestIntValues(10);
  TestIntValues(100);
}

// TODO: more tests.
//  - The stream can operate with many modes and
//  - more tuple layouts.

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true);
  impala::InitFeSupport();
  impala::TmpFileMgr::Init();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
