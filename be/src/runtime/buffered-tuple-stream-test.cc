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
#include <boost/filesystem.hpp>

#include <gtest/gtest.h>

#include <string>

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/row-batch.h"
#include "runtime/tmp-file-mgr.h"
#include "runtime/string-value.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace boost;
using namespace std;

const int BATCH_SIZE = 250;

namespace impala {

static const StringValue STRINGS[] = {
  StringValue("ABC"),
  StringValue("HELLO"),
  StringValue("123456789"),
  StringValue("FOOBAR"),
  StringValue("ONE"),
  StringValue("THREE"),
  StringValue("abcdefghijklmno"),
  StringValue("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
  StringValue("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
};

static const int NUM_STRINGS = sizeof(STRINGS) / sizeof(StringValue);

class BufferedTupleStreamTest : public testing::Test {
 protected:
  virtual void SetUp() {
    exec_env_.reset(new ExecEnv);
    exec_env_->disk_io_mgr()->Init(&tracker_);
    runtime_state_.reset(
        new RuntimeState(TPlanFragmentInstanceCtx(), "", exec_env_.get()));

    vector<bool> nullable_tuples(1, false);
    vector<TTupleId> tuple_ids(1, static_cast<TTupleId>(0));

    DescriptorTblBuilder int_builder(&pool_);
    int_builder.DeclareTuple() << TYPE_INT;
    int_desc_ = pool_.Add(new RowDescriptor(
        *int_builder.Build(), tuple_ids, nullable_tuples));

    DescriptorTblBuilder string_builder(&pool_);
    string_builder.DeclareTuple() << TYPE_STRING;
    string_desc_ = pool_.Add(new RowDescriptor(
        *string_builder.Build(), tuple_ids, nullable_tuples));

    mem_pool_.reset(new MemPool(&tracker_));
  }

  virtual void TearDown() {
    block_mgr_.reset();
    block_mgr_parent_tracker_.reset();
    runtime_state_.reset();
    exec_env_.reset();
    mem_pool_->FreeAll();
  }

  void CreateMgr(int64_t limit, int block_size) {
    Status status = BufferedBlockMgr::Create(runtime_state_.get(),
        &tracker_, runtime_state_->runtime_profile(), limit, block_size, &block_mgr_);
    EXPECT_TRUE(status.ok());
    status = block_mgr_->RegisterClient(0, NULL, runtime_state_.get(), &client_);
    EXPECT_TRUE(status.ok());
  }

  RowBatch* CreateIntBatch(int start_val, int num_rows) {
    RowBatch* batch = pool_.Add(new RowBatch(*int_desc_, num_rows, &tracker_));
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

  RowBatch* CreateStringBatch(int string_idx, int num_rows) {
    int tuple_size = sizeof(StringValue) + 1;
    RowBatch* batch = pool_.Add(new RowBatch(*string_desc_, num_rows, &tracker_));
    uint8_t* tuple_mem = batch->tuple_data_pool()->Allocate(tuple_size * num_rows);
    memset(tuple_mem, 0, tuple_size * num_rows);
    for (int i = 0; i < num_rows; ++i) {
      TupleRow* row = batch->GetRow(batch->AddRow());
      string_idx %= NUM_STRINGS;
      *reinterpret_cast<StringValue*>(tuple_mem + 1) = STRINGS[string_idx];
      ++string_idx;
      row->SetTuple(0, reinterpret_cast<Tuple*>(tuple_mem));
      batch->CommitLastRow();
      tuple_mem += tuple_size;
    }
    return batch;
  }

  void AppendValue(Tuple* t, vector<int32_t>* results) {
    results->push_back(*reinterpret_cast<int32_t*>(t));
  }

  void AppendValue(Tuple* t, vector<StringValue>* results) {
    uint8_t* mem = reinterpret_cast<uint8_t*>(t);
    StringValue sv = *reinterpret_cast<StringValue*>(mem + 1);
    uint8_t* copy = mem_pool_->Allocate(sv.len);
    memcpy(copy, sv.ptr, sv.len);
    sv.ptr = reinterpret_cast<char*>(copy);
    results->push_back(sv);
  }

  template <typename T>
  void ReadValues(BufferedTupleStream* stream, RowDescriptor* desc, vector<T>* results,
      int num_batches = -1) {
    bool eos = false;
    RowBatch batch(*desc, BATCH_SIZE, &tracker_);
    int batches_read = 0;
    do {
      batch.Reset();
      Status status = stream->GetNext(&batch, &eos);
      EXPECT_TRUE(status.ok());
      ++batches_read;
      for (int i = 0; i < batch.num_rows(); ++i) {
        AppendValue(batch.GetRow(i)->GetTuple(0), results);
      }
    } while (!eos && (num_batches < 0 || batches_read <= num_batches));
  }

  void VerifyResults(const vector<int32_t>& results) {
    for (int i = 0; i < results.size(); ++i) {
      ASSERT_EQ(results[i], i);
    }
  }

  void VerifyResults(const vector<StringValue>& results) {
    int idx = 0;
    for (int i = 0; i < results.size(); ++i) {
      ASSERT_TRUE(results[i] == STRINGS[idx]) << results[i] << " != " << STRINGS[idx];
      idx = (idx + 1) % NUM_STRINGS;
    }
  }

  // Test adding num_batches of ints to the stream and reading them back.
  template <typename T>
  void TestValues(int num_batches, RowDescriptor* desc) {
    BufferedTupleStream stream(runtime_state_.get(), *desc, block_mgr_.get(), client_);
    Status status = stream.Init();
    ASSERT_TRUE(status.ok());
    status = stream.UnpinStream();
    ASSERT_TRUE(status.ok());

    // Add rows to the stream
    int offset = 0;
    for (int i = 0; i < num_batches; ++i) {
      RowBatch* batch = NULL;
      if (sizeof(T) == sizeof(int32_t)) {
        batch = CreateIntBatch(offset, BATCH_SIZE);
      } else if (sizeof(T) == sizeof(StringValue)) {
        batch = CreateStringBatch(offset, BATCH_SIZE);
      } else {
        DCHECK(false);
      }
      for (int j = 0; j < batch->num_rows(); ++j) {
        bool b = stream.AddRow(batch->GetRow(j));
        ASSERT_TRUE(b);
      }
      offset += batch->num_rows();
      // Reset the batch to make sure the stream handles the memory correctly.
      batch->Reset();
    }

    status = stream.PrepareForRead();
    ASSERT_TRUE(status.ok());

    // Read all the rows back
    vector<T> results;
    ReadValues(&stream, desc, &results);

    // Verify result
    EXPECT_EQ(results.size(), BATCH_SIZE * num_batches);
    VerifyResults(results);

    stream.Close();
  }

  void TestIntValuesInterleaved(int num_batches, int num_batches_before_read) {
    BufferedTupleStream stream(runtime_state_.get(), *int_desc_, block_mgr_.get(),
        client_,
        true,  // delete_on_read
        true); // read_write
    Status status = stream.Init();
    ASSERT_TRUE(status.ok());
    status = stream.UnpinStream();
    ASSERT_TRUE(status.ok());

    vector<int32_t> results;

    for (int i = 0; i < num_batches; ++i) {
      RowBatch* batch = CreateIntBatch(i * BATCH_SIZE, BATCH_SIZE);
      for (int j = 0; j < batch->num_rows(); ++j) {
        bool b = stream.AddRow(batch->GetRow(j));
        ASSERT_TRUE(b);
      }
      // Reset the batch to make sure the stream handles the memory correctly.
      batch->Reset();
      if (i % num_batches_before_read == 0) {
        ReadValues(&stream, int_desc_, &results, (rand() % num_batches_before_read) + 1);
      }
    }
    ReadValues(&stream, int_desc_, &results);

    // Verify result
    EXPECT_EQ(results.size(), BATCH_SIZE * num_batches);
    for (int i = 0; i < results.size(); ++i) {
      ASSERT_EQ(results[i], i);
    }

    stream.Close();
  }

  scoped_ptr<ExecEnv> exec_env_;
  scoped_ptr<RuntimeState> runtime_state_;
  scoped_ptr<MemTracker> block_mgr_parent_tracker_;

  shared_ptr<BufferedBlockMgr> block_mgr_;
  BufferedBlockMgr::Client* client_;

  MemTracker tracker_;
  ObjectPool pool_;
  RowDescriptor* int_desc_;
  RowDescriptor* string_desc_;
  scoped_ptr<MemPool> mem_pool_;
};

// Basic API test. No data should be going to disk.
TEST_F(BufferedTupleStreamTest, Basic) {
  CreateMgr(-1, 8 * 1024 * 1024);
  TestValues<int32_t>(1, int_desc_);
  TestValues<int32_t>(10, int_desc_);
  TestValues<int32_t>(100, int_desc_);
  TestValues<StringValue>(1, string_desc_);
  TestValues<StringValue>(10, string_desc_);
  TestValues<StringValue>(100, string_desc_);

  TestIntValuesInterleaved(1, 1);
  TestIntValuesInterleaved(10, 5);
  TestIntValuesInterleaved(100, 15);
}

// Test with only 1 buffer.
TEST_F(BufferedTupleStreamTest, OneBufferSpill) {
  // Each buffer can only hold 100 ints, so this spills quite often.
  int buffer_size = 100 * sizeof(int32_t);
  CreateMgr(buffer_size, buffer_size);
  TestValues<int32_t>(1, int_desc_);
  TestValues<int32_t>(10, int_desc_);

  TestValues<StringValue>(1, string_desc_);
  TestValues<StringValue>(10, string_desc_);
}

// Test with a few buffers.
TEST_F(BufferedTupleStreamTest, ManyBufferSpill) {
  int buffer_size = 100 * sizeof(int32_t);
  CreateMgr(10 * buffer_size, buffer_size);

  TestValues<int32_t>(1, int_desc_);
  TestValues<int32_t>(10, int_desc_);
  TestValues<int32_t>(100, int_desc_);
  TestValues<StringValue>(1, string_desc_);
  TestValues<StringValue>(10, string_desc_);
  TestValues<StringValue>(100, string_desc_);

  TestIntValuesInterleaved(1, 1);
  TestIntValuesInterleaved(10, 5);
  TestIntValuesInterleaved(100, 15);
}

TEST_F(BufferedTupleStreamTest, UnpinPin) {
  int buffer_size = 100 * sizeof(int32_t);
  CreateMgr(3 * buffer_size, buffer_size);

  BufferedTupleStream stream(runtime_state_.get(), *int_desc_, block_mgr_.get(), client_);
  Status status = stream.Init();
  ASSERT_TRUE(status.ok());

  int offset = 0;
  bool full = false;
  while (!full) {
    RowBatch* batch = CreateIntBatch(offset, BATCH_SIZE);
    int j = 0;
    for (; j < batch->num_rows(); ++j) {
      full = !stream.AddRow(batch->GetRow(j));
      if (full) break;
    }
    offset += j;
  }

  status = stream.UnpinStream();
  ASSERT_TRUE(status.ok());

  bool pinned = false;
  status = stream.PinStream(&pinned);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(pinned);

  vector<int32_t> results;

  // Read and verify result a few times. We should be able to reread the stream.
  for (int i = 0; i < 3; ++i) {
    status = stream.PrepareForRead();
    ASSERT_TRUE(status.ok());
    results.clear();
    ReadValues(&stream, int_desc_, &results);
    EXPECT_EQ(results.size(), offset);
    VerifyResults(results);
  }

  stream.Close();
}

// TODO: more tests.
//  - The stream can operate with many modes

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true);
  impala::InitFeSupport();
  impala::TmpFileMgr::Init();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
