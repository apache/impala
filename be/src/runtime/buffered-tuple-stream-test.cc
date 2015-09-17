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
#include <limits> // for std::numeric_limits<int>::max()

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "gutil/gscoped_ptr.h"
#include "runtime/array-value.h"
#include "runtime/array-value-builder.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/row-batch.h"
#include "runtime/tmp-file-mgr.h"
#include "runtime/string-value.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"
#include "util/test-info.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "common/names.h"

using base::FreeDeleter;

static const int BATCH_SIZE = 250;
static const uint32_t PRIME = 479001599;

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

class SimpleTupleStreamTest : public testing::Test {
 protected:
  virtual void SetUp() {
    exec_env_.reset(new ExecEnv);
    exec_env_->disk_io_mgr()->Init(&tracker_);
    runtime_state_.reset(
        new RuntimeState(TExecPlanFragmentParams(), "", exec_env_.get()));

    CreateDescriptors();

    mem_pool_.reset(new MemPool(&tracker_));
    metrics_.reset(new MetricGroup("buffered-tuple-stream-test"));
    tmp_file_mgr_.reset(new TmpFileMgr);
    tmp_file_mgr_->Init(metrics_.get());
  }

  virtual void CreateDescriptors() {
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
  }

  virtual void TearDown() {
    block_mgr_.reset();
    block_mgr_parent_tracker_.reset();
    runtime_state_.reset();
    exec_env_.reset();
    mem_pool_->FreeAll();
  }

  void CreateMgr(int64_t limit, int block_size) {
    Status status = BufferedBlockMgr::Create(runtime_state_.get(), &tracker_,
        runtime_state_->runtime_profile(), tmp_file_mgr_.get(), limit, block_size,
        &block_mgr_);
    EXPECT_TRUE(status.ok());
    status = block_mgr_->RegisterClient(0, &tracker_, runtime_state_.get(), &client_);
    EXPECT_TRUE(status.ok());
  }

  /// Generate the ith element of a sequence of int values.
  int GenIntValue(int i) {
    // Multiply by large prime to get varied bit patterns.
    return i * PRIME;
  }

  /// Generate the ith element of a sequence of bool values.
  bool GenBoolValue(int i) {
    // Use a middle bit of the int value.
    return ((GenIntValue(i) >> 8) & 0x1) != 0;
  }

  virtual RowBatch* CreateIntBatch(int offset, int num_rows, bool gen_null) {
    RowBatch* batch = pool_.Add(new RowBatch(*int_desc_, num_rows, &tracker_));
    int tuple_size = int_desc_->tuple_descriptors()[0]->byte_size();
    uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(
        batch->tuple_data_pool()->Allocate(tuple_size * num_rows));
    memset(tuple_mem, 0, tuple_size * num_rows);

    const int int_tuples = int_desc_->tuple_descriptors().size();
    for (int i = 0; i < num_rows; ++i) {
      int idx = batch->AddRow();
      TupleRow* row = batch->GetRow(idx);
      Tuple* int_tuple = reinterpret_cast<Tuple*>(tuple_mem + i * tuple_size);
      *reinterpret_cast<int*>(int_tuple + 1) = GenIntValue(i + offset);
      for (int j = 0; j < int_tuples; ++j) {
        int idx = (i + offset) * int_tuples + j;
        if (!gen_null || GenBoolValue(idx)) {
          row->SetTuple(j, int_tuple);
        } else {
          row->SetTuple(j, NULL);
        }
      }
      batch->CommitLastRow();
    }
    return batch;
  }

  virtual RowBatch* CreateStringBatch(int offset, int num_rows, bool gen_null) {
    int tuple_size = sizeof(StringValue) + 1;
    RowBatch* batch = pool_.Add(new RowBatch(*string_desc_, num_rows, &tracker_));
    uint8_t* tuple_mem = batch->tuple_data_pool()->Allocate(tuple_size * num_rows);
    memset(tuple_mem, 0, tuple_size * num_rows);
    const int string_tuples = string_desc_->tuple_descriptors().size();
    for (int i = 0; i < num_rows; ++i) {
      TupleRow* row = batch->GetRow(batch->AddRow());
      *reinterpret_cast<StringValue*>(tuple_mem + 1) = STRINGS[(i + offset) % NUM_STRINGS];
      for (int j = 0; j < string_tuples; ++j) {
        int idx = (i + offset) * string_tuples + j;
        if (!gen_null || GenBoolValue(idx)) {
          row->SetTuple(j, reinterpret_cast<Tuple*>(tuple_mem));
        } else {
          row->SetTuple(j, NULL);
        }
      }
      batch->CommitLastRow();
      tuple_mem += tuple_size;
    }
    return batch;
  }

  void AppendRowTuples(TupleRow* row, vector<int>* results) {
    DCHECK(row != NULL);
    const int int_tuples = int_desc_->tuple_descriptors().size();
    for (int i = 0; i < int_tuples; ++i) {
      AppendValue(row->GetTuple(i), results);
    }
  }

  void AppendRowTuples(TupleRow* row, vector<StringValue>* results) {
    DCHECK(row != NULL);
    const int string_tuples = string_desc_->tuple_descriptors().size();
    for (int i = 0; i < string_tuples; ++i) {
      AppendValue(row->GetTuple(i), results);
    }
  }

  void AppendValue(Tuple* t, vector<int>* results) {
    if (t == NULL) {
      // For the tests indicate null-ability using the max int value
      results->push_back(std::numeric_limits<int>::max());
    } else {
      results->push_back(*reinterpret_cast<int*>(reinterpret_cast<uint8_t*>(t) + 1));
    }
  }

  void AppendValue(Tuple* t, vector<StringValue>* results) {
    if (t == NULL) {
      results->push_back(StringValue());
    } else {
      uint8_t* mem = reinterpret_cast<uint8_t*>(t);
      StringValue sv = *reinterpret_cast<StringValue*>(mem + 1);
      uint8_t* copy = mem_pool_->Allocate(sv.len);
      memcpy(copy, sv.ptr, sv.len);
      sv.ptr = reinterpret_cast<char*>(copy);
      results->push_back(sv);
    }
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
        AppendRowTuples(batch.GetRow(i), results);
      }
    } while (!eos && (num_batches < 0 || batches_read <= num_batches));
  }

  virtual void VerifyResults(const vector<int>& results, int exp_rows, bool gen_null) {
    const int int_tuples = int_desc_->tuple_descriptors().size();
    EXPECT_EQ(results.size(), exp_rows * int_tuples);
    for (int i = 0; i < exp_rows; ++i) {
      for (int j = 0; j < int_tuples; ++j) {
        int idx = i * int_tuples + j;
        if (!gen_null || GenBoolValue(idx)) {
          ASSERT_EQ(results[idx], GenIntValue(i))
              << " results[" << idx << "]: " << results[idx]
              << " != " << GenIntValue(i) << " gen_null=" << gen_null;
        } else {
          ASSERT_TRUE(results[idx] == std::numeric_limits<int>::max())
              << "i: " << i << " j: " << j << " results[" << idx << "]: "
              << results[idx] << " != " << std::numeric_limits<int>::max();
        }
      }
    }
  }

  virtual void VerifyResults(const vector<StringValue>& results, int exp_rows,
      bool gen_null) {
    const int string_tuples = string_desc_->tuple_descriptors().size();
    EXPECT_EQ(results.size(), exp_rows * string_tuples);
    for (int i = 0; i < exp_rows; ++i) {
      for (int j = 0; j < string_tuples; ++j) {
        int idx = i * string_tuples + j;
        if (!gen_null || GenBoolValue(idx)) {
          ASSERT_TRUE(results[idx] == STRINGS[i % NUM_STRINGS])
              << "results[" << idx << "] " << results[idx]
              << " != " << STRINGS[i % NUM_STRINGS] << " i=" << i << " gen_null="
              << gen_null;
        } else {
          ASSERT_TRUE(results[idx] == StringValue())
              << "results[" << idx << "] " << results[idx] << " not NULL";
        }
      }
    }
  }

  // Test adding num_batches of ints to the stream and reading them back.
  template <typename T>
  void TestValues(int num_batches, RowDescriptor* desc, bool gen_null) {
    BufferedTupleStream stream(runtime_state_.get(), *desc, block_mgr_.get(), client_);
    Status status = stream.Init(-1, NULL, true);
    ASSERT_TRUE(status.ok()) << status.GetDetail();
    status = stream.UnpinStream();
    ASSERT_TRUE(status.ok());

    // Add rows to the stream
    int offset = 0;
    for (int i = 0; i < num_batches; ++i) {
      RowBatch* batch = NULL;
      if (sizeof(T) == sizeof(int)) {
        batch = CreateIntBatch(offset, BATCH_SIZE, gen_null);
      } else if (sizeof(T) == sizeof(StringValue)) {
        batch = CreateStringBatch(offset, BATCH_SIZE, gen_null);
      } else {
        DCHECK(false);
      }
      for (int j = 0; j < batch->num_rows(); ++j) {
        bool b = stream.AddRow(batch->GetRow(j), &status);
        ASSERT_TRUE(status.ok());
        if (!b) {
          ASSERT_TRUE(stream.using_small_buffers());
          bool got_buffer;
          status = stream.SwitchToIoBuffers(&got_buffer);
          ASSERT_TRUE(status.ok());
          ASSERT_TRUE(got_buffer);
          b = stream.AddRow(batch->GetRow(j), &status);
          ASSERT_TRUE(status.ok());
        }
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
    VerifyResults(results, BATCH_SIZE * num_batches, gen_null);

    stream.Close();
  }

  void TestIntValuesInterleaved(int num_batches, int num_batches_before_read) {
    for (int small_buffers = 0; small_buffers < 2; ++small_buffers) {
      BufferedTupleStream stream(runtime_state_.get(), *int_desc_, block_mgr_.get(),
          client_,
          small_buffers == 0,  // initial small buffers
          true,  // delete_on_read
          true); // read_write
      Status status = stream.Init(-1, NULL, true);
      ASSERT_TRUE(status.ok());
      status = stream.UnpinStream();
      ASSERT_TRUE(status.ok());

      vector<int> results;

      for (int i = 0; i < num_batches; ++i) {
        RowBatch* batch = CreateIntBatch(i * BATCH_SIZE, BATCH_SIZE, false);
        for (int j = 0; j < batch->num_rows(); ++j) {
          bool b = stream.AddRow(batch->GetRow(j), &status);
          ASSERT_TRUE(b);
          ASSERT_TRUE(status.ok());
        }
        // Reset the batch to make sure the stream handles the memory correctly.
        batch->Reset();
        if (i % num_batches_before_read == 0) {
          ReadValues(&stream, int_desc_, &results,
              (rand() % num_batches_before_read) + 1);
        }
      }
      ReadValues(&stream, int_desc_, &results);

      VerifyResults(results, BATCH_SIZE * num_batches, false);

      stream.Close();
    }
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
  scoped_ptr<MetricGroup> metrics_;
  scoped_ptr<TmpFileMgr> tmp_file_mgr_;
}; // SimpleTupleStreamTest


// Tests with a non-NULLable tuple per row.
class SimpleNullStreamTest : public SimpleTupleStreamTest {
 protected:
  virtual void CreateDescriptors() {
    vector<bool> nullable_tuples(1, true);
    vector<TTupleId> tuple_ids(1, static_cast<TTupleId>(0));

    DescriptorTblBuilder int_builder(&pool_);
    int_builder.DeclareTuple() << TYPE_INT;
    int_desc_ = pool_.Add(new RowDescriptor(
        *int_builder.Build(), tuple_ids, nullable_tuples));

    DescriptorTblBuilder string_builder(&pool_);
    string_builder.DeclareTuple() << TYPE_STRING;
    string_desc_ = pool_.Add(new RowDescriptor(
        *string_builder.Build(), tuple_ids, nullable_tuples));
  }
}; // SimpleNullStreamTest

// Tests with multiple non-NULLable tuples per row.
class MultiTupleStreamTest : public SimpleTupleStreamTest {
 protected:
  virtual void CreateDescriptors() {
    vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    nullable_tuples.push_back(false);
    nullable_tuples.push_back(false);

    vector<TTupleId> tuple_ids;
    tuple_ids.push_back(static_cast<TTupleId>(0));
    tuple_ids.push_back(static_cast<TTupleId>(1));
    tuple_ids.push_back(static_cast<TTupleId>(2));

    DescriptorTblBuilder int_builder(&pool_);
    int_builder.DeclareTuple() << TYPE_INT;
    int_builder.DeclareTuple() << TYPE_INT;
    int_builder.DeclareTuple() << TYPE_INT;
    int_desc_ = pool_.Add(new RowDescriptor(
        *int_builder.Build(), tuple_ids, nullable_tuples));

    DescriptorTblBuilder string_builder(&pool_);
    string_builder.DeclareTuple() << TYPE_STRING;
    string_builder.DeclareTuple() << TYPE_STRING;
    string_builder.DeclareTuple() << TYPE_STRING;
    string_desc_ = pool_.Add(new RowDescriptor(
        *string_builder.Build(), tuple_ids, nullable_tuples));
  }
};

// Tests with multiple NULLable tuples per row.
class MultiNullableTupleStreamTest : public SimpleTupleStreamTest {
 protected:
  virtual void CreateDescriptors() {
    vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    nullable_tuples.push_back(true);
    nullable_tuples.push_back(true);

    vector<TTupleId> tuple_ids;
    tuple_ids.push_back(static_cast<TTupleId>(0));
    tuple_ids.push_back(static_cast<TTupleId>(1));
    tuple_ids.push_back(static_cast<TTupleId>(2));

    DescriptorTblBuilder int_builder(&pool_);
    int_builder.DeclareTuple() << TYPE_INT;
    int_builder.DeclareTuple() << TYPE_INT;
    int_builder.DeclareTuple() << TYPE_INT;
    int_desc_ = pool_.Add(new RowDescriptor(
        *int_builder.Build(), tuple_ids, nullable_tuples));

    DescriptorTblBuilder string_builder(&pool_);
    string_builder.DeclareTuple() << TYPE_STRING;
    string_builder.DeclareTuple() << TYPE_STRING;
    string_builder.DeclareTuple() << TYPE_STRING;
    string_desc_ = pool_.Add(new RowDescriptor(
        *string_builder.Build(), tuple_ids, nullable_tuples));
  }
};

/// Tests with collection types.
class ArrayTupleStreamTest : public SimpleTupleStreamTest {
 protected:
  RowDescriptor* array_desc_;

  virtual void CreateDescriptors() {
    // tuples: (array<string>, array<array<int>>) (array<int>)
    vector<bool> nullable_tuples(2, true);
    vector<TTupleId> tuple_ids;
    tuple_ids.push_back(static_cast<TTupleId>(0));
    tuple_ids.push_back(static_cast<TTupleId>(1));
    ColumnType string_array_type;
    string_array_type.type = TYPE_ARRAY;
    string_array_type.children.push_back(TYPE_STRING);

    ColumnType int_array_type;
    int_array_type.type = TYPE_ARRAY;
    int_array_type.children.push_back(TYPE_STRING);

    ColumnType nested_array_type;
    nested_array_type.type = TYPE_ARRAY;
    nested_array_type.children.push_back(int_array_type);

    DescriptorTblBuilder builder(&pool_);
    builder.DeclareTuple() << string_array_type << nested_array_type;
    builder.DeclareTuple() << int_array_type;
    array_desc_ = pool_.Add(new RowDescriptor(
        *builder.Build(), tuple_ids, nullable_tuples));
  }
};

// Basic API test. No data should be going to disk.
TEST_F(SimpleTupleStreamTest, Basic) {
  CreateMgr(-1, 8 * 1024 * 1024);
  TestValues<int>(1, int_desc_, false);
  TestValues<int>(10, int_desc_, false);
  TestValues<int>(100, int_desc_, false);

  TestValues<StringValue>(1, string_desc_, false);
  TestValues<StringValue>(10, string_desc_, false);
  TestValues<StringValue>(100, string_desc_, false);

  TestIntValuesInterleaved(1, 1);
  TestIntValuesInterleaved(10, 5);
  TestIntValuesInterleaved(100, 15);
}

// Test with only 1 buffer.
TEST_F(SimpleTupleStreamTest, OneBufferSpill) {
  // Each buffer can only hold 100 ints, so this spills quite often.
  int buffer_size = 100 * sizeof(int);
  CreateMgr(buffer_size, buffer_size);
  TestValues<int>(1, int_desc_, false);
  TestValues<int>(10, int_desc_, false);

  TestValues<StringValue>(1, string_desc_, false);
  TestValues<StringValue>(10, string_desc_, false);
}

// Test with a few buffers.
TEST_F(SimpleTupleStreamTest, ManyBufferSpill) {
  int buffer_size = 100 * sizeof(int);
  CreateMgr(10 * buffer_size, buffer_size);

  TestValues<int>(1, int_desc_, false);
  TestValues<int>(10, int_desc_, false);
  TestValues<int>(100, int_desc_, false);
  TestValues<StringValue>(1, string_desc_, false);
  TestValues<StringValue>(10, string_desc_, false);
  TestValues<StringValue>(100, string_desc_, false);

  TestIntValuesInterleaved(1, 1);
  TestIntValuesInterleaved(10, 5);
  TestIntValuesInterleaved(100, 15);
}

TEST_F(SimpleTupleStreamTest, UnpinPin) {
  int buffer_size = 100 * sizeof(int);
  CreateMgr(3 * buffer_size, buffer_size);

  BufferedTupleStream stream(runtime_state_.get(), *int_desc_, block_mgr_.get(), client_);
  Status status = stream.Init(-1, NULL, true);
  ASSERT_TRUE(status.ok());

  int offset = 0;
  bool full = false;
  while (!full) {
    RowBatch* batch = CreateIntBatch(offset, BATCH_SIZE, false);
    int j = 0;
    for (; j < batch->num_rows(); ++j) {
      full = !stream.AddRow(batch->GetRow(j), &status);
      ASSERT_TRUE(status.ok());
      if (full) break;
    }
    offset += j;
  }

  status = stream.UnpinStream();
  ASSERT_TRUE(status.ok());

  bool pinned = false;
  status = stream.PinStream(false, &pinned);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(pinned);

  vector<int> results;

  // Read and verify result a few times. We should be able to reread the stream.
  for (int i = 0; i < 3; ++i) {
    status = stream.PrepareForRead();
    ASSERT_TRUE(status.ok());
    results.clear();
    ReadValues(&stream, int_desc_, &results);
    VerifyResults(results, offset, false);
  }

  stream.Close();
}

TEST_F(SimpleTupleStreamTest, SmallBuffers) {
  int buffer_size = 8 * 1024 * 1024;
  CreateMgr(2 * buffer_size, buffer_size);

  BufferedTupleStream stream(runtime_state_.get(), *int_desc_, block_mgr_.get(), client_);
  Status status = stream.Init(-1, NULL, false);
  ASSERT_TRUE(status.ok());

  // Initial buffer should be small.
  EXPECT_LT(stream.bytes_in_mem(false), buffer_size);

  RowBatch* batch = CreateIntBatch(0, 1024, false);
  for (int i = 0; i < batch->num_rows(); ++i) {
    bool ret = stream.AddRow(batch->GetRow(i), &status);
    EXPECT_TRUE(ret);
    ASSERT_TRUE(status.ok());
  }
  EXPECT_LT(stream.bytes_in_mem(false), buffer_size);
  EXPECT_LT(stream.byte_size(), buffer_size);

  // 40 MB of ints
  batch = CreateIntBatch(0, 10 * 1024 * 1024, false);
  for (int i = 0; i < batch->num_rows(); ++i) {
    bool ret = stream.AddRow(batch->GetRow(i), &status);
    ASSERT_TRUE(status.ok());
    if (!ret) {
      ASSERT_TRUE(stream.using_small_buffers());
      bool got_buffer;
      status = stream.SwitchToIoBuffers(&got_buffer);
      ASSERT_TRUE(status.ok());
      ASSERT_TRUE(got_buffer);
      ret = stream.AddRow(batch->GetRow(i), &status);
      ASSERT_TRUE(status.ok());
    }
    ASSERT_TRUE(ret);
  }
  EXPECT_EQ(stream.bytes_in_mem(false), buffer_size);

  stream.Close();
}

// Basic API test. No data should be going to disk.
TEST_F(SimpleNullStreamTest, Basic) {
  CreateMgr(-1, 8 * 1024 * 1024);
  TestValues<int>(1, int_desc_, false);
  TestValues<int>(10, int_desc_, false);
  TestValues<int>(100, int_desc_, false);
  TestValues<int>(1, int_desc_, true);
  TestValues<int>(10, int_desc_, true);
  TestValues<int>(100, int_desc_, true);

  TestValues<StringValue>(1, string_desc_, false);
  TestValues<StringValue>(10, string_desc_, false);
  TestValues<StringValue>(100, string_desc_, false);
  TestValues<StringValue>(1, string_desc_, true);
  TestValues<StringValue>(10, string_desc_, true);
  TestValues<StringValue>(100, string_desc_, true);

  TestIntValuesInterleaved(1, 1);
  TestIntValuesInterleaved(10, 5);
  TestIntValuesInterleaved(100, 15);
}

// Test tuple stream with only 1 buffer and rows with multiple tuples.
TEST_F(MultiTupleStreamTest, MultiTupleOneBufferSpill) {
  // Each buffer can only hold 100 ints, so this spills quite often.
  int buffer_size = 100 * sizeof(int);
  CreateMgr(buffer_size, buffer_size);
  TestValues<int>(1, int_desc_, false);
  TestValues<int>(10, int_desc_, false);

  TestValues<StringValue>(1, string_desc_, false);
  TestValues<StringValue>(10, string_desc_, false);
}

// Test with a few buffers and rows with multiple tuples.
TEST_F(MultiTupleStreamTest, MultiTupleManyBufferSpill) {
  int buffer_size = 100 * sizeof(int);
  CreateMgr(10 * buffer_size, buffer_size);

  TestValues<int>(1, int_desc_, false);
  TestValues<int>(10, int_desc_, false);
  TestValues<int>(100, int_desc_, false);

  TestValues<StringValue>(1, string_desc_, false);
  TestValues<StringValue>(10, string_desc_, false);
  TestValues<StringValue>(100, string_desc_, false);

  TestIntValuesInterleaved(1, 1);
  TestIntValuesInterleaved(10, 5);
  TestIntValuesInterleaved(100, 15);
}

// Test with rows with multiple nullable tuples.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleOneBufferSpill) {
  // Each buffer can only hold 100 ints, so this spills quite often.
  int buffer_size = 100 * sizeof(int);
  CreateMgr(buffer_size, buffer_size);
  TestValues<int>(1, int_desc_, false);
  TestValues<int>(10, int_desc_, false);
  TestValues<int>(1, int_desc_, true);
  TestValues<int>(10, int_desc_, true);

  TestValues<StringValue>(1, string_desc_, false);
  TestValues<StringValue>(10, string_desc_, false);
  TestValues<StringValue>(1, string_desc_, true);
  TestValues<StringValue>(10, string_desc_, true);
}

// Test with a few buffers.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleManyBufferSpill) {
  int buffer_size = 100 * sizeof(int);
  CreateMgr(10 * buffer_size, buffer_size);

  TestValues<int>(1, int_desc_, false);
  TestValues<int>(10, int_desc_, false);
  TestValues<int>(100, int_desc_, false);
  TestValues<int>(1, int_desc_, true);
  TestValues<int>(10, int_desc_, true);
  TestValues<int>(100, int_desc_, true);

  TestValues<StringValue>(1, string_desc_, false);
  TestValues<StringValue>(10, string_desc_, false);
  TestValues<StringValue>(100, string_desc_, false);
  TestValues<StringValue>(1, string_desc_, true);
  TestValues<StringValue>(10, string_desc_, true);
  TestValues<StringValue>(100, string_desc_, true);

  TestIntValuesInterleaved(1, 1);
  TestIntValuesInterleaved(10, 5);
  TestIntValuesInterleaved(100, 15);
}

/// Test that deep copy works with arrays by copying into a BufferedTupleStream, freeing
/// the original rows, then reading back the rows and verifying the contents.
TEST_F(ArrayTupleStreamTest, TestArrayDeepCopy) {
  CreateMgr(-1, 8 * 1024 * 1024);
  const int NUM_ROWS = 4000;
  BufferedTupleStream stream(runtime_state_.get(), *array_desc_, block_mgr_.get(),
      client_, false);
  Status status;
  const vector<TupleDescriptor*>& tuple_descs = array_desc_->tuple_descriptors();
  // Write out a predictable pattern of data by iterating over arrays of constants.
  int strings_index = 0; // we take the mod of this as index into STRINGS.
  int array_lens[] = { 0, 1, 5, 10, 1000, 2, 49, 20 };
  int num_array_lens = sizeof(array_lens) / sizeof(array_lens[0]);
  int array_len_index = 0;
  for (int i = 0; i < NUM_ROWS; ++i) {
    int expected_row_size = tuple_descs[0]->byte_size() + tuple_descs[1]->byte_size();
    gscoped_ptr<TupleRow, FreeDeleter> row(reinterpret_cast<TupleRow*>(
          malloc(tuple_descs.size() * sizeof(Tuple*))));
    gscoped_ptr<Tuple, FreeDeleter> tuple0(reinterpret_cast<Tuple*>(
          malloc(tuple_descs[0]->byte_size())));
    gscoped_ptr<Tuple, FreeDeleter> tuple1(reinterpret_cast<Tuple*>(
          malloc(tuple_descs[1]->byte_size())));
    memset(tuple0.get(), 0, tuple_descs[0]->byte_size());
    memset(tuple1.get(), 0, tuple_descs[1]->byte_size());
    row->SetTuple(0, tuple0.get());
    row->SetTuple(1, tuple1.get());

    // Only array<string> is non-null.
    tuple0->SetNull(tuple_descs[0]->slots()[1]->null_indicator_offset());
    tuple1->SetNull(tuple_descs[1]->slots()[0]->null_indicator_offset());
    const SlotDescriptor* array_slot_desc = tuple_descs[0]->slots()[0];
    const TupleDescriptor* item_desc = array_slot_desc->collection_item_descriptor();

    int array_len = array_lens[array_len_index++ % num_array_lens];
    ArrayValue* av = tuple0->GetCollectionSlot(array_slot_desc->tuple_offset());
    av->ptr = NULL;
    av->num_tuples = 0;
    ArrayValueBuilder builder(av, *item_desc, mem_pool_.get(), array_len);
    Tuple* array_data;
    builder.GetFreeMemory(&array_data);
    expected_row_size += item_desc->byte_size() * array_len;

    // Fill the array with pointers to our constant strings.
    for (int j = 0; j < array_len; ++j) {
      const StringValue* string = &STRINGS[strings_index++ % NUM_STRINGS];
      array_data->SetNotNull(item_desc->slots()[0]->null_indicator_offset());
      RawValue::Write(string, array_data, item_desc->slots()[0], mem_pool_.get());
      array_data += item_desc->byte_size();
      expected_row_size += string->len;
    }
    builder.CommitTuples(array_len);

    // Check that internal row size computation gives correct result.
    EXPECT_EQ(expected_row_size, stream.ComputeRowSize(row.get()));
    bool b = stream.AddRow(row.get(), &status);
    ASSERT_TRUE(b);
    ASSERT_TRUE(status.ok());
    mem_pool_->FreeAll(); // Free data as soon as possible to smoke out issues.
  }

  // Read back and verify data.
  stream.PrepareForRead();
  strings_index = 0;
  array_len_index = 0;
  bool eos = false;
  int rows_read = 0;
  RowBatch batch(*array_desc_, BATCH_SIZE, &tracker_);
  do {
    batch.Reset();
    ASSERT_TRUE(stream.GetNext(&batch, &eos).ok());
    for (int i = 0; i < batch.num_rows(); ++i) {
      TupleRow* row = batch.GetRow(i);
      Tuple* tuple0 = row->GetTuple(0);
      Tuple* tuple1 = row->GetTuple(1);
      ASSERT_TRUE(tuple0 != NULL);
      ASSERT_TRUE(tuple1 != NULL);
      const SlotDescriptor* array_slot_desc = tuple_descs[0]->slots()[0];
      ASSERT_FALSE(tuple0->IsNull(array_slot_desc->null_indicator_offset()));
      ASSERT_TRUE(tuple0->IsNull(tuple_descs[0]->slots()[1]->null_indicator_offset()));
      ASSERT_TRUE(tuple1->IsNull(tuple_descs[1]->slots()[0]->null_indicator_offset()));

      const TupleDescriptor* item_desc = array_slot_desc->collection_item_descriptor();
      int expected_array_len = array_lens[array_len_index++ % num_array_lens];
      ArrayValue* av = tuple0->GetCollectionSlot(array_slot_desc->tuple_offset());
      ASSERT_EQ(expected_array_len, av->num_tuples);
      for (int j = 0; j < av->num_tuples; ++j) {
        Tuple* item = reinterpret_cast<Tuple*>(av->ptr + j * item_desc->byte_size());
        const SlotDescriptor* string_desc = item_desc->slots()[0];
        ASSERT_FALSE(item->IsNull(string_desc->null_indicator_offset()));
        const StringValue* expected = &STRINGS[strings_index++ % NUM_STRINGS];
        const StringValue* actual = item->GetStringSlot(string_desc->tuple_offset());
        ASSERT_EQ(*expected, *actual);
      }
    }
    rows_read += batch.num_rows();
  } while (!eos);
  ASSERT_EQ(NUM_ROWS, rows_read);
}

// TODO: more tests.
//  - The stream can operate in many modes

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
