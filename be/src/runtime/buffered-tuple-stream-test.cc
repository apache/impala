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

#include <set>
#include <string>
#include <limits> // for std::numeric_limits<int>::max()

#include "testutil/gtest-util.h"
#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "gutil/gscoped_ptr.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/collection-value.h"
#include "runtime/collection-value-builder.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/test-env.h"
#include "runtime/tmp-file-mgr.h"
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
    test_env_.reset(new TestEnv());

    CreateDescriptors();

    mem_pool_.reset(new MemPool(&tracker_));
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
    runtime_state_ = NULL;
    client_ = NULL;
    pool_.Clear();
    mem_pool_->FreeAll();
    test_env_.reset();
  }

  /// Setup a block manager with the provided settings and client with no reservation,
  /// tracked by tracker_.
  void InitBlockMgr(int64_t limit, int block_size) {
    ASSERT_OK(test_env_->CreateQueryState(0, limit, block_size, &runtime_state_));
    ASSERT_OK(runtime_state_->block_mgr()->RegisterClient(0, false, &tracker_,
        runtime_state_, &client_));
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
    const int int_tuples = int_desc_->tuple_descriptors().size();
    int tuple_size = int_desc_->tuple_descriptors()[0]->byte_size();
    int total_tuple_mem = tuple_size * num_rows * int_tuples;
    uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(
        batch->tuple_data_pool()->Allocate(total_tuple_mem));
    memset(tuple_mem, 0, total_tuple_mem);

    for (int i = 0; i < num_rows; ++i) {
      int row_idx = batch->AddRow();
      TupleRow* row = batch->GetRow(row_idx);
      for (int j = 0; j < int_tuples; ++j) {
        int idx = (i + offset) * int_tuples + j;
        Tuple* int_tuple = reinterpret_cast<Tuple*>(tuple_mem +
            (i * int_tuples + j) * tuple_size);
        *reinterpret_cast<int*>(int_tuple + 1) = GenIntValue(idx);
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
    const int string_tuples = string_desc_->tuple_descriptors().size();
    int total_tuple_mem = tuple_size * num_rows * string_tuples;
    uint8_t* tuple_mem = batch->tuple_data_pool()->Allocate(total_tuple_mem);
    memset(tuple_mem, 0, total_tuple_mem);
    for (int i = 0; i < num_rows; ++i) {
      TupleRow* row = batch->GetRow(batch->AddRow());
      for (int j = 0; j < string_tuples; ++j) {
        int idx = (i + offset) * string_tuples + j;
        *reinterpret_cast<StringValue*>(tuple_mem + 1) = STRINGS[idx % NUM_STRINGS];
        if (!gen_null || GenBoolValue(idx)) {
          row->SetTuple(j, reinterpret_cast<Tuple*>(tuple_mem));
        } else {
          row->SetTuple(j, NULL);
        }
        tuple_mem += tuple_size;
      }
      batch->CommitLastRow();
    }
    return batch;
  }

  void AppendRowTuples(TupleRow* row, vector<int>* results) {
    ASSERT_TRUE(row != NULL);
    const int int_tuples = int_desc_->tuple_descriptors().size();
    for (int i = 0; i < int_tuples; ++i) {
      AppendValue(row->GetTuple(i), results);
    }
  }

  void AppendRowTuples(TupleRow* row, vector<StringValue>* results) {
    ASSERT_TRUE(row != NULL);
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
      EXPECT_OK(stream->GetNext(&batch, &eos));
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
          ASSERT_EQ(results[idx], GenIntValue(idx))
              << " results[" << idx << "]: " << results[idx]
              << " != " << GenIntValue(idx) << " gen_null=" << gen_null;
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
          ASSERT_TRUE(results[idx] == STRINGS[idx % NUM_STRINGS])
              << "results[" << idx << "] " << results[idx]
              << " != " << STRINGS[idx % NUM_STRINGS] << " i=" << i << " j=" << j
              << " gen_null=" << gen_null;
        } else {
          ASSERT_TRUE(results[idx] == StringValue())
              << "results[" << idx << "] " << results[idx] << " not NULL";
        }
      }
    }
  }

  // Test adding num_batches of ints to the stream and reading them back.
  // If unpin_stream is true, operate the stream in unpinned mode.
  // Assumes that enough buffers are available to read and write the stream.
  template <typename T>
  void TestValues(int num_batches, RowDescriptor* desc, bool gen_null,
      bool unpin_stream) {
    BufferedTupleStream stream(runtime_state_, *desc, runtime_state_->block_mgr(),
        client_, true, false);
    ASSERT_OK(stream.Init(-1, NULL, true));

    if (unpin_stream) ASSERT_OK(stream.UnpinStream());
    // Add rows to the stream
    int offset = 0;
    for (int i = 0; i < num_batches; ++i) {
      RowBatch* batch = NULL;
      if (sizeof(T) == sizeof(int)) {
        batch = CreateIntBatch(offset, BATCH_SIZE, gen_null);
      } else if (sizeof(T) == sizeof(StringValue)) {
        batch = CreateStringBatch(offset, BATCH_SIZE, gen_null);
      } else {
        ASSERT_TRUE(false);
      }
      Status status;
      for (int j = 0; j < batch->num_rows(); ++j) {
        bool b = stream.AddRow(batch->GetRow(j), &status);
        ASSERT_OK(status)
        if (!b) {
          ASSERT_TRUE(stream.using_small_buffers());
          bool got_buffer;
          ASSERT_OK(stream.SwitchToIoBuffers(&got_buffer));
          ASSERT_TRUE(got_buffer);
          b = stream.AddRow(batch->GetRow(j), &status);
          ASSERT_OK(status);
        }
        ASSERT_TRUE(b);
      }
      offset += batch->num_rows();
      // Reset the batch to make sure the stream handles the memory correctly.
      batch->Reset();
    }

    ASSERT_OK(stream.PrepareForRead(false));

    // Read all the rows back
    vector<T> results;
    ReadValues(&stream, desc, &results);

    // Verify result
    VerifyResults(results, BATCH_SIZE * num_batches, gen_null);

    stream.Close();
  }

  void TestIntValuesInterleaved(int num_batches, int num_batches_before_read,
      bool unpin_stream) {
    for (int small_buffers = 0; small_buffers < 2; ++small_buffers) {
      BufferedTupleStream stream(runtime_state_, *int_desc_, runtime_state_->block_mgr(),
          client_, small_buffers == 0,  // initial small buffers
          true); // read_write
      ASSERT_OK(stream.Init(-1, NULL, true));
      ASSERT_OK(stream.PrepareForRead(true));
      if (unpin_stream) ASSERT_OK(stream.UnpinStream());

      vector<int> results;

      for (int i = 0; i < num_batches; ++i) {
        RowBatch* batch = CreateIntBatch(i * BATCH_SIZE, BATCH_SIZE, false);
        for (int j = 0; j < batch->num_rows(); ++j) {
          Status status;
          bool b = stream.AddRow(batch->GetRow(j), &status);
          ASSERT_TRUE(b);
          ASSERT_OK(status);
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

  void TestUnpinPin(bool varlen_data);

  scoped_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;
  BufferedBlockMgr::Client* client_;

  MemTracker tracker_;
  ObjectPool pool_;
  RowDescriptor* int_desc_;
  RowDescriptor* string_desc_;
  scoped_ptr<MemPool> mem_pool_;
};


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
  InitBlockMgr(-1, 8 * 1024 * 1024);
  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);
  TestValues<int>(100, int_desc_, false, true);
  TestValues<int>(1, int_desc_, false, false);
  TestValues<int>(10, int_desc_, false, false);
  TestValues<int>(100, int_desc_, false, false);

  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
  TestValues<StringValue>(100, string_desc_, false, true);
  TestValues<StringValue>(1, string_desc_, false, false);
  TestValues<StringValue>(10, string_desc_, false, false);
  TestValues<StringValue>(100, string_desc_, false, false);

  TestIntValuesInterleaved(1, 1, true);
  TestIntValuesInterleaved(10, 5, true);
  TestIntValuesInterleaved(100, 15, true);
  TestIntValuesInterleaved(1, 1, false);
  TestIntValuesInterleaved(10, 5, false);
  TestIntValuesInterleaved(100, 15, false);
}

// Test with only 1 buffer.
TEST_F(SimpleTupleStreamTest, OneBufferSpill) {
  // Each buffer can only hold 100 ints, so this spills quite often.
  int buffer_size = 100 * sizeof(int);
  InitBlockMgr(buffer_size, buffer_size);
  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);

  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
}

// Test with a few buffers.
TEST_F(SimpleTupleStreamTest, ManyBufferSpill) {
  int buffer_size = 100 * sizeof(int);
  InitBlockMgr(10 * buffer_size, buffer_size);

  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);
  TestValues<int>(100, int_desc_, false, true);
  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
  TestValues<StringValue>(100, string_desc_, false, true);

  TestIntValuesInterleaved(1, 1, true);
  TestIntValuesInterleaved(10, 5, true);
  TestIntValuesInterleaved(100, 15, true);
}

void SimpleTupleStreamTest::TestUnpinPin(bool varlen_data) {
  int buffer_size = 100 * sizeof(int);
  InitBlockMgr(3 * buffer_size, buffer_size);
  RowDescriptor* row_desc = varlen_data ? string_desc_ : int_desc_;

  BufferedTupleStream stream(runtime_state_, *row_desc, runtime_state_->block_mgr(),
      client_, true, false);
  ASSERT_OK(stream.Init(-1, NULL, true));

  int offset = 0;
  bool full = false;
  while (!full) {
    RowBatch* batch = varlen_data ? CreateStringBatch(offset, BATCH_SIZE, false)
                                  : CreateIntBatch(offset, BATCH_SIZE, false);
    int j = 0;
    for (; j < batch->num_rows(); ++j) {
      Status status;
      full = !stream.AddRow(batch->GetRow(j), &status);
      ASSERT_OK(status);
      if (full) break;
    }
    offset += j;
  }

  ASSERT_OK(stream.UnpinStream());

  bool pinned = false;
  ASSERT_OK(stream.PinStream(false, &pinned));
  ASSERT_TRUE(pinned);


  // Read and verify result a few times. We should be able to reread the stream if
  // we don't use delete on read mode.
  int read_iters = 3;
  for (int i = 0; i < read_iters; ++i) {
    bool delete_on_read = i == read_iters - 1;
    ASSERT_OK(stream.PrepareForRead(delete_on_read));

    if (varlen_data) {
      vector<StringValue> results;
      ReadValues(&stream, row_desc, &results);
      VerifyResults(results, offset, false);
    } else {
      vector<int> results;
      ReadValues(&stream, row_desc, &results);
      VerifyResults(results, offset, false);
    }
  }

  // After delete_on_read, all blocks aside from the last should be deleted.
  // Note: this should really be 0, but the BufferedTupleStream returns eos before
  // deleting the last block, rather than after, so the last block isn't deleted
  // until the stream is closed.
  ASSERT_EQ(stream.bytes_in_mem(false), buffer_size);

  stream.Close();

  ASSERT_EQ(stream.bytes_in_mem(false), 0);
}

TEST_F(SimpleTupleStreamTest, UnpinPin) {
  TestUnpinPin(false);
}

TEST_F(SimpleTupleStreamTest, UnpinPinVarlen) {
  TestUnpinPin(false);
}

TEST_F(SimpleTupleStreamTest, SmallBuffers) {
  int buffer_size = 8 * 1024 * 1024;
  InitBlockMgr(2 * buffer_size, buffer_size);

  BufferedTupleStream stream(runtime_state_, *int_desc_, runtime_state_->block_mgr(),
      client_, true, false);
  ASSERT_OK(stream.Init(-1, NULL, false));

  // Initial buffer should be small.
  EXPECT_LT(stream.bytes_in_mem(false), buffer_size);

  RowBatch* batch = CreateIntBatch(0, 1024, false);

  Status status;
  for (int i = 0; i < batch->num_rows(); ++i) {
    bool ret = stream.AddRow(batch->GetRow(i), &status);
    EXPECT_TRUE(ret);
    ASSERT_OK(status);
  }
  EXPECT_LT(stream.bytes_in_mem(false), buffer_size);
  EXPECT_LT(stream.byte_size(), buffer_size);
  ASSERT_TRUE(stream.using_small_buffers());

  // 40 MB of ints
  batch = CreateIntBatch(0, 10 * 1024 * 1024, false);
  for (int i = 0; i < batch->num_rows(); ++i) {
    bool ret = stream.AddRow(batch->GetRow(i), &status);
    ASSERT_OK(status);
    if (!ret) {
      ASSERT_TRUE(stream.using_small_buffers());
      bool got_buffer;
      ASSERT_OK(stream.SwitchToIoBuffers(&got_buffer));
      ASSERT_TRUE(got_buffer);
      ret = stream.AddRow(batch->GetRow(i), &status);
      ASSERT_OK(status);
    }
    ASSERT_TRUE(ret);
  }
  EXPECT_EQ(stream.bytes_in_mem(false), buffer_size);

  // TODO: Test for IMPALA-2330. In case SwitchToIoBuffers() fails to get buffer then
  // using_small_buffers() should still return true.
  stream.Close();
}

// Test that tuple stream functions if it references strings outside stream. The
// aggregation node relies on this since it updates tuples in-place.
TEST_F(SimpleTupleStreamTest, StringsOutsideStream) {
  int buffer_size = 8 * 1024 * 1024;
  InitBlockMgr(2 * buffer_size, buffer_size);
  Status status = Status::OK();

  int num_batches = 100;
  int rows_added = 0;
  DCHECK_EQ(string_desc_->tuple_descriptors().size(), 1);
  TupleDescriptor& tuple_desc = *string_desc_->tuple_descriptors()[0];

  set<SlotId> external_slots;
  for (int i = 0; i < tuple_desc.string_slots().size(); ++i) {
    external_slots.insert(tuple_desc.string_slots()[i]->id());
  }

  BufferedTupleStream stream(runtime_state_, *string_desc_, runtime_state_->block_mgr(),
      client_, true, false, external_slots);
  for (int i = 0; i < num_batches; ++i) {
    RowBatch* batch = CreateStringBatch(rows_added, BATCH_SIZE, false);
    for (int j = 0; j < batch->num_rows(); ++j) {
      uint8_t* varlen_data;
      int fixed_size = tuple_desc.byte_size();
      uint8_t* tuple = stream.AllocateRow(fixed_size, 0, &varlen_data, &status);
      ASSERT_TRUE(tuple != NULL);
      ASSERT_TRUE(status.ok());
      // Copy fixed portion in, but leave it pointing to row batch's varlen data.
      memcpy(tuple, batch->GetRow(j)->GetTuple(0), fixed_size);
    }
    rows_added += batch->num_rows();
  }

  DCHECK_EQ(rows_added, stream.num_rows());

  for (int delete_on_read = 0; delete_on_read <= 1; ++delete_on_read) {
    // Keep stream in memory and test we can read ok.
    vector<StringValue> results;
    stream.PrepareForRead(delete_on_read);
    ReadValues(&stream, string_desc_, &results);
    VerifyResults(results, rows_added, false);
  }

  stream.Close();
}

// Basic API test. No data should be going to disk.
TEST_F(SimpleNullStreamTest, Basic) {
  InitBlockMgr(-1, 8 * 1024 * 1024);
  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);
  TestValues<int>(100, int_desc_, false, true);
  TestValues<int>(1, int_desc_, true, true);
  TestValues<int>(10, int_desc_, true, true);
  TestValues<int>(100, int_desc_, true, true);
  TestValues<int>(1, int_desc_, false, false);
  TestValues<int>(10, int_desc_, false, false);
  TestValues<int>(100, int_desc_, false, false);
  TestValues<int>(1, int_desc_, true, false);
  TestValues<int>(10, int_desc_, true, false);
  TestValues<int>(100, int_desc_, true, false);

  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
  TestValues<StringValue>(100, string_desc_, false, true);
  TestValues<StringValue>(1, string_desc_, true, true);
  TestValues<StringValue>(10, string_desc_, true, true);
  TestValues<StringValue>(100, string_desc_, true, true);
  TestValues<StringValue>(1, string_desc_, false, false);
  TestValues<StringValue>(10, string_desc_, false, false);
  TestValues<StringValue>(100, string_desc_, false, false);
  TestValues<StringValue>(1, string_desc_, true, false);
  TestValues<StringValue>(10, string_desc_, true, false);
  TestValues<StringValue>(100, string_desc_, true, false);

  TestIntValuesInterleaved(1, 1, true);
  TestIntValuesInterleaved(10, 5, true);
  TestIntValuesInterleaved(100, 15, true);
  TestIntValuesInterleaved(1, 1, false);
  TestIntValuesInterleaved(10, 5, false);
  TestIntValuesInterleaved(100, 15, false);
}

// Test tuple stream with only 1 buffer and rows with multiple tuples.
TEST_F(MultiTupleStreamTest, MultiTupleOneBufferSpill) {
  // Each buffer can only hold 100 ints, so this spills quite often.
  int buffer_size = 100 * sizeof(int);
  InitBlockMgr(buffer_size, buffer_size);
  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);

  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
}

// Test with a few buffers and rows with multiple tuples.
TEST_F(MultiTupleStreamTest, MultiTupleManyBufferSpill) {
  int buffer_size = 100 * sizeof(int);
  InitBlockMgr(10 * buffer_size, buffer_size);

  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);
  TestValues<int>(100, int_desc_, false, true);

  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
  TestValues<StringValue>(100, string_desc_, false, true);

  TestIntValuesInterleaved(1, 1, true);
  TestIntValuesInterleaved(10, 5, true);
  TestIntValuesInterleaved(100, 15, true);
}

// Test that we can allocate a row in the stream and copy in multiple tuples then
// read it back from the stream.
TEST_F(MultiTupleStreamTest, MultiTupleAllocateRow) {
  // Use small buffers so it will be flushed to disk.
  int buffer_size = 4 * 1024;
  InitBlockMgr(2 * buffer_size, buffer_size);
  Status status = Status::OK();

  int num_batches = 1;
  int rows_added = 0;
  BufferedTupleStream stream(runtime_state_, *string_desc_, runtime_state_->block_mgr(),
      client_, false, false);

  for (int i = 0; i < num_batches; ++i) {
    RowBatch* batch = CreateStringBatch(rows_added, 1, false);
    for (int j = 0; j < batch->num_rows(); ++j) {
      TupleRow* row = batch->GetRow(j);
      int64_t fixed_size = 0;
      int64_t varlen_size = 0;
      for (int k = 0; k < string_desc_->tuple_descriptors().size(); k++) {
        TupleDescriptor* tuple_desc = string_desc_->tuple_descriptors()[k];
        fixed_size += tuple_desc->byte_size();
        varlen_size += row->GetTuple(k)->VarlenByteSize(*tuple_desc);
      }
      uint8_t* varlen_data;
      uint8_t* fixed_data = stream.AllocateRow(fixed_size, varlen_size, &varlen_data,
          &status);
      ASSERT_TRUE(fixed_data != NULL);
      ASSERT_TRUE(status.ok());
      uint8_t* varlen_write_ptr = varlen_data;
      for (int k = 0; k < string_desc_->tuple_descriptors().size(); k++) {
        TupleDescriptor* tuple_desc = string_desc_->tuple_descriptors()[k];
        Tuple* src = row->GetTuple(k);
        Tuple* dst = reinterpret_cast<Tuple*>(fixed_data);
        fixed_data += tuple_desc->byte_size();
        memcpy(dst, src, tuple_desc->byte_size());
        for (int l = 0; l < tuple_desc->slots().size(); l++) {
          SlotDescriptor* slot = tuple_desc->slots()[l];
          StringValue* src_string = src->GetStringSlot(slot->tuple_offset());
          StringValue* dst_string = dst->GetStringSlot(slot->tuple_offset());
          dst_string->ptr = reinterpret_cast<char*>(varlen_write_ptr);
          memcpy(dst_string->ptr, src_string->ptr, src_string->len);
          varlen_write_ptr += src_string->len;
        }
      }
      ASSERT_EQ(varlen_data + varlen_size, varlen_write_ptr);
    }
    rows_added += batch->num_rows();
  }

  for (int i = 0; i < 3; ++i) {
    bool delete_on_read = i == 2;
    vector<StringValue> results;
    stream.PrepareForRead(delete_on_read);
    ReadValues(&stream, string_desc_, &results);
    VerifyResults(results, rows_added, false);
  }

  stream.Close();
}

// Test with rows with multiple nullable tuples.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleOneBufferSpill) {
  // Each buffer can only hold 100 ints, so this spills quite often.
  int buffer_size = 100 * sizeof(int);
  InitBlockMgr(buffer_size, buffer_size);
  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);
  TestValues<int>(1, int_desc_, true, true);
  TestValues<int>(10, int_desc_, true, true);

  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
  TestValues<StringValue>(1, string_desc_, true, true);
  TestValues<StringValue>(10, string_desc_, true, true);
}

// Test with a few buffers.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleManyBufferSpill) {
  int buffer_size = 100 * sizeof(int);
  InitBlockMgr(10 * buffer_size, buffer_size);

  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);
  TestValues<int>(100, int_desc_, false, true);
  TestValues<int>(1, int_desc_, true, true);
  TestValues<int>(10, int_desc_, true, true);
  TestValues<int>(100, int_desc_, true, true);

  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
  TestValues<StringValue>(100, string_desc_, false, true);
  TestValues<StringValue>(1, string_desc_, true, true);
  TestValues<StringValue>(10, string_desc_, true, true);
  TestValues<StringValue>(100, string_desc_, true, true);

  TestIntValuesInterleaved(1, 1, true);
  TestIntValuesInterleaved(10, 5, true);
  TestIntValuesInterleaved(100, 15, true);
}

/// Test that ComputeRowSize handles nulls
TEST_F(MultiNullableTupleStreamTest, TestComputeRowSize) {
  InitBlockMgr(-1, 8 * 1024 * 1024);
  const vector<TupleDescriptor*>& tuple_descs = string_desc_->tuple_descriptors();
  // String in second tuple is stored externally.
  set<SlotId> external_slots;
  const SlotDescriptor* external_string_slot = tuple_descs[1]->slots()[0];
  external_slots.insert(external_string_slot->id());

  BufferedTupleStream stream(runtime_state_, *string_desc_, runtime_state_->block_mgr(),
      client_, false, false, external_slots);
  gscoped_ptr<TupleRow, FreeDeleter> row(reinterpret_cast<TupleRow*>(
        malloc(tuple_descs.size() * sizeof(Tuple*))));
  gscoped_ptr<Tuple, FreeDeleter> tuple0(reinterpret_cast<Tuple*>(
        malloc(tuple_descs[0]->byte_size())));
  gscoped_ptr<Tuple, FreeDeleter> tuple1(reinterpret_cast<Tuple*>(
        malloc(tuple_descs[1]->byte_size())));
  gscoped_ptr<Tuple, FreeDeleter> tuple2(reinterpret_cast<Tuple*>(
        malloc(tuple_descs[2]->byte_size())));
  memset(tuple0.get(), 0, tuple_descs[0]->byte_size());
  memset(tuple1.get(), 0, tuple_descs[1]->byte_size());
  memset(tuple2.get(), 0, tuple_descs[2]->byte_size());

  // All nullable tuples are NULL.
  row->SetTuple(0, tuple0.get());
  row->SetTuple(1, NULL);
  row->SetTuple(2, NULL);
  EXPECT_EQ(tuple_descs[0]->byte_size(), stream.ComputeRowSize(row.get()));

  // Tuples are initialized to empty and have no var-len data.
  row->SetTuple(1, tuple1.get());
  row->SetTuple(2, tuple2.get());
  EXPECT_EQ(string_desc_->GetRowSize(), stream.ComputeRowSize(row.get()));

  // Tuple 0 has some data.
  const SlotDescriptor* string_slot = tuple_descs[0]->slots()[0];
  StringValue* sv = tuple0->GetStringSlot(string_slot->tuple_offset());
  *sv = STRINGS[0];
  int64_t expected_len = string_desc_->GetRowSize() + sv->len;
  EXPECT_EQ(expected_len, stream.ComputeRowSize(row.get()));

  // Check that external slots aren't included in count.
  sv = tuple1->GetStringSlot(external_string_slot->tuple_offset());
  sv->ptr = reinterpret_cast<char*>(1234);
  sv->len = 1234;
  EXPECT_EQ(expected_len, stream.ComputeRowSize(row.get()));

  stream.Close();
}

/// Test that deep copy works with arrays by copying into a BufferedTupleStream, freeing
/// the original rows, then reading back the rows and verifying the contents.
TEST_F(ArrayTupleStreamTest, TestArrayDeepCopy) {
  Status status;
  InitBlockMgr(-1, 8 * 1024 * 1024);
  const int NUM_ROWS = 4000;
  BufferedTupleStream stream(runtime_state_, *array_desc_, runtime_state_->block_mgr(),
      client_, false, false);
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
    CollectionValue* cv = tuple0->GetCollectionSlot(array_slot_desc->tuple_offset());
    cv->ptr = NULL;
    cv->num_tuples = 0;
    CollectionValueBuilder builder(cv, *item_desc, mem_pool_.get(), array_len);
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
    ASSERT_OK(status);
    mem_pool_->FreeAll(); // Free data as soon as possible to smoke out issues.
  }

  // Read back and verify data.
  stream.PrepareForRead(false);
  strings_index = 0;
  array_len_index = 0;
  bool eos = false;
  int rows_read = 0;
  RowBatch batch(*array_desc_, BATCH_SIZE, &tracker_);
  do {
    batch.Reset();
    ASSERT_OK(stream.GetNext(&batch, &eos));
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
      CollectionValue* cv = tuple0->GetCollectionSlot(array_slot_desc->tuple_offset());
      ASSERT_EQ(expected_array_len, cv->num_tuples);
      for (int j = 0; j < cv->num_tuples; ++j) {
        Tuple* item = reinterpret_cast<Tuple*>(cv->ptr + j * item_desc->byte_size());
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
  stream.Close();
}

/// Test that ComputeRowSize handles nulls
TEST_F(ArrayTupleStreamTest, TestComputeRowSize) {
  InitBlockMgr(-1, 8 * 1024 * 1024);
  const vector<TupleDescriptor*>& tuple_descs = array_desc_->tuple_descriptors();
  set<SlotId> external_slots;
  // Second array slot in first tuple is stored externally.
  const SlotDescriptor* external_array_slot = tuple_descs[0]->slots()[1];
  external_slots.insert(external_array_slot->id());

  BufferedTupleStream stream(runtime_state_, *array_desc_, runtime_state_->block_mgr(),
      client_, false, false, external_slots);
  gscoped_ptr<TupleRow, FreeDeleter> row(reinterpret_cast<TupleRow*>(
        malloc(tuple_descs.size() * sizeof(Tuple*))));
  gscoped_ptr<Tuple, FreeDeleter> tuple0(reinterpret_cast<Tuple*>(
        malloc(tuple_descs[0]->byte_size())));
  gscoped_ptr<Tuple, FreeDeleter> tuple1(reinterpret_cast<Tuple*>(
        malloc(tuple_descs[1]->byte_size())));
  memset(tuple0.get(), 0, tuple_descs[0]->byte_size());
  memset(tuple1.get(), 0, tuple_descs[1]->byte_size());

  // All tuples are NULL.
  row->SetTuple(0, NULL);
  row->SetTuple(1, NULL);
  EXPECT_EQ(0, stream.ComputeRowSize(row.get()));

  // Tuples are initialized to empty and have no var-len data.
  row->SetTuple(0, tuple0.get());
  row->SetTuple(1, tuple1.get());
  EXPECT_EQ(array_desc_->GetRowSize(), stream.ComputeRowSize(row.get()));

  // Tuple 0 has an array.
  int expected_row_size = array_desc_->GetRowSize();
  const SlotDescriptor* array_slot = tuple_descs[0]->slots()[0];
  const TupleDescriptor* item_desc = array_slot->collection_item_descriptor();
  int array_len = 128;
  CollectionValue* cv = tuple0->GetCollectionSlot(array_slot->tuple_offset());
  CollectionValueBuilder builder(cv, *item_desc, mem_pool_.get(), array_len);
  Tuple* array_data;
  builder.GetFreeMemory(&array_data);
  expected_row_size += item_desc->byte_size() * array_len;

  // Fill the array with pointers to our constant strings.
  for (int i = 0; i < array_len; ++i) {
    const StringValue* str = &STRINGS[i % NUM_STRINGS];
    array_data->SetNotNull(item_desc->slots()[0]->null_indicator_offset());
    RawValue::Write(str, array_data, item_desc->slots()[0], mem_pool_.get());
    array_data += item_desc->byte_size();
    expected_row_size += str->len;
  }
  builder.CommitTuples(array_len);
  EXPECT_EQ(expected_row_size, stream.ComputeRowSize(row.get()));

  // Check that the external slot isn't included in size.
  cv = tuple0->GetCollectionSlot(external_array_slot->tuple_offset());
  // ptr of external slot shouldn't be dereferenced when computing size.
  cv->ptr = reinterpret_cast<uint8_t*>(1234);
  cv->num_tuples = 1234;
  EXPECT_EQ(expected_row_size, stream.ComputeRowSize(row.get()));

  // Check that the array is excluded if tuple 0's array has its null indicator set.
  tuple0->SetNull(array_slot->null_indicator_offset());
  EXPECT_EQ(array_desc_->GetRowSize(), stream.ComputeRowSize(row.get()));

  stream.Close();
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
