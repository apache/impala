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

#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/scoped_ptr.hpp>

#include <limits> // for std::numeric_limits<int>::max()
#include <set>
#include <string>

#include "codegen/llvm-codegen.h"
#include "gutil/gscoped_ptr.h"
#include "runtime/buffered-tuple-stream-v2.inline.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/collection-value-builder.h"
#include "runtime/collection-value.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.inline.h"
#include "runtime/test-env.h"
#include "runtime/tmp-file-mgr.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/gtest-util.h"
#include "util/test-info.h"

#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Types_types.h"

#include "common/names.h"

using kudu::FreeDeleter;
using std::numeric_limits;

static const int BATCH_SIZE = 250;
// Allow arbitrarily small pages in our test buffer pool.
static const int MIN_PAGE_LEN = 1;
// Limit the size of the buffer pool to bound memory consumption.
static const int64_t BUFFER_POOL_LIMIT = 1024L * 1024L * 1024L;

// The page length to use for the streams.
static const int PAGE_LEN = 2 * 1024 * 1024;
static const uint32_t PRIME = 479001599;

namespace impala {

static const StringValue STRINGS[] = {
    StringValue("ABC"), StringValue("HELLO"), StringValue("123456789"),
    StringValue("FOOBAR"), StringValue("ONE"), StringValue("THREE"),
    StringValue("abcdefghijklmno"), StringValue("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    StringValue("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
};

static const int NUM_STRINGS = sizeof(STRINGS) / sizeof(StringValue);

class SimpleTupleStreamTest : public testing::Test {
 protected:
  virtual void SetUp() {}

  virtual void CreateDescriptors() {
    vector<bool> nullable_tuples(1, false);
    vector<TTupleId> tuple_ids(1, static_cast<TTupleId>(0));

    DescriptorTblBuilder int_builder(test_env_->exec_env()->frontend(), &pool_);
    int_builder.DeclareTuple() << TYPE_INT;
    int_desc_ =
        pool_.Add(new RowDescriptor(*int_builder.Build(), tuple_ids, nullable_tuples));

    DescriptorTblBuilder string_builder(test_env_->exec_env()->frontend(), &pool_);
    string_builder.DeclareTuple() << TYPE_STRING;
    string_desc_ =
        pool_.Add(new RowDescriptor(*string_builder.Build(), tuple_ids, nullable_tuples));
  }

  virtual void TearDown() {
    if (client_.is_registered()) {
      test_env_->exec_env()->buffer_pool()->DeregisterClient(&client_);
    }
    runtime_state_ = nullptr;
    pool_.Clear();
    mem_pool_->FreeAll();
    test_env_.reset();
  }

  /// Set up all of the test state: the buffer pool, a query state, a client with no
  /// reservation and any other descriptors, etc.
  /// The buffer pool's capacity is limited to 'buffer_pool_limit'.
  void Init(int64_t buffer_pool_limit) {
    test_env_.reset(new TestEnv());
    test_env_->SetBufferPoolArgs(MIN_PAGE_LEN, buffer_pool_limit);
    ASSERT_OK(test_env_->Init());

    CreateDescriptors();
    mem_pool_.reset(new MemPool(&tracker_));

    ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &runtime_state_));
    query_state_ = runtime_state_->query_state();

    RuntimeProfile* client_profile = pool_.Add(new RuntimeProfile(&pool_, "client"));
    MemTracker* client_tracker =
        pool_.Add(new MemTracker(-1, "client", runtime_state_->instance_mem_tracker()));
    ASSERT_OK(test_env_->exec_env()->buffer_pool()->RegisterClient("",
        query_state_->file_group(), runtime_state_->instance_buffer_reservation(),
        client_tracker, numeric_limits<int>::max(), client_profile, &client_));
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

  /// Count the total number of slots per row based on the given row descriptor.
  int CountSlotsPerRow(const RowDescriptor& row_desc) {
    int slots_per_row = 0;
    for (int i = 0; i < row_desc.tuple_descriptors().size(); ++i) {
      TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[i];
      slots_per_row += tuple_desc->slots().size();
    }
    return slots_per_row;
  }

  /// Allocate a row batch with 'num_rows' of rows with layout described by 'row_desc'.
  /// 'offset' is used to account for rows occupied by any previous row batches. This is
  /// needed to match the values generated in VerifyResults(). If 'gen_null' is true,
  /// some tuples will be set to NULL.
  virtual RowBatch* CreateBatch(
      const RowDescriptor& row_desc, int offset, int num_rows, bool gen_null) {
    RowBatch* batch = pool_.Add(new RowBatch(row_desc, num_rows, &tracker_));
    int num_tuples = row_desc.tuple_descriptors().size();

    int idx = offset * CountSlotsPerRow(row_desc);
    for (int row_idx = 0; row_idx < num_rows; ++row_idx) {
      TupleRow* row = batch->GetRow(row_idx);
      for (int tuple_idx = 0; tuple_idx < num_tuples; ++tuple_idx) {
        TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[tuple_idx];
        Tuple* tuple = Tuple::Create(tuple_desc->byte_size(), batch->tuple_data_pool());
        bool is_null = gen_null && !GenBoolValue(idx);
        for (int slot_idx = 0; slot_idx < tuple_desc->slots().size(); ++slot_idx, ++idx) {
          SlotDescriptor* slot_desc = tuple_desc->slots()[slot_idx];
          void* slot = tuple->GetSlot(slot_desc->tuple_offset());
          switch (slot_desc->type().type) {
            case TYPE_INT:
              *reinterpret_cast<int*>(slot) = GenIntValue(idx);
              break;
            case TYPE_STRING:
              *reinterpret_cast<StringValue*>(slot) = STRINGS[idx % NUM_STRINGS];
              break;
            default:
              // The memory has been zero'ed out already by Tuple::Create().
              break;
          }
        }
        if (is_null) {
          row->SetTuple(tuple_idx, nullptr);
        } else {
          row->SetTuple(tuple_idx, tuple);
        }
      }
      batch->CommitLastRow();
    }
    return batch;
  }

  virtual RowBatch* CreateIntBatch(int offset, int num_rows, bool gen_null) {
    return CreateBatch(*int_desc_, offset, num_rows, gen_null);
  }

  virtual RowBatch* CreateStringBatch(int offset, int num_rows, bool gen_null) {
    return CreateBatch(*string_desc_, offset, num_rows, gen_null);
  }

  void AppendValue(uint8_t* ptr, vector<int>* results) {
    if (ptr == nullptr) {
      // For the tests indicate null-ability using the max int value
      results->push_back(numeric_limits<int>::max());
    } else {
      results->push_back(*reinterpret_cast<int*>(ptr));
    }
  }

  void AppendValue(uint8_t* ptr, vector<StringValue>* results) {
    if (ptr == nullptr) {
      results->push_back(StringValue());
    } else {
      StringValue sv = *reinterpret_cast<StringValue*>(ptr);
      uint8_t* copy = mem_pool_->Allocate(sv.len);
      memcpy(copy, sv.ptr, sv.len);
      sv.ptr = reinterpret_cast<char*>(copy);
      results->push_back(sv);
    }
  }

  template <typename T>
  void AppendRowTuples(TupleRow* row, RowDescriptor* row_desc, vector<T>* results) {
    DCHECK(row != nullptr);
    const int num_tuples = row_desc->tuple_descriptors().size();

    for (int tuple_idx = 0; tuple_idx < num_tuples; ++tuple_idx) {
      TupleDescriptor* tuple_desc = row_desc->tuple_descriptors()[tuple_idx];
      Tuple* tuple = row->GetTuple(tuple_idx);
      const int num_slots = tuple_desc->slots().size();
      for (int slot_idx = 0; slot_idx < num_slots; ++slot_idx) {
        SlotDescriptor* slot_desc = tuple_desc->slots()[slot_idx];
        if (tuple == nullptr) {
          AppendValue(nullptr, results);
        } else {
          void* slot = tuple->GetSlot(slot_desc->tuple_offset());
          AppendValue(reinterpret_cast<uint8_t*>(slot), results);
        }
      }
    }
  }

  template <typename T>
  void ReadValues(BufferedTupleStreamV2* stream, RowDescriptor* desc, vector<T>* results,
      int num_batches = -1) {
    bool eos = false;
    RowBatch batch(*desc, BATCH_SIZE, &tracker_);
    int batches_read = 0;
    do {
      batch.Reset();
      EXPECT_OK(stream->GetNext(&batch, &eos));
      ++batches_read;
      for (int i = 0; i < batch.num_rows(); ++i) {
        AppendRowTuples(batch.GetRow(i), desc, results);
      }
    } while (!eos && (num_batches < 0 || batches_read <= num_batches));
  }

  void GetExpectedValue(int idx, bool is_null, int* val) {
    if (is_null) {
      *val = numeric_limits<int>::max();
    } else {
      *val = GenIntValue(idx);
    }
  }

  void GetExpectedValue(int idx, bool is_null, StringValue* val) {
    if (is_null) {
      *val = StringValue();
    } else {
      *val = STRINGS[idx % NUM_STRINGS];
    }
  }

  template <typename T>
  void VerifyResults(const RowDescriptor& row_desc, const vector<T>& results,
      int num_rows, bool gen_null) {
    int idx = 0;
    for (int row_idx = 0; row_idx < num_rows; ++row_idx) {
      const int num_tuples = row_desc.tuple_descriptors().size();
      for (int tuple_idx = 0; tuple_idx < num_tuples; ++tuple_idx) {
        const TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[tuple_idx];
        const int num_slots = tuple_desc->slots().size();
        bool is_null = gen_null && !GenBoolValue(idx);
        for (int slot_idx = 0; slot_idx < num_slots; ++slot_idx, ++idx) {
          T expected_val;
          GetExpectedValue(idx, is_null, &expected_val);
          ASSERT_EQ(results[idx], expected_val)
              << "results[" << idx << "] " << results[idx] << " != " << expected_val
              << " row_idx=" << row_idx << " tuple_idx=" << tuple_idx
              << " slot_idx=" << slot_idx << " gen_null=" << gen_null;
        }
      }
    }
    DCHECK_EQ(results.size(), idx);
  }

  // Test adding num_batches of ints to the stream and reading them back.
  // If unpin_stream is true, operate the stream in unpinned mode.
  // Assumes that enough buffers are available to read and write the stream.
  template <typename T>
  void TestValues(int num_batches, RowDescriptor* desc, bool gen_null, bool unpin_stream,
      int64_t page_len = PAGE_LEN, int num_rows = BATCH_SIZE) {
    BufferedTupleStreamV2 stream(runtime_state_, *desc, &client_, page_len);
    ASSERT_OK(stream.Init(-1, true));
    bool got_write_reservation;
    ASSERT_OK(stream.PrepareForWrite(&got_write_reservation));
    ASSERT_TRUE(got_write_reservation);

    if (unpin_stream) {
      stream.UnpinStream(BufferedTupleStreamV2::UNPIN_ALL_EXCEPT_CURRENT);
    }
    // Add rows to the stream
    int offset = 0;
    for (int i = 0; i < num_batches; ++i) {
      RowBatch* batch = nullptr;

      Status status;
      ASSERT_TRUE(sizeof(T) == sizeof(int) || sizeof(T) == sizeof(StringValue));
      batch = CreateBatch(*desc, offset, num_rows, gen_null);
      for (int j = 0; j < batch->num_rows(); ++j) {
        // TODO: test that AddRow succeeds after freeing memory.
        bool b = stream.AddRow(batch->GetRow(j), &status);
        ASSERT_OK(status);
        ASSERT_TRUE(b);
      }
      offset += batch->num_rows();
      // Reset the batch to make sure the stream handles the memory correctly.
      batch->Reset();
    }

    bool got_read_reservation;
    ASSERT_OK(stream.PrepareForRead(false, &got_read_reservation));
    ASSERT_TRUE(got_read_reservation);

    // Read all the rows back
    vector<T> results;
    ReadValues(&stream, desc, &results);

    // Verify result
    VerifyResults<T>(*desc, results, num_rows * num_batches, gen_null);

    stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }

  void TestIntValuesInterleaved(int num_batches, int num_batches_before_read,
      bool unpin_stream, int64_t page_len = PAGE_LEN) {
    BufferedTupleStreamV2 stream(runtime_state_, *int_desc_, &client_, page_len);
    ASSERT_OK(stream.Init(-1, true));
    bool got_reservation;
    ASSERT_OK(stream.PrepareForReadWrite(true, &got_reservation));
    ASSERT_TRUE(got_reservation);
    if (unpin_stream) {
      stream.UnpinStream(BufferedTupleStreamV2::UNPIN_ALL_EXCEPT_CURRENT);
    }

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
        ReadValues(&stream, int_desc_, &results, (rand() % num_batches_before_read) + 1);
      }
    }
    ReadValues(&stream, int_desc_, &results);

    VerifyResults<int>(*int_desc_, results, BATCH_SIZE * num_batches, false);

    stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }

  void TestUnpinPin(bool varlen_data, bool read_write);

  void TestTransferMemory(bool pinned_stream, bool read_write);

  // The temporary runtime environment used for the test.
  scoped_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;
  QueryState* query_state_;

  // Buffer pool client - automatically deregistered in TearDown().
  BufferPool::ClientHandle client_;

  // Dummy MemTracker used for miscellaneous memory.
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

    DescriptorTblBuilder int_builder(test_env_->exec_env()->frontend(), &pool_);
    int_builder.DeclareTuple() << TYPE_INT;
    int_desc_ =
        pool_.Add(new RowDescriptor(*int_builder.Build(), tuple_ids, nullable_tuples));

    DescriptorTblBuilder string_builder(test_env_->exec_env()->frontend(), &pool_);
    string_builder.DeclareTuple() << TYPE_STRING;
    string_desc_ =
        pool_.Add(new RowDescriptor(*string_builder.Build(), tuple_ids, nullable_tuples));
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

    DescriptorTblBuilder int_builder(test_env_->exec_env()->frontend(), &pool_);
    int_builder.DeclareTuple() << TYPE_INT;
    int_builder.DeclareTuple() << TYPE_INT;
    int_builder.DeclareTuple() << TYPE_INT;
    int_desc_ =
        pool_.Add(new RowDescriptor(*int_builder.Build(), tuple_ids, nullable_tuples));

    DescriptorTblBuilder string_builder(test_env_->exec_env()->frontend(), &pool_);
    string_builder.DeclareTuple() << TYPE_STRING;
    string_builder.DeclareTuple() << TYPE_STRING;
    string_builder.DeclareTuple() << TYPE_STRING;
    string_desc_ =
        pool_.Add(new RowDescriptor(*string_builder.Build(), tuple_ids, nullable_tuples));
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

    DescriptorTblBuilder int_builder(test_env_->exec_env()->frontend(), &pool_);
    int_builder.DeclareTuple() << TYPE_INT;
    int_builder.DeclareTuple() << TYPE_INT;
    int_builder.DeclareTuple() << TYPE_INT;
    int_desc_ =
        pool_.Add(new RowDescriptor(*int_builder.Build(), tuple_ids, nullable_tuples));

    DescriptorTblBuilder string_builder(test_env_->exec_env()->frontend(), &pool_);
    string_builder.DeclareTuple() << TYPE_STRING;
    string_builder.DeclareTuple() << TYPE_STRING;
    string_builder.DeclareTuple() << TYPE_STRING;
    string_desc_ =
        pool_.Add(new RowDescriptor(*string_builder.Build(), tuple_ids, nullable_tuples));
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

    DescriptorTblBuilder builder(test_env_->exec_env()->frontend(), &pool_);
    builder.DeclareTuple() << string_array_type << nested_array_type;
    builder.DeclareTuple() << int_array_type;
    array_desc_ =
        pool_.Add(new RowDescriptor(*builder.Build(), tuple_ids, nullable_tuples));
  }
};

// Basic API test. No data should be going to disk.
TEST_F(SimpleTupleStreamTest, Basic) {
  Init(numeric_limits<int64_t>::max());
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
  // Each buffer can only hold 128 ints, so this spills quite often.
  int buffer_size = 128 * sizeof(int);
  Init(buffer_size);
  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);

  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
}

// Test with a few buffers.
TEST_F(SimpleTupleStreamTest, ManyBufferSpill) {
  int buffer_size = 128 * sizeof(int);
  Init(10 * buffer_size);

  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);
  TestValues<int>(100, int_desc_, false, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(100, string_desc_, false, true, buffer_size);

  TestIntValuesInterleaved(1, 1, true, buffer_size);
  TestIntValuesInterleaved(10, 5, true, buffer_size);
  TestIntValuesInterleaved(100, 15, true, buffer_size);
}

void SimpleTupleStreamTest::TestUnpinPin(bool varlen_data, bool read_write) {
  int buffer_size = 128 * sizeof(int);
  int num_buffers = 10;
  Init(num_buffers * buffer_size);
  RowDescriptor* row_desc = varlen_data ? string_desc_ : int_desc_;

  BufferedTupleStreamV2 stream(runtime_state_, *row_desc, &client_, buffer_size);
  ASSERT_OK(stream.Init(-1, true));
  if (read_write) {
    bool got_reservation = false;
    ASSERT_OK(stream.PrepareForReadWrite(false, &got_reservation));
    ASSERT_TRUE(got_reservation);
  } else {
    bool got_write_reservation;
    ASSERT_OK(stream.PrepareForWrite(&got_write_reservation));
    ASSERT_TRUE(got_write_reservation);
  }

  int offset = 0;
  bool full = false;
  int num_batches = 0;
  while (!full) {
    // Make sure we can switch between pinned and unpinned states while writing.
    if (num_batches % 10 == 0) {
      bool pinned;
      stream.UnpinStream(BufferedTupleStreamV2::UNPIN_ALL_EXCEPT_CURRENT);
      ASSERT_OK(stream.PinStream(&pinned));
      DCHECK(pinned);
    }

    RowBatch* batch = varlen_data ? CreateStringBatch(offset, BATCH_SIZE, false) :
                                    CreateIntBatch(offset, BATCH_SIZE, false);
    int j = 0;
    for (; j < batch->num_rows(); ++j) {
      Status status;
      full = !stream.AddRow(batch->GetRow(j), &status);
      ASSERT_OK(status);
      if (full) break;
    }
    offset += j;
    ++num_batches;
  }

  stream.UnpinStream(BufferedTupleStreamV2::UNPIN_ALL_EXCEPT_CURRENT);

  bool pinned = false;
  ASSERT_OK(stream.PinStream(&pinned));
  ASSERT_TRUE(pinned);

  // Read and verify result a few times. We should be able to reread the stream if
  // we don't use delete on read mode.
  int read_iters = 3;
  for (int i = 0; i < read_iters; ++i) {
    bool delete_on_read = i == read_iters - 1;
    if (i > 0 || !read_write) {
      bool got_read_reservation;
      ASSERT_OK(stream.PrepareForRead(delete_on_read, &got_read_reservation));
      ASSERT_TRUE(got_read_reservation);
    }

    if (varlen_data) {
      vector<StringValue> results;
      ReadValues(&stream, row_desc, &results);
      VerifyResults<StringValue>(*string_desc_, results, offset, false);
    } else {
      vector<int> results;
      ReadValues(&stream, row_desc, &results);
      VerifyResults<int>(*int_desc_, results, offset, false);
    }
  }

  // After delete_on_read, all blocks aside from the last should be deleted.
  // Note: this should really be 0, but the BufferedTupleStreamV2 returns eos before
  // deleting the last block, rather than after, so the last block isn't deleted
  // until the stream is closed.
  ASSERT_EQ(stream.BytesPinned(false), buffer_size);

  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);

  ASSERT_EQ(stream.BytesPinned(false), 0);
}

TEST_F(SimpleTupleStreamTest, UnpinPin) {
  TestUnpinPin(false, false);
}

TEST_F(SimpleTupleStreamTest, UnpinPinReadWrite) {
  TestUnpinPin(false, true);
}

TEST_F(SimpleTupleStreamTest, UnpinPinVarlen) {
  TestUnpinPin(false, false);
}

void SimpleTupleStreamTest::TestTransferMemory(bool pin_stream, bool read_write) {
  // Use smaller buffers so that the explicit FLUSH_RESOURCES flag is required to
  // make the batch at capacity.
  int buffer_size = 4 * 1024;
  Init(100 * buffer_size);

  BufferedTupleStreamV2 stream(runtime_state_, *int_desc_, &client_, buffer_size);
  ASSERT_OK(stream.Init(-1, pin_stream));
  if (read_write) {
    bool got_reservation;
    ASSERT_OK(stream.PrepareForReadWrite(true, &got_reservation));
    ASSERT_TRUE(got_reservation);
  } else {
    bool got_write_reservation;
    ASSERT_OK(stream.PrepareForWrite(&got_write_reservation));
    ASSERT_TRUE(got_write_reservation);
  }
  RowBatch* batch = CreateIntBatch(0, 1024, false);

  // Construct a stream with 4 blocks.
  const int total_num_buffers = 4;
  while (stream.byte_size() < total_num_buffers * buffer_size) {
    Status status;
    for (int i = 0; i < batch->num_rows(); ++i) {
      bool ret = stream.AddRow(batch->GetRow(i), &status);
      EXPECT_TRUE(ret);
      ASSERT_OK(status);
    }
  }

  batch->Reset();
  stream.Close(batch, RowBatch::FlushMode::FLUSH_RESOURCES);
  if (pin_stream) {
    DCHECK_EQ(total_num_buffers, batch->num_buffers());
  } else if (read_write) {
    // Read and write block should be attached.
    DCHECK_EQ(2, batch->num_buffers());
  } else {
    // Read block should be attached.
    DCHECK_EQ(1, batch->num_buffers());
  }
  DCHECK(batch->AtCapacity()); // Flush resources flag should have been set.
  batch->Reset();
  DCHECK_EQ(0, batch->num_buffers());
}

/// Test attaching memory to a row batch from a pinned stream.
TEST_F(SimpleTupleStreamTest, TransferMemoryFromPinnedStreamReadWrite) {
  TestTransferMemory(true, true);
}

TEST_F(SimpleTupleStreamTest, TransferMemoryFromPinnedStreamNoReadWrite) {
  TestTransferMemory(true, false);
}

/// Test attaching memory to a row batch from an unpinned stream.
TEST_F(SimpleTupleStreamTest, TransferMemoryFromUnpinnedStreamReadWrite) {
  TestTransferMemory(false, true);
}

TEST_F(SimpleTupleStreamTest, TransferMemoryFromUnpinnedStreamNoReadWrite) {
  TestTransferMemory(false, false);
}

// Test that tuple stream functions if it references strings outside stream. The
// aggregation node relies on this since it updates tuples in-place.
TEST_F(SimpleTupleStreamTest, StringsOutsideStream) {
  int buffer_size = 8 * 1024 * 1024;
  Init(2 * buffer_size);
  Status status = Status::OK();

  int num_batches = 100;
  int rows_added = 0;
  DCHECK_EQ(string_desc_->tuple_descriptors().size(), 1);
  TupleDescriptor& tuple_desc = *string_desc_->tuple_descriptors()[0];

  set<SlotId> external_slots;
  for (int i = 0; i < tuple_desc.string_slots().size(); ++i) {
    external_slots.insert(tuple_desc.string_slots()[i]->id());
  }

  BufferedTupleStreamV2 stream(
      runtime_state_, *string_desc_, &client_, buffer_size, external_slots);
  ASSERT_OK(stream.Init(0, false));

  for (int i = 0; i < num_batches; ++i) {
    RowBatch* batch = CreateStringBatch(rows_added, BATCH_SIZE, false);
    for (int j = 0; j < batch->num_rows(); ++j) {
      uint8_t* varlen_data;
      int fixed_size = tuple_desc.byte_size();
      uint8_t* tuple = stream.AllocateRow(fixed_size, 0, &varlen_data, &status);
      ASSERT_TRUE(tuple != nullptr);
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
    bool got_read_reservation;
    ASSERT_OK(stream.PrepareForRead(delete_on_read, &got_read_reservation));
    ASSERT_TRUE(got_read_reservation);
    ReadValues(&stream, string_desc_, &results);
    VerifyResults<StringValue>(*string_desc_, results, rows_added, false);
  }

  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

// Construct a big row by stiching together many tuples so the total row size
// will be close to the IO block size. With null indicators, stream will fail to
// be initialized; Without null indicators, things should work fine.
TEST_F(SimpleTupleStreamTest, BigRow) {
  Init(2 * PAGE_LEN);
  vector<TupleId> tuple_ids;
  vector<bool> nullable_tuples;
  vector<bool> non_nullable_tuples;

  DescriptorTblBuilder big_row_builder(test_env_->exec_env()->frontend(), &pool_);
  // Each tuple contains 8 slots of TYPE_INT and a single byte for null indicator.
  const int num_tuples = PAGE_LEN / (8 * sizeof(int) + 1);
  for (int tuple_idx = 0; tuple_idx < num_tuples; ++tuple_idx) {
    big_row_builder.DeclareTuple() << TYPE_INT << TYPE_INT << TYPE_INT << TYPE_INT
                                   << TYPE_INT << TYPE_INT << TYPE_INT << TYPE_INT;
    tuple_ids.push_back(static_cast<TTupleId>(tuple_idx));
    nullable_tuples.push_back(true);
    non_nullable_tuples.push_back(false);
  }
  DescriptorTbl* desc = big_row_builder.Build();

  // Construct a big row with all non-nullable tuples.
  RowDescriptor* row_desc =
      pool_.Add(new RowDescriptor(*desc, tuple_ids, non_nullable_tuples));
  ASSERT_FALSE(row_desc->IsAnyTupleNullable());
  // Test writing this row into the stream and then reading it back.
  TestValues<int>(1, row_desc, false, false, PAGE_LEN, 1);
  TestValues<int>(1, row_desc, false, true, PAGE_LEN, 1);

  // Construct a big row with nullable tuples. This requires extra space for null
  // indicators in the stream so adding the row will fail.
  RowDescriptor* nullable_row_desc =
      pool_.Add(new RowDescriptor(*desc, tuple_ids, nullable_tuples));
  ASSERT_TRUE(nullable_row_desc->IsAnyTupleNullable());
  BufferedTupleStreamV2 nullable_stream(
      runtime_state_, *nullable_row_desc, &client_, PAGE_LEN);
  ASSERT_OK(nullable_stream.Init(-1, true));
  bool got_reservation;
  Status status = nullable_stream.PrepareForWrite(&got_reservation);
  EXPECT_EQ(TErrorCode::BTS_BLOCK_OVERFLOW, status.code());
  nullable_stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

// Test for IMPALA-3923: overflow of 32-bit int in GetRows().
TEST_F(SimpleTupleStreamTest, TestGetRowsOverflow) {
  Init(BUFFER_POOL_LIMIT);
  BufferedTupleStreamV2 stream(runtime_state_, *int_desc_, &client_, PAGE_LEN);
  ASSERT_OK(stream.Init(-1, true));

  Status status;
  // Add more rows than can be fit in a RowBatch (limited by its 32-bit row count).
  // Actually adding the rows would take a very long time, so just set num_rows_.
  // This puts the stream in an inconsistent state, but exercises the right code path.
  stream.num_rows_ = 1L << 33;
  bool got_rows;
  scoped_ptr<RowBatch> overflow_batch;
  ASSERT_FALSE(stream.GetRows(&tracker_, &overflow_batch, &got_rows).ok());
  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

// Basic API test. No data should be going to disk.
TEST_F(SimpleNullStreamTest, Basic) {
  Init(BUFFER_POOL_LIMIT);
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
  // Each buffer can only hold 128 ints, so this spills quite often.
  int buffer_size = 128 * sizeof(int);
  Init(buffer_size);
  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);

  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
}

// Test with a few buffers and rows with multiple tuples.
TEST_F(MultiTupleStreamTest, MultiTupleManyBufferSpill) {
  int buffer_size = 128 * sizeof(int);
  Init(10 * buffer_size);

  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);
  TestValues<int>(100, int_desc_, false, true, buffer_size);

  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(100, string_desc_, false, true, buffer_size);

  TestIntValuesInterleaved(1, 1, true, buffer_size);
  TestIntValuesInterleaved(10, 5, true, buffer_size);
  TestIntValuesInterleaved(100, 15, true, buffer_size);
}

// Test that we can allocate a row in the stream and copy in multiple tuples then
// read it back from the stream.
TEST_F(MultiTupleStreamTest, MultiTupleAllocateRow) {
  // Use small buffers so it will be flushed to disk.
  int buffer_size = 4 * 1024;
  Init(2 * buffer_size);
  Status status = Status::OK();

  int num_batches = 1;
  int rows_added = 0;
  BufferedTupleStreamV2 stream(runtime_state_, *string_desc_, &client_, buffer_size);
  ASSERT_OK(stream.Init(-1, false));
  bool got_write_reservation;
  ASSERT_OK(stream.PrepareForWrite(&got_write_reservation));
  ASSERT_TRUE(got_write_reservation);

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
      uint8_t* fixed_data =
          stream.AllocateRow(fixed_size, varlen_size, &varlen_data, &status);
      ASSERT_TRUE(fixed_data != nullptr);
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
    bool got_read_reservation;
    ASSERT_OK(stream.PrepareForRead(delete_on_read, &got_read_reservation));
    ASSERT_TRUE(got_read_reservation);
    ReadValues(&stream, string_desc_, &results);
    VerifyResults<StringValue>(*string_desc_, results, rows_added, false);
  }

  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

// Test with rows with multiple nullable tuples.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleOneBufferSpill) {
  // Each buffer can only hold 128 ints, so this spills quite often.
  int buffer_size = 128 * sizeof(int);
  Init(buffer_size);
  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);
  TestValues<int>(1, int_desc_, true, true, buffer_size);
  TestValues<int>(10, int_desc_, true, true, buffer_size);

  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, true, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, true, true, buffer_size);
}

// Test with a few buffers.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleManyBufferSpill) {
  int buffer_size = 128 * sizeof(int);
  Init(10 * buffer_size);

  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);
  TestValues<int>(100, int_desc_, false, true, buffer_size);
  TestValues<int>(1, int_desc_, true, true, buffer_size);
  TestValues<int>(10, int_desc_, true, true, buffer_size);
  TestValues<int>(100, int_desc_, true, true, buffer_size);

  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(100, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, true, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, true, true, buffer_size);
  TestValues<StringValue>(100, string_desc_, true, true, buffer_size);

  TestIntValuesInterleaved(1, 1, true, buffer_size);
  TestIntValuesInterleaved(10, 5, true, buffer_size);
  TestIntValuesInterleaved(100, 15, true, buffer_size);
}

/// Test that ComputeRowSize handles nulls
TEST_F(MultiNullableTupleStreamTest, TestComputeRowSize) {
  Init(BUFFER_POOL_LIMIT);
  const vector<TupleDescriptor*>& tuple_descs = string_desc_->tuple_descriptors();
  // String in second tuple is stored externally.
  set<SlotId> external_slots;
  const SlotDescriptor* external_string_slot = tuple_descs[1]->slots()[0];
  external_slots.insert(external_string_slot->id());

  BufferedTupleStreamV2 stream(
      runtime_state_, *string_desc_, &client_, PAGE_LEN, external_slots);
  gscoped_ptr<TupleRow, FreeDeleter> row(
      reinterpret_cast<TupleRow*>(malloc(tuple_descs.size() * sizeof(Tuple*))));
  gscoped_ptr<Tuple, FreeDeleter> tuple0(
      reinterpret_cast<Tuple*>(malloc(tuple_descs[0]->byte_size())));
  gscoped_ptr<Tuple, FreeDeleter> tuple1(
      reinterpret_cast<Tuple*>(malloc(tuple_descs[1]->byte_size())));
  gscoped_ptr<Tuple, FreeDeleter> tuple2(
      reinterpret_cast<Tuple*>(malloc(tuple_descs[2]->byte_size())));
  memset(tuple0.get(), 0, tuple_descs[0]->byte_size());
  memset(tuple1.get(), 0, tuple_descs[1]->byte_size());
  memset(tuple2.get(), 0, tuple_descs[2]->byte_size());
  const int tuple_null_indicator_bytes = 1; // Need 1 bytes for 3 tuples.

  // All nullable tuples are NULL.
  row->SetTuple(0, tuple0.get());
  row->SetTuple(1, nullptr);
  row->SetTuple(2, nullptr);
  EXPECT_EQ(tuple_null_indicator_bytes + tuple_descs[0]->byte_size(),
      stream.ComputeRowSize(row.get()));

  // Tuples are initialized to empty and have no var-len data.
  row->SetTuple(1, tuple1.get());
  row->SetTuple(2, tuple2.get());
  EXPECT_EQ(tuple_null_indicator_bytes + string_desc_->GetRowSize(),
      stream.ComputeRowSize(row.get()));

  // Tuple 0 has some data.
  const SlotDescriptor* string_slot = tuple_descs[0]->slots()[0];
  StringValue* sv = tuple0->GetStringSlot(string_slot->tuple_offset());
  *sv = STRINGS[0];
  int64_t expected_len =
      tuple_null_indicator_bytes + string_desc_->GetRowSize() + sv->len;
  EXPECT_EQ(expected_len, stream.ComputeRowSize(row.get()));

  // Check that external slots aren't included in count.
  sv = tuple1->GetStringSlot(external_string_slot->tuple_offset());
  sv->ptr = reinterpret_cast<char*>(1234);
  sv->len = 1234;
  EXPECT_EQ(expected_len, stream.ComputeRowSize(row.get()));

  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

/// Test that deep copy works with arrays by copying into a BufferedTupleStream, freeing
/// the original rows, then reading back the rows and verifying the contents.
TEST_F(ArrayTupleStreamTest, TestArrayDeepCopy) {
  Status status;
  Init(BUFFER_POOL_LIMIT);
  const int NUM_ROWS = 4000;
  BufferedTupleStreamV2 stream(runtime_state_, *array_desc_, &client_, PAGE_LEN);
  const vector<TupleDescriptor*>& tuple_descs = array_desc_->tuple_descriptors();
  // Write out a predictable pattern of data by iterating over arrays of constants.
  int strings_index = 0; // we take the mod of this as index into STRINGS.
  int array_lens[] = {0, 1, 5, 10, 1000, 2, 49, 20};
  int num_array_lens = sizeof(array_lens) / sizeof(array_lens[0]);
  int array_len_index = 0;
  ASSERT_OK(stream.Init(-1, false));
  bool got_write_reservation;
  ASSERT_OK(stream.PrepareForWrite(&got_write_reservation));
  ASSERT_TRUE(got_write_reservation);

  for (int i = 0; i < NUM_ROWS; ++i) {
    const int tuple_null_indicator_bytes = 1; // Need 1 bytes for 2 tuples.
    int expected_row_size = tuple_null_indicator_bytes + tuple_descs[0]->byte_size()
        + tuple_descs[1]->byte_size();
    gscoped_ptr<TupleRow, FreeDeleter> row(
        reinterpret_cast<TupleRow*>(malloc(tuple_descs.size() * sizeof(Tuple*))));
    gscoped_ptr<Tuple, FreeDeleter> tuple0(
        reinterpret_cast<Tuple*>(malloc(tuple_descs[0]->byte_size())));
    gscoped_ptr<Tuple, FreeDeleter> tuple1(
        reinterpret_cast<Tuple*>(malloc(tuple_descs[1]->byte_size())));
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
    cv->ptr = nullptr;
    cv->num_tuples = 0;
    CollectionValueBuilder builder(
        cv, *item_desc, mem_pool_.get(), runtime_state_, array_len);
    Tuple* array_data;
    int num_rows;
    builder.GetFreeMemory(&array_data, &num_rows);
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
  bool got_read_reservation;
  ASSERT_OK(stream.PrepareForRead(false, &got_read_reservation));
  ASSERT_TRUE(got_read_reservation);
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
      ASSERT_TRUE(tuple0 != nullptr);
      ASSERT_TRUE(tuple1 != nullptr);
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
  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

/// Test that ComputeRowSize handles nulls
TEST_F(ArrayTupleStreamTest, TestComputeRowSize) {
  Init(BUFFER_POOL_LIMIT);
  const vector<TupleDescriptor*>& tuple_descs = array_desc_->tuple_descriptors();
  set<SlotId> external_slots;
  // Second array slot in first tuple is stored externally.
  const SlotDescriptor* external_array_slot = tuple_descs[0]->slots()[1];
  external_slots.insert(external_array_slot->id());

  BufferedTupleStreamV2 stream(
      runtime_state_, *array_desc_, &client_, PAGE_LEN, external_slots);
  gscoped_ptr<TupleRow, FreeDeleter> row(
      reinterpret_cast<TupleRow*>(malloc(tuple_descs.size() * sizeof(Tuple*))));
  gscoped_ptr<Tuple, FreeDeleter> tuple0(
      reinterpret_cast<Tuple*>(malloc(tuple_descs[0]->byte_size())));
  gscoped_ptr<Tuple, FreeDeleter> tuple1(
      reinterpret_cast<Tuple*>(malloc(tuple_descs[1]->byte_size())));
  memset(tuple0.get(), 0, tuple_descs[0]->byte_size());
  memset(tuple1.get(), 0, tuple_descs[1]->byte_size());

  const int tuple_null_indicator_bytes = 1; // Need 1 bytes for 3 tuples.

  // All tuples are NULL - only need null indicators.
  row->SetTuple(0, nullptr);
  row->SetTuple(1, nullptr);
  EXPECT_EQ(tuple_null_indicator_bytes, stream.ComputeRowSize(row.get()));

  // Tuples are initialized to empty and have no var-len data.
  row->SetTuple(0, tuple0.get());
  row->SetTuple(1, tuple1.get());
  EXPECT_EQ(tuple_null_indicator_bytes + array_desc_->GetRowSize(),
      stream.ComputeRowSize(row.get()));

  // Tuple 0 has an array.
  int expected_row_size = tuple_null_indicator_bytes + array_desc_->GetRowSize();
  const SlotDescriptor* array_slot = tuple_descs[0]->slots()[0];
  const TupleDescriptor* item_desc = array_slot->collection_item_descriptor();
  int array_len = 128;
  CollectionValue* cv = tuple0->GetCollectionSlot(array_slot->tuple_offset());
  CollectionValueBuilder builder(
      cv, *item_desc, mem_pool_.get(), runtime_state_, array_len);
  Tuple* array_data;
  int num_rows;
  builder.GetFreeMemory(&array_data, &num_rows);
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
  EXPECT_EQ(tuple_null_indicator_bytes + array_desc_->GetRowSize(),
      stream.ComputeRowSize(row.get()));

  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
