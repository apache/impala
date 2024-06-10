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
#include <boost/thread/thread.hpp>

#include <limits> // for std::numeric_limits<int>::max()
#include <set>
#include <string>

#include "codegen/llvm-codegen.h"
#include "gutil/gscoped_ptr.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/query-state.h"
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
#include "util/error-util.h"
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

constexpr int ErrorMsg::MAX_ERROR_MESSAGE_LEN;

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

    // Construct descriptors for big rows with and without nullable tuples.
    // Each tuple contains 8 slots of TYPE_INT and a single byte for null indicator.
    DescriptorTblBuilder big_row_builder(test_env_->exec_env()->frontend(), &pool_);
    tuple_ids.clear();
    nullable_tuples.clear();
    vector<bool> non_nullable_tuples;
    const int num_tuples = BIG_ROW_BYTES / (8 * sizeof(int) + 1);
    for (int tuple_idx = 0; tuple_idx < num_tuples; ++tuple_idx) {
      big_row_builder.DeclareTuple() << TYPE_INT << TYPE_INT << TYPE_INT << TYPE_INT
                                     << TYPE_INT << TYPE_INT << TYPE_INT << TYPE_INT;
      tuple_ids.push_back(static_cast<TTupleId>(tuple_idx));
      nullable_tuples.push_back(true);
      non_nullable_tuples.push_back(false);
    }
    big_row_desc_ = pool_.Add(
        new RowDescriptor(*big_row_builder.Build(), tuple_ids, non_nullable_tuples));
    ASSERT_FALSE(big_row_desc_->IsAnyTupleNullable());
    nullable_big_row_desc_ = pool_.Add(
        new RowDescriptor(*big_row_builder.Build(), tuple_ids, nullable_tuples));
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

    RuntimeProfile* client_profile = RuntimeProfile::Create(&pool_, "client");
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
      const RowDescriptor* row_desc, int offset, int num_rows, bool gen_null) {
    RowBatch* batch = pool_.Add(new RowBatch(row_desc, num_rows, &tracker_));
    int num_tuples = row_desc->tuple_descriptors().size();

    int idx = offset * CountSlotsPerRow(*row_desc);
    for (int row_idx = 0; row_idx < num_rows; ++row_idx) {
      TupleRow* row = batch->GetRow(row_idx);
      for (int tuple_idx = 0; tuple_idx < num_tuples; ++tuple_idx) {
        TupleDescriptor* tuple_desc = row_desc->tuple_descriptors()[tuple_idx];
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
    return CreateBatch(int_desc_, offset, num_rows, gen_null);
  }

  virtual RowBatch* CreateStringBatch(int offset, int num_rows, bool gen_null) {
    return CreateBatch(string_desc_, offset, num_rows, gen_null);
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
      if (!sv.IsSmall()) {
        uint8_t* copy = mem_pool_->Allocate(sv.Len());
        memcpy(copy, sv.Ptr(), sv.Len());
        sv.SetPtr(reinterpret_cast<char*>(copy));
      }
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

  /// Read values from 'stream' into 'results' using the embedded read iterator. 'stream'
  /// must have been prepared for reading.
  template <typename T>
  void ReadValues(BufferedTupleStream* stream, RowDescriptor* desc, vector<T>* results,
      int num_batches = -1) {
    return ReadValues(stream, nullptr, desc, results, num_batches);
  }

  /// Read values from 'stream' into 'results'. If 'read_it' is non-NULL, reads via that
  /// iterator. Otherwise use the embedded read iterator, in which case 'stream' must have
  /// been prepared for reading.
  template <typename T>
  void ReadValues(BufferedTupleStream* stream,
      BufferedTupleStream::ReadIterator* read_it, RowDescriptor* desc, vector<T>* results,
      int num_batches = -1) {
    bool eos = false;
    RowBatch batch(desc, BATCH_SIZE, &tracker_);
    int batches_read = 0;
    do {
      batch.Reset();
      if (read_it != nullptr) {
        EXPECT_OK(stream->GetNext(read_it, &batch, &eos));
      } else {
        EXPECT_OK(stream->GetNext(&batch, &eos));
      }
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
      int64_t default_page_len = PAGE_LEN, int64_t max_page_len = -1,
      int num_rows = BATCH_SIZE) {
    if (max_page_len == -1) max_page_len = default_page_len;

    BufferedTupleStream stream(
        runtime_state_, desc, &client_, default_page_len, max_page_len);
    ASSERT_OK(stream.Init("SimpleTupleStreamTest", true));
    bool got_write_reservation;
    ASSERT_OK(stream.PrepareForWrite(&got_write_reservation));
    ASSERT_TRUE(got_write_reservation);

    if (unpin_stream) {
      ASSERT_OK(stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
    }
    // Add rows to the stream
    int offset = 0;
    for (int i = 0; i < num_batches; ++i) {
      RowBatch* batch = nullptr;

      Status status;
      ASSERT_TRUE(sizeof(T) == sizeof(int) || sizeof(T) == sizeof(StringValue));
      batch = CreateBatch(desc, offset, num_rows, gen_null);
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
    BufferedTupleStream stream(runtime_state_, int_desc_, &client_, page_len, page_len);
    ASSERT_OK(stream.Init("SimpleTupleStreamTest", true));
    bool got_reservation;
    ASSERT_OK(stream.PrepareForReadWrite(true, &got_reservation));
    ASSERT_TRUE(got_reservation);
    if (unpin_stream) {
      ASSERT_OK(stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
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

  void TestAttachMemory(bool pinned_stream, bool attach_on_read);

  void TestFlushResourcesReadWrite(bool pinned_stream, bool attach_on_read);

  /// Helper for TestFlushResourcesReadWrite() to write and read back rows from
  /// *stream. 'append_batch_size' is the number of rows to append at a time before
  /// reading them back. *num_buffers_attached tracks the number of buffers attached
  /// to the output batch.
  void AppendToReadWriteStream(int64_t append_batch_size, int64_t buffer_size,
      int* num_buffers_attached, BufferedTupleStream* stream);

  // Helper for AppendToReadWriteStream() to verify 'out_batch' contents. The value of
  // row i of 'out_batch' is expected to be the same as the row at index
  // (i + start_index) % out_batch->num_rows() of 'in_batch'.
  void VerifyReadWriteBatch(RowBatch* in_batch, RowBatch* out_batch, int64_t start_index);

  // Helper to writes 'row' comprised of only string slots to 'data'. The expected
  // length of the data written is 'expected_len'.
  void WriteStringRow(const RowDescriptor* row_desc, TupleRow* row, int64_t fixed_size,
      int64_t varlen_size, uint8_t* data);

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

  static const int64_t BIG_ROW_BYTES = 16 * 1024;
  RowDescriptor* big_row_desc_;
  RowDescriptor* nullable_big_row_desc_;
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
    string_array_type.children.push_back(ColumnType(TYPE_STRING));

    ColumnType int_array_type;
    int_array_type.type = TYPE_ARRAY;
    int_array_type.children.push_back(ColumnType(TYPE_STRING));

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

/// Test internal stream state under certain corner cases.
class StreamStateTest : public SimpleTupleStreamTest {
 protected:
  // Test that UnpinStream defers advancing the read page when all rows from the read
  // page are attached to a returned RowBatch but got not enough reservation.
  void TestDeferAdvancingReadPage();

  // Test unpinning a read-write stream when the read page has been fully exhausted but
  // its buffer is not attached yet to the output row batch.
  void TestUnpinAfterFullStreamRead(
      bool read_write, bool attach_on_read, bool refill_before_unpin);

  // Fill up the stream by repeatedly inserting write_batch into the stream until it is
  // full. Return number of rows successfully inserted into the stream.
  // Stream must be in pinned mode.
  Status FillUpStream(
      BufferedTupleStream* stream, RowBatch* write_batch, int64_t& num_inserted);

  // Read out the stream until eos is reached. Return number of rows successfully read.
  Status ReadOutStream(
      BufferedTupleStream* stream, RowBatch* read_batch, int64_t& num_read);

  // Verify that page count, pinned bytes, and unpinned bytes of the stream match the
  // expectation.
  void VerifyStreamState(BufferedTupleStream* stream, int num_page, int num_pinned_page,
      int num_unpinned_page, int buffer_size);

  // Test that stream's debug string is capped only for the first
  // BufferedTupleStream::MAX_PAGE_ITER_DEBUG.
  void TestShortDebugString();
};

// Basic API test. No data should be going to disk.
TEST_F(SimpleTupleStreamTest, Basic) {
  Init(numeric_limits<int64_t>::max());
  TestValues<int>(0, int_desc_, false, true);
  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);
  TestValues<int>(100, int_desc_, false, true);
  TestValues<int>(0, int_desc_, false, false);
  TestValues<int>(1, int_desc_, false, false);
  TestValues<int>(10, int_desc_, false, false);
  TestValues<int>(100, int_desc_, false, false);

  TestValues<StringValue>(0, string_desc_, false, true);
  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
  TestValues<StringValue>(100, string_desc_, false, true);
  TestValues<StringValue>(0, string_desc_, false, false);
  TestValues<StringValue>(1, string_desc_, false, false);
  TestValues<StringValue>(10, string_desc_, false, false);
  TestValues<StringValue>(100, string_desc_, false, false);

  TestIntValuesInterleaved(0, 1, true);
  TestIntValuesInterleaved(1, 1, true);
  TestIntValuesInterleaved(10, 5, true);
  TestIntValuesInterleaved(100, 15, true);
  TestIntValuesInterleaved(0, 1, false);
  TestIntValuesInterleaved(1, 1, false);
  TestIntValuesInterleaved(10, 5, false);
  TestIntValuesInterleaved(100, 15, false);
}

// Test with only 1 buffer.
TEST_F(SimpleTupleStreamTest, OneBufferSpill) {
  // Each buffer can only hold 128 ints, so this spills quite often.
  int buffer_size = 128 * sizeof(int);
  Init(buffer_size);
  TestValues<int>(0, int_desc_, false, true, buffer_size);
  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);

  TestValues<StringValue>(0, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
}

// Test with a few buffers.
TEST_F(SimpleTupleStreamTest, ManyBufferSpill) {
  int buffer_size = 128 * sizeof(int);
  Init(10 * buffer_size);

  TestValues<int>(0, int_desc_, false, true, buffer_size);
  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);
  TestValues<int>(100, int_desc_, false, true, buffer_size);
  TestValues<StringValue>(0, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(100, string_desc_, false, true, buffer_size);

  TestIntValuesInterleaved(0, 1, true, buffer_size);
  TestIntValuesInterleaved(1, 1, true, buffer_size);
  TestIntValuesInterleaved(10, 5, true, buffer_size);
  TestIntValuesInterleaved(100, 15, true, buffer_size);
}

void SimpleTupleStreamTest::TestUnpinPin(bool varlen_data, bool read_write) {
  int buffer_size = 128 * sizeof(int);
  int num_buffers = 10;
  Init(num_buffers * buffer_size);
  RowDescriptor* row_desc = varlen_data ? string_desc_ : int_desc_;

  BufferedTupleStream stream(
      runtime_state_, row_desc, &client_, buffer_size, buffer_size);
  ASSERT_OK(stream.Init("SimpleTupleStreamTest::TestUnpinPin", true));
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
      ASSERT_OK(stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
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

  ASSERT_OK(stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));

  bool pinned = false;
  ASSERT_OK(stream.PinStream(&pinned));
  ASSERT_TRUE(pinned);

  // Read and verify result a few times. We should be able to reread the stream if
  // we don't use attach on read mode.
  int read_iters = 3;
  for (int i = 0; i < read_iters; ++i) {
    bool attach_on_read = i == read_iters - 1;
    if (i > 0 || !read_write) {
      bool got_read_reservation;
      ASSERT_OK(stream.PrepareForRead(attach_on_read, &got_read_reservation));
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

  // After attach_on_read, all buffers should have been attached to the output batches
  // on previous GetNext() calls.
  ASSERT_EQ(0, stream.BytesPinned(false));

  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);

  ASSERT_EQ(0, stream.BytesPinned(false));
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

  BufferedTupleStream stream(
      runtime_state_, int_desc_, &client_, buffer_size, buffer_size);
  ASSERT_OK(stream.Init("SimpleTupleStreamTest::TestTransferMemory", pin_stream));
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

  // Construct a stream with 4 pages.
  const int total_num_pages = 4;
  while (stream.byte_size() < total_num_pages * buffer_size) {
    Status status;
    for (int i = 0; i < batch->num_rows(); ++i) {
      bool ret = stream.AddRow(batch->GetRow(i), &status);
      EXPECT_TRUE(ret);
      ASSERT_OK(status);
    }
  }

  batch->Reset();

  if (read_write) {
    // Read back batch so that we have a read buffer in memory.
    bool eos;
    ASSERT_OK(stream.GetNext(batch, &eos));
    EXPECT_FALSE(eos);
  }
  stream.Close(batch, RowBatch::FlushMode::FLUSH_RESOURCES);
  if (pin_stream) {
    EXPECT_EQ(total_num_pages, batch->num_buffers());
  } else if (read_write) {
    // Read and write buffer should be attached.
    EXPECT_EQ(2, batch->num_buffers());
  } else {
    // Read buffer should be attached.
    EXPECT_EQ(1, batch->num_buffers());
  }
  EXPECT_TRUE(batch->AtCapacity()); // Flush resources flag should have been set.
  batch->Reset();
  EXPECT_EQ(0, batch->num_buffers());
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

/// Test iteration over a stream with and without attaching memory.
void SimpleTupleStreamTest::TestAttachMemory(bool pin_stream, bool attach_on_read) {
  // Use smaller buffers so that the explicit FLUSH_RESOURCES flag is required to
  // make the batch at capacity.
  int buffer_size = 4 * 1024;
  Init(100 * buffer_size);

  BufferedTupleStream stream(
      runtime_state_, int_desc_, &client_, buffer_size, buffer_size);
  ASSERT_OK(stream.Init("SimpleTupleStreamTest::TestAttachMemory", pin_stream));
  bool got_write_reservation;
  ASSERT_OK(stream.PrepareForWrite(&got_write_reservation));
  ASSERT_TRUE(got_write_reservation);
  RowBatch* in_batch = CreateIntBatch(0, 1024, false);

  // Construct a stream with 4 pages.
  const int total_num_pages = 4;
  while (stream.byte_size() < total_num_pages * buffer_size) {
    Status status;
    for (int i = 0; i < in_batch->num_rows(); ++i) {
      bool ret = stream.AddRow(in_batch->GetRow(i), &status);
      EXPECT_TRUE(ret);
      ASSERT_OK(status);
    }
  }

  RowBatch* out_batch = pool_.Add(new RowBatch(int_desc_, 100, &tracker_));
  int num_buffers_attached = 0;
  int num_flushes = 0;
  int64_t num_rows_returned = 0;
  bool got_read_reservation;
  ASSERT_OK(stream.PrepareForRead(attach_on_read, &got_read_reservation));
  ASSERT_TRUE(got_read_reservation);
  bool eos;
  do {
    ASSERT_EQ(0, out_batch->num_buffers());
    ASSERT_OK(stream.GetNext(out_batch, &eos));
    EXPECT_LE(out_batch->num_buffers(), 1) << "Should only attach one buffer at a time";
    if (out_batch->num_buffers() > 0) {
      EXPECT_TRUE(out_batch->AtCapacity()) << "Flush resources flag should have been set";
    }
    num_buffers_attached += out_batch->num_buffers();
    for (int i = 0; i < out_batch->num_rows(); ++i) {
      int slot_offset = int_desc_->tuple_descriptors()[0]->slots()[0]->tuple_offset();
      TupleRow* in_row = in_batch->GetRow(num_rows_returned % in_batch->num_rows());
      EXPECT_EQ(*in_row->GetTuple(0)->GetIntSlot(slot_offset),
          *out_batch->GetRow(i)->GetTuple(0)->GetIntSlot(slot_offset));
      ++num_rows_returned;
    }
    num_flushes += out_batch->flush_mode() == RowBatch::FlushMode::FLUSH_RESOURCES;
    out_batch->Reset();
  } while (!eos);

  if (attach_on_read) {
    EXPECT_EQ(4, num_buffers_attached) << "All buffers attached during iteration.";
  } else {
    EXPECT_EQ(0, num_buffers_attached) << "No buffers attached during iteration.";
  }
  if (attach_on_read || !pin_stream) {
    EXPECT_EQ(4, num_flushes);
  }
  out_batch->Reset();
  stream.Close(out_batch, RowBatch::FlushMode::FLUSH_RESOURCES);
  if (attach_on_read) {
    EXPECT_EQ(0, out_batch->num_buffers());
  } else if (pin_stream) {
    // All buffers should be attached.
    EXPECT_EQ(4, out_batch->num_buffers());
  } else {
    // Buffer from last pinned page should be attached.
    EXPECT_EQ(1, out_batch->num_buffers());
  }
  in_batch->Reset();
  out_batch->Reset();
}

TEST_F(SimpleTupleStreamTest, TestAttachMemoryPinned) {
  TestAttachMemory(true, true);
}

TEST_F(SimpleTupleStreamTest, TestNoAttachMemoryPinned) {
  TestAttachMemory(true, false);
}

TEST_F(SimpleTupleStreamTest, TestAttachMemoryUnpinned) {
  TestAttachMemory(false, true);
}

TEST_F(SimpleTupleStreamTest, TestNoAttachMemoryUnpinned) {
  TestAttachMemory(false, false);
}

// Test for advancing the read/write page with resource flushing.
void SimpleTupleStreamTest::TestFlushResourcesReadWrite(
    bool pin_stream, bool attach_on_read) {
  // Use smaller buffers so that the explicit FLUSH_RESOURCES flag is required to
  // make the batch at capacity.
  const int BUFFER_SIZE = 512;
  const int BATCH_SIZE = 100;
  // For unpinned streams, we should be able to iterate with only two buffers.
  const int MAX_PINNED_PAGES = pin_stream ? 1000 : 2;
  Init(MAX_PINNED_PAGES * BUFFER_SIZE);

  BufferedTupleStream stream(
      runtime_state_, int_desc_, &client_, BUFFER_SIZE, BUFFER_SIZE);
  ASSERT_OK(
      stream.Init("SimpleTupleStreamTest::TestFlushResourcesReadWrite", pin_stream));
  bool got_reservation;
  ASSERT_OK(stream.PrepareForReadWrite(attach_on_read, &got_reservation));
  ASSERT_TRUE(got_reservation);
  int num_buffers_attached = 0;
  /// Read over the page in different increments.
  for (int append_batch_size : {1, 10, 100, 1000}) {
    AppendToReadWriteStream(
        append_batch_size, BUFFER_SIZE, &num_buffers_attached, &stream);
  }

  if (attach_on_read) {
    EXPECT_EQ(stream.byte_size() / BUFFER_SIZE - 1, num_buffers_attached)
        << "All buffers except the current write page should have been attached";
  } else {
    EXPECT_EQ(0, num_buffers_attached);
  }

  RowBatch* final_out_batch = pool_.Add(new RowBatch(int_desc_, BATCH_SIZE, &tracker_));
  stream.Close(final_out_batch, RowBatch::FlushMode::FLUSH_RESOURCES);
  final_out_batch->Reset();
}

void SimpleTupleStreamTest::AppendToReadWriteStream(int64_t append_batch_size,
    int64_t buffer_size, int* num_buffers_attached, BufferedTupleStream* stream) {
  RowBatch* in_batch = CreateIntBatch(0, BATCH_SIZE, false);

  /// Accumulate row batches until we see a flush. The contents of the batches should
  /// remain valid until reset or delete trailing batches.
  vector<unique_ptr<RowBatch>> out_batches;
  // The start row index of each batch in 'out_batches'.
  vector<int64_t> out_batch_start_indices;
  // Iterate over at least 10 pages.
  int64_t start_byte_size = stream->byte_size();
  while (stream->byte_size() - start_byte_size < 10 * buffer_size) {
    Status status;
    for (int i = 0; i < append_batch_size; ++i) {
      bool ret = stream->AddRow(
          in_batch->GetRow(stream->num_rows() % in_batch->num_rows()), &status);
      EXPECT_TRUE(ret);
      ASSERT_OK(status);
    }
    int64_t rows_read = 0;
    bool eos;
    while (rows_read < append_batch_size) {
      out_batches.emplace_back(new RowBatch(int_desc_, BATCH_SIZE, &tracker_));
      out_batch_start_indices.push_back(stream->rows_returned());
      ASSERT_OK(stream->GetNext(out_batches.back().get(), &eos));
      // Verify the contents of all valid batches to make sure that they haven't become
      // invalid.
      LOG(INFO) << "Verifying " << out_batches.size() << " batches";
      for (int i = 0; i < out_batches.size(); ++i) {
        VerifyReadWriteBatch(in_batch, out_batches[i].get(), out_batch_start_indices[i]);
      }
      *num_buffers_attached += out_batches.back()->num_buffers();
      rows_read += out_batches.back()->num_rows();
      EXPECT_EQ(rows_read == append_batch_size, eos);
      if (out_batches.back().get()->flush_mode()
          == RowBatch::FlushMode::FLUSH_RESOURCES) {
        out_batches.clear();
        out_batch_start_indices.clear();
      }
    }
    EXPECT_EQ(append_batch_size, rows_read);
    EXPECT_EQ(true, eos);
  }
  in_batch->Reset();
}

void SimpleTupleStreamTest::VerifyReadWriteBatch(
    RowBatch* in_batch, RowBatch* out_batch, int64_t start_index) {
  int slot_offset = int_desc_->tuple_descriptors()[0]->slots()[0]->tuple_offset();
  int64_t row_index = start_index;
  for (int i = 0; i < out_batch->num_rows(); ++i) {
    TupleRow* in_row = in_batch->GetRow(row_index++ % in_batch->num_rows());
    EXPECT_EQ(*in_row->GetTuple(0)->GetIntSlot(slot_offset),
        *out_batch->GetRow(i)->GetTuple(0)->GetIntSlot(slot_offset));
  }
}

TEST_F(SimpleTupleStreamTest, TestFlushResourcesReadWritePinnedAttach) {
  TestFlushResourcesReadWrite(true, true);
}

TEST_F(SimpleTupleStreamTest, TestFlushResourcesReadWritePinnedNoAttach) {
  TestFlushResourcesReadWrite(true, false);
}

TEST_F(SimpleTupleStreamTest, TestFlushResourcesReadWriteUnpinnedAttach) {
  TestFlushResourcesReadWrite(false, true);
}

TEST_F(SimpleTupleStreamTest, TestFlushResourcesReadWriteUnpinnedNoAttach) {
  TestFlushResourcesReadWrite(false, false);
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

  BufferedTupleStream stream(
      runtime_state_, string_desc_, &client_, buffer_size, buffer_size, external_slots);
  ASSERT_OK(stream.Init("SimpleTupleStreamTest::StringsOutsideStream", false));
  bool got_reservation;
  ASSERT_OK(stream.PrepareForWrite(&got_reservation));
  ASSERT_TRUE(got_reservation);

  for (int i = 0; i < num_batches; ++i) {
    RowBatch* batch = CreateStringBatch(rows_added, BATCH_SIZE, false);
    for (int j = 0; j < batch->num_rows(); ++j) {
      int fixed_size = tuple_desc.byte_size();
      // Copy fixed portion in, but leave it pointing to row batch's varlen data.
      uint8_t* tuple_data = stream.AddRowCustomBegin(fixed_size, &status);
      ASSERT_TRUE(tuple_data != nullptr);
      ASSERT_TRUE(status.ok());
      memcpy(tuple_data, batch->GetRow(j)->GetTuple(0), fixed_size);
      stream.AddRowCustomEnd(fixed_size);
    }
    rows_added += batch->num_rows();
  }

  DCHECK_EQ(rows_added, stream.num_rows());

  for (int attach_on_read = 0; attach_on_read <= 1; ++attach_on_read) {
    // Keep stream in memory and test we can read ok.
    vector<StringValue> results;
    bool got_read_reservation;
    ASSERT_OK(stream.PrepareForRead(attach_on_read, &got_read_reservation));
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
  const int64_t MAX_BUFFERS = 10;
  Init(MAX_BUFFERS * BIG_ROW_BYTES);

  // Test writing this row into the stream and then reading it back.
  // Make sure to exercise the case where the row is larger than the default page.
  // If the stream is pinned, we can only fit MAX_BUFFERS - 1 rows (since we always
  // advance to the next page). In the unpinned case we should be able to write
  // arbitrarily many rows.
  TestValues<int>(1, big_row_desc_, false, false, BIG_ROW_BYTES, BIG_ROW_BYTES, 1);
  TestValues<int>(
      MAX_BUFFERS - 1, big_row_desc_, false, false, BIG_ROW_BYTES, BIG_ROW_BYTES, 1);
  TestValues<int>(1, big_row_desc_, false, false, BIG_ROW_BYTES / 4, BIG_ROW_BYTES, 1);
  TestValues<int>(
      MAX_BUFFERS - 1, big_row_desc_, false, false, BIG_ROW_BYTES / 4, BIG_ROW_BYTES, 1);
  TestValues<int>(1, big_row_desc_, false, true, BIG_ROW_BYTES, BIG_ROW_BYTES, 1);
  TestValues<int>(
      MAX_BUFFERS - 1, big_row_desc_, false, true, BIG_ROW_BYTES, BIG_ROW_BYTES, 1);
  TestValues<int>(
      5 * MAX_BUFFERS, big_row_desc_, false, true, BIG_ROW_BYTES, BIG_ROW_BYTES, 1);
  TestValues<int>(1, big_row_desc_, false, true, BIG_ROW_BYTES / 4, BIG_ROW_BYTES, 1);
  TestValues<int>(
      MAX_BUFFERS - 1, big_row_desc_, false, true, BIG_ROW_BYTES / 4, BIG_ROW_BYTES, 1);
  TestValues<int>(
      5 * MAX_BUFFERS, big_row_desc_, false, true, BIG_ROW_BYTES / 4, BIG_ROW_BYTES, 1);

  // Test the case where it fits in an in-between page size.
  TestValues<int>(MAX_BUFFERS - 1, big_row_desc_, false, false, BIG_ROW_BYTES / 4,
      BIG_ROW_BYTES * 2, 1);
  TestValues<int>(MAX_BUFFERS - 1, big_row_desc_, false, true, BIG_ROW_BYTES / 4,
      BIG_ROW_BYTES * 2, 1);

  // Construct a big row with nullable tuples. This requires extra space for null
  // indicators in the stream so adding the row will fail.
  ASSERT_TRUE(nullable_big_row_desc_->IsAnyTupleNullable());
  BufferedTupleStream nullable_stream(
      runtime_state_, nullable_big_row_desc_, &client_, BIG_ROW_BYTES, BIG_ROW_BYTES);
  ASSERT_OK(nullable_stream.Init("SimpleTupleStreamTest::BigRow", true));
  bool got_reservation;
  ASSERT_OK(nullable_stream.PrepareForWrite(&got_reservation));

  // With null tuples, a row can fit in the stream.
  RowBatch* batch = CreateBatch(nullable_big_row_desc_, 0, 1, true);
  Status status;
  EXPECT_TRUE(nullable_stream.AddRow(batch->GetRow(0), &status));
  // With the additional null indicator, we can't fit all the tuples of a row into
  // the stream.
  batch = CreateBatch(nullable_big_row_desc_, 0, 1, false);
  EXPECT_FALSE(nullable_stream.AddRow(batch->GetRow(0), &status));
  EXPECT_EQ(TErrorCode::MAX_ROW_SIZE, status.code());
  nullable_stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

// Test the memory use for large rows.
TEST_F(SimpleTupleStreamTest, BigRowMemoryUse) {
  const int64_t MAX_BUFFERS = 10;
  const int64_t DEFAULT_PAGE_LEN = BIG_ROW_BYTES / 4;
  Init(MAX_BUFFERS * BIG_ROW_BYTES);
  Status status;
  BufferedTupleStream stream(
      runtime_state_, big_row_desc_, &client_, DEFAULT_PAGE_LEN, BIG_ROW_BYTES * 2);
  ASSERT_OK(stream.Init("SimpleTupleStreamTest::BigRowMemoryUse", true));
  RowBatch* batch;
  bool got_reservation;
  ASSERT_OK(stream.PrepareForWrite(&got_reservation));
  ASSERT_TRUE(got_reservation);
  // We should be able to append MAX_BUFFERS without problem.
  for (int i = 0; i < MAX_BUFFERS; ++i) {
    batch = CreateBatch(big_row_desc_, i, 1, false);
    bool success = stream.AddRow(batch->GetRow(0), &status);
    ASSERT_TRUE(success);
    // We should have one large page per row.
    EXPECT_EQ(BIG_ROW_BYTES * (i + 1), client_.GetUsedReservation())
        << i << ": " << client_.DebugString();
  }

  // We can't fit another row in memory - need to unpin to make progress.
  batch = CreateBatch(big_row_desc_, MAX_BUFFERS, 1, false);
  bool success = stream.AddRow(batch->GetRow(0), &status);
  ASSERT_FALSE(success);
  ASSERT_OK(status);
  ASSERT_OK(stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
  success = stream.AddRow(batch->GetRow(0), &status);
  ASSERT_TRUE(success);
  // Read all the rows back and verify.
  ASSERT_OK(stream.PrepareForRead(false, &got_reservation));
  ASSERT_TRUE(got_reservation);
  vector<int> results;
  ReadValues(&stream, big_row_desc_, &results);
  VerifyResults<int>(*big_row_desc_, results, MAX_BUFFERS + 1, false);
  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

// Test rows greater than the default page size. Also exercise the read/write
// mode with large pages.
TEST_F(SimpleTupleStreamTest, BigStringReadWrite) {
  const int64_t MAX_BUFFERS = 10;
  const int64_t DEFAULT_PAGE_LEN = BIG_ROW_BYTES / 4;
  Init(MAX_BUFFERS * BIG_ROW_BYTES);
  Status status;
  BufferedTupleStream stream(
      runtime_state_, string_desc_, &client_, DEFAULT_PAGE_LEN, BIG_ROW_BYTES * 2);
  ASSERT_OK(stream.Init("SimpleTupleStreamTest::BigStringReadWrite", true));
  RowBatch write_batch(string_desc_, 1024, &tracker_);
  RowBatch read_batch(string_desc_, 1024, &tracker_);
  bool got_reservation;
  ASSERT_OK(stream.PrepareForReadWrite(false, &got_reservation));
  ASSERT_TRUE(got_reservation);
  TupleRow* write_row = write_batch.GetRow(0);
  TupleDescriptor* tuple_desc = string_desc_->tuple_descriptors()[0];
  vector<uint8_t> tuple_mem(tuple_desc->byte_size());
  Tuple* write_tuple = reinterpret_cast<Tuple*>(tuple_mem.data());
  write_row->SetTuple(0, write_tuple);
  StringValue* write_str =
      write_tuple->GetStringSlot(tuple_desc->slots()[0]->tuple_offset());
  // Make the string large enough to fill a page.
  const int64_t string_len = BIG_ROW_BYTES - tuple_desc->byte_size();
  vector<char> data(string_len);
  write_str->Assign(data.data(), string_len);

  // We should be able to append MAX_BUFFERS without problem.
  for (int i = 0; i < MAX_BUFFERS; ++i) {
    // Fill the string with the value i.
    memset(write_str->Ptr(), i, write_str->Len());
    bool success = stream.AddRow(write_row, &status);
    ASSERT_TRUE(success);
    // We should have one large page per row, plus a default-size read/write page, plus
    // we waste the first default-size page in the stream by leaving it empty.
    EXPECT_EQ(BIG_ROW_BYTES * (i + 1), client_.GetUsedReservation())
        << i << ": " << client_.DebugString() << "\n"
        << stream.DebugString();

    // Read back the rows as we write them to test read/write mode.
    read_batch.Reset();
    bool eos;
    ASSERT_OK(stream.GetNext(&read_batch, &eos));
    EXPECT_EQ(1, read_batch.num_rows());
    EXPECT_TRUE(eos) << i << " " << stream.DebugString();
    Tuple* tuple = read_batch.GetRow(0)->GetTuple(0);
    StringValue* str = tuple->GetStringSlot(tuple_desc->slots()[0]->tuple_offset());
    EXPECT_EQ(string_len, str->Len());
    for (int j = 0; j < string_len; ++j) {
      EXPECT_EQ(i, str->Ptr()[j]) << j;
    }
  }

  // We can't fit another row in memory - need to unpin to make progress.
  memset(write_str->Ptr(), MAX_BUFFERS, write_str->Len());
  bool success = stream.AddRow(write_row, &status);
  ASSERT_FALSE(success);
  ASSERT_OK(status);
  ASSERT_OK(stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
  success = stream.AddRow(write_row, &status);
  ASSERT_TRUE(success);

  // Read all the rows back and verify.
  ASSERT_OK(stream.PrepareForRead(false, &got_reservation));
  ASSERT_TRUE(got_reservation);
  for (int i = 0; i < MAX_BUFFERS + 1; ++i) {
    read_batch.Reset();
    bool eos;
    ASSERT_OK(stream.GetNext(&read_batch, &eos));
    EXPECT_EQ(1, read_batch.num_rows());
    EXPECT_EQ(eos, i == MAX_BUFFERS) << i;
    Tuple* tuple = read_batch.GetRow(0)->GetTuple(0);
    StringValue* str = tuple->GetStringSlot(tuple_desc->slots()[0]->tuple_offset());
    EXPECT_EQ(string_len, str->Len());
    for (int j = 0; j < string_len; ++j) {
      ASSERT_EQ(i, str->Ptr()[j]) << j;
    }
  }
  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

// Test that UnpinStream advances the read page if all rows from the read page are
// attached to a returned RowBatch.
TEST_F(SimpleTupleStreamTest, UnpinReadPage) {
  int num_rows = 1024;
  int buffer_size = 4 * 1024;
  Init(100 * buffer_size);

  bool eos;
  bool got_reservation;
  Status status;
  RowBatch* write_batch = CreateIntBatch(0, num_rows, false);

  {
    // Test unpinning a stream when the read page has been attached to the output batch.
    BufferedTupleStream stream(
        runtime_state_, int_desc_, &client_, buffer_size, buffer_size);
    ASSERT_OK(stream.Init("SimpleTupleStreamTest::UnpinReadPage", true));
    ASSERT_OK(stream.PrepareForReadWrite(true, &got_reservation));
    ASSERT_TRUE(got_reservation);

    // Add rows to stream.
    for (int i = 0; i < write_batch->num_rows(); ++i) {
      EXPECT_TRUE(stream.AddRow(write_batch->GetRow(i), &status));
      ASSERT_OK(status);
    }

    // Read until the read page is attached to the output.
    RowBatch read_batch(int_desc_, num_rows, &tracker_);
    ASSERT_OK(stream.GetNext(&read_batch, &eos));
    // If GetNext did hit the capacity of the RowBatch, then the read page should have
    // been attached to read_batch.
    ASSERT_TRUE(read_batch.num_rows() < num_rows);
    ASSERT_TRUE(!eos);
    read_batch.Reset();

    // Unpin the stream.
    ASSERT_OK(stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
    stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }

  {
    // Test unpinning an empty stream (all rows have been attached to RowBatches).
    BufferedTupleStream stream(
        runtime_state_, int_desc_, &client_, buffer_size, buffer_size);
    ASSERT_OK(stream.Init("SimpleTupleStreamTest::UnpinReadPage", true));
    ASSERT_OK(stream.PrepareForReadWrite(true, &got_reservation));
    ASSERT_TRUE(got_reservation);

    for (int i = 0; i < write_batch->num_rows(); ++i) {
      EXPECT_TRUE(stream.AddRow(write_batch->GetRow(i), &status));
      ASSERT_OK(status);
    }

    // Read and validate all contents of the stream.
    vector<int> results;
    ReadValues(&stream, int_desc_, &results);
    VerifyResults<int>(*int_desc_, results, num_rows, false);

    // Unpin and close the stream.
    ASSERT_OK(stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
    stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }
  write_batch->Reset();
}

void StreamStateTest::TestDeferAdvancingReadPage() {
  int num_rows = 1024;
  int buffer_size = 4 * 1024;
  // Only give 2 * buffer_size for the stream initial read and write page reservation.
  Init(2 * buffer_size);

  bool eos;
  bool got_reservation;
  Status status;
  RowBatch* write_batch = CreateIntBatch(0, num_rows, false);

  {
    // Test unpinning a stream when the read page has been attached to the output batch
    // and the output batch has NOT been reset.
    BufferedTupleStream stream(
        runtime_state_, int_desc_, &client_, buffer_size, buffer_size);
    ASSERT_OK(stream.Init("StreamStateTest::DeferAdvancingReadPage", true));
    ASSERT_OK(stream.PrepareForReadWrite(true, &got_reservation));
    ASSERT_TRUE(got_reservation);

    // Add rows to stream.
    for (int i = 0; i < write_batch->num_rows(); ++i) {
      EXPECT_TRUE(stream.AddRow(write_batch->GetRow(i), &status));
      ASSERT_OK(status);
    }

    // Read until the read page is attached to the output.
    RowBatch read_batch(int_desc_, num_rows, &tracker_);
    ASSERT_OK(stream.GetNext(&read_batch, &eos));
    // If GetNext did hit the capacity of the RowBatch, then the read page should have
    // been attached to read_batch.
    ASSERT_TRUE(read_batch.num_rows() < num_rows);
    ASSERT_TRUE(!eos);

    // We continue adding rows into the stream without releasing the read_batch. We expect
    // that reservation limit will be hit and stream will need to be unpinned. We also
    // expect that, after unpinning the stream, subsequent AddRow is always successful
    // even if we're not immediately releasing the read_batch. We insert write_batch twice
    // to ensure that we're inserting both in pinned and unpinned mode.
    ASSERT_TRUE(stream.is_pinned());
    for (int j = 0; j < 2; ++j) {
      for (int i = 0; i < write_batch->num_rows(); ++i) {
        bool succeed = stream.AddRow(write_batch->GetRow(i), &status);
        ASSERT_TRUE(succeed || stream.is_pinned());
        if (!succeed) {
          // Unpin the stream.
          status = stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT);
          ASSERT_OK(status);
          ASSERT_FALSE(stream.is_pinned());
          ASSERT_EQ(stream.bytes_unpinned(), 0);
          ASSERT_EQ(stream.pages_.size(), 2);
          ASSERT_EQ(stream.num_pages_, 2);
          // Retry inserting this row by decreasing the index.
          // After stream get into unpinned mode, further inserts should be successful,
          // even if we're not immediately cleaning up the read_batch.
          // Stream should be able to unpin the previous write page to reclaim some
          // memory reservation to allocate new write page.
          --i;
        }
      }
    }
    ASSERT_FALSE(stream.is_pinned());
    stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
    read_batch.Reset();
  }
  write_batch->Reset();
}

void StreamStateTest::TestUnpinAfterFullStreamRead(
    bool read_write, bool attach_on_read, bool refill_before_unpin) {
  DCHECK(read_write || !refill_before_unpin)
      << "Only read-write stream support refilling stream after full read.";

  int num_rows = 1024;
  int buffer_size = 4 * 1024;
  int max_num_pages = 4;
  Init(max_num_pages * buffer_size);

  bool got_reservation;
  RowBatch* write_batch = CreateIntBatch(0, num_rows, false);

  {
    BufferedTupleStream stream(
        runtime_state_, int_desc_, &client_, buffer_size, buffer_size);
    ASSERT_OK(stream.Init("StreamStateTest::TestUnpinAfterFullStreamRead", true));
    if (read_write) {
      ASSERT_OK(stream.PrepareForReadWrite(attach_on_read, &got_reservation));
    } else {
      ASSERT_OK(stream.PrepareForWrite(&got_reservation));
    }
    ASSERT_TRUE(got_reservation);
    RowBatch read_batch(int_desc_, num_rows, &tracker_);
    int64_t num_rows_written = 0;
    int64_t num_rows_read = 0;

    // Add rows into the stream until the stream is full.
    ASSERT_OK(FillUpStream(&stream, write_batch, num_rows_written));
    int num_pages = max_num_pages;
    ASSERT_EQ(stream.pages_.size(), num_pages);
    ASSERT_FALSE(stream.has_read_write_page());

    // Read the entire rows out of the stream.
    if (!read_write) {
      ASSERT_OK(stream.PrepareForRead(attach_on_read, &got_reservation));
      ASSERT_TRUE(got_reservation);
    }
    ASSERT_OK(ReadOutStream(&stream, &read_batch, num_rows_read));
    if (attach_on_read) num_pages = 1;
    ASSERT_EQ(stream.pages_.size(), num_pages);
    ASSERT_EQ(stream.has_read_write_page(), read_write);

    if (read_write && refill_before_unpin) {
      // Fill the stream until it is full again.
      ASSERT_OK(FillUpStream(&stream, write_batch, num_rows_written));
      num_pages = max_num_pages;
      ASSERT_EQ(stream.pages_.size(), num_pages);
      ASSERT_EQ(stream.has_read_write_page(), !attach_on_read);
    }

    // Verify that the read page has been fully read before unpinning the stream.
    ASSERT_EQ(
        stream.read_it_.read_page_rows_returned_, stream.read_it_.read_page_->num_rows);
    // read_page_ should NOT be attached to output batch unless stream is in read-only and
    // attach_on_read mode.
    bool attached = !read_write && attach_on_read;
    ASSERT_EQ(stream.read_it_.read_page_->attached_to_output_batch, attached);

    // Verify stream state before UnpinStream.
    int num_pinned_pages = num_pages;
    ASSERT_TRUE(stream.is_pinned());
    if (attached) {
      // In a pinned + read-only + attach_on_read stream, a fully exhausted read page is
      // automatically unpinned and destroyed, but not yet removed from stream.pages_
      // until the next GetNext() or UnpinStream() call.
      ASSERT_EQ(stream.pages_.size(), 1);
      num_pinned_pages = 0;
    }
    VerifyStreamState(&stream, num_pages, num_pinned_pages, 0, buffer_size);

    // Unpin the stream.
    ASSERT_OK(stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));
    ASSERT_FALSE(stream.is_pinned());

    // Verify stream state after UnpinStream. num_pages should remain unchanged after
    // UnpinStream() except for the case of read-only + attach_on_read stream.
    if (read_write) {
      if (attach_on_read) {
        if (refill_before_unpin) {
          num_pinned_pages = 2;
          ASSERT_TRUE(stream.pages_.begin()->is_pinned());
          ASSERT_TRUE(stream.pages_.back().is_pinned());
        } else {
          num_pinned_pages = 1;
          ASSERT_TRUE(stream.pages_.back().is_pinned());
        }
      } else {
        num_pinned_pages = 1;
        ASSERT_TRUE(stream.pages_.back().is_pinned());
      }
    } else {
      if (attach_on_read) {
        num_pages = 0;
      }
      num_pinned_pages = 0;
    }
    int num_unpinned_pages = num_pages - num_pinned_pages;
    VerifyStreamState(
        &stream, num_pages, num_pinned_pages, num_unpinned_pages, buffer_size);

    if (read_write) {
      // Additionally, test that write and read operation still work in read-write
      // stream after UnpinStream.
      Status status;
      ASSERT_OK(ReadOutStream(&stream, &read_batch, num_rows_read));
      for (int i = 0; i < write_batch->num_rows(); ++i) {
        EXPECT_TRUE(stream.AddRow(write_batch->GetRow(i), &status));
        ASSERT_OK(status);
      }
      ASSERT_OK(ReadOutStream(&stream, &read_batch, num_rows_read));
      ASSERT_EQ(write_batch->num_rows(), num_rows_read);
    }

    stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
    read_batch.Reset();
  }
  write_batch->Reset();
}

// Test writing to a stream (AddRow and UnpinStream), even though attached pages have not
// been released yet.
TEST_F(SimpleTupleStreamTest, WriteAfterReadAttached) {
  int buffer_size = 4 * 1024;
  Init(100 * buffer_size);

  bool eos;
  bool got_reservation;
  Status status;

  vector<int> results;
  RowBatch read_batch(int_desc_, 1, &tracker_);
  RowBatch* write_batch = CreateIntBatch(0, 1, false);
  ASSERT_EQ(write_batch->num_rows(), 1);

  // Test adding a row to the stream before releasing an output batch returned by
  // GetNext.
  BufferedTupleStream stream(
      runtime_state_, int_desc_, &client_, buffer_size, buffer_size);
  ASSERT_OK(stream.Init("SimpleTupleStreamTest::InterleaveReadAndWrite", true));
  ASSERT_OK(stream.PrepareForReadWrite(true, &got_reservation));
  ASSERT_TRUE(got_reservation);

  // Add a row to the stream.
  EXPECT_TRUE(stream.AddRow(write_batch->GetRow(0), &status));
  ASSERT_OK(status);

  // Read a row from the stream, but do not reset the read_batch.
  ASSERT_OK(stream.GetNext(&read_batch, &eos));

  // Add a row to the stream.
  EXPECT_TRUE(stream.AddRow(write_batch->GetRow(0), &status));
  ASSERT_OK(status);

  // Validate the contents of read_batch.
  AppendRowTuples(read_batch.GetRow(0), int_desc_, &results);
  VerifyResults<int>(*int_desc_, results, 1, false);
  results.clear();

  // Validate the data just added to the stream.
  ReadValues(&stream, int_desc_, &results);
  VerifyResults<int>(*int_desc_, results, 1, false);
  results.clear();

  // Reset the read_batch and close the stream.
  read_batch.Reset();
  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);

  // Read a batch from the stream, unpin the stream, add a row, and then validate the
  // contents of read batch.
  BufferedTupleStream unpin_stream(
      runtime_state_, int_desc_, &client_, buffer_size, buffer_size);
  ASSERT_OK(unpin_stream.Init("SimpleTupleStreamTest::InterleaveReadAndWrite", true));
  ASSERT_OK(unpin_stream.PrepareForReadWrite(true, &got_reservation));
  ASSERT_TRUE(got_reservation);

  // Add a row to the stream.
  EXPECT_TRUE(unpin_stream.AddRow(write_batch->GetRow(0), &status));
  ASSERT_OK(status);

  // Read a row from the stream, but do not reset the read_batch.
  ASSERT_OK(unpin_stream.GetNext(&read_batch, &eos));

  // Unpin the stream.
  ASSERT_OK(unpin_stream.UnpinStream(BufferedTupleStream::UNPIN_ALL_EXCEPT_CURRENT));

  // Add a row to the stream.
  EXPECT_TRUE(unpin_stream.AddRow(write_batch->GetRow(0), &status));
  ASSERT_OK(status);

  // Validate the contents of read_batch.
  AppendRowTuples(read_batch.GetRow(0), int_desc_, &results);
  VerifyResults<int>(*int_desc_, results, 1, false);
  results.clear();

  // Validate the data just added to the stream.
  ReadValues(&unpin_stream, int_desc_, &results);
  VerifyResults<int>(*int_desc_, results, 1, false);
  results.clear();

  // Reset the read_batch and close the stream.
  read_batch.Reset();
  unpin_stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);

  write_batch->Reset();
}

/// Test multiple threads reading from a pinned stream using separate read iterators.
TEST_F(SimpleTupleStreamTest, ConcurrentReaders) {
  const int BUFFER_SIZE = 1024;
  // Each tuple is an integer plus a null indicator byte.
  const int VALS_PER_BUFFER = BUFFER_SIZE / (sizeof(int32_t) + 1);
  const int NUM_BUFFERS = 100;
  const int TOTAL_MEM = NUM_BUFFERS * BUFFER_SIZE;
  Init(TOTAL_MEM);
  BufferedTupleStream stream(
      runtime_state_, int_desc_, &client_, BUFFER_SIZE, BUFFER_SIZE);
  ASSERT_OK(stream.Init("ConcurrentReaders", true));
  bool got_write_reservation;
  ASSERT_OK(stream.PrepareForWrite(&got_write_reservation));
  ASSERT_TRUE(got_write_reservation);

  // Add rows to the stream.
  int offset = 0;
  const int NUM_BATCHES = NUM_BUFFERS;
  const int ROWS_PER_BATCH = VALS_PER_BUFFER;
  for (int i = 0; i < NUM_BATCHES; ++i) {
    RowBatch* batch = nullptr;
    Status status;
    batch = CreateBatch(int_desc_, offset, ROWS_PER_BATCH, false);
    for (int j = 0; j < batch->num_rows(); ++j) {
      bool b = stream.AddRow(batch->GetRow(j), &status);
      ASSERT_OK(status);
      ASSERT_TRUE(b);
    }
    offset += batch->num_rows();
    // Reset the batch to make sure the stream handles the memory correctly.
    batch->Reset();
  }
  // Invalidate the write iterator explicitly so that we can read concurrently.
  stream.DoneWriting();

  const int READ_ITERS = 10; // Do multiple read passes per thread.
  const int NUM_THREADS = 4;

  // Read from the main thread with the built-in iterator and other threads with
  // external iterators.
  thread_group workers;
  for (int i = 0; i < NUM_THREADS; ++i) {
    workers.add_thread(new thread([&] () {
      for (int j = 0; j < READ_ITERS; ++j) {
        BufferedTupleStream::ReadIterator it;
        ASSERT_OK(stream.PrepareForPinnedRead(&it));

        // Read all the rows back
        vector<int> results;
        ReadValues(&stream, &it, int_desc_, &results);

        // Verify result
        VerifyResults<int>(*int_desc_, results, ROWS_PER_BATCH * NUM_BATCHES, false);
      }
    }));
  }

  for (int i = 0; i < READ_ITERS; ++i) {
    bool got_read_reservation;
    ASSERT_OK(stream.PrepareForRead(false, &got_read_reservation));
    ASSERT_TRUE(got_read_reservation);

    // Read all the rows back
    vector<int> results;
    ReadValues(&stream, int_desc_, &results);

    // Verify result
    VerifyResults<int>(*int_desc_, results, ROWS_PER_BATCH * NUM_BATCHES, false);
  }
  workers.join_all();
  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

void StreamStateTest::TestShortDebugString() {
  Init(BUFFER_POOL_LIMIT);

  int num_batches = 50;
  RowDescriptor* desc = int_desc_;
  bool gen_null = false;
  int64_t default_page_len = 128 * sizeof(int);
  int64_t max_page_len = default_page_len;
  int num_rows = BATCH_SIZE;

  BufferedTupleStream stream(
      runtime_state_, desc, &client_, default_page_len, max_page_len);
  ASSERT_OK(stream.Init("StreamStateTest::ShortDebugString", true));
  bool got_write_reservation;
  ASSERT_OK(stream.PrepareForWrite(&got_write_reservation));
  ASSERT_TRUE(got_write_reservation);

  // Add rows to the stream
  int offset = 0;
  for (int i = 0; i < num_batches; ++i) {
    RowBatch* batch = nullptr;

    Status status;
    batch = CreateBatch(desc, offset, num_rows, gen_null);
    for (int j = 0; j < batch->num_rows(); ++j) {
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
  vector<int> results;
  ReadValues(&stream, desc, &results);

  // Verify result
  VerifyResults<int>(*desc, results, num_rows * num_batches, gen_null);

  // Verify that stream contains more than MAX_PAGE_ITER_DEBUG pages and only subset of
  // pages are included in DebugString().
  DCHECK_GT(stream.num_pages_, BufferedTupleStream::MAX_PAGE_ITER_DEBUG);
  string page_count_substr = Substitute(
      "$0 out of $1 pages=", BufferedTupleStream::MAX_PAGE_ITER_DEBUG, stream.num_pages_);
  string debug_string = stream.DebugString();
  ASSERT_NE(debug_string.find(page_count_substr), string::npos)
      << page_count_substr << " not found at BufferedTupleStream::DebugString(). "
      << debug_string;
  ASSERT_LE(debug_string.length(), ErrorMsg::MAX_ERROR_MESSAGE_LEN);

  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

// Basic API test. No data should be going to disk.
TEST_F(SimpleNullStreamTest, Basic) {
  Init(BUFFER_POOL_LIMIT);
  TestValues<int>(0, int_desc_, false, true);
  TestValues<int>(1, int_desc_, false, true);
  TestValues<int>(10, int_desc_, false, true);
  TestValues<int>(100, int_desc_, false, true);
  TestValues<int>(0, int_desc_, true, true);
  TestValues<int>(1, int_desc_, true, true);
  TestValues<int>(10, int_desc_, true, true);
  TestValues<int>(100, int_desc_, true, true);
  TestValues<int>(0, int_desc_, false, false);
  TestValues<int>(1, int_desc_, false, false);
  TestValues<int>(10, int_desc_, false, false);
  TestValues<int>(100, int_desc_, false, false);
  TestValues<int>(0, int_desc_, true, false);
  TestValues<int>(1, int_desc_, true, false);
  TestValues<int>(10, int_desc_, true, false);
  TestValues<int>(100, int_desc_, true, false);

  TestValues<StringValue>(0, string_desc_, false, true);
  TestValues<StringValue>(1, string_desc_, false, true);
  TestValues<StringValue>(10, string_desc_, false, true);
  TestValues<StringValue>(100, string_desc_, false, true);
  TestValues<StringValue>(0, string_desc_, true, true);
  TestValues<StringValue>(1, string_desc_, true, true);
  TestValues<StringValue>(10, string_desc_, true, true);
  TestValues<StringValue>(100, string_desc_, true, true);
  TestValues<StringValue>(0, string_desc_, false, false);
  TestValues<StringValue>(1, string_desc_, false, false);
  TestValues<StringValue>(10, string_desc_, false, false);
  TestValues<StringValue>(100, string_desc_, false, false);
  TestValues<StringValue>(0, string_desc_, true, false);
  TestValues<StringValue>(1, string_desc_, true, false);
  TestValues<StringValue>(10, string_desc_, true, false);
  TestValues<StringValue>(100, string_desc_, true, false);

  TestIntValuesInterleaved(0, 1, true);
  TestIntValuesInterleaved(1, 1, true);
  TestIntValuesInterleaved(10, 5, true);
  TestIntValuesInterleaved(100, 15, true);
  TestIntValuesInterleaved(0, 1, false);
  TestIntValuesInterleaved(1, 1, false);
  TestIntValuesInterleaved(10, 5, false);
  TestIntValuesInterleaved(100, 15, false);
}

// Test tuple stream with only 1 buffer and rows with multiple tuples.
TEST_F(MultiTupleStreamTest, MultiTupleOneBufferSpill) {
  // Each buffer can only hold 128 ints, so this spills quite often.
  int buffer_size = 128 * sizeof(int);
  Init(buffer_size);
  TestValues<int>(0, int_desc_, false, true, buffer_size);
  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);

  TestValues<StringValue>(0, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
}

// Test with a few buffers and rows with multiple tuples.
TEST_F(MultiTupleStreamTest, MultiTupleManyBufferSpill) {
  int buffer_size = 128 * sizeof(int);
  Init(10 * buffer_size);

  TestValues<int>(0, int_desc_, false, true, buffer_size);
  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);
  TestValues<int>(100, int_desc_, false, true, buffer_size);

  TestValues<StringValue>(0, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(100, string_desc_, false, true, buffer_size);

  TestIntValuesInterleaved(1, 1, true, buffer_size);
  TestIntValuesInterleaved(10, 5, true, buffer_size);
  TestIntValuesInterleaved(100, 15, true, buffer_size);
}

// Test that we can allocate a row in the stream and copy in multiple tuples then
// read it back from the stream.
TEST_F(MultiTupleStreamTest, MultiTupleAddRowCustom) {
  // Use small buffers so it will be flushed to disk.
  int buffer_size = 4 * 1024;
  Init(2 * buffer_size);
  Status status = Status::OK();

  int num_batches = 1;
  int rows_added = 0;
  BufferedTupleStream stream(
      runtime_state_, string_desc_, &client_, buffer_size, buffer_size);
  ASSERT_OK(stream.Init("MultiTupleStreamTest::MultiTupleAddRowCustom", false));
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
      uint8_t* data = stream.AddRowCustomBegin(fixed_size + varlen_size, &status);
      ASSERT_TRUE(data != nullptr);
      ASSERT_TRUE(status.ok());
      WriteStringRow(string_desc_, row, fixed_size, varlen_size, data);
      stream.AddRowCustomEnd(fixed_size + varlen_size);
    }
    rows_added += batch->num_rows();
  }

  for (int i = 0; i < 3; ++i) {
    bool attach_on_read = i == 2;
    vector<StringValue> results;
    bool got_read_reservation;
    ASSERT_OK(stream.PrepareForRead(attach_on_read, &got_read_reservation));
    ASSERT_TRUE(got_read_reservation);
    ReadValues(&stream, string_desc_, &results);
    VerifyResults<StringValue>(*string_desc_, results, rows_added, false);
  }

  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

void SimpleTupleStreamTest::WriteStringRow(const RowDescriptor* row_desc, TupleRow* row,
    int64_t fixed_size, int64_t varlen_size, uint8_t* data) {
  uint8_t* fixed_data = data;
  uint8_t* varlen_write_ptr = data + fixed_size;
  for (int i = 0; i < row_desc->tuple_descriptors().size(); i++) {
    TupleDescriptor* tuple_desc = row_desc->tuple_descriptors()[i];
    Tuple* src = row->GetTuple(i);
    Tuple* dst = reinterpret_cast<Tuple*>(fixed_data);
    fixed_data += tuple_desc->byte_size();
    memcpy(dst, src, tuple_desc->byte_size());
    for (SlotDescriptor* slot : tuple_desc->slots()) {
      StringValue* src_string = src->GetStringSlot(slot->tuple_offset());
      if (src_string->IsSmall()) continue;
      StringValue* dst_string = dst->GetStringSlot(slot->tuple_offset());
      dst_string->Assign(reinterpret_cast<char*>(varlen_write_ptr), src_string->Len());
      memcpy(dst_string->Ptr(), src_string->Ptr(), src_string->Len());
      varlen_write_ptr += src_string->Len();
    }
  }
  ASSERT_EQ(data + fixed_size + varlen_size, varlen_write_ptr);
}

// Test with rows with multiple nullable tuples.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleOneBufferSpill) {
  // Each buffer can only hold 128 ints, so this spills quite often.
  int buffer_size = 128 * sizeof(int);
  Init(buffer_size);
  TestValues<int>(0, int_desc_, false, true, buffer_size);
  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);
  TestValues<int>(0, int_desc_, true, true, buffer_size);
  TestValues<int>(1, int_desc_, true, true, buffer_size);
  TestValues<int>(10, int_desc_, true, true, buffer_size);

  TestValues<StringValue>(0, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(0, string_desc_, true, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, true, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, true, true, buffer_size);
}

// Test with a few buffers.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleManyBufferSpill) {
  int buffer_size = 128 * sizeof(int);
  Init(10 * buffer_size);

  TestValues<int>(0, int_desc_, false, true, buffer_size);
  TestValues<int>(1, int_desc_, false, true, buffer_size);
  TestValues<int>(10, int_desc_, false, true, buffer_size);
  TestValues<int>(100, int_desc_, false, true, buffer_size);
  TestValues<int>(0, int_desc_, true, true, buffer_size);
  TestValues<int>(1, int_desc_, true, true, buffer_size);
  TestValues<int>(10, int_desc_, true, true, buffer_size);
  TestValues<int>(100, int_desc_, true, true, buffer_size);

  TestValues<StringValue>(0, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(100, string_desc_, false, true, buffer_size);
  TestValues<StringValue>(0, string_desc_, true, true, buffer_size);
  TestValues<StringValue>(1, string_desc_, true, true, buffer_size);
  TestValues<StringValue>(10, string_desc_, true, true, buffer_size);
  TestValues<StringValue>(100, string_desc_, true, true, buffer_size);

  TestIntValuesInterleaved(0, 1, true, buffer_size);
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

  BufferedTupleStream stream(
      runtime_state_, string_desc_, &client_, PAGE_LEN, PAGE_LEN, external_slots);
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
  sv->Assign(StringValue::MakeSmallStringFrom(STRINGS[0]));
  int64_t expected_len =
      tuple_null_indicator_bytes + string_desc_->GetRowSize() +
      (sv->IsSmall() ? 0 : sv->Len());
  EXPECT_EQ(expected_len, stream.ComputeRowSize(row.get()));

  // Check that external slots aren't included in count.
  sv = tuple1->GetStringSlot(external_string_slot->tuple_offset());
  sv->Assign(reinterpret_cast<char*>(1234), 1234);
  EXPECT_EQ(expected_len, stream.ComputeRowSize(row.get()));

  stream.Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
}

/// Test that deep copy works with arrays by copying into a BufferedTupleStream, freeing
/// the original rows, then reading back the rows and verifying the contents.
TEST_F(ArrayTupleStreamTest, TestArrayDeepCopy) {
  Status status;
  Init(BUFFER_POOL_LIMIT);
  const int NUM_ROWS = 4000;
  BufferedTupleStream stream(runtime_state_, array_desc_, &client_, PAGE_LEN, PAGE_LEN);
  const vector<TupleDescriptor*>& tuple_descs = array_desc_->tuple_descriptors();
  // Write out a predictable pattern of data by iterating over arrays of constants.
  int strings_index = 0; // we take the mod of this as index into STRINGS.
  int array_lens[] = {0, 1, 5, 10, 1000, 2, 49, 20};
  int num_array_lens = sizeof(array_lens) / sizeof(array_lens[0]);
  int array_len_index = 0;
  ASSERT_OK(stream.Init("ArrayTupleStreamTest::TestArrayDeepCopy", false));
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
    const TupleDescriptor* item_desc = array_slot_desc->children_tuple_descriptor();

    int array_len = array_lens[array_len_index++ % num_array_lens];
    CollectionValue* cv = tuple0->GetCollectionSlot(array_slot_desc->tuple_offset());
    cv->ptr = nullptr;
    cv->num_tuples = 0;
    CollectionValueBuilder builder(
        cv, *item_desc, mem_pool_.get(), runtime_state_, array_len);
    Tuple* array_data;
    int num_rows;
    ASSERT_OK(builder.GetFreeMemory(&array_data, &num_rows));
    expected_row_size += item_desc->byte_size() * array_len;

    // Fill the array with pointers to our constant strings.
    for (int j = 0; j < array_len; ++j) {
      const StringValue* string = &STRINGS[strings_index++ % NUM_STRINGS];
      array_data->SetNotNull(item_desc->slots()[0]->null_indicator_offset());
      RawValue::Write(string, array_data, item_desc->slots()[0], mem_pool_.get());
      array_data += item_desc->byte_size();
      expected_row_size += string->Len();
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
  RowBatch batch(array_desc_, BATCH_SIZE, &tracker_);
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

      const TupleDescriptor* item_desc = array_slot_desc->children_tuple_descriptor();
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

  BufferedTupleStream stream(
      runtime_state_, array_desc_, &client_, PAGE_LEN, PAGE_LEN, external_slots);
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
  EXPECT_EQ(tuple_null_indicator_bytes,
      stream.ComputeRowSize(row.get()));

  // Tuples are initialized to empty and have no var-len data.
  row->SetTuple(0, tuple0.get());
  row->SetTuple(1, tuple1.get());
  EXPECT_EQ(tuple_null_indicator_bytes + array_desc_->GetRowSize(),
      stream.ComputeRowSize(row.get()));

  // Tuple 0 has an array.
  int expected_row_size = tuple_null_indicator_bytes + array_desc_->GetRowSize();
  const SlotDescriptor* array_slot = tuple_descs[0]->slots()[0];
  const TupleDescriptor* item_desc = array_slot->children_tuple_descriptor();
  int array_len = 128;
  CollectionValue* cv = tuple0->GetCollectionSlot(array_slot->tuple_offset());
  CollectionValueBuilder builder(
      cv, *item_desc, mem_pool_.get(), runtime_state_, array_len);
  Tuple* array_data;
  int num_rows;
  ASSERT_OK(builder.GetFreeMemory(&array_data, &num_rows));
  expected_row_size += item_desc->byte_size() * array_len;

  // Fill the array with pointers to our constant strings.
  for (int i = 0; i < array_len; ++i) {
    const StringValue* str = &STRINGS[i % NUM_STRINGS];
    array_data->SetNotNull(item_desc->slots()[0]->null_indicator_offset());
    RawValue::Write(str, array_data, item_desc->slots()[0], mem_pool_.get());
    array_data += item_desc->byte_size();
    expected_row_size += str->Len();
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

Status StreamStateTest::FillUpStream(
    BufferedTupleStream* stream, RowBatch* write_batch, int64_t& num_inserted) {
  DCHECK(stream->is_pinned());
  int64_t idx = 0;
  Status status;
  num_inserted = 0;
  while (stream->AddRow(write_batch->GetRow(idx), &status)) {
    RETURN_IF_ERROR(status);
    idx = (idx + 1) % write_batch->num_rows();
    num_inserted++;
  }
  return status;
}

Status StreamStateTest::ReadOutStream(
    BufferedTupleStream* stream, RowBatch* read_batch, int64_t& num_read) {
  bool eos = false;
  num_read = 0;
  do {
    read_batch->Reset();
    RETURN_IF_ERROR(stream->GetNext(read_batch, &eos));
    num_read += read_batch->num_rows();
  } while (!eos);
  return Status::OK();
}

void StreamStateTest::VerifyStreamState(BufferedTupleStream* stream, int num_page,
    int num_pinned_page, int num_unpinned_page, int buffer_size) {
  ASSERT_EQ(stream->pages_.size(), num_page);
  ASSERT_EQ(stream->num_pages_, num_page);
  ASSERT_EQ(stream->BytesPinned(false), buffer_size * num_pinned_page);
  ASSERT_EQ(stream->bytes_unpinned(), buffer_size * num_unpinned_page);
  stream->CheckConsistencyFull(stream->read_it_);
}

TEST_F(StreamStateTest, DeferAdvancingReadPage) {
  TestDeferAdvancingReadPage();
}

TEST_F(StreamStateTest, UnpinFullyExhaustedReadPageOnReadWriteStreamNoAttachRefill) {
  TestUnpinAfterFullStreamRead(true, false, true);
}

TEST_F(StreamStateTest, UnpinFullyExhaustedReadPageOnReadWriteStreamNoAttachNoRefill) {
  TestUnpinAfterFullStreamRead(true, false, false);
}

TEST_F(StreamStateTest, UnpinFullyExhaustedReadPageOnReadWriteStreamAttachRefill) {
  TestUnpinAfterFullStreamRead(true, true, true);
}

TEST_F(StreamStateTest, UnpinFullyExhaustedReadPageOnReadWriteStreamAttachNoRefill) {
  TestUnpinAfterFullStreamRead(true, true, false);
}

TEST_F(StreamStateTest, UnpinFullyExhaustedReadPageOnReadOnlyStreamAttach) {
  TestUnpinAfterFullStreamRead(false, true, false);
}

TEST_F(StreamStateTest, UnpinFullyExhaustedReadPageOnReadOnlyStreamNoAttach) {
  TestUnpinAfterFullStreamRead(false, false, false);
}

TEST_F(StreamStateTest, ShortDebugString) {
  TestShortDebugString();
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  return RUN_ALL_TESTS();
}
