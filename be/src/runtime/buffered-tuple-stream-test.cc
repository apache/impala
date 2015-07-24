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

  virtual RowBatch* CreateIntBatch(int start_val, int num_rows, bool gen_null) {
    RowBatch* batch = pool_.Add(new RowBatch(*int_desc_, num_rows, &tracker_));
    int* tuple_mem = reinterpret_cast<int*>(
        batch->tuple_data_pool()->Allocate(sizeof(int) * num_rows));
    const int int_tuples = int_desc_->tuple_descriptors().size();
    if (int_tuples > 1) {
      for (int i = 0; i < num_rows; ++i) {
        int idx = batch->AddRow();
        TupleRow* row = batch->GetRow(idx);
        tuple_mem[i] = i + start_val;
        for (int j = 0; j < int_tuples; ++j) {
          if (!gen_null || (j % 2) == 0) {
            row->SetTuple(j, reinterpret_cast<Tuple*>(&tuple_mem[i]));
          } else {
            row->SetTuple(j, NULL);
          }
        }
        batch->CommitLastRow();
      }
    } else {
      for (int i = 0; i < num_rows; ++i) {
        int idx = batch->AddRow();
        TupleRow* row = batch->GetRow(idx);
        tuple_mem[i] = i + start_val;
        if (!gen_null || (i % 2) == 0) {
            row->SetTuple(0, reinterpret_cast<Tuple*>(&tuple_mem[i]));
        } else {
            row->SetTuple(0, NULL);
        }
        batch->CommitLastRow();
      }
    }
    return batch;
  }

  virtual RowBatch* CreateStringBatch(int string_idx, int num_rows, bool gen_null) {
    int tuple_size = sizeof(StringValue) + 1;
    RowBatch* batch = pool_.Add(new RowBatch(*string_desc_, num_rows, &tracker_));
    uint8_t* tuple_mem = batch->tuple_data_pool()->Allocate(tuple_size * num_rows);
    memset(tuple_mem, 0, tuple_size * num_rows);
    const int string_tuples = string_desc_->tuple_descriptors().size();
    if (string_tuples > 1) {
      for (int i = 0; i < num_rows; ++i) {
        TupleRow* row = batch->GetRow(batch->AddRow());
        string_idx %= NUM_STRINGS;
        *reinterpret_cast<StringValue*>(tuple_mem + 1) = STRINGS[string_idx];
        ++string_idx;
        for (int j = 0; j < string_tuples; ++j) {
          if (!gen_null || (j % 2) == 0) {
            row->SetTuple(j, reinterpret_cast<Tuple*>(tuple_mem));
          } else {
            row->SetTuple(j, NULL);
          }
        }
        batch->CommitLastRow();
        tuple_mem += tuple_size;
      }
    } else {
      for (int i = 0; i < num_rows; ++i) {
        TupleRow* row = batch->GetRow(batch->AddRow());
        string_idx %= NUM_STRINGS;
        *reinterpret_cast<StringValue*>(tuple_mem + 1) = STRINGS[string_idx];
        ++string_idx;
        if (!gen_null || (i % 2) == 0) {
          row->SetTuple(0, reinterpret_cast<Tuple*>(tuple_mem));
        } else {
          row->SetTuple(0, NULL);
        }
        batch->CommitLastRow();
        tuple_mem += tuple_size;
      }
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
      results->push_back(*reinterpret_cast<int*>(t));
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
    if (int_tuples > 1) {
      for (int i = 0; i < results.size(); i += int_tuples) {
        for (int j = 0; j < int_tuples; ++j) {
          if (!gen_null || (j % 2) == 0) {
            ASSERT_EQ(results[i+j], i / int_tuples)
                << " results[" << (i + j) << "]: " << results[i + j]
                << " != " << (i / int_tuples) << " gen_null=" << gen_null;
          } else {
            ASSERT_TRUE(results[i+j] == std::numeric_limits<int>::max())
                << "i: " << i << " j: " << j << " results[" << (i + j) << "]: "
                << results[i+j] << " != " << std::numeric_limits<int>::max();
          }
        }
      }
    } else {
      for (int i = 0; i < results.size(); i += int_tuples) {
        if (!gen_null || (i % 2) == 0) {
          ASSERT_TRUE(results[i] == i)
              << " results[" << (i) << "]: " << results[i]
              << " != " << i << " gen_null=" << gen_null;
        } else {
          ASSERT_TRUE(results[i] == std::numeric_limits<int>::max())
              << "i: " << i << " results[" << i << "]: "
              << results[i] << " != " << std::numeric_limits<int>::max();
        }
      }
    }
  }

  virtual void VerifyResults(const vector<StringValue>& results, int exp_rows,
      bool gen_null) {
    const int string_tuples = string_desc_->tuple_descriptors().size();
    EXPECT_EQ(results.size(), exp_rows * string_tuples);
    int idx = 0;
    if (string_tuples > 1) {
      for (int i = 0; i < results.size(); i += string_tuples) {
        for (int j = 0; j < string_tuples; ++j) {
          if (!gen_null || (j % 2) == 0) {
            ASSERT_TRUE(results[i+j] == STRINGS[idx])
                << "results[" << i << "+" << j << "] " << results[i+j]
                << " != " << STRINGS[idx] << " idx=" << idx << " gen_null=" << gen_null;
          } else {
            ASSERT_TRUE(results[i+j] == StringValue())
                << "results[" << i << "+" << j << "] " << results[i+j] << " not NULL";
          }
        }
        idx = (idx + 1) % NUM_STRINGS;
      }
    } else {
      for (int i = 0; i < results.size(); i += string_tuples) {
        if (!gen_null || (i % 2) == 0) {
          ASSERT_TRUE(results[i] == STRINGS[idx])
              << "results[" << i << "] " << results[i]
              << " != " << STRINGS[idx] << " idx=" << idx << " gen_null=" << gen_null;
        } else {
          ASSERT_TRUE(results[i] == StringValue())
              << "results[" << i << "] " << results[i] << " not NULL";
        }
        idx = (idx + 1) % NUM_STRINGS;
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

      // Verify result
      const int int_tuples = int_desc_->tuple_descriptors().size();
      EXPECT_EQ(results.size(), BATCH_SIZE * num_batches * int_tuples);
      for (int i = 0; i < results.size(); ++i) {
        ASSERT_EQ(results[i], i / int_tuples);
      }

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
