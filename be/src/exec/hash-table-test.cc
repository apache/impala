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

#include <boost/scoped_ptr.hpp>

#include <stdio.h>
#include <stdlib.h>
#include <limits>
#include <vector>

#include "common/compiler-util.h"
#include "common/init.h"
#include "exec/hash-table.inline.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/slot-ref.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-value.h"
#include "runtime/test-env.h"
#include "runtime/tuple-row.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "util/cpu-info.h"
#include "util/runtime-profile-counters.h"
#include "util/test-info.h"

#include "common/names.h"

using namespace std;
using namespace boost;

namespace impala {

class HashTableTest : public testing::Test {
 public:
  HashTableTest() : mem_pool_(&tracker_) {}

 protected:
  /// Temporary runtime environment for the hash table.
  scoped_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;

  /// Hash tables and associated clients - automatically closed in TearDown().
  vector<BufferPool::ClientHandle*> clients_;
  vector<HashTable*> hash_tables_;

  ObjectPool pool_;
  /// A dummy MemTracker used for exprs and other things we don't need to have limits on.
  MemTracker tracker_;
  MemPool mem_pool_;
  vector<ScalarExpr*> build_exprs_;
  vector<ScalarExprEvaluator*> build_expr_evals_;
  vector<ScalarExpr*> probe_exprs_;
  vector<ScalarExprEvaluator*> probe_expr_evals_;
  int next_query_id_ = 0;

  virtual void SetUp() {
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
    RowDescriptor desc;

    // Not very easy to test complex tuple layouts so this test will use the
    // simplest.  The purpose of these tests is to exercise the hash map
    // internals so a simple build/probe expr is fine.
    ScalarExpr* build_expr =
      pool_.Add(new SlotRef(ColumnType(TYPE_INT), 1, true /* nullable */));
    ASSERT_OK(build_expr->Init(desc, true, nullptr));
    build_exprs_.push_back(build_expr);
    ASSERT_OK(ScalarExprEvaluator::Create(build_exprs_, nullptr, &pool_, &mem_pool_,
        &mem_pool_, &build_expr_evals_));
    ASSERT_OK(ScalarExprEvaluator::Open(build_expr_evals_, nullptr));

    ScalarExpr* probe_expr =
      pool_.Add(new SlotRef(ColumnType(TYPE_INT), 1, true /* nullable */));
    ASSERT_OK(probe_expr->Init(desc, true, nullptr));
    probe_exprs_.push_back(probe_expr);
    ASSERT_OK(ScalarExprEvaluator::Create(probe_exprs_, nullptr, &pool_, &mem_pool_,
        &mem_pool_, &probe_expr_evals_));
    ASSERT_OK(ScalarExprEvaluator::Open(probe_expr_evals_, nullptr));

    CreateTestEnv();
  }

  virtual void TearDown() {
    ScalarExprEvaluator::Close(build_expr_evals_, nullptr);
    ScalarExprEvaluator::Close(probe_expr_evals_, nullptr);
    ScalarExpr::Close(build_exprs_);
    ScalarExpr::Close(probe_exprs_);

    for (HashTable* hash_table : hash_tables_) hash_table->Close();
    hash_tables_.clear();

    for (BufferPool::ClientHandle* client : clients_) {
      test_env_->exec_env()->buffer_pool()->DeregisterClient(client);
    }
    clients_.clear();

    runtime_state_ = nullptr;
    test_env_.reset();
    mem_pool_.FreeAll();
    pool_.Clear();
  }

  /// Initialize test_env_ and runtime_state_ with the given page size and capacity
  /// for the given number of pages. If test_env_ was already created, then re-creates it.
  void CreateTestEnv(int64_t min_page_size = 64 * 1024,
      int64_t buffer_bytes_limit = 4L * 1024 * 1024 * 1024) {
    test_env_.reset(new TestEnv());
    test_env_->SetBufferPoolArgs(min_page_size, buffer_bytes_limit);
    ASSERT_OK(test_env_->Init());

    TQueryOptions query_options;
    query_options.__set_default_spillable_buffer_size(min_page_size);
    query_options.__set_min_spillable_buffer_size(min_page_size);
    query_options.__set_buffer_pool_limit(buffer_bytes_limit);
    ASSERT_OK(test_env_->CreateQueryState(0, &query_options, &runtime_state_));
  }

  TupleRow* CreateTupleRow(int32_t val) {
    uint8_t* tuple_row_mem = mem_pool_.Allocate(sizeof(int32_t*));
    Tuple* tuple_mem = Tuple::Create(sizeof(char) + sizeof(int32_t), &mem_pool_);
    *reinterpret_cast<int32_t *>(tuple_mem->GetSlot(1)) = val;
    tuple_mem->SetNotNull(NullIndicatorOffset(0,1));
    TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    row->SetTuple(0, tuple_mem);
    return row;
  }

  TupleRow* CreateNullTupleRow() {
    uint8_t* tuple_row_mem = mem_pool_.Allocate(sizeof(int32_t*));
    Tuple* tuple_mem = Tuple::Create(sizeof(int32_t), &mem_pool_);
    tuple_mem->SetNull(NullIndicatorOffset(0,1));
    TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    row->SetTuple(0, tuple_mem);
    return row;
  }

  // Wrapper to call private methods on HashTable
  // TODO: understand google testing, there must be a more natural way to do this
  Status ResizeTable(
      HashTable* table, int64_t new_size, HashTableCtx* ht_ctx, bool* success) {
    return table->ResizeBuckets(new_size, ht_ctx, success);
  }

  // Do a full table scan on table.  All values should be between [min,max).  If
  // all_unique, then each key(int value) should only appear once.  Results are
  // stored in results, indexed by the key.  Results must have been preallocated to
  // be at least max size.
  void FullScan(HashTable* table, HashTableCtx* ht_ctx, int min, int max,
      bool all_unique, TupleRow** results, TupleRow** expected) {
    HashTable::Iterator iter = table->Begin(ht_ctx);
    while (!iter.AtEnd()) {
      TupleRow* row = iter.GetRow();
      int32_t val = *reinterpret_cast<int32_t*>(build_expr_evals_[0]->GetValue(row));
      EXPECT_GE(val, min);
      EXPECT_LT(val, max);
      if (all_unique) {
        EXPECT_TRUE(results[val] == nullptr);
      }
      EXPECT_EQ(row->GetTuple(0), expected[val]->GetTuple(0));
      results[val] = row;
      iter.Next();
    }
  }

  // Validate that probe_row evaluates overs probe_exprs is equal to build_row
  // evaluated over build_exprs
  void ValidateMatch(TupleRow* probe_row, TupleRow* build_row) {
    EXPECT_TRUE(probe_row != build_row);
    int32_t build_val =
        *reinterpret_cast<int32_t*>(build_expr_evals_[0]->GetValue(probe_row));
    int32_t probe_val =
        *reinterpret_cast<int32_t*>(probe_expr_evals_[0]->GetValue(build_row));
    EXPECT_EQ(build_val, probe_val);
  }

  struct ProbeTestData {
    TupleRow* probe_row;
    vector<TupleRow*> expected_build_rows;
  };

  void ProbeTest(HashTable* table, HashTableCtx* ht_ctx,
      ProbeTestData* data, int num_data, bool scan) {
    for (int i = 0; i < num_data; ++i) {
      TupleRow* row = data[i].probe_row;

      HashTable::Iterator iter;
      if (ht_ctx->EvalAndHashProbe(row)) continue;
      iter = table->FindProbeRow(ht_ctx);

      if (data[i].expected_build_rows.size() == 0) {
        EXPECT_TRUE(iter.AtEnd());
      } else {
        if (scan) {
          map<TupleRow*, bool> matched;
          while (!iter.AtEnd()) {
            EXPECT_EQ(matched.find(iter.GetRow()), matched.end());
            matched[iter.GetRow()] = true;
            iter.Next();
          }
          EXPECT_EQ(matched.size(), data[i].expected_build_rows.size());
          for (int j = 0; i < data[j].expected_build_rows.size(); ++j) {
            EXPECT_TRUE(matched[data[i].expected_build_rows[j]]);
          }
        } else {
          EXPECT_EQ(data[i].expected_build_rows.size(), 1);
          EXPECT_EQ(data[i].expected_build_rows[0]->GetTuple(0),
                    iter.GetRow()->GetTuple(0));
          ValidateMatch(row, iter.GetRow());
        }
      }
    }
  }

  /// Construct hash table and buffer pool client.
  /// Returns true if HashTable::Init() was successful. Created objects
  /// and resources (e.g. reservations) are automatically freed in TearDown().
  bool CreateHashTable(bool quadratic, int64_t initial_num_buckets, HashTable** table,
      int64_t block_size = 8 * 1024 * 1024, int max_num_blocks = 100,
      int initial_reserved_blocks = 10, int64_t suballocator_buffer_len = 64 * 1024) {
    BufferPool* buffer_pool = test_env_->exec_env()->buffer_pool();
    RuntimeProfile* profile = RuntimeProfile::Create(&pool_, "ht");

    // Set up memory tracking for the hash table.
    MemTracker* client_tracker =
        pool_.Add(new MemTracker(-1, "client", runtime_state_->instance_mem_tracker()));
    int64_t initial_reservation_bytes = block_size * initial_reserved_blocks;
    int64_t max_reservation_bytes = block_size * max_num_blocks;

    // Set up the memory allocator.
    BufferPool::ClientHandle* client = pool_.Add(new BufferPool::ClientHandle);
    clients_.push_back(client);
    EXPECT_OK(buffer_pool->RegisterClient("", nullptr,
        runtime_state_->instance_buffer_reservation(), client_tracker,
        max_reservation_bytes, profile, client));
    EXPECT_TRUE(client->IncreaseReservation(initial_reservation_bytes));
    Suballocator* allocator =
        pool_.Add(new Suballocator(buffer_pool, client, suballocator_buffer_len));

    // Initial_num_buckets must be a power of two.
    EXPECT_EQ(initial_num_buckets, BitUtil::RoundUpToPowerOfTwo(initial_num_buckets));
    int64_t max_num_buckets = 1L << 31;
    *table = pool_.Add(new HashTable(
        quadratic, allocator, true, 1, nullptr, max_num_buckets, initial_num_buckets));
    hash_tables_.push_back(*table);
    bool success;
    Status status = (*table)->Init(&success);
    EXPECT_OK(status);
    return status.ok() && success;
  }

  // Constructs and closes a hash table.
  void SetupTest(bool quadratic, int64_t initial_num_buckets, bool too_big) {
    TupleRow* build_row1 = CreateTupleRow(1);
    TupleRow* build_row2 = CreateTupleRow(2);
    TupleRow* probe_row3 = CreateTupleRow(3);
    TupleRow* probe_row4 = CreateTupleRow(4);

    int32_t* val_row1 =
        reinterpret_cast<int32_t*>(build_expr_evals_[0]->GetValue(build_row1));
    EXPECT_EQ(*val_row1, 1);
    int32_t* val_row2 =
        reinterpret_cast<int32_t*>(build_expr_evals_[0]->GetValue(build_row2));
    EXPECT_EQ(*val_row2, 2);
    int32_t* val_row3 =
        reinterpret_cast<int32_t*>(probe_expr_evals_[0]->GetValue(probe_row3));
    EXPECT_EQ(*val_row3, 3);
    int32_t* val_row4 =
        reinterpret_cast<int32_t*>(probe_expr_evals_[0]->GetValue(probe_row4));
    EXPECT_EQ(*val_row4, 4);

    // Create and close the hash table.
    HashTable* hash_table;
    bool initialized = CreateHashTable(quadratic, initial_num_buckets, &hash_table);
    EXPECT_EQ(too_big, !initialized);
    if (initialized && initial_num_buckets > 0) {
      EXPECT_NE(hash_table->ByteSize(), 0);
    }
  }

  // IMPALA-2897: Build rows that are equivalent (where nullptrs are counted as equivalent)
  // should not occupy distinct buckets.
  void NullBuildRowTest() {
    TupleRow* build_rows[2];
    for (int i = 0; i < 2; ++i) build_rows[i] = CreateNullTupleRow();

    // Create the hash table and insert the build rows
    HashTable* hash_table;
    ASSERT_TRUE(CreateHashTable(true, 1024, &hash_table));
    scoped_ptr<HashTableCtx> ht_ctx;
    EXPECT_OK(HashTableCtx::Create(&pool_, runtime_state_,
        build_exprs_, probe_exprs_, true /* stores_nulls_ */,
        vector<bool>(build_exprs_.size(), false), 1, 0, 1, &mem_pool_, &mem_pool_,
        &mem_pool_, &ht_ctx));
    EXPECT_OK(ht_ctx->Open(runtime_state_));

    for (int i = 0; i < 2; ++i) {
      if (!ht_ctx->EvalAndHashBuild(build_rows[i])) continue;
      BufferedTupleStream::FlatRowPtr dummy_flat_row = nullptr;
      EXPECT_TRUE(hash_table->stores_tuples_);
      Status status;
      bool inserted =
          hash_table->Insert(ht_ctx.get(), dummy_flat_row, build_rows[i], &status);
      EXPECT_TRUE(inserted);
      ASSERT_OK(status);
    }
    EXPECT_EQ(hash_table->num_buckets() - hash_table->EmptyBuckets(), 1);
    ht_ctx->Close(runtime_state_);
  }

  // This test inserts the build rows [0->5) to hash table. It validates that they
  // are all there using a full table scan. It also validates that Find() is correct
  // testing for probe rows that are both there and not.
  // The hash table is resized a few times and the scans/finds are tested again.
  void BasicTest(bool quadratic, int initial_num_buckets) {
    TupleRow* build_rows[5];
    TupleRow* scan_rows[5] = {0};
    for (int i = 0; i < 5; ++i) build_rows[i] = CreateTupleRow(i);

    ProbeTestData probe_rows[10];
    for (int i = 0; i < 10; ++i) {
      probe_rows[i].probe_row = CreateTupleRow(i);
      if (i < 5) probe_rows[i].expected_build_rows.push_back(build_rows[i]);
    }

    // Create the hash table and insert the build rows
    HashTable* hash_table;
    ASSERT_TRUE(CreateHashTable(quadratic, initial_num_buckets, &hash_table));
    scoped_ptr<HashTableCtx> ht_ctx;
    Status status = HashTableCtx::Create(&pool_, runtime_state_, build_exprs_,
        probe_exprs_, false /* !stores_nulls_ */,
        vector<bool>(build_exprs_.size(), false), 1, 0, 1, &mem_pool_, &mem_pool_,
        &mem_pool_, &ht_ctx);
    EXPECT_OK(status);
    EXPECT_OK(ht_ctx->Open(runtime_state_));
    bool success;
    EXPECT_OK(hash_table->CheckAndResize(5, ht_ctx.get(), &success));
    ASSERT_TRUE(success);
    for (int i = 0; i < 5; ++i) {
      if (!ht_ctx->EvalAndHashBuild(build_rows[i])) continue;
      BufferedTupleStream::FlatRowPtr dummy_flat_row = nullptr;
      EXPECT_TRUE(hash_table->stores_tuples_);
      bool inserted =
          hash_table->Insert(ht_ctx.get(), dummy_flat_row, build_rows[i], &status);
      EXPECT_TRUE(inserted);
      ASSERT_OK(status);
    }
    EXPECT_EQ(hash_table->size(), 5);

    // Do a full table scan and validate returned pointers
    FullScan(hash_table, ht_ctx.get(), 0, 5, true, scan_rows, build_rows);
    ProbeTest(hash_table, ht_ctx.get(), probe_rows, 10, false);

    // Double the size of the hash table and scan again.
    EXPECT_OK(ResizeTable(hash_table, 2048, ht_ctx.get(), &success));
    EXPECT_TRUE(success);
    EXPECT_EQ(hash_table->num_buckets(), 2048);
    EXPECT_EQ(hash_table->size(), 5);
    memset(scan_rows, 0, sizeof(scan_rows));
    FullScan(hash_table, ht_ctx.get(), 0, 5, true, scan_rows, build_rows);
    ProbeTest(hash_table, ht_ctx.get(), probe_rows, 10, false);

    // Try to shrink and scan again.
    EXPECT_OK(ResizeTable(hash_table, 64, ht_ctx.get(), &success));
    EXPECT_TRUE(success);
    EXPECT_EQ(hash_table->num_buckets(), 64);
    EXPECT_EQ(hash_table->size(), 5);
    memset(scan_rows, 0, sizeof(scan_rows));
    FullScan(hash_table, ht_ctx.get(), 0, 5, true, scan_rows, build_rows);
    ProbeTest(hash_table, ht_ctx.get(), probe_rows, 10, false);

    // Resize to 8, which is the smallest value to fit the number of filled buckets.
    EXPECT_OK(ResizeTable(hash_table, 8, ht_ctx.get(), &success));
    EXPECT_TRUE(success);
    EXPECT_EQ(hash_table->num_buckets(), 8);
    EXPECT_EQ(hash_table->size(), 5);
    memset(scan_rows, 0, sizeof(scan_rows));
    FullScan(hash_table, ht_ctx.get(), 0, 5, true, scan_rows, build_rows);
    ProbeTest(hash_table, ht_ctx.get(), probe_rows, 10, false);

    ht_ctx->Close(runtime_state_);
  }

  void ScanTest(
      bool quadratic, int initial_size, int rows_to_insert, int additional_rows) {
    HashTable* hash_table;
    ASSERT_TRUE(CreateHashTable(quadratic, initial_size, &hash_table));

    int total_rows = rows_to_insert + additional_rows;
    scoped_ptr<HashTableCtx> ht_ctx;
    Status status = HashTableCtx::Create(&pool_, runtime_state_, build_exprs_,
        probe_exprs_, false /* !stores_nulls_ */,
        vector<bool>(build_exprs_.size(), false), 1, 0, 1, &mem_pool_, &mem_pool_,
        &mem_pool_, &ht_ctx);
    EXPECT_OK(status);
    EXPECT_OK(ht_ctx->Open(runtime_state_));

    // Add 1 row with val 1, 2 with val 2, etc.
    bool success;
    vector<TupleRow*> build_rows;
    ProbeTestData* probe_rows = new ProbeTestData[total_rows];
    probe_rows[0].probe_row = CreateTupleRow(0);
    for (int val = 1; val <= rows_to_insert; ++val) {
      EXPECT_OK(hash_table->CheckAndResize(val, ht_ctx.get(), &success));
      EXPECT_TRUE(success) << " failed to resize: " << val;
      probe_rows[val].probe_row = CreateTupleRow(val);
      for (int i = 0; i < val; ++i) {
        TupleRow* row = CreateTupleRow(val);
        if (!ht_ctx->EvalAndHashBuild(row)) continue;
        BufferedTupleStream::FlatRowPtr dummy_flat_row = nullptr;
        EXPECT_TRUE(hash_table->stores_tuples_);
        ASSERT_TRUE(hash_table->Insert(ht_ctx.get(), dummy_flat_row, row, &status));
        ASSERT_OK(status);
        build_rows.push_back(row);
        probe_rows[val].expected_build_rows.push_back(row);
      }
    }

    // Add some more probe rows that aren't there.
    for (int val = rows_to_insert; val < rows_to_insert + additional_rows; ++val) {
      probe_rows[val].probe_row = CreateTupleRow(val);
    }

    // Test that all the builds were found.
    ProbeTest(hash_table, ht_ctx.get(), probe_rows, total_rows, true);

    // Resize and try again.
    int target_size = BitUtil::RoundUpToPowerOfTwo(2 * total_rows);
    EXPECT_OK(ResizeTable(hash_table, target_size, ht_ctx.get(), &success));
    EXPECT_TRUE(success);
    EXPECT_EQ(hash_table->num_buckets(), target_size);
    ProbeTest(hash_table, ht_ctx.get(), probe_rows, total_rows, true);

    target_size = BitUtil::RoundUpToPowerOfTwo(total_rows + 1);
    EXPECT_OK(ResizeTable(hash_table, target_size, ht_ctx.get(), &success));
    EXPECT_TRUE(success);
    EXPECT_EQ(hash_table->num_buckets(), target_size);
    ProbeTest(hash_table, ht_ctx.get(), probe_rows, total_rows, true);

    delete [] probe_rows;
    ht_ctx->Close(runtime_state_);
  }

  // This test continues adding tuples to the hash table and exercises the resize code
  // paths.
  void GrowTableTest(bool quadratic) {
    uint64_t num_to_add = 4;
    int expected_size = 0;

    // Need enough memory for two hash table bucket directories during resize.
    const int64_t mem_limit_mb = 128 + 64;
    HashTable* hash_table;
    ASSERT_TRUE(
        CreateHashTable(quadratic, num_to_add, &hash_table, 1024 * 1024, mem_limit_mb));
    scoped_ptr<HashTableCtx> ht_ctx;
    Status status = HashTableCtx::Create(&pool_, runtime_state_, build_exprs_,
        probe_exprs_, false /* !stores_nulls_ */,
        vector<bool>(build_exprs_.size(), false), 1, 0, 1, &mem_pool_, &mem_pool_,
        &mem_pool_, &ht_ctx);
    EXPECT_OK(status);

    // Inserts num_to_add + (num_to_add^2) + (num_to_add^4) + ... + (num_to_add^20)
    // entries. When num_to_add == 4, then the total number of inserts is 4194300.
    int build_row_val = 0;
    for (int i = 0; i < 20; ++i) {
      bool success;
      EXPECT_OK(hash_table->CheckAndResize(num_to_add, ht_ctx.get(), &success));
      EXPECT_TRUE(success) << " failed to resize: " << num_to_add << "\n"
                           << tracker_.LogUsage(MemTracker::UNLIMITED_DEPTH) << "\n"
                           << clients_.back()->DebugString();
      for (int j = 0; j < num_to_add; ++build_row_val, ++j) {
        TupleRow* row = CreateTupleRow(build_row_val);
        if (!ht_ctx->EvalAndHashBuild(row)) continue;
        BufferedTupleStream::FlatRowPtr dummy_flat_row = nullptr;
        EXPECT_TRUE(hash_table->stores_tuples_);
        bool inserted = hash_table->Insert(ht_ctx.get(), dummy_flat_row, row, &status);
        ASSERT_OK(status);
        if (!inserted) goto done_inserting;
      }
      expected_size += num_to_add;
      num_to_add *= 2;
    }
  done_inserting:
    EXPECT_EQ(hash_table->size(), 4194300);

    // The next allocation should put us over the limit, since we'll need 128MB for
    // the old buckets and 256MB for the new buckets.
    bool success;
    EXPECT_OK(hash_table->CheckAndResize(num_to_add * 2, ht_ctx.get(), &success));
    EXPECT_FALSE(success);

    // Validate that we can find the entries before we went over the limit
    for (int i = 0; i < expected_size * 5; i += 100000) {
      TupleRow* probe_row = CreateTupleRow(i);
      if (!ht_ctx->EvalAndHashProbe(probe_row)) continue;
      HashTable::Iterator iter = hash_table->FindProbeRow(ht_ctx.get());
      if (i < hash_table->size()) {
        EXPECT_TRUE(!iter.AtEnd()) << " i: " << i;
        ValidateMatch(probe_row, iter.GetRow());
      } else {
        EXPECT_TRUE(iter.AtEnd()) << " i: " << i;
      }
    }

    // Insert duplicates to also hit OOM.
    int64_t num_duplicates_inserted = 0;
    const int DUPLICATE_VAL = 1234;
    while (true) {
      TupleRow* duplicate_row = CreateTupleRow(DUPLICATE_VAL);
      if (!ht_ctx->EvalAndHashBuild(duplicate_row)) continue;
      BufferedTupleStream::FlatRowPtr dummy_flat_row = nullptr;
      bool inserted =
          hash_table->Insert(ht_ctx.get(), dummy_flat_row, duplicate_row, &status);
      ASSERT_OK(status);
      if (!inserted) break;
      ++num_duplicates_inserted;
    }

    // Check that the duplicates that we successfully inserted are all present.
    TupleRow* duplicate_row = CreateTupleRow(DUPLICATE_VAL);
    ASSERT_TRUE(ht_ctx->EvalAndHashProbe(duplicate_row));
    HashTable::Iterator iter = hash_table->FindProbeRow(ht_ctx.get());
    ValidateMatch(duplicate_row, iter.GetRow());
    for (int64_t i = 0; i < num_duplicates_inserted; ++i) {
      ASSERT_FALSE(iter.AtEnd());
      iter.NextDuplicate();
      ValidateMatch(duplicate_row, iter.GetRow());
    }
    iter.NextDuplicate();
    EXPECT_TRUE(iter.AtEnd());

    ht_ctx->Close(runtime_state_);
  }

  // This test inserts and probes as many elements as the size of the hash table without
  // calling resize. All the inserts and probes are expected to succeed, because there is
  // enough space in the hash table (it is also expected to be slow). It also expects that
  // a probe for a N+1 element will return BUCKET_NOT_FOUND.
  void InsertFullTest(bool quadratic, int table_size) {
    HashTable* hash_table;
    ASSERT_TRUE(CreateHashTable(quadratic, table_size, &hash_table));
    EXPECT_EQ(hash_table->EmptyBuckets(), table_size);
    scoped_ptr<HashTableCtx> ht_ctx;
    Status status = HashTableCtx::Create(&pool_, runtime_state_, build_exprs_,
        probe_exprs_, false /* !stores_nulls_ */,
        vector<bool>(build_exprs_.size(), false), 1, 0, 1, &mem_pool_,
        &mem_pool_, &mem_pool_, &ht_ctx);
    EXPECT_OK(status);

    // Insert and probe table_size different tuples. All of them are expected to be
    // successfully inserted and probed.
    uint32_t hash = 0;
    HashTable::Iterator iter;
    bool found;
    for (int build_row_val = 0; build_row_val < table_size; ++build_row_val) {
      TupleRow* row = CreateTupleRow(build_row_val);
      bool passes = ht_ctx->EvalAndHashBuild(row);
      hash = ht_ctx->expr_values_cache()->CurExprValuesHash();
      EXPECT_TRUE(passes);

      // Insert using both Insert() and FindBucket() methods.
      if (build_row_val % 2 == 0) {
        BufferedTupleStream::FlatRowPtr dummy_flat_row = nullptr;
        EXPECT_TRUE(hash_table->stores_tuples_);
        bool inserted = hash_table->Insert(ht_ctx.get(), dummy_flat_row, row, &status);
        EXPECT_TRUE(inserted);
        ASSERT_OK(status);
      } else {
        iter = hash_table->FindBuildRowBucket(ht_ctx.get(), &found);
        EXPECT_FALSE(iter.AtEnd());
        EXPECT_FALSE(found);
        iter.SetTuple(row->GetTuple(0), hash);
      }
      EXPECT_EQ(hash_table->EmptyBuckets(), table_size - build_row_val - 1);

      passes = ht_ctx->EvalAndHashProbe(row);
      (void)ht_ctx->expr_values_cache()->CurExprValuesHash();
      EXPECT_TRUE(passes);
      iter = hash_table->FindProbeRow(ht_ctx.get());
      EXPECT_FALSE(iter.AtEnd());
      EXPECT_EQ(row->GetTuple(0), iter.GetTuple());

      iter = hash_table->FindBuildRowBucket(ht_ctx.get(), &found);
      EXPECT_FALSE(iter.AtEnd());
      EXPECT_TRUE(found);
      EXPECT_EQ(row->GetTuple(0), iter.GetTuple());
    }

    // Probe for a tuple that does not exist. This should exercise the probe of a full
    // hash table code path.
    EXPECT_EQ(hash_table->EmptyBuckets(), 0);
    TupleRow* probe_row = CreateTupleRow(table_size);
    bool passes = ht_ctx->EvalAndHashProbe(probe_row);
    EXPECT_TRUE(passes);
    iter = hash_table->FindProbeRow(ht_ctx.get());
    EXPECT_TRUE(iter.AtEnd());

    // Since hash_table is full, FindBucket cannot find an empty bucket, so returns End().
    iter = hash_table->FindBuildRowBucket(ht_ctx.get(), &found);
    EXPECT_TRUE(iter.AtEnd());
    EXPECT_FALSE(found);

    ht_ctx->Close(runtime_state_);
  }

  // This test makes sure we can tolerate the low memory case where we do not have enough
  // memory to allocate the array of buckets for the hash table.
  void VeryLowMemTest(bool quadratic) {
    const int64_t block_size = 2 * 1024;
    const int max_num_blocks = 1;
    const int table_size = 1024;
    CreateTestEnv(block_size, block_size * max_num_blocks);

    HashTable* hash_table;
    ASSERT_FALSE(CreateHashTable(
        quadratic, table_size, &hash_table, block_size, max_num_blocks, 0, 1024));
    scoped_ptr<HashTableCtx> ht_ctx;
    Status status = HashTableCtx::Create(&pool_, runtime_state_, build_exprs_,
        probe_exprs_, false /* !stores_nulls_ */, vector<bool>(build_exprs_.size(), false), 1, 0, 1,
        &mem_pool_, &mem_pool_, &mem_pool_, &ht_ctx);
    EXPECT_OK(status);
    HashTable::Iterator iter = hash_table->Begin(ht_ctx.get());
    EXPECT_TRUE(iter.AtEnd());
    ht_ctx->Close(runtime_state_);
  }
};

TEST_F(HashTableTest, LinearSetupTest) {
  SetupTest(false, 1, false);
  SetupTest(false, 1024, false);
  SetupTest(false, 65536, false);

  // Regression test for IMPALA-2065. Trying to init a hash table with large (>2^31)
  // number of buckets.
  SetupTest(false, 4294967296, true); // 2^32
}

TEST_F(HashTableTest, QuadraticSetupTest) {
  SetupTest(true, 1, false);
  SetupTest(true, 1024, false);
  SetupTest(true, 65536, false);

  // Regression test for IMPALA-2065. Trying to init a hash table with large (>2^31)
  // number of buckets.
  SetupTest(true, 4294967296, true); // 2^32
}

TEST_F(HashTableTest, NullBuildRowTest) {
  NullBuildRowTest();
}

TEST_F(HashTableTest, LinearBasicTest) {
  BasicTest(false, 1);
  BasicTest(false, 1024);
  BasicTest(false, 65536);
}

TEST_F(HashTableTest, QuadraticBasicTest) {
  BasicTest(true, 1);
  BasicTest(true, 1024);
  BasicTest(true, 65536);
}

// This test makes sure we can scan ranges of buckets.
TEST_F(HashTableTest, LinearScanTest) {
  ScanTest(false, 1, 10, 5);
  ScanTest(false, 1024, 1000, 5);
  ScanTest(false, 1024, 1000, 500);
}

TEST_F(HashTableTest, QuadraticScanTest) {
  ScanTest(true, 1, 10, 5);
  ScanTest(true, 1024, 1000, 5);
  ScanTest(true, 1024, 1000, 500);
}

TEST_F(HashTableTest, LinearGrowTableTest) {
  GrowTableTest(false);
}

TEST_F(HashTableTest, QuadraticGrowTableTest) {
  GrowTableTest(true);
}

TEST_F(HashTableTest, LinearInsertFullTest) {
  InsertFullTest(false, 1);
  InsertFullTest(false, 4);
  InsertFullTest(false, 64);
  InsertFullTest(false, 1024);
  InsertFullTest(false, 65536);
}

TEST_F(HashTableTest, QuadraticInsertFullTest) {
  InsertFullTest(true, 1);
  InsertFullTest(true, 4);
  InsertFullTest(true, 64);
  InsertFullTest(true, 1024);
  InsertFullTest(true, 65536);
}

// Test that hashing empty string updates hash value.
TEST_F(HashTableTest, HashEmpty) {
  scoped_ptr<HashTableCtx> ht_ctx;
  Status status = HashTableCtx::Create(&pool_, runtime_state_, build_exprs_,
      probe_exprs_, false /* !stores_nulls_ */,
      vector<bool>(build_exprs_.size(), false), 1, 2, 1, &mem_pool_, &mem_pool_,
      &mem_pool_, &ht_ctx);
  EXPECT_OK(status);
  EXPECT_OK(ht_ctx->Open(runtime_state_));

  uint32_t seed = 9999;
  ht_ctx->set_level(0);
  EXPECT_NE(seed, ht_ctx->Hash(nullptr, 0, seed));
  // TODO: level 0 uses CRC hash, which only swaps bytes around on empty input.
  // EXPECT_NE(seed, ht_ctx->Hash(nullptr, 0, ht_ctx->Hash(nullptr, 0, seed)));
  ht_ctx->set_level(1);
  EXPECT_NE(seed, ht_ctx->Hash(nullptr, 0, seed));
  EXPECT_NE(seed, ht_ctx->Hash(nullptr, 0, ht_ctx->Hash(nullptr, 0, seed)));
  ht_ctx->Close(runtime_state_);
}

TEST_F(HashTableTest, VeryLowMemTest) {
  VeryLowMemTest(true);
  VeryLowMemTest(false);
}

// Test to ensure the bucket size doesn't change accidentally.
// On intentional changes to Bucket Size this test should be changed.
TEST_F(HashTableTest, BucketSize) {
  int bucket_size = HashTable::BUCKET_SIZE;
  EXPECT_EQ(bucket_size, 8);
}
}
