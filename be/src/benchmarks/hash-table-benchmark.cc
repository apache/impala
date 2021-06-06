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
#include "exec/hash-table.inline.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/slot-ref.h"
#include "gtest/gtest.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/runtime-profile-counters.h"

using namespace impala;
using namespace std;

// Sample Benchmark Results:
//
// Machine Info: Intel(R) Core(TM) i7-4770 CPU @ 3.40GHz
// Note: Benchmark name are in format <name>_XX_YY:
// name represents the name of benchmark (probe|build|memory).
// XX represents the number of rows in the dataset.
// YY represents the percentage of unique values in dataset.
// Runtime Benchmark
// -----------------
// 21/06/30 08:44:20 INFO util.JvmPauseMonitor: Starting JVM pause monitor
// Hash Table Build:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                     build_65536_100               4.04     4.35     4.35         1X         1X         1X

//                      build_65536_60               8.27     9.09     9.26         1X         1X         1X

//                      build_65536_20               8.78     9.0     9.3           1X         1X         1X

//                    build_262144_100              0.386    0.407    0.407         1X         1X         1X

//                     build_262144_60              0.316    0.407    0.415         1X         1X         1X

//                     build_262144_20              0.295     0.31    0.316         1X         1X         1X

// Hash Table Probe:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                     probe_65536_100               2.64     2.64     2.69         1X         1X         1X

//                      probe_65536_60                5.1      5.96     6.08        1X         1X         1X

//                      probe_65536_20               6.2      6.35      6.35        1X         1X         1X

//                    probe_262144_100              0.233    0.237    0.237         1X         1X         1X

//                     probe_262144_60              0.237     0.25     0.25         1X         1X         1X

//                     probe_262144_20              0.255    0.259    0.259         1X         1X         1X

//              probe_65536_absentkeys               20.1     21.6     21.6         1X         1X         1X

//             probe_262144_absentkeys              0.727    0.741    0.741         1X         1X         1X

// Memory Benchmark
// ----------------
// Hash Table Memory Consumption:            Function      Bytes Consumed
// ----------------------------------------------------------------------
//                                 memory_1048576_100            12582912
//                                  memory_1048576_60            16777212
//                                  memory_1048576_20            20971512
//                                 memory_4194304_100            50331648
//                                  memory_4194304_60            67108868
//                                  memory_4194304_20            83886088

namespace htbenchmark {

class TestCtx {
 public:
  TestCtx() : mem_pool_(&tracker_) {}
  /// Temporary runtime environment for the hash table.
  unique_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_ = nullptr;

  /// Data for HashTable
  vector<TupleRow*> data;
  /// Hash tables and associated clients - automatically closed in TearDown().
  vector<BufferPool::ClientHandle*> clients_;
  HashTable* hash_table_;
  boost::scoped_ptr<HashTableCtx> hash_context_;
  vector<ScalarExpr*> build_exprs_;
  vector<ScalarExpr*> probe_exprs_;
  ObjectPool pool_;
  /// A dummy MemTracker used for exprs and other things we don't need to have limits on.
  MemTracker tracker_;
  MemPool mem_pool_;
  int initial_num_buckets;
  void SetUp(int num_buckets) {
    CreateTestEnv();
    initial_num_buckets = num_buckets;
    bool ht_success = CreateHashTable();
    CHECK(ht_success) << "Creation of HashTable failed";
    RowDescriptor rd;
    ScalarExpr* build_expr = pool_.Add(new SlotRef(ColumnType(TYPE_INT), 1, false));
    Status status = ((SlotRef*)build_expr)->Init(rd, true, nullptr);
    CHECK(status.ok());
    build_exprs_.push_back(build_expr);
    ScalarExpr* probe_expr = pool_.Add(new SlotRef(ColumnType(TYPE_INT), 1, false));
    status = ((SlotRef*)probe_expr)->Init(rd, true, nullptr);
    CHECK(status.ok());
    probe_exprs_.push_back(probe_expr);
    status = HashTableCtx::Create(&pool_, runtime_state_, build_exprs_, probe_exprs_,
        false /* !stores_nulls_ */, vector<bool>(build_exprs_.size(), false), 1, 0, 1,
        &mem_pool_, &mem_pool_, &mem_pool_, &hash_context_);
    CHECK(status.ok());
  }
  void TearDown() {
    ScalarExpr::Close(build_exprs_);
    ScalarExpr::Close(probe_exprs_);
    hash_table_->Close();
    hash_context_->Close(runtime_state_);
    for (BufferPool::ClientHandle* client : clients_) {
      test_env_->exec_env()->buffer_pool()->DeregisterClient(client);
    }
    clients_.clear();
    runtime_state_ = nullptr;
    mem_pool_.FreeAll();
    pool_.Clear();
  }
  /// Initialize test_env_ and runtime_state_ with the given page size and capacity
  /// for the given number of pages. If test_env_ was already created, then re-creates it.
  void CreateTestEnv(int64_t min_page_size = 64 * 1024,
      int64_t buffer_bytes_limit = 4L * 1024 * 1024 * 1024) {
    test_env_.reset(new TestEnv());
    test_env_->SetBufferPoolArgs(min_page_size, buffer_bytes_limit);
    Status status = test_env_->Init();
    CHECK(status.ok());

    TQueryOptions query_options;
    query_options.__set_default_spillable_buffer_size(min_page_size);
    query_options.__set_min_spillable_buffer_size(min_page_size);
    query_options.__set_buffer_pool_limit(buffer_bytes_limit);
    // Also initializes runtime_state_
    status = test_env_->CreateQueryState(0, &query_options, &runtime_state_);
    CHECK(status.ok());
  }
  /// Construct hash table and buffer pool client.
  /// Created objects and resources (e.g. reservations) are automatically freed
  /// in TearDown().
  bool CreateHashTable(int64_t block_size = 8 * 1024 * 1024, int max_num_blocks = 100,
      int initial_reserved_blocks = 10, int64_t suballocator_buffer_len = 64 * 1024) {
    ExecEnv* exec_env = test_env_->exec_env();
    BufferPool* buffer_pool = exec_env->buffer_pool();
    RuntimeProfile* profile = RuntimeProfile::Create(&pool_, "ht");

    // Set up memory tracking for the hash table.
    MemTracker* client_tracker =
        pool_.Add(new MemTracker(-1, "client", runtime_state_->instance_mem_tracker()));
    int64_t initial_reservation_bytes = block_size * initial_reserved_blocks;
    int64_t max_reservation_bytes = block_size * max_num_blocks;

    // Set up the memory allocator.
    BufferPool::ClientHandle* client = pool_.Add(new BufferPool::ClientHandle);
    clients_.push_back(client);
    Status status = buffer_pool->RegisterClient("htbenchmark", nullptr,
        runtime_state_->instance_buffer_reservation(), client_tracker,
        max_reservation_bytes, profile, client);
    if (!status.ok()) {
      std::cout << "Registering client to buffer pool failed" << std::endl;
      return false;
    }
    bool success = client->IncreaseReservation(initial_reservation_bytes);
    if (!success) {
      std::cout << "Client increasing reservation failed" << std::endl;
      return false;
    }
    Suballocator* allocator =
        pool_.Add(new Suballocator(buffer_pool, client, suballocator_buffer_len));

    int64_t max_num_buckets = 1L << 31;
    hash_table_ = pool_.Add(HashTable::Create(
        allocator, true, 1, nullptr, max_num_buckets, initial_num_buckets));
    status = hash_table_->Init(&success);
    if (!(status.ok() && success)) {
      std::cout << "HashTable Init failed" << std::endl;
    }
    return status.ok() && success;
  }
  TupleRow* CreateTupleRow(int32_t val) {
    uint8_t* tuple_row_mem = mem_pool_.Allocate(sizeof(int32_t*));
    Tuple* tuple_mem = Tuple::Create(sizeof(char) + sizeof(int32_t), &mem_pool_);
    *reinterpret_cast<int32_t*>(tuple_mem->GetSlot(1)) = val;
    tuple_mem->SetNotNull(NullIndicatorOffset(0, 1));
    TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    row->SetTuple(0, tuple_mem);
    return row;
  }
  void CreateDataSet(int num_buckets, int unique_percent, int dup = 2) {
    int u_idx = (num_buckets * unique_percent) / 100;
    TupleRow* row;
    for (int i = 1; i <= u_idx; i++) {
      row = CreateTupleRow(i);
      data.push_back(row);
    }
    int count = u_idx;
    for (int j = u_idx + 1; j <= num_buckets && count < num_buckets; j++) {
      for (int i = 0; i < dup && count < num_buckets; i++) {
        row = CreateTupleRow(j);
        data.push_back(row);
        count++;
      }
    }
  }
  void CreateAbsentKeysData(int num_buckets) {
    data.clear();
    TupleRow* row;
    for (int i = 1; i <= num_buckets; i++) {
      row = CreateTupleRow(num_buckets + i);
      data.push_back(row);
    }
  }
};

void Probe(TestCtx* ctx, vector<TupleRow*>& pdata) {
  HashTable* hTable = ctx->hash_table_;
  HashTableCtx* ht_ctx = ctx->hash_context_.get();
  HashTable::Iterator iter;
  for (int i = 0; i < pdata.size(); i++) {
    const TupleRow* row = pdata[i];
    if (!ht_ctx->EvalAndHashProbe(row)) continue;
    iter = hTable->FindProbeRow(ht_ctx);
  }
}

void Build(TestCtx* ctx, vector<TupleRow*>& bdata) {
  HashTable* ht = ctx->hash_table_;
  HashTableCtx* ht_ctx = ctx->hash_context_.get();
  for (int i = 0; i < bdata.size(); i++) {
    TupleRow* row = bdata[i];
    if (!ht_ctx->EvalAndHashBuild(row)) continue;
    BufferedTupleStream::FlatRowPtr dummy_flat_row = nullptr;
    Status status;
    bool success = ht->Insert(ht_ctx, dummy_flat_row, row, &status);
    CHECK(status.ok() && success) << "Inserting a tuple in HashTable failed";
  }
}

namespace build {
void SetUp(void* args) {
  TestCtx* ctx = reinterpret_cast<TestCtx*>(args);
  // Clear old tuples of hash table
  ctx->hash_table_->Close();
  bool got_memory = true;
  Status status = ctx->hash_table_->Init(&got_memory);
  CHECK(status.ok() && got_memory) << "HashTable Reinitialization failed";
}
void Benchmark(int batch_size, void* args) {
  // batch_size is ignored. This is run just once.
  TestCtx* ctx = reinterpret_cast<TestCtx*>(args);
  Build(ctx, ctx->data);
}
}; // namespace build

namespace probe {
void Benchmark(int batch_size, void* args) {
  // batch_size is ignored. This is run just once.
  TestCtx* ctx = reinterpret_cast<TestCtx*>(args);
  Probe(ctx, ctx->data);
}
}; // namespace probe
}; // namespace htbenchmark

using namespace htbenchmark;
std::string ResultString(
    vector<std::string> benchmark_names, vector<int64_t> memory_consumption) {
  stringstream ss;
  int function_out_width = 50;
  int mem_width = 20;
  int total_width = function_out_width + mem_width;
  std::string name("Hash Table Memory Consumption");
  ss << name << ":" << setw(function_out_width - name.size() - 1) << "Function"
     << setw(mem_width) << "Bytes Consumed" << std::endl;
  for (int i = 0; i < total_width; ++i) {
    ss << '-';
  }
  ss << std::endl;
  for (int i = 0; i < benchmark_names.size(); i++) {
    ss << setw(function_out_width) << benchmark_names[i] << setw(mem_width)
       << memory_consumption[i] << std::endl;
  }
  return ss.str();
}

int64_t GetMemoryBytesConsumed(HashTable* ht) {
  int empty_buckets = ht->EmptyBuckets();
  int64_t mem_size = ht->CurrentMemSize();
  return mem_size - (empty_buckets * (HashTable::BUCKET_SIZE + 4));
}

int main(int argc, char** argv) {
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  /// Runtime Benchmark
  std::cout << Benchmark::GetMachineInfo() << std::endl;
  std::cout << "Note: Benchmark name are in format <name>_XX_YY:" << std::endl
            << "name represents the name of benchmark (probe|build|memory)." << std::endl
            << "XX represents the number of rows in the dataset." << std::endl
            << "YY represents the percentage of unique values in dataset." << std::endl;
  std::cout << "Runtime Benchmark" << std::endl;
  std::cout << "-----------------" << std::endl;

  Benchmark hash_table_build("Hash Table Build", false);
  Benchmark hash_table_probe("Hash Table Probe", false);
  vector<int> num_tuples{65536, 262144};
  vector<int> unique_percent{100, 60, 20};
  vector<TestCtx*> ctxs;
  for (int num = 0; num < num_tuples.size(); num++) {
    for (int up = 0; up < unique_percent.size(); up++) {
      std::stringstream pname;
      std::stringstream bname;
      pname << "probe_" << num_tuples[num] << "_" << unique_percent[up];
      bname << "build_" << num_tuples[num] << "_" << unique_percent[up];
      TestCtx* ctx = new TestCtx();
      ctx->SetUp(num_tuples[num]);
      ctxs.push_back(ctx);
      ctx->CreateDataSet(num_tuples[num], unique_percent[up]);
      hash_table_build.AddBenchmark(bname.str(), build::Benchmark, (void*)ctx, -1);
      hash_table_probe.AddBenchmark(pname.str(), probe::Benchmark, (void*)ctx, -1);
    }
  }

  // Create Probe benchmark for Data not found in the table
  for (int num = 0; num < num_tuples.size(); num++) {
    TestCtx* ctx = new TestCtx();
    ctx->SetUp(num_tuples[num]);
    ctxs.push_back(ctx);
    ctx->CreateDataSet(num_tuples[num], 10);
    Build(ctx, ctx->data);
    ctx->CreateAbsentKeysData(num_tuples[num]);
    std::stringstream pname;
    pname << "probe_" << num_tuples[num] << "_absentkeys";
    hash_table_probe.AddBenchmark(pname.str(), probe::Benchmark, (void*)ctx, -1);
  }
  std::cout << hash_table_build.Measure(50, 10, build::SetUp) << std::endl;
  std::cout << hash_table_probe.Measure() << std::endl;
  // Cleanup contexts
  for (TestCtx* ct : ctxs) {
    ct->TearDown();
    free(ct);
  }

  /// Memory Benchmark
  std::cout << "Memory Benchmark" << std::endl;
  std::cout << "----------------" << std::endl;
  vector<std::string> benchmark_names;
  vector<int64_t> memory_consumption;
  num_tuples.clear();
  num_tuples.push_back(1024 * 1024);
  num_tuples.push_back(4 * 1024 * 1024);
  for (int num = 0; num < num_tuples.size(); num++) {
    for (int up = 0; up < unique_percent.size(); up++) {
      std::stringstream bname;
      bname << "memory_" << num_tuples[num] << "_" << unique_percent[up];
      TestCtx ctx;
      ctx.SetUp(num_tuples[num]);
      ctx.CreateDataSet(num_tuples[num], unique_percent[up]);
      Build(&ctx, ctx.data);
      benchmark_names.push_back(bname.str());
      memory_consumption.push_back(GetMemoryBytesConsumed(ctx.hash_table_));
      ctx.TearDown();
    }
  }
  std::cout << ResultString(benchmark_names, memory_consumption) << std::endl;
  return 0;
}