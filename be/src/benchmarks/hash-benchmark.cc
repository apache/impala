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

#include <iostream>
#include <vector>

#include <boost/functional/hash.hpp>

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "experiments/data-provider.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "util/benchmark.h"

#include "common/names.h"

using boost::hash_combine;
using boost::hash_range;
using namespace impala;


// Benchmark tests for hashing tuples.  There are two sets of inputs
// that are benchmarked.  The 'Int' set which consists of hashing tuples
// with 4 int32_t values.  The 'Mixed' set consists of tuples with different
// data types, including strings.  The resulting hashes are put into 1000
// buckets so the expected number of collisions is roughly ~1/3.
//
// The different hash functions benchmarked:
//   1. FNV Hash: Fowler-Noll-Vo hash function
//   2. FNV Hash with empty string handling: FNV with special-case for empty string
//   3. FastHash64: 64-bit FastHash function
//   3. Murmur2_64 Hash: Murmur2 hash function
//   4. Boost Hash: boost hash function
//   5. Crc: hash using sse4 crc hash instruction
//   6. Codegen: hash using sse4 with the tuple types baked into the codegen function
//
// n is the number of buckets, k is the number of items
// Expected(collisions) = n - k + E(X)
//                      = n - k + k(1 - 1/k)^n
// For k == n (1000 items into 1000 buckets)
//                      = lim n->inf n(1 - 1/n) ^n
//                      = n / e
//                      = 367
/*
Machine Info:  Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz
Int Hash:
Function     10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                      (relative) (relative) (relative)
----------- ----------------------------------------------------------
        Fnv    88.5      109      111         1X         1X         1X
 FastHash64    96.9      110      112       1.1X      1.01X      1.01X
 Murmur2_64    90.7      124      126      1.03X      1.14X      1.14X
      Boost     203      277      282       2.3X      2.55X      2.55X
        Crc     415      536      540      4.68X      4.93X      4.89X
    Codegen 1.5e+03 1.85e+03 1.88e+03      16.9X        17X        17X
Mixed Hash:
Function  10%ile   50%ile   90%ile        10%ile     50%ile     90%ile
                                      (relative) (relative) (relative)
----------------------------------------------------------------------
        Fnv    75.3       78     78.9         1X         1X         1X
 FastHash64    91.2     95.4     96.3      1.21X      1.22X      1.22X
   FnvEmpty    82.7     86.3     86.9       1.1X      1.11X       1.1X
 Murmur2_64     109      113      114      1.45X      1.45X      1.45X
      Boost     109      112      113      1.45X      1.43X      1.44X
        Crc     467      481      489      6.21X      6.17X      6.21X
    Codegen     292      312      318      3.88X         4X      4.03X
*/

typedef uint32_t (*CodegenHashFn)(int rows, char* data, int32_t* results);

struct TestData {
  void* data;
  int num_cols;
  int num_rows;
  vector<int32_t> results;
  CodegenHashFn jitted_fn;
};

void TestFnvIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  int cols = data->num_cols;
  for (int i = 0; i < batch; ++i) {
    int32_t* values = reinterpret_cast<int32_t*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FNV_SEED;
      for (int k = 0; k < cols; ++k) {
        hash = HashUtil::FnvHash64to32(&values[k], sizeof(uint32_t), hash);
      }
      data->results[j] = hash;
      values += cols;
    }
  }
}

void TestCrcIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  int cols = data->num_cols;
  for (int i = 0; i < batch; ++i) {
    int32_t* values = reinterpret_cast<int32_t*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FNV_SEED;
      for (int k = 0; k < cols; ++k) {
        hash = HashUtil::CrcHash(&values[k], sizeof(uint32_t), hash);
      }
      data->results[j] = hash;
      values += cols;
    }
  }
}

void TestFastHashIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  int cols = data->num_cols;
  for (int i = 0; i < batch; ++i) {
    int32_t* values = reinterpret_cast<int32_t*>(data->data);
    for (int j = 0; j < rows; ++j) {
      uint64_t hash = HashUtil::FNV_SEED;
      for (int k = 0; k < cols; ++k) {
        hash = HashUtil::FastHash64(&values[k], sizeof(uint32_t), hash);
      }
      data->results[j] = hash;
      values += cols;
    }
  }
}

void TestBoostIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  int cols = data->num_cols;
  for (int i = 0; i < batch; ++i) {
    int32_t* values = reinterpret_cast<int32_t*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t h = HashUtil::FNV_SEED;
      for (int k = 0; k < cols; ++k) {
        size_t hash_value = boost::hash<int32_t>().operator()(values[k]);
        hash_combine(h, hash_value);
      }
      data->results[j] = h;
      values += cols;
    }
  }
}

void TestCodegenIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  CodegenHashFn fn = data->jitted_fn;
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    fn(rows, values, &data->results[0]);
  }
}

void TestMurmur2_64IntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  int cols = data->num_cols;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      uint64_t hash = 0;
      for (int k = 0; k < cols; ++k) {
        hash = HashUtil::MurmurHash2_64(&values[k], sizeof(uint32_t), hash);
      }
      data->results[j] = hash;
      values += cols;
    }
  }
}

template <bool handle_empty>
void TestFnvMixedHashTemplate(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FNV_SEED;

      hash = HashUtil::FnvHash64to32(values, sizeof(int8_t), hash);
      values += sizeof(int8_t);

      hash = HashUtil::FnvHash64to32(values, sizeof(int32_t), hash);
      values += sizeof(int32_t);

      hash = HashUtil::FnvHash64to32(values, sizeof(int64_t), hash);
      values += sizeof(int64_t);

      StringValue* str = reinterpret_cast<StringValue*>(values);
      if (handle_empty && str->Len() == 0) {
        hash = HashUtil::HashCombine32(0, hash);
      } else {
        hash = HashUtil::FnvHash64to32(str->Ptr(), str->Len(), hash);
      }
      values += sizeof(StringValue);

      data->results[j] = hash;
    }
  }
}

void TestFnvMixedHash(int batch, void* d) {
  return TestFnvMixedHashTemplate<false>(batch, d);
}

void TestFnvEmptyMixedHash(int batch, void* d) {
  return TestFnvMixedHashTemplate<true>(batch, d);
}

void TestCrcMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FNV_SEED;

      hash = HashUtil::CrcHash(values, sizeof(int8_t), hash);
      values += sizeof(int8_t);

      hash = HashUtil::CrcHash(values, sizeof(int32_t), hash);
      values += sizeof(int32_t);

      hash = HashUtil::CrcHash(values, sizeof(int64_t), hash);
      values += sizeof(int64_t);

      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash = HashUtil::CrcHash(str->Ptr(), str->Len(), hash);
      values += sizeof(StringValue);

      data->results[j] = hash;
    }
  }
}

void TestFastHashMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      uint64_t hash = HashUtil::FNV_SEED;

      hash = HashUtil::FastHash64(values, sizeof(int8_t), hash);
      values += sizeof(int8_t);

      hash = HashUtil::FastHash64(values, sizeof(int32_t), hash);
      values += sizeof(int32_t);

      hash = HashUtil::FastHash64(values, sizeof(int64_t), hash);
      values += sizeof(int64_t);

      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash = HashUtil::FastHash64(str->Ptr(), str->Len(), hash);
      values += sizeof(StringValue);

      data->results[j] = hash;
    }
  }
}

void TestCodegenMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  CodegenHashFn fn = data->jitted_fn;
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    fn(rows, values, &data->results[0]);
  }
}

void TestBoostMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t h = HashUtil::FNV_SEED;

      size_t hash_value = boost::hash<int8_t>().operator()(*reinterpret_cast<int8_t*>(values));
      hash_combine(h, hash_value);
      values += sizeof(int8_t);

      hash_value = boost::hash<int32_t>().operator()(*reinterpret_cast<int32_t*>(values));
      hash_combine(h, hash_value);
      values += sizeof(int32_t);

      hash_value = boost::hash<int64_t>().operator()(*reinterpret_cast<int64_t*>(values));
      hash_combine(h, hash_value);
      values += sizeof(int64_t);

      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash_value = hash_range<char*>(str->Ptr(), str->Ptr() + str->Len());
      hash_combine(h, hash_value);
      values += sizeof(StringValue);

      data->results[j] = h;
    }
  }
}

void TestMurmur2_64MixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      uint64_t hash = 0;

      hash = HashUtil::MurmurHash2_64(values, sizeof(int8_t), hash);
      values += sizeof(int8_t);

      hash = HashUtil::MurmurHash2_64(values, sizeof(int32_t), hash);
      values += sizeof(int32_t);

      hash = HashUtil::MurmurHash2_64(values, sizeof(int64_t), hash);
      values += sizeof(int64_t);

      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash = HashUtil::MurmurHash2_64(str->Ptr(), str->Len(), hash);
      values += sizeof(StringValue);

      data->results[j] = hash;
    }
  }
}

int NumCollisions(TestData* data, int num_buckets) {
  vector<bool> buckets;
  buckets.resize(num_buckets);

  int num_collisions = 0;
  for (int i = 0; i < data->results.size(); ++i) {
    uint32_t hash = data->results[i];
    int bucket = hash % num_buckets;
    if (buckets[bucket]) ++num_collisions;
    buckets[bucket] = true;
  }
  memset(&data->results[0], 0, data->results.size() * sizeof(uint32_t));
  return num_collisions;
}

// Codegen for looping through a batch of tuples
// define void @HashInt(i32 %rows, i8* %data, i32* %results) {
// entry:
//   %0 = icmp sgt i32 %rows, 0
//   br i1 %0, label %loop, label %exit
//
// loop:                                             ; preds = %loop, %entry
//   %counter = phi i32 [ 0, %entry ], [ %1, %loop ]
//   %1 = add i32 %counter, 1
//   %2 = mul i32 %counter, 16
//   %3 = getelementptr i8* %data, i32 %2
//   %4 = call i32 @CrcHash16(i8* %3, i32 0, i32 -2128831035)
//   %5 = getelementptr i32* %results, i32 %counter
//   store i32 %4, i32* %5
//   %6 = icmp slt i32 %counter, %rows
//   br i1 %6, label %loop, label %exit
//
// exit:                                             ; preds = %loop, %entry
//   ret void
// }
llvm::Function* CodegenCrcHash(LlvmCodeGen* codegen, bool mixed) {
  string name = mixed ? "HashMixed" : "HashInt";
  LlvmCodeGen::FnPrototype prototype(codegen, name, codegen->void_type());
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("rows", codegen->i32_type()));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("data", codegen->ptr_type()));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("results", codegen->i32_ptr_type()));

  LlvmBuilder builder(codegen->context());
  llvm::Value* args[3];
  llvm::Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  llvm::BasicBlock* loop_start = builder.GetInsertBlock();
  llvm::BasicBlock* loop_body = llvm::BasicBlock::Create(codegen->context(), "loop", fn);
  llvm::BasicBlock* loop_exit = llvm::BasicBlock::Create(codegen->context(), "exit", fn);

  int fixed_byte_size = mixed ?
    sizeof(int8_t) + sizeof(int32_t) + sizeof(int64_t) : sizeof(int32_t) * 4;

  llvm::Function* fixed_fn = codegen->GetHashFunction(fixed_byte_size);
  llvm::Function* string_hash_fn = codegen->GetHashFunction();

  llvm::Value* row_size = NULL;
  if (mixed) {
    row_size = codegen->GetI32Constant(
        sizeof(int8_t) + sizeof(int32_t) + sizeof(int64_t) + sizeof(StringValue));
  } else {
    row_size = codegen->GetI32Constant(fixed_byte_size);
  }
  llvm::Value* dummy_len = codegen->GetI32Constant(0);

  // Check loop counter
  llvm::Value* counter_check =
      builder.CreateICmpSGT(args[0], codegen->GetI32Constant(0));
  builder.CreateCondBr(counter_check, loop_body, loop_exit);

  // Loop body
  builder.SetInsertPoint(loop_body);
  llvm::PHINode* counter = builder.CreatePHI(codegen->i32_type(), 2, "counter");
  counter->addIncoming(codegen->GetI32Constant(0), loop_start);

  llvm::Value* next_counter =
      builder.CreateAdd(counter, codegen->GetI32Constant(1));
  counter->addIncoming(next_counter, loop_body);

  // Hash the current data
  llvm::Value* offset = builder.CreateMul(counter, row_size);
  llvm::Value* data = builder.CreateGEP(args[1], offset);

  llvm::Value* seed = codegen->GetI32Constant(HashUtil::FNV_SEED);
  seed =
      builder.CreateCall(fixed_fn, llvm::ArrayRef<llvm::Value*>({data, dummy_len, seed}));

  // Get the string data
  if (mixed) {
    llvm::Value* string_data =
        builder.CreateGEP(data, codegen->GetI32Constant(fixed_byte_size));
    llvm::Value* string_val = builder.CreateBitCast(string_data,
            codegen->GetSlotPtrType(ColumnType(TYPE_STRING)));

    llvm::Function* str_ptr_fn = codegen->GetFunction(
        IRFunction::STRING_VALUE_PTR, false);
    llvm::Function* str_len_fn = codegen->GetFunction(
        IRFunction::STRING_VALUE_LEN, false);

    llvm::Value* str_ptr = builder.CreateCall(str_ptr_fn,
        llvm::ArrayRef<llvm::Value*>({string_val}), "ptr");
    llvm::Value* str_len = builder.CreateCall(str_len_fn,
        llvm::ArrayRef<llvm::Value*>({string_val}), "len");

    str_ptr = builder.CreateLoad(str_ptr);
    str_len = builder.CreateLoad(str_len);
    seed = builder.CreateCall(
        string_hash_fn, llvm::ArrayRef<llvm::Value*>({str_ptr, str_len, seed}));
  }

  llvm::Value* result = builder.CreateGEP(args[2], counter);
  builder.CreateStore(seed, result);

  counter_check = builder.CreateICmpSLT(next_counter, args[0]);
  builder.CreateCondBr(counter_check, loop_body, loop_exit);

  // Loop exit
  builder.SetInsertPoint(loop_exit);
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(fn);
}

int main(int argc, char **argv) {
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  cout << Benchmark::GetMachineInfo() << endl;
  impala::InitFeSupport();
  ABORT_IF_ERROR(LlvmCodeGen::InitializeLlvm());

  const int NUM_ROWS = 1024;

  Status status;
  RuntimeState* state;
  TestEnv test_env;
  status = test_env.Init();
  if (!status.ok()) {
    cout << "Could not init TestEnv";
    return -1;
  }
  status = test_env.CreateQueryState(0, nullptr, &state);
  QueryState* qs = state->query_state();
  TPlanFragment* fragment = qs->obj_pool()->Add(new TPlanFragment());
  PlanFragmentCtxPB* fragment_ctx = qs->obj_pool()->Add(new PlanFragmentCtxPB());
  FragmentState* fragment_state =
      qs->obj_pool()->Add(new FragmentState(qs, *fragment, *fragment_ctx));
  if (!status.ok()) {
    cout << "Could not create RuntimeState";
    return -1;
  }

  MemTracker tracker;
  MemPool mem_pool(&tracker);
  RuntimeProfile* int_profile = RuntimeProfile::Create(state->obj_pool(), "IntGen");
  RuntimeProfile* mixed_profile = RuntimeProfile::Create(state->obj_pool(), "MixedGen");
  DataProvider int_provider(&mem_pool, int_profile);
  DataProvider mixed_provider(&mem_pool, mixed_profile);

  scoped_ptr<LlvmCodeGen> codegen;
  status = LlvmCodeGen::CreateImpalaCodegen(fragment_state, NULL, "test", &codegen);
  if (!status.ok()) {
    cout << "Could not start codegen.";
    return -1;
  }
  codegen->EnableOptimizations(true);

  llvm::Function* hash_ints = CodegenCrcHash(codegen.get(), false);
  CodegenFnPtr<CodegenHashFn> jitted_hash_ints;
  codegen->AddFunctionToJit(hash_ints, &jitted_hash_ints);

  llvm::Function* hash_mixed = CodegenCrcHash(codegen.get(), true);
  CodegenFnPtr<CodegenHashFn> jitted_hash_mixed;
  codegen->AddFunctionToJit(hash_mixed, &jitted_hash_mixed);

  status = codegen->FinalizeModule();
  if (!status.ok()) {
    cout << "Could not compile module: " << status.GetDetail();
    return -1;
  }

  // Test a tuple consisting of just 4 int cols.  This is representative of
  // group by queries.  The test is hardcoded to know it will only be int cols
  vector<DataProvider::ColDesc> int_cols;
  int_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 10000));
  int_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 1000000));
  int_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 1000000));
  int_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 1000000));

  int_provider.Reset(NUM_ROWS, NUM_ROWS, int_cols);

  TestData int_data;
  int_data.data = int_provider.NextBatch(&int_data.num_rows);
  int_data.num_cols = int_cols.size();
  int_data.results.resize(int_data.num_rows);
  int_data.jitted_fn = jitted_hash_ints.load();

  // Some mixed col types.  The test hash function will know the exact
  // layout.  This is reasonable to do since we can use llvm
  string min_std_str("aaa");
  string max_std_str("zzzzzzzzzz");

  StringValue min_str(const_cast<char*>(min_std_str.c_str()), min_std_str.size());
  StringValue max_str(const_cast<char*>(max_std_str.c_str()), max_std_str.size());

  vector<DataProvider::ColDesc> mixed_cols;
  mixed_cols.push_back(DataProvider::ColDesc::Create<int8_t>(0, 25));
  mixed_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 10000));
  mixed_cols.push_back(DataProvider::ColDesc::Create<int64_t>(0, 100000000));
  mixed_cols.push_back(DataProvider::ColDesc::Create<StringValue>(min_str, max_str));
  mixed_provider.Reset(NUM_ROWS, NUM_ROWS, mixed_cols);

  TestData mixed_data;
  mixed_data.data = mixed_provider.NextBatch(&mixed_data.num_rows);
  mixed_data.num_cols = mixed_cols.size();
  mixed_data.results.resize(mixed_data.num_rows);
  mixed_data.jitted_fn = jitted_hash_mixed.load();

  Benchmark int_suite("Int Hash");
  int_suite.AddBenchmark("Fnv", TestFnvIntHash, &int_data);
  int_suite.AddBenchmark("FastHash64", TestFastHashIntHash, &int_data);
  int_suite.AddBenchmark("Murmur2_64", TestMurmur2_64IntHash, &int_data);
  int_suite.AddBenchmark("Boost", TestBoostIntHash, &int_data);
  int_suite.AddBenchmark("Crc", TestCrcIntHash, &int_data);
  int_suite.AddBenchmark("Codegen", TestCodegenIntHash, &int_data);
  cout << int_suite.Measure() << endl;

  Benchmark mixed_suite("Mixed Hash");
  mixed_suite.AddBenchmark("Fnv", TestFnvMixedHash, &mixed_data);
  mixed_suite.AddBenchmark("FastHash64", TestFastHashMixedHash, &mixed_data);
  mixed_suite.AddBenchmark("FnvEmpty", TestFnvEmptyMixedHash, &mixed_data);
  mixed_suite.AddBenchmark("Murmur2_64", TestMurmur2_64MixedHash, &mixed_data);
  mixed_suite.AddBenchmark("Boost", TestBoostMixedHash, &mixed_data);
  mixed_suite.AddBenchmark("Crc", TestCrcMixedHash, &mixed_data);
  mixed_suite.AddBenchmark("Codegen", TestCodegenMixedHash, &mixed_data);
  cout << mixed_suite.Measure();

  codegen->Close();
  mem_pool.FreeAll();
  return 0;
}
