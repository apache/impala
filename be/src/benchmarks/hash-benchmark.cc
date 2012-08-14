// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <vector>

#include <boost/functional/hash.hpp>

#include "codegen/llvm-codegen.h"
#include "experiments/data-provider.h"
#include "runtime/raw-value.h"
#include "runtime/string-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/hash-util.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

// Benchmark tests for hashing tuples.  There are two sets of inputs
// that are benchmarked.  The 'Int' set which consists of hashing tuples
// with 4 int32_t values.  The 'Mixed' set consists of tuples with different
// data types, including strings.  The resulting hashes are put into 1000 
// buckets so the expected number of collisions is roughly ~1/3.
//
// The different hash functions benchmarked:
//   1. FVN Hash: Fowler-Noll-Vo hash function
//   2. Boost Hash: boost hash function
//   3. Crc: hash using sse4 crc hash instruction
//   4. Codegen: hash using sse4 with the tuple types baked into the codegen function
// 
// n is the number of buckets, k is the number of items
// Expected(collisions) = n - k + E(X) 
//                      = n - k + k(1 - 1/k)^n
// For k == n (1000 items into 1000 buckets)
//                      = lim n->inf n(1 - 1/n) ^n
//                      = n / e 
//                      = 367
// Results:
//  FVN Int Rate: 95.6969
//  Boost Int Rate: 175.069
//  Crc Int Rate: 372.465
//  Codegen Int Rate: 886.139
//  
//  FVN Int Collisions: 407
//  Boost Int Collisions: 390
//  Crc Int Collisions: 387
//  Codegen Int Collisions: 387
//  
//  FVN Mixed Rate: 80.3885
//  Boost Mixed Rate: 78.4977
//  Crc Mixed Rate: 270.356
//  Codegen Mixed Rate: 216.901  // TODO: why is this slower???
//  
//  FVN Mixed Collisions: 366
//  Boost Mixed Collisions: 374
//  Crc Mixed Collisions: 376
//  Codegen Mixed Collisions: 376
typedef uint32_t (*CodegenHashFn)(int rows, char* data, int32_t* results);

struct TestData {
  void* data;
  int num_cols;
  int num_rows;
  vector<int32_t> results;
  void* jitted_fn;
};

void TestFvnIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  int cols = data->num_cols;
  for (int i = 0; i < batch; ++i) {
    int32_t* values = reinterpret_cast<int32_t*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FVN_SEED;
      for (int k = 0; k < cols; ++k) {
        hash = HashUtil::FvnHash(&values[k], sizeof(uint32_t), hash);
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
      size_t hash = HashUtil::FVN_SEED;
      for (int k = 0; k < cols; ++k) {
        hash = HashUtil::CrcHash(&values[k], sizeof(uint32_t), hash);
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
      size_t h = HashUtil::FVN_SEED;
      for (int k = 0; k < cols; ++k) {
        size_t hash_value = hash<int32_t>().operator()(values[k]);
        hash_combine(h, hash_value);
      }
      data->results[j] = h;
      values += cols;
    }
  }
}

void TestCodegenIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  CodegenHashFn fn = reinterpret_cast<CodegenHashFn>(data->jitted_fn);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    fn(rows, values, &data->results[0]);
  }
}

void TestFvnMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FVN_SEED;
      
      hash = HashUtil::FvnHash(values, sizeof(int8_t), hash);
      values += sizeof(int8_t);
      
      hash = HashUtil::FvnHash(values, sizeof(int32_t), hash);
      values += sizeof(int32_t);
      
      hash = HashUtil::FvnHash(values, sizeof(int64_t), hash);
      values += sizeof(int64_t);
      
      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash = HashUtil::FvnHash(str->ptr, str->len, hash);
      values += sizeof(StringValue);

      data->results[j] = hash;
    }
  }
}

void TestCrcMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FVN_SEED;
      
      hash = HashUtil::CrcHash(values, sizeof(int8_t), hash);
      values += sizeof(int8_t);
      
      hash = HashUtil::CrcHash(values, sizeof(int32_t), hash);
      values += sizeof(int32_t);
      
      hash = HashUtil::CrcHash(values, sizeof(int64_t), hash);
      values += sizeof(int64_t);
      
      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash = HashUtil::CrcHash(str->ptr, str->len, hash);
      values += sizeof(StringValue);

      data->results[j] = hash;
    }
  }
}

void TestCodegenMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  CodegenHashFn fn = reinterpret_cast<CodegenHashFn>(data->jitted_fn);
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
      size_t h = HashUtil::FVN_SEED;
      
      size_t hash_value = hash<int8_t>().operator()(*reinterpret_cast<int8_t*>(values));
      hash_combine(h, hash_value);
      values += sizeof(int8_t);

      hash_value = hash<int32_t>().operator()(*reinterpret_cast<int32_t*>(values));
      hash_combine(h, hash_value);
      values += sizeof(int32_t);
      
      hash_value = hash<int64_t>().operator()(*reinterpret_cast<int64_t*>(values));
      hash_combine(h, hash_value);
      values += sizeof(int64_t);
      
      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash_value = hash_range<char*>(str->ptr, str->ptr + str->len);
      hash_combine(h, hash_value);
      values += sizeof(StringValue);
      
      data->results[j] = h;
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
Function* CodegenCrcHash(LlvmCodeGen* codegen, bool mixed) {
  string name = mixed ? "HashMixed" : "HashInt";
  LlvmCodeGen::FnPrototype prototype(codegen, name, codegen->void_type());
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("rows", codegen->GetType(TYPE_INT)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("data", codegen->ptr_type()));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("results", codegen->GetPtrType(TYPE_INT)));
  
  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  BasicBlock* loop_start = builder.GetInsertBlock();
  BasicBlock* loop_body = BasicBlock::Create(codegen->context(), "loop", fn);
  BasicBlock* loop_exit = BasicBlock::Create(codegen->context(), "exit", fn);

  int fixed_byte_size = mixed ? 
    sizeof(int8_t) + sizeof(int32_t) + sizeof(int64_t) : sizeof(int32_t) * 4;

  Function* fixed_fn = codegen->GetHashFunction(fixed_byte_size);
  Function* string_hash_fn = codegen->GetHashFunction();

  Value* row_size = NULL;
  if (mixed) {
    row_size = codegen->GetIntConstant(TYPE_INT, 
      sizeof(int8_t) + sizeof(int32_t) + sizeof(int64_t) + sizeof(StringValue));
  } else {
    row_size = codegen->GetIntConstant(TYPE_INT, fixed_byte_size);
  }
  Value* dummy_len = codegen->GetIntConstant(TYPE_INT, 0);

  // Check loop counter
  Value* counter_check = 
      builder.CreateICmpSGT(args[0], codegen->GetIntConstant(TYPE_INT, 0));
  builder.CreateCondBr(counter_check, loop_body, loop_exit);
  
  // Loop body
  builder.SetInsertPoint(loop_body);
  PHINode* counter = builder.CreatePHI(codegen->GetType(TYPE_INT), 2, "counter");
  counter->addIncoming(codegen->GetIntConstant(TYPE_INT, 0), loop_start);

  Value* next_counter = builder.CreateAdd(counter, codegen->GetIntConstant(TYPE_INT, 1));
  counter->addIncoming(next_counter, loop_body);

  // Hash the current data
  Value* offset = builder.CreateMul(counter, row_size);
  Value* data = builder.CreateGEP(args[1], offset);
  
  Value* seed = codegen->GetIntConstant(TYPE_INT, HashUtil::FVN_SEED);
  seed = builder.CreateCall3(fixed_fn, data, dummy_len, seed);

  // Get the string data
  if (mixed) {
    Value* string_data = builder.CreateGEP(
        data, codegen->GetIntConstant(TYPE_INT, fixed_byte_size));
    Value* string_val = 
        builder.CreateBitCast(string_data, codegen->GetPtrType(TYPE_STRING));
    Value* str_ptr = builder.CreateStructGEP(string_val, 0);
    Value* str_len = builder.CreateStructGEP(string_val, 1);
    str_ptr = builder.CreateLoad(str_ptr);
    str_len = builder.CreateLoad(str_len);
    seed = builder.CreateCall3(string_hash_fn, str_ptr, str_len, seed);
  }

  Value* result = builder.CreateGEP(args[2], counter);
  builder.CreateStore(seed, result);

  counter_check = builder.CreateICmpSLT(counter, args[0]);
  builder.CreateCondBr(counter_check, loop_body, loop_exit);

  // Loop exit
  builder.SetInsertPoint(loop_exit);
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(fn);
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  LlvmCodeGen::InitializeLlvm();

  const int NUM_ROWS = 1024;

  ObjectPool obj_pool;
  MemPool mem_pool;
  RuntimeProfile int_profile(&obj_pool, "IntGen");
  RuntimeProfile mixed_profile(&obj_pool, "MixedGen");
  DataProvider int_provider(&mem_pool, &int_profile);
  DataProvider mixed_provider(&mem_pool, &mixed_profile);

  Status status;
  scoped_ptr<LlvmCodeGen> codegen;
  status = LlvmCodeGen::LoadImpalaIR(&obj_pool, &codegen);
  if (!status.ok()) {
    cout << "Could not start codegen.";
    return -1;
  }
  codegen->EnableOptimizations(true);

  Function* hash_ints = CodegenCrcHash(codegen.get(), false);
  void* jitted_hash_ints = codegen->JitFunction(hash_ints);

  Function* hash_mixed = CodegenCrcHash(codegen.get(), true);
  void* jitted_hash_mixed = codegen->JitFunction(hash_mixed);

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
  int_data.jitted_fn = jitted_hash_ints;

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
  mixed_data.jitted_fn = jitted_hash_mixed;

  TestFvnIntHash(10, &int_data);  // warm-up run
  double fvn_int_rate = Benchmark::Measure(TestFvnIntHash, &int_data);
  int fvn_int_collisions = NumCollisions(&int_data, NUM_ROWS);
  
  double boost_int_rate = Benchmark::Measure(TestBoostIntHash, &int_data);
  int boost_int_collisions = NumCollisions(&int_data, NUM_ROWS);
  
  double crc_int_rate = Benchmark::Measure(TestCrcIntHash, &int_data);
  int crc_int_collisions = NumCollisions(&int_data, NUM_ROWS);
  
  double codegen_int_rate = Benchmark::Measure(TestCodegenIntHash, &int_data);
  int codegen_int_collisions = NumCollisions(&int_data, NUM_ROWS);
  
  TestFvnMixedHash(10, &mixed_data);  // warm-up run
  double fvn_mixed_rate = Benchmark::Measure(TestFvnMixedHash, &mixed_data);
  int fvn_mixed_collisions = NumCollisions(&mixed_data, NUM_ROWS);
  
  double boost_mixed_rate = Benchmark::Measure(TestBoostMixedHash, &mixed_data);
  int boost_mixed_collisions = NumCollisions(&mixed_data, NUM_ROWS);
  
  double crc_mixed_rate = Benchmark::Measure(TestCrcMixedHash, &mixed_data);
  int crc_mixed_collisions = NumCollisions(&mixed_data, NUM_ROWS);
  
  double codegen_mixed_rate = Benchmark::Measure(TestCodegenMixedHash, &mixed_data);
  int codegen_mixed_collisions = NumCollisions(&mixed_data, NUM_ROWS);

  cout << "FVN Int Rate: " << fvn_int_rate << endl;
  cout << "Boost Int Rate: " << boost_int_rate << endl;
  cout << "Crc Int Rate: " << crc_int_rate << endl;
  cout << "Codegen Int Rate: " << codegen_int_rate << endl;
  cout << endl;
  
  cout << "FVN Int Collisions: " << fvn_int_collisions << endl;
  cout << "Boost Int Collisions: " << boost_int_collisions << endl;
  cout << "Crc Int Collisions: " << crc_int_collisions << endl;
  cout << "Codegen Int Collisions: " << codegen_int_collisions << endl;
  cout << endl;

  cout << "FVN Mixed Rate: " << fvn_mixed_rate << endl;
  cout << "Boost Mixed Rate: " << boost_mixed_rate << endl;
  cout << "Crc Mixed Rate: " << crc_mixed_rate << endl;
  cout << "Codegen Mixed Rate: " << codegen_mixed_rate << endl;
  cout << endl;
  
  cout << "FVN Mixed Collisions: " << fvn_mixed_collisions << endl;
  cout << "Boost Mixed Collisions: " << boost_mixed_collisions << endl;
  cout << "Crc Mixed Collisions: " << crc_mixed_collisions << endl;
  cout << "Codegen Mixed Collisions: " << codegen_mixed_collisions << endl;

  return 0;
}

