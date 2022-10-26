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

#include "codegen/llvm-codegen-cache.h"
#include <boost/thread/thread.hpp>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "codegen/mcjit-mem-mgr.h"
#include "common/object-pool.h"
#include "runtime/fragment-state.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"

using namespace std;
using boost::scoped_ptr;
using boost::thread;
using boost::thread_group;

DECLARE_bool(cache_force_single_shard);
DECLARE_string(codegen_cache_capacity);

namespace impala {
class LlvmCodeGenCacheTest : public testing::Test {
 public:
  virtual void SetUp() {
    FLAGS_codegen_cache_capacity = "0";
    metrics_.reset(new MetricGroup("codegen-cache-test"));
    profile_ = RuntimeProfile::Create(&obj_pool_, "codegen-cache-test");
    test_env_.reset(new TestEnv);
    ASSERT_OK(test_env_->Init());
    RuntimeState* runtime_state_;
    ASSERT_OK(test_env_->CreateQueryState(0, &query_options_, &runtime_state_));
    QueryState* qs = runtime_state_->query_state();
    TPlanFragment* fragment = qs->obj_pool()->Add(new TPlanFragment());
    PlanFragmentCtxPB* fragment_ctx = qs->obj_pool()->Add(new PlanFragmentCtxPB());
    fragment_state_ =
        qs->obj_pool()->Add(new FragmentState(qs, *fragment, *fragment_ctx));
  }

  virtual void TearDown() {
    fragment_state_->ReleaseResources();
    fragment_state_ = nullptr;
    codegen_cache_.reset();
    test_env_.reset();
    metrics_.reset();
    obj_pool_.Clear();
  }

  void Reset() {
    TearDown();
    SetUp();
  }

  static void AddFunctionToJit(
      LlvmCodeGen* codegen, llvm::Function* fn, CodegenFnPtrBase* fn_ptr) {
    return codegen->AddFunctionToJitInternal(fn, fn_ptr);
  }

  static Status FinalizeModule(LlvmCodeGen* codegen) { return codegen->FinalizeModule(); }

  static void CheckResult(CodeGenCacheEntry& entry, bool is_double = false) {
    ASSERT_TRUE(!entry.Empty());
    ASSERT_TRUE(entry.engine_pointer != nullptr);
    CheckResult(entry.engine_pointer, is_double);
  }

  static void CheckResult(LlvmCodeGen* codegen, bool is_double = false) {
    ASSERT_TRUE(codegen->execution_engine_cached_ != nullptr);
    CheckResult(codegen->execution_engine_cached_.get(), is_double);
  }

  static void CheckResult(llvm::ExecutionEngine* engine, bool is_double = false) {
    void* test_fn;
    if (!is_double) {
      test_fn = reinterpret_cast<void*>(engine->getFunctionAddress("Echo"));
    } else {
      test_fn = reinterpret_cast<void*>(engine->getFunctionAddress("Double"));
    }
    ASSERT_TRUE(test_fn != nullptr);
    int input = 1;
    if (!is_double) {
      EXPECT_EQ(((TestEcho)test_fn)(input), input);
    } else {
      EXPECT_EQ(((TestDouble)test_fn)(input), input * 2);
    }
  }

  void AddLlvmCodegenEcho(LlvmCodeGen* codegen);
  void AddLlvmCodegenDouble(LlvmCodeGen* codegen);
  void GetLlvmEmptyFunction(LlvmCodeGen* codegen, llvm::Function**);
  typedef int (*TestEcho)(int);
  typedef int (*TestDouble)(int);
  typedef void (*TestEmpty)();
  void TestBasicFunction(TCodeGenCacheMode::type mode);
  void TestAtCapacity(TCodeGenCacheMode::type mode);
  void TestSkipCache();
  void CheckMetrics(CodeGenCache*, int, int, int);
  void CheckInUseMetrics(CodeGenCache*, int, int64_t);
  void CheckEvictMetrics(CodeGenCache*, int);
  int64_t GetMemCharge(LlvmCodeGen* codegen, string key_str, bool is_normal_mode);
  void CheckEngineCount(LlvmCodeGen*, int expect_count);
  void CheckToInsertMap();
  void TestSwitchModeHelper(TCodeGenCacheMode::type mode, string key,
      int expect_entry_num, int expect_engine_num, llvm::ExecutionEngine** engine);
  bool CheckKeyExist(TCodeGenCacheMode::type mode, string key);
  bool CheckEngineExist(llvm::ExecutionEngine* engine);
  void ExpectNumEngineSameAsEntry();
  void StoreHelper(TCodeGenCacheMode::type mode, string key);
  void TestConcurrentStore(int num_threads);

  vector<TCodeGenCacheMode::type> all_modes = {TCodeGenCacheMode::OPTIMAL,
      TCodeGenCacheMode::NORMAL, TCodeGenCacheMode::OPTIMAL_DEBUG,
      TCodeGenCacheMode::NORMAL_DEBUG};

  FragmentState* fragment_state_;
  ObjectPool obj_pool_;
  scoped_ptr<MetricGroup> metrics_;
  RuntimeProfile* profile_;
  scoped_ptr<TestEnv> test_env_;
  scoped_ptr<CodeGenCache> codegen_cache_;
  shared_ptr<llvm::ExecutionEngine> exec_engine_;
  TQueryOptions query_options_;
};

void LlvmCodeGenCacheTest::AddLlvmCodegenEcho(LlvmCodeGen* codegen) {
  ASSERT_TRUE(codegen != NULL);
  LlvmCodeGen::FnPrototype prototype(codegen, "Echo", codegen->i32_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("n", codegen->i32_type()));
  LlvmBuilder builder(codegen->context());
  llvm::Value* args[1];
  llvm::Function* fn = prototype.GeneratePrototype(&builder, args);
  builder.CreateRet(args[0]);
  fn = codegen->FinalizeFunction(fn);
  ASSERT_TRUE(fn != NULL);
  CodegenFnPtr<TestEcho> jitted_fn;
  AddFunctionToJit(codegen, fn, &jitted_fn);
  ASSERT_OK(FinalizeModule(codegen));
  ASSERT_TRUE(jitted_fn.load() != nullptr);
  TestEcho test_fn = jitted_fn.load();
  ASSERT_EQ(test_fn(1), 1);
}

void LlvmCodeGenCacheTest::GetLlvmEmptyFunction(
    LlvmCodeGen* codegen, llvm::Function** func) {
  ASSERT_TRUE(codegen != NULL);
  LlvmCodeGen::FnPrototype prototype(codegen, "TestEmpty", codegen->void_type());
  LlvmBuilder builder(codegen->context());
  llvm::Function* fn = prototype.GeneratePrototype(&builder, nullptr);
  builder.CreateRetVoid();
  fn = codegen->FinalizeFunction(fn);
  ASSERT_TRUE(fn != NULL);
  *func = fn;
}

void LlvmCodeGenCacheTest::AddLlvmCodegenDouble(LlvmCodeGen* codegen) {
  ASSERT_TRUE(codegen != NULL);
  LlvmCodeGen::FnPrototype prototype(codegen, "Double", codegen->i32_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("n", codegen->i32_type()));
  LlvmBuilder builder(codegen->context());
  llvm::Value* args[1];
  llvm::Function* fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* mul = codegen->GetI32Constant(2);
  args[0] = builder.CreateMul(args[0], mul);
  builder.CreateRet(args[0]);
  fn = codegen->FinalizeFunction(fn);
  ASSERT_TRUE(fn != NULL);
  CodegenFnPtr<TestDouble> jitted_fn;
  AddFunctionToJit(codegen, fn, &jitted_fn);
  ASSERT_OK(FinalizeModule(codegen));
  ASSERT_TRUE(jitted_fn.load() != nullptr);
  TestEcho test_fn = jitted_fn.load();
  ASSERT_EQ(test_fn(1), 2);
}

/// Test the basic function of a codegen cache.
void LlvmCodeGenCacheTest::TestBasicFunction(TCodeGenCacheMode::type mode) {
  int64_t codegen_cache_capacity = 256 * 1024; // 256KB
  codegen_cache_.reset(new CodeGenCache(metrics_.get()));
  bool is_normal_mode = !CodeGenCacheModeAnalyzer::is_optimal(mode);
  EXPECT_OK(codegen_cache_->Init(codegen_cache_capacity));

  // Create a LlvmCodeGen containing a codegen function Echo.
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(fragment_state_, NULL, "test", &codegen));
  AddLlvmCodegenEcho(codegen.get());
  // Create a LlvmCodeGen containing a codegen function Double.
  scoped_ptr<LlvmCodeGen> codegen_double;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(
      fragment_state_, NULL, "test_double", &codegen_double));
  AddLlvmCodegenDouble(codegen_double.get());

  CodeGenCacheKey cache_key;
  CodeGenCacheEntry entry;
  string key = "key";
  CodeGenCacheKeyConstructor::construct(key, &cache_key);
  int64_t mem_charge = GetMemCharge(codegen.get(), cache_key.data(), is_normal_mode);
  int64_t mem_charge_double =
      GetMemCharge(codegen_double.get(), cache_key.data(), is_normal_mode);

  // Store and lookup the entry by the key.
  EXPECT_OK(codegen_cache_->Store(cache_key, codegen.get(), mode));
  CheckInUseMetrics(
      codegen_cache_.get(), 1 /*num_entry_in_use*/, mem_charge /*bytes_in_use*/);
  EXPECT_OK(codegen_cache_->Lookup(cache_key, mode, &entry, &exec_engine_));
  CheckResult(entry);
  codegen->Close();
  // Close the LlvmCodeGen, but should not affect the stored cache.
  EXPECT_OK(codegen_cache_->Lookup(cache_key, mode, &entry, &exec_engine_));
  CheckResult(entry);
  // Override the entry with a different function, should be able to find the new
  // function from the new entry.
  EXPECT_OK(codegen_cache_->Store(cache_key, codegen_double.get(), mode));
  CheckInUseMetrics(
      codegen_cache_.get(), 1 /*num_entry_in_use*/, mem_charge_double /*bytes_in_use*/);
  EXPECT_OK(codegen_cache_->Lookup(cache_key, mode, &entry, &exec_engine_));
  CheckResult(entry, true /*is_double*/);
  EXPECT_EQ(codegen_cache_->codegen_cache_entries_evicted_->GetValue(), 1);
  codegen_double->Close();
}

void LlvmCodeGenCacheTest::CheckMetrics(
    CodeGenCache* codegen_cache, int hit, int miss, int evict) {
  EXPECT_EQ(codegen_cache->codegen_cache_hits_->GetValue(), hit);
  EXPECT_EQ(codegen_cache->codegen_cache_misses_->GetValue(), miss);
  EXPECT_EQ(codegen_cache->codegen_cache_entries_evicted_->GetValue(), evict);
}

void LlvmCodeGenCacheTest::CheckInUseMetrics(
    CodeGenCache* codegen_cache, int num_entry, int64_t bytes = -1) {
  EXPECT_EQ(codegen_cache->codegen_cache_entries_in_use_->GetValue(), num_entry);
  if (bytes != -1) {
    EXPECT_EQ(codegen_cache->codegen_cache_entries_in_use_bytes_->GetValue(), bytes);
  }
}

void LlvmCodeGenCacheTest::CheckEvictMetrics(CodeGenCache* codegen_cache, int evict) {
  EXPECT_EQ(codegen_cache->codegen_cache_entries_evicted_->GetValue(), evict);
}

int64_t LlvmCodeGenCacheTest::GetMemCharge(
    LlvmCodeGen* codegen, string key_str, bool is_normal_mode) {
  if (is_normal_mode) {
    return codegen->memory_manager_->bytes_allocated() + key_str.size()
        + sizeof(CodeGenCacheEntry);
  }
  // Optimal mode would use hash code and length as the key.
  return codegen->memory_manager_->bytes_allocated() + CodeGenCacheKey::OptimalKeySize
      + sizeof(CodeGenCacheEntry);
}

/// Test the situation that the codegen cache hits the limit of capacity, in this case,
/// eviction is needed when new insertion comes.
void LlvmCodeGenCacheTest::TestAtCapacity(TCodeGenCacheMode::type mode) {
  int64_t codegen_cache_capacity = 196; // 196B
  bool is_normal_mode = !CodeGenCacheModeAnalyzer::is_optimal(mode);
  // 128B for optimal mode
  if (!is_normal_mode) codegen_cache_capacity = 128;
  // Using single shard makes the logic of scenarios simple for capacity and
  // eviction-related behavior.
  FLAGS_cache_force_single_shard = true;

  // Create two LlvmCodeGen objects containing a different codegen function separately.
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(fragment_state_, NULL, "test", &codegen));
  AddLlvmCodegenEcho(codegen.get());
  codegen->GenerateFunctionNamesHashCode();
  scoped_ptr<LlvmCodeGen> codegen_double;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(
      fragment_state_, NULL, "test_double", &codegen_double));
  AddLlvmCodegenDouble(codegen_double.get());
  codegen_double->GenerateFunctionNamesHashCode();

  CodeGenCacheKey cache_key_1;
  CodeGenCacheKey cache_key_2;
  string key_1 = "key1";
  string key_2 = "key2";
  CodeGenCacheKeyConstructor::construct(key_1, &cache_key_1);
  CodeGenCacheKeyConstructor::construct(key_2, &cache_key_2);
  int64_t mem_charge_1 = GetMemCharge(codegen.get(), cache_key_1.data(), is_normal_mode);
  int64_t mem_charge_2 = GetMemCharge(codegen.get(), cache_key_2.data(), is_normal_mode);
  // Make sure the memory charge of two keys is larger than capacity for testing the
  // eviction.
  ASSERT_LE(mem_charge_1, codegen_cache_capacity);
  ASSERT_LE(mem_charge_2, codegen_cache_capacity);
  ASSERT_GE(mem_charge_1 + mem_charge_2, codegen_cache_capacity);

  test_env_->ResetCodegenCache(metrics_.get());
  CodeGenCache* cache = test_env_->codegen_cache();
  EXPECT_OK(cache->Init(codegen_cache_capacity));

  // Store key_1 and lookup.
  EXPECT_OK(codegen->StoreCache(cache_key_1));
  EXPECT_TRUE(codegen->LookupCache(cache_key_1));
  CheckResult(codegen.get());
  CheckMetrics(cache, 1 /*hit*/, 0 /*miss*/, 0 /*evict*/);

  // Store key_2, key_1 should be evicted due to hitting the capaticy limit.
  EXPECT_OK(codegen_double->StoreCache(cache_key_2));
  CheckMetrics(cache, 1 /*hit*/, 0 /*miss*/, 1 /*evict*/);

  // Lookup key_1, should be gone. Lookup key_2, should be successful.
  EXPECT_FALSE(codegen->LookupCache(cache_key_1));
  CheckMetrics(cache, 1 /*hit*/, 1 /*miss*/, 1 /*evict*/);
  EXPECT_TRUE(codegen_double->LookupCache(cache_key_2));
  CheckResult(codegen_double.get(), /*is_double*/ true);
  CheckMetrics(cache, 2 /*hit*/, 1 /*miss*/, 1 /*evict*/);

  // Store key_1 again, should evict the key_2, check again to see if everything
  // is alright.
  EXPECT_OK(codegen->StoreCache(cache_key_1));
  CheckMetrics(cache, 2 /*hit*/, 1 /*miss*/, 2 /*evict*/);
  EXPECT_FALSE(codegen_double->LookupCache(cache_key_2));
  CheckMetrics(cache, 2 /*hit*/, 2 /*miss*/, 2 /*evict*/);
  EXPECT_TRUE(codegen->LookupCache(cache_key_1));
  CheckResult(codegen.get());
  CheckMetrics(cache, 3 /*hit*/, 2 /*miss*/, 2 /*evict*/);

  codegen->Close();
  codegen_double->Close();
}

/// Test the case if we have a cache but doesn't contain the function
/// we want, should switch to the cache missing path.
void LlvmCodeGenCacheTest::TestSkipCache() {
  // Initial a LlvmCodeGen object with a normal function.
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(fragment_state_, NULL, "test", &codegen));
  AddLlvmCodegenEcho(codegen.get());
  // Create an empty function from other LlvmCodeGen to create the failure later.
  scoped_ptr<LlvmCodeGen> codegen_empty;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(
      fragment_state_, NULL, "test_empty", &codegen_empty));
  llvm::Function* empty_func;
  GetLlvmEmptyFunction(codegen_empty.get(), &empty_func);

  test_env_->ResetCodegenCache(metrics_.get());
  EXPECT_OK(test_env_->codegen_cache()->Init(256 * 1024 /*capacity*/));

  CheckMetrics(test_env_->codegen_cache(), 0 /*hit*/, 0 /*miss*/, 0 /*evict*/);
  CodeGenCacheKey cache_key;
  string key = "key";
  CodeGenCacheKeyConstructor::construct(key, &cache_key);
  codegen->GenerateFunctionNamesHashCode();
  // Store and lookup the entry by the key, should be successful.
  EXPECT_OK(codegen->StoreCache(cache_key));
  EXPECT_TRUE(codegen->LookupCache(cache_key));
  CheckMetrics(test_env_->codegen_cache(), 1 /*hit*/, 0 /*miss*/, 0 /*evict*/);
  CodegenFnPtr<TestEmpty> fn_ptr;
  // Insert a new function to the codegen, and regenerate the function names hash
  // code, expect a failure because the hash code inconsistency with the code in
  // the cache.
  codegen->fns_to_jit_compile_.emplace_back(empty_func, &fn_ptr);
  codegen->GenerateFunctionNamesHashCode();
  // Expect a look up failure.
  EXPECT_FALSE(codegen->LookupCache(cache_key));
  CheckMetrics(test_env_->codegen_cache(), 1 /*hit*/, 1 /*miss*/, 0 /*evict*/);
  codegen->Close();
  codegen_empty->Close();
}

// Test the basic function of using the codegen cache.
TEST_F(LlvmCodeGenCacheTest, BasicFunction) {
  for (auto mode : all_modes) {
    Reset();
    TestBasicFunction(mode);
  }
}

// Test when the codegen cache is at capacity.
TEST_F(LlvmCodeGenCacheTest, EvictionAtCapacity) {
  for (auto mode : all_modes) {
    query_options_.codegen_cache_mode = mode;
    Reset();
    TestAtCapacity(query_options_.codegen_cache_mode);
  }
}

// Test when the cache hits, but has different function names, in that case,
// we will skip the cache and fall back to normal path.
TEST_F(LlvmCodeGenCacheTest, SkipCache) {
  for (auto mode : all_modes) {
    query_options_.codegen_cache_mode = mode;
    Reset();
    TestSkipCache();
  }
}

// Test whether the codegen cache mode analyzer produces the correct result for all
// the modes.
TEST_F(LlvmCodeGenCacheTest, ModeAnalyzer) {
  EXPECT_FALSE(CodeGenCacheModeAnalyzer::is_debug(TCodeGenCacheMode::OPTIMAL));
  EXPECT_FALSE(CodeGenCacheModeAnalyzer::is_debug(TCodeGenCacheMode::NORMAL));
  EXPECT_TRUE(CodeGenCacheModeAnalyzer::is_debug(TCodeGenCacheMode::OPTIMAL_DEBUG));
  EXPECT_TRUE(CodeGenCacheModeAnalyzer::is_debug(TCodeGenCacheMode::NORMAL_DEBUG));
  EXPECT_TRUE(CodeGenCacheModeAnalyzer::is_optimal(TCodeGenCacheMode::OPTIMAL));
  EXPECT_FALSE(CodeGenCacheModeAnalyzer::is_optimal(TCodeGenCacheMode::NORMAL));
  EXPECT_TRUE(CodeGenCacheModeAnalyzer::is_optimal(TCodeGenCacheMode::OPTIMAL_DEBUG));
  EXPECT_FALSE(CodeGenCacheModeAnalyzer::is_optimal(TCodeGenCacheMode::NORMAL_DEBUG));
}

// Check the number of execution engine stored in the cache.
// Because the shared pointer of the execution engine needs to be stored in the codegen
// cache while the entry using the execution engine is stored in the cache.
void LlvmCodeGenCacheTest::CheckEngineCount(LlvmCodeGen* codegen, int expect_count) {
  lock_guard<mutex> lock(codegen_cache_->cached_engines_lock_);
  auto engine_it = codegen_cache_->cached_engines_.find(codegen->execution_engine_.get());
  EXPECT_TRUE(engine_it != codegen_cache_->cached_engines_.end());
  EXPECT_EQ(codegen_cache_->cached_engines_.size(), expect_count);
}

void LlvmCodeGenCacheTest::CheckToInsertMap() {
  lock_guard<mutex> lock(codegen_cache_->to_insert_set_lock_);
  EXPECT_EQ(codegen_cache_->keys_to_insert_.size(), 0);
}

// Return true if the provided key exists.
bool LlvmCodeGenCacheTest::CheckKeyExist(TCodeGenCacheMode::type mode, string key) {
  CodeGenCacheKey cache_key;
  CodeGenCacheEntry entry;
  CodeGenCacheKeyConstructor::construct(key, &cache_key);
  EXPECT_OK(codegen_cache_->Lookup(cache_key, mode, &entry, &exec_engine_));
  return !entry.Empty();
}

// Return true if the provided engine exists.
bool LlvmCodeGenCacheTest::CheckEngineExist(llvm::ExecutionEngine* engine) {
  auto engine_it = codegen_cache_->cached_engines_.find(engine);
  return engine_it != codegen_cache_->cached_engines_.end();
}

// Expect the number of execution engine is the same as the entry number in the global
// codegen cache.
void LlvmCodeGenCacheTest::ExpectNumEngineSameAsEntry() {
  EXPECT_EQ(codegen_cache_->cached_engines_.size(),
      codegen_cache_->codegen_cache_entries_in_use_->GetValue());
}

/// Helper function to test swithing modes. Helps to insert an entry with provided key
/// and mode.
void LlvmCodeGenCacheTest::TestSwitchModeHelper(TCodeGenCacheMode::type mode, string key,
    int expect_entry_num = -1, int expect_engine_num = -1,
    llvm::ExecutionEngine** engine = nullptr) {
  // Create a LlvmCodeGen containing a codegen function Echo.
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(fragment_state_, NULL, "test", &codegen));
  AddLlvmCodegenEcho(codegen.get());

  CodeGenCacheKey cache_key;
  CodeGenCacheEntry entry;
  CodeGenCacheKeyConstructor::construct(key, &cache_key);

  // Store and lookup the entry by the key.
  EXPECT_OK(codegen_cache_->Store(cache_key, codegen.get(), mode));
  if (expect_entry_num != -1) {
    CheckInUseMetrics(codegen_cache_.get(), expect_entry_num /*num_entry_in_use*/);
  }
  if (expect_engine_num != -1) {
    CheckEngineCount(codegen.get(), expect_engine_num);
  }
  EXPECT_OK(codegen_cache_->Lookup(cache_key, mode, &entry, &exec_engine_));
  CheckResult(entry);
  if (engine) *engine = codegen->execution_engine_.get();
  codegen->Close();
}

// Test to switch among different modes.
TEST_F(LlvmCodeGenCacheTest, SwitchMode) {
  int64_t codegen_cache_capacity = 512; // 512B
  codegen_cache_.reset(new CodeGenCache(metrics_.get()));
  EXPECT_OK(codegen_cache_->Init(codegen_cache_capacity));
  string key = "key";
  // Insert one entry to the cache with the key provided in each TestSwitchModeHelper().
  // The key of debug and non-debug are the same for the same mode, therefore,
  // we expect the entry number and engine number would not be changed between the
  // switch of debug and non-debug modes. But the key would be different between
  // NORMAL and OPTIMAL.
  TestSwitchModeHelper(TCodeGenCacheMode::OPTIMAL, key, 1, 1);
  TestSwitchModeHelper(TCodeGenCacheMode::OPTIMAL_DEBUG, key, 1, 1);
  TestSwitchModeHelper(TCodeGenCacheMode::NORMAL, key, 2, 2);
  TestSwitchModeHelper(TCodeGenCacheMode::NORMAL_DEBUG, key, 2, 2);
  // Try again, the new insertion should replace the old ones, so the entry number and
  // engine number won't change.
  llvm::ExecutionEngine *engine_opt, *engine_opt_dbg, *engine_normal, *engine_normal_dbg;
  TestSwitchModeHelper(TCodeGenCacheMode::OPTIMAL, key, 2, 2, &engine_opt);
  TestSwitchModeHelper(TCodeGenCacheMode::OPTIMAL_DEBUG, key, 2, 2, &engine_opt_dbg);
  // Expect the engines with the same key should be different, because the engine is
  // created every time.
  EXPECT_NE(engine_opt, engine_opt_dbg);
  // Search the engine, the later one should exist, while the early one not.
  EXPECT_FALSE(CheckEngineExist(engine_opt));
  EXPECT_TRUE(CheckEngineExist(engine_opt_dbg));

  // Same as above, but use the NORMAL type.
  TestSwitchModeHelper(TCodeGenCacheMode::NORMAL, key, 2, 2, &engine_normal);
  TestSwitchModeHelper(TCodeGenCacheMode::NORMAL_DEBUG, key, 2, 2, &engine_normal_dbg);
  EXPECT_NE(engine_normal, engine_normal_dbg);
  EXPECT_FALSE(CheckEngineExist(engine_normal));
  EXPECT_TRUE(CheckEngineExist(engine_normal_dbg));

  // Expect the two existing engines are not the same.
  EXPECT_NE(engine_opt_dbg, engine_normal_dbg);
  // Expect the number of existing engines are the same as the entries.
  ExpectNumEngineSameAsEntry();
  // Insert lots of different keys, so that to evict the original key due to reaching
  // capacity.
  for (int i = 0; i < 10; i++) {
    TestSwitchModeHelper(TCodeGenCacheMode::NORMAL, key + std::to_string(i));
  }
  // As the entries with originla key are evicted, expect we can't find them in the
  // codegen cache anymore.
  EXPECT_FALSE(CheckKeyExist(TCodeGenCacheMode::OPTIMAL, key));
  EXPECT_FALSE(CheckKeyExist(TCodeGenCacheMode::OPTIMAL_DEBUG, key));
  EXPECT_FALSE(CheckKeyExist(TCodeGenCacheMode::NORMAL, key));
  EXPECT_FALSE(CheckKeyExist(TCodeGenCacheMode::NORMAL_DEBUG, key));
  ExpectNumEngineSameAsEntry();
}

/// Helper function to store a specific key to the global codegen cache.
void LlvmCodeGenCacheTest::StoreHelper(TCodeGenCacheMode::type mode, string key) {
  // Create a LlvmCodeGen containing a codegen function Echo.
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(fragment_state_, NULL, "test", &codegen));
  AddLlvmCodegenEcho(codegen.get());
  CodeGenCacheKey cache_key;
  CodeGenCacheEntry entry;
  CodeGenCacheKeyConstructor::construct(key, &cache_key);
  EXPECT_OK(codegen_cache_->Store(cache_key, codegen.get(), mode));
  codegen->Close();
}

/// Concurrently store random entries to the global codegen cache, and check if all
/// the resources alright.
void LlvmCodeGenCacheTest::TestConcurrentStore(int num_threads) {
  thread_group workers;
  for (int i = 0; i < num_threads; ++i) {
    workers.add_thread(new thread([this, num_threads]() {
      int test_times = 100;
      while (test_times-- > 0) {
        string key = std::to_string(rand() % num_threads);
        int mode_idx = rand() % all_modes.size();
        StoreHelper(all_modes[mode_idx], key);
      }
    }));
  }
  workers.join_all();

  // Check the metrics and number of elements in the global cache to make sure there will
  // be no leaking.
  EXPECT_LE(codegen_cache_->codegen_cache_entries_in_use_->GetValue(), num_threads);
  EXPECT_GT(codegen_cache_->codegen_cache_entries_evicted_->GetValue(), 0);
  {
    lock_guard<mutex> lock(codegen_cache_->cached_engines_lock_);
    EXPECT_LE(codegen_cache_->cached_engines_.size(), num_threads);
  }
  CheckToInsertMap();
}

TEST_F(LlvmCodeGenCacheTest, ConcurrentStore) {
  int64_t codegen_cache_capacity = 512; // 512B
  codegen_cache_.reset(new CodeGenCache(metrics_.get()));
  EXPECT_OK(codegen_cache_->Init(codegen_cache_capacity));
  TestConcurrentStore(8);
}

} // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport(false);
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  return RUN_ALL_TESTS();
}
