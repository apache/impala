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

#include <memory>
#include <string>
#include <boost/thread/thread.hpp>

#include "testutil/gtest-util.h"
#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "runtime/fragment-state.h"
#include "runtime/query-state.h"
#include "runtime/string-value.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "util/cpu-info.h"
#include "util/filesystem-util.h"
#include "util/hash-util.h"
#include "util/path-builder.h"
#include "util/scope-exit-trigger.h"
#include "util/test-info.h"

#include "common/names.h"

using std::unique_ptr;

namespace impala {

class CodegenFnPtrTest : public testing:: Test {
 protected:
   typedef int (*FnPtr)(int);
   static int int_identity(int a) { return a; }
   CodegenFnPtr<FnPtr> codegen_fn_ptr;
};

TEST_F(CodegenFnPtrTest, StoreVoidPtrAndLoad) {
  void* fn_ptr = reinterpret_cast<void*>(int_identity);
  codegen_fn_ptr.CodegenFnPtrBase::store(fn_ptr);
  FnPtr loaded_fn_ptr = codegen_fn_ptr.load();
  ASSERT_TRUE(loaded_fn_ptr == int_identity);
}

TEST_F(CodegenFnPtrTest, StoreNonVoidPtrAndLoad) {
  codegen_fn_ptr.store(int_identity);
  FnPtr loaded_fn_ptr = codegen_fn_ptr.load();
  ASSERT_TRUE(loaded_fn_ptr == int_identity);
}

class LlvmCodeGenTest : public testing::Test {
 protected:
  scoped_ptr<TestEnv> test_env_;
  FragmentState* fragment_state_;

  virtual void SetUp() {
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
    RuntimeState* runtime_state_;
    ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &runtime_state_));
    QueryState* qs = runtime_state_->query_state();
    TPlanFragment* fragment = qs->obj_pool()->Add(new TPlanFragment());
    PlanFragmentCtxPB* fragment_ctx = qs->obj_pool()->Add(new PlanFragmentCtxPB());
    fragment_state_ =
        qs->obj_pool()->Add(new FragmentState(qs, *fragment, *fragment_ctx));
  }

  virtual void TearDown() {
    fragment_state_->ReleaseResources();
    fragment_state_ = nullptr;
    test_env_.reset();
  }

  static void LifetimeTest() {
    ObjectPool pool;
    Status status;
    for (int i = 0; i < 10; ++i) {
      LlvmCodeGen object1(NULL, &pool, NULL, "Test");
      LlvmCodeGen object2(NULL, &pool, NULL, "Test");
      LlvmCodeGen object3(NULL, &pool, NULL, "Test");

      ASSERT_OK(object1.Init(
          unique_ptr<llvm::Module>(new llvm::Module("Test", object1.context()))));
      ASSERT_OK(object2.Init(
          unique_ptr<llvm::Module>(new llvm::Module("Test", object2.context()))));
      ASSERT_OK(object3.Init(
          unique_ptr<llvm::Module>(new llvm::Module("Test", object3.context()))));

      object1.Close();
      object2.Close();
      object3.Close();
    }
  }

  // Wrapper to call private test-only methods on LlvmCodeGen object
  Status CreateFromFile(const string& filename, scoped_ptr<LlvmCodeGen>* codegen) {
    RETURN_IF_ERROR(LlvmCodeGen::CreateFromFile(fragment_state_,
        fragment_state_->obj_pool(), NULL, filename, "test", codegen));
    return (*codegen)->MaterializeModule();
  }

  static void AddFunctionToJit(LlvmCodeGen* codegen, llvm::Function* fn,
      CodegenFnPtrBase* fn_ptr) {
    // Bypass Impala-specific logic in AddFunctionToJit() that assumes Impala's struct
    // types are available in the module.
    return codegen->AddFunctionToJitInternal(fn, fn_ptr);
  }

  static bool VerifyFunction(LlvmCodeGen* codegen, llvm::Function* fn) {
    return codegen->VerifyFunction(fn);
  }

  static Status FinalizeModule(LlvmCodeGen* codegen) { return codegen->FinalizeModule(); }

  static Status LinkModuleFromLocalFs(LlvmCodeGen* codegen, const string& file) {
    return codegen->LinkModuleFromLocalFs(file);
  }

  static Status LinkModuleFromHdfs(LlvmCodeGen* codegen, const string& hdfs_file) {
    return codegen->LinkModuleFromHdfs(hdfs_file, -1);
  }

  static bool ContainsHandcraftedFn(LlvmCodeGen* codegen, llvm::Function* function) {
    const auto& hf = codegen->handcrafted_functions_;
    return find(hf.begin(), hf.end(), function) != hf.end();
  }

  // Helper function to enable and disable LlvmCodeGen's cpu_attrs_.
  static void EnableCodeGenCPUFeature(int64_t flag, bool enable) {
    DCHECK(LlvmCodeGen::llvm_initialized_);
    auto enable_flag_it = LlvmCodeGen::cpu_flag_mappings_.find(flag);
    auto disable_flag_it = LlvmCodeGen::cpu_flag_mappings_.find(~flag);
    DCHECK(enable_flag_it != LlvmCodeGen::cpu_flag_mappings_.end());
    DCHECK(disable_flag_it != LlvmCodeGen::cpu_flag_mappings_.end());
    const std::string& enable_feature = enable_flag_it->second;
    const std::string& disable_feature = disable_flag_it->second;
    if (enable) {
      auto attr_it = LlvmCodeGen::cpu_attrs_.find(disable_feature);
      if (attr_it != LlvmCodeGen::cpu_attrs_.end()) {
        LlvmCodeGen::cpu_attrs_.erase(attr_it);
      }
      LlvmCodeGen::cpu_attrs_.insert(enable_feature);
    } else {
      auto attr_it = LlvmCodeGen::cpu_attrs_.find(enable_feature);
      // Disable feature if currently enabled.
      if (attr_it != LlvmCodeGen::cpu_attrs_.end()) {
        LlvmCodeGen::cpu_attrs_.erase(attr_it);
        LlvmCodeGen::cpu_attrs_.insert(disable_feature);
      }
    }
  }
};

// Simple test to just make and destroy llvmcodegen objects.  LLVM
// has non-obvious object ownership transfers and this sanity checks that.
TEST_F(LlvmCodeGenTest, BasicLifetime) {
  LifetimeTest();
}

// Same as above but multithreaded
TEST_F(LlvmCodeGenTest, MultithreadedLifetime) {
  const int NUM_THREADS = 10;
  thread_group thread_group;
  for (int i = 0; i < NUM_THREADS; ++i) {
    thread_group.add_thread(new thread(&LifetimeTest));
  }
  thread_group.join_all();
}

// Test loading a non-existent file
TEST_F(LlvmCodeGenTest, BadIRFile) {
  string module_file = "NonExistentFile.ir";
  scoped_ptr<LlvmCodeGen> codegen;
  EXPECT_FALSE(CreateFromFile(module_file.c_str(), &codegen).ok());
  codegen->Close();
}

// IR for the generated linner loop:
// define void @JittedInnerLoop(i64* %counter) {
// entry:
//   %0 = call i32 (i8*, ...) @printf(
//       i8* getelementptr inbounds ([19 x i8], [19 x i8]* @0, i32 0, i32 0))
//   %1 = load i64, i64* %counter
//   %2 = add i64 %1, 1
//   store i64 %2, i64* %counter
//   ret void
// }
llvm::Function* CodegenInnerLoop(
    LlvmCodeGen* codegen, int delta) {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  LlvmCodeGen::FnPrototype fn_prototype(codegen, "JittedInnerLoop", codegen->void_type());
  fn_prototype.AddArgument(
      LlvmCodeGen::NamedVariable("counter", codegen->i64_ptr_type()));
  llvm::Value* counter;
  llvm::Function* jitted_loop_call = fn_prototype.GeneratePrototype(&builder, &counter);
  codegen->CodegenDebugTrace(&builder, "Jitted\n");

  // Store &jitted_counter as a constant.
  llvm::Value* const_delta = codegen->GetI64Constant(delta);
  llvm::Value* loaded_counter = builder.CreateLoad(counter);
  llvm::Value* incremented_value = builder.CreateAdd(loaded_counter, const_delta);
  builder.CreateStore(incremented_value, counter);
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(jitted_loop_call);
}

// This test loads a precompiled IR file (compiled from testdata/llvm/test-loop.cc).
// The test contains two functions, an outer loop function and an inner loop function.
// The outer loop calls the inner loop function.
// The test will
//   1. Create a LlvmCodegen object from the precompiled file
//   2. Add another function to the module with the same signature as the inner
//      loop function.
//   3. Replace the call instruction in a clone of the outer loop to a call to the new
//      inner loop function.
//   4. Similar to 3, but replace the call with a different inner loop function.
//   5. Compile and run all loops and make sure the correct inner loop is called
TEST_F(LlvmCodeGenTest, ReplaceFnCall) {
  const string loop_call_name("_Z21DefaultImplementationPl");
  const string loop_name("_Z8TestLoopiPl");
  typedef void (*TestLoopFn)(int, int64_t*);

  string module_file;
  PathBuilder::GetFullPath("llvm-ir/test-loop.bc", &module_file);

  // Part 1: Load the module and make sure everything is loaded correctly.
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(CreateFromFile(module_file.c_str(), &codegen));
  EXPECT_TRUE(codegen.get() != nullptr);

  llvm::Function* loop_call = codegen->GetFunction(loop_call_name, false);
  EXPECT_TRUE(loop_call != nullptr);
  EXPECT_EQ(loop_call->arg_size(), 1);
  llvm::Function* loop = codegen->GetFunction(loop_name, false);
  EXPECT_TRUE(loop != nullptr);
  EXPECT_EQ(loop->arg_size(), 2);

  // Part 2: Generate a new inner loop function.
  //
  // The c++ version of the code is
  // void JittedInnerLoop(int64_t* counter) {
  //   printf("LLVM Trace: Jitted\n");
  //   ++*counter;
  // }
  //
  llvm::Function* jitted_loop_call = CodegenInnerLoop(codegen.get(), 1);

  // Part 3: Clone 'loop' and replace the call instruction to the normal function with a
  // call to the jitted one
  llvm::Function* jitted_loop = codegen->CloneFunction(loop);
  int num_replaced =
      codegen->ReplaceCallSites(jitted_loop, jitted_loop_call, loop_call_name);
  EXPECT_EQ(1, num_replaced);
  EXPECT_TRUE(VerifyFunction(codegen.get(), jitted_loop));

  // Part 4: Generate a new inner loop function and a new loop function
  llvm::Function* jitted_loop_call2 =
      CodegenInnerLoop(codegen.get(), -2);
  llvm::Function* jitted_loop2 = codegen->CloneFunction(loop);
  num_replaced = codegen->ReplaceCallSites(
      jitted_loop2, jitted_loop_call2, loop_call_name);
  EXPECT_EQ(1, num_replaced);
  EXPECT_TRUE(VerifyFunction(codegen.get(), jitted_loop2));

  CodegenFnPtr<TestLoopFn> original_loop;
  AddFunctionToJit(codegen.get(), loop, &original_loop);
  CodegenFnPtr<TestLoopFn> new_loop;
  AddFunctionToJit(codegen.get(), jitted_loop, &new_loop);
  CodegenFnPtr<TestLoopFn> new_loop2;
  AddFunctionToJit(codegen.get(), jitted_loop2, &new_loop2);

  // Part 5: compile all the functions (we can't add more functions after jitting with
  // MCJIT) then run them.
  ASSERT_OK(LlvmCodeGenTest::FinalizeModule(codegen.get()));
  ASSERT_TRUE(original_loop.load() != nullptr);
  ASSERT_TRUE(new_loop.load() != nullptr);
  ASSERT_TRUE(new_loop2.load() != nullptr);

  int64_t counter = 0;
  TestLoopFn original_loop_fn = original_loop.load();
  original_loop_fn(5, &counter);
  EXPECT_EQ(0, counter);

  TestLoopFn new_loop_fn = new_loop.load();
  new_loop_fn(5, &counter);
  EXPECT_EQ(5, counter);
  new_loop_fn(5, &counter);
  EXPECT_EQ(10, counter);

  TestLoopFn new_loop_fn2 = new_loop2.load();
  new_loop_fn2(5, &counter);
  EXPECT_EQ(0, counter);
  codegen->Close();
}

// Test function for c++/ir interop for strings.  Function will do:
// int StringTest(StringValue* strval) {
//   strval->ptr[0] = 'A';
//   int len = strval->len;
//   strval->len = 1;
//   return len;
// }
// Corresponding IR is:
// define i32 @StringTest(%StringValue* %str) {
// entry:
//   %str_ptr = getelementptr inbounds %StringValue* %str, i32 0, i32 0
//   %ptr = load i8** %str_ptr
//   %first_char_ptr = getelementptr i8* %ptr, i32 0
//   store i8 65, i8* %first_char_ptr
//   %len_ptr = getelementptr inbounds %StringValue* %str, i32 0, i32 1
//   %len = load i32* %len_ptr
//   store i32 1, i32* %len_ptr
//   ret i32 %len
// }
llvm::Function* CodegenStringTest(LlvmCodeGen* codegen) {
  llvm::PointerType* string_val_ptr_type =
      codegen->GetSlotPtrType(ColumnType(TYPE_STRING));
  EXPECT_TRUE(string_val_ptr_type != NULL);

  LlvmCodeGen::FnPrototype prototype(codegen, "StringTest", codegen->i32_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("str", string_val_ptr_type));
  LlvmBuilder builder(codegen->context());

  llvm::Value* str;
  llvm::Function* interop_fn = prototype.GeneratePrototype(&builder, &str);

  // strval->ptr[0] = 'A'
  llvm::Function* str_ptr_fn = codegen->GetFunction(
      IRFunction::STRING_VALUE_PTR, false);
  llvm::Function* str_len_fn = codegen->GetFunction(
      IRFunction::STRING_VALUE_LEN, false);
  llvm::Function* str_setlen_fn = codegen->GetFunction(
      IRFunction::STRING_VALUE_SETLEN, false);

  llvm::Value* str_ptr = builder.CreateCall(str_ptr_fn,
      llvm::ArrayRef<llvm::Value*>({str}), "ptr");

  llvm::Value* first_char_offset[] = {codegen->GetI32Constant(0)};
  llvm::Value* first_char_ptr =
      builder.CreateGEP(str_ptr, first_char_offset, "first_char_ptr");
  builder.CreateStore(codegen->GetI8Constant('A'), first_char_ptr);

  // Update and return old len
  llvm::Value* str_len = builder.CreateCall(str_len_fn,
      llvm::ArrayRef<llvm::Value*>({str}), "len");

  builder.CreateCall(str_setlen_fn,
      llvm::ArrayRef<llvm::Value*>({str, codegen->GetI32Constant(1)}));

  builder.CreateRet(str_len);

  return codegen->FinalizeFunction(interop_fn);
}

// This test validates that the llvm StringValue struct matches the c++ stringvalue
// struct.  Just create a simple StringValue struct and make sure the IR can read it
// and modify it.
TEST_F(LlvmCodeGenTest, StringValue) {
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(fragment_state_, NULL, "test", &codegen));
  EXPECT_TRUE(codegen.get() != NULL);

  string str("Test");

  StringValue str_val;
  str_val.Assign(const_cast<char*>(str.c_str()), str.length());

  llvm::Function* string_test_fn = CodegenStringTest(codegen.get());
  EXPECT_TRUE(string_test_fn != NULL);

  // Jit compile function
  typedef int (*TestStringInteropFn)(StringValue*);
  CodegenFnPtr<TestStringInteropFn> jitted_fn;
  AddFunctionToJit(codegen.get(), string_test_fn, &jitted_fn);
  ASSERT_OK(LlvmCodeGenTest::FinalizeModule(codegen.get()));
  ASSERT_TRUE(jitted_fn.load() != nullptr);

  // Call IR function
  TestStringInteropFn fn = jitted_fn.load();
  int result = fn(&str_val);

  // Validate
  EXPECT_EQ(str.length(), result);
  EXPECT_EQ('A', str_val.Ptr()[0]);
  EXPECT_EQ(1, str_val.Len());
  EXPECT_EQ(static_cast<void*>(str_val.Ptr()), static_cast<const void*>(str.c_str()));

  // After IMPALA-7367 removed the padding from the StringValue struct, validate the
  // length byte alone. To avoid warnings about constructing a pointer into a packed
  // struct (Waddress-of-packed-member), this needs to copy the bytes out to a
  // temporary variable.
  int8_t* bytes = reinterpret_cast<int8_t*>(&str_val);
  int32_t len = 0;
  memcpy(static_cast<void*>(&len), static_cast<void*>(&bytes[8]), sizeof(int32_t));
  EXPECT_EQ(1, len);   // str_val.len
  codegen->Close();
}

// Test calling memcpy intrinsic
TEST_F(LlvmCodeGenTest, MemcpyTest) {
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(fragment_state_, NULL, "test", &codegen));
  ASSERT_TRUE(codegen.get() != NULL);

  LlvmCodeGen::FnPrototype prototype(codegen.get(), "MemcpyTest", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("dest", codegen->ptr_type()));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("src", codegen->ptr_type()));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("n", codegen->i32_type()));

  LlvmBuilder builder(codegen->context());

  char src[] = "abcd";
  char dst[] = "aaaa";

  llvm::Value* args[3];
  llvm::Function* fn = prototype.GeneratePrototype(&builder, &args[0]);
  codegen->CodegenMemcpy(&builder, args[0], args[1], sizeof(src));
  builder.CreateRetVoid();

  fn = codegen->FinalizeFunction(fn);
  ASSERT_TRUE(fn != NULL);

  typedef void (*TestMemcpyFn)(char*, char*, int64_t);
  CodegenFnPtr<TestMemcpyFn> jitted_fn;
  LlvmCodeGenTest::AddFunctionToJit(codegen.get(), fn, &jitted_fn);
  ASSERT_OK(LlvmCodeGenTest::FinalizeModule(codegen.get()));
  ASSERT_TRUE(jitted_fn.load() != nullptr);

  TestMemcpyFn test_fn = jitted_fn.load();

  test_fn(dst, src, 4);

  EXPECT_EQ(memcmp(src, dst, 4), 0);
  codegen->Close();
}

// Test codegen for hash
TEST_F(LlvmCodeGenTest, HashTest) {
  // Values to compute hash on
  const char* data1 = "test string";
  const char* data2 = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(fragment_state_, NULL, "test", &codegen));
  ASSERT_TRUE(codegen.get() != NULL);
  const auto close_codegen =
      MakeScopeExitTrigger([&codegen]() { codegen->Close(); });

  LlvmBuilder builder(codegen->context());
  llvm::Value* llvm_data1 = codegen->GetStringConstant(&builder, data1, strlen(data1));
  llvm::Value* llvm_data2 = codegen->GetStringConstant(&builder, data2, strlen(data2));
  llvm::Value* llvm_len1 = codegen->GetI32Constant(strlen(data1));
  llvm::Value* llvm_len2 = codegen->GetI32Constant(strlen(data2));

  uint32_t expected_hash = 0;
  expected_hash = HashUtil::Hash(data1, strlen(data1), expected_hash);
  expected_hash = HashUtil::Hash(data2, strlen(data2), expected_hash);
  expected_hash = HashUtil::Hash(data1, strlen(data1), expected_hash);

  // Create a codegen'd function that hashes all the types and returns the results.
  // The tuple/values to hash are baked into the codegen for simplicity.
  LlvmCodeGen::FnPrototype prototype(
      codegen.get(), "HashTest", codegen->i32_type());

  // Test both byte-size specific hash functions and the generic loop hash function
  llvm::Function* fn_fixed = prototype.GeneratePrototype(&builder, NULL);
  llvm::Function* data1_hash_fn = codegen->GetHashFunction(strlen(data1));
  llvm::Function* data2_hash_fn = codegen->GetHashFunction(strlen(data2));
  llvm::Function* generic_hash_fn = codegen->GetHashFunction();

  ASSERT_TRUE(data1_hash_fn != NULL);
  ASSERT_TRUE(data2_hash_fn != NULL);
  ASSERT_TRUE(generic_hash_fn != NULL);

  llvm::Value* seed = codegen->GetI32Constant(0);
  seed = builder.CreateCall(
      data1_hash_fn, llvm::ArrayRef<llvm::Value*>({llvm_data1, llvm_len1, seed}));
  seed = builder.CreateCall(
      data2_hash_fn, llvm::ArrayRef<llvm::Value*>({llvm_data2, llvm_len2, seed}));
  seed = builder.CreateCall(
      generic_hash_fn, llvm::ArrayRef<llvm::Value*>({llvm_data1, llvm_len1, seed}));
  builder.CreateRet(seed);

  fn_fixed = codegen->FinalizeFunction(fn_fixed);
  ASSERT_TRUE(fn_fixed != NULL);

  typedef uint32_t (*TestHashFn)();
  CodegenFnPtr<TestHashFn> jitted_fn;
  LlvmCodeGenTest::AddFunctionToJit(codegen.get(), fn_fixed, &jitted_fn);
  ASSERT_OK(LlvmCodeGenTest::FinalizeModule(codegen.get()));
  ASSERT_TRUE(jitted_fn.load() != nullptr);

  TestHashFn test_fn = jitted_fn.load();

  uint32_t result = test_fn();

  // Validate that the hashes are identical
  EXPECT_EQ(result, expected_hash) << LlvmCodeGen::IsCPUFeatureEnabled(CpuInfo::SSE4_2);
}

// Test that an error propagating through codegen's diagnostic handler is
// captured by impala. An error is induced by asking Llvm to link the same lib twice.
TEST_F(LlvmCodeGenTest, HandleLinkageError) {
  string ir_file_path("llvm-ir/test-loop.bc");
  string module_file;
  PathBuilder::GetFullPath(ir_file_path, &module_file);
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(CreateFromFile(module_file.c_str(), &codegen));
  EXPECT_TRUE(codegen.get() != nullptr);
  Status status = LinkModuleFromLocalFs(codegen.get(), module_file);
  EXPECT_STR_CONTAINS(status.GetDetail(), "symbol multiply defined");
  codegen->Close();
}

// Test that the default whitelisting disables the expected attributes.
TEST_F(LlvmCodeGenTest, CpuAttrWhitelist) {
  // Non-existent attributes should be disabled regardless of initial states.
  // Whitelisted attributes like sse2 and lzcnt should retain their initial
  // state.
  // arm does not have sse2
  EXPECT_EQ(std::unordered_set<string>(
                {"-dummy1", "-dummy2", "-dummy3", "-dummy4",
                IS_AARCH64 ? "-sse2" : "+sse2", "-lzcnt"}),
      LlvmCodeGen::ApplyCpuAttrWhitelist(
                {"+dummy1", "+dummy2", "-dummy3", "+dummy4", "+sse2", "-lzcnt"}));
  // IMPALA-6291: Test that all AVX512 attributes are disabled.
  vector<string> avx512_attrs;
  EXPECT_EQ(std::unordered_set<string>({"-avx512ifma", "-avx512dqavx512er", "-avx512f",
                "-avx512bw", "-avx512vl", "-avx512cd", "-avx512vbmi", "-avx512pf"}),
      LlvmCodeGen::ApplyCpuAttrWhitelist({"+avx512ifma", "+avx512dqavx512er", "+avx512f",
          "+avx512bw", "+avx512vl", "+avx512cd", "+avx512vbmi", "+avx512pf"}));
}

// Test that exercises the code path that deletes non-finalized methods before it
// finalizes the llvm module.
TEST_F(LlvmCodeGenTest, CleanupNonFinalizedMethodsTest) {
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(fragment_state_, nullptr, "test", &codegen));
  ASSERT_TRUE(codegen.get() != nullptr);
  const auto close_codegen = MakeScopeExitTrigger([&codegen]() { codegen->Close(); });
  LlvmBuilder builder(codegen->context());
  LlvmCodeGen::FnPrototype incomplete_prototype(
      codegen.get(), "IncompleteFn", codegen->void_type());
  LlvmCodeGen::FnPrototype complete_prototype(
      codegen.get(), "CompleteFn", codegen->void_type());
  llvm::Function* incomplete_fn =
      incomplete_prototype.GeneratePrototype(&builder, nullptr);
  llvm::Function* complete_fn = complete_prototype.GeneratePrototype(&builder, nullptr);
  builder.CreateRetVoid();
  complete_fn = codegen->FinalizeFunction(complete_fn);
  EXPECT_TRUE(complete_fn != nullptr);
  ASSERT_TRUE(ContainsHandcraftedFn(codegen.get(), incomplete_fn));
  ASSERT_TRUE(ContainsHandcraftedFn(codegen.get(), complete_fn));
  ASSERT_OK(FinalizeModule(codegen.get()));
}

class LlvmOptTest :
    public testing::TestWithParam<std::tuple<TCodeGenOptLevel::type, bool>> {
 protected:
  scoped_ptr<MetricGroup> metrics_;
  scoped_ptr<TestEnv> test_env_;
  FragmentState* fragment_state_;
  TQueryOptions query_opts_;

  virtual void SetUp() {
    metrics_.reset(new MetricGroup("codegen-test"));
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
    SetUpQuery(0, std::get<0>(GetParam()));
  }

  virtual void TearDown() {
    fragment_state_->ReleaseResources();
    fragment_state_ = nullptr;
    test_env_.reset();
    metrics_.reset();
  }

  void SetUpQuery(int64_t query_id, TCodeGenOptLevel::type level) {
    query_opts_.__set_codegen_opt_level(level);
    RuntimeState* runtime_state_;
    ASSERT_OK(test_env_->CreateQueryState(query_id, &query_opts_, &runtime_state_));
    QueryState* qs = runtime_state_->query_state();
    TPlanFragment* fragment = qs->obj_pool()->Add(new TPlanFragment());
    PlanFragmentCtxPB* fragment_ctx = qs->obj_pool()->Add(new PlanFragmentCtxPB());
    fragment_state_ =
        qs->obj_pool()->Add(new FragmentState(qs, *fragment, *fragment_ctx));
  }

  void EnableCodegenCache() {
    test_env_->ResetCodegenCache(metrics_.get());
    EXPECT_OK(test_env_->codegen_cache()->Init(1024*1024));
  }

  // Wrapper to call private test-only methods on LlvmCodeGen object
  Status CreateFromFile(const string& filename, scoped_ptr<LlvmCodeGen>* codegen) {
    RETURN_IF_ERROR(LlvmCodeGen::CreateFromFile(fragment_state_,
        fragment_state_->obj_pool(), nullptr, filename, "test", codegen));
    return (*codegen)->MaterializeModule();
  }

  static void AddFunctionToJit(LlvmCodeGen* codegen, llvm::Function* fn,
      CodegenFnPtrBase* fn_ptr) {
    // Bypass Impala-specific logic in AddFunctionToJit() that assumes Impala's struct
    // types are available in the module.
    return codegen->AddFunctionToJitInternal(fn, fn_ptr);
  }

  static Status FinalizeModule(LlvmCodeGen* codegen) { return codegen->FinalizeModule(); }

  static void VerifyCounters(LlvmCodeGen* codegen, bool expect_unopt) {
    ASSERT_GT(codegen->num_functions_->value(), 0);
    ASSERT_GT(codegen->num_instructions_->value(), 0);
    if (expect_unopt) {
      ASSERT_EQ(codegen->num_opt_functions_->value(), codegen->num_functions_->value());
      ASSERT_EQ(
          codegen->num_opt_instructions_->value(), codegen->num_instructions_->value());
    } else {
      ASSERT_LT(codegen->num_opt_functions_->value(), codegen->num_functions_->value());
      ASSERT_LT(
          codegen->num_opt_instructions_->value(), codegen->num_instructions_->value());
    }
  }

  void LoadTestOpt(scoped_ptr<LlvmCodeGen>& codegen) {
    const string func("_Z9loop_nullPii");
    typedef void (*LoopFn)(int*, int);

    string module_file;
    PathBuilder::GetFullPath("llvm-ir/test-opt.bc", &module_file);

    ASSERT_OK(CreateFromFile(module_file.c_str(), &codegen));
    EXPECT_TRUE(codegen.get() != nullptr);
    codegen->EnableOptimizations(true);

    llvm::Function* fn = codegen->GetFunction(func, false);
    EXPECT_TRUE(fn != nullptr);
    EXPECT_EQ(fn->arg_size(), 2);
    CodegenFnPtr<LoopFn> fn_impl;
    AddFunctionToJit(codegen.get(), fn, &fn_impl);

    ASSERT_OK(FinalizeModule(codegen.get()));
    ASSERT_TRUE(fn_impl.load() != nullptr);
  }
};

TEST_P(LlvmOptTest, OptFunction) {
  scoped_ptr<LlvmCodeGen> codegen;
  LoadTestOpt(codegen);
  VerifyCounters(codegen.get(), std::get<1>(GetParam()));
  codegen->Close();
}

TEST_P(LlvmOptTest, CachedOptFunction) {
  TCodeGenOptLevel::type opt_level = std::get<0>(GetParam());
  bool expect_unoptimized = std::get<1>(GetParam());
  EnableCodegenCache();

  scoped_ptr<LlvmCodeGen> codegen;
  LoadTestOpt(codegen);
  VerifyCounters(codegen.get(), expect_unoptimized);
  codegen->Close();

  int64_t query_id = 0;
  constexpr std::array<TCodeGenOptLevel::type, 5> opt_levels{{TCodeGenOptLevel::O0,
      TCodeGenOptLevel::O1, TCodeGenOptLevel::Os, TCodeGenOptLevel::O2,
      TCodeGenOptLevel::O3}};
  for (TCodeGenOptLevel::type level : opt_levels) {
    codegen.reset();
    SetUpQuery(++query_id, level);
    LoadTestOpt(codegen);
    // Higher levels are by definition O1+, which expects optimized size.
    // Lower or equal levels should use the cached value from before the loop.
    VerifyCounters(codegen.get(), level > opt_level ? false : expect_unoptimized);
    codegen->Close();
  }

  // Check for cache hits/misses. Levels greater than opt_level will be a hit, others
  // will be misses.
  int num_less = opt_level - TCodeGenOptLevel::O0;
  IntCounter* cache_hits =
      metrics_->FindMetricForTesting<IntCounter>("impala.codegen-cache.hits");
  EXPECT_NE(cache_hits, nullptr);
  EXPECT_EQ(cache_hits->GetValue(), 1 + num_less);
  IntCounter* cache_misses =
      metrics_->FindMetricForTesting<IntCounter>("impala.codegen-cache.misses");
  EXPECT_NE(cache_misses, nullptr);
  EXPECT_EQ(cache_misses->GetValue(), opt_levels.size() - num_less);
}

INSTANTIATE_TEST_SUITE_P(OptLevels, LlvmOptTest, ::testing::Values(
  //              Optimization level    Expect unoptimized
  std::make_tuple(TCodeGenOptLevel::O0, true),
  std::make_tuple(TCodeGenOptLevel::O1, false),
  std::make_tuple(TCodeGenOptLevel::Os, false),
  std::make_tuple(TCodeGenOptLevel::O2, false),
  std::make_tuple(TCodeGenOptLevel::O3, false)));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport(false);
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  return RUN_ALL_TESTS();
}
