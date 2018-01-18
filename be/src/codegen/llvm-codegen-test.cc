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

class LlvmCodeGenTest : public testing:: Test {
 protected:
  scoped_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;

  virtual void SetUp() {
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
    ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &runtime_state_));
  }

  virtual void TearDown() {
    runtime_state_ = NULL;
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
    RETURN_IF_ERROR(LlvmCodeGen::CreateFromFile(runtime_state_,
        runtime_state_->obj_pool(), NULL, filename, "test", codegen));
    return (*codegen)->MaterializeModule();
  }

  static void ClearHashFns(LlvmCodeGen* codegen) {
    codegen->ClearHashFns();
  }

  static void AddFunctionToJit(LlvmCodeGen* codegen, llvm::Function* fn, void** fn_ptr) {
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
    return codegen->LinkModuleFromHdfs(hdfs_file);
  }

  static bool ContainsHandcraftedFn(LlvmCodeGen* codegen, llvm::Function* function) {
    const auto& hf = codegen->handcrafted_functions_;
    return find(hf.begin(), hf.end(), function) != hf.end();
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

// IR for the generated linner loop
// define void @JittedInnerLoop() {
// entry:
//   call void @DebugTrace(i8* inttoptr (i64 18970856 to i8*))
//   %0 = load i64* inttoptr (i64 140735197627800 to i64*)
//   %1 = add i64 %0, <delta>
//   store i64 %1, i64* inttoptr (i64 140735197627800 to i64*)
//   ret void
// }
// The random int in there is the address of jitted_counter
llvm::Function* CodegenInnerLoop(
    LlvmCodeGen* codegen, int64_t* jitted_counter, int delta) {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  LlvmCodeGen::FnPrototype fn_prototype(codegen, "JittedInnerLoop", codegen->void_type());
  llvm::Function* jitted_loop_call = fn_prototype.GeneratePrototype();
  llvm::BasicBlock* entry_block =
      llvm::BasicBlock::Create(context, "entry", jitted_loop_call);
  builder.SetInsertPoint(entry_block);
  codegen->CodegenDebugTrace(&builder, "Jitted\n");

  // Store &jitted_counter as a constant.
  llvm::Value* const_delta = codegen->GetI64Constant(delta);
  llvm::Value* counter_ptr =
      codegen->CastPtrToLlvmPtr(codegen->i64_ptr_type(), jitted_counter);
  llvm::Value* loaded_counter = builder.CreateLoad(counter_ptr);
  llvm::Value* incremented_value = builder.CreateAdd(loaded_counter, const_delta);
  builder.CreateStore(incremented_value, counter_ptr);
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(jitted_loop_call);
}

// This test loads a precompiled IR file (compiled from testdata/llvm/test-loop.cc).
// The test contains two functions, an outer loop function and an inner loop function.
// The outer loop calls the inner loop function.
// The test will
//   1. create a LlvmCodegen object from the precompiled file
//   2. add another function to the module with the same signature as the inner
//      loop function.
//   3. Replace the call instruction in a clone of the outer loop to a call to the new
//      inner loop function.
//   4. Update the original loop with another jitted inner loop function
//   5. Run both loops and make sure the correct inner loop is called

//   4. Run the loop and make sure the inner loop is called.
//   5. Updated the jitted loop in place with another jitted inner loop function
//   6. Run the loop and make sure the updated is called.
TEST_F(LlvmCodeGenTest, ReplaceFnCall) {
  const string loop_call_name("_Z21DefaultImplementationv");
  const string loop_name("_Z8TestLoopi");
  typedef void (*TestLoopFn)(int);

  string module_file;
  PathBuilder::GetFullPath("llvm-ir/test-loop.bc", &module_file);

  // Part 1: Load the module and make sure everything is loaded correctly.
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(CreateFromFile(module_file.c_str(), &codegen));
  EXPECT_TRUE(codegen.get() != NULL);

  llvm::Function* loop_call = codegen->GetFunction(loop_call_name, false);
  EXPECT_TRUE(loop_call != NULL);
  EXPECT_TRUE(loop_call->arg_empty());
  llvm::Function* loop = codegen->GetFunction(loop_name, false);
  EXPECT_TRUE(loop != NULL);
  EXPECT_EQ(loop->arg_size(), 1);

  // Part 2: Generate a new inner loop function.
  //
  // The c++ version of the code is
  // static int64_t* counter;
  // void JittedInnerLoop() {
  //   printf("LLVM Trace: Jitted\n");
  //   ++*counter;
  // }
  //
  int64_t jitted_counter = 0;
  llvm::Function* jitted_loop_call = CodegenInnerLoop(codegen.get(), &jitted_counter, 1);

  // Part 3: Clone 'loop' and replace the call instruction to the normal function with a
  // call to the jitted one
  llvm::Function* jitted_loop = codegen->CloneFunction(loop);
  int num_replaced =
      codegen->ReplaceCallSites(jitted_loop, jitted_loop_call, loop_call_name);
  EXPECT_EQ(1, num_replaced);
  EXPECT_TRUE(VerifyFunction(codegen.get(), jitted_loop));

  // Part 4: Generate a new inner loop function and a new loop function
  llvm::Function* jitted_loop_call2 =
      CodegenInnerLoop(codegen.get(), &jitted_counter, -2);
  llvm::Function* jitted_loop2 = codegen->CloneFunction(loop);
  num_replaced = codegen->ReplaceCallSites(jitted_loop2, jitted_loop_call2, loop_call_name);
  EXPECT_EQ(1, num_replaced);
  EXPECT_TRUE(VerifyFunction(codegen.get(), jitted_loop2));

  void* original_loop = NULL;
  AddFunctionToJit(codegen.get(), loop, &original_loop);
  void* new_loop = NULL;
  AddFunctionToJit(codegen.get(), jitted_loop, &new_loop);
  void* new_loop2 = NULL;
  AddFunctionToJit(codegen.get(), jitted_loop2, &new_loop2);

  // Part 5: compile all the functions (we can't add more functions after jitting with
  // MCJIT) then run them.
  ASSERT_OK(LlvmCodeGenTest::FinalizeModule(codegen.get()));
  ASSERT_TRUE(original_loop != NULL);
  ASSERT_TRUE(new_loop != NULL);
  ASSERT_TRUE(new_loop2 != NULL);

  TestLoopFn original_loop_fn = reinterpret_cast<TestLoopFn>(original_loop);
  original_loop_fn(5);
  EXPECT_EQ(0, jitted_counter);

  TestLoopFn new_loop_fn = reinterpret_cast<TestLoopFn>(new_loop);
  new_loop_fn(5);
  EXPECT_EQ(5, jitted_counter);
  new_loop_fn(5);
  EXPECT_EQ(10, jitted_counter);

  TestLoopFn new_loop_fn2 = reinterpret_cast<TestLoopFn>(new_loop2);
  new_loop_fn2(5);
  EXPECT_EQ(0, jitted_counter);
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
      codegen->GetSlotPtrType(TYPE_STRING);
  EXPECT_TRUE(string_val_ptr_type != NULL);

  LlvmCodeGen::FnPrototype prototype(codegen, "StringTest", codegen->i32_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("str", string_val_ptr_type));
  LlvmBuilder builder(codegen->context());

  llvm::Value* str;
  llvm::Function* interop_fn = prototype.GeneratePrototype(&builder, &str);

  // strval->ptr[0] = 'A'
  llvm::Value* str_ptr = builder.CreateStructGEP(NULL, str, 0, "str_ptr");
  llvm::Value* ptr = builder.CreateLoad(str_ptr, "ptr");
  llvm::Value* first_char_offset[] = {codegen->GetI32Constant(0)};
  llvm::Value* first_char_ptr =
      builder.CreateGEP(ptr, first_char_offset, "first_char_ptr");
  builder.CreateStore(codegen->GetI8Constant('A'), first_char_ptr);

  // Update and return old len
  llvm::Value* len_ptr = builder.CreateStructGEP(NULL, str, 1, "len_ptr");
  llvm::Value* len = builder.CreateLoad(len_ptr, "len");
  builder.CreateStore(codegen->GetI32Constant(1), len_ptr);
  builder.CreateRet(len);

  return codegen->FinalizeFunction(interop_fn);
}

// This test validates that the llvm StringValue struct matches the c++ stringvalue
// struct.  Just create a simple StringValue struct and make sure the IR can read it
// and modify it.
TEST_F(LlvmCodeGenTest, StringValue) {
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(runtime_state_, NULL, "test", &codegen));
  EXPECT_TRUE(codegen.get() != NULL);

  string str("Test");

  StringValue str_val;
  // Call memset to make sure padding bits are zero.
  memset(&str_val, 0, sizeof(str_val));
  str_val.ptr = const_cast<char*>(str.c_str());
  str_val.len = str.length();

  llvm::Function* string_test_fn = CodegenStringTest(codegen.get());
  EXPECT_TRUE(string_test_fn != NULL);

  // Jit compile function
  void* jitted_fn = NULL;
  AddFunctionToJit(codegen.get(), string_test_fn, &jitted_fn);
  ASSERT_OK(LlvmCodeGenTest::FinalizeModule(codegen.get()));
  ASSERT_TRUE(jitted_fn != NULL);

  // Call IR function
  typedef int (*TestStringInteropFn)(StringValue*);
  TestStringInteropFn fn = reinterpret_cast<TestStringInteropFn>(jitted_fn);
  int result = fn(&str_val);

  // Validate
  EXPECT_EQ(str.length(), result);
  EXPECT_EQ('A', str_val.ptr[0]);
  EXPECT_EQ(1, str_val.len);
  EXPECT_EQ(static_cast<void*>(str_val.ptr), static_cast<const void*>(str.c_str()));

  // Validate padding bytes are unchanged
  int32_t* bytes = reinterpret_cast<int32_t*>(&str_val);
  EXPECT_EQ(1, bytes[2]);   // str_val.len
  EXPECT_EQ(0, bytes[3]);   // padding
  codegen->Close();
}

// Test calling memcpy intrinsic
TEST_F(LlvmCodeGenTest, MemcpyTest) {
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(runtime_state_, NULL, "test", &codegen));
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

  void* jitted_fn = NULL;
  LlvmCodeGenTest::AddFunctionToJit(codegen.get(), fn, &jitted_fn);
  ASSERT_OK(LlvmCodeGenTest::FinalizeModule(codegen.get()));
  ASSERT_TRUE(jitted_fn != NULL);

  typedef void (*TestMemcpyFn)(char*, char*, int64_t);
  TestMemcpyFn test_fn = reinterpret_cast<TestMemcpyFn>(jitted_fn);

  test_fn(dst, src, 4);

  EXPECT_EQ(memcmp(src, dst, 4), 0);
  codegen->Close();
}

// Test codegen for hash
TEST_F(LlvmCodeGenTest, HashTest) {
  // Values to compute hash on
  const char* data1 = "test string";
  const char* data2 = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

  bool restore_sse_support = false;

  // Loop to test both the sse4 on/off paths
  for (int i = 0; i < 2; ++i) {
    scoped_ptr<LlvmCodeGen> codegen;
    ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(runtime_state_, NULL, "test", &codegen));
    ASSERT_TRUE(codegen.get() != NULL);
    const auto close_codegen =
        MakeScopeExitTrigger([&codegen]() { codegen->Close(); });

    llvm::Value* llvm_data1 =
        codegen->CastPtrToLlvmPtr(codegen->ptr_type(), const_cast<char*>(data1));
    llvm::Value* llvm_data2 =
        codegen->CastPtrToLlvmPtr(codegen->ptr_type(), const_cast<char*>(data2));
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
    LlvmBuilder builder(codegen->context());

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

    void* jitted_fn = NULL;
    LlvmCodeGenTest::AddFunctionToJit(codegen.get(), fn_fixed, &jitted_fn);
    ASSERT_OK(LlvmCodeGenTest::FinalizeModule(codegen.get()));
    ASSERT_TRUE(jitted_fn != NULL);

    typedef uint32_t (*TestHashFn)();
    TestHashFn test_fn = reinterpret_cast<TestHashFn>(jitted_fn);

    uint32_t result = test_fn();

    // Validate that the hashes are identical
    EXPECT_EQ(result, expected_hash) << CpuInfo::IsSupported(CpuInfo::SSE4_2);

    if (i == 0 && CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
      CpuInfo::EnableFeature(CpuInfo::SSE4_2, false);
      restore_sse_support = true;
      LlvmCodeGenTest::ClearHashFns(codegen.get());
    } else {
      // System doesn't have sse, no reason to test non-sse path again.
      break;
    }
  }

  // Restore hardware feature for next test
  CpuInfo::EnableFeature(CpuInfo::SSE4_2, restore_sse_support);
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
  EXPECT_EQ(vector<string>(
          {"-dummy1", "-dummy2", "-dummy3", "-dummy4", "+sse2", "-lzcnt"}),
      LlvmCodeGen::ApplyCpuAttrWhitelist(
          {"+dummy1", "+dummy2", "-dummy3", "+dummy4", "+sse2", "-lzcnt"}));

  // IMPALA-6291: Test that all AVX512 attributes are disabled.
  vector<string> avx512_attrs;
  EXPECT_EQ(vector<string>(
        {"-avx512ifma", "-avx512dqavx512er", "-avx512f", "-avx512bw", "-avx512vl",
         "-avx512cd", "-avx512vbmi", "-avx512pf"}),
      LlvmCodeGen::ApplyCpuAttrWhitelist(
        {"+avx512ifma", "+avx512dqavx512er", "+avx512f", "+avx512bw", "+avx512vl",
         "+avx512cd", "+avx512vbmi", "+avx512pf"}));
}

// Test that exercises the code path that deletes non-finalized methods before it
// finalizes the llvm module.
TEST_F(LlvmCodeGenTest, CleanupNonFinalizedMethodsTest) {
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::CreateImpalaCodegen(runtime_state_, nullptr, "test", &codegen));
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
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport(false);
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  return RUN_ALL_TESTS();
}
