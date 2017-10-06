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

using namespace llvm;
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

      ASSERT_OK(object1.Init(unique_ptr<Module>(new Module("Test", object1.context()))));
      ASSERT_OK(object2.Init(unique_ptr<Module>(new Module("Test", object2.context()))));
      ASSERT_OK(object3.Init(unique_ptr<Module>(new Module("Test", object3.context()))));

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

  static Status LinkModule(LlvmCodeGen* codegen, const string& file) {
    return codegen->LinkModule(file);
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
Function* CodegenInnerLoop(LlvmCodeGen* codegen, int64_t* jitted_counter, int delta) {
  LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  LlvmCodeGen::FnPrototype fn_prototype(codegen, "JittedInnerLoop", codegen->void_type());
  Function* jitted_loop_call = fn_prototype.GeneratePrototype();
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", jitted_loop_call);
  builder.SetInsertPoint(entry_block);
  codegen->CodegenDebugTrace(&builder, "Jitted\n");

  // Store &jitted_counter as a constant.
  Value* const_delta = ConstantInt::get(context, APInt(64, delta));
  Value* counter_ptr = codegen->CastPtrToLlvmPtr(codegen->GetPtrType(TYPE_BIGINT),
      jitted_counter);
  Value* loaded_counter = builder.CreateLoad(counter_ptr);
  Value* incremented_value = builder.CreateAdd(loaded_counter, const_delta);
  builder.CreateStore(incremented_value, counter_ptr);
  builder.CreateRetVoid();

  return jitted_loop_call;
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

  Function* loop_call = codegen->GetFunction(loop_call_name, false);
  EXPECT_TRUE(loop_call != NULL);
  EXPECT_TRUE(loop_call->arg_empty());
  Function* loop = codegen->GetFunction(loop_name, false);
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
  Function* jitted_loop_call = CodegenInnerLoop(codegen.get(), &jitted_counter, 1);

  // Part 3: Clone 'loop' and replace the call instruction to the normal function with a
  // call to the jitted one
  Function* jitted_loop = codegen->CloneFunction(loop);
  int num_replaced =
      codegen->ReplaceCallSites(jitted_loop, jitted_loop_call, loop_call_name);
  EXPECT_EQ(1, num_replaced);
  EXPECT_TRUE(VerifyFunction(codegen.get(), jitted_loop));

  // Part 4: Generate a new inner loop function and a new loop function
  Function* jitted_loop_call2 = CodegenInnerLoop(codegen.get(), &jitted_counter, -2);
  Function* jitted_loop2 = codegen->CloneFunction(loop);
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
Function* CodegenStringTest(LlvmCodeGen* codegen) {
  PointerType* string_val_ptr_type = codegen->GetPtrType(TYPE_STRING);
  EXPECT_TRUE(string_val_ptr_type != NULL);

  LlvmCodeGen::FnPrototype prototype(codegen, "StringTest", codegen->GetType(TYPE_INT));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("str", string_val_ptr_type));
  LlvmBuilder builder(codegen->context());

  Value* str;
  Function* interop_fn = prototype.GeneratePrototype(&builder, &str);

  // strval->ptr[0] = 'A'
  Value* str_ptr = builder.CreateStructGEP(NULL, str, 0, "str_ptr");
  Value* ptr = builder.CreateLoad(str_ptr, "ptr");
  Value* first_char_offset[] = { codegen->GetIntConstant(TYPE_INT, 0) };
  Value* first_char_ptr = builder.CreateGEP(ptr, first_char_offset, "first_char_ptr");
  builder.CreateStore(codegen->GetIntConstant(TYPE_TINYINT, 'A'), first_char_ptr);

  // Update and return old len
  Value* len_ptr = builder.CreateStructGEP(NULL, str, 1, "len_ptr");
  Value* len = builder.CreateLoad(len_ptr, "len");
  builder.CreateStore(codegen->GetIntConstant(TYPE_INT, 1), len_ptr);
  builder.CreateRet(len);

  return interop_fn;
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

  Function* string_test_fn = CodegenStringTest(codegen.get());
  EXPECT_TRUE(string_test_fn != NULL);
  EXPECT_TRUE(VerifyFunction(codegen.get(), string_test_fn));

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
  prototype.AddArgument(LlvmCodeGen::NamedVariable("n", codegen->GetType(TYPE_INT)));

  LlvmBuilder builder(codegen->context());

  char src[] = "abcd";
  char dst[] = "aaaa";

  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);
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

    Value* llvm_data1 =
        codegen->CastPtrToLlvmPtr(codegen->ptr_type(), const_cast<char*>(data1));
    Value* llvm_data2 =
        codegen->CastPtrToLlvmPtr(codegen->ptr_type(), const_cast<char*>(data2));
    Value* llvm_len1 = codegen->GetIntConstant(TYPE_INT, strlen(data1));
    Value* llvm_len2 = codegen->GetIntConstant(TYPE_INT, strlen(data2));

    uint32_t expected_hash = 0;
    expected_hash = HashUtil::Hash(data1, strlen(data1), expected_hash);
    expected_hash = HashUtil::Hash(data2, strlen(data2), expected_hash);
    expected_hash = HashUtil::Hash(data1, strlen(data1), expected_hash);

    // Create a codegen'd function that hashes all the types and returns the results.
    // The tuple/values to hash are baked into the codegen for simplicity.
    LlvmCodeGen::FnPrototype prototype(
        codegen.get(), "HashTest", codegen->GetType(TYPE_INT));
    LlvmBuilder builder(codegen->context());

    // Test both byte-size specific hash functions and the generic loop hash function
    Function* fn_fixed = prototype.GeneratePrototype(&builder, NULL);
    Function* data1_hash_fn = codegen->GetHashFunction(strlen(data1));
    Function* data2_hash_fn = codegen->GetHashFunction(strlen(data2));
    Function* generic_hash_fn = codegen->GetHashFunction();

    ASSERT_TRUE(data1_hash_fn != NULL);
    ASSERT_TRUE(data2_hash_fn != NULL);
    ASSERT_TRUE(generic_hash_fn != NULL);

    Value* seed = codegen->GetIntConstant(TYPE_INT, 0);
    seed = builder.CreateCall(data1_hash_fn,
        ArrayRef<Value*>({llvm_data1, llvm_len1, seed}));
    seed = builder.CreateCall(data2_hash_fn,
        ArrayRef<Value*>({llvm_data2, llvm_len2, seed}));
    seed = builder.CreateCall(generic_hash_fn,
        ArrayRef<Value*>({llvm_data1, llvm_len1, seed}));
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
  string temp_copy_path("/tmp/test-loop.bc");
  string module_file;
  PathBuilder::GetFullPath(ir_file_path, &module_file);
  ASSERT_OK(FileSystemUtil::CopyFile(module_file, temp_copy_path));
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(CreateFromFile(module_file.c_str(), &codegen));
  EXPECT_TRUE(codegen.get() != NULL);
  Status status = LinkModule(codegen.get(), temp_copy_path);
  EXPECT_STR_CONTAINS(status.GetDetail(), "symbol multiply defined");
  codegen->Close();
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport(false);
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  return RUN_ALL_TESTS();
}
