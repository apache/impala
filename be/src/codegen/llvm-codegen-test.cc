// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>
#include <gtest/gtest.h>

#include "codegen/llvm-codegen.h"

#include <boost/thread/thread.hpp>

using namespace std;
using namespace boost;
using namespace llvm;

namespace impala {

void LifetimeTest() {
  Status status;
  for (int i = 0; i < 10; ++i) {
    LlvmCodeGen object1("Test");
    LlvmCodeGen object2("Test");
    LlvmCodeGen object3("Test");
    
    status = object1.Init();
    ASSERT_TRUE(status.ok());

    status = object2.Init();
    ASSERT_TRUE(status.ok());

    status = object3.Init();
    ASSERT_TRUE(status.ok());
  }
}

// Simple test to just make and destroy llvmcodegen objects.  LLVM 
// has non-obvious object ownership transfers and this sanity checks that.
TEST(LlvmLifetimeTest, Basic) {
  LifetimeTest();
}

// Same as above but multithreaded
TEST(LlvmMultithreadedLifetimeTest, Basic) {
  const int NUM_THREADS = 10;
  thread_group thread_group;
  for (int i = 0; i < NUM_THREADS; ++i) {
    thread_group.add_thread(new thread(&LifetimeTest));
  }
  thread_group.join_all();
}

// IR for the generated linner loop
// define void @JittedInnerLoop() {
// entry:
//   call void @DebugTrace(i8* inttoptr (i64 29347384 to i8*))
//   %0 = load i64* bitcast (i64 140736121079224 to i64*)
//   %1 = add i64 %0, <delta>
//   store i64 %1, i64* bitcast (i64 140736121079224 to i64*)
//   ret void
// }
// The random int in there is the address of jitted_counter
Function* CodegenInnerLoop(LlvmCodeGen* codegen, int64_t* jitted_counter, int delta) {
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder* builder = codegen->builder();

  LlvmCodeGen::FnPrototype fn_prototype(codegen, "JittedInnerLoop", codegen->void_type());
  Function* jitted_loop_call = fn_prototype.GeneratePrototype();
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", jitted_loop_call);
  builder->SetInsertPoint(entry_block);
  codegen->CodegenDebugTrace("Jitted");

  // Store &jitted_counter as a constant.
  // TODO: there is probably a better way to pass a random pointer to llvm
  Value* const_one = ConstantInt::get(context, APInt(64, delta));
  Constant* ptr_as_int = ConstantInt::get(codegen->GetType(TYPE_BIGINT),
      reinterpret_cast<int64_t>(jitted_counter));
  Value* cast_ptr = builder->CreateBitCast(ptr_as_int, codegen->int64_ptr_type());
  Value* loaded_counter = builder->CreateLoad(cast_ptr);
  Value* incremented_value = builder->CreateAdd(loaded_counter, const_one);
  builder->CreateStore(incremented_value, cast_ptr);
  builder->CreateRetVoid();

  return jitted_loop_call;
}

// This test loads a precompiled IR file (compiled from testdata/llvm/test-loop.cc).
// The test contains two functions, an outer loop function and an inner loop function.
// The outer loop calls the inner loop function.
// The test will 
//   1. create a LlvmCodegen object from the precompiled file
//   2. add another function to the module with the same signature as the inner 
//      loop function.
//   3. Replace the call instruction in the outer loop to a call to the new inner loop
//      function.
//   4. Run the loop and make sure the inner loop is called.
//   5. Updated the jitted loop in place with another jitted inner loop function
//   6. Run the loop and make sure the updated is called.
TEST(LlvmUpdateModuleTest, Basic) {
  const char* test_ir_file = "testdata/llvm/test-loop.ir";
  const char* loop_call_name = "DefaultImplementation";
  const char* loop_name = "TestLoop";
  typedef void (*TestLoopFn)(int);
  
  char* home = getenv("IMPALA_HOME");
  char module_file[strlen(test_ir_file) + strlen(home) + 2];
  sprintf(module_file, "%s/%s", home, test_ir_file);

  // Part 1: Load the module and make sure everything is loaded correctly.
  LlvmCodeGen* codegen = LlvmCodeGen::LoadFromFile(module_file);
  EXPECT_TRUE(codegen != NULL);
  Status status = codegen->Init();
  EXPECT_TRUE(status.ok());

  vector<Function*> functions;
  codegen->GetFunctions(&functions);
  EXPECT_EQ(functions.size(), 2);

  Function* loop_call = functions[0];
  Function* loop = functions[1];

  EXPECT_TRUE(loop_call->getName().find(loop_call_name) != string::npos);
  EXPECT_TRUE(loop_call->arg_empty());
  EXPECT_TRUE(loop->getName().find(loop_name) != string::npos);
  EXPECT_EQ(loop->arg_size(), 1);

  int scratch_size;
  void* original_loop = codegen->JitFunction(loop, &scratch_size);
  EXPECT_EQ(scratch_size, 0);
  EXPECT_TRUE(original_loop != NULL);
  
  TestLoopFn original_loop_fn = reinterpret_cast<TestLoopFn>(original_loop); 
  original_loop_fn(5);

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
  Function* jitted_loop_call = CodegenInnerLoop(codegen, &jitted_counter, 1);

  // Part 3: Replace the call instruction to the normal function with a call to the
  // jitted one
  int num_replaced;
  Function* jitted_loop = codegen->ReplaceCallSites(
      loop, false, jitted_loop_call, loop_call_name, &num_replaced);
  EXPECT_EQ(num_replaced, 1);
  EXPECT_TRUE(codegen->VerifyFunction(jitted_loop));

  // Part4: Call the new loop and verify results
  void* new_loop = codegen->JitFunction(jitted_loop, &scratch_size);
  EXPECT_EQ(scratch_size, 0);
  EXPECT_TRUE(new_loop != NULL);

  TestLoopFn new_loop_fn = reinterpret_cast<TestLoopFn>(new_loop);
  EXPECT_EQ(jitted_counter, 0);
  new_loop_fn(5);
  EXPECT_EQ(jitted_counter, 5);  
  new_loop_fn(5);
  EXPECT_EQ(jitted_counter, 10);  

  // Part5: Generate a new inner loop function and a new loop function in place
  Function* jitted_loop_call2 = CodegenInnerLoop(codegen, &jitted_counter, -2);
  Function* jitted_loop2 = codegen->ReplaceCallSites(loop, true, jitted_loop_call2, 
      loop_call_name, &num_replaced);
  EXPECT_EQ(num_replaced, 1);
  EXPECT_TRUE(codegen->VerifyFunction(jitted_loop2));

  // Part6: Call new loop
  void* new_loop2 = codegen->JitFunction(jitted_loop2, &scratch_size);
  EXPECT_EQ(scratch_size, 0);
  EXPECT_TRUE(new_loop2 != NULL);

  TestLoopFn new_loop_fn2 = reinterpret_cast<TestLoopFn>(new_loop2);
  new_loop_fn2(5);
  EXPECT_EQ(jitted_counter, 0);  
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}

