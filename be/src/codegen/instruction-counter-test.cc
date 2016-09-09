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

#include <boost/thread/thread.hpp>
#include <string>

#include "llvm/IR/Module.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/CallingConv.h"
#include "llvm/IR/IRBuilder.h"

#include "codegen/llvm-codegen.h"
#include "codegen/instruction-counter.h"
#include "testutil/gtest-util.h"

#include "common/names.h"
using namespace llvm;

namespace impala {

class InstructionCounterTest : public testing:: Test {
};

// IR output from CodegenMullAdd
// define i32 @mul_add(i32 %x, i32 %y, i32 %z) {
// entry:
//   %tmp = mul i32 %x, %y
//   %tmp2 = add i32 %tmp, %z
//   ret i32 %tmp2
// }
// Create a module with one function, which multiplies two arguments, add a third and
// then returns the result.
Module* CodegenMulAdd(LLVMContext* context) {
  Module* mod = new Module("test", *context);
  Constant* c = mod->getOrInsertFunction("mul_add", IntegerType::get(*context, 32),
      IntegerType::get(*context, 32), IntegerType::get(*context, 32),
      IntegerType::get(*context, 32), NULL);
  Function* mul_add = cast<Function>(c);
  mul_add->setCallingConv(CallingConv::C);
  Function::arg_iterator args = mul_add->arg_begin();
  Value* x = &*args;
  ++args;
  x->setName("x");
  Value* y = &*args;
  ++args;
  y->setName("y");
  Value* z = &*args;
  ++args;
  z->setName("z");
  BasicBlock* block = BasicBlock::Create(*context, "entry", mul_add);
  IRBuilder<> builder(block);
  Value* tmp = builder.CreateBinOp(Instruction::Mul, x, y, "tmp");
  Value* tmp2 = builder.CreateBinOp(Instruction::Add, tmp, z, "tmp2");
  builder.CreateRet(tmp2);
  return mod;
}

TEST_F(InstructionCounterTest, Count) {
  Module* MulAddModule = CodegenMulAdd(&getGlobalContext());
  InstructionCounter* instruction_counter = new InstructionCounter();
  instruction_counter->visit(*MulAddModule);
  instruction_counter->PrintCounters();
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_FUNCTIONS), 1);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_INSTS), 3);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TERMINATOR_INSTS), 1);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::MEMORY_INSTS), 0);

  // Test Reset
  instruction_counter->ResetCount();
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_FUNCTIONS), 0);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_INSTS), 0);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::MEMORY_INSTS), 0);
}

// IR output from CodegenGcd
// define i32 @gcd(i32 %x, i32 %y) {
// entry:
//   %tmp = icmp eq i32 %x, %y
//   br i1 %tmp, label %return, label %cond_false
//
// return:                                           ; preds = %entry
//   ret i32 %x
//
// cond_false:                                       ; preds = %entry
//   %tmp2 = icmp ult i32 %x, %y
//   br i1 %tmp2, label %cond_true, label %cond_false1
//
// cond_true:                                        ; preds = %cond_false
//   %tmp3 = sub i32 %y, %x
//   %tmp4 = call i32 @gcd(i32 %x, i32 %tmp3)
//   ret i32 %tmp4
//
// cond_false1:                                      ; preds = %cond_false
//   %tmp5 = sub i32 %x, %y
//   %tmp6 = call i32 @gcd(i32 %tmp5, i32 %y)
//   ret i32 %tmp6
// LLVM IR module which contains one function to return the GCD of two numbers
// }
Module* CodegenGcd(LLVMContext* context) {
  Module* mod = new Module("gcd", *context);
  Constant* c = mod->getOrInsertFunction("gcd", IntegerType::get(*context, 32),
      IntegerType::get(*context, 32), IntegerType::get(*context, 32), NULL);
  Function* gcd = cast<Function>(c);
  Function::arg_iterator args = gcd->arg_begin();
  Value* x = &*args;
  ++args;
  x->setName("x");
  Value* y = &*args;
  ++args;
  y->setName("y");
  BasicBlock* entry = BasicBlock::Create(*context, "entry", gcd);
  BasicBlock* ret = BasicBlock::Create(*context, "return", gcd);
  BasicBlock* cond_false = BasicBlock::Create(*context, "cond_false", gcd);
  BasicBlock* cond_true = BasicBlock::Create(*context, "cond_true", gcd);
  BasicBlock* cond_false_2 = BasicBlock::Create(*context, "cond_false", gcd);
  IRBuilder<> builder(entry);
  Value* xEqualsY = builder.CreateICmpEQ(x, y, "tmp");
  builder.CreateCondBr(xEqualsY, ret, cond_false);  builder.SetInsertPoint(ret);
  builder.CreateRet(x);
  builder.SetInsertPoint(cond_false);
  Value* xLessThanY = builder.CreateICmpULT(x, y, "tmp");
  builder.CreateCondBr(xLessThanY, cond_true, cond_false_2);
  builder.SetInsertPoint(cond_true);
  Value* yMinusX = builder.CreateSub(y, x, "tmp");
  Value* args1[2] =  {x , yMinusX};
  Value* recur_1 = builder.CreateCall(gcd, args1, "tmp");
  builder.CreateRet(recur_1);
  builder.SetInsertPoint(cond_false_2);
  Value* xMinusY = builder.CreateSub(x, y, "tmp");
  Value* args2[2] = {xMinusY, y};
  Value* recur_2 = builder.CreateCall(gcd, args2,  "tmp");
  builder.CreateRet(recur_2);
  return mod;
}

TEST_F(InstructionCounterTest, TestMemInstrCount) {
  Module* GcdModule = CodegenGcd(&getGlobalContext());
  InstructionCounter* instruction_counter = new InstructionCounter();
  instruction_counter->visit(*GcdModule);
  std::cout << instruction_counter->PrintCounters();
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_FUNCTIONS), 1);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_BLOCKS), 5);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_INSTS), 11);
  // Test Category Totals
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TERMINATOR_INSTS), 5);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::MEMORY_INSTS), 0);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::OTHER_INSTS), 4);

  // Test Reset
  instruction_counter->ResetCount();
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_FUNCTIONS), 0);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_BLOCKS), 0);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TOTAL_INSTS), 0);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::TERMINATOR_INSTS), 0);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::MEMORY_INSTS), 0);
  EXPECT_EQ(instruction_counter->GetCount(InstructionCounter::OTHER_INSTS), 0);
}

}  // namespace impala

IMPALA_TEST_MAIN();

